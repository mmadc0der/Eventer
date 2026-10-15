use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use crate::codec::{
    accumulate_histogram_with, block_row_count, cmp_numeric, count_header_equality,
    count_rows_filtered, count_rows_filtered_with, decode_rows_filtered_with, decode_rows_included,
    image_contains_column, range_impossible, range_matches, read_timestamp_column,
    read_timestamp_column_included, tighter_lower, tighter_upper, timestamp_column_span,
    try_accumulate_exact_sum, try_add_dense_float, try_unit_stride_runs, visit_matching_timestamps,
    visit_matching_timestamps_with, CapturedColumn, ColumnPredicate, PrefixFilter, RangeEnd,
    RangeFilter, UnitRun,
};
use crate::error::{Error, Result};
use crate::pipeline::{self, Pipeline, PipelineConfig};
use crate::schema::{self, FieldType, Schema};
use crate::segment::{self, Catalog};
use crate::value::{
    self, append_row_json, row_to_json_bytes, row_to_json_bytes_selected, Row, Scalar,
};

/// Maximum JSON bytes a single query may materialize in the response buffer.
pub const MAX_QUERY_BYTES: usize = 64 * 1024 * 1024;

/// Maximum rows a single query may return.
pub const MAX_QUERY_ROWS: usize = 1_000_000;

/// Most buckets one histogram may return. A wider request is rejected before any block is read.
pub const MAX_HISTOGRAM_BUCKETS: usize = 4096;

/// Most distinct non-null values [`Store::distinct_count`] may count.
///
/// One more value is [`Error::Event`](Error::Event) and the caller does not
/// receive a partial count.
pub const MAX_DISTINCT_VALUES: usize = 1_000_000;

/// One epoch-aligned bucket of a histogram.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HistogramBucket {
    pub start_ms: i64,
    pub count: u64,
}

/// One value and how many matching rows have it.
///
/// [`Store::topk`] returns these ordered by `count` descending. Equal counts
/// use the same key order as a grouped aggregate: null first, then numeric
/// order, `false` then `true`, and byte order for string and text. A float
/// uses IEEE-754 total order, so `-0.0` precedes `0.0` when the counts match.
#[derive(Debug, Clone, PartialEq)]
pub struct TopValue {
    pub value: Scalar,
    pub count: u64,
}

/// One column to aggregate. `sum`, `min`, `max`, `var`, and `stddev` may be combined.
///
/// The same field may appear in more than one request; the flags are OR-ed and
/// the column is reported once, in the order the field was first named.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StatRequest {
    pub field: String,
    pub sum: bool,
    pub min: bool,
    pub max: bool,
    pub var: bool,
    pub stddev: bool,
}

/// Population variance of one numeric column.
///
/// [`StatVar::Exact`] is the reduced rational of an int, timestamp, or decimal,
/// already rendered: integer digits when the denominator is 1, the shortest
/// decimal when the denominator's prime factors are only 2 and 5, otherwise
/// `numerator/denominator` with a positive denominator. [`StatVar::Float`] is
/// the IEEE-754 value of `(sum_of_squares / n) - (sum / n)^2`.
#[derive(Debug, Clone, PartialEq)]
pub enum StatVar {
    Exact(String),
    Float(f64),
}

/// Total for one numeric column.
///
/// [`StatSum::Exact`] is the 128-bit sum of an int, a decimal, or a timestamp.
/// `contributed` is false when every matching value was null or no row matched,
/// so a decimal total of zero is formatted as `"0"` rather than at the schema
/// scale, and an empty float sum is the JSON integer `0` rather than `0.0`.
/// [`StatSum::Float`] is IEEE-754 addition in ingest order.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum StatSum {
    Exact { total: i128, contributed: bool },
    Float { total: f64, contributed: bool },
}

/// Aggregates for one column. An absent stat was not requested.
///
/// Requested `min` and `max` are `Some(None)` when no non-null value matched.
/// Requested `var` and `stddev` are `Some(None)` in that same case. One
/// non-null value has variance and standard deviation zero.
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnStats {
    pub field: String,
    pub sum: Option<StatSum>,
    pub min: Option<Option<Scalar>>,
    pub max: Option<Option<Scalar>>,
    pub var: Option<Option<StatVar>>,
    pub stddev: Option<Option<f64>>,
}

/// One group from [`Store::aggregate_grouped`].
///
/// `key` is [`Scalar::Null`] when the group column is null. `columns` is the
/// ungrouped result for the rows in that group alone.
#[derive(Debug, Clone, PartialEq)]
pub struct StatGroup {
    pub key: Scalar,
    pub columns: Vec<ColumnStats>,
}

/// A probability in `0..=1` for [`Store::quantile_with_filter`].
///
/// `1` (including `1.0`) selects the last sorted value. Every smaller `p`
/// selects index `floor(p * n)`. Build one with [`parse_quantile_p`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QuantileP {
    numer: u128,
    scale: u8,
}

/// Counters for the bytes sitting in segment files after the last flush.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Stats {
    pub rows: u64,
    pub blocks: u64,
    pub segments: u64,
    pub data_bytes: u64,
    pub index_bytes: u64,
}

/// Tuning knobs. Defaults target a compact block and a short linger so durable appends finish quickly.
#[derive(Debug, Clone)]
pub struct StoreOptions {
    /// Rows per columnar block. The last block in a flush may be smaller.
    pub block_rows: usize,
    /// zstd level, 1 through 22. Higher is smaller and slower.
    pub zstd_level: i32,
    /// Rotate to a new segment after this many data bytes.
    pub segment_bytes: u64,
    pub parser_threads: usize,
    pub compress_threads: usize,
    /// How long the encoder holds a partial block waiting for more rows.
    pub linger: Duration,
}

impl Default for StoreOptions {
    fn default() -> Self {
        let parallelism = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(2)
            .clamp(2, 8);
        Self {
            block_rows: 2048,
            zstd_level: 3,
            segment_bytes: 64 * 1024 * 1024,
            parser_threads: (parallelism / 2).max(1),
            compress_threads: (parallelism / 2).max(1),
            linger: Duration::from_millis(5),
        }
    }
}

/// Ordered comparison. [`CmpOp::Gt`] and [`CmpOp::Lt`] are exclusive.
/// [`CmpOp::Ge`] and [`CmpOp::Le`] are inclusive.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CmpOp {
    /// `field > value`.
    Gt,
    /// `field >= value`.
    Ge,
    /// `field < value`.
    Lt,
    /// `field <= value`.
    Le,
}

/// Constraint pushed into block decoding.
///
/// Predicates on one query are AND-ed. [`Predicate::In`] matches any listed value.
/// [`Scalar::Null`] matches a null column. An empty [`Predicate::In`] matches nothing.
/// [`Predicate::Cmp`] compares an int, timestamp, decimal, or float column. Nulls
/// do not match a range. A bool, string, text, or json column is rejected.
/// [`Predicate::Prefix`] matches a byte prefix of a string or text value. A null
/// does not match. The literal is raw text, including the word `null`. An empty
/// prefix, or a prefix on any other column type, is rejected.
#[derive(Debug, Clone, PartialEq)]
pub enum Predicate {
    /// `field == value`, for example `Predicate::Eq("type".into(), "assistant".into())`.
    Eq(String, Scalar),
    /// `field` equals one of `values`.
    In(String, Vec<Scalar>),
    /// `field` compared with `value` by [`CmpOp`].
    Cmp(String, CmpOp, Scalar),
    /// `field` starts with `prefix` bytes. String and text only.
    Prefix(String, String),
}

/// Append-only event store. Clone the directory handle by wrapping `Store` in `Arc`.
pub struct Store {
    schema: Schema,
    dir: PathBuf,
    pipeline: Pipeline,
}

impl Store {
    pub fn open(dir: impl AsRef<Path>, schema_path: impl AsRef<Path>) -> Result<Self> {
        Self::open_with(dir, schema_path, StoreOptions::default())
    }

    pub fn open_with(
        dir: impl AsRef<Path>,
        schema_path: impl AsRef<Path>,
        options: StoreOptions,
    ) -> Result<Self> {
        validate_options(&options)?;
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir)?;
        let schema = schema::load_schema(schema_path.as_ref())?;
        let lock_action = schema_lock_action(&dir, &schema)?;
        let catalog = Arc::new(std::sync::Mutex::new(segment::load_catalog(&dir, &schema)?));
        if lock_action == SchemaLockAction::Rewrite {
            // Catalog load already decoded the narrower blocks. Commit the wider
            // schema only after that succeeds, so a decode failure leaves the lock.
            write_schema_lock(&dir.join("schema.lock"), &schema.canonical())?;
        }
        let pipeline = pipeline::spawn(PipelineConfig {
            dir: dir.clone(),
            schema: Arc::new(schema.clone()),
            catalog,
            block_rows: options.block_rows,
            zstd_level: options.zstd_level,
            segment_bytes: options.segment_bytes,
            parser_threads: options.parser_threads,
            compress_threads: options.compress_threads,
            linger: options.linger,
        })?;
        Ok(Store {
            schema,
            dir,
            pipeline,
        })
    }

    /// Queue an event. It is visible to queries after [`Store::flush`] or [`Store::query`].
    pub fn append_json(&self, json: &[u8]) -> Result<()> {
        self.pipeline.append(json, false)
    }

    /// Queue an event and wait until its block is on disk and fsynced.
    pub fn append_json_durable(&self, json: &[u8]) -> Result<()> {
        self.pipeline.append(json, true)
    }

    /// Parse every event, queue the batch, and fsync it once.
    ///
    /// Each event is checked against the schema before any of them is queued.
    /// An empty batch is an error and does not touch the pipeline. The writer
    /// fsyncs when the last event's block is committed, so earlier events in the
    /// same batch share that sync.
    pub fn append_json_batch_durable(&self, events: &[impl AsRef<[u8]>]) -> Result<()> {
        if events.is_empty() {
            return Err(Error::event("event batch must not be empty"));
        }
        for event in events {
            value::parse_event(&self.schema, event.as_ref())?;
        }
        self.pipeline.append_batch_durable(events)
    }

    /// Force a partial block out and fsync it.
    pub fn flush(&self) -> Result<()> {
        self.pipeline.flush()
    }

    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    /// Inclusive range on the schema timestamp, in unix milliseconds. Results are in ingest order.
    pub fn query(&self, from_ms: i64, to_ms: i64) -> Result<Vec<Row>> {
        self.query_with_filter(from_ms, to_ms, &[])
    }

    /// Inclusive time range plus equality predicates and numeric ranges.
    ///
    /// Blocks whose zone map cannot contain the requested values are not read.
    /// An integer, decimal, or float zone whose max is below a lower bound, or
    /// whose min is above an upper bound, is not read, including exclusive
    /// bounds. A float equality still reads a block whose range contains the
    /// value. JSON and a float column stored as tag `0` are not a skip. Each
    /// predicate on a block that is read is applied while the block is decoded.
    /// Nulls do not match a range.
    /// If a filter column's constant or dictionary cannot contain the requested value,
    /// the rest of that block is not decoded. Unknown fields and values of the wrong
    /// column type return [`Error::Schema`]. Checks run before [`Store::flush`].
    pub fn query_with_filter(
        &self,
        from_ms: i64,
        to_ms: i64,
        predicates: &[Predicate],
    ) -> Result<Vec<Row>> {
        self.query_window(from_ms, to_ms, predicates, 0, None)
    }

    /// Same match set as [`Store::query_with_filter`], then skip and limit.
    ///
    /// `offset` skips that many matching rows. `limit` of `None` returns the rest,
    /// and `Some(0)` is an error. Time bounds and predicates are applied before
    /// the skip. A contained block with no predicates is skipped only when the
    /// remaining offset covers its row count, the payload row count matches the
    /// frame header, and every payload timestamp is inside the query. A timestamp
    /// outside the query leaves the skip count unchanged. Once `limit` rows have
    /// been collected, later blocks
    /// are not read. Order is ingest order.
    pub fn query_window(
        &self,
        from_ms: i64,
        to_ms: i64,
        predicates: &[Predicate],
        offset: u64,
        limit: Option<u64>,
    ) -> Result<Vec<Row>> {
        if limit == Some(0) {
            return Err(Error::event("query limit must be a positive integer"));
        }
        let Some(resolved) = self.prepare_scan(from_ms, to_ms, predicates)? else {
            return Ok(Vec::new());
        };
        let _retention = self.pipeline.lock_for_read();
        let blocks = self.blocks_in_range(from_ms, to_ms, &resolved);
        let mut rows_out = Vec::new();
        let mut response_bytes = 1usize;
        let mut skipped = 0u64;
        let mut dictionaries = HashMap::new();
        for block in blocks {
            if window_full(limit, rows_out.len()) {
                break;
            }
            if rows_out.len() >= MAX_QUERY_ROWS {
                return Err(Error::event("query row limit exceeded"));
            }
            let string_budget = remaining_query_bytes(response_bytes)?;
            let contained = block.min_ts >= from_ms && block.max_ts <= to_ms;
            let whole = returns_whole_block(
                resolved.unfiltered(),
                contained,
                skipped,
                offset,
                limit,
                rows_out.len(),
                block.row_count,
            );
            if block_exceeds_read_budget(block.uncompressed_len, whole, string_budget, true) {
                return Err(Error::event("query response size limit exceeded"));
            }
            let payload = self.load_scan_block(&block, &mut dictionaries, &resolved, true)?;
            let nrows = block_row_count(&payload)?;
            require_header_row_count(block.row_count, nrows)?;
            if resolved.unfiltered()
                && contained
                && offset_covers_block(offset, skipped, block.row_count)
            {
                if every_timestamp_in_range(&self.schema, &payload, from_ms, to_ms)?
                    && skip_whole_block(offset, block.row_count, &mut skipped)
                {
                    continue;
                }
            }
            if whole && rows_out.len() + nrows > MAX_QUERY_ROWS {
                return Err(Error::event("query row limit exceeded"));
            }
            let rows = decode_rows_filtered_with(
                &self.schema,
                &payload,
                from_ms,
                to_ms,
                string_budget,
                &resolved.predicates,
                &resolved.ranges,
                &resolved.prefixes,
            )?;
            let mut page_done = false;
            for row in rows {
                if row.ts >= from_ms && row.ts <= to_ms {
                    match take_match(offset, limit, &mut skipped, rows_out.len()) {
                        Take::Skip => continue,
                        Take::Done => {
                            page_done = true;
                            break;
                        }
                        Take::Keep => {}
                    }
                    if rows_out.len() >= MAX_QUERY_ROWS {
                        return Err(Error::event("query row limit exceeded"));
                    }
                    let row_bytes = row_to_json_bytes(&self.schema, &row)?;
                    let row_cost = row_bytes
                        .len()
                        .checked_add(if rows_out.is_empty() { 0 } else { 1 })
                        .ok_or_else(|| Error::event("query response size overflow"))?;
                    let next_bytes = response_bytes
                        .checked_add(row_cost)
                        .and_then(|n| n.checked_add(1))
                        .ok_or_else(|| Error::event("query response size overflow"))?;
                    if next_bytes > MAX_QUERY_BYTES {
                        return Err(Error::event("query response size limit exceeded"));
                    }
                    response_bytes = response_bytes
                        .checked_add(row_cost)
                        .ok_or_else(|| Error::event("query response size overflow"))?;
                    rows_out.push(row);
                }
            }
            if page_done {
                break;
            }
        }
        Ok(rows_out)
    }

    /// Same as [`Store::query`], encoded as one JSON array.
    pub fn query_json(&self, from_ms: i64, to_ms: i64) -> Result<Vec<u8>> {
        self.query_json_with_filter(from_ms, to_ms, &[])
    }

    /// Same as [`Store::query_with_filter`], encoded as one JSON array.
    pub fn query_json_with_filter(
        &self,
        from_ms: i64,
        to_ms: i64,
        predicates: &[Predicate],
    ) -> Result<Vec<u8>> {
        self.query_json_window(from_ms, to_ms, predicates, 0, None)
    }

    /// Same as [`Store::query_window`], encoded as one JSON array.
    pub fn query_json_window(
        &self,
        from_ms: i64,
        to_ms: i64,
        predicates: &[Predicate],
        offset: u64,
        limit: Option<u64>,
    ) -> Result<Vec<u8>> {
        self.query_json_projected(from_ms, to_ms, predicates, offset, limit, None)
    }

    /// [`Store::query_json_window`] with a column list.
    ///
    /// Each object contains `columns` in schema order, once each. A selected
    /// null is JSON `null`. A field that is not selected is absent. Filters may
    /// name a column that is not selected. A sliced block does not decompress
    /// a column that is neither selected nor needed by a filter. A single-frame
    /// block is decoded in full and the omitted values are dropped after that.
    /// An index past the schema is [`Error::Schema`](Error::Schema) and does
    /// not read the store.
    pub fn query_json_columns(
        &self,
        from_ms: i64,
        to_ms: i64,
        predicates: &[Predicate],
        offset: u64,
        limit: Option<u64>,
        columns: &[usize],
    ) -> Result<Vec<u8>> {
        self.query_json_projected(from_ms, to_ms, predicates, offset, limit, Some(columns))
    }

    fn query_json_projected(
        &self,
        from_ms: i64,
        to_ms: i64,
        predicates: &[Predicate],
        offset: u64,
        limit: Option<u64>,
        columns: Option<&[usize]>,
    ) -> Result<Vec<u8>> {
        if limit == Some(0) {
            return Err(Error::event("query limit must be a positive integer"));
        }
        let keep = match columns {
            None => None,
            Some(indexes) => Some(select_keep(&self.schema, indexes)?),
        };
        let Some(resolved) = self.prepare_scan(from_ms, to_ms, predicates)? else {
            return Ok(b"[]".to_vec());
        };
        let load_mask = keep
            .as_ref()
            .map(|keep| columns_to_load(&self.schema, &resolved, keep));
        let _retention = self.pipeline.lock_for_read();
        let blocks = self.blocks_in_range(from_ms, to_ms, &resolved);
        let mut out = Vec::from(b"[");
        let mut wrote = false;
        let mut row_count = 0usize;
        let mut skipped = 0u64;
        let mut dictionaries = HashMap::new();
        for block in blocks {
            if window_full(limit, row_count) {
                break;
            }
            if row_count >= MAX_QUERY_ROWS {
                return Err(Error::event("query row limit exceeded"));
            }
            let string_budget = remaining_query_bytes(out.len())?;
            let contained = block.min_ts >= from_ms && block.max_ts <= to_ms;
            let whole = returns_whole_block(
                resolved.unfiltered(),
                contained,
                skipped,
                offset,
                limit,
                row_count,
                block.row_count,
            );
            if block_exceeds_read_budget(
                block.uncompressed_len,
                whole,
                string_budget,
                keep.is_none(),
            ) {
                return Err(Error::event("query response size limit exceeded"));
            }
            let loaded = if let Some(mask) = &load_mask {
                self.read_loaded_block(
                    &block,
                    &mut dictionaries,
                    segment::BlockLoad {
                        through_column: None,
                        columns: Some(mask),
                    },
                )?
            } else {
                segment::LoadedBlock {
                    bytes: self.load_scan_block(&block, &mut dictionaries, &resolved, true)?,
                    included: None,
                }
            };
            let payload = &loaded.bytes;
            let nrows = block_row_count(payload)?;
            require_header_row_count(block.row_count, nrows)?;
            if resolved.unfiltered()
                && contained
                && offset_covers_block(offset, skipped, block.row_count)
            {
                let covered = match &loaded.included {
                    Some(included) => {
                        let timestamps =
                            read_timestamp_column_included(&self.schema, payload, included)?;
                        timestamps.iter().all(|ts| *ts >= from_ms && *ts <= to_ms)
                    }
                    None => every_timestamp_in_range(&self.schema, payload, from_ms, to_ms)?,
                };
                if covered && skip_whole_block(offset, block.row_count, &mut skipped) {
                    continue;
                }
            }
            if whole && row_count + nrows > MAX_QUERY_ROWS {
                return Err(Error::event("query row limit exceeded"));
            }
            let rows = decode_rows_included(
                &self.schema,
                payload,
                from_ms,
                to_ms,
                string_budget,
                &resolved.predicates,
                &resolved.ranges,
                &resolved.prefixes,
                loaded.included.as_deref(),
            )?;
            let mut page_done = false;
            for row in rows {
                if row.ts >= from_ms && row.ts <= to_ms {
                    match take_match(offset, limit, &mut skipped, row_count) {
                        Take::Skip => continue,
                        Take::Done => {
                            page_done = true;
                            break;
                        }
                        Take::Keep => {}
                    }
                    if row_count >= MAX_QUERY_ROWS {
                        return Err(Error::event("query row limit exceeded"));
                    }
                    let mark = out.len();
                    if wrote {
                        out.push(b',');
                    }
                    let appended = if let Some(keep) = &keep {
                        row_to_json_bytes_selected(&self.schema, &row, keep).map(|bytes| {
                            out.extend_from_slice(&bytes);
                        })
                    } else {
                        append_row_json(&mut out, &self.schema, &row, None)
                    };
                    if let Err(err) = appended {
                        out.truncate(mark);
                        return Err(err);
                    }
                    let next_len = out
                        .len()
                        .checked_add(1)
                        .ok_or_else(|| Error::event("query response size overflow"))?;
                    if next_len > MAX_QUERY_BYTES {
                        out.truncate(mark);
                        return Err(Error::event("query response size limit exceeded"));
                    }
                    wrote = true;
                    row_count += 1;
                }
            }
            if page_done {
                break;
            }
        }
        out.push(b']');
        Ok(out)
    }

    /// One JSON object per distinct value of `by`: the matching row with the
    /// greatest timestamp. A tie keeps the later row in ingest order.
    ///
    /// `columns` is `None` for every field, or schema indexes. Each object
    /// contains those fields in schema order. A selected null is JSON `null`.
    /// A field that is not selected is absent. Filters may name a column that
    /// is not selected. A sliced block does not decompress a column that is
    /// neither selected, nor the group key, nor needed by a filter. A
    /// single-frame block is decoded in full and the omitted values are dropped
    /// after that.
    ///
    /// `by` is one bool, string, text, int, timestamp, or decimal column. An
    /// empty name, an unknown field, a float, or a json column is an error and
    /// does not read the store. The array is ordered by that key: null first,
    /// then numeric order for int, timestamp, and decimal, `false` then `true`
    /// for bool, and byte order for string and text. A null key is one object.
    /// No matching row is `[]`.
    ///
    /// More than [`MAX_HISTOGRAM_BUCKETS`] distinct keys, or a response that
    /// would exceed [`MAX_QUERY_BYTES`], returns [`Error::Event`](Error::Event)
    /// and no array. A block the sparse index places outside the range is not
    /// read. A block the zone map can reject is not read.
    pub fn latest_json(
        &self,
        from_ms: i64,
        to_ms: i64,
        predicates: &[Predicate],
        by: &str,
        columns: Option<&[usize]>,
    ) -> Result<Vec<u8>> {
        let group = resolve_group_field(&self.schema, by)?;
        let keep = match columns {
            None => None,
            Some(indexes) => Some(select_keep(&self.schema, indexes)?),
        };
        let Some(resolved) = self.prepare_scan(from_ms, to_ms, predicates)? else {
            return Ok(b"[]".to_vec());
        };
        let load_mask = keep.as_ref().map(|keep| {
            let mut load = columns_to_load(&self.schema, &resolved, keep);
            if let Some(slot) = load.get_mut(group.index) {
                *slot = true;
            }
            load
        });
        let _retention = self.pipeline.lock_for_read();
        let blocks = self.blocks_in_range(from_ms, to_ms, &resolved);
        let mut winners: BTreeMap<GroupKey, LatestHit> = BTreeMap::new();
        let mut key_bytes = 0usize;
        let mut json_bytes = 0usize;
        let mut dictionaries = HashMap::new();
        for block in blocks {
            if block_exceeds_read_budget(block.uncompressed_len, false, 0, true) {
                return Err(Error::event("query response size limit exceeded"));
            }
            let loaded = if let Some(mask) = &load_mask {
                self.read_loaded_block(
                    &block,
                    &mut dictionaries,
                    segment::BlockLoad {
                        through_column: None,
                        columns: Some(mask),
                    },
                )?
            } else {
                segment::LoadedBlock {
                    bytes: self.load_scan_block(&block, &mut dictionaries, &resolved, true)?,
                    included: None,
                }
            };
            let payload = &loaded.bytes;
            let nrows = block_row_count(payload)?;
            require_header_row_count(block.row_count, nrows)?;
            let rows = decode_rows_included(
                &self.schema,
                payload,
                from_ms,
                to_ms,
                MAX_QUERY_BYTES,
                &resolved.predicates,
                &resolved.ranges,
                &resolved.prefixes,
                loaded.included.as_deref(),
            )?;
            for row in rows {
                if row.ts < from_ms || row.ts > to_ms {
                    continue;
                }
                let json = if let Some(keep) = &keep {
                    row_to_json_bytes_selected(&self.schema, &row, keep)?
                } else {
                    row_to_json_bytes(&self.schema, &row)?
                };
                let scalar = row.values.get(group.index).cloned().ok_or_else(|| {
                    Error::corrupt(format!("field `{}` is missing a row", group.name))
                })?;
                let key = group_key_from_scalar(&group, scalar)?;
                note_latest(
                    &mut winners,
                    &mut key_bytes,
                    &mut json_bytes,
                    key,
                    row.ts,
                    json,
                )?;
            }
        }
        let mut out = Vec::from(b"[");
        let mut wrote = false;
        for hit in winners.values() {
            if wrote {
                out.push(b',');
            }
            wrote = true;
            out.extend_from_slice(&hit.json);
        }
        out.push(b']');
        Ok(out)
    }

    /// Inclusive range count. [`Store::count_with_filter`] with no predicates.
    pub fn count(&self, from_ms: i64, to_ms: i64) -> Result<u64> {
        self.count_with_filter(from_ms, to_ms, &[])
    }

    /// How many rows an unpaged [`Store::query_with_filter`] would return.
    ///
    /// A block the zone map can reject is not read. A block with no predicates
    /// contributes the number of payload timestamps inside the range and does
    /// not decode the other columns. Columns after the last predicate are not
    /// decompressed. When every timestamp is inside, that
    /// number is the block row count. A timestamp outside the range is not
    /// counted, even when the index min/max sit inside the range. Predicate
    /// blocks decode the timestamp and the predicate columns only.
    ///
    /// A range on an int, decimal, or float column is the exception. When the
    /// zone's minimum and maximum both sit inside every range and that column
    /// recorded no nulls, the block is counted the same way as a block with no
    /// predicates: timestamps only, and the proved column stays compressed or
    /// unread. `gt` needs a minimum strictly above the bound. `ge` needs a
    /// minimum greater than or equal to it. `lt` and `le` use the maximum the
    /// same way. A null flag, tag `0`, or a bound the zone does not cover still
    /// decodes. An equality on a numeric or json column still reads that column.
    /// A histogram with no equality uses the same proof, then fills its buckets
    /// from the timestamp header.
    ///
    /// One equality on a string or text column, with no other predicate, is
    /// counted from the column header when every payload timestamp sits in the
    /// inclusive range. A constant string contributes the block row count or
    /// zero. One period of dictionary codes contributes the matches in that
    /// period, repeated across the rows, without expanding the codes. A null,
    /// a full per-row code vector, raw strings, a second predicate, or a
    /// timestamp outside the range still walks rows. The sparse index is not
    /// that check. A constant or a positive non-wrapping timestamp stride may
    /// prove it from its header. Any other timestamp encoding still expands.
    ///
    /// One equality on a bool column uses the same rule. A constant bool
    /// contributes the block row count or zero. One bitmap period contributes
    /// the matches in that period, repeated across the rows, without expanding
    /// a bool per row. A null, a full per-row bitmap, a second predicate, or a
    /// timestamp outside the range still walks rows.
    ///
    /// A numeric range on that same count is not a residual when the zone
    /// implies every range on the block. The count then drops those ranges
    /// and uses the header path above, and does not decompress the range
    /// columns. A null in the zone is not a proof. A block whose zone does
    /// not cover a bound still walks, and so does a second equality, a bloom
    /// or unknown zone that does not prove the range, or a timestamp outside
    /// the range. The histogram path does not use this shortcut.
    pub fn count_with_filter(
        &self,
        from_ms: i64,
        to_ms: i64,
        predicates: &[Predicate],
    ) -> Result<u64> {
        let Some(resolved) = self.prepare_scan(from_ms, to_ms, predicates)? else {
            return Ok(0);
        };
        let _retention = self.pipeline.lock_for_read();
        let blocks = self.blocks_for_count(from_ms, to_ms, &resolved);
        let mut total = 0u64;
        let mut dictionaries = HashMap::new();
        for (block, plan) in blocks {
            if block_exceeds_read_budget(block.uncompressed_len, false, 0, true) {
                return Err(Error::event("query response size limit exceeded"));
            }
            let matched = match plan {
                CountPlan::ProvedHeader { column } => self.count_zone_proved_header(
                    &block,
                    &mut dictionaries,
                    from_ms,
                    to_ms,
                    &resolved,
                    column,
                )?,
                CountPlan::ProvedRanges | CountPlan::Rows => {
                    let proved = matches!(plan, CountPlan::ProvedRanges);
                    let through = if proved {
                        self.schema.timestamp_index
                    } else {
                        needed_column(&self.schema, &resolved, None)
                    };
                    let payload = self.read_block_bytes(
                        &block,
                        &mut dictionaries,
                        segment::BlockLoad {
                            through_column: Some(through),
                            columns: None,
                        },
                    )?;
                    let nrows = block_row_count(&payload)?;
                    require_header_row_count(block.row_count, nrows)?;
                    let (predicates, ranges, prefixes) = if proved {
                        ([].as_slice(), [].as_slice(), [].as_slice())
                    } else {
                        (
                            resolved.predicates.as_slice(),
                            resolved.ranges.as_slice(),
                            resolved.prefixes.as_slice(),
                        )
                    };
                    count_rows_filtered_with(
                        &self.schema,
                        &payload,
                        from_ms,
                        to_ms,
                        predicates,
                        ranges,
                        prefixes,
                    )?
                }
            };
            total = total
                .checked_add(matched)
                .ok_or_else(|| Error::event("event count overflow"))?;
        }
        Ok(total)
    }

    /// How many distinct non-null values `field` takes inside the filtered window.
    ///
    /// Filters are the same ones [`Store::count_with_filter`] applies. A block
    /// the sparse index places outside the range is not read. A block the zone
    /// map can reject is not read. Nulls do not count. No non-null match is
    /// `0`.
    ///
    /// An int, timestamp, decimal, bool, string, or text value uses the same
    /// equality as a grouped aggregate key. A float uses IEEE-754 bits, so
    /// `0.0` and `-0.0` are one value when those bits match and two values
    /// when they do not. A non-finite float is the same corrupt error a float
    /// sum returns.
    ///
    /// More than [`MAX_DISTINCT_VALUES`] distinct values returns
    /// [`Error::Event`](Error::Event) and no count. String and text values
    /// also count their bytes. When a new value would push that total past
    /// [`MAX_QUERY_BYTES`], the scan returns `query response size limit
    /// exceeded` and no count. An empty name, an unknown field, or a json
    /// column is an error and does not read the store.
    ///
    /// One int, timestamp, or decimal, with no row predicate, reads each
    /// surviving block's timestamp column. When every payload timestamp is
    /// inside the inclusive range and the target column has no nulls, a
    /// constant, a stride, or stride pieces whose stride is -1, 0, or 1 and
    /// whose values do not wrap become inclusive intervals. The count is the
    /// number of integers in the union of those intervals. Rows are not
    /// inserted one by one. A null, a bit-packed or frame-of-reference column,
    /// a stride other than -1, 0, or 1, wrapping arithmetic, a float, a bool,
    /// a string, a text, a json column, a block the range cuts, or a predicate
    /// still inserts the surviving values into the set. A stride of 2 whose
    /// values are 0 and 2 counts as 2, not as the closed interval from 0 to 2.
    /// The sparse index is not the inside check.
    ///
    /// When the request has no row predicate and the column is a bool, string,
    /// or text, each surviving block is checked from its timestamp column. If
    /// every payload timestamp is inside the inclusive range, a bool zone
    /// contributes 0, 1, or 2 and an exact string or text zone contributes its
    /// values. Nulls do not count, so an all-null column contributes nothing.
    /// Those sets are unioned and the column is not decoded. A bloom, an
    /// unknown zone, a float, an int, a timestamp, a decimal, or a json column
    /// still walks the rows. A block the range cuts, or any predicate, still
    /// walks. A span that mixes the two unions the proved blocks and walks
    /// only the rest. The sparse index is not that inside check.
    pub fn distinct_count(
        &self,
        from_ms: i64,
        to_ms: i64,
        predicates: &[Predicate],
        field: &str,
    ) -> Result<u64> {
        let column = resolve_distinct_field(&self.schema, field)?;
        let Some(resolved) = self.prepare_scan(from_ms, to_ms, predicates)? else {
            return Ok(0);
        };
        let _retention = self.pipeline.lock_for_read();
        let mut seen = DistinctSet::new(column.ty);
        let mut dictionaries = HashMap::new();
        if resolved.unfiltered() && zone_distinct_column(column.ty) {
            let blocks = self.prepare_zone_distinct_blocks(from_ms, to_ms, &column);
            for (block, answer) in blocks {
                if let ZoneDistinct::Contained(values) = &answer {
                    let payload = self.read_distinct_prefix(&block, &mut dictionaries)?;
                    match timestamp_column_span(&self.schema, &payload, from_ms, to_ms)? {
                        Some(span) if span.inside => {
                            for value in values {
                                seen.note(&column.name, column.ty, value)?;
                            }
                            continue;
                        }
                        _ => {}
                    }
                    if image_contains_column(&self.schema, &payload, column.index)? {
                        self.absorb_distinct_payload(
                            &payload, from_ms, to_ms, &resolved, &column, &mut seen,
                        )?;
                        continue;
                    }
                }
                self.absorb_distinct_block(
                    &block,
                    &mut dictionaries,
                    from_ms,
                    to_ms,
                    &resolved,
                    &column,
                    &mut seen,
                )?;
            }
        } else if resolved.unfiltered()
            && matches!(
                column.ty,
                FieldType::Int | FieldType::Timestamp | FieldType::Decimal { .. }
            )
        {
            let blocks = self.blocks_in_range(from_ms, to_ms, &resolved);
            let timestamp_field = column.index == self.schema.timestamp_index;
            let mut runs = Some(Vec::new());
            for block in blocks {
                if block_exceeds_read_budget(block.uncompressed_len, false, 0, true) {
                    return Err(Error::event("query response size limit exceeded"));
                }
                let payload = self.read_block_bytes(
                    &block,
                    &mut dictionaries,
                    segment::BlockLoad {
                        through_column: Some(needed_column(
                            &self.schema,
                            &resolved,
                            Some(column.index),
                        )),
                        columns: None,
                    },
                )?;
                let nrows = block_row_count(&payload)?;
                require_header_row_count(block.row_count, nrows)?;
                if runs.is_some() {
                    let contained =
                        match timestamp_column_span(&self.schema, &payload, from_ms, to_ms)? {
                            Some(span) if span.inside => {
                                try_unit_stride_runs(&self.schema, &payload, column.index)?
                            }
                            Some(_) => None,
                            None => Some(Vec::new()),
                        };
                    match contained {
                        Some(block_runs) => {
                            runs.as_mut()
                                .expect("unit-stride runs are still open")
                                .extend(block_runs.into_iter().map(|run| (run.start, run.end)));
                            continue;
                        }
                        None => {
                            let taken = runs.take().expect("unit-stride runs are still open");
                            spill_distinct_runs(&taken, &mut seen)?;
                        }
                    }
                }
                let mut captures = if timestamp_field {
                    Vec::new()
                } else {
                    capture_columns([column.index])
                };
                visit_matching_timestamps_with(
                    &self.schema,
                    &payload,
                    from_ms,
                    to_ms,
                    &resolved.predicates,
                    &resolved.ranges,
                    &resolved.prefixes,
                    &mut captures,
                    |ts, row, captured| {
                        if timestamp_field {
                            return seen.note(&column.name, column.ty, &Scalar::Timestamp(ts));
                        }
                        let scalar = captured
                            .first()
                            .and_then(|slot| slot.values.get(row))
                            .ok_or_else(|| {
                                Error::corrupt(format!("field `{}` is missing a row", column.name))
                            })?;
                        seen.note(&column.name, column.ty, scalar)
                    },
                )?;
            }
            if let Some(collected) = runs {
                return union_distinct_count(&collected);
            }
        } else {
            let blocks = self.blocks_in_range(from_ms, to_ms, &resolved);
            for block in blocks {
                self.absorb_distinct_block(
                    &block,
                    &mut dictionaries,
                    from_ms,
                    to_ms,
                    &resolved,
                    &column,
                    &mut seen,
                )?;
            }
        }
        Ok(seen.len() as u64)
    }

    /// Overlapping blocks, with the zone set to union when the block is fully inside.
    ///
    /// A missing zone, a bloom, or any column this path cannot trust is
    /// [`ZoneDistinct::Walk`]. The sparse index only drops blocks that cannot
    /// overlap the range.
    fn prepare_zone_distinct_blocks(
        &self,
        from_ms: i64,
        to_ms: i64,
        column: &DistinctColumn,
    ) -> Vec<(segment::BlockMeta, ZoneDistinct)> {
        let catalog = self.catalog();
        let mut blocks = Vec::new();
        for segment in &catalog.segments {
            for (index, block) in segment.blocks.iter().enumerate() {
                if block.max_ts < from_ms || block.min_ts > to_ms {
                    continue;
                }
                let answer = segment
                    .zones
                    .get(index)
                    .and_then(|zone| zone.columns.get(column.index))
                    .map(|zone| zone_distinct_values(column.ty, zone))
                    .unwrap_or(ZoneDistinct::Walk);
                blocks.push((block.clone(), answer));
            }
        }
        blocks
    }

    /// Timestamp prefix of one block. A single frame is the whole block.
    ///
    /// An empty block is not a proof. The sparse index is not this check.
    fn read_distinct_prefix(
        &self,
        block: &segment::BlockMeta,
        dictionaries: &mut HashMap<u32, Option<Vec<u8>>>,
    ) -> Result<Vec<u8>> {
        if block_exceeds_read_budget(block.uncompressed_len, false, 0, true) {
            return Err(Error::event("query response size limit exceeded"));
        }
        let payload = self.read_block_bytes(
            block,
            dictionaries,
            segment::BlockLoad {
                through_column: Some(self.schema.timestamp_index),
                columns: None,
            },
        )?;
        let nrows = block_row_count(&payload)?;
        require_header_row_count(block.row_count, nrows)?;
        Ok(payload)
    }

    fn absorb_distinct_block(
        &self,
        block: &segment::BlockMeta,
        dictionaries: &mut HashMap<u32, Option<Vec<u8>>>,
        from_ms: i64,
        to_ms: i64,
        resolved: &ResolvedScan,
        column: &DistinctColumn,
        seen: &mut DistinctSet,
    ) -> Result<()> {
        if block_exceeds_read_budget(block.uncompressed_len, false, 0, true) {
            return Err(Error::event("query response size limit exceeded"));
        }
        let payload = self.read_block_bytes(
            block,
            dictionaries,
            segment::BlockLoad {
                through_column: Some(needed_column(&self.schema, resolved, Some(column.index))),
                columns: None,
            },
        )?;
        let nrows = block_row_count(&payload)?;
        require_header_row_count(block.row_count, nrows)?;
        self.absorb_distinct_payload(&payload, from_ms, to_ms, resolved, column, seen)
    }

    fn absorb_distinct_payload(
        &self,
        payload: &[u8],
        from_ms: i64,
        to_ms: i64,
        resolved: &ResolvedScan,
        column: &DistinctColumn,
        seen: &mut DistinctSet,
    ) -> Result<()> {
        let timestamp_field = column.index == self.schema.timestamp_index;
        let mut captures = if timestamp_field {
            Vec::new()
        } else {
            capture_columns([column.index])
        };
        visit_matching_timestamps_with(
            &self.schema,
            &payload,
            from_ms,
            to_ms,
            &resolved.predicates,
            &resolved.ranges,
            &resolved.prefixes,
            &mut captures,
            |ts, row, captured| {
                if timestamp_field {
                    return seen.note(&column.name, column.ty, &Scalar::Timestamp(ts));
                }
                let scalar = captured
                    .first()
                    .and_then(|slot| slot.values.get(row))
                    .ok_or_else(|| {
                        Error::corrupt(format!("field `{}` is missing a row", column.name))
                    })?;
                seen.note(&column.name, column.ty, scalar)
            },
        )
    }

    /// The `k` values of `field` that occur most often inside the filtered window.
    ///
    /// Filters are the same ones [`Store::count_with_filter`] applies. A block
    /// the sparse index places outside the range is not read. A block the zone
    /// map can reject is not read. `count` is how many matching rows have that
    /// value, including rows whose value is null. No matching row is an empty
    /// list. The list has at most `k` entries. It is ordered by `count`
    /// descending. Equal counts use the same key order as
    /// [`Store::aggregate_grouped`]: null first, then numeric order for int,
    /// timestamp, and decimal, `false` then `true` for bool, and byte order for
    /// string and text. A float compares by IEEE-754 bits, so `0.0` and `-0.0`
    /// are one value when those bits match and two values when they do not.
    /// Equal float counts use total order. A non-finite float is the same
    /// corrupt error a float sum returns.
    ///
    /// `k` is an integer in `1..=4096`. More than [`MAX_DISTINCT_VALUES`]
    /// distinct values, including null, returns [`Error::Event`](Error::Event)
    /// (`distinct value limit exceeded`) and no list. String and text values
    /// also count their bytes. When a new value would push that total past
    /// [`MAX_QUERY_BYTES`], the scan returns `query response size limit
    /// exceeded` and no list. An empty name, an unknown field, or a json
    /// column is an error and does not read the store.
    pub fn topk(
        &self,
        from_ms: i64,
        to_ms: i64,
        predicates: &[Predicate],
        field: &str,
        k: usize,
    ) -> Result<Vec<TopValue>> {
        if k == 0 || k > MAX_HISTOGRAM_BUCKETS {
            return Err(Error::event("`k` must be an integer in 1..=4096"));
        }
        let column = resolve_topk_field(&self.schema, field)?;
        let Some(resolved) = self.prepare_scan(from_ms, to_ms, predicates)? else {
            return Ok(Vec::new());
        };
        let _retention = self.pipeline.lock_for_read();
        let blocks = self.blocks_in_range(from_ms, to_ms, &resolved);
        let mut tally = TopTally::new(column.ty);
        let timestamp_field = column.index == self.schema.timestamp_index;
        let mut dictionaries = HashMap::new();
        for block in blocks {
            if block_exceeds_read_budget(block.uncompressed_len, false, 0, true) {
                return Err(Error::event("query response size limit exceeded"));
            }
            let payload = self.read_block_bytes(
                &block,
                &mut dictionaries,
                segment::BlockLoad {
                    through_column: Some(needed_column(
                        &self.schema,
                        &resolved,
                        Some(column.index),
                    )),
                    columns: None,
                },
            )?;
            let nrows = block_row_count(&payload)?;
            require_header_row_count(block.row_count, nrows)?;
            let mut captures = if timestamp_field {
                Vec::new()
            } else {
                capture_columns([column.index])
            };
            visit_matching_timestamps_with(
                &self.schema,
                &payload,
                from_ms,
                to_ms,
                &resolved.predicates,
                &resolved.ranges,
                &resolved.prefixes,
                &mut captures,
                |ts, row, captured| {
                    if timestamp_field {
                        return tally.note(&column.name, column.ty, &Scalar::Timestamp(ts));
                    }
                    let scalar = captured
                        .first()
                        .and_then(|slot| slot.values.get(row))
                        .ok_or_else(|| {
                            Error::corrupt(format!("field `{}` is missing a row", column.name))
                        })?;
                    tally.note(&column.name, column.ty, scalar)
                },
            )?;
        }
        Ok(tally.finish(k))
    }

    /// Inclusive range histogram. [`Store::histogram_with_filter`] with no predicates.
    pub fn histogram(
        &self,
        from_ms: i64,
        to_ms: i64,
        bucket_ms: i64,
    ) -> Result<Vec<HistogramBucket>> {
        self.histogram_with_filter(from_ms, to_ms, bucket_ms, &[])
    }

    /// Counts for every epoch-aligned bucket that intersects `from_ms..=to_ms`.
    ///
    /// Bucket `i` starts at `floor(from_ms / bucket_ms) * bucket_ms + i * bucket_ms`,
    /// using division that rounds toward negative infinity. The list includes a
    /// bucket whose count is zero. The sum of the counts is what
    /// [`Store::count_with_filter`] would return for the same range and predicates.
    /// A row is counted in the bucket that contains its own timestamp.
    ///
    /// `bucket_ms` is a positive integer. More than [`MAX_HISTOGRAM_BUCKETS`]
    /// buckets is an error, and no block is read for that call. A block the
    /// sparse index places outside the range is not read. A block the zone map
    /// can reject is not read. A block with no equality, no in list, and no
    /// prefix reads the timestamp column only when it has no numeric range, or
    /// when the zone implies every numeric range. When that column is a
    /// constant or a positive stride that does not wrap, it has no nulls, and
    /// both encoded endpoints sit inside the inclusive range, each run is
    /// added to the buckets without expanding the column. A range the zone
    /// does not prove still walks the rows. An equality, an in list, a prefix,
    /// a null in the zone, a negative stride, wrapping arithmetic, stride
    /// pieces, or a bit-packed timestamp still walks. A timestamp
    /// outside the range is not counted. The sparse-index min/max are not that
    /// check.
    pub fn histogram_with_filter(
        &self,
        from_ms: i64,
        to_ms: i64,
        bucket_ms: i64,
        predicates: &[Predicate],
    ) -> Result<Vec<HistogramBucket>> {
        let Some(span) = histogram_span(from_ms, to_ms, bucket_ms)? else {
            return Ok(Vec::new());
        };
        let Some(resolved) = self.prepare_scan(from_ms, to_ms, predicates)? else {
            return histogram_buckets(span);
        };
        let _retention = self.pipeline.lock_for_read();
        let blocks = self.blocks_for_count(from_ms, to_ms, &resolved);
        let mut counts = vec![0u64; span.buckets];
        let mut dictionaries = HashMap::new();
        for (block, plan) in blocks {
            let proved = matches!(plan, CountPlan::ProvedRanges);
            if block_exceeds_read_budget(block.uncompressed_len, false, 0, true) {
                return Err(Error::event("query response size limit exceeded"));
            }
            let through = if proved {
                self.schema.timestamp_index
            } else {
                needed_column(&self.schema, &resolved, None)
            };
            let payload = self.read_block_bytes(
                &block,
                &mut dictionaries,
                segment::BlockLoad {
                    through_column: Some(through),
                    columns: None,
                },
            )?;
            let nrows = block_row_count(&payload)?;
            require_header_row_count(block.row_count, nrows)?;
            let (predicates, ranges, prefixes) = if proved {
                ([].as_slice(), [].as_slice(), [].as_slice())
            } else {
                (
                    resolved.predicates.as_slice(),
                    resolved.ranges.as_slice(),
                    resolved.prefixes.as_slice(),
                )
            };
            accumulate_histogram_with(
                &self.schema,
                &payload,
                from_ms,
                to_ms,
                predicates,
                ranges,
                prefixes,
                bucket_ms,
                span.first_start,
                &mut counts,
            )?;
        }
        buckets_from_counts(span, &counts)
    }

    /// Sum, minimum, and maximum of numeric columns over an inclusive range.
    ///
    /// Filters are the same ones [`Store::count_with_filter`] applies. A block
    /// the sparse index places outside the range is not read. A block the zone
    /// map can reject is not read. Rows that share a block with a match are
    /// filtered while the block is decoded.
    ///
    /// A request that asks only for `min` and `max`, with no `sum`, no `var`,
    /// no `stddev`, and no row predicate, reads each surviving block's timestamp
    /// column. When every
    /// payload timestamp is inside the inclusive range, int, decimal, and float
    /// extremes come from that block's zone, including a column that also holds
    /// nulls. An all-null column contributes nothing. An unknown zone, including
    /// tag `0` and a non-finite float, still decodes that column. The primary
    /// timestamp is taken from the timestamps just read. A payload timestamp
    /// outside the range decodes the stat columns and keeps the in-range rows.
    /// The sparse index is not that check. A grouped aggregate, or a predicate,
    /// still decodes.
    ///
    /// A request for one int, timestamp, or decimal `sum`, with no `min`, no
    /// `max`, no `var`, no `stddev`, and no row predicate, reads each surviving block's timestamp
    /// column. When every payload timestamp is inside the inclusive range and
    /// the target column has no nulls, a constant, a stride, or stride pieces
    /// are added in row order into the 128-bit accumulator. Other columns in
    /// that block are not decoded. A null, a bit-packed or frame-of-reference
    /// column, a block the timestamp column does not place entirely inside the
    /// range, a `min` or `max`, or a variance keeps the row visitor, including the overflow
    /// error that visitor already returns. The sparse index is not that check.
    ///
    /// Nulls do not contribute. A column whose matching values are all null, or
    /// a filter that matches nothing, has null `min` and `max` and a zero sum.
    /// An int, decimal, or timestamp sum uses a 128-bit accumulator and returns
    /// [`Error::Event`](Error::Event) if that accumulator would overflow. A
    /// float sum is IEEE-754 addition in ingest order inside a block and in
    /// block order across the store. When the request is that one float sum,
    /// with no `min`, no `max`, no `var`, no `stddev`, and no row predicate, a block whose every
    /// payload timestamp is inside the range and whose float column has no
    /// nulls is added from the encoded constant, raw frame, or repeated decimal
    /// stride. The other columns of that block are not visited. The sparse
    /// index is not the inside check. A null, a non-finite value, a block the
    /// range cuts, a `min` or `max`, a variance, or a predicate still visits matching rows,
    /// including the error a non-finite float already returns. A finished float
    /// sum that is not finite returns [`Error::Event`](Error::Event) and no
    /// column stats. A non-finite float already stored is
    /// [`Error::Corrupt`](Error::Corrupt).
    ///
    /// `var` is the population variance and `stddev` is its square root. Nulls
    /// do not contribute to either. No non-null match leaves both empty, which
    /// is not zero. One non-null value is zero. A float variance is the
    /// IEEE-754 value of `(sum_of_squares / n) - (sum / n)^2`, including a
    /// negative result of cancellation. Both sums accumulate in ingest order
    /// inside a block and in block order across the store, the same order a
    /// float sum uses. The square root runs only when `stddev` was requested.
    /// A negative float variance together with `stddev` returns
    /// [`Error::Event`](Error::Event) and no column stats; the message says
    /// the computed variance is negative. A non-finite float moment, or a
    /// non-finite square root when `stddev` was requested, returns that same
    /// error for overflow. An int or timestamp variance is the exact rational
    /// `(n * sum_of_squares - sum^2) / n^2` in lowest terms, in the same
    /// integer units the 128-bit sum accumulates. A decimal variance divides
    /// that rational by `10^(2*scale)` and reduces again, so it uses the same
    /// decimal units as `sum`, `min`, and `max`. Scale 0 is unchanged.
    /// A denominator of 1 is the integer digits. A denominator whose prime
    /// factors are only 2 and 5 is the shortest decimal. Anything else is
    /// `numerator/denominator` with a positive denominator. `stddev` of those
    /// types is the f64 square root of that rational. A sum or a sum of squares
    /// that does not fit in 128 bits returns [`Error::Event`](Error::Event)
    /// and no column stats. At least one of `sum`, `min`, `max`, `var`, or
    /// `stddev` is required. A bool, string, text, or json column is
    /// [`Error::Schema`](Error::Schema). Checks run before the store is read.
    pub fn aggregate_with_filter(
        &self,
        from_ms: i64,
        to_ms: i64,
        predicates: &[Predicate],
        stats: &[StatRequest],
    ) -> Result<Vec<ColumnStats>> {
        let specs = resolve_stat_requests(&self.schema, stats)?;
        let Some(resolved) = self.prepare_scan(from_ms, to_ms, predicates)? else {
            return Ok(empty_column_stats(&specs));
        };
        let _retention = self.pipeline.lock_for_read();
        let mut running: Vec<RunningStat> = specs.into_iter().map(RunningStat::new).collect();
        let dense_float = dense_float_sum(&running, &resolved);
        let minmax_only = running
            .iter()
            .all(|stat| !stat.spec.sum && !stat.spec.needs_moment());
        let exact_sum_only = exact_sum_request(&running);
        let mut dictionaries = HashMap::new();
        if minmax_only && resolved.unfiltered() {
            let blocks = self.prepare_minmax_blocks(from_ms, to_ms, &running);
            for (block, columns) in blocks {
                self.absorb_minmax_block(
                    &block,
                    &columns,
                    &mut dictionaries,
                    from_ms,
                    to_ms,
                    &mut running,
                )?;
            }
        } else if exact_sum_only && resolved.unfiltered() {
            let blocks = self.blocks_in_range(from_ms, to_ms, &resolved);
            for block in blocks {
                self.absorb_exact_sum_block(
                    &block,
                    &mut dictionaries,
                    from_ms,
                    to_ms,
                    &mut running,
                )?;
            }
        } else {
            let blocks = self.blocks_in_range(from_ms, to_ms, &resolved);
            for block in blocks {
                if block_exceeds_read_budget(block.uncompressed_len, false, 0, true) {
                    return Err(Error::event("query response size limit exceeded"));
                }
                let mut through = needed_column(&self.schema, &resolved, None);
                for stat in &running {
                    through = through.max(stat.spec.index);
                }
                let payload = self.read_block_bytes(
                    &block,
                    &mut dictionaries,
                    segment::BlockLoad {
                        through_column: Some(through),
                        columns: None,
                    },
                )?;
                let nrows = block_row_count(&payload)?;
                require_header_row_count(block.row_count, nrows)?;
                if let Some(index) = dense_float {
                    match try_add_dense_float(
                        &self.schema,
                        &payload,
                        index,
                        from_ms,
                        to_ms,
                        &mut running[0].float_sum,
                    )? {
                        Some(rows) => {
                            if rows > 0 {
                                running[0].contributed = true;
                            }
                            continue;
                        }
                        None => {}
                    }
                }
                let mut captures = capture_columns(running.iter().map(|stat| stat.spec.index));
                visit_matching_timestamps_with(
                    &self.schema,
                    &payload,
                    from_ms,
                    to_ms,
                    &resolved.predicates,
                    &resolved.ranges,
                    &resolved.prefixes,
                    &mut captures,
                    |ts, row, captured| {
                        for stat in &mut running {
                            let value = stat_value(&self.schema, stat, ts, row, captured)?;
                            stat.absorb(value)?;
                        }
                        Ok(())
                    },
                )?;
            }
        }
        running.into_iter().map(RunningStat::finish).collect()
    }

    /// Group [`Store::aggregate_with_filter`] by one column.
    ///
    /// `by` is a bool, string, text, int, timestamp, or decimal field. A float
    /// or json column, an empty name, or an unknown field is an error and does
    /// not read the store. The field may also be a `sum`, `min`, `max`, `var`,
    /// or `stddev` target. At least one of those is still required.
    ///
    /// Each group's `columns` are the aggregates of the rows with that key.
    /// Nulls do not contribute to `sum`, `min`, `max`, `var`, or `stddev`. A null key is its own
    /// group. Groups are ordered by the key: null first, then numeric order for
    /// int, timestamp, and decimal, `false` then `true` for bool, and byte order
    /// for string and text. An empty match is an empty list.
    ///
    /// More than [`MAX_HISTOGRAM_BUCKETS`] distinct keys, or string and text
    /// keys whose retained bytes exceed [`MAX_QUERY_BYTES`], returns
    /// [`Error::Event`](Error::Event) and no groups. A block the sparse index
    /// places outside the range is not read. A block the zone map can reject is
    /// not read. Keys come from the rows that survive the filters, not from the
    /// zone's distinct set, because a time bound can cut a block.
    pub fn aggregate_grouped(
        &self,
        from_ms: i64,
        to_ms: i64,
        predicates: &[Predicate],
        stats: &[StatRequest],
        by: &str,
    ) -> Result<Vec<StatGroup>> {
        let specs = resolve_stat_requests(&self.schema, stats)?;
        let group = resolve_group_field(&self.schema, by)?;
        let Some(resolved) = self.prepare_scan(from_ms, to_ms, predicates)? else {
            return Ok(Vec::new());
        };
        let _retention = self.pipeline.lock_for_read();
        let blocks = self.blocks_in_range(from_ms, to_ms, &resolved);
        let mut indexes: Vec<usize> = specs.iter().map(|spec| spec.index).collect();
        if !indexes.contains(&group.index) {
            indexes.push(group.index);
        }
        let mut groups: BTreeMap<GroupKey, Vec<RunningStat>> = BTreeMap::new();
        let mut string_groups: BTreeMap<String, Vec<RunningStat>> = BTreeMap::new();
        let mut null_group: Option<Vec<RunningStat>> = None;
        let mut key_bytes = 0usize;
        let text_group = matches!(group.ty, FieldType::String | FieldType::Text);
        let mut dictionaries = HashMap::new();
        for block in blocks {
            if block_exceeds_read_budget(block.uncompressed_len, false, 0, true) {
                return Err(Error::event("query response size limit exceeded"));
            }
            let through = indexes
                .iter()
                .copied()
                .fold(needed_column(&self.schema, &resolved, None), usize::max);
            let payload = self.read_block_bytes(
                &block,
                &mut dictionaries,
                segment::BlockLoad {
                    through_column: Some(through),
                    columns: None,
                },
            )?;
            let nrows = block_row_count(&payload)?;
            require_header_row_count(block.row_count, nrows)?;
            let mut captures = capture_columns(indexes.iter().copied());
            visit_matching_timestamps_with(
                &self.schema,
                &payload,
                from_ms,
                to_ms,
                &resolved.predicates,
                &resolved.ranges,
                &resolved.prefixes,
                &mut captures,
                |ts, row, captured| {
                    if text_group {
                        match group_text_from_row(&self.schema, &group, row, captured)? {
                            None => {
                                if null_group.is_none() {
                                    reserve_group_key(string_groups.len(), &mut key_bytes, 0)?;
                                    null_group =
                                        Some(specs.iter().cloned().map(RunningStat::new).collect());
                                }
                                absorb_group_stats(
                                    &self.schema,
                                    null_group.as_mut().expect("null group was just inserted"),
                                    ts,
                                    row,
                                    captured,
                                )?;
                            }
                            Some(text) => {
                                if string_groups.contains_key(text) {
                                    absorb_group_stats(
                                        &self.schema,
                                        string_groups.get_mut(text).expect("string group exists"),
                                        ts,
                                        row,
                                        captured,
                                    )?;
                                } else {
                                    let distinct =
                                        string_groups.len() + usize::from(null_group.is_some());
                                    reserve_group_key(distinct, &mut key_bytes, text.len())?;
                                    let running =
                                        string_groups.entry(text.to_owned()).or_insert_with(|| {
                                            specs.iter().cloned().map(RunningStat::new).collect()
                                        });
                                    absorb_group_stats(&self.schema, running, ts, row, captured)?;
                                }
                            }
                        }
                        return Ok(());
                    }
                    let key = group_key_from_row(&self.schema, &group, ts, row, captured)?;
                    if groups.len() >= MAX_HISTOGRAM_BUCKETS && !groups.contains_key(&key) {
                        return Err(Error::event("stats would emit more than 4096 groups"));
                    }
                    let running = groups
                        .entry(key)
                        .or_insert_with(|| specs.iter().cloned().map(RunningStat::new).collect());
                    absorb_group_stats(&self.schema, running, ts, row, captured)?;
                    Ok(())
                },
            )?;
        }
        let ordered: Vec<(GroupKey, Vec<RunningStat>)> = if text_group {
            let mut ordered = Vec::new();
            if let Some(running) = null_group {
                ordered.push((GroupKey::Null, running));
            }
            for (key, running) in string_groups {
                ordered.push((GroupKey::Str(key), running));
            }
            ordered
        } else {
            groups.into_iter().collect()
        };
        ordered
            .into_iter()
            .map(|(key, running)| {
                let columns = running
                    .into_iter()
                    .map(RunningStat::finish)
                    .collect::<Result<_>>()?;
                Ok(StatGroup {
                    key: key.scalar(),
                    columns,
                })
            })
            .collect()
    }

    /// Exact rank of one numeric column over an inclusive range.
    ///
    /// Filters are the same ones [`Store::count_with_filter`] applies. A block
    /// the sparse index places outside the range is not read. A block the zone
    /// map can reject is not read. Rows that share a block with a match are
    /// filtered while the block is decoded, so a timestamp outside the range
    /// does not enter the sample.
    ///
    /// Nulls are not part of `n`. No non-null match returns `Ok(None)`. The
    /// other values are sorted in numeric order and the result is index
    /// `floor(p * n)` when `p < 1`, or index `n - 1` when `p` is 1. An int,
    /// timestamp, decimal, or float uses that column's scalar. A bool, string,
    /// text, or json column, an empty name, or an unknown field is an error
    /// and does not read the store. A non-finite float is corrupt.
    ///
    /// One int, timestamp, or decimal, with no row predicate, reads the
    /// timestamp column. When every payload timestamp sits in the inclusive
    /// range and the target column has no nulls, a constant, a stride, or
    /// stride pieces whose stride is -1, 0, or 1 and whose values do not wrap
    /// are merged as inclusive intervals. Each integer in an interval has that
    /// run's multiplicity, and the same index is selected from the merge.
    /// Those rows are not stored as samples. A null, a bit-packed or
    /// frame-of-reference body, any other stride, wrapping arithmetic, a
    /// float, a timestamp outside the range, or a predicate still sorts one
    /// sample per surviving value. The sparse index is not the containment
    /// check.
    ///
    /// Retained samples are one `i64` or `f64` (8 bytes) or one `i128` (16
    /// bytes) per non-null match. A sample count already at [`MAX_QUERY_ROWS`]
    /// returns [`Error::Event`](Error::Event) (`query row limit exceeded`)
    /// before that sample is stored. The next sample that would push retained
    /// bytes past [`MAX_QUERY_BYTES`] returns the same error kind (`query
    /// response size limit exceeded`). The unit-stride path uses those same
    /// ceilings on the number of values, and it does not keep the values.
    /// Either refusal returns no rank. A prefix of the rows is not a result.
    pub fn quantile_with_filter(
        &self,
        from_ms: i64,
        to_ms: i64,
        predicates: &[Predicate],
        field: &str,
        p: QuantileP,
    ) -> Result<Option<Scalar>> {
        let spec = resolve_quantile_field(&self.schema, field)?;
        let Some(resolved) = self.prepare_scan(from_ms, to_ms, predicates)? else {
            return Ok(None);
        };
        let _retention = self.pipeline.lock_for_read();
        let blocks = self.blocks_in_range(from_ms, to_ms, &resolved);
        if unit_quantile(&spec, &resolved) {
            if let Some(value) = self.quantile_from_unit_runs(from_ms, to_ms, &blocks, &spec, p)? {
                return Ok(value);
            }
        }
        let mut samples = QuantileSamples::new(spec.ty);
        let mut dictionaries = HashMap::new();
        for block in blocks {
            if block_exceeds_read_budget(block.uncompressed_len, false, 0, true) {
                return Err(Error::event("query response size limit exceeded"));
            }
            let through = needed_column(&self.schema, &resolved, Some(spec.index));
            let payload = self.read_block_bytes(
                &block,
                &mut dictionaries,
                segment::BlockLoad {
                    through_column: Some(through),
                    columns: None,
                },
            )?;
            let nrows = block_row_count(&payload)?;
            require_header_row_count(block.row_count, nrows)?;
            let mut captures = capture_columns(std::iter::once(spec.index));
            visit_matching_timestamps_with(
                &self.schema,
                &payload,
                from_ms,
                to_ms,
                &resolved.predicates,
                &resolved.ranges,
                &resolved.prefixes,
                &mut captures,
                |ts, row, captured| {
                    let value = quantile_value(&self.schema, &spec, ts, row, captured)?;
                    samples.push(value)
                },
            )?;
        }
        samples.finish(p)
    }

    /// Delete every block whose maximum timestamp is strictly less than `cutoff_ms`.
    ///
    /// The cutoff is the caller's. It is not taken from the newest timestamp in the
    /// store, so one future event cannot expire the rest of the table. A block is
    /// kept or dropped as a whole: a row older than `cutoff_ms` stays when it shares
    /// a block with a row at or after the cutoff. A segment is removed, including
    /// its `.dat`, `.idx`, `.zon`, and `.dict`, only when every block in it is
    /// eligible. A mixed segment is rewritten by copying the surviving compressed
    /// frames unchanged and publishing that file with rename. The segment
    /// dictionary stays when any surviving frame is `EVBD`.
    ///
    /// Queued events are flushed before any file is removed. After this returns,
    /// a query no longer returns a row from a block whose maximum timestamp is
    /// below the cutoff, including after the directory is opened again. Rows that
    /// shared a kept block stay.
    pub fn drop_blocks_before(&self, cutoff_ms: i64) -> Result<()> {
        self.pipeline.drop_blocks_before(cutoff_ms)
    }

    /// Copy `source` into `destination` with the current encoder, then flush once.
    ///
    /// `source` is an existing store directory (`schema.lock` plus its segment
    /// files). `destination` must not already exist, or it must be an empty
    /// directory. The call reads every row in ingest order, including rows
    /// written before a column was appended, and appends them through the
    /// default pipeline. A `json` column is written back as the original field
    /// text. The destination schema is the source schema.
    ///
    /// The source directory is only read. Nothing in it is truncated, and no
    /// index or zone file is written there. A rejected event, a full disk, or a
    /// crash before the destination is published leaves those files in place
    /// and does not put a store at `destination`. The rewritten store is written
    /// in a private directory and published after the flush. When `destination`
    /// does not exist yet, that directory is renamed into place. When
    /// `destination` already exists, a marker file is created there before the
    /// private directory. The finished files are moved in after they are
    /// fsynced, with `schema.lock` last. The marker is removed only after that
    /// lock is in place and the destination directory is fsynced. Opening the
    /// destination while the marker is present fails, including when
    /// `schema.lock` is already there, and does not read segment files or
    /// create `schema.lock`. A destination that still has that marker is an
    /// unfinished attempt: the marker, any `eventer-rewrite-*` scratch
    /// directory, and store files left beside them are removed and the copy
    /// starts again. A directory that holds a store and has no marker is
    /// refused. A rename or fsync error moves the published names back. The
    /// marker stays until the scratch directory has been removed, and is
    /// unlinked after that.
    ///
    /// The report uses the same totals as [`Store::stats`]: `stored_data_bytes`
    /// is segment data plus dictionary sidecars, and `stored_index_bytes` is
    /// sparse indexes plus zone files. `data_bytes_change` is
    /// `(source − destination) / source` for data bytes, and
    /// `stored_bytes_change` is that ratio for data plus index. A source total
    /// of zero reports a change of `0.0`. A directory that is already in the
    /// current format is still copied.
    ///
    /// Queued events still sitting in a live [`Store`] are not on disk yet.
    /// Flush that store before calling this if those rows should be copied.
    pub fn rewrite_directory(
        source: impl AsRef<Path>,
        destination: impl AsRef<Path>,
    ) -> Result<RewriteReport> {
        let source = source.as_ref();
        let destination = destination.as_ref();
        if !source.is_dir() {
            return Err(Error::io(format!(
                "source {} is not a directory",
                source.display()
            )));
        }
        if !source.join("schema.lock").is_file() {
            return Err(Error::schema(
                "source directory has no schema.lock; this is not a store directory",
            ));
        }
        if destination.exists() {
            if !destination.is_dir() {
                return Err(Error::io(format!(
                    "destination {} is not a directory",
                    destination.display()
                )));
            }
            if rewrite_marker_present(destination) {
                clear_unfinished_rewrite(destination)?;
            }
            if directory_holds_store(destination)? {
                return Err(Error::event(
                    "destination already holds a store; refusing to replace it",
                ));
            }
            if directory_has_entries(destination)? {
                return Err(Error::event(
                    "destination directory is not empty; refusing to publish a store into it",
                ));
            }
        }
        let source_abs = source.canonicalize().map_err(Error::io)?;
        let destination_abs = anchor_path(destination)?;
        if paths_overlap(&source_abs, &destination_abs) {
            return Err(Error::event(
                "source and destination directories must be separate",
            ));
        }

        let source_bytes = directory_stored_bytes(&source_abs)?;
        let schema = schema::load_schema(&source_abs.join("schema.lock"))?;
        let destination_existed = destination_abs.exists();
        let marker = if destination_existed {
            write_rewrite_marker(&destination_abs)?;
            Some(RemoveRewriteMarker::new(destination_abs.clone()))
        } else {
            None
        };
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(Error::io)?
            .as_nanos();
        let scratch_name = format!("eventer-rewrite-{}-{nanos}", std::process::id());
        // An existing destination may be a mount point. `rename` onto that
        // directory returns EBUSY, so the new store has to be written on that
        // filesystem and the files moved in. A path that does not exist yet
        // is still published by renaming a directory on the parent filesystem.
        let bases = if destination_existed {
            vec![destination_abs.clone()]
        } else {
            scratch_bases(&source_abs, &destination_abs)
        };
        let mut last_error = None;
        for base in &bases {
            let Some(created) = scratch_dir_in(
                std::slice::from_ref(base),
                &scratch_name,
                &source_abs,
                &destination_abs,
            )
            .map(Some)
            .or_else(|err| {
                last_error = Some(err);
                Ok::<Option<PathBuf>, Error>(None)
            })?
            else {
                continue;
            };
            let scratch = RemoveOnDrop::new(created);
            match write_rewritten_store(scratch.path(), &source_abs, &schema) {
                Ok((staging, destination_bytes)) => {
                    if rewrite_should_fail_before_publish() {
                        return Err(Error::io(
                            "rewrite failed before the destination was published",
                        ));
                    }
                    publish_directory(&staging, &destination_abs, destination_existed)?;
                    if let Some(marker) = marker {
                        marker.disarm();
                    }
                    return Ok(RewriteReport {
                        source: source_bytes,
                        destination: destination_bytes,
                        data_bytes_change: relative_change(
                            source_bytes.stored_data_bytes,
                            destination_bytes.stored_data_bytes,
                        ),
                        stored_bytes_change: relative_change_sum(
                            source_bytes.stored_data_bytes,
                            source_bytes.stored_index_bytes,
                            destination_bytes.stored_data_bytes,
                            destination_bytes.stored_index_bytes,
                        ),
                    });
                }
                Err(err) if is_no_space(&err) => {
                    last_error = Some(err);
                }
                Err(err) => return Err(err),
            }
        }
        Err(last_error.unwrap_or_else(|| {
            Error::io("could not create a scratch directory outside the source and the destination")
        }))
    }

    /// File sizes from the last committed batch. Call [`Store::flush`] first for a stable view.
    pub fn stats(&self) -> Stats {
        let catalog = self.catalog();
        Stats {
            rows: catalog.rows,
            blocks: catalog.blocks,
            segments: catalog.segments.len() as u64,
            data_bytes: catalog.data_bytes,
            index_bytes: catalog.index_bytes,
        }
    }

    pub fn close(&self) -> Result<()> {
        self.pipeline.shutdown()
    }

    fn prepare_scan(
        &self,
        from_ms: i64,
        to_ms: i64,
        predicates: &[Predicate],
    ) -> Result<Option<ResolvedScan>> {
        if from_ms > to_ms {
            return Ok(None);
        }
        let resolved = resolve_predicates(&self.schema, predicates)?;
        if resolved.impossible {
            return Ok(None);
        }
        self.flush()?;
        Ok(Some(resolved))
    }

    fn read_block_bytes(
        &self,
        block: &segment::BlockMeta,
        dictionaries: &mut HashMap<u32, Option<Vec<u8>>>,
        load: segment::BlockLoad<'_>,
    ) -> Result<Vec<u8>> {
        Ok(self.read_loaded_block(block, dictionaries, load)?.bytes)
    }

    fn read_loaded_block(
        &self,
        block: &segment::BlockMeta,
        dictionaries: &mut HashMap<u32, Option<Vec<u8>>>,
        load: segment::BlockLoad<'_>,
    ) -> Result<segment::LoadedBlock> {
        let data_path = segment::data_path(&self.dir, block.segment_id);
        let uses_dict = segment::frame_uses_dictionary(&data_path, block)?;
        if uses_dict && !dictionaries.contains_key(&block.segment_id) {
            let stored =
                segment::read_dictionary(&segment::dictionary_path(&self.dir, block.segment_id))?;
            dictionaries.insert(block.segment_id, stored.map(|dict| dict.bytes));
        }
        let dictionary = if uses_dict {
            dictionaries
                .get(&block.segment_id)
                .and_then(|dict| dict.as_deref())
        } else {
            None
        };
        segment::read_block_payload_with(&data_path, block, dictionary, load)
    }

    /// Materialize the columns a filtered scan needs, or the whole block when
    /// the caller is going to return rows.
    ///
    /// Count, histogram, and aggregate pass `rows` false and stop after the
    /// predicate column. A query passes `rows` true. The zone map has already
    /// rejected blocks that cannot match, so the row path reads each surviving
    /// block once.
    fn load_scan_block(
        &self,
        block: &segment::BlockMeta,
        dictionaries: &mut HashMap<u32, Option<Vec<u8>>>,
        resolved: &ResolvedScan,
        rows: bool,
    ) -> Result<Vec<u8>> {
        let through = if rows {
            None
        } else {
            Some(needed_column(&self.schema, resolved, None))
        };
        self.read_block_bytes(
            block,
            dictionaries,
            segment::BlockLoad {
                through_column: through,
                columns: None,
            },
        )
    }

    fn blocks_in_range(
        &self,
        from_ms: i64,
        to_ms: i64,
        resolved: &ResolvedScan,
    ) -> Vec<segment::BlockMeta> {
        if resolved.impossible {
            return Vec::new();
        }
        let catalog = self.catalog();
        let mut blocks = Vec::new();
        for segment in &catalog.segments {
            for (index, block) in segment.blocks.iter().enumerate() {
                if block.max_ts < from_ms || block.min_ts > to_ms {
                    continue;
                }
                if segment.zones.get(index).is_some_and(|zone| {
                    !crate::zone::may_match_filters(
                        zone,
                        &resolved.predicates,
                        &resolved.ranges,
                        &resolved.prefixes,
                    )
                }) {
                    continue;
                }
                blocks.push(block.clone());
            }
        }
        blocks
    }

    /// Blocks a count or histogram will read, and which columns that read still needs.
    ///
    /// [`CountPlan::ProvedRanges`] is set only when the block has no equality
    /// and the zone implies every range. A histogram uses that plan to fill
    /// buckets from the timestamp header. [`CountPlan::ProvedHeader`] is set
    /// when the only equality is one string, text, or bool value and that same
    /// zone proof holds. A histogram does not take the header plan: an equality
    /// still walks. Row scans keep using [`Store::blocks_in_range`].
    fn blocks_for_count(
        &self,
        from_ms: i64,
        to_ms: i64,
        resolved: &ResolvedScan,
    ) -> Vec<(segment::BlockMeta, CountPlan)> {
        if resolved.impossible {
            return Vec::new();
        }
        let catalog = self.catalog();
        let mut blocks = Vec::new();
        for segment in &catalog.segments {
            for (index, block) in segment.blocks.iter().enumerate() {
                if block.max_ts < from_ms || block.min_ts > to_ms {
                    continue;
                }
                let zone = segment.zones.get(index);
                if zone.is_some_and(|zone| {
                    !crate::zone::may_match_filters(
                        zone,
                        &resolved.predicates,
                        &resolved.ranges,
                        &resolved.prefixes,
                    )
                }) {
                    continue;
                }
                let proved = resolved.prefixes.is_empty()
                    && zone.is_some_and(|zone| crate::zone::ranges_implied(zone, &resolved.ranges));
                let plan = if proved && resolved.predicates.is_empty() {
                    CountPlan::ProvedRanges
                } else if proved {
                    match header_equality_column(&self.schema, &resolved.predicates) {
                        Some(column) => CountPlan::ProvedHeader { column },
                        None => CountPlan::Rows,
                    }
                } else {
                    CountPlan::Rows
                };
                blocks.push((block.clone(), plan));
            }
        }
        blocks
    }

    /// Header count of one equality after the zone has proved every range.
    ///
    /// The timestamp column and the equality column are the only slices read.
    /// A constant or one dictionary-code period, and a constant or one bool
    /// bitmap period, are the only forms that return here. Anything else walks
    /// the rows and does read the range columns.
    fn count_zone_proved_header(
        &self,
        block: &segment::BlockMeta,
        dictionaries: &mut HashMap<u32, Option<Vec<u8>>>,
        from_ms: i64,
        to_ms: i64,
        resolved: &ResolvedScan,
        column: usize,
    ) -> Result<u64> {
        let mut keep = vec![false; self.schema.fields.len()];
        if let Some(slot) = keep.get_mut(self.schema.timestamp_index) {
            *slot = true;
        }
        if let Some(slot) = keep.get_mut(column) {
            *slot = true;
        }
        let loaded = self.read_loaded_block(
            block,
            dictionaries,
            segment::BlockLoad {
                through_column: Some(column.max(self.schema.timestamp_index)),
                columns: Some(&keep),
            },
        )?;
        let nrows = block_row_count(&loaded.bytes)?;
        require_header_row_count(block.row_count, nrows)?;
        if let Some(matched) = count_header_equality(
            &self.schema,
            &loaded.bytes,
            loaded.included.as_deref(),
            from_ms,
            to_ms,
            &resolved.predicates,
        )? {
            return Ok(matched);
        }
        if loaded.included.is_none() {
            return count_rows_filtered(
                &self.schema,
                &loaded.bytes,
                from_ms,
                to_ms,
                &resolved.predicates,
                &resolved.ranges,
            );
        }
        let payload = self.read_block_bytes(
            block,
            dictionaries,
            segment::BlockLoad {
                through_column: Some(needed_column(&self.schema, resolved, None)),
                columns: None,
            },
        )?;
        let nrows = block_row_count(&payload)?;
        require_header_row_count(block.row_count, nrows)?;
        count_rows_filtered(
            &self.schema,
            &payload,
            from_ms,
            to_ms,
            &resolved.predicates,
            &resolved.ranges,
        )
    }

    /// Blocks that overlap the range, with one zone answer per running stat.
    ///
    /// Copies the numeric extremes only. A missing zone, tag `0`, or any other
    /// column this path cannot trust is [`ColumnMinMax::Decode`].
    fn prepare_minmax_blocks(
        &self,
        from_ms: i64,
        to_ms: i64,
        running: &[RunningStat],
    ) -> Vec<(segment::BlockMeta, Vec<ColumnMinMax>)> {
        let catalog = self.catalog();
        let mut blocks = Vec::new();
        for segment in &catalog.segments {
            for (index, block) in segment.blocks.iter().enumerate() {
                if block.max_ts < from_ms || block.min_ts > to_ms {
                    continue;
                }
                let zone = segment
                    .zones
                    .get(index)
                    .filter(|zone| !zone.columns.is_empty());
                let columns = running
                    .iter()
                    .map(|stat| {
                        if stat.spec.index == self.schema.timestamp_index {
                            return ColumnMinMax::FromTimestamps;
                        }
                        let Some(column) = zone.and_then(|zone| zone.columns.get(stat.spec.index))
                        else {
                            return ColumnMinMax::Decode;
                        };
                        match zone_minmax(stat.spec.ty, column) {
                            Some(ZoneMinMax::Empty) => ColumnMinMax::Empty,
                            Some(ZoneMinMax::Bounds { min, max }) => {
                                ColumnMinMax::Bounds { min, max }
                            }
                            None => ColumnMinMax::Decode,
                        }
                    })
                    .collect();
                blocks.push((block.clone(), columns));
            }
        }
        blocks
    }

    /// `min` and `max` for one block that has no row predicate and no `sum`.
    ///
    /// The timestamp column decides whether the block is fully inside the
    /// range. The sparse index is only used to skip blocks that cannot overlap.
    fn absorb_minmax_block(
        &self,
        block: &segment::BlockMeta,
        columns: &[ColumnMinMax],
        dictionaries: &mut HashMap<u32, Option<Vec<u8>>>,
        from_ms: i64,
        to_ms: i64,
        running: &mut [RunningStat],
    ) -> Result<()> {
        if block_exceeds_read_budget(block.uncompressed_len, false, 0, true) {
            return Err(Error::event("query response size limit exceeded"));
        }
        let payload = self.read_block_bytes(
            block,
            dictionaries,
            segment::BlockLoad {
                through_column: Some(self.schema.timestamp_index),
                columns: None,
            },
        )?;
        let nrows = block_row_count(&payload)?;
        require_header_row_count(block.row_count, nrows)?;
        let Some(span) = timestamp_column_span(&self.schema, &payload, from_ms, to_ms)? else {
            return Ok(());
        };
        if !span.inside {
            return self.absorb_decoded_stats(block, dictionaries, from_ms, to_ms, running, None);
        }
        let mut decode = Vec::new();
        for (stat, column) in running.iter_mut().zip(columns.iter()) {
            match column {
                ColumnMinMax::FromTimestamps => {
                    absorb_bounds(
                        stat,
                        Scalar::Timestamp(span.min),
                        Scalar::Timestamp(span.max),
                    )?;
                }
                ColumnMinMax::Empty => {}
                ColumnMinMax::Bounds { min, max } => {
                    absorb_bounds(stat, min.clone(), max.clone())?;
                }
                ColumnMinMax::Decode => decode.push(stat.spec.index),
            }
        }
        if decode.is_empty() {
            return Ok(());
        }
        self.absorb_decoded_stats(block, dictionaries, from_ms, to_ms, running, Some(&decode))
    }

    /// Decode stat columns and fold the rows whose timestamps fall inside the range.
    ///
    /// `only` limits the fold to columns the zone could not answer. `None` folds
    /// every running stat, which is the path a cut block takes.
    fn absorb_decoded_stats(
        &self,
        block: &segment::BlockMeta,
        dictionaries: &mut HashMap<u32, Option<Vec<u8>>>,
        from_ms: i64,
        to_ms: i64,
        running: &mut [RunningStat],
        only: Option<&[usize]>,
    ) -> Result<()> {
        if block_exceeds_read_budget(block.uncompressed_len, false, 0, true) {
            return Err(Error::event("query response size limit exceeded"));
        }
        let selected = |index: usize| only.is_none_or(|indexes| indexes.contains(&index));
        let mut through = self.schema.timestamp_index;
        for stat in running.iter() {
            if selected(stat.spec.index) {
                through = through.max(stat.spec.index);
            }
        }
        let payload = self.read_block_bytes(
            block,
            dictionaries,
            segment::BlockLoad {
                through_column: Some(through),
                columns: None,
            },
        )?;
        let nrows = block_row_count(&payload)?;
        require_header_row_count(block.row_count, nrows)?;
        fold_stat_payload(&self.schema, &payload, from_ms, to_ms, running, only)
    }

    /// One exact `sum` and no row predicate.
    ///
    /// The timestamp column decides whether the block is fully inside the
    /// range. A constant, stride, or stride pieces then update the accumulator.
    /// Anything else uses the row visitor on this same block.
    fn absorb_exact_sum_block(
        &self,
        block: &segment::BlockMeta,
        dictionaries: &mut HashMap<u32, Option<Vec<u8>>>,
        from_ms: i64,
        to_ms: i64,
        running: &mut [RunningStat],
    ) -> Result<()> {
        if block_exceeds_read_budget(block.uncompressed_len, false, 0, true) {
            return Err(Error::event("query response size limit exceeded"));
        }
        let index = running[0].spec.index;
        let through = self.schema.timestamp_index.max(index);
        let payload = self.read_block_bytes(
            block,
            dictionaries,
            segment::BlockLoad {
                through_column: Some(through),
                columns: None,
            },
        )?;
        let nrows = block_row_count(&payload)?;
        require_header_row_count(block.row_count, nrows)?;
        let Some(span) = timestamp_column_span(&self.schema, &payload, from_ms, to_ms)? else {
            return Ok(());
        };
        if !span.inside {
            return fold_stat_payload(&self.schema, &payload, from_ms, to_ms, running, None);
        }
        let name = running[0].spec.name.clone();
        match try_accumulate_exact_sum(&self.schema, &payload, index, &name, &mut running[0].exact)?
        {
            Some(contributed) => {
                if contributed {
                    running[0].contributed = true;
                }
                Ok(())
            }
            None => fold_stat_payload(&self.schema, &payload, from_ms, to_ms, running, None),
        }
    }

    /// `Ok(None)` means this column is not an int, timestamp, or decimal.
    /// `Ok(Some(None))` is an empty match. A qualifying prefix that crosses the
    /// sample ceilings returns the same error the sample path would.
    ///
    /// A block whose timestamps are not all inside the range, or whose body is
    /// not a unit stride, keeps the runs already accepted. Those runs are
    /// expanded through [`push_sample`], this payload is visited, and only the
    /// blocks after it are read. The rank is the sample rank.
    fn quantile_from_unit_runs(
        &self,
        from_ms: i64,
        to_ms: i64,
        blocks: &[segment::BlockMeta],
        spec: &ResolvedQuantile,
        p: QuantileP,
    ) -> Result<Option<Option<Scalar>>> {
        let width = match spec.ty {
            FieldType::Int | FieldType::Timestamp => std::mem::size_of::<i64>(),
            FieldType::Decimal { .. } => std::mem::size_of::<i128>(),
            _ => return Ok(None),
        };
        let mut runs = Vec::new();
        let mut n = 0usize;
        let mut dictionaries = HashMap::new();
        let through = self.schema.timestamp_index.max(spec.index);
        for (index, block) in blocks.iter().enumerate() {
            if block_exceeds_read_budget(block.uncompressed_len, false, 0, true) {
                return Err(Error::event("query response size limit exceeded"));
            }
            let payload = self.read_block_bytes(
                block,
                &mut dictionaries,
                segment::BlockLoad {
                    through_column: Some(through),
                    columns: None,
                },
            )?;
            let nrows = block_row_count(&payload)?;
            require_header_row_count(block.row_count, nrows)?;
            let Some(span) = timestamp_column_span(&self.schema, &payload, from_ms, to_ms)? else {
                continue;
            };
            if span.inside {
                if let Some(block_runs) = try_unit_stride_runs(&self.schema, &payload, spec.index)?
                {
                    for run in block_runs {
                        let rows = unit_run_rows(&run)?;
                        n = n
                            .checked_add(usize::try_from(rows).unwrap_or(usize::MAX))
                            .ok_or_else(|| Error::event("query row limit exceeded"))?;
                        if n > MAX_QUERY_ROWS {
                            return Err(Error::event("query row limit exceeded"));
                        }
                        if n > 0 {
                            retain_sample_bytes(n - 1, width)?;
                        }
                        runs.push(run);
                    }
                    continue;
                }
            }
            return self.quantile_samples_after_runs(
                from_ms,
                to_ms,
                spec,
                p,
                &runs,
                &payload,
                &blocks[index + 1..],
                &mut dictionaries,
                through,
            );
        }
        let Some(rank) = quantile_rank(p, n) else {
            return Ok(Some(None));
        };
        let value = select_unit_rank(&runs, rank as u128)?;
        Ok(Some(Some(unit_scalar(&spec.ty, value)?)))
    }

    /// Expand qualified unit runs, visit the declining payload, then read the rest.
    fn quantile_samples_after_runs(
        &self,
        from_ms: i64,
        to_ms: i64,
        spec: &ResolvedQuantile,
        p: QuantileP,
        runs: &[UnitRun],
        payload: &[u8],
        rest: &[segment::BlockMeta],
        dictionaries: &mut HashMap<u32, Option<Vec<u8>>>,
        through: usize,
    ) -> Result<Option<Option<Scalar>>> {
        let mut samples = QuantileSamples::new(spec.ty);
        expand_unit_runs(&mut samples, &spec.ty, runs)?;
        push_quantile_payload(&self.schema, &mut samples, spec, payload, from_ms, to_ms)?;
        for block in rest {
            if block_exceeds_read_budget(block.uncompressed_len, false, 0, true) {
                return Err(Error::event("query response size limit exceeded"));
            }
            let payload = self.read_block_bytes(
                block,
                dictionaries,
                segment::BlockLoad {
                    through_column: Some(through),
                    columns: None,
                },
            )?;
            let nrows = block_row_count(&payload)?;
            require_header_row_count(block.row_count, nrows)?;
            push_quantile_payload(&self.schema, &mut samples, spec, &payload, from_ms, to_ms)?;
        }
        Ok(Some(samples.finish(p)?))
    }

    fn catalog(&self) -> std::sync::MutexGuard<'_, Catalog> {
        self.pipeline
            .catalog
            .lock()
            .unwrap_or_else(|err| err.into_inner())
    }
}

/// Data and index bytes, using the same split as [`Stats`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StoredBytes {
    pub stored_data_bytes: u64,
    pub stored_index_bytes: u64,
}

/// Sizes before and after [`Store::rewrite_directory`].
///
/// `data_bytes_change` and `stored_bytes_change` are
/// `(source − destination) / source`. A zero source total is `0.0`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RewriteReport {
    pub source: StoredBytes,
    pub destination: StoredBytes,
    pub data_bytes_change: f64,
    pub stored_bytes_change: f64,
}

impl Drop for Store {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

/// How [`Store::count_with_filter`] reads one block that survived the zone map.
enum CountPlan {
    /// Decode the timestamp and every predicate column.
    Rows,
    /// No equality, and the zone implies every range. Timestamps only.
    ProvedRanges,
    /// One string, text, or bool equality, and the zone implies every range.
    ProvedHeader { column: usize },
}

/// Bytes already written, including the opening `[`. A further row needs at
/// least one payload byte plus the closing `]`, so a full buffer stops the
/// next block before it is read or decoded.
struct ResolvedScan {
    predicates: Vec<ColumnPredicate>,
    ranges: Vec<RangeFilter>,
    prefixes: Vec<PrefixFilter>,
    impossible: bool,
}

impl ResolvedScan {
    fn unfiltered(&self) -> bool {
        self.predicates.is_empty() && self.ranges.is_empty() && self.prefixes.is_empty()
    }
}

#[derive(Clone)]
struct ResolvedStat {
    index: usize,
    name: String,
    ty: FieldType,
    sum: bool,
    min: bool,
    max: bool,
    var: bool,
    stddev: bool,
}

impl ResolvedStat {
    fn needs_moment(&self) -> bool {
        self.var || self.stddev
    }
}

struct ResolvedGroup {
    index: usize,
    name: String,
    ty: FieldType,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
enum GroupKey {
    Null,
    Bool(bool),
    Int(i64),
    Timestamp(i64),
    Decimal(i128),
    Str(String),
}

impl GroupKey {
    fn scalar(&self) -> Scalar {
        match self {
            GroupKey::Null => Scalar::Null,
            GroupKey::Bool(value) => Scalar::Bool(*value),
            GroupKey::Int(value) => Scalar::Int(*value),
            GroupKey::Timestamp(value) => Scalar::Timestamp(*value),
            GroupKey::Decimal(value) => Scalar::Decimal(*value),
            GroupKey::Str(value) => Scalar::Str(value.clone()),
        }
    }
}

struct LatestHit {
    ts: i64,
    json: Vec<u8>,
}

struct RunningStat {
    spec: ResolvedStat,
    exact: i128,
    exact_sq: i128,
    float_sum: f64,
    float_sq: f64,
    /// Non-null values folded into `var` and `stddev`.
    n: u64,
    contributed: bool,
    min: Option<Scalar>,
    max: Option<Scalar>,
}

impl RunningStat {
    fn new(spec: ResolvedStat) -> Self {
        Self {
            spec,
            exact: 0,
            exact_sq: 0,
            float_sum: 0.0,
            float_sq: 0.0,
            n: 0,
            contributed: false,
            min: None,
            max: None,
        }
    }

    fn absorb(&mut self, value: Scalar) -> Result<()> {
        let numeric = match (self.spec.ty, &value) {
            (_, Scalar::Null) => return Ok(()),
            (FieldType::Int, Scalar::Int(value)) => Scalar::Int(*value),
            (FieldType::Timestamp, Scalar::Timestamp(value)) => Scalar::Timestamp(*value),
            (FieldType::Decimal { .. }, Scalar::Decimal(value)) => Scalar::Decimal(*value),
            (FieldType::Float, Scalar::Float(value)) => {
                if !value.is_finite() {
                    return Err(Error::corrupt(format!(
                        "field `{}` is not a finite float",
                        self.spec.name
                    )));
                }
                Scalar::Float(*value)
            }
            _ => {
                return Err(Error::corrupt(format!(
                    "field `{}` has a value that does not match its type",
                    self.spec.name
                )))
            }
        };
        let moment = self.spec.needs_moment();
        if self.spec.sum || moment {
            match numeric {
                Scalar::Float(value) => {
                    self.float_sum += value;
                    if moment {
                        self.float_sq += value * value;
                    }
                }
                Scalar::Int(value) => self.absorb_integer(i128::from(value), moment)?,
                Scalar::Timestamp(value) => self.absorb_integer(i128::from(value), moment)?,
                Scalar::Decimal(value) => self.absorb_integer(value, moment)?,
                _ => {
                    return Err(Error::corrupt(format!(
                        "field `{}` has a value that does not match its type",
                        self.spec.name
                    )))
                }
            }
        }
        if moment {
            self.n = self
                .n
                .checked_add(1)
                .ok_or_else(|| variance_overflow(&self.spec.name))?;
        }
        self.contributed = true;
        if self.spec.min {
            self.min = Some(tighter_extreme(self.min.take(), numeric.clone(), true)?);
        }
        if self.spec.max {
            self.max = Some(tighter_extreme(self.max.take(), numeric, false)?);
        }
        Ok(())
    }

    fn absorb_integer(&mut self, value: i128, moment: bool) -> Result<()> {
        self.add_exact(value)?;
        if moment {
            let square = value
                .checked_mul(value)
                .ok_or_else(|| variance_overflow(&self.spec.name))?;
            self.exact_sq = self
                .exact_sq
                .checked_add(square)
                .ok_or_else(|| variance_overflow(&self.spec.name))?;
        }
        Ok(())
    }

    fn add_exact(&mut self, value: i128) -> Result<()> {
        self.exact = self.exact.checked_add(value).ok_or_else(|| {
            if self.spec.sum {
                Error::event(format!("sum of `{}` overflowed", self.spec.name))
            } else {
                variance_overflow(&self.spec.name)
            }
        })?;
        Ok(())
    }

    fn finish(self) -> Result<ColumnStats> {
        let (var, stddev) = self.moments()?;
        let sum = if self.spec.sum {
            Some(match self.spec.ty {
                FieldType::Float => {
                    if !self.float_sum.is_finite() {
                        return Err(Error::event(format!(
                            "sum of `{}` overflowed",
                            self.spec.name
                        )));
                    }
                    StatSum::Float {
                        total: self.float_sum,
                        contributed: self.contributed,
                    }
                }
                _ => StatSum::Exact {
                    total: self.exact,
                    contributed: self.contributed,
                },
            })
        } else {
            None
        };
        Ok(ColumnStats {
            field: self.spec.name,
            sum,
            min: self.spec.min.then_some(self.min),
            max: self.spec.max.then_some(self.max),
            var,
            stddev,
        })
    }

    fn moments(&self) -> Result<(Option<Option<StatVar>>, Option<Option<f64>>)> {
        if !self.spec.needs_moment() {
            return Ok((None, None));
        }
        if self.n == 0 {
            return Ok((
                self.spec.var.then_some(None),
                self.spec.stddev.then_some(None),
            ));
        }
        let (var_value, dev) = match self.spec.ty {
            FieldType::Float => {
                let n = self.n as f64;
                let mean = self.float_sum / n;
                let var = (self.float_sq / n) - (mean * mean);
                if !var.is_finite() {
                    return Err(variance_overflow(&self.spec.name));
                }
                // Cancellation can leave a finite negative moment. That number is
                // the variance. The square root runs only when stddev was
                // requested, and a negative radicand fails the whole response.
                let dev = if self.spec.stddev {
                    if var < 0.0 {
                        return Err(variance_negative(&self.spec.name));
                    }
                    let dev = var.sqrt();
                    if !dev.is_finite() {
                        return Err(variance_overflow(&self.spec.name));
                    }
                    dev
                } else {
                    0.0
                };
                (StatVar::Float(var), dev)
            }
            FieldType::Int | FieldType::Timestamp | FieldType::Decimal { .. } => {
                let mut reduced = exact_population_var(self.n, self.exact, self.exact_sq)
                    .map_err(|_| variance_overflow(&self.spec.name))?;
                if let FieldType::Decimal { scale } = self.spec.ty {
                    reduced = scale_decimal_var(reduced, scale);
                }
                let dev = if self.spec.stddev {
                    let dev = reduced.stddev();
                    if !dev.is_finite() {
                        return Err(variance_overflow(&self.spec.name));
                    }
                    dev
                } else {
                    0.0
                };
                (StatVar::Exact(reduced.format()), dev)
            }
            _ => {
                return Err(Error::schema(format!(
                    "stat does not apply to {} column `{}`",
                    self.spec.ty.name(),
                    self.spec.name
                )))
            }
        };
        Ok((
            self.spec.var.then_some(Some(var_value)),
            self.spec.stddev.then_some(Some(dev)),
        ))
    }
}

fn variance_overflow(name: &str) -> Error {
    Error::event(format!("variance of `{name}` overflowed"))
}

fn variance_negative(name: &str) -> Error {
    Error::event(format!(
        "variance of `{name}` is negative; the computed variance is negative"
    ))
}

/// Per-column answer copied out of the zone before the payload is read.
enum ColumnMinMax {
    /// Primary timestamp. The payload timestamps supply both extremes.
    FromTimestamps,
    /// The zone column is all nulls.
    Empty,
    Bounds {
        min: Scalar,
        max: Scalar,
    },
    /// Tag `0`, a non-finite float, a type mismatch, or a missing zone.
    Decode,
}

/// A zone column that can answer `min` and `max` without reading the payload.
enum ZoneMinMax {
    /// Every value is null, so the column does not move either extreme.
    Empty,
    Bounds {
        min: Scalar,
        max: Scalar,
    },
}

/// Fold rows already loaded. `only` limits the fold the way
/// [`Store::absorb_decoded_stats`] does after it reads the block.
fn fold_stat_payload(
    schema: &Schema,
    payload: &[u8],
    from_ms: i64,
    to_ms: i64,
    running: &mut [RunningStat],
    only: Option<&[usize]>,
) -> Result<()> {
    let selected = |index: usize| only.is_none_or(|indexes| indexes.contains(&index));
    let mut captures = capture_columns(
        running
            .iter()
            .map(|stat| stat.spec.index)
            .filter(|index| selected(*index)),
    );
    visit_matching_timestamps(
        schema,
        payload,
        from_ms,
        to_ms,
        &[],
        &[],
        &mut captures,
        |ts, row, captured| {
            for stat in running.iter_mut() {
                if !selected(stat.spec.index) {
                    continue;
                }
                let value = stat_value(schema, stat, ts, row, captured)?;
                stat.absorb(value)?;
            }
            Ok(())
        },
    )
}

/// One int, timestamp, or decimal `sum`, with no `min`, `max`, or variance.
fn exact_sum_request(running: &[RunningStat]) -> bool {
    let [stat] = running else {
        return false;
    };
    stat.spec.sum
        && !stat.spec.min
        && !stat.spec.max
        && !stat.spec.needs_moment()
        && matches!(
            stat.spec.ty,
            FieldType::Int | FieldType::Timestamp | FieldType::Decimal { .. }
        )
}

/// Int, decimal, and finite float extremes. Anything else, including tag `0`
/// and a non-finite float, is `None` and the column is decoded.
fn zone_minmax(ty: FieldType, column: &crate::zone::ColumnZone) -> Option<ZoneMinMax> {
    match (ty, column) {
        (_, crate::zone::ColumnZone::AllNull) => Some(ZoneMinMax::Empty),
        (FieldType::Int, crate::zone::ColumnZone::I64 { min, max, .. }) => {
            Some(ZoneMinMax::Bounds {
                min: Scalar::Int(*min),
                max: Scalar::Int(*max),
            })
        }
        (FieldType::Timestamp, crate::zone::ColumnZone::I64 { min, max, .. }) => {
            Some(ZoneMinMax::Bounds {
                min: Scalar::Timestamp(*min),
                max: Scalar::Timestamp(*max),
            })
        }
        (FieldType::Decimal { .. }, crate::zone::ColumnZone::I128 { min, max, .. }) => {
            Some(ZoneMinMax::Bounds {
                min: Scalar::Decimal(*min),
                max: Scalar::Decimal(*max),
            })
        }
        (FieldType::Float, crate::zone::ColumnZone::F64 { min, max, .. })
            if min.is_finite() && max.is_finite() =>
        {
            Some(ZoneMinMax::Bounds {
                min: Scalar::Float(*min),
                max: Scalar::Float(*max),
            })
        }
        _ => None,
    }
}

fn absorb_bounds(stat: &mut RunningStat, min: Scalar, max: Scalar) -> Result<()> {
    if stat.spec.min {
        stat.absorb(min)?;
    }
    if stat.spec.max {
        stat.absorb(max)?;
    }
    Ok(())
}

fn tighter_extreme(current: Option<Scalar>, next: Scalar, want_min: bool) -> Result<Scalar> {
    let Some(current) = current else {
        return Ok(next);
    };
    match cmp_numeric(&next, &current) {
        Some(Ordering::Less) if want_min => Ok(next),
        Some(Ordering::Greater) if !want_min => Ok(next),
        Some(_) => Ok(current),
        None => Err(Error::corrupt("stat values cannot be compared")),
    }
}

fn stat_value(
    schema: &Schema,
    stat: &RunningStat,
    ts: i64,
    row: usize,
    captures: &[CapturedColumn],
) -> Result<Scalar> {
    if let Some(slot) = captures
        .iter()
        .find(|column| column.index == stat.spec.index)
    {
        if !slot.values.is_empty() {
            return slot.values.get(row).cloned().ok_or_else(|| {
                Error::corrupt(format!("field `{}` is missing a row", stat.spec.name))
            });
        }
    }
    if stat.spec.index == schema.timestamp_index {
        return Ok(Scalar::Timestamp(ts));
    }
    Err(Error::corrupt(format!(
        "field `{}` was not decoded",
        stat.spec.name
    )))
}

fn resolve_stat_requests(schema: &Schema, stats: &[StatRequest]) -> Result<Vec<ResolvedStat>> {
    if stats.is_empty() || stats.iter().all(|stat| !stat_requested(stat)) {
        return Err(Error::event(
            "at least one of sum, min, max, var, or stddev is required",
        ));
    }
    let mut resolved: Vec<ResolvedStat> = Vec::new();
    for stat in stats {
        if stat.field.is_empty() {
            return Err(Error::event("stat field name is empty"));
        }
        if !stat_requested(stat) {
            continue;
        }
        let index = schema
            .fields
            .iter()
            .position(|field| field.name == stat.field)
            .ok_or_else(|| Error::schema(format!("unknown stat field `{}`", stat.field)))?;
        let ty = schema.fields[index].ty;
        if !matches!(
            ty,
            FieldType::Int | FieldType::Timestamp | FieldType::Float | FieldType::Decimal { .. }
        ) {
            return Err(Error::schema(format!(
                "stat does not apply to {} column `{}`",
                ty.name(),
                stat.field
            )));
        }
        if let Some(existing) = resolved.iter_mut().find(|column| column.index == index) {
            existing.sum |= stat.sum;
            existing.min |= stat.min;
            existing.max |= stat.max;
            existing.var |= stat.var;
            existing.stddev |= stat.stddev;
        } else {
            resolved.push(ResolvedStat {
                index,
                name: stat.field.clone(),
                ty,
                sum: stat.sum,
                min: stat.min,
                max: stat.max,
                var: stat.var,
                stddev: stat.stddev,
            });
        }
    }
    if resolved.is_empty() {
        return Err(Error::event(
            "at least one of sum, min, max, var, or stddev is required",
        ));
    }
    Ok(resolved)
}

fn stat_requested(stat: &StatRequest) -> bool {
    stat.sum || stat.min || stat.max || stat.var || stat.stddev
}

struct DistinctColumn {
    index: usize,
    name: String,
    ty: FieldType,
}

/// Parse `p` as a decimal in `0..=1`.
///
/// The text is digits with one optional `.`. An exponent, a sign, or a value
/// outside that range is an error. `1` and `1.0` are the same probability.
/// Trailing zeros in the fraction do not change the rank.
pub fn parse_quantile_p(text: &str) -> Result<QuantileP> {
    parse_quantile_level(text).ok_or_else(|| Error::event("`p` must be a decimal in 0..=1"))
}

fn parse_quantile_level(text: &str) -> Option<QuantileP> {
    if text.is_empty() || text == "." {
        return None;
    }
    if !text
        .bytes()
        .all(|byte| byte.is_ascii_digit() || byte == b'.')
    {
        return None;
    }
    let mut parts = text.split('.');
    let whole = parts.next().unwrap_or("");
    let frac = parts.next();
    if parts.next().is_some() {
        return None;
    }
    let whole_value = integer_digit(whole)?;
    let frac = frac.unwrap_or("");
    if !frac.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    if whole_value == 1 {
        if frac.bytes().all(|byte| byte == b'0') {
            return Some(QuantileP { numer: 1, scale: 0 });
        }
        return None;
    }
    let frac = frac.trim_end_matches('0');
    if frac.is_empty() {
        return Some(QuantileP { numer: 0, scale: 0 });
    }
    if frac.len() > 38 {
        return None;
    }
    let numer = frac.parse::<u128>().ok()?;
    Some(QuantileP {
        numer,
        scale: frac.len() as u8,
    })
}

fn integer_digit(whole: &str) -> Option<u8> {
    if whole.is_empty() {
        return Some(0);
    }
    if !whole.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    let trimmed = whole.trim_start_matches('0');
    if trimmed.is_empty() {
        Some(0)
    } else if trimmed == "1" {
        Some(1)
    } else {
        None
    }
}

/// Int, timestamp, or decimal, and no row predicate.
fn unit_quantile(spec: &ResolvedQuantile, resolved: &ResolvedScan) -> bool {
    resolved.unfiltered()
        && matches!(
            spec.ty,
            FieldType::Int | FieldType::Timestamp | FieldType::Decimal { .. }
        )
}

fn unit_run_rows(run: &UnitRun) -> Result<u64> {
    if run.mult == 0 || run.start > run.end {
        return Err(Error::corrupt("quantile stride is empty"));
    }
    let width = ordered_i128(run.end) - ordered_i128(run.start) + 1;
    let rows = width
        .checked_mul(u128::from(run.mult))
        .ok_or_else(|| Error::corrupt("quantile stride is too wide"))?;
    u64::try_from(rows).map_err(|_| Error::corrupt("quantile stride is too wide"))
}

fn ordered_i128(value: i128) -> u128 {
    (value as u128) ^ (1u128 << 127)
}

fn from_ordered_i128(value: u128) -> i128 {
    (value ^ (1u128 << 127)) as i128
}

/// Index `floor(p * n)` across inclusive unit-stride intervals.
///
/// A difference sweep adds each run's multiplicity at its first integer and
/// removes it just after the last. The rank is the integer where the running
/// count crosses `rank`.
fn select_unit_rank(runs: &[UnitRun], rank: u128) -> Result<i128> {
    let mut events = Vec::with_capacity(runs.len().saturating_mul(2));
    for run in runs {
        if run.mult == 0 || run.start > run.end {
            return Err(Error::corrupt("quantile stride is empty"));
        }
        events.push((run.start, i128::from(run.mult)));
        if run.end < i128::MAX {
            let after = run
                .end
                .checked_add(1)
                .ok_or_else(|| Error::corrupt("quantile stride overflow"))?;
            events.push((after, -i128::from(run.mult)));
        }
    }
    events.sort_unstable_by_key(|event| event.0);
    let mut active = 0i128;
    let mut seen = 0u128;
    let mut index = 0usize;
    while index < events.len() {
        let pos = events[index].0;
        while index < events.len() && events[index].0 == pos {
            active += events[index].1;
            index += 1;
        }
        let next = events.get(index).map(|event| event.0);
        if let Some(value) = cover_unit_rank(pos, next, active, &mut seen, rank)? {
            return Ok(value);
        }
    }
    Err(Error::corrupt("quantile rank is outside the stride runs"))
}

fn cover_unit_rank(
    pos: i128,
    next: Option<i128>,
    active: i128,
    seen: &mut u128,
    rank: u128,
) -> Result<Option<i128>> {
    if active == 0 {
        return Ok(None);
    }
    if active < 0 {
        return Err(Error::corrupt("quantile stride multiplicity is negative"));
    }
    let width = match next {
        Some(next) => {
            if next <= pos {
                return Err(Error::corrupt("quantile stride is not ordered"));
            }
            ordered_i128(next) - ordered_i128(pos)
        }
        None => ordered_i128(i128::MAX) - ordered_i128(pos) + 1,
    };
    if width == 0 {
        return Ok(None);
    }
    let mult = active as u128;
    let count = width
        .checked_mul(mult)
        .ok_or_else(|| Error::corrupt("quantile stride is too wide"))?;
    let end_seen = seen
        .checked_add(count)
        .ok_or_else(|| Error::corrupt("quantile stride is too wide"))?;
    if end_seen > rank {
        let offset = (rank - *seen) / mult;
        let at = ordered_i128(pos)
            .checked_add(offset)
            .ok_or_else(|| Error::corrupt("quantile stride is too wide"))?;
        return Ok(Some(from_ordered_i128(at)));
    }
    *seen = end_seen;
    Ok(None)
}

fn expand_unit_runs(samples: &mut QuantileSamples, ty: &FieldType, runs: &[UnitRun]) -> Result<()> {
    for run in runs {
        if run.mult == 0 || run.start > run.end {
            return Err(Error::corrupt("quantile stride is empty"));
        }
        let mut value = run.start;
        loop {
            let scalar = unit_scalar(ty, value)?;
            for _ in 0..run.mult {
                samples.push(scalar.clone())?;
            }
            if value == run.end {
                break;
            }
            value = value
                .checked_add(1)
                .ok_or_else(|| Error::corrupt("quantile stride overflow"))?;
        }
    }
    Ok(())
}

fn push_quantile_payload(
    schema: &Schema,
    samples: &mut QuantileSamples,
    spec: &ResolvedQuantile,
    payload: &[u8],
    from_ms: i64,
    to_ms: i64,
) -> Result<()> {
    let mut captures = capture_columns(std::iter::once(spec.index));
    visit_matching_timestamps(
        schema,
        payload,
        from_ms,
        to_ms,
        &[],
        &[],
        &mut captures,
        |ts, row, captured| {
            let value = quantile_value(schema, spec, ts, row, captured)?;
            samples.push(value)
        },
    )
}

fn unit_scalar(ty: &FieldType, value: i128) -> Result<Scalar> {
    match ty {
        FieldType::Int => i64::try_from(value)
            .map(Scalar::Int)
            .map_err(|_| Error::corrupt("quantile stride does not fit an int")),
        FieldType::Timestamp => i64::try_from(value)
            .map(Scalar::Timestamp)
            .map_err(|_| Error::corrupt("quantile stride does not fit a timestamp")),
        FieldType::Decimal { .. } => Ok(Scalar::Decimal(value)),
        _ => Err(Error::corrupt("quantile stride does not match its column")),
    }
}

/// Index `floor(p * n)` for `p < 1`, and `n - 1` when `p` is 1.
/// `n == 0` has no rank.
fn quantile_rank(p: QuantileP, n: usize) -> Option<usize> {
    if n == 0 {
        return None;
    }
    if p.numer == 1 && p.scale == 0 {
        return Some(n - 1);
    }
    if p.numer == 0 {
        return Some(0);
    }
    let den = ten_pow(p.scale);
    Some(floor_rank(p.numer, n as u128, den))
}

const fn ten_pow(scale: u8) -> u128 {
    let mut value = 1u128;
    let mut left = scale;
    while left > 0 {
        value *= 10;
        left -= 1;
    }
    value
}

/// Largest `k < n` such that `k * den <= numer * n`, with `numer < den`.
fn floor_rank(numer: u128, n: u128, den: u128) -> usize {
    let mut lo = 0u128;
    let mut hi = n;
    while lo + 1 < hi {
        let mid = lo + (hi - lo) / 2;
        if mul_le(mid, den, numer, n) {
            lo = mid;
        } else {
            hi = mid;
        }
    }
    lo as usize
}

fn mul_le(left_a: u128, left_b: u128, right_a: u128, right_b: u128) -> bool {
    cmp_limbs(mul_limbs(left_a, left_b), mul_limbs(right_a, right_b)) != Ordering::Greater
}

fn cmp_limbs(left: [u64; 4], right: [u64; 4]) -> Ordering {
    for index in (0..4).rev() {
        match left[index].cmp(&right[index]) {
            Ordering::Equal => {}
            other => return other,
        }
    }
    Ordering::Equal
}

/// Little-endian 64-bit limbs of `a * b`.
fn mul_limbs(a: u128, b: u128) -> [u64; 4] {
    let a0 = a as u64 as u128;
    let a1 = a >> 64;
    let b0 = b as u64 as u128;
    let b1 = b >> 64;
    let p00 = a0 * b0;
    let p01 = a0 * b1;
    let p10 = a1 * b0;
    let p11 = a1 * b1;
    let l0 = p00 as u64;
    let sum1 = (p00 >> 64) + (p01 as u64) as u128 + (p10 as u64) as u128;
    let l1 = sum1 as u64;
    let sum2 = (sum1 >> 64) + (p01 >> 64) + (p10 >> 64) + (p11 as u64) as u128;
    let l2 = sum2 as u64;
    let l3 = ((sum2 >> 64) + (p11 >> 64)) as u64;
    [l0, l1, l2, l3]
}

struct ResolvedQuantile {
    index: usize,
    name: String,
    ty: FieldType,
}

enum DistinctSet {
    Bool(HashSet<bool>),
    Int(HashSet<i64>),
    Timestamp(HashSet<i64>),
    Decimal(HashSet<i128>),
    Float(HashSet<u64>),
    Str(HashSet<String>, usize),
}

impl DistinctSet {
    fn new(ty: FieldType) -> Self {
        match ty {
            FieldType::Bool => DistinctSet::Bool(HashSet::new()),
            FieldType::Int => DistinctSet::Int(HashSet::new()),
            FieldType::Timestamp => DistinctSet::Timestamp(HashSet::new()),
            FieldType::Decimal { .. } => DistinctSet::Decimal(HashSet::new()),
            FieldType::Float => DistinctSet::Float(HashSet::new()),
            FieldType::String | FieldType::Text => DistinctSet::Str(HashSet::new(), 0),
            FieldType::Json => DistinctSet::Str(HashSet::new(), 0),
        }
    }

    fn len(&self) -> usize {
        match self {
            DistinctSet::Bool(values) => values.len(),
            DistinctSet::Int(values) => values.len(),
            DistinctSet::Timestamp(values) => values.len(),
            DistinctSet::Decimal(values) => values.len(),
            DistinctSet::Float(values) => values.len(),
            DistinctSet::Str(values, _) => values.len(),
        }
    }

    fn note(&mut self, name: &str, ty: FieldType, scalar: &Scalar) -> Result<()> {
        match (ty, scalar) {
            (_, Scalar::Null) => Ok(()),
            (FieldType::Bool, Scalar::Bool(value)) => self.insert_bool(*value),
            (FieldType::Int, Scalar::Int(value)) => self.insert_int(*value),
            (FieldType::Timestamp, Scalar::Timestamp(value)) => self.insert_timestamp(*value),
            (FieldType::Decimal { .. }, Scalar::Decimal(value)) => self.insert_decimal(*value),
            (FieldType::Float, Scalar::Float(value)) => {
                if !value.is_finite() {
                    return Err(Error::corrupt(format!(
                        "field `{name}` is not a finite float"
                    )));
                }
                self.insert_float(value.to_bits())
            }
            (FieldType::String | FieldType::Text, Scalar::Str(value)) => self.insert_str(value),
            _ => Err(Error::corrupt(format!(
                "field `{name}` has a value that does not match its type"
            ))),
        }
    }

    fn insert_bool(&mut self, value: bool) -> Result<()> {
        let DistinctSet::Bool(values) = self else {
            return Err(Error::corrupt("distinct set does not match the column"));
        };
        insert_new(values, value)
    }

    fn insert_int(&mut self, value: i64) -> Result<()> {
        let DistinctSet::Int(values) = self else {
            return Err(Error::corrupt("distinct set does not match the column"));
        };
        insert_new(values, value)
    }

    fn insert_timestamp(&mut self, value: i64) -> Result<()> {
        let DistinctSet::Timestamp(values) = self else {
            return Err(Error::corrupt("distinct set does not match the column"));
        };
        insert_new(values, value)
    }

    fn insert_decimal(&mut self, value: i128) -> Result<()> {
        let DistinctSet::Decimal(values) = self else {
            return Err(Error::corrupt("distinct set does not match the column"));
        };
        insert_new(values, value)
    }

    fn insert_float(&mut self, bits: u64) -> Result<()> {
        let DistinctSet::Float(values) = self else {
            return Err(Error::corrupt("distinct set does not match the column"));
        };
        insert_new(values, bits)
    }

    fn insert_i128(&mut self, value: i128) -> Result<()> {
        match self {
            DistinctSet::Int(_) => {
                let value = i64::try_from(value)
                    .map_err(|_| Error::corrupt("distinct integer does not fit in i64"))?;
                self.insert_int(value)
            }
            DistinctSet::Timestamp(_) => {
                let value = i64::try_from(value)
                    .map_err(|_| Error::corrupt("distinct timestamp does not fit in i64"))?;
                self.insert_timestamp(value)
            }
            DistinctSet::Decimal(_) => self.insert_decimal(value),
            _ => Err(Error::corrupt("distinct set does not match the column")),
        }
    }

    fn insert_str(&mut self, value: &str) -> Result<()> {
        let DistinctSet::Str(values, bytes) = self else {
            return Err(Error::corrupt("distinct set does not match the column"));
        };
        if values.contains(value) {
            return Ok(());
        }
        if values.len() >= MAX_DISTINCT_VALUES {
            return Err(Error::event("distinct value limit exceeded"));
        }
        let next = bytes.saturating_add(value.len());
        if next > MAX_QUERY_BYTES {
            return Err(Error::event("query response size limit exceeded"));
        }
        values.insert(value.to_owned());
        *bytes = next;
        Ok(())
    }
}

fn spill_distinct_runs(runs: &[(i128, i128)], seen: &mut DistinctSet) -> Result<()> {
    for (lo, hi) in merge_distinct_runs(runs) {
        let len = inclusive_run_len(lo, hi)
            .ok_or_else(|| Error::event("distinct value limit exceeded"))?;
        if seen.len() as u128 + len > MAX_DISTINCT_VALUES as u128 {
            return Err(Error::event("distinct value limit exceeded"));
        }
        let mut value = lo;
        loop {
            seen.insert_i128(value)?;
            if value == hi {
                break;
            }
            value = value
                .checked_add(1)
                .ok_or_else(|| Error::event("distinct value limit exceeded"))?;
        }
    }
    Ok(())
}

fn union_distinct_count(runs: &[(i128, i128)]) -> Result<u64> {
    let mut total = 0u128;
    for (lo, hi) in merge_distinct_runs(runs) {
        let len = inclusive_run_len(lo, hi)
            .ok_or_else(|| Error::event("distinct value limit exceeded"))?;
        total = total.saturating_add(len);
        if total > MAX_DISTINCT_VALUES as u128 {
            return Err(Error::event("distinct value limit exceeded"));
        }
    }
    Ok(total as u64)
}

fn merge_distinct_runs(runs: &[(i128, i128)]) -> Vec<(i128, i128)> {
    let mut ordered: Vec<(i128, i128)> = runs
        .iter()
        .copied()
        .map(|(lo, hi)| if lo <= hi { (lo, hi) } else { (hi, lo) })
        .collect();
    ordered.sort_unstable();
    let mut merged = Vec::new();
    let Some((mut cur_lo, mut cur_hi)) = ordered.first().copied() else {
        return merged;
    };
    for (lo, hi) in ordered.into_iter().skip(1) {
        let touches = match cur_hi.checked_add(1) {
            Some(next) => lo <= next,
            None => true,
        };
        if touches {
            if hi > cur_hi {
                cur_hi = hi;
            }
        } else {
            merged.push((cur_lo, cur_hi));
            cur_lo = lo;
            cur_hi = hi;
        }
    }
    merged.push((cur_lo, cur_hi));
    merged
}

fn inclusive_run_len(lo: i128, hi: i128) -> Option<u128> {
    let width = (hi as u128).wrapping_sub(lo as u128);
    width.checked_add(1)
}

fn insert_new<T: Eq + std::hash::Hash>(values: &mut HashSet<T>, value: T) -> Result<()> {
    if values.contains(&value) {
        return Ok(());
    }
    if values.len() >= MAX_DISTINCT_VALUES {
        return Err(Error::event("distinct value limit exceeded"));
    }
    values.insert(value);
    Ok(())
}

struct TopTally {
    kind: TopKind,
    nulls: u64,
}

enum TopKind {
    Bool([u64; 2]),
    Int(HashMap<i64, u64>),
    Timestamp(HashMap<i64, u64>),
    Decimal(HashMap<i128, u64>),
    Float(HashMap<u64, u64>),
    Str(HashMap<String, u64>, usize),
}

impl TopTally {
    fn new(ty: FieldType) -> Self {
        let kind = match ty {
            FieldType::Bool => TopKind::Bool([0, 0]),
            FieldType::Int => TopKind::Int(HashMap::new()),
            FieldType::Timestamp => TopKind::Timestamp(HashMap::new()),
            FieldType::Decimal { .. } => TopKind::Decimal(HashMap::new()),
            FieldType::Float => TopKind::Float(HashMap::new()),
            FieldType::String | FieldType::Text | FieldType::Json => {
                TopKind::Str(HashMap::new(), 0)
            }
        };
        Self { kind, nulls: 0 }
    }

    fn note(&mut self, name: &str, ty: FieldType, scalar: &Scalar) -> Result<()> {
        match (ty, scalar) {
            (_, Scalar::Null) => self.note_null(),
            (FieldType::Bool, Scalar::Bool(value)) => self.note_bool(*value),
            (FieldType::Int, Scalar::Int(value)) => self.note_int(*value),
            (FieldType::Timestamp, Scalar::Timestamp(value)) => self.note_timestamp(*value),
            (FieldType::Decimal { .. }, Scalar::Decimal(value)) => self.note_decimal(*value),
            (FieldType::Float, Scalar::Float(value)) => {
                if !value.is_finite() {
                    return Err(Error::corrupt(format!(
                        "field `{name}` is not a finite float"
                    )));
                }
                self.note_float(value.to_bits())
            }
            (FieldType::String | FieldType::Text, Scalar::Str(value)) => self.note_str(value),
            _ => Err(Error::corrupt(format!(
                "field `{name}` has a value that does not match its type"
            ))),
        }
    }

    fn note_null(&mut self) -> Result<()> {
        if self.nulls > 0 {
            self.nulls = bump_count(self.nulls)?;
            return Ok(());
        }
        if self.occupied(0) >= MAX_DISTINCT_VALUES {
            return Err(Error::event("distinct value limit exceeded"));
        }
        self.nulls = 1;
        Ok(())
    }

    fn note_bool(&mut self, value: bool) -> Result<()> {
        let nulls = self.nulls;
        let TopKind::Bool(counts) = &mut self.kind else {
            return Err(Error::corrupt("topk tally does not match the column"));
        };
        let slot = usize::from(value);
        if counts[slot] > 0 {
            counts[slot] = bump_count(counts[slot])?;
            return Ok(());
        }
        let occupied =
            usize::from(counts[0] > 0) + usize::from(counts[1] > 0) + usize::from(nulls > 0);
        if occupied >= MAX_DISTINCT_VALUES {
            return Err(Error::event("distinct value limit exceeded"));
        }
        counts[slot] = 1;
        Ok(())
    }

    fn note_int(&mut self, value: i64) -> Result<()> {
        self.note_map(value, |kind| match kind {
            TopKind::Int(values) => Some(values),
            _ => None,
        })
    }

    fn note_timestamp(&mut self, value: i64) -> Result<()> {
        self.note_map(value, |kind| match kind {
            TopKind::Timestamp(values) => Some(values),
            _ => None,
        })
    }

    fn note_decimal(&mut self, value: i128) -> Result<()> {
        self.note_map(value, |kind| match kind {
            TopKind::Decimal(values) => Some(values),
            _ => None,
        })
    }

    fn note_float(&mut self, bits: u64) -> Result<()> {
        self.note_map(bits, |kind| match kind {
            TopKind::Float(values) => Some(values),
            _ => None,
        })
    }

    fn note_map<T, F>(&mut self, value: T, mut select: F) -> Result<()>
    where
        T: Eq + std::hash::Hash,
        F: FnMut(&mut TopKind) -> Option<&mut HashMap<T, u64>>,
    {
        let nulls = self.nulls;
        let Some(values) = select(&mut self.kind) else {
            return Err(Error::corrupt("topk tally does not match the column"));
        };
        if let Some(count) = values.get_mut(&value) {
            *count = bump_count(*count)?;
            return Ok(());
        }
        if values.len() + usize::from(nulls > 0) >= MAX_DISTINCT_VALUES {
            return Err(Error::event("distinct value limit exceeded"));
        }
        values.insert(value, 1);
        Ok(())
    }

    fn note_str(&mut self, value: &str) -> Result<()> {
        let nulls = self.nulls;
        let TopKind::Str(values, bytes) = &mut self.kind else {
            return Err(Error::corrupt("topk tally does not match the column"));
        };
        if let Some(count) = values.get_mut(value) {
            *count = bump_count(*count)?;
            return Ok(());
        }
        if values.len() + usize::from(nulls > 0) >= MAX_DISTINCT_VALUES {
            return Err(Error::event("distinct value limit exceeded"));
        }
        let next = bytes.saturating_add(value.len());
        if next > MAX_QUERY_BYTES {
            return Err(Error::event("query response size limit exceeded"));
        }
        values.insert(value.to_owned(), 1);
        *bytes = next;
        Ok(())
    }

    fn occupied(&self, extra: usize) -> usize {
        let values = match &self.kind {
            TopKind::Bool(counts) => usize::from(counts[0] > 0) + usize::from(counts[1] > 0),
            TopKind::Int(values) => values.len(),
            TopKind::Timestamp(values) => values.len(),
            TopKind::Decimal(values) => values.len(),
            TopKind::Float(values) => values.len(),
            TopKind::Str(values, _) => values.len(),
        };
        values + usize::from(self.nulls > 0) + extra
    }

    fn finish(self, k: usize) -> Vec<TopValue> {
        let mut hits = Vec::new();
        if self.nulls > 0 {
            hits.push(TopValue {
                value: Scalar::Null,
                count: self.nulls,
            });
        }
        match self.kind {
            TopKind::Bool(counts) => {
                if counts[0] > 0 {
                    hits.push(TopValue {
                        value: Scalar::Bool(false),
                        count: counts[0],
                    });
                }
                if counts[1] > 0 {
                    hits.push(TopValue {
                        value: Scalar::Bool(true),
                        count: counts[1],
                    });
                }
            }
            TopKind::Int(values) => {
                hits.extend(values.into_iter().map(|(value, count)| TopValue {
                    value: Scalar::Int(value),
                    count,
                }));
            }
            TopKind::Timestamp(values) => {
                hits.extend(values.into_iter().map(|(value, count)| TopValue {
                    value: Scalar::Timestamp(value),
                    count,
                }));
            }
            TopKind::Decimal(values) => {
                hits.extend(values.into_iter().map(|(value, count)| TopValue {
                    value: Scalar::Decimal(value),
                    count,
                }));
            }
            TopKind::Float(values) => {
                hits.extend(values.into_iter().map(|(bits, count)| TopValue {
                    value: Scalar::Float(f64::from_bits(bits)),
                    count,
                }));
            }
            TopKind::Str(values, _) => {
                hits.extend(values.into_iter().map(|(value, count)| TopValue {
                    value: Scalar::Str(value),
                    count,
                }));
            }
        }
        hits.sort_by(|left, right| {
            right
                .count
                .cmp(&left.count)
                .then_with(|| cmp_top_value(&left.value, &right.value))
        });
        hits.truncate(k);
        hits
    }
}

fn bump_count(count: u64) -> Result<u64> {
    count
        .checked_add(1)
        .ok_or_else(|| Error::event("event count overflow"))
}

/// Null first, then the same order as a grouped-stats key. Floats use total order.
fn cmp_top_value(left: &Scalar, right: &Scalar) -> Ordering {
    match (left, right) {
        (Scalar::Null, Scalar::Null) => Ordering::Equal,
        (Scalar::Null, _) => Ordering::Less,
        (_, Scalar::Null) => Ordering::Greater,
        (Scalar::Bool(left), Scalar::Bool(right)) => left.cmp(right),
        (Scalar::Int(left), Scalar::Int(right)) => left.cmp(right),
        (Scalar::Timestamp(left), Scalar::Timestamp(right)) => left.cmp(right),
        (Scalar::Decimal(left), Scalar::Decimal(right)) => left.cmp(right),
        (Scalar::Float(left), Scalar::Float(right)) => left.total_cmp(right),
        (Scalar::Str(left), Scalar::Str(right)) => left.cmp(right),
        _ => Ordering::Equal,
    }
}

enum ZoneDistinct {
    /// Non-null values to union once every payload timestamp is inside the range.
    Contained(Vec<Scalar>),
    Walk,
}

fn zone_distinct_column(ty: FieldType) -> bool {
    matches!(ty, FieldType::Bool | FieldType::String | FieldType::Text)
}

fn zone_distinct_values(ty: FieldType, column: &crate::zone::ColumnZone) -> ZoneDistinct {
    match (ty, column) {
        (FieldType::Bool, crate::zone::ColumnZone::AllNull) => ZoneDistinct::Contained(Vec::new()),
        (
            FieldType::Bool,
            crate::zone::ColumnZone::Bool {
                has_false,
                has_true,
                ..
            },
        ) => {
            let mut values = Vec::new();
            if *has_false {
                values.push(Scalar::Bool(false));
            }
            if *has_true {
                values.push(Scalar::Bool(true));
            }
            ZoneDistinct::Contained(values)
        }
        (FieldType::String | FieldType::Text, crate::zone::ColumnZone::AllNull) => {
            ZoneDistinct::Contained(Vec::new())
        }
        (FieldType::String | FieldType::Text, crate::zone::ColumnZone::Exact { values, .. }) => {
            ZoneDistinct::Contained(
                values
                    .iter()
                    .map(|text| Scalar::Str(text.clone()))
                    .collect(),
            )
        }
        _ => ZoneDistinct::Walk,
    }
}

fn resolve_distinct_field(schema: &Schema, field: &str) -> Result<DistinctColumn> {
    if field.is_empty() {
        return Err(Error::event("distinct field name is empty"));
    }
    let index = schema
        .fields
        .iter()
        .position(|field_def| field_def.name == field)
        .ok_or_else(|| Error::schema(format!("unknown distinct field `{field}`")))?;
    let ty = schema.fields[index].ty;
    if matches!(ty, FieldType::Json) {
        return Err(Error::schema(format!(
            "distinct does not apply to json column `{field}`"
        )));
    }
    Ok(DistinctColumn {
        index,
        name: field.to_string(),
        ty,
    })
}

fn resolve_topk_field(schema: &Schema, field: &str) -> Result<DistinctColumn> {
    if field.is_empty() {
        return Err(Error::event("topk field name is empty"));
    }
    let index = schema
        .fields
        .iter()
        .position(|field_def| field_def.name == field)
        .ok_or_else(|| Error::schema(format!("unknown topk field `{field}`")))?;
    let ty = schema.fields[index].ty;
    if matches!(ty, FieldType::Json) {
        return Err(Error::schema(format!(
            "topk does not apply to json column `{field}`"
        )));
    }
    Ok(DistinctColumn {
        index,
        name: field.to_string(),
        ty,
    })
}

fn resolve_quantile_field(schema: &Schema, field: &str) -> Result<ResolvedQuantile> {
    if field.is_empty() {
        return Err(Error::event("quantile field name is empty"));
    }
    let index = schema
        .fields
        .iter()
        .position(|candidate| candidate.name == field)
        .ok_or_else(|| Error::schema(format!("unknown quantile field `{field}`")))?;
    let ty = schema.fields[index].ty;
    if !matches!(
        ty,
        FieldType::Int | FieldType::Timestamp | FieldType::Float | FieldType::Decimal { .. }
    ) {
        return Err(Error::schema(format!(
            "quantile does not apply to {} column `{field}`",
            ty.name()
        )));
    }
    Ok(ResolvedQuantile {
        index,
        name: field.to_string(),
        ty,
    })
}

fn quantile_value(
    schema: &Schema,
    spec: &ResolvedQuantile,
    ts: i64,
    row: usize,
    captures: &[CapturedColumn],
) -> Result<Scalar> {
    if let Some(slot) = captures.iter().find(|column| column.index == spec.index) {
        if !slot.values.is_empty() {
            return slot
                .values
                .get(row)
                .cloned()
                .ok_or_else(|| Error::corrupt(format!("field `{}` is missing a row", spec.name)));
        }
    }
    if spec.index == schema.timestamp_index {
        return Ok(Scalar::Timestamp(ts));
    }
    Err(Error::corrupt(format!(
        "field `{}` was not decoded",
        spec.name
    )))
}

enum QuantileSamples {
    Int(Vec<i64>),
    Timestamp(Vec<i64>),
    Decimal(Vec<i128>),
    Float(Vec<f64>),
}

impl QuantileSamples {
    fn new(ty: FieldType) -> Self {
        match ty {
            FieldType::Int => Self::Int(Vec::new()),
            FieldType::Timestamp => Self::Timestamp(Vec::new()),
            FieldType::Decimal { .. } => Self::Decimal(Vec::new()),
            FieldType::Float => Self::Float(Vec::new()),
            _ => Self::Int(Vec::new()),
        }
    }

    fn push(&mut self, value: Scalar) -> Result<()> {
        match (self, value) {
            (_, Scalar::Null) => Ok(()),
            (Self::Int(samples), Scalar::Int(value)) => push_sample(samples, value),
            (Self::Timestamp(samples), Scalar::Timestamp(value)) => push_sample(samples, value),
            (Self::Decimal(samples), Scalar::Decimal(value)) => push_sample(samples, value),
            (Self::Float(samples), Scalar::Float(value)) => {
                if !value.is_finite() {
                    return Err(Error::corrupt("quantile float is not finite"));
                }
                push_sample(samples, value)
            }
            _ => Err(Error::corrupt(
                "quantile value does not match its column type",
            )),
        }
    }

    fn finish(self, p: QuantileP) -> Result<Option<Scalar>> {
        let n = match &self {
            Self::Int(samples) => samples.len(),
            Self::Timestamp(samples) => samples.len(),
            Self::Decimal(samples) => samples.len(),
            Self::Float(samples) => samples.len(),
        };
        let Some(index) = quantile_rank(p, n) else {
            return Ok(None);
        };
        let value = match self {
            Self::Int(mut samples) => {
                samples.sort_unstable();
                Scalar::Int(samples[index])
            }
            Self::Timestamp(mut samples) => {
                samples.sort_unstable();
                Scalar::Timestamp(samples[index])
            }
            Self::Decimal(mut samples) => {
                samples.sort_unstable();
                Scalar::Decimal(samples[index])
            }
            Self::Float(mut samples) => {
                samples.sort_unstable_by(|left, right| left.total_cmp(right));
                Scalar::Float(samples[index])
            }
        };
        Ok(Some(value))
    }
}

fn push_sample<T>(samples: &mut Vec<T>, value: T) -> Result<()> {
    if samples.len() >= MAX_QUERY_ROWS {
        return Err(Error::event("query row limit exceeded"));
    }
    retain_sample_bytes(samples.len(), std::mem::size_of::<T>())?;
    samples.push(value);
    Ok(())
}

fn retain_sample_bytes(len: usize, width: usize) -> Result<()> {
    let retained = len
        .checked_mul(width)
        .ok_or_else(|| Error::event("query response size limit exceeded"))?;
    if width > MAX_QUERY_BYTES.saturating_sub(retained) {
        return Err(Error::event("query response size limit exceeded"));
    }
    Ok(())
}

fn resolve_group_field(schema: &Schema, by: &str) -> Result<ResolvedGroup> {
    if by.is_empty() {
        return Err(Error::event("group field name is empty"));
    }
    let index = schema
        .fields
        .iter()
        .position(|field| field.name == by)
        .ok_or_else(|| Error::schema(format!("unknown group field `{by}`")))?;
    let ty = schema.fields[index].ty;
    if !matches!(
        ty,
        FieldType::Int
            | FieldType::Timestamp
            | FieldType::Decimal { .. }
            | FieldType::Bool
            | FieldType::String
            | FieldType::Text
    ) {
        return Err(Error::schema(format!(
            "group does not apply to {} column `{by}`",
            ty.name()
        )));
    }
    Ok(ResolvedGroup {
        index,
        name: by.to_string(),
        ty,
    })
}

fn reserve_group_key(distinct: usize, key_bytes: &mut usize, extra: usize) -> Result<()> {
    if distinct >= MAX_HISTOGRAM_BUCKETS {
        return Err(Error::event("stats would emit more than 4096 groups"));
    }
    if extra == 0 {
        return Ok(());
    }
    let room = remaining_query_bytes(*key_bytes)?;
    if extra > room {
        return Err(Error::event("query response size limit exceeded"));
    }
    *key_bytes += extra;
    Ok(())
}

fn absorb_group_stats(
    schema: &Schema,
    running: &mut [RunningStat],
    ts: i64,
    row: usize,
    captured: &[CapturedColumn],
) -> Result<()> {
    for stat in running.iter_mut() {
        let value = stat_value(schema, stat, ts, row, captured)?;
        stat.absorb(value)?;
    }
    Ok(())
}

/// `None` is a null key. The `&str` points at the captured column, so a repeated
/// key can be probed without allocating another `String`.
fn group_text_from_row<'a>(
    schema: &Schema,
    group: &ResolvedGroup,
    row: usize,
    captures: &'a [CapturedColumn],
) -> Result<Option<&'a str>> {
    let slot = captures
        .iter()
        .find(|column| column.index == group.index)
        .ok_or_else(|| Error::corrupt(format!("field `{}` was not decoded", group.name)))?;
    match slot.values.get(row) {
        Some(Scalar::Null) => Ok(None),
        Some(Scalar::Str(value)) => Ok(Some(value.as_str())),
        Some(_) => Err(Error::corrupt(format!(
            "field `{}` has a value that does not match its type",
            group.name
        ))),
        None => {
            if group.index == schema.timestamp_index {
                return Err(Error::corrupt(format!(
                    "field `{}` has a value that does not match its type",
                    group.name
                )));
            }
            Err(Error::corrupt(format!(
                "field `{}` is missing a row",
                group.name
            )))
        }
    }
}

fn group_key_from_scalar(group: &ResolvedGroup, scalar: Scalar) -> Result<GroupKey> {
    match (group.ty, scalar) {
        (_, Scalar::Null) => Ok(GroupKey::Null),
        (FieldType::Bool, Scalar::Bool(value)) => Ok(GroupKey::Bool(value)),
        (FieldType::Int, Scalar::Int(value)) => Ok(GroupKey::Int(value)),
        (FieldType::Timestamp, Scalar::Timestamp(value)) => Ok(GroupKey::Timestamp(value)),
        (FieldType::Decimal { .. }, Scalar::Decimal(value)) => Ok(GroupKey::Decimal(value)),
        (FieldType::String | FieldType::Text, Scalar::Str(value)) => Ok(GroupKey::Str(value)),
        _ => Err(Error::corrupt(format!(
            "field `{}` has a value that does not match its type",
            group.name
        ))),
    }
}

fn note_latest(
    winners: &mut BTreeMap<GroupKey, LatestHit>,
    key_bytes: &mut usize,
    json_bytes: &mut usize,
    key: GroupKey,
    ts: i64,
    json: Vec<u8>,
) -> Result<()> {
    let extra = match &key {
        GroupKey::Str(text) => text.len(),
        _ => 0,
    };
    if winners.contains_key(&key) {
        let current = winners
            .get(&key)
            .expect("latest key was present in the map");
        if ts < current.ts {
            return Ok(());
        }
        let next_json = json_bytes
            .checked_sub(current.json.len())
            .and_then(|n| n.checked_add(json.len()))
            .ok_or_else(|| Error::event("query response size overflow"))?;
        ensure_latest_response(next_json, winners.len())?;
        *json_bytes = next_json;
        winners.insert(key, LatestHit { ts, json });
        return Ok(());
    }
    reserve_latest_key(winners.len(), key_bytes, extra)?;
    let next_json = json_bytes
        .checked_add(json.len())
        .ok_or_else(|| Error::event("query response size overflow"))?;
    ensure_latest_response(next_json, winners.len() + 1)?;
    *json_bytes = next_json;
    winners.insert(key, LatestHit { ts, json });
    Ok(())
}

fn reserve_latest_key(distinct: usize, key_bytes: &mut usize, extra: usize) -> Result<()> {
    if distinct >= MAX_HISTOGRAM_BUCKETS {
        return Err(Error::event("latest would emit more than 4096 keys"));
    }
    if extra == 0 {
        return Ok(());
    }
    let room = remaining_query_bytes(*key_bytes)?;
    if extra > room {
        return Err(Error::event("query response size limit exceeded"));
    }
    *key_bytes += extra;
    Ok(())
}

fn ensure_latest_response(json_bytes: usize, groups: usize) -> Result<()> {
    let len = if groups == 0 {
        2
    } else {
        json_bytes
            .checked_add(groups - 1)
            .and_then(|n| n.checked_add(2))
            .ok_or_else(|| Error::event("query response size overflow"))?
    };
    if len > MAX_QUERY_BYTES {
        return Err(Error::event("query response size limit exceeded"));
    }
    Ok(())
}

fn group_key_from_row(
    schema: &Schema,
    group: &ResolvedGroup,
    ts: i64,
    row: usize,
    captures: &[CapturedColumn],
) -> Result<GroupKey> {
    let scalar = if group.index == schema.timestamp_index {
        Scalar::Timestamp(ts)
    } else {
        let slot = captures
            .iter()
            .find(|column| column.index == group.index)
            .ok_or_else(|| Error::corrupt(format!("field `{}` was not decoded", group.name)))?;
        slot.values
            .get(row)
            .cloned()
            .ok_or_else(|| Error::corrupt(format!("field `{}` is missing a row", group.name)))?
    };
    group_key_from_scalar(group, scalar)
}

/// One float `sum`, nothing else, and no row predicate. The column index is
/// the encoded values to add when the block is fully inside the range.
fn dense_float_sum(stats: &[RunningStat], resolved: &ResolvedScan) -> Option<usize> {
    if !resolved.unfiltered() || stats.len() != 1 {
        return None;
    }
    let stat = &stats[0];
    if stat.spec.ty == FieldType::Float
        && stat.spec.sum
        && !stat.spec.min
        && !stat.spec.max
        && !stat.spec.needs_moment()
    {
        Some(stat.spec.index)
    } else {
        None
    }
}

fn capture_columns(indexes: impl IntoIterator<Item = usize>) -> Vec<CapturedColumn> {
    let mut captures: Vec<CapturedColumn> = Vec::new();
    for index in indexes {
        if captures.iter().any(|column| column.index == index) {
            continue;
        }
        captures.push(CapturedColumn {
            index,
            values: Vec::new(),
        });
    }
    captures
}

fn empty_column_stats(columns: &[ResolvedStat]) -> Vec<ColumnStats> {
    columns
        .iter()
        .map(|column| ColumnStats {
            field: column.name.clone(),
            sum: column.sum.then(|| match column.ty {
                FieldType::Float => StatSum::Float {
                    total: 0.0,
                    contributed: false,
                },
                _ => StatSum::Exact {
                    total: 0,
                    contributed: false,
                },
            }),
            min: column.min.then_some(None),
            max: column.max.then_some(None),
            var: column.var.then_some(None),
            stddev: column.stddev.then_some(None),
        })
        .collect()
}

/// Reduced population variance `(n * sum_sq - sum^2) / n^2`.
///
/// `extra_twos` and `extra_fives` are factors of the denominator that did not
/// fit in `denom` after dividing a decimal variance by `10^(2*scale)`. The
/// value is `numer / (denom * 2^extra_twos * 5^extra_fives)`.
struct ReducedVar {
    numer: U256,
    denom: u128,
    extra_twos: u32,
    extra_fives: u32,
}

impl ReducedVar {
    fn stddev(&self) -> f64 {
        if self.denom == 0 {
            return f64::INFINITY;
        }
        let mut value = u256_to_f64(self.numer) / self.denom as f64;
        if self.extra_twos > 0 {
            value /= 2f64.powi(self.extra_twos as i32);
        }
        if self.extra_fives > 0 {
            value /= 5f64.powi(self.extra_fives as i32);
        }
        value.sqrt()
    }

    fn format(&self) -> String {
        if self.extra_twos == 0 && self.extra_fives == 0 {
            return format_reduced(self.numer, self.denom);
        }
        if let Some((twos, fives)) = factor_two_five(self.denom) {
            let twos = twos + self.extra_twos;
            let fives = fives + self.extra_fives;
            let places = twos.max(fives);
            let mut numer = self.numer;
            let mut fit = true;
            for _ in 0..fives.saturating_sub(twos) {
                match numer.mul_u64(2) {
                    Some(next) => numer = next,
                    None => fit = false,
                }
                if !fit {
                    break;
                }
            }
            if fit {
                for _ in 0..twos.saturating_sub(fives) {
                    match numer.mul_u64(5) {
                        Some(next) => numer = next,
                        None => {
                            fit = false;
                            break;
                        }
                    }
                }
            }
            if fit {
                return insert_decimal_point(&format_u256(numer), places as usize);
            }
        }
        let mut denom = self.denom.to_string();
        for _ in 0..self.extra_twos {
            denom = mul_decimal_string(&denom, 2);
        }
        for _ in 0..self.extra_fives {
            denom = mul_decimal_string(&denom, 5);
        }
        format!("{}/{}", format_u256(self.numer), denom)
    }
}

fn format_reduced(numer: U256, denom: u128) -> String {
    if denom == 1 {
        return format_u256(numer);
    }
    if let Some((twos, fives)) = factor_two_five(denom) {
        let places = twos.max(fives) as usize;
        let (whole, mut rem) =
            div_rem_u128(numer, denom).expect("reduced variance denominator is non-zero");
        let mut frac = String::new();
        for _ in 0..places {
            let prod = U256::from_u128(rem)
                .mul_u64(10)
                .expect("one decimal step of a u128 remainder fits");
            let (digit, next) =
                div_rem_u128(prod, denom).expect("decimal digit divisor is non-zero");
            frac.push(char::from(b'0' + digit.low_u64() as u8));
            rem = next;
        }
        while frac.ends_with('0') {
            frac.pop();
        }
        let whole = format_u256(whole);
        if frac.is_empty() {
            whole
        } else {
            format!("{whole}.{frac}")
        }
    } else {
        format!("{}/{}", format_u256(numer), denom)
    }
}

fn insert_decimal_point(digits: &str, places: usize) -> String {
    if places == 0 {
        return digits.to_string();
    }
    let digits = if digits.len() > places {
        digits.to_string()
    } else {
        format!("{}{digits}", "0".repeat(places + 1 - digits.len()))
    };
    let split = digits.len() - places;
    let whole = &digits[..split];
    let mut frac = digits[split..].to_string();
    while frac.ends_with('0') {
        frac.pop();
    }
    if frac.is_empty() {
        whole.to_string()
    } else {
        format!("{whole}.{frac}")
    }
}

fn mul_decimal_string(digits: &str, factor: u32) -> String {
    let bytes = digits.as_bytes();
    let mut out = vec![b'0'; bytes.len() + 1];
    let mut carry = 0u32;
    for index in (0..bytes.len()).rev() {
        let digit = u32::from(bytes[index] - b'0');
        let prod = digit * factor + carry;
        out[index + 1] = b'0' + (prod % 10) as u8;
        carry = prod / 10;
    }
    out[0] = b'0' + carry as u8;
    let text = String::from_utf8(out).expect("decimal digits are ascii");
    let trimmed = text.trim_start_matches('0');
    if trimmed.is_empty() {
        "0".to_string()
    } else {
        trimmed.to_string()
    }
}

/// Divide a reduced population variance by `10^(2*scale)` and reduce again.
fn scale_decimal_var(var: ReducedVar, scale: u32) -> ReducedVar {
    if scale == 0 {
        return var;
    }
    let exp = scale.saturating_mul(2);
    let mut numer = var.numer;
    let mut twos = exp;
    let mut fives = exp;
    while twos > 0 && !numer.is_zero() && numer.limbs[0] % 2 == 0 {
        numer = numer.shr1();
        twos -= 1;
    }
    while fives > 0 && !numer.is_zero() {
        let Some((quot, rem)) = div_rem_u128(numer, 5) else {
            break;
        };
        if rem != 0 {
            break;
        }
        numer = quot;
        fives -= 1;
    }
    let mut denom = var.denom;
    while twos > 0 {
        match denom.checked_mul(2) {
            Some(next) => {
                denom = next;
                twos -= 1;
            }
            None => break,
        }
    }
    while fives > 0 {
        match denom.checked_mul(5) {
            Some(next) => {
                denom = next;
                fives -= 1;
            }
            None => break,
        }
    }
    ReducedVar {
        numer,
        denom,
        extra_twos: twos,
        extra_fives: fives,
    }
}

fn exact_population_var(n: u64, sum: i128, sum_sq: i128) -> Result<ReducedVar> {
    if n == 0 || sum_sq < 0 {
        return Err(Error::event("variance overflowed"));
    }
    let sum_sq = U256::from_u128(sum_sq as u128);
    let n_sum_sq = sum_sq
        .mul_u64(n)
        .ok_or_else(|| Error::event("variance overflowed"))?;
    let sum_squared = U256::from_limbs(mul_limbs(sum.unsigned_abs(), sum.unsigned_abs()));
    let numer = n_sum_sq
        .checked_sub(sum_squared)
        .ok_or_else(|| Error::event("variance overflowed"))?;
    let denom = u128::from(n)
        .checked_mul(u128::from(n))
        .ok_or_else(|| Error::event("variance overflowed"))?;
    let gcd = gcd_u128(numer, denom);
    let numer = div_rem_u128(numer, gcd)
        .ok_or_else(|| Error::event("variance overflowed"))?
        .0;
    Ok(ReducedVar {
        numer,
        denom: denom / gcd,
        extra_twos: 0,
        extra_fives: 0,
    })
}

/// Little-endian 64-bit limbs.
#[derive(Clone, Copy, Debug)]
struct U256 {
    limbs: [u64; 4],
}

impl U256 {
    fn zero() -> Self {
        Self { limbs: [0; 4] }
    }

    fn from_limbs(limbs: [u64; 4]) -> Self {
        Self { limbs }
    }

    fn from_u128(value: u128) -> Self {
        Self {
            limbs: [value as u64, (value >> 64) as u64, 0, 0],
        }
    }

    fn is_zero(self) -> bool {
        self.limbs.iter().all(|limb| *limb == 0)
    }

    fn bit_len(self) -> u32 {
        for (index, limb) in self.limbs.iter().copied().enumerate().rev() {
            if limb != 0 {
                return index as u32 * 64 + (64 - limb.leading_zeros());
            }
        }
        0
    }

    fn bit(self, index: u32) -> u64 {
        let limb = self.limbs[(index / 64) as usize];
        (limb >> (index % 64)) & 1
    }

    fn set_bit(&mut self, index: u32) {
        self.limbs[(index / 64) as usize] |= 1u64 << (index % 64);
    }

    fn cmp(self, other: Self) -> Ordering {
        self.limbs.iter().rev().cmp(other.limbs.iter().rev())
    }

    fn shr1(self) -> Self {
        let mut out = [0u64; 4];
        let mut carry = 0u64;
        for index in (0..4).rev() {
            let next = self.limbs[index] & 1;
            out[index] = (self.limbs[index] >> 1) | (carry << 63);
            carry = next;
        }
        Self { limbs: out }
    }

    fn shl1_add(self, bit: u64) -> Self {
        let mut out = [0u64; 4];
        let mut carry = bit;
        for i in 0..4 {
            let next = self.limbs[i] >> 63;
            out[i] = (self.limbs[i] << 1) | carry;
            carry = next;
        }
        debug_assert_eq!(carry, 0);
        Self { limbs: out }
    }

    fn checked_sub(self, other: Self) -> Option<Self> {
        let mut out = [0u64; 4];
        let mut borrow = 0u64;
        for i in 0..4 {
            let (diff, borrowed) = self.limbs[i].overflowing_sub(other.limbs[i]);
            let (diff, borrowed_more) = diff.overflowing_sub(borrow);
            out[i] = diff;
            borrow = u64::from(borrowed || borrowed_more);
        }
        if borrow == 0 {
            Some(Self { limbs: out })
        } else {
            None
        }
    }

    fn mul_u64(self, rhs: u64) -> Option<Self> {
        let mut out = [0u64; 4];
        let mut carry = 0u128;
        for i in 0..4 {
            let prod = self.limbs[i] as u128 * u128::from(rhs) + carry;
            out[i] = prod as u64;
            carry = prod >> 64;
        }
        if carry == 0 {
            Some(Self { limbs: out })
        } else {
            None
        }
    }

    fn as_u128(self) -> Option<u128> {
        if self.limbs[2] != 0 || self.limbs[3] != 0 {
            None
        } else {
            Some((self.limbs[1] as u128) << 64 | self.limbs[0] as u128)
        }
    }

    fn low_u64(self) -> u64 {
        self.limbs[0]
    }
}

fn div_rem_u128(numer: U256, denom: u128) -> Option<(U256, u128)> {
    if denom == 0 {
        return None;
    }
    let divisor = U256::from_u128(denom);
    let mut quot = U256::zero();
    let mut rem = U256::zero();
    let bits = numer.bit_len();
    for index in (0..bits).rev() {
        rem = rem.shl1_add(numer.bit(index));
        if rem.cmp(divisor) != Ordering::Less {
            rem = rem.checked_sub(divisor)?;
            quot.set_bit(index);
        }
    }
    Some((quot, rem.as_u128()?))
}

fn gcd_u128(mut numer: U256, mut denom: u128) -> u128 {
    while denom != 0 {
        let Some((_, rem)) = div_rem_u128(numer, denom) else {
            return 1;
        };
        numer = U256::from_u128(denom);
        denom = rem;
    }
    numer.as_u128().unwrap_or(1)
}

fn factor_two_five(mut denom: u128) -> Option<(u32, u32)> {
    if denom == 0 {
        return None;
    }
    let mut twos = 0u32;
    while denom % 2 == 0 {
        denom /= 2;
        twos += 1;
    }
    let mut fives = 0u32;
    while denom % 5 == 0 {
        denom /= 5;
        fives += 1;
    }
    (denom == 1).then_some((twos, fives))
}

fn format_u256(mut value: U256) -> String {
    if value.is_zero() {
        return "0".to_string();
    }
    let mut digits = Vec::new();
    while !value.is_zero() {
        let (quot, rem) = div_rem_u128(value, 10).expect("dividing by ten");
        digits.push(char::from(b'0' + rem as u8));
        value = quot;
    }
    digits.iter().rev().collect()
}

/// Round `n` to the nearest f64, ties to even.
fn u256_to_f64(n: U256) -> f64 {
    let bits = n.bit_len();
    if bits == 0 {
        return 0.0;
    }
    if bits <= 53 {
        return n.low_u64() as f64;
    }
    if bits > 1024 {
        return f64::INFINITY;
    }
    let shift = bits - 54;
    let top = shr_u256_u64(n, shift);
    let mut mant = top >> 1;
    let round = top & 1 == 1;
    let sticky = shift > 0 && low_bits_nonzero(n, shift);
    if round && (sticky || mant & 1 == 1) {
        mant += 1;
    }
    let mut exp = bits - 1;
    if mant == 1 << 53 {
        mant >>= 1;
        exp += 1;
    }
    if exp >= 1024 {
        return f64::INFINITY;
    }
    let frac = mant & ((1u64 << 52) - 1);
    f64::from_bits(((u64::from(exp) + 1023) << 52) | frac)
}

fn shr_u256_u64(n: U256, shift: u32) -> u64 {
    if shift >= 256 {
        return 0;
    }
    let limb = (shift / 64) as usize;
    let offset = shift % 64;
    let low = n.limbs.get(limb).copied().unwrap_or(0);
    let high = n.limbs.get(limb + 1).copied().unwrap_or(0);
    if offset == 0 {
        low
    } else {
        (low >> offset) | (high << (64 - offset))
    }
}

fn low_bits_nonzero(n: U256, bits: u32) -> bool {
    if bits == 0 {
        return false;
    }
    let full = (bits / 64) as usize;
    if n.limbs.iter().take(full).any(|limb| *limb != 0) {
        return true;
    }
    let rem = bits % 64;
    if rem == 0 {
        return false;
    }
    let mask = (1u64 << rem) - 1;
    n.limbs[full] & mask != 0
}

fn resolve_predicates(schema: &Schema, predicates: &[Predicate]) -> Result<ResolvedScan> {
    let mut grouped: Vec<ColumnPredicate> = Vec::new();
    let mut ranges: Vec<RangeFilter> = Vec::new();
    let mut prefixes: Vec<PrefixFilter> = Vec::new();
    let mut prefix_conflict = false;
    for predicate in predicates {
        match predicate {
            Predicate::Eq(name, value) => {
                push_equality(schema, &mut grouped, name, vec![value.clone()])?;
            }
            Predicate::In(name, values) => {
                push_equality(schema, &mut grouped, name, values.clone())?;
            }
            Predicate::Cmp(name, op, value) => {
                push_range(schema, &mut ranges, name, *op, value)?;
            }
            Predicate::Prefix(name, prefix) => {
                if !push_prefix(schema, &mut prefixes, name, prefix)? {
                    prefix_conflict = true;
                }
            }
        }
    }
    for range in &ranges {
        if let Some(existing) = grouped.iter_mut().find(|pred| pred.index == range.index) {
            existing
                .allowed
                .retain(|current| range_matches(current, range));
        }
    }
    for prefix in &prefixes {
        if let Some(existing) = grouped.iter_mut().find(|pred| pred.index == prefix.index) {
            let needle = prefix.prefix.as_bytes();
            existing.allowed.retain(|current| match current {
                Scalar::Str(text) => text.as_bytes().starts_with(needle),
                _ => false,
            });
        }
    }
    // An equality set that already satisfies the prefix is the tighter filter.
    prefixes.retain(|prefix| {
        !grouped
            .iter()
            .any(|pred| pred.index == prefix.index && !pred.allowed.is_empty())
    });
    let impossible = prefix_conflict
        || grouped.iter().any(|pred| pred.allowed.is_empty())
        || ranges.iter().any(range_impossible);
    Ok(ResolvedScan {
        predicates: grouped,
        ranges,
        prefixes,
        impossible,
    })
}

/// `Ok(false)` means the prefixes on this column cannot match one string.
fn push_prefix(
    schema: &Schema,
    prefixes: &mut Vec<PrefixFilter>,
    name: &str,
    prefix: &str,
) -> Result<bool> {
    if prefix.is_empty() {
        return Err(Error::schema(format!(
            "prefix on `{name}` is missing a value"
        )));
    }
    let index = field_index(schema, name)?;
    let ty = schema.fields[index].ty;
    if !matches!(ty, FieldType::String | FieldType::Text) {
        return Err(Error::schema(format!(
            "prefix does not apply to {} column `{name}`",
            ty.name()
        )));
    }
    if let Some(existing) = prefixes.iter_mut().find(|filter| filter.index == index) {
        if prefix.as_bytes().starts_with(existing.prefix.as_bytes()) {
            existing.prefix = prefix.to_string();
            return Ok(true);
        }
        if existing.prefix.as_bytes().starts_with(prefix.as_bytes()) {
            return Ok(true);
        }
        return Ok(false);
    }
    prefixes.push(PrefixFilter {
        index,
        prefix: prefix.to_string(),
    });
    Ok(true)
}

fn push_equality(
    schema: &Schema,
    grouped: &mut Vec<ColumnPredicate>,
    name: &str,
    values: Vec<Scalar>,
) -> Result<()> {
    let index = field_index(schema, name)?;
    let ty = schema.fields[index].ty;
    for value in &values {
        if !value::scalar_matches_field(value, ty) {
            return Err(Error::schema(format!(
                "filter value for `{name}` does not match type {}",
                ty.name()
            )));
        }
    }
    if let Some(existing) = grouped.iter_mut().find(|pred| pred.index == index) {
        existing
            .allowed
            .retain(|current| values.iter().any(|next| next == current));
    } else {
        grouped.push(ColumnPredicate {
            index,
            allowed: values,
        });
    }
    Ok(())
}

fn push_range(
    schema: &Schema,
    ranges: &mut Vec<RangeFilter>,
    name: &str,
    op: CmpOp,
    value: &Scalar,
) -> Result<()> {
    let index = field_index(schema, name)?;
    let ty = schema.fields[index].ty;
    if !matches!(
        ty,
        crate::schema::FieldType::Int
            | crate::schema::FieldType::Timestamp
            | crate::schema::FieldType::Float
            | crate::schema::FieldType::Decimal { .. }
    ) {
        return Err(Error::schema(format!(
            "range filters do not apply to {} column `{name}`",
            ty.name()
        )));
    }
    if !value::scalar_matches_field(value, ty) || matches!(value, Scalar::Null) {
        return Err(Error::schema(format!(
            "filter value for `{name}` does not match type {}",
            ty.name()
        )));
    }
    if let Scalar::Float(number) = value {
        if !number.is_finite() {
            return Err(Error::schema(format!(
                "filter value for `{name}` must be a finite number"
            )));
        }
    }
    let end = RangeEnd {
        inclusive: matches!(op, CmpOp::Ge | CmpOp::Le),
        value: value.clone(),
    };
    let lower = matches!(op, CmpOp::Gt | CmpOp::Ge);
    if let Some(existing) = ranges.iter_mut().find(|range| range.index == index) {
        if lower {
            existing.lower = Some(match existing.lower.clone() {
                Some(previous) => tighter_lower(previous, end),
                None => end,
            });
        } else {
            existing.upper = Some(match existing.upper.clone() {
                Some(previous) => tighter_upper(previous, end),
                None => end,
            });
        }
    } else {
        let (lower_end, upper_end) = if lower {
            (Some(end), None)
        } else {
            (None, Some(end))
        };
        ranges.push(RangeFilter {
            index,
            lower: lower_end,
            upper: upper_end,
        });
    }
    Ok(())
}

fn field_index(schema: &Schema, name: &str) -> Result<usize> {
    schema
        .fields
        .iter()
        .position(|field| field.name == name)
        .ok_or_else(|| Error::schema(format!("unknown filter field `{name}`")))
}

enum Take {
    Skip,
    Keep,
    Done,
}

fn window_full(limit: Option<u64>, emitted: usize) -> bool {
    limit.is_some_and(|limit| emitted as u64 >= limit)
}

fn take_match(offset: u64, limit: Option<u64>, skipped: &mut u64, emitted: usize) -> Take {
    if *skipped < offset {
        *skipped += 1;
        return Take::Skip;
    }
    if window_full(limit, emitted) {
        Take::Done
    } else {
        Take::Keep
    }
}

/// The frame header's uncompressed length is not covered by the payload CRC.
/// Every read refuses a length above [`MAX_QUERY_BYTES`]. A block that will be
/// returned in full, with every column, also refuses a length the shrinking
/// response budget cannot hold. A projected scan leaves that check to the
/// per-row JSON length, because unread columns inflate `uncompressed_len`.
fn block_exceeds_read_budget(
    uncompressed_len: u32,
    whole: bool,
    string_budget: usize,
    full_columns: bool,
) -> bool {
    let declared = uncompressed_len as usize;
    declared > MAX_QUERY_BYTES || (full_columns && whole && declared > string_budget)
}

/// `block.row_count` is the frame header. The payload CRC does not cover it.
fn require_header_row_count(header_rows: u32, payload_rows: usize) -> Result<()> {
    if payload_rows != header_rows as usize {
        return Err(Error::corrupt(
            "block row count does not match the frame header",
        ));
    }
    Ok(())
}

/// Index min/max can claim a block is inside the query while a payload timestamp is not.
/// A null or undecodable timestamp is [`Error::Corrupt`], the same error a row decode returns.
fn every_timestamp_in_range(
    schema: &Schema,
    payload: &[u8],
    from_ms: i64,
    to_ms: i64,
) -> Result<bool> {
    let timestamps = read_timestamp_column(schema, payload)?;
    Ok(timestamps.iter().all(|ts| *ts >= from_ms && *ts <= to_ms))
}

/// The remaining offset covers every row in the block, so a timestamp scan can decide a skip.
/// Selected fields, in schema order, with duplicates collapsed.
fn select_keep(schema: &Schema, columns: &[usize]) -> Result<Vec<bool>> {
    let mut keep = vec![false; schema.fields.len()];
    for index in columns {
        let Some(slot) = keep.get_mut(*index) else {
            return Err(Error::schema("unknown select field"));
        };
        *slot = true;
    }
    Ok(keep)
}

/// Timestamp, filter columns, and selected columns. Anything else stays compressed.
fn columns_to_load(schema: &Schema, resolved: &ResolvedScan, keep: &[bool]) -> Vec<bool> {
    let mut load = keep.to_vec();
    if let Some(slot) = load.get_mut(schema.timestamp_index) {
        *slot = true;
    }
    for predicate in &resolved.predicates {
        if let Some(slot) = load.get_mut(predicate.index) {
            *slot = true;
        }
    }
    for range in &resolved.ranges {
        if let Some(slot) = load.get_mut(range.index) {
            *slot = true;
        }
    }
    for prefix in &resolved.prefixes {
        if let Some(slot) = load.get_mut(prefix.index) {
            *slot = true;
        }
    }
    load
}

/// The column of the only predicate when that predicate is one string, text,
/// or bool value. Any other shape still walks rows.
fn header_equality_column(schema: &Schema, predicates: &[ColumnPredicate]) -> Option<usize> {
    let [predicate] = predicates else {
        return None;
    };
    let field = schema.fields.get(predicate.index)?;
    let one_value = match field.ty {
        FieldType::String | FieldType::Text => {
            matches!(predicate.allowed.as_slice(), [Scalar::Str(_)])
        }
        FieldType::Bool => matches!(predicate.allowed.as_slice(), [Scalar::Bool(_)]),
        _ => false,
    };
    one_value.then_some(predicate.index)
}

fn needed_column(schema: &Schema, resolved: &ResolvedScan, extra: Option<usize>) -> usize {
    resolved
        .predicates
        .iter()
        .map(|predicate| predicate.index)
        .chain(resolved.ranges.iter().map(|range| range.index))
        .chain(resolved.prefixes.iter().map(|prefix| prefix.index))
        .chain(extra)
        .fold(schema.timestamp_index, usize::max)
}

fn offset_covers_block(offset: u64, skipped: u64, block_rows: u32) -> bool {
    let block_rows = u64::from(block_rows);
    block_rows > 0 && offset.saturating_sub(skipped) >= block_rows
}

/// Skip a block whose every row is a match when those rows fall entirely inside the offset.
fn skip_whole_block(offset: u64, block_rows: u32, skipped: &mut u64) -> bool {
    let block_rows = u64::from(block_rows);
    if block_rows == 0 {
        return false;
    }
    let remaining = offset.saturating_sub(*skipped);
    if remaining >= block_rows {
        *skipped += block_rows;
        true
    } else {
        false
    }
}

fn returns_whole_block(
    unfiltered: bool,
    contained: bool,
    skipped: u64,
    offset: u64,
    limit: Option<u64>,
    emitted: usize,
    block_rows: u32,
) -> bool {
    if !unfiltered || !contained || skipped < offset {
        return false;
    }
    match limit {
        None => true,
        Some(limit) => limit.saturating_sub(emitted as u64) >= u64::from(block_rows),
    }
}

struct HistogramSpan {
    first_start: i128,
    bucket_ms: i64,
    buckets: usize,
}

/// `None` when `from_ms > to_ms` (no bucket intersects the range).
/// More than [`MAX_HISTOGRAM_BUCKETS`] is an error and does not read the store.
fn histogram_span(from_ms: i64, to_ms: i64, bucket_ms: i64) -> Result<Option<HistogramSpan>> {
    if bucket_ms <= 0 {
        return Err(Error::event("bucket_ms must be a positive integer"));
    }
    if from_ms > to_ms {
        return Ok(None);
    }
    let width = i128::from(bucket_ms);
    let first_start = (i128::from(from_ms)).div_euclid(width) * width;
    let last_start = (i128::from(to_ms)).div_euclid(width) * width;
    let buckets = (last_start - first_start) / width + 1;
    if buckets > i128::from(MAX_HISTOGRAM_BUCKETS as u32) {
        return Err(Error::event("histogram would emit more than 4096 buckets"));
    }
    // Later starts sit between `first_start` and `floor(to / bucket_ms) * bucket_ms`,
    // and that last start is always `<= to`, so a representable first start is enough.
    if i64::try_from(first_start).is_err() {
        return Err(Error::event("histogram bucket start is outside i64"));
    }
    let buckets =
        usize::try_from(buckets).map_err(|_| Error::event("histogram bucket overflow"))?;
    Ok(Some(HistogramSpan {
        first_start,
        bucket_ms,
        buckets,
    }))
}

fn histogram_buckets(span: HistogramSpan) -> Result<Vec<HistogramBucket>> {
    let zeros = vec![0; span.buckets];
    buckets_from_counts(span, &zeros)
}

fn buckets_from_counts(span: HistogramSpan, counts: &[u64]) -> Result<Vec<HistogramBucket>> {
    let mut out = Vec::with_capacity(span.buckets);
    for (index, count) in counts.iter().copied().enumerate() {
        let start = span.first_start + i128::from(index as u64) * i128::from(span.bucket_ms);
        let start_ms = i64::try_from(start)
            .map_err(|_| Error::event("histogram bucket start is outside i64"))?;
        out.push(HistogramBucket { start_ms, count });
    }
    Ok(out)
}

fn remaining_query_bytes(produced: usize) -> Result<usize> {
    if produced >= MAX_QUERY_BYTES || MAX_QUERY_BYTES - produced <= 1 {
        return Err(Error::event("query response size limit exceeded"));
    }
    Ok(MAX_QUERY_BYTES - produced - 1)
}

fn validate_options(options: &StoreOptions) -> Result<()> {
    if options.block_rows == 0 || options.block_rows > 100_000 {
        return Err(Error::schema("block_rows must be between 1 and 100000"));
    }
    if !(1..=22).contains(&options.zstd_level) {
        return Err(Error::schema("zstd_level must be between 1 and 22"));
    }
    if options.segment_bytes == 0 {
        return Err(Error::schema("segment_bytes must be greater than zero"));
    }
    if options.parser_threads == 0 || options.compress_threads == 0 {
        return Err(Error::schema(
            "worker thread counts must be greater than zero",
        ));
    }
    if options.linger < Duration::from_millis(1) {
        return Err(Error::schema("linger must be at least 1ms"));
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SchemaLockAction {
    /// `schema.lock` already describes this schema, or it was just created.
    Ready,
    /// The open schema appends fields. Rewrite the lock after the catalog loads.
    Rewrite,
}

fn schema_lock_action(dir: &Path, schema: &Schema) -> Result<SchemaLockAction> {
    if rewrite_marker_present(dir) {
        return Err(Error::corrupt("directory contains an unfinished rewrite"));
    }
    let path = dir.join("schema.lock");
    let canonical = schema.canonical();
    if !path.exists() {
        write_schema_lock(&path, &canonical)?;
        return Ok(SchemaLockAction::Ready);
    }
    let existing = fs::read_to_string(&path)?;
    if existing == canonical {
        return Ok(SchemaLockAction::Ready);
    }
    let locked = schema::parse_schema(&existing).map_err(|err| {
        Error::schema(format!(
            "schema.lock does not match the supplied schema; this directory was created with a different schema ({err})"
        ))
    })?;
    match schema::schema_evolution(&locked, schema)? {
        schema::SchemaEvolution::Unchanged => Ok(SchemaLockAction::Ready),
        schema::SchemaEvolution::Appended { .. } => Ok(SchemaLockAction::Rewrite),
    }
}

#[cfg(test)]
thread_local! {
    static REWRITE_FAIL_BEFORE_PUBLISH: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    static REWRITE_COPY_ACROSS_DEVICES: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    /// Fail the publish rename once this many calls have been allowed through.
    /// `0` disables the hook. `1` fails the first rename.
    static REWRITE_FAIL_PUBLISH_RENAME_AFTER: std::cell::Cell<u32> =
        const { std::cell::Cell::new(0) };
}

fn rewrite_should_copy_across_devices() -> bool {
    #[cfg(test)]
    {
        REWRITE_COPY_ACROSS_DEVICES.with(|flag| {
            let copy = flag.get();
            flag.set(false);
            copy
        })
    }
    #[cfg(not(test))]
    {
        false
    }
}

fn rewrite_publish_rename_should_fail() -> bool {
    #[cfg(test)]
    {
        REWRITE_FAIL_PUBLISH_RENAME_AFTER.with(|flag| {
            let left = flag.get();
            if left == 0 {
                return false;
            }
            if left == 1 {
                flag.set(0);
                return true;
            }
            flag.set(left - 1);
            false
        })
    }
    #[cfg(not(test))]
    {
        false
    }
}

fn rewrite_should_fail_before_publish() -> bool {
    #[cfg(test)]
    {
        REWRITE_FAIL_BEFORE_PUBLISH.with(|flag| {
            let fail = flag.get();
            flag.set(false);
            fail
        })
    }
    #[cfg(not(test))]
    {
        false
    }
}

fn relative_change(source: u64, destination: u64) -> f64 {
    if source == 0 {
        0.0
    } else {
        (source as f64 - destination as f64) / source as f64
    }
}

fn relative_change_sum(
    source_a: u64,
    source_b: u64,
    destination_a: u64,
    destination_b: u64,
) -> f64 {
    let source = u128::from(source_a) + u128::from(source_b);
    let destination = u128::from(destination_a) + u128::from(destination_b);
    if source == 0 {
        0.0
    } else {
        (source as f64 - destination as f64) / source as f64
    }
}

fn directory_has_entries(dir: &Path) -> Result<bool> {
    Ok(fs::read_dir(dir)?.next().is_some())
}

/// Present from the moment an existing destination starts a rewrite until
/// `schema.lock` is in place and that directory has been fsynced.
const REWRITE_INCOMPLETE_MARKER: &str = "eventer-rewrite.incomplete";

fn write_rewrite_marker(dir: &Path) -> Result<()> {
    let path = dir.join(REWRITE_INCOMPLETE_MARKER);
    {
        let mut file = fs::File::create(&path).map_err(Error::io)?;
        file.write_all(b"incomplete\n").map_err(Error::io)?;
        file.sync_all().map_err(Error::io)?;
    }
    fsync_dir(dir)
}

fn remove_rewrite_marker(dir: &Path) -> Result<()> {
    let path = dir.join(REWRITE_INCOMPLETE_MARKER);
    if path.exists() {
        fs::remove_file(&path).map_err(Error::io)?;
        fsync_dir(dir)?;
    }
    Ok(())
}

fn rewrite_marker_present(dir: &Path) -> bool {
    dir.join(REWRITE_INCOMPLETE_MARKER).is_file()
}

/// Remove a killed rewrite so a later call can publish into `dir`.
/// Store files and scratch directories go first. The marker is last, so a
/// crash in the middle still leaves a directory that [`Store::open`] refuses.
fn clear_unfinished_rewrite(dir: &Path) -> Result<()> {
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let file_type = entry.file_type()?;
        if file_type.is_symlink() {
            continue;
        }
        let file_name = entry.file_name();
        let name = file_name.to_string_lossy();
        if name == REWRITE_INCOMPLETE_MARKER {
            continue;
        }
        if file_type.is_dir() {
            if name.starts_with("eventer-rewrite-") {
                fs::remove_dir_all(entry.path()).map_err(Error::io)?;
            }
            continue;
        }
        if file_type.is_file() && (name == "schema.lock" || name.starts_with("seg-")) {
            fs::remove_file(entry.path()).map_err(Error::io)?;
        }
    }
    fsync_dir(dir)?;
    remove_rewrite_marker(dir)
}

/// Deletes the rewrite marker unless [`RemoveRewriteMarker::disarm`] ran.
struct RemoveRewriteMarker {
    dir: Option<PathBuf>,
}

impl RemoveRewriteMarker {
    fn new(dir: PathBuf) -> Self {
        Self { dir: Some(dir) }
    }

    fn disarm(mut self) {
        self.dir.take();
    }
}

impl Drop for RemoveRewriteMarker {
    fn drop(&mut self) {
        if let Some(dir) = self.dir.take() {
            let _ = remove_rewrite_marker(&dir);
        }
    }
}

fn directory_holds_store(dir: &Path) -> Result<bool> {
    if !dir.exists() {
        return Ok(false);
    }
    if dir.join("schema.lock").exists() {
        return Ok(true);
    }
    for entry in fs::read_dir(dir)? {
        let name = entry?.file_name();
        let name = name.to_string_lossy();
        if name.starts_with("seg-") {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Segment `.dat` and `.dict` files are data. `.idx` and `.zon` files are the index.
fn directory_stored_bytes(dir: &Path) -> Result<StoredBytes> {
    let mut stored_data_bytes = 0u64;
    let mut stored_index_bytes = 0u64;
    if dir.exists() {
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            if !entry.file_type()?.is_file() {
                continue;
            }
            let name = entry.file_name();
            let name = name.to_string_lossy();
            let len = entry.metadata()?.len();
            if name.ends_with(".dat") || name.ends_with(".dict") {
                stored_data_bytes += len;
            } else if name.ends_with(".idx") || name.ends_with(".zon") {
                stored_index_bytes += len;
            }
        }
    }
    Ok(StoredBytes {
        stored_data_bytes,
        stored_index_bytes,
    })
}

fn anchor_path(path: &Path) -> Result<PathBuf> {
    if path.exists() {
        return path.canonicalize().map_err(Error::io);
    }
    let name = path.file_name().ok_or_else(|| {
        Error::io(format!(
            "destination {} has no directory name",
            path.display()
        ))
    })?;
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty());
    let parent = match parent {
        Some(parent) => parent.canonicalize().map_err(Error::io)?,
        None => std::env::current_dir().map_err(Error::io)?,
    };
    Ok(parent.join(name))
}

fn paths_overlap(left: &Path, right: &Path) -> bool {
    left == right || right.starts_with(left) || left.starts_with(right)
}

/// A scratch directory inside an existing destination is the filesystem the
/// caller asked for. Every other overlap with the source or the destination
/// is refused.
fn scratch_overlaps(path: &Path, source: &Path, destination: &Path) -> bool {
    if paths_overlap(path, source) || path == destination {
        return true;
    }
    if destination.is_dir() && path.starts_with(destination) {
        return false;
    }
    paths_overlap(path, destination)
}

fn scratch_bases(source: &Path, destination: &Path) -> Vec<PathBuf> {
    let mut bases = Vec::new();
    if let Some(parent) = destination.parent() {
        if !parent.as_os_str().is_empty() {
            bases.push(parent.to_path_buf());
        }
    }
    if let Some(parent) = source.parent() {
        if !parent.as_os_str().is_empty() && !bases.iter().any(|base| base == parent) {
            bases.push(parent.to_path_buf());
        }
    }
    let temp = std::env::temp_dir();
    if !bases.iter().any(|base| base == &temp) {
        bases.push(temp);
    }
    bases
}

fn is_no_space(err: &Error) -> bool {
    let text = err.to_string();
    text.contains("No space left on device") || text.contains("os error 28")
}

fn write_rewritten_store(
    scratch: &Path,
    source: &Path,
    schema: &Schema,
) -> Result<(PathBuf, StoredBytes)> {
    let staging = scratch.join("staging");
    let schema_file = scratch.join("schema.json");
    fs::write(&schema_file, schema.canonical())?;
    let destination_store = Store::open(&staging, &schema_file)?;
    let copied_rows = segment::visit_stored_rows(source, schema, |row| {
        destination_store.pipeline.append_row(row)
    })?;
    destination_store.flush()?;
    let stats = destination_store.stats();
    if stats.rows != copied_rows {
        return Err(Error::corrupt(
            "rewritten directory row count does not match the source",
        ));
    }
    destination_store.close()?;
    let destination_bytes = directory_stored_bytes(&staging)?;
    if destination_bytes.stored_data_bytes != stats.data_bytes
        || destination_bytes.stored_index_bytes != stats.index_bytes
    {
        return Err(Error::corrupt(
            "rewritten directory file sizes do not match the flushed store",
        ));
    }
    Ok((staging, destination_bytes))
}

fn scratch_dir_in(
    bases: &[PathBuf],
    name: &str,
    source: &Path,
    destination: &Path,
) -> Result<PathBuf> {
    let mut last_error = None;
    for base in bases {
        if !base.exists() {
            continue;
        }
        let base = match base.canonicalize() {
            Ok(path) => path,
            Err(err) => {
                last_error = Some(Error::io(err));
                continue;
            }
        };
        let path = base.join(&name);
        if scratch_overlaps(&path, source, destination) {
            continue;
        }
        match create_private_dir(&path) {
            Ok(()) => return Ok(path),
            Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(err) => last_error = Some(Error::io(err)),
        }
    }
    Err(last_error.unwrap_or_else(|| {
        Error::io("could not create a scratch directory outside the source and the destination")
    }))
}

struct RemoveOnDrop {
    path: Option<PathBuf>,
}

impl RemoveOnDrop {
    fn new(path: PathBuf) -> Self {
        Self { path: Some(path) }
    }

    fn path(&self) -> &Path {
        self.path.as_deref().unwrap_or(Path::new(""))
    }

    fn disarm(&mut self) {
        self.path.take();
    }
}

impl Drop for RemoveOnDrop {
    fn drop(&mut self) {
        if let Some(path) = self.path.take() {
            let _ = fs::remove_dir_all(path);
        }
    }
}

/// Mode 0700 survives umask 022, so a traversable parent cannot list the directory.
fn create_private_dir(path: &Path) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        fs::DirBuilder::new().mode(0o700).create(path)
    }
    #[cfg(not(unix))]
    {
        fs::create_dir(path)
    }
}

fn is_store_file(name: &str) -> bool {
    if name == "schema.lock" {
        return true;
    }
    let Some(rest) = name.strip_prefix("seg-") else {
        return false;
    };
    let Some((id, ext)) = rest.split_once('.') else {
        return false;
    };
    id.len() == 6
        && id.bytes().all(|byte| byte.is_ascii_digit())
        && matches!(ext, "dat" | "idx" | "dict" | "zon")
}

fn fsync_dir(path: &Path) -> Result<()> {
    fs::File::open(path)
        .and_then(|file| file.sync_all())
        .map_err(Error::io)
}

/// Copy store files into `to`, which must already exist and be empty.
/// Each file and `to` itself are fsynced. An extra name in `to` is an error.
fn copy_store_files_synced(from: &Path, to: &Path) -> Result<()> {
    let mut copied = Vec::new();
    for entry in fs::read_dir(from)? {
        let entry = entry?;
        let file_type = entry.file_type()?;
        if file_type.is_dir() {
            continue;
        }
        let file_name = entry.file_name();
        let name = file_name.to_string_lossy();
        if !is_store_file(&name) {
            continue;
        }
        if file_type.is_symlink() {
            return Err(Error::io(format!(
                "refusing to copy symlink {}",
                entry.path().display()
            )));
        }
        if !file_type.is_file() {
            return Err(Error::io(format!(
                "unsupported file in store directory: {}",
                entry.path().display()
            )));
        }
        let target = to.join(&file_name);
        if target.exists() {
            return Err(Error::io(format!(
                "publish directory already contains {}",
                target.display()
            )));
        }
        fs::copy(entry.path(), &target)?;
        fs::File::options()
            .write(true)
            .open(&target)
            .and_then(|file| file.sync_all())
            .map_err(Error::io)?;
        copied.push(file_name);
    }
    for entry in fs::read_dir(to)? {
        let name = entry?.file_name();
        if !copied.iter().any(|copied| copied == &name) {
            return Err(Error::io(format!(
                "publish directory contains {} which was not copied",
                name.to_string_lossy()
            )));
        }
    }
    fsync_dir(to)
}

fn exclusive_publish_dir(destination: &Path) -> Result<PathBuf> {
    let parent = destination
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty());
    let parent = match parent {
        Some(parent) => parent,
        None => {
            return Err(Error::io(format!(
                "destination {} has no parent directory",
                destination.display()
            )))
        }
    };
    let dest_name = destination
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("store");
    for _ in 0..8 {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(Error::io)?
            .as_nanos();
        let sibling = parent.join(format!(
            ".{dest_name}-publish-{}-{nanos}",
            std::process::id()
        ));
        if paths_overlap(&sibling, destination) {
            continue;
        }
        match create_private_dir(&sibling) {
            Ok(()) => return Ok(sibling),
            Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(err) => return Err(Error::io(err)),
        }
    }
    Err(Error::io(
        "could not create an exclusive directory to publish the rewritten store",
    ))
}

fn fsync_file(path: &Path) -> Result<()> {
    fs::File::options()
        .write(true)
        .open(path)
        .and_then(|file| file.sync_all())
        .map_err(Error::io)
}

/// Move finished store files from `staging` into an existing `destination`.
/// `schema.lock` is last. The incomplete-rewrite marker stays until that
/// lock is in place and `destination` has been fsynced, then it is removed.
/// A rename or fsync error puts the published names back into `staging` and
/// leaves the marker. The scratch directory is removed before that marker,
/// so the unfinished copy is gone before open would accept the directory.
/// `rename` of the staging directory itself is not used: onto a mount point
/// it returns `EBUSY`.
fn publish_into_existing_directory(staging: &Path, destination: &Path) -> Result<()> {
    let mut files = Vec::new();
    for entry in fs::read_dir(staging)? {
        let entry = entry?;
        let file_type = entry.file_type()?;
        if file_type.is_dir() {
            continue;
        }
        let file_name = entry.file_name();
        let name = file_name.to_string_lossy();
        if !is_store_file(&name) {
            continue;
        }
        if file_type.is_symlink() {
            return Err(Error::io(format!(
                "refusing to publish symlink {}",
                entry.path().display()
            )));
        }
        if !file_type.is_file() {
            return Err(Error::io(format!(
                "unsupported file in store directory: {}",
                entry.path().display()
            )));
        }
        let target = destination.join(&file_name);
        if target.exists() {
            return Err(Error::io(format!(
                "publish directory already contains {}",
                target.display()
            )));
        }
        fsync_file(&entry.path())?;
        files.push(file_name);
    }
    if !files.iter().any(|name| name == "schema.lock") {
        return Err(Error::io(
            "rewritten store is missing schema.lock and was not published",
        ));
    }
    files.sort_by(|left, right| {
        let left_lock = left == "schema.lock";
        let right_lock = right == "schema.lock";
        left_lock.cmp(&right_lock).then_with(|| left.cmp(right))
    });
    let mut moved = Vec::new();
    for name in &files {
        if rewrite_publish_rename_should_fail() {
            return abort_existing_publish(&moved, staging, destination).and(Err(Error::io(
                "publish rename failed before the rewritten store was complete",
            )));
        }
        match fs::rename(staging.join(name), destination.join(name)) {
            Ok(()) => moved.push(name.clone()),
            Err(err) => {
                return abort_existing_publish(&moved, staging, destination)
                    .and(Err(Error::io(err)));
            }
        }
    }
    if let Err(err) = fsync_dir(destination) {
        return abort_existing_publish(&moved, staging, destination).and(Err(err));
    }
    remove_rewrite_marker(destination)
}

fn abort_existing_publish(
    moved: &[std::ffi::OsString],
    staging: &Path,
    destination: &Path,
) -> Result<()> {
    // The marker stays. `RemoveOnDrop` for the scratch directory runs before
    // `RemoveRewriteMarker`, which unlinks and fsyncs the marker only after
    // that scratch directory is gone.
    rollback_published_files(moved, staging, destination)
}

fn rollback_published_files(
    moved: &[std::ffi::OsString],
    staging: &Path,
    destination: &Path,
) -> Result<()> {
    for name in moved.iter().rev() {
        fs::rename(destination.join(name), staging.join(name)).map_err(Error::io)?;
    }
    Ok(())
}

fn publish_directory(staging: &Path, destination: &Path, destination_existed: bool) -> Result<()> {
    if destination_existed {
        return publish_into_existing_directory(staging, destination);
    }
    if let Some(parent) = destination.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)?;
        }
    }
    let cross_device = cfg!(test) && rewrite_should_copy_across_devices();
    if !cross_device {
        match fs::rename(staging, destination) {
            Ok(()) => return Ok(()),
            Err(err) if err.kind() == std::io::ErrorKind::CrossesDevices => {}
            Err(err) => return Err(Error::io(err)),
        }
    }
    let sibling = exclusive_publish_dir(destination)?;
    let mut cleanup = RemoveOnDrop::new(sibling.clone());
    copy_store_files_synced(staging, &sibling)?;
    if let Some(parent) = sibling.parent() {
        fsync_dir(parent)?;
    }
    fs::rename(&sibling, destination)?;
    cleanup.disarm();
    Ok(())
}

fn write_schema_lock(path: &Path, canonical: &str) -> Result<()> {
    let tmp = path.with_file_name("schema.lock.partial");
    {
        let mut file = fs::File::create(&tmp)?;
        file.write_all(canonical.as_bytes())?;
        file.sync_all()?;
    }
    fs::rename(&tmp, path)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::parse_schema;
    use crate::value::{row_to_json, row_to_json_bytes};
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::thread;
    use std::time::{SystemTime, UNIX_EPOCH};

    static TEMP_SEQ: AtomicU64 = AtomicU64::new(0);

    fn row_value(store: &Store, row: &Row) -> serde_json::Value {
        row_to_json(store.schema(), row).unwrap()
    }

    const SCHEMA_JSON: &str = r#"{
        "timestamp_field": "ts",
        "fields": [
            {"name": "ts", "type": "timestamp"},
            {"name": "user_id", "type": "int"},
            {"name": "score", "type": "float"},
            {"name": "ok", "type": "bool"},
            {"name": "action", "type": "string"},
            {"name": "note", "type": "text"},
            {"name": "amount", "type": "decimal", "scale": 2}
        ]
    }"#;

    struct TempDir(PathBuf);

    impl TempDir {
        fn new() -> Self {
            let n = TEMP_SEQ.fetch_add(1, Ordering::Relaxed);
            let nanos = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            let path = std::env::temp_dir().join(format!(
                "eventer-{}-{}-{}",
                std::process::id(),
                n,
                nanos
            ));
            fs::create_dir_all(&path).unwrap();
            Self(path)
        }

        fn path(&self) -> &Path {
            &self.0
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn write_schema(dir: &Path) -> PathBuf {
        let path = dir.join("schema.json");
        fs::write(&path, SCHEMA_JSON).unwrap();
        path
    }

    fn test_options(block_rows: usize) -> StoreOptions {
        StoreOptions {
            block_rows,
            zstd_level: 1,
            segment_bytes: 64 * 1024 * 1024,
            parser_threads: 2,
            compress_threads: 2,
            linger: Duration::from_secs(60),
        }
    }

    fn event(
        ts: i64,
        user: Option<i64>,
        action: &str,
        note: Option<&str>,
        amount: &str,
    ) -> Vec<u8> {
        let mut obj = serde_json::json!({
            "ts": ts,
            "score": 1.5,
            "ok": user.is_some(),
            "action": action,
            "amount": amount,
        });
        if let Some(user) = user {
            obj["user_id"] = serde_json::json!(user);
        }
        if let Some(note) = note {
            obj["note"] = serde_json::json!(note);
        }
        serde_json::to_vec(&obj).unwrap()
    }

    #[test]
    fn query_is_inclusive_and_skips_blocks_outside_the_range() {
        let dir = TempDir::new();
        let schema = write_schema(dir.path());
        let data = dir.path().join("data");
        let store = Store::open_with(&data, &schema, test_options(2)).unwrap();
        store
            .append_json(&event(1000, Some(1), "click", None, "1.00"))
            .unwrap();
        store
            .append_json(&event(2000, Some(2), "view", Some("a"), "2.50"))
            .unwrap();
        store
            .append_json(&event(3000, None, "buy", Some("b"), "0.05"))
            .unwrap();
        store
            .append_json(&event(4000, Some(4), "click", Some("c"), "8.00"))
            .unwrap();

        let middle = store.query(2000, 3000).unwrap();
        assert_eq!(middle.len(), 2);
        assert_eq!(row_value(&store, &middle[0])["ts"], 2000);
        assert_eq!(row_value(&store, &middle[1])["amount"], "0.05");
        assert!(row_value(&store, &middle[1])["user_id"].is_null());
        assert!(
            row_value(&store, &middle[1])
                .get("note")
                .unwrap()
                .as_str()
                .unwrap()
                == "b"
        );

        let one = store.query(2000, 2000).unwrap();
        assert_eq!(one.len(), 1);
        assert!(store.query(3001, 3002).unwrap().is_empty());
        assert!(store.query(5000, 1000).unwrap().is_empty());

        let stats = store.stats();
        assert_eq!(stats.rows, 4);
        assert_eq!(stats.blocks, 2);
        store.close().unwrap();
    }

    #[test]
    fn query_window_skips_and_limits_in_ingest_order() {
        let dir = TempDir::new();
        let schema = write_schema(dir.path());
        let data = dir.path().join("data");
        let store = Store::open_with(&data, &schema, test_options(2)).unwrap();
        for (ts, action) in [
            (10, "click"),
            (20, "view"),
            (30, "click"),
            (40, "view"),
            (50, "click"),
        ] {
            store
                .append_json(&event(ts, Some(ts), action, None, "1.00"))
                .unwrap();
        }

        let timestamps = |bytes: &[u8]| -> Vec<i64> {
            serde_json::from_slice::<Vec<serde_json::Value>>(bytes)
                .unwrap()
                .into_iter()
                .map(|row| row["ts"].as_i64().unwrap())
                .collect()
        };

        let all = store.query_json_window(10, 50, &[], 0, None).unwrap();
        assert_eq!(timestamps(&all), vec![10, 20, 30, 40, 50]);
        assert_eq!(
            timestamps(&store.query_json_window(10, 50, &[], 0, Some(2)).unwrap()),
            vec![10, 20]
        );
        assert_eq!(
            timestamps(&store.query_json_window(10, 50, &[], 2, Some(2)).unwrap()),
            vec![30, 40]
        );
        assert_eq!(
            timestamps(&store.query_json_window(10, 50, &[], 4, None).unwrap()),
            vec![50]
        );
        assert_eq!(
            store.query_json_window(10, 50, &[], 5, None).unwrap(),
            b"[]"
        );
        assert_eq!(
            timestamps(&store.query_json_window(20, 40, &[], 0, Some(2)).unwrap()),
            vec![20, 30]
        );
        let clicks = store
            .query_json_window(
                10,
                50,
                &[Predicate::Eq("action".into(), "click".into())],
                0,
                Some(2),
            )
            .unwrap();
        assert_eq!(timestamps(&clicks), vec![10, 30]);
        assert!(store
            .query_json_window(10, 50, &[], 0, Some(0))
            .unwrap_err()
            .to_string()
            .contains("positive integer"));

        assert!(store.stats().blocks >= 3);
        store.close().unwrap();

        let reopened = Store::open_with(&data, &schema, test_options(2)).unwrap();
        assert_eq!(
            timestamps(&reopened.query_json_window(10, 50, &[], 0, Some(2)).unwrap()),
            vec![10, 20]
        );
        assert_eq!(
            timestamps(&reopened.query_json_window(10, 50, &[], 2, Some(2)).unwrap()),
            vec![30, 40]
        );
        reopened.close().unwrap();
    }

    #[test]
    fn window_rejects_a_header_row_count_the_payload_does_not_match() {
        let dir = TempDir::new();
        let schema = write_schema(dir.path());
        let data = dir.path().join("data");
        let options = test_options(2);
        let store = Store::open_with(&data, &schema, options.clone()).unwrap();
        store
            .append_json(&event(10, Some(1), "click", None, "1.00"))
            .unwrap();
        store
            .append_json(&event(20, Some(2), "view", None, "1.00"))
            .unwrap();
        store.close().unwrap();

        patch_block_header(&data, 1, None, Some(100));

        let store = Store::open_with(&data, &schema, options).unwrap();
        for (offset, limit) in [(100, None), (0, None), (0, Some(1))] {
            let rows = store.query_window(10, 20, &[], offset, limit).unwrap_err();
            assert!(
                matches!(rows, Error::Corrupt(_)),
                "query_window offset={offset} limit={limit:?}: {rows}"
            );
            let json = store
                .query_json_window(10, 20, &[], offset, limit)
                .unwrap_err();
            assert!(
                matches!(json, Error::Corrupt(_)),
                "query_json_window offset={offset} limit={limit:?}: {json}"
            );
        }
        let count = store.count(10, 20).unwrap_err();
        assert!(matches!(count, Error::Corrupt(_)), "count: {count}");
        store.close().unwrap();
    }

    #[test]
    fn short_page_refuses_an_uncompressed_len_above_the_query_ceiling() {
        let dir = TempDir::new();
        let schema = write_schema(dir.path());
        let data = dir.path().join("data");
        let options = test_options(2);
        let store = Store::open_with(&data, &schema, options.clone()).unwrap();
        store
            .append_json(&event(10, Some(1), "click", None, "1.00"))
            .unwrap();
        store
            .append_json(&event(20, Some(2), "view", None, "1.00"))
            .unwrap();
        store.close().unwrap();

        patch_block_header(&data, 1, Some(u32::MAX), None);

        let store = Store::open_with(&data, &schema, options).unwrap();
        let rows = store.query_window(10, 20, &[], 0, Some(1)).unwrap_err();
        assert!(
            rows.to_string()
                .contains("query response size limit exceeded"),
            "{rows}"
        );
        let json = store
            .query_json_window(10, 20, &[], 0, Some(1))
            .unwrap_err();
        assert!(
            json.to_string()
                .contains("query response size limit exceeded"),
            "{json}"
        );
        let count = store.count(10, 20).unwrap_err();
        assert!(
            count
                .to_string()
                .contains("query response size limit exceeded"),
            "{count}"
        );
        store.close().unwrap();
    }

    #[test]
    fn projected_scan_ignores_unread_columns_in_the_response_budget() {
        let dir = TempDir::new();
        let schema = write_schema(dir.path());
        let data = dir.path().join("data");
        let options = test_options(2);
        let store = Store::open_with(&data, &schema, options.clone()).unwrap();
        store
            .append_json(&event(10, Some(1), "click", None, "1.00"))
            .unwrap();
        store
            .append_json(&event(20, Some(2), "view", None, "1.00"))
            .unwrap();
        store.close().unwrap();

        patch_block_header(&data, 1, Some(MAX_QUERY_BYTES as u32), None);

        let store = Store::open_with(&data, &schema, options.clone()).unwrap();
        let full = store.query_json_window(10, 20, &[], 0, None).unwrap_err();
        assert!(
            full.to_string()
                .contains("query response size limit exceeded"),
            "{full}"
        );
        let projected = store
            .query_json_columns(10, 20, &[], 0, None, &[0])
            .unwrap();
        assert_eq!(projected, br#"[{"ts":10},{"ts":20}]"#);
        store.close().unwrap();

        patch_block_header(&data, 1, Some(MAX_QUERY_BYTES as u32 + 1), None);
        let store = Store::open_with(&data, &schema, options).unwrap();
        let over = store
            .query_json_columns(10, 20, &[], 0, None, &[0])
            .unwrap_err();
        assert!(
            over.to_string()
                .contains("query response size limit exceeded"),
            "{over}"
        );
        store.close().unwrap();
    }

    #[test]
    fn window_does_not_skip_a_block_whose_index_range_hides_an_outside_timestamp() {
        let dir = TempDir::new();
        let schema = write_schema(dir.path());
        let data = dir.path().join("data");
        let options = test_options(3);
        let store = Store::open_with(&data, &schema, options.clone()).unwrap();
        for ts in [10, 20, 30, 22] {
            store
                .append_json(&event(ts, Some(ts), "click", None, "1.00"))
                .unwrap();
        }
        store.close().unwrap();

        patch_index_minmax(&data, 1, 10, 20);

        let store = Store::open_with(&data, &schema, options).unwrap();
        let timestamps = |bytes: &[u8]| -> Vec<i64> {
            serde_json::from_slice::<Vec<serde_json::Value>>(bytes)
                .unwrap()
                .into_iter()
                .map(|row| row["ts"].as_i64().unwrap())
                .collect()
        };
        let unpaged = timestamps(&store.query_json_window(10, 25, &[], 0, None).unwrap());
        assert_eq!(unpaged, vec![10, 20, 22]);
        assert_eq!(
            timestamps(&store.query_json_window(10, 25, &[], 3, Some(1)).unwrap()),
            Vec::<i64>::new()
        );
        let rows = store.query_window(10, 25, &[], 3, Some(1)).unwrap();
        assert!(rows.is_empty());
        store.close().unwrap();
    }

    /// Rewrite uncompressed length and row count in the index. A 12-byte frame
    /// does not store them, and open keeps the index values for that frame.
    fn patch_block_header(
        dir: &Path,
        segment_id: u32,
        uncompressed_len: Option<u32>,
        row_count: Option<u32>,
    ) {
        let data_path = segment::data_path(dir, segment_id);
        let data = fs::read(&data_path).unwrap();
        assert_eq!(&data[..4], segment::BLOCK_MAGIC);

        let index_path = segment::index_path(dir, segment_id);
        let mut blocks = segment::read_index(&index_path).unwrap();
        assert!(!blocks.is_empty());
        if let Some(len) = uncompressed_len {
            blocks[0].uncompressed_len = len;
        }
        if let Some(rows) = row_count {
            blocks[0].row_count = rows;
        }
        fs::write(&index_path, segment::encode_index(&blocks)).unwrap();
    }

    /// A 20-byte frame does not store min/max. Open keeps the index values when
    /// the other header fields match, so a lying zone can hide an outside timestamp.
    fn patch_index_minmax(dir: &Path, segment_id: u32, min_ts: i64, max_ts: i64) {
        let index_path = segment::index_path(dir, segment_id);
        let mut blocks = segment::read_index(&index_path).unwrap();
        assert!(!blocks.is_empty());
        blocks[0].min_ts = min_ts;
        blocks[0].max_ts = max_ts;
        fs::write(&index_path, segment::encode_index(&blocks)).unwrap();
    }

    #[test]
    fn reopening_rebuilds_a_truncated_index() {
        let dir = TempDir::new();
        let schema = write_schema(dir.path());
        let data = dir.path().join("data");
        let store = Store::open_with(&data, &schema, test_options(8)).unwrap();
        for i in 0..10 {
            store
                .append_json(&event(1_000 + i, Some(i), "click", Some("n"), "3.25"))
                .unwrap();
        }
        store.close().unwrap();

        let data_file = segment::data_path(&data, 1);
        let segment_bytes = fs::read(&data_file).unwrap();
        let frames = segment::frames_in(&segment_bytes);
        assert!(
            frames.len() >= 2,
            "expected more than one block, got {}",
            frames.len()
        );
        assert!(
            frames.iter().all(|frame| {
                frame.header_len == segment::BLOCK_HEADER_LEN && frame.min_ts.is_none()
            }),
            "new frames must omit min_ts and max_ts"
        );

        let index = segment::index_path(&data, 1);
        fs::write(&index, b"EVIX").unwrap();
        let schema_text = parse_schema(SCHEMA_JSON).unwrap();
        assert!(schema_text.canonical().contains("timestamp"));

        let store = Store::open_with(&data, &schema, test_options(8)).unwrap();
        let rows = store.query(0, 10_000).unwrap();
        assert_eq!(rows.len(), 10);
        assert_eq!(row_value(&store, &rows[9])["user_id"], 9);
        assert_eq!(
            fs::read(&data_file).unwrap(),
            segment_bytes,
            "rebuilding the index must not write timestamps back into the segment"
        );
        let indexed = segment::read_index(&segment::index_path(&data, 1)).unwrap();
        assert_eq!(indexed.len(), frames.len());
        assert_eq!(indexed[0].min_ts, 1_000);
        assert_eq!(indexed.last().unwrap().max_ts, 1_009);
        store.close().unwrap();
    }

    #[test]
    fn compressed_index_reopens_and_raw_index_still_loads() {
        let dir = TempDir::new();
        let schema = write_schema(dir.path());
        let data = dir.path().join("data");
        let store = Store::open_with(&data, &schema, test_options(8)).unwrap();
        let count = 80i64;
        for i in 0..count {
            store
                .append_json(&event(i, Some(i), "click", Some("n"), "3.25"))
                .unwrap();
        }
        store.flush().unwrap();
        assert_eq!(store.query(0, count).unwrap().len(), count as usize);
        let index_on_disk = dir_suffix_bytes(&data, ".idx") + dir_suffix_bytes(&data, ".zon");
        assert_eq!(store.stats().index_bytes, index_on_disk);
        store.drop_blocks_before(i64::MIN).unwrap();
        assert_eq!(
            store.stats().index_bytes,
            index_on_disk,
            "a no-op retention pass still counts the compressed index"
        );
        store.close().unwrap();

        let index = segment::index_path(&data, 1);
        let compressed = fs::read(&index).unwrap();
        assert_eq!(&compressed[..4], &[0x28, 0xB5, 0x2F, 0xFD]);
        let raw = zstd::stream::decode_all(compressed.as_slice()).unwrap();
        assert!(compressed.len() < raw.len());
        assert_eq!(&raw[..4], b"EVIX");

        let reopened = Store::open_with(&data, &schema, test_options(8)).unwrap();
        assert_eq!(reopened.query(0, count).unwrap().len(), count as usize);
        assert_eq!(reopened.stats().index_bytes, index_on_disk);
        reopened.close().unwrap();

        fs::write(&index, &raw).unwrap();
        let legacy = Store::open_with(&data, &schema, test_options(8)).unwrap();
        assert_eq!(legacy.query(0, count).unwrap().len(), count as usize);
        assert_eq!(
            fs::read(&index).unwrap(),
            raw,
            "a raw EVIX file from an older writer is left in place"
        );
        legacy.close().unwrap();

        let level1 = zstd::bulk::compress(&raw, 1).unwrap();
        assert!(level1.len() < raw.len());
        assert_eq!(&level1[..4], &[0x28, 0xB5, 0x2F, 0xFD]);
        fs::write(&index, &level1).unwrap();
        let legacy = Store::open_with(&data, &schema, test_options(8)).unwrap();
        assert_eq!(legacy.query(0, count).unwrap().len(), count as usize);
        assert_eq!(
            fs::read(&index).unwrap(),
            level1,
            "a level-1 index frame from an older writer is left in place"
        );
        legacy.close().unwrap();

        let blocks = segment::read_index(&index).unwrap();
        let mut version_one = Vec::with_capacity(raw.len());
        version_one.extend_from_slice(segment::INDEX_MAGIC);
        version_one.extend_from_slice(&1u16.to_le_bytes());
        version_one.extend_from_slice(&0u16.to_le_bytes());
        for block in &blocks {
            version_one.extend_from_slice(&segment::index_entry_bytes(block));
        }
        fs::write(&index, &version_one).unwrap();
        let opened = Store::open_with(&data, &schema, test_options(8)).unwrap();
        assert_eq!(opened.query(0, count).unwrap().len(), count as usize);
        assert_eq!(
            fs::read(&index).unwrap(),
            version_one,
            "a raw version-1 index is left in place"
        );
        opened.close().unwrap();

        fs::write(&index, &compressed[..compressed.len() - 1]).unwrap();
        let rebuilt = Store::open_with(&data, &schema, test_options(8)).unwrap();
        assert_eq!(rebuilt.query(0, count).unwrap().len(), count as usize);
        let rebuilt_bytes = fs::read(&index).unwrap();
        assert_ne!(rebuilt_bytes, compressed[..compressed.len() - 1]);
        rebuilt.close().unwrap();
        let used = Store::open_with(&data, &schema, test_options(8)).unwrap();
        assert_eq!(used.query(0, count).unwrap().len(), count as usize);
        assert_eq!(
            fs::read(&index).unwrap(),
            rebuilt_bytes,
            "the rebuilt index is reused"
        );
        used.close().unwrap();

        fs::remove_file(&index).unwrap();
        let restored = Store::open_with(&data, &schema, test_options(8)).unwrap();
        assert_eq!(restored.query(0, count).unwrap().len(), count as usize);
        assert!(index.exists());
        restored.close().unwrap();
    }

    #[test]
    fn schema_mismatch_is_rejected() {
        let dir = TempDir::new();
        let schema = write_schema(dir.path());
        let data = dir.path().join("data");
        let store = Store::open_with(&data, &schema, test_options(8)).unwrap();
        store.close().unwrap();
        let other = dir.path().join("other.json");
        fs::write(
            &other,
            r#"{"timestamp_field":"ts","fields":[{"name":"ts","type":"timestamp"},{"name":"n","type":"int"}]}"#,
        )
        .unwrap();
        match Store::open_with(&data, &other, test_options(8)) {
            Err(err) => assert!(err.to_string().contains("schema.lock"), "{err}"),
            Ok(_store) => panic!("opened a directory with a mismatched schema"),
        }
    }

    const NARROW_SCHEMA: &str = r#"{
        "timestamp_field": "ts",
        "fields": [
            {"name": "ts", "type": "timestamp"},
            {"name": "user_id", "type": "int"},
            {"name": "event_time", "type": "timestamp"},
            {"name": "amount", "type": "decimal", "scale": 2}
        ]
    }"#;

    const WIDE_SCHEMA: &str = r#"{
        "timestamp_field": "ts",
        "fields": [
            {"name": "ts", "type": "timestamp"},
            {"name": "user_id", "type": "int"},
            {"name": "event_time", "type": "timestamp"},
            {"name": "amount", "type": "decimal", "scale": 2},
            {"name": "region", "type": "string"},
            {"name": "extra", "type": "int"}
        ]
    }"#;

    #[test]
    fn appended_columns_read_null_on_old_rows_and_roundtrip_on_new_rows() {
        let dir = TempDir::new();
        let narrow = dir.path().join("narrow.json");
        fs::write(&narrow, NARROW_SCHEMA).unwrap();
        let data = dir.path().join("data");
        let store = Store::open_with(&data, &narrow, test_options(2)).unwrap();
        store
            .append_json(br#"{"ts":1000,"user_id":7,"amount":"1.50"}"#)
            .unwrap();
        store
            .append_json(br#"{"ts":2000,"user_id":8,"event_time":1500,"amount":"2.00"}"#)
            .unwrap();
        store.close().unwrap();

        let segment = segment::data_path(&data, 1);
        let segment_before = fs::read(&segment).unwrap();
        let wide = dir.path().join("wide.json");
        fs::write(&wide, WIDE_SCHEMA).unwrap();
        let store = Store::open_with(&data, &wide, test_options(2)).unwrap();
        assert_eq!(
            fs::read(&segment).unwrap(),
            segment_before,
            "opening with appended columns rewrote a segment file"
        );
        let locked = fs::read_to_string(data.join("schema.lock")).unwrap();
        assert_eq!(locked, parse_schema(WIDE_SCHEMA).unwrap().canonical());

        let old = store.query(0, 10_000).unwrap();
        assert_eq!(old.len(), 2);
        for (row, user_id, amount) in [(&old[0], 7, "1.50"), (&old[1], 8, "2.00")] {
            let value = row_value(&store, row);
            assert_eq!(value["user_id"], user_id);
            assert_eq!(value["amount"], amount);
            assert!(value["region"].is_null());
            assert!(value["extra"].is_null());
        }
        assert!(row_value(&store, &old[0])["event_time"].is_null());
        assert_eq!(row_value(&store, &old[1])["event_time"], 1500);

        let only_null_region = store
            .query_with_filter(0, 10_000, &[Predicate::Eq("region".into(), Scalar::Null)])
            .unwrap();
        assert_eq!(only_null_region.len(), 2);
        let west = store
            .query_with_filter(0, 10_000, &[Predicate::Eq("region".into(), "west".into())])
            .unwrap();
        assert!(west.is_empty());

        store
            .append_json(br#"{"ts":3000,"user_id":9,"amount":"3.25","region":"west","extra":4}"#)
            .unwrap();
        store.close().unwrap();
        let segment_after = fs::read(&segment).unwrap();
        assert!(
            segment_after.starts_with(&segment_before),
            "the new row rewrote blocks stored before the added columns"
        );

        let store = Store::open_with(&data, &wide, test_options(2)).unwrap();
        assert_eq!(
            fs::read_to_string(data.join("schema.lock")).unwrap(),
            locked
        );
        let rows = store.query(0, 10_000).unwrap();
        assert_eq!(rows.len(), 3);
        assert!(row_value(&store, &rows[0])["region"].is_null());
        assert!(row_value(&store, &rows[0])["extra"].is_null());
        assert_eq!(row_value(&store, &rows[0])["user_id"], 7);
        assert_eq!(row_value(&store, &rows[2])["region"], "west");
        assert_eq!(row_value(&store, &rows[2])["extra"], 4);
        assert_eq!(row_value(&store, &rows[2])["amount"], "3.25");
        let west = store
            .query_with_filter(0, 10_000, &[Predicate::Eq("region".into(), "west".into())])
            .unwrap();
        assert_eq!(west.len(), 1);
        assert_eq!(row_value(&store, &west[0])["user_id"], 9);
        store.close().unwrap();
    }

    #[test]
    fn schema_edits_other_than_append_are_rejected() {
        let dir = TempDir::new();
        let narrow = dir.path().join("narrow.json");
        fs::write(&narrow, NARROW_SCHEMA).unwrap();
        let data = dir.path().join("data");
        let store = Store::open_with(&data, &narrow, test_options(2)).unwrap();
        store
            .append_json(br#"{"ts":1000,"user_id":1,"amount":"1.00"}"#)
            .unwrap();
        store.close().unwrap();
        let lock_before = fs::read(data.join("schema.lock")).unwrap();

        let rejected = [
            (
                "type",
                r#"{
                    "timestamp_field": "ts",
                    "fields": [
                        {"name": "ts", "type": "timestamp"},
                        {"name": "user_id", "type": "float"},
                        {"name": "event_time", "type": "timestamp"},
                        {"name": "amount", "type": "decimal", "scale": 2}
                    ]
                }"#,
            ),
            (
                "removed",
                r#"{
                    "timestamp_field": "ts",
                    "fields": [
                        {"name": "ts", "type": "timestamp"},
                        {"name": "user_id", "type": "int"},
                        {"name": "event_time", "type": "timestamp"}
                    ]
                }"#,
            ),
            (
                "reordered",
                r#"{
                    "timestamp_field": "ts",
                    "fields": [
                        {"name": "ts", "type": "timestamp"},
                        {"name": "event_time", "type": "timestamp"},
                        {"name": "user_id", "type": "int"},
                        {"name": "amount", "type": "decimal", "scale": 2}
                    ]
                }"#,
            ),
            (
                "renamed",
                r#"{
                    "timestamp_field": "ts",
                    "fields": [
                        {"name": "ts", "type": "timestamp"},
                        {"name": "uid", "type": "int"},
                        {"name": "event_time", "type": "timestamp"},
                        {"name": "amount", "type": "decimal", "scale": 2}
                    ]
                }"#,
            ),
            (
                "scale",
                r#"{
                    "timestamp_field": "ts",
                    "fields": [
                        {"name": "ts", "type": "timestamp"},
                        {"name": "user_id", "type": "int"},
                        {"name": "event_time", "type": "timestamp"},
                        {"name": "amount", "type": "decimal", "scale": 4}
                    ]
                }"#,
            ),
            (
                "timestamp",
                r#"{
                    "timestamp_field": "event_time",
                    "fields": [
                        {"name": "ts", "type": "timestamp"},
                        {"name": "user_id", "type": "int"},
                        {"name": "event_time", "type": "timestamp"},
                        {"name": "amount", "type": "decimal", "scale": 2}
                    ]
                }"#,
            ),
        ];
        for (reason, text) in rejected {
            let path = dir.path().join(format!("{reason}.json"));
            fs::write(&path, text).unwrap();
            match Store::open_with(&data, &path, test_options(2)) {
                Err(err) => assert!(err.to_string().contains("schema.lock"), "{reason}: {err}"),
                Ok(_store) => panic!("opened after a {reason} schema change"),
            }
        }
        assert_eq!(fs::read(data.join("schema.lock")).unwrap(), lock_before);
    }

    #[test]
    fn rotates_segments_and_keeps_every_row() {
        let dir = TempDir::new();
        let schema = write_schema(dir.path());
        let data = dir.path().join("data");
        let mut options = test_options(4);
        options.segment_bytes = 1;
        let store = Store::open_with(&data, &schema, options).unwrap();
        for i in 0..12 {
            store
                .append_json(&event(i, Some(i), "view", Some("repeat"), "1.00"))
                .unwrap();
        }
        store.flush().unwrap();
        let stats = store.stats();
        assert_eq!(stats.rows, 12);
        assert!(stats.segments >= 2, "segments={}", stats.segments);
        assert_eq!(store.query(0, 100).unwrap().len(), 12);
        store.close().unwrap();
    }

    #[test]
    fn parallel_appends_survive_one_flush() {
        let dir = TempDir::new();
        let schema = write_schema(dir.path());
        let data = dir.path().join("data");
        let store = Arc::new(Store::open_with(&data, &schema, test_options(32)).unwrap());
        let mut joins = Vec::new();
        for worker in 0..4 {
            let store = Arc::clone(&store);
            joins.push(thread::spawn(move || {
                for i in 0..25 {
                    let ts = worker * 1000 + i;
                    store
                        .append_json(&event(ts, Some(ts), "buy", Some("t"), "9.99"))
                        .unwrap();
                }
            }));
        }
        for join in joins {
            join.join().unwrap();
        }
        store.flush().unwrap();
        assert_eq!(store.query(i64::MIN, i64::MAX).unwrap().len(), 100);
        store.close().unwrap();
    }

    #[test]
    fn repetitive_events_use_fewer_bytes_than_json() {
        let dir = TempDir::new();
        let schema = write_schema(dir.path());
        let data = dir.path().join("data");
        let mut options = test_options(256);
        options.zstd_level = 3;
        let store = Store::open_with(&data, &schema, options).unwrap();
        let mut raw = 0u64;
        for i in 0..2000i64 {
            let json = event(
                1_700_000_000_000 + i,
                Some(i % 50),
                "click",
                Some("hello"),
                "19.99",
            );
            raw += json.len() as u64;
            store.append_json(&json).unwrap();
        }
        store.flush().unwrap();
        let stats = store.stats();
        assert_eq!(stats.rows, 2000);
        assert!(
            stats.data_bytes * 2 < raw,
            "stored {} raw {}",
            stats.data_bytes,
            raw
        );
        store.close().unwrap();
    }

    #[test]
    fn json_field_survives_flush_and_query() {
        let dir = TempDir::new();
        let schema = dir.path().join("schema.json");
        fs::write(
            &schema,
            r#"{
                "timestamp_field": "ts",
                "fields": [
                    {"name": "ts", "type": "timestamp"},
                    {"name": "props", "type": "json"}
                ]
            }"#,
        )
        .unwrap();
        let data = dir.path().join("data");
        let store = Store::open_with(&data, &schema, test_options(8)).unwrap();
        store
            .append_json(br#"{"ts":10,"props":{"user":{"id":7},"tags":["a","b"]}}"#)
            .unwrap();
        store
            .append_json(br#"{"ts":20,"props":[1,{"n":2},"z"]}"#)
            .unwrap();
        store.append_json(br#"{"ts":30,"props":"solo"}"#).unwrap();
        store.append_json(br#"{"ts":40}"#).unwrap();
        let rows = store.query(10, 40).unwrap();
        assert_eq!(rows.len(), 4);
        assert_eq!(row_value(&store, &rows[0])["props"]["user"]["id"], 7);
        assert_eq!(row_value(&store, &rows[0])["props"]["tags"][1], "b");
        assert_eq!(row_value(&store, &rows[1])["props"][0], 1);
        assert_eq!(row_value(&store, &rows[1])["props"][1]["n"], 2);
        assert_eq!(row_value(&store, &rows[2])["props"], "solo");
        assert!(row_value(&store, &rows[3])["props"].is_null());
        store.close().unwrap();

        let reopened = Store::open_with(&data, &schema, test_options(8)).unwrap();
        let again = reopened.query(10, 10).unwrap();
        assert_eq!(row_value(&reopened, &again[0])["props"]["user"]["id"], 7);
        reopened.close().unwrap();
    }

    #[test]
    fn json_field_preserves_query_bytes() {
        let dir = TempDir::new();
        let schema = dir.path().join("schema.json");
        fs::write(
            &schema,
            r#"{
                "timestamp_field": "ts",
                "fields": [
                    {"name": "ts", "type": "timestamp"},
                    {"name": "props", "type": "json"}
                ]
            }"#,
        )
        .unwrap();
        let data = dir.path().join("data");
        let store = Store::open_with(&data, &schema, test_options(8)).unwrap();
        let cases: &[&[u8]] = &[
            br#"{"ts":1,"props":9007199254740993.0}"#,
            br#"{"ts":2,"props":18446744073709551617}"#,
            br#"{"ts":3,"props":{"a":1,"a":2,"b":{"z":1,"z":3}}}"#,
        ];
        for input in cases {
            store.append_json(input).unwrap();
        }
        store.flush().unwrap();
        for input in cases {
            let ts = serde_json::from_slice::<serde_json::Value>(input).unwrap()["ts"]
                .as_i64()
                .unwrap();
            let out = store.query_json(ts, ts).unwrap();
            assert_eq!(
                out,
                format!("[{}]", String::from_utf8(input.to_vec()).unwrap()).as_bytes()
            );
        }
        store.close().unwrap();
    }

    #[test]
    fn query_does_not_reject_block_of_small_events() {
        let dir = TempDir::new();
        let schema = write_schema(dir.path());
        let data = dir.path().join("data");
        let mut options = test_options(128);
        options.block_rows = 128;
        let store = Store::open_with(&data, &schema, options).unwrap();
        for ts in 1..=80 {
            store
                .append_json(&event(ts, Some(ts), "click", None, "1.00"))
                .unwrap();
        }
        store.flush().unwrap();
        let rows = store.query(1, 80).unwrap();
        assert_eq!(rows.len(), 80);
        let json = store.query_json(1, 80).unwrap();
        assert!(json.len() < MAX_QUERY_BYTES);
        store.close().unwrap();
    }

    #[test]
    fn oversized_json_block_is_split_and_every_row_is_readable() {
        let dir = TempDir::new();
        let schema = dir.path().join("schema.json");
        fs::write(
            &schema,
            r#"{
                "timestamp_field": "ts",
                "fields": [
                    {"name": "ts", "type": "timestamp"},
                    {"name": "props", "type": "json"}
                ]
            }"#,
        )
        .unwrap();
        let data = dir.path().join("data");
        let store = Store::open_with(&data, &schema, test_options(80)).unwrap();
        store
            .append_json(br#"{"ts":1,"props":{"ok":true}}"#)
            .unwrap();
        store.flush().unwrap();

        let body = "x".repeat(890 * 1024);
        for ts in 2..=101 {
            let event = format!(r#"{{"ts":{ts},"props":{{"n":{ts},"body":"{body}"}}}}"#);
            store.append_json(event.as_bytes()).unwrap();
        }
        store.flush().unwrap();
        let stats = store.stats();
        assert_eq!(stats.rows, 101);
        assert!(stats.blocks >= 3);
        for ts in [1, 2, 81, 82, 101] {
            assert_eq!(store.query(ts, ts).unwrap().len(), 1, "ts {ts}");
        }
        store.close().unwrap();

        let reopened = Store::open_with(&data, &schema, test_options(80)).unwrap();
        assert_eq!(reopened.stats().rows, 101);
        for ts in [1, 2, 81, 82, 101] {
            assert_eq!(reopened.query(ts, ts).unwrap().len(), 1, "ts {ts}");
        }
        reopened.close().unwrap();
    }

    #[test]
    fn json_field_preserves_store_query() {
        let dir = TempDir::new();
        let schema = dir.path().join("schema.json");
        fs::write(
            &schema,
            r#"{
                "timestamp_field": "ts",
                "fields": [
                    {"name": "ts", "type": "timestamp"},
                    {"name": "props", "type": "json"}
                ]
            }"#,
        )
        .unwrap();
        let data = dir.path().join("data");
        let store = Store::open_with(&data, &schema, test_options(8)).unwrap();
        let schema_model = parse_schema(
            r#"{"timestamp_field":"ts","fields":[{"name":"ts","type":"timestamp"},{"name":"props","type":"json"}]}"#,
        )
        .unwrap();
        let number = br#"{"ts":1,"props":9007199254740993.0}"#;
        let dup_keys = br#"{"ts":3,"props":{"a":1,"a":2,"b":{"z":1,"z":3}}}"#;
        store.append_json(number).unwrap();
        store.append_json(dup_keys).unwrap();
        store.flush().unwrap();

        let rows = store.query(1, 1).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(row_to_json_bytes(&schema_model, &rows[0]).unwrap(), number);

        let json_out = store.query_json(3, 3).unwrap();
        assert_eq!(
            json_out,
            format!("[{}]", String::from_utf8(dup_keys.to_vec()).unwrap()).as_bytes()
        );
        let rows = store.query(3, 3).unwrap();
        assert_eq!(
            row_to_json_bytes(&schema_model, &rows[0]).unwrap(),
            dup_keys
        );
        store.close().unwrap();
    }

    #[test]
    fn query_with_filter_pushes_equality_and_in_predicates() {
        let dir = TempDir::new();
        let schema = write_schema(dir.path());
        let data = dir.path().join("data");
        let store = Store::open_with(&data, &schema, test_options(2)).unwrap();
        store
            .append_json(&event(1000, Some(1), "click", Some("alpha"), "1.00"))
            .unwrap();
        store
            .append_json(&event(2000, Some(2), "view", Some("beta"), "2.50"))
            .unwrap();
        store
            .append_json(&event(3000, None, "buy", Some("alpha"), "0.05"))
            .unwrap();
        store
            .append_json(&event(4000, Some(4), "click", None, "8.00"))
            .unwrap();

        let clicks = store
            .query_with_filter(0, 10_000, &[Predicate::Eq("action".into(), "click".into())])
            .unwrap();
        assert_eq!(clicks.len(), 2);
        assert_eq!(row_value(&store, &clicks[0])["ts"], 1000);
        assert_eq!(row_value(&store, &clicks[1])["ts"], 4000);
        assert!(row_value(&store, &clicks[1])["note"].is_null());

        let narrowed = store
            .query_with_filter(
                1500,
                4000,
                &[Predicate::Eq("action".into(), "click".into())],
            )
            .unwrap();
        assert_eq!(narrowed.len(), 1);
        assert_eq!(row_value(&store, &narrowed[0])["user_id"], 4);

        let either = store
            .query_with_filter(
                0,
                10_000,
                &[Predicate::In(
                    "action".into(),
                    vec!["view".into(), "buy".into()],
                )],
            )
            .unwrap();
        assert_eq!(either.len(), 2);
        assert_eq!(row_value(&store, &either[0])["action"], "view");
        assert_eq!(row_value(&store, &either[1])["action"], "buy");

        let both = store
            .query_with_filter(
                0,
                10_000,
                &[
                    Predicate::Eq("action".into(), "buy".into()),
                    Predicate::Eq("note".into(), "alpha".into()),
                ],
            )
            .unwrap();
        assert_eq!(both.len(), 1);
        assert_eq!(row_value(&store, &both[0])["ts"], 3000);

        let null_user = store
            .query_with_filter(0, 10_000, &[Predicate::Eq("user_id".into(), Scalar::Null)])
            .unwrap();
        assert_eq!(null_user.len(), 1);
        assert_eq!(row_value(&store, &null_user[0])["action"], "buy");

        assert!(store
            .query_with_filter(0, 10_000, &[Predicate::In("action".into(), Vec::new())])
            .unwrap()
            .is_empty());
        assert!(store
            .query_with_filter(
                0,
                10_000,
                &[Predicate::Eq("action".into(), "missing".into())],
            )
            .unwrap()
            .is_empty());

        let unknown =
            store.query_with_filter(0, 10_000, &[Predicate::Eq("type".into(), "click".into())]);
        assert!(unknown
            .unwrap_err()
            .to_string()
            .contains("unknown filter field"));

        let wrong = store.query_with_filter(
            0,
            10_000,
            &[Predicate::Eq("user_id".into(), "click".into())],
        );
        assert!(wrong
            .unwrap_err()
            .to_string()
            .contains("does not match type"));

        let disagree = store.query_with_filter(
            0,
            10_000,
            &[
                Predicate::Eq("action".into(), "click".into()),
                Predicate::Eq("action".into(), "view".into()),
            ],
        );
        assert!(disagree.unwrap().is_empty());

        let same = store
            .query_with_filter(
                0,
                10_000,
                &[
                    Predicate::Eq("action".into(), "click".into()),
                    Predicate::In("action".into(), vec!["click".into(), "view".into()]),
                ],
            )
            .unwrap();
        assert_eq!(same.len(), 2);
        store.close().unwrap();
    }

    #[test]
    fn range_filters_compare_numbers_and_skip_a_low_integer_zone() {
        let dir = TempDir::new();
        let schema = write_schema(dir.path());
        let data = dir.path().join("data");
        let store = Store::open_with(&data, &schema, test_options(8)).unwrap();
        for (ts, user, amount, score) in [
            (1000i64, Some(1i64), "1.00", 1.0f64),
            (2000, Some(5), "5.00", 5.0),
            (3000, Some(9), "9.00", 9.0),
            (4000, None, "5.00", 0.0),
        ] {
            let mut obj = serde_json::json!({
                "ts": ts,
                "score": score,
                "ok": user.is_some(),
                "action": "click",
                "amount": amount,
            });
            if let Some(user) = user {
                obj["user_id"] = serde_json::json!(user);
            }
            store
                .append_json(&serde_json::to_vec(&obj).unwrap())
                .unwrap();
        }
        store.flush().unwrap();

        let ids = |op, bound: i64| -> Vec<Option<i64>> {
            store
                .query_with_filter(
                    1000,
                    4000,
                    &[Predicate::Cmp("user_id".into(), op, Scalar::Int(bound))],
                )
                .unwrap()
                .into_iter()
                .map(|row| match &row.values[1] {
                    Scalar::Int(value) => Some(*value),
                    Scalar::Null => None,
                    other => panic!("user_id {other:?}"),
                })
                .collect()
        };
        assert_eq!(
            ids(CmpOp::Ge, 5),
            vec![Some(5), Some(9)],
            "ge includes the bound and every larger int"
        );
        assert_eq!(ids(CmpOp::Gt, 5), vec![Some(9)]);
        assert_eq!(ids(CmpOp::Le, 5), vec![Some(1), Some(5)]);
        assert_eq!(ids(CmpOp::Lt, 5), vec![Some(1)]);

        let decimal = crate::schema::FieldType::Decimal { scale: 2 };
        let amount = crate::value::scalar_from_literal(decimal, "5.00").unwrap();
        let below = store
            .query_with_filter(
                1000,
                4000,
                &[Predicate::Cmp("amount".into(), CmpOp::Lt, amount)],
            )
            .unwrap();
        assert_eq!(below.len(), 1);
        assert_eq!(row_value(&store, &below[0])["amount"], "1.00");

        let above_score = store
            .query_with_filter(
                1000,
                4000,
                &[Predicate::Cmp(
                    "score".into(),
                    CmpOp::Gt,
                    Scalar::Float(5.0),
                )],
            )
            .unwrap();
        assert_eq!(above_score.len(), 1);
        assert_eq!(
            row_value(&store, &above_score[0])["score"].as_f64(),
            Some(9.0)
        );

        let ge = [Predicate::Cmp("user_id".into(), CmpOp::Ge, Scalar::Int(5))];
        assert_eq!(store.count_with_filter(1000, 4000, &ge).unwrap(), 2);
        assert_eq!(
            store
                .histogram_with_filter(1000, 4000, 1000, &ge)
                .unwrap()
                .iter()
                .map(|bucket| bucket.count)
                .sum::<u64>(),
            2
        );
        let page = store.query_window(1000, 4000, &ge, 1, Some(1)).unwrap();
        assert_eq!(page.len(), 1);
        assert_eq!(row_value(&store, &page[0])["user_id"], 9);

        let bad = store.query_with_filter(
            1000,
            4000,
            &[Predicate::Cmp("action".into(), CmpOp::Gt, "a".into())],
        );
        assert!(matches!(bad, Err(Error::Schema(_))), "{bad:?}");
        assert_eq!(store.count(1000, 4000).unwrap(), 4);
        store.close().unwrap();

        let low_dir = TempDir::new();
        let schema = write_schema(low_dir.path());
        let data = low_dir.path().join("data");
        let store = Store::open_with(&data, &schema, test_options(2)).unwrap();
        store
            .append_json(&event(10, Some(1), "click", None, "1.00"))
            .unwrap();
        store
            .append_json(&event(11, Some(2), "click", None, "1.00"))
            .unwrap();
        store
            .append_json(&event(20, Some(10), "click", None, "1.00"))
            .unwrap();
        store
            .append_json(&event(21, Some(11), "click", None, "1.00"))
            .unwrap();
        store.flush().unwrap();
        let gt = [Predicate::Cmp("user_id".into(), CmpOp::Gt, Scalar::Int(5))];
        let resolved = resolve_predicates(store.schema(), &gt).unwrap();
        assert_eq!(store.blocks_in_range(0, 100, &resolved).len(), 1);
        let victim = store.catalog().segments[0]
            .blocks
            .iter()
            .find(|block| block.max_ts <= 11)
            .cloned()
            .unwrap();
        let path = segment::data_path(&data, victim.segment_id);
        let mut bytes = fs::read(&path).unwrap();
        bytes[victim.offset as usize + segment::BLOCK_HEADER_LEN + 4] ^= 0xff;
        fs::write(&path, &bytes).unwrap();
        assert_eq!(store.query_with_filter(0, 100, &gt).unwrap().len(), 2);
        assert_eq!(store.count_with_filter(0, 100, &gt).unwrap(), 2);
        assert_eq!(
            store
                .histogram_with_filter(0, 100, 1, &gt)
                .unwrap()
                .iter()
                .map(|bucket| bucket.count)
                .sum::<u64>(),
            2
        );
        let unread = store.query_with_filter(
            0,
            100,
            &[Predicate::Cmp("ok".into(), CmpOp::Gt, Scalar::Bool(true))],
        );
        assert!(matches!(unread, Err(Error::Schema(_))), "{unread:?}");
        assert!(store.query(victim.min_ts, victim.max_ts).is_err());
        store.close().unwrap();

        let float_dir = TempDir::new();
        let schema = write_schema(float_dir.path());
        let data = float_dir.path().join("data");
        let store = Store::open_with(&data, &schema, test_options(1)).unwrap();
        for (ts, score) in [(1i64, 1.0), (2, 9.0)] {
            let obj = serde_json::json!({
                "ts": ts,
                "user_id": ts,
                "score": score,
                "ok": true,
                "action": "click",
                "amount": "1.00",
            });
            store
                .append_json(&serde_json::to_vec(&obj).unwrap())
                .unwrap();
        }
        store.flush().unwrap();
        let low = store.catalog().segments[0].blocks[0].clone();
        assert!(low.max_ts <= 1);
        let path = segment::data_path(&data, low.segment_id);
        let mut bytes = fs::read(&path).unwrap();
        bytes[low.offset as usize + segment::BLOCK_HEADER_LEN + 4] ^= 0xff;
        fs::write(&path, &bytes).unwrap();
        let read = store.query_with_filter(
            0,
            10,
            &[Predicate::Cmp(
                "score".into(),
                CmpOp::Gt,
                Scalar::Float(5.0),
            )],
        );
        assert_eq!(read.unwrap().len(), 1, "the low float block is skipped");
        let equality =
            store.query_with_filter(0, 10, &[Predicate::Eq("score".into(), Scalar::Float(1.0))]);
        assert!(
            equality.is_err(),
            "equality reads a block whose float range contains the value"
        );
        store.close().unwrap();
    }

    #[test]
    fn old_float_zone_tag_returns_the_same_rows_and_does_not_skip() {
        let dir = TempDir::new();
        let schema = write_schema(dir.path());
        let data = dir.path().join("data");
        let options = test_options(1);
        let store = Store::open_with(&data, &schema, options.clone()).unwrap();
        for (ts, score) in [(1i64, 1.0), (2, 9.0)] {
            let obj = serde_json::json!({
                "ts": ts,
                "user_id": ts,
                "score": score,
                "ok": true,
                "action": "click",
                "amount": "1.00",
            });
            store
                .append_json(&serde_json::to_vec(&obj).unwrap())
                .unwrap();
        }
        store.flush().unwrap();
        let above = [Predicate::Cmp(
            "score".into(),
            CmpOp::Gt,
            Scalar::Float(5.0),
        )];
        let fresh = store.query_with_filter(0, 10, &above).unwrap();
        assert_eq!(fresh.len(), 1);
        assert_eq!(row_value(&store, &fresh[0])["score"].as_f64(), Some(9.0));
        let bounds: Vec<(i64, i64)> = store.catalog().segments[0]
            .blocks
            .iter()
            .map(|block| (block.min_ts, block.max_ts))
            .collect();
        let schema_ref = store.schema().clone();
        store.close().unwrap();

        let zone_file = crate::zone::zone_path(&data, 1);
        crate::zone::rewrite_float_zones_unknown(
            &zone_file,
            crate::zone::schema_crc(&schema_ref),
            crate::zone::field_count(&schema_ref).unwrap(),
            &bounds,
        )
        .unwrap();
        let store = Store::open_with(&data, &schema, options).unwrap();
        let again = store.query_with_filter(0, 10, &above).unwrap();
        assert_eq!(again.len(), 1);
        assert_eq!(row_value(&store, &again[0])["score"].as_f64(), Some(9.0));
        let resolved = resolve_predicates(store.schema(), &above).unwrap();
        assert_eq!(
            store.blocks_in_range(0, 10, &resolved).len(),
            2,
            "tag 0 does not skip the float column"
        );
        let low = store.catalog().segments[0].blocks[0].clone();
        assert!(low.max_ts <= 1);
        let path = segment::data_path(&data, low.segment_id);
        let mut bytes = fs::read(&path).unwrap();
        bytes[low.offset as usize + segment::BLOCK_HEADER_LEN + 4] ^= 0xff;
        fs::write(&path, &bytes).unwrap();
        let read = store.query_with_filter(0, 10, &above);
        assert!(read.is_err(), "an unknown float tag still reads the block");
        store.close().unwrap();
    }

    #[test]
    fn count_matches_the_unpaged_query_and_ignores_a_lying_index() {
        let dir = TempDir::new();
        let schema = write_schema(dir.path());
        let data = dir.path().join("data");
        let store = Store::open_with(&data, &schema, test_options(8)).unwrap();
        for (ts, action) in [
            (1000i64, "click"),
            (2000, "view"),
            (3000, "click"),
            (4000, "buy"),
            (5000, "view"),
        ] {
            store
                .append_json(&event(ts, Some(ts), action, Some("note"), "1.00"))
                .unwrap();
        }
        store.flush().unwrap();
        assert_eq!(store.count(1000, 5000).unwrap(), 5);
        assert_eq!(store.query(1000, 5000).unwrap().len(), 5);
        assert_eq!(store.count(2000, 3000).unwrap(), 2);
        assert_eq!(store.query(2000, 3000).unwrap().len(), 2);
        let buy = [Predicate::Eq("action".into(), "buy".into())];
        assert_eq!(store.count_with_filter(1000, 5000, &buy).unwrap(), 1);
        assert_eq!(store.query_with_filter(1000, 5000, &buy).unwrap().len(), 1);
        assert_eq!(store.count(9000, 9000).unwrap(), 0);
        assert!(store.query(9000, 9000).unwrap().is_empty());

        let narrow = Store::open_with(dir.path().join("narrow"), &schema, test_options(8)).unwrap();
        for ts in [100i64, 200, 300] {
            narrow
                .append_json(&event(ts, Some(1), "click", Some("note"), "1.00"))
                .unwrap();
        }
        narrow.flush().unwrap();
        {
            let mut catalog = narrow.catalog();
            let block = &mut catalog.segments[0].blocks[0];
            assert_eq!(block.row_count, 3);
            block.min_ts = 150;
            block.max_ts = 250;
        }
        assert_eq!(narrow.count(150, 250).unwrap(), 1);
        assert_eq!(narrow.query(150, 250).unwrap().len(), 1);
        let lied = narrow.histogram(150, 250, 50).unwrap();
        assert_eq!(
            lied,
            vec![
                HistogramBucket {
                    start_ms: 150,
                    count: 0
                },
                HistogramBucket {
                    start_ms: 200,
                    count: 1
                },
                HistogramBucket {
                    start_ms: 250,
                    count: 0
                },
            ]
        );
        narrow.close().unwrap();
        store.close().unwrap();
    }

    #[test]
    fn filtered_read_keeps_json_when_each_column_is_its_own_frame() {
        let dir = TempDir::new();
        let schema_text = r#"{
            "timestamp_field": "time",
            "fields": [
                {"name": "time", "type": "timestamp"},
                {"name": "type", "type": "string"},
                {"name": "data", "type": "json"}
            ]
        }"#;
        let schema = dir.path().join("schema.json");
        fs::write(&schema, schema_text).unwrap();
        let data = dir.path().join("data");
        let store = Store::open_with(&data, &schema, test_options(128)).unwrap();
        let blob = "x".repeat(200);
        for index in 0..128 {
            let kind = if index < 64 {
                "dev.genesis.run.accepted"
            } else {
                "dev.genesis.run.other"
            };
            let event = format!(
                r#"{{"time":{index},"type":"{kind}","data":{{"n":{index},"blob":"{blob}"}}}}"#
            );
            store.append_json(event.as_bytes()).unwrap();
        }
        store.flush().unwrap();
        let bytes = fs::read(segment::data_path(&data, 1)).unwrap();
        assert!(
            bytes.windows(4).any(|magic| magic == b"EVCS"),
            "a large json column is its own slice"
        );
        let accepted = [Predicate::Eq(
            "type".into(),
            "dev.genesis.run.accepted".into(),
        )];
        assert_eq!(store.count_with_filter(0, 127, &accepted).unwrap(), 64);
        let rows = store.query_with_filter(0, 127, &accepted).unwrap();
        assert_eq!(rows.len(), 64);
        for (offset, row) in rows.iter().enumerate() {
            match &row.values[2] {
                Scalar::Json(text) => {
                    assert!(text.contains(&format!("\"n\":{offset}")));
                    assert!(text.contains(&blob));
                }
                other => panic!("json column missing: {other:?}"),
            }
        }
        let other = [Predicate::Eq("type".into(), "dev.genesis.run.other".into())];
        let rows = store.query_with_filter(0, 127, &other).unwrap();
        assert_eq!(rows.len(), 64);
        match &rows[0].values[2] {
            Scalar::Json(text) => assert!(text.contains("\"n\":64")),
            other => panic!("json column missing: {other:?}"),
        }
        store.close().unwrap();
    }

    #[test]
    fn select_skips_a_slice_it_does_not_need_and_projects_a_single_frame() {
        let dir = TempDir::new();
        let schema_text = r#"{
            "timestamp_field": "time",
            "fields": [
                {"name": "time", "type": "timestamp"},
                {"name": "type", "type": "string"},
                {"name": "data", "type": "json"}
            ]
        }"#;
        let schema = dir.path().join("schema.json");
        fs::write(&schema, schema_text).unwrap();
        let data = dir.path().join("data");
        let store = Store::open_with(&data, &schema, test_options(128)).unwrap();
        let blob = "x".repeat(200);
        for index in 0..128 {
            let event = format!(
                r#"{{"time":{index},"type":"click","data":{{"n":{index},"blob":"{blob}"}}}}"#
            );
            store.append_json(event.as_bytes()).unwrap();
        }
        store.flush().unwrap();
        let path = segment::data_path(&data, 1);
        let mut bytes = fs::read(&path).unwrap();
        assert!(
            bytes.windows(4).any(|magic| magic == b"EVCS"),
            "a large json column is its own slice"
        );
        let from = 0;
        let to = 127;
        let projected = store
            .query_json_columns(from, to, &[], 0, None, &[0, 1])
            .unwrap();
        let rows: serde_json::Value = serde_json::from_slice(&projected).unwrap();
        assert_eq!(rows.as_array().unwrap().len(), 128);
        assert!(rows[0].get("data").is_none(), "{rows}");
        assert_eq!(rows[0]["time"], 0);
        assert_eq!(rows[0]["type"], "click");
        let filtered = [Predicate::Eq("type".into(), "click".into())];
        let times = store
            .query_json_columns(from, to, &filtered, 0, Some(1), &[0])
            .unwrap();
        assert_eq!(times, br#"[{"time":0}]"#);

        flip_column_slice(&mut bytes, 1);
        fs::write(&path, &bytes).unwrap();
        let skipped = store.query_json_columns(from, to, &[], 0, None, &[0]);
        assert!(
            skipped.is_ok(),
            "type slice was decompressed: {}",
            skipped.unwrap_err()
        );
        flip_column_slice(&mut bytes, 1);
        flip_column_slice(&mut bytes, 2);
        fs::write(&path, &bytes).unwrap();
        assert!(
            store
                .query_json_columns(from, to, &[], 0, None, &[0, 2])
                .is_err(),
            "selected json slice was not read"
        );
        let still = store.query_json_columns(from, to, &[], 0, None, &[0, 1]);
        assert!(
            still.is_ok(),
            "json slice was decompressed: {}",
            still.unwrap_err()
        );
        store.close().unwrap();

        let plain = TempDir::new();
        let plain_schema = write_schema(plain.path());
        let plain_store =
            Store::open_with(plain.path().join("data"), &plain_schema, test_options(8)).unwrap();
        plain_store
            .append_json(&event(10, Some(1), "click", Some("landing"), "1.00"))
            .unwrap();
        plain_store.flush().unwrap();
        let dat = fs::read(segment::data_path(&plain.path().join("data"), 1)).unwrap();
        assert!(
            !dat.windows(4).any(|magic| magic == b"EVCS"),
            "a short block stays one frame"
        );
        let one = plain_store
            .query_json_columns(10, 10, &[], 0, None, &[4])
            .unwrap();
        assert_eq!(one, br#"[{"action":"click"}]"#);
        let full = plain_store.query_json(10, 10).unwrap();
        assert!(full.windows(6).any(|window| window == b"\"note\""));
        plain_store.close().unwrap();
    }

    /// Release-only check from the column-projection contract. Debug builds
    /// do not meet the timing, so `cargo test` leaves this ignored.
    #[test]
    #[ignore]
    fn projected_select_is_at_least_twice_as_fast_on_wide_json() {
        let dir = TempDir::new();
        let schema = dir.path().join("schema.json");
        fs::write(
            &schema,
            r#"{
                "timestamp_field": "time",
                "fields": [
                    {"name": "time", "type": "timestamp"},
                    {"name": "type", "type": "string"},
                    {"name": "data", "type": "json"}
                ]
            }"#,
        )
        .unwrap();
        let options = StoreOptions {
            block_rows: 2048,
            zstd_level: 3,
            ..test_options(2048)
        };
        let store = Store::open_with(dir.path().join("data"), &schema, options).unwrap();
        let count = 8192usize;
        for index in 0..count {
            let mut blob = String::with_capacity(640);
            while blob.len() < 640 {
                blob.push_str(&format!("{index:08x}{:08x}", blob.len()));
            }
            let event = format!(
                r#"{{"time":{index},"type":"click","data":{{"n":{index},"blob":"{blob}"}}}}"#
            );
            store.append_json(event.as_bytes()).unwrap();
        }
        store.flush().unwrap();
        let from = 0i64;
        let to = count as i64;
        let time_one = |projected: bool| {
            let started = std::time::Instant::now();
            let bytes = if projected {
                store
                    .query_json_columns(from, to, &[], 0, None, &[0, 1])
                    .unwrap()
            } else {
                store.query_json(from, to).unwrap()
            };
            (started.elapsed(), bytes)
        };
        let _ = time_one(false);
        let _ = time_one(true);
        let mut full = Vec::new();
        let mut narrow = Vec::new();
        for _ in 0..3 {
            let (elapsed, bytes) = time_one(false);
            let rows: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(rows.as_array().unwrap().len(), count);
            full.push(elapsed);
            let (elapsed, bytes) = time_one(true);
            let rows: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(rows.as_array().unwrap().len(), count);
            assert!(rows[0].get("data").is_none(), "{rows}");
            assert!(rows[0].get("time").is_some());
            assert!(rows[0].get("type").is_some());
            narrow.push(elapsed);
        }
        full.sort();
        narrow.sort();
        let full_median = full[1];
        let narrow_median = narrow[1];
        eprintln!(
            "full median {full_median:?} ({full:?}); select median {narrow_median:?} ({narrow:?})"
        );
        assert!(
            full_median >= narrow_median * 2,
            "select median {narrow_median:?} was not twice as fast as {full_median:?}"
        );
        store.close().unwrap();
    }

    fn flip_column_slice(bytes: &mut [u8], column: u16) {
        let start = bytes
            .windows(4)
            .position(|magic| magic == b"EVCS")
            .expect("column container");
        let mut cursor = start + 4;
        assert_eq!(bytes[cursor], 1, "slice version");
        cursor += 1 + 4;
        let n_slices = u16::from_le_bytes(bytes[cursor..cursor + 2].try_into().unwrap()) as usize;
        cursor += 2;
        for _ in 0..n_slices {
            let first = u16::from_le_bytes(bytes[cursor..cursor + 2].try_into().unwrap());
            cursor += 4;
            cursor += 1;
            cursor += 4;
            let len = u32::from_le_bytes(bytes[cursor..cursor + 4].try_into().unwrap()) as usize;
            cursor += 4;
            if first == column {
                bytes[cursor] ^= 0xff;
                // The frame CRC covers every slice. Repair it so a skipped
                // slice is the only thing that can fail, and only if read.
                let header = start - segment::BLOCK_HEADER_LEN;
                let compressed_len =
                    u32::from_le_bytes(bytes[header + 4..header + 8].try_into().unwrap()) as usize;
                let payload = start;
                let crc = crc32fast::hash(&bytes[payload..payload + compressed_len]);
                bytes[header + 8..header + 12].copy_from_slice(&crc.to_le_bytes());
                return;
            }
            cursor += len;
        }
        panic!("column {column} was not a slice");
    }

    #[test]
    fn histogram_aligns_to_the_epoch_and_keeps_empty_buckets() {
        let dir = TempDir::new();
        let schema = write_schema(dir.path());
        let store = Store::open_with(dir.path().join("data"), &schema, test_options(8)).unwrap();
        for ts in [1000i64, 2000, 3000] {
            store
                .append_json(&event(ts, Some(ts), "click", None, "1.00"))
                .unwrap();
        }
        store
            .append_json(&event(-1500, Some(1), "view", None, "1.00"))
            .unwrap();
        store.flush().unwrap();

        assert_eq!(
            store.histogram(1000, 3000, 1000).unwrap(),
            vec![
                HistogramBucket {
                    start_ms: 1000,
                    count: 1
                },
                HistogramBucket {
                    start_ms: 2000,
                    count: 1
                },
                HistogramBucket {
                    start_ms: 3000,
                    count: 1
                },
            ]
        );
        assert_eq!(
            store.histogram(1000, 3000, 2000).unwrap(),
            vec![
                HistogramBucket {
                    start_ms: 0,
                    count: 1
                },
                HistogramBucket {
                    start_ms: 2000,
                    count: 2
                },
            ]
        );
        let gapped = store.histogram(1000, 5000, 1000).unwrap();
        assert_eq!(
            gapped,
            vec![
                HistogramBucket {
                    start_ms: 1000,
                    count: 1
                },
                HistogramBucket {
                    start_ms: 2000,
                    count: 1
                },
                HistogramBucket {
                    start_ms: 3000,
                    count: 1
                },
                HistogramBucket {
                    start_ms: 4000,
                    count: 0
                },
                HistogramBucket {
                    start_ms: 5000,
                    count: 0
                },
            ]
        );
        assert_eq!(
            gapped.iter().map(|bucket| bucket.count).sum::<u64>(),
            store.count(1000, 5000).unwrap()
        );

        assert_eq!(
            store.histogram(-1500, -100, 1000).unwrap(),
            vec![
                HistogramBucket {
                    start_ms: -2000,
                    count: 1
                },
                HistogramBucket {
                    start_ms: -1000,
                    count: 0
                },
            ]
        );

        let none = [Predicate::Eq("action".into(), "missing".into())];
        let empty = store
            .histogram_with_filter(1000, 3000, 1000, &none)
            .unwrap();
        assert!(empty.iter().all(|bucket| bucket.count == 0));
        assert_eq!(
            empty.iter().map(|bucket| bucket.count).sum::<u64>(),
            store.count_with_filter(1000, 3000, &none).unwrap()
        );
        assert!(store.histogram(10, 1, 1000).unwrap().is_empty());
        let too_many = store.histogram(0, 4096, 1).unwrap_err();
        assert!(too_many.to_string().contains("4096"), "{too_many}");
        assert_eq!(store.count(1000, 3000).unwrap(), 3);
        store.close().unwrap();
    }

    #[test]
    fn histogram_rejects_a_wide_span_without_reading_the_block() {
        let dir = TempDir::new();
        let schema = write_schema(dir.path());
        let data = dir.path().join("data");
        let store = Store::open_with(&data, &schema, test_options(8)).unwrap();
        store
            .append_json(&event(1000, Some(1), "click", None, "1.00"))
            .unwrap();
        store.flush().unwrap();
        let block = store.catalog().segments[0].blocks[0].clone();
        let path = segment::data_path(&data, block.segment_id);
        let mut bytes = fs::read(&path).unwrap();
        bytes[block.offset as usize + segment::BLOCK_HEADER_LEN + 4] ^= 0xff;
        fs::write(&path, &bytes).unwrap();
        let err = store.histogram(0, 4096, 1).unwrap_err();
        assert!(err.to_string().contains("4096"), "{err}");
        let unrepresentable = store.histogram(i64::MIN, i64::MIN, 1000).unwrap_err();
        assert!(
            unrepresentable
                .to_string()
                .contains("histogram bucket start is outside i64"),
            "{unrepresentable}"
        );
        assert!(store.count(1000, 1000).is_err());
        store.close().unwrap();
    }

    #[test]
    fn histogram_does_not_read_a_block_outside_the_index_range() {
        let dir = TempDir::new();
        let schema = write_schema(dir.path());
        let data = dir.path().join("data");
        let store = Store::open_with(&data, &schema, test_options(2)).unwrap();
        for ts in [1i64, 2, 10_000, 10_001] {
            store
                .append_json(&event(ts, Some(ts), "click", None, "1.00"))
                .unwrap();
        }
        store.flush().unwrap();
        let victim = store.catalog().segments[0].blocks[1].clone();
        assert!(victim.min_ts > 2);
        let path = segment::data_path(&data, victim.segment_id);
        let mut bytes = fs::read(&path).unwrap();
        let flip_at = victim.offset as usize + segment::BLOCK_HEADER_LEN + 4;
        bytes[flip_at] ^= 0xff;
        fs::write(&path, &bytes).unwrap();
        assert_eq!(
            store.histogram(1, 2, 1).unwrap(),
            vec![
                HistogramBucket {
                    start_ms: 1,
                    count: 1
                },
                HistogramBucket {
                    start_ms: 2,
                    count: 1
                },
            ]
        );
        assert!(store.count(victim.min_ts, victim.max_ts).is_err());
        store.close().unwrap();
    }

    #[test]
    fn count_does_not_read_a_block_the_zone_map_rejects() {
        let dir = TempDir::new();
        let schema = write_schema(dir.path());
        let data = dir.path().join("data");
        let store = Store::open_with(&data, &schema, test_options(32)).unwrap();
        for run in 0..2i64 {
            for seq in 0..32i64 {
                let action = format!("run-{run}");
                store
                    .append_json(&event(
                        run * 32 + seq,
                        Some(run),
                        &action,
                        Some("note"),
                        "1.00",
                    ))
                    .unwrap();
            }
        }
        store.flush().unwrap();
        let predicates = [Predicate::Eq("action".into(), "run-0".into())];
        let resolved = resolve_predicates(store.schema(), &predicates).unwrap();
        let candidates = store.blocks_in_range(0, 10_000, &resolved);
        assert_eq!(candidates.len(), 1);
        let kept = candidates[0].offset;
        let victim = store.catalog().segments[0]
            .blocks
            .iter()
            .find(|block| block.offset != kept)
            .cloned()
            .unwrap();
        let path = segment::data_path(&data, victim.segment_id);
        let mut bytes = fs::read(&path).unwrap();
        let flip_at = victim.offset as usize + segment::BLOCK_HEADER_LEN + 4;
        bytes[flip_at] ^= 0xff;
        fs::write(&path, &bytes).unwrap();
        assert_eq!(store.count_with_filter(0, 10_000, &predicates).unwrap(), 32);
        assert_eq!(
            store
                .histogram_with_filter(0, 63, 32, &predicates)
                .unwrap()
                .iter()
                .map(|bucket| bucket.count)
                .sum::<u64>(),
            32
        );
        assert!(store.query(victim.min_ts, victim.max_ts).is_err());
        store.close().unwrap();
    }

    #[test]
    fn count_accepts_a_range_the_zone_already_proves() {
        let dir = TempDir::new();
        let schema = write_schema(dir.path());
        let data = dir.path().join("data");
        let store = Store::open_with(&data, &schema, test_options(2)).unwrap();
        for (ts, user, score, amount) in [
            (10i64, Some(1i64), Some(1.0f64), Some("1.00")),
            (20, Some(2), Some(2.0), Some("2.00")),
            (30, Some(8), Some(6.0), Some("8.00")),
            (40, Some(9), Some(7.0), Some("9.00")),
            (50, None, None, None),
            (60, Some(3), Some(3.0), Some("3.00")),
        ] {
            let mut obj = serde_json::json!({
                "ts": ts,
                "ok": user.is_some(),
                "action": "click",
            });
            if let Some(score) = score {
                obj["score"] = serde_json::json!(score);
            }
            if let Some(user) = user {
                obj["user_id"] = serde_json::json!(user);
            }
            if let Some(amount) = amount {
                obj["amount"] = serde_json::json!(amount);
            }
            store
                .append_json(&serde_json::to_vec(&obj).unwrap())
                .unwrap();
        }
        store.flush().unwrap();
        assert_eq!(store.catalog().segments[0].blocks.len(), 3);

        let ge = |field: &str, value: Scalar| [Predicate::Cmp(field.into(), CmpOp::Ge, value)];
        let gt = |field: &str, value: Scalar| [Predicate::Cmp(field.into(), CmpOp::Gt, value)];
        let counted = |from, to, predicates: &[Predicate]| {
            store.count_with_filter(from, to, predicates).unwrap()
        };
        let score_ge = ge("score", Scalar::Float(0.0));
        assert_eq!(counted(0, 100, &score_ge), 5);
        assert_eq!(counted(0, 100, &ge("score", Scalar::Float(5.0))), 2);
        assert_eq!(counted(0, 100, &gt("score", Scalar::Float(100.0))), 0);
        assert_eq!(counted(0, 100, &gt("score", Scalar::Float(0.0))), 5);
        assert_eq!(counted(0, 100, &ge("user_id", Scalar::Int(0))), 5);
        assert_eq!(counted(0, 100, &ge("user_id", Scalar::Int(8))), 2);
        let decimal = crate::schema::FieldType::Decimal { scale: 2 };
        let amount_zero = crate::value::scalar_from_literal(decimal, "0.00").unwrap();
        let amount_eight = crate::value::scalar_from_literal(decimal, "8.00").unwrap();
        assert_eq!(counted(0, 100, &ge("amount", amount_zero.clone())), 5);
        assert_eq!(counted(0, 100, &ge("amount", amount_eight)), 2);
        assert_eq!(
            counted(20, 20, &score_ge),
            1,
            "a proved block still drops a timestamp outside the query"
        );
        assert_eq!(
            counted(0, 100, &[Predicate::Eq("score".into(), Scalar::Float(6.0))]),
            1
        );

        let histogram_total = |predicates: &[Predicate]| {
            store
                .histogram_with_filter(0, 100, 10, predicates)
                .unwrap()
                .iter()
                .map(|bucket| bucket.count)
                .sum::<u64>()
        };
        assert_eq!(histogram_total(&score_ge), 5);
        assert_eq!(histogram_total(&ge("score", Scalar::Float(5.0))), 2);
        assert_eq!(histogram_total(&gt("score", Scalar::Float(100.0))), 0);
        assert_eq!(
            store
                .query_with_filter(0, 100, &ge("score", Scalar::Float(5.0)))
                .unwrap()
                .len(),
            2
        );

        {
            let mut catalog = store.catalog();
            let block = &mut catalog.segments[0].blocks[0];
            assert_eq!(block.min_ts, 10);
            block.min_ts = 15;
            block.max_ts = 25;
        }
        assert_eq!(
            counted(15, 25, &score_ge),
            1,
            "the sparse index does not replace the payload timestamps"
        );
        store.close().unwrap();
    }

    #[test]
    fn count_reads_one_string_period_and_still_walks_a_cut() {
        let dir = TempDir::new();
        let schema = write_schema(dir.path());
        let data = dir.path().join("data");
        let store = Store::open_with(&data, &schema, test_options(32)).unwrap();
        let actions = ["click", "view", "buy", "scroll"];
        for i in 0..8i64 {
            let mut obj = serde_json::json!({
                "ts": i * 10,
                "user_id": i,
                "score": 1.0,
                "ok": i != 0,
                "action": actions[i as usize % actions.len()],
                "amount": "1.00",
            });
            if i % 4 != 3 {
                obj["note"] = serde_json::json!(["landing", "checkout", "search"][i as usize % 3]);
            }
            store
                .append_json(&serde_json::to_vec(&obj).unwrap())
                .unwrap();
        }
        store.flush().unwrap();
        assert_eq!(store.catalog().segments[0].blocks.len(), 1);

        let click = [Predicate::Eq("action".into(), "click".into())];
        let counted = store.count_with_filter(0, 70, &click).unwrap();
        let walked = store.query_with_filter(0, 70, &click).unwrap().len() as u64;
        assert_eq!(counted, 2);
        assert_eq!(counted, walked);
        assert_eq!(
            store
                .count_with_filter(0, 70, &[Predicate::Eq("action".into(), "missing".into())])
                .unwrap(),
            0
        );
        let cut = store.count_with_filter(15, 45, &click).unwrap();
        assert_eq!(cut, 1);
        assert_eq!(
            cut,
            store.query_with_filter(15, 45, &click).unwrap().len() as u64
        );
        let both = [
            Predicate::Eq("action".into(), "click".into()),
            Predicate::Eq("ok".into(), Scalar::Bool(true)),
        ];
        assert_eq!(store.count_with_filter(0, 70, &both).unwrap(), 1);
        assert_eq!(
            store.count_with_filter(0, 70, &both).unwrap(),
            store.query_with_filter(0, 70, &both).unwrap().len() as u64
        );
        let ranged = [
            Predicate::Eq("action".into(), "click".into()),
            Predicate::Cmp("user_id".into(), CmpOp::Ge, Scalar::Int(4)),
        ];
        assert_eq!(store.count_with_filter(0, 70, &ranged).unwrap(), 1);
        assert_eq!(
            store
                .histogram_with_filter(0, 70, 10, &click)
                .unwrap()
                .iter()
                .map(|bucket| bucket.count)
                .sum::<u64>(),
            2
        );

        {
            let mut catalog = store.catalog();
            let block = &mut catalog.segments[0].blocks[0];
            block.min_ts = 0;
            block.max_ts = 10;
        }
        assert_eq!(
            store.count_with_filter(0, 10, &click).unwrap(),
            1,
            "the sparse index does not prove that every payload timestamp is inside"
        );
        assert_eq!(store.count_with_filter(0, 70, &click).unwrap(), 2);
        store.close().unwrap();
    }

    #[test]
    fn count_reads_one_bool_period_and_still_walks_a_cut() {
        let dir = TempDir::new();
        let schema = write_schema(dir.path());
        let data = dir.path().join("data");
        let store = Store::open_with(&data, &schema, test_options(32)).unwrap();
        let actions = ["click", "view", "buy", "scroll"];
        for i in 0..64i64 {
            let mut obj = serde_json::json!({
                "ts": i * 10,
                "user_id": i,
                "score": 1.0,
                "ok": i % 2 == 0,
                "action": actions[i as usize % actions.len()],
                "amount": "1.00",
            });
            if i % 4 != 3 {
                obj["note"] = serde_json::json!(["landing", "checkout", "search"][i as usize % 3]);
            }
            store
                .append_json(&serde_json::to_vec(&obj).unwrap())
                .unwrap();
        }
        store.flush().unwrap();
        assert_eq!(store.catalog().segments[0].blocks.len(), 2);

        let yes = [Predicate::Eq("ok".into(), Scalar::Bool(true))];
        let no = [Predicate::Eq("ok".into(), Scalar::Bool(false))];
        let counted = store.count_with_filter(0, 630, &yes).unwrap();
        assert_eq!(counted, 32);
        assert_eq!(
            counted,
            store.query_with_filter(0, 630, &yes).unwrap().len() as u64
        );
        let falses = store.count_with_filter(0, 630, &no).unwrap();
        assert_eq!(falses, 32);
        assert_eq!(
            falses,
            store.query_with_filter(0, 630, &no).unwrap().len() as u64
        );
        let cut = store.count_with_filter(0, 50, &yes).unwrap();
        assert_eq!(cut, 3);
        assert_eq!(
            cut,
            store.query_with_filter(0, 50, &yes).unwrap().len() as u64
        );
        let both = [
            Predicate::Eq("ok".into(), Scalar::Bool(true)),
            Predicate::Eq("action".into(), "click".into()),
        ];
        assert_eq!(store.count_with_filter(0, 630, &both).unwrap(), 16);
        assert_eq!(
            store.count_with_filter(0, 630, &both).unwrap(),
            store.query_with_filter(0, 630, &both).unwrap().len() as u64
        );
        let ranged = [
            Predicate::Eq("ok".into(), Scalar::Bool(true)),
            Predicate::Cmp("user_id".into(), CmpOp::Ge, Scalar::Int(50)),
        ];
        assert_eq!(store.count_with_filter(0, 630, &ranged).unwrap(), 7);
        assert_eq!(
            store.count_with_filter(0, 630, &ranged).unwrap(),
            store.query_with_filter(0, 630, &ranged).unwrap().len() as u64
        );
        assert_eq!(
            store
                .histogram_with_filter(0, 630, 10, &yes)
                .unwrap()
                .iter()
                .map(|bucket| bucket.count)
                .sum::<u64>(),
            32
        );

        {
            let mut catalog = store.catalog();
            let block = &mut catalog.segments[0].blocks[0];
            block.min_ts = 0;
            block.max_ts = 50;
        }
        assert_eq!(
            store.count_with_filter(0, 50, &yes).unwrap(),
            3,
            "the sparse index does not prove that every payload timestamp is inside"
        );
        assert_eq!(store.count_with_filter(0, 630, &yes).unwrap(), 32);
        store.close().unwrap();
    }

    #[test]
    fn count_drops_a_zone_proved_range_before_the_header() {
        let dir = TempDir::new();
        let schema_text = r#"{
            "timestamp_field": "ts",
            "fields": [
                {"name": "ts", "type": "timestamp"},
                {"name": "user_id", "type": "int"},
                {"name": "score", "type": "float"},
                {"name": "ok", "type": "bool"},
                {"name": "action", "type": "string"},
                {"name": "note", "type": "text"},
                {"name": "amount", "type": "decimal", "scale": 2},
                {"name": "blob", "type": "json"}
            ]
        }"#;
        let schema = dir.path().join("schema.json");
        fs::write(&schema, schema_text).unwrap();
        let data = dir.path().join("data");
        let options = test_options(128);
        let store = Store::open_with(&data, &schema, options.clone()).unwrap();
        let actions = ["click", "view", "buy", "scroll"];
        let notes = ["landing", "checkout", "search"];
        let rows_n = 128i64;
        for i in 0..rows_n {
            let blob: String = (0..512)
                .map(|byte| {
                    let mixed = (i as u32)
                        .wrapping_mul(1_103_515_245)
                        .wrapping_add(12_345)
                        .wrapping_add(byte);
                    char::from(b'A' + (mixed % 26) as u8)
                })
                .collect();
            let mut obj = serde_json::json!({
                "ts": i * 10,
                "user_id": i,
                "score": (i % 10) as f64,
                "ok": i % 2 == 0,
                "action": actions[i as usize % actions.len()],
                "amount": "1.00",
                "blob": {"n": i, "blob": blob},
            });
            if i % 4 != 3 {
                obj["note"] = serde_json::json!(notes[i as usize % 3]);
            }
            store
                .append_json(&serde_json::to_vec(&obj).unwrap())
                .unwrap();
        }
        store.flush().unwrap();
        assert_eq!(store.catalog().segments[0].blocks.len(), 1);
        let path = segment::data_path(&data, 1);
        let original = fs::read(&path).unwrap();
        assert!(
            original.windows(4).any(|magic| magic == b"EVCS"),
            "each column is its own slice"
        );

        let click = [Predicate::Eq("action".into(), "click".into())];
        let covered = [
            Predicate::Eq("action".into(), "click".into()),
            Predicate::Cmp("score".into(), CmpOp::Ge, Scalar::Float(0.0)),
        ];
        let partial = [
            Predicate::Eq("action".into(), "click".into()),
            Predicate::Cmp("score".into(), CmpOp::Ge, Scalar::Float(5.0)),
        ];
        let above = [
            Predicate::Eq("action".into(), "click".into()),
            Predicate::Cmp("score".into(), CmpOp::Gt, Scalar::Float(100.0)),
        ];
        let yes = [
            Predicate::Eq("ok".into(), Scalar::Bool(true)),
            Predicate::Cmp("score".into(), CmpOp::Ge, Scalar::Float(0.0)),
        ];
        let both = [
            Predicate::Eq("action".into(), "click".into()),
            Predicate::Eq("ok".into(), Scalar::Bool(true)),
            Predicate::Cmp("score".into(), CmpOp::Ge, Scalar::Float(0.0)),
        ];
        let amount_ty = crate::schema::FieldType::Decimal { scale: 2 };
        let amount_zero = crate::value::scalar_from_literal(amount_ty, "0.00").unwrap();
        let amount = [
            Predicate::Eq("action".into(), "click".into()),
            Predicate::Cmp("amount".into(), CmpOp::Ge, amount_zero),
        ];
        let users = [
            Predicate::Eq("action".into(), "click".into()),
            Predicate::Cmp("user_id".into(), CmpOp::Ge, Scalar::Int(0)),
            Predicate::Cmp("score".into(), CmpOp::Ge, Scalar::Float(0.0)),
        ];
        let note = [
            Predicate::Eq("note".into(), "landing".into()),
            Predicate::Cmp("score".into(), CmpOp::Ge, Scalar::Float(0.0)),
        ];
        let end = (rows_n - 1) * 10;
        let rows = |predicates: &[Predicate]| {
            store.query_with_filter(0, end, predicates).unwrap().len() as u64
        };
        let clicks = rows_n as u64 / 4;
        assert_eq!(store.count_with_filter(0, end, &click).unwrap(), clicks);
        assert_eq!(store.count_with_filter(0, end, &covered).unwrap(), clicks);
        assert_eq!(
            store.count_with_filter(0, end, &covered).unwrap(),
            rows(&covered)
        );
        let walked = store.count_with_filter(0, end, &partial).unwrap();
        assert_eq!(walked, rows(&partial));
        assert!(
            walked < clicks,
            "a score below 5 keeps the block on the row walk"
        );
        assert_eq!(store.count_with_filter(0, end, &above).unwrap(), 0);
        assert_eq!(
            store.count_with_filter(0, end, &yes).unwrap(),
            rows_n as u64 / 2
        );
        assert_eq!(store.count_with_filter(0, end, &yes).unwrap(), rows(&yes));
        assert_eq!(store.count_with_filter(0, end, &both).unwrap(), rows(&both));
        assert_eq!(store.count_with_filter(0, end, &amount).unwrap(), clicks);
        assert_eq!(store.count_with_filter(0, end, &users).unwrap(), clicks);
        assert_eq!(store.count_with_filter(0, end, &note).unwrap(), rows(&note));
        let cut = store.count_with_filter(15, 45, &covered).unwrap();
        assert_eq!(
            cut,
            store.query_with_filter(15, 45, &covered).unwrap().len() as u64
        );
        assert_eq!(
            store
                .histogram_with_filter(0, end, 10, &covered)
                .unwrap()
                .iter()
                .map(|bucket| bucket.count)
                .sum::<u64>(),
            clicks
        );

        let bounds = {
            let catalog = store.catalog();
            let block = &catalog.segments[0].blocks[0];
            [(block.min_ts, block.max_ts)]
        };
        {
            let mut catalog = store.catalog();
            let block = &mut catalog.segments[0].blocks[0];
            block.min_ts = 0;
            block.max_ts = 10;
        }
        assert_eq!(
            store.count_with_filter(0, 10, &covered).unwrap(),
            1,
            "the sparse index does not prove that every payload timestamp is inside"
        );
        {
            let mut catalog = store.catalog();
            let block = &mut catalog.segments[0].blocks[0];
            block.min_ts = bounds[0].0;
            block.max_ts = bounds[0].1;
        }
        let zone_file = crate::zone::zone_path(&data, 1);
        let zone_original = fs::read(&zone_file).unwrap();
        let schema_ref = store.schema().clone();
        store.close().unwrap();

        crate::zone::rewrite_float_zones_unknown(
            &zone_file,
            crate::zone::schema_crc(&schema_ref),
            crate::zone::field_count(&schema_ref).unwrap(),
            &bounds,
        )
        .unwrap();
        let store = Store::open_with(&data, &schema, options.clone()).unwrap();
        assert_eq!(
            store.count_with_filter(0, end, &covered).unwrap(),
            clicks,
            "an unknown float zone does not prove the range"
        );
        store.close().unwrap();
        let mut unknown = fs::read(&path).unwrap();
        flip_column_slice(&mut unknown, 2);
        fs::write(&path, &unknown).unwrap();
        let store = Store::open_with(&data, &schema, options.clone()).unwrap();
        assert!(
            store.count_with_filter(0, end, &covered).is_err(),
            "a bloom or unknown zone still reads the range column"
        );
        store.close().unwrap();

        fs::write(&path, &original).unwrap();
        fs::write(&zone_file, &zone_original).unwrap();
        let store = Store::open_with(&data, &schema, options).unwrap();
        let mut bytes = fs::read(&path).unwrap();
        flip_column_slice(&mut bytes, 1);
        flip_column_slice(&mut bytes, 2);
        flip_column_slice(&mut bytes, 6);
        fs::write(&path, &bytes).unwrap();
        assert_eq!(
            store.count_with_filter(0, end, &covered).unwrap(),
            clicks,
            "a proved score column is not decoded"
        );
        assert_eq!(store.count_with_filter(0, end, &users).unwrap(), clicks);
        assert_eq!(store.count_with_filter(0, end, &amount).unwrap(), clicks);
        assert_eq!(
            store.count_with_filter(0, end, &yes).unwrap(),
            rows_n as u64 / 2
        );
        assert_eq!(store.count_with_filter(0, end, &above).unwrap(), 0);
        assert!(store.count_with_filter(0, end, &partial).is_err());
        assert!(store.count_with_filter(0, end, &both).is_err());
        assert!(store.count_with_filter(0, end, &note).is_err());
        assert!(store.count_with_filter(15, 45, &covered).is_err());
        assert!(store.histogram_with_filter(0, end, 10, &covered).is_err());
        assert!(store.query_with_filter(0, end, &covered).is_err());
        store.close().unwrap();

        let dir = TempDir::new();
        let schema = dir.path().join("schema.json");
        fs::write(&schema, schema_text).unwrap();
        let data = dir.path().join("data");
        let store = Store::open_with(&data, &schema, test_options(32)).unwrap();
        for i in 0..4i64 {
            let mut obj = serde_json::json!({
                "ts": i * 10,
                "user_id": i,
                "ok": true,
                "action": "click",
                "amount": "1.00",
            });
            if i != 1 {
                obj["score"] = serde_json::json!(1.0);
            }
            store
                .append_json(&serde_json::to_vec(&obj).unwrap())
                .unwrap();
        }
        store.flush().unwrap();
        let with_null = store.count_with_filter(0, 30, &covered).unwrap();
        assert_eq!(
            with_null,
            store.query_with_filter(0, 30, &covered).unwrap().len() as u64
        );
        assert_eq!(with_null, 3, "a null score matches no range");
        store.close().unwrap();
    }

    #[test]
    fn count_does_not_decode_a_proved_float_slice() {
        let dir = TempDir::new();
        let schema_text = r#"{
            "timestamp_field": "time",
            "fields": [
                {"name": "time", "type": "timestamp"},
                {"name": "score", "type": "float"},
                {"name": "data", "type": "json"}
            ]
        }"#;
        let schema = dir.path().join("schema.json");
        fs::write(&schema, schema_text).unwrap();
        let data = dir.path().join("data");
        let store = Store::open_with(&data, &schema, test_options(128)).unwrap();
        let blob = "x".repeat(200);
        for index in 0..128i64 {
            let event = format!(
                r#"{{"time":{index},"score":{},"data":{{"n":{index},"blob":"{blob}"}}}}"#,
                (index % 10) as f64
            );
            store.append_json(event.as_bytes()).unwrap();
        }
        store.flush().unwrap();
        let path = segment::data_path(&data, 1);
        let mut bytes = fs::read(&path).unwrap();
        assert!(
            bytes.windows(4).any(|magic| magic == b"EVCS"),
            "the json column is its own slice"
        );
        let covered = [Predicate::Cmp(
            "score".into(),
            CmpOp::Ge,
            Scalar::Float(0.0),
        )];
        let partial = [Predicate::Cmp(
            "score".into(),
            CmpOp::Ge,
            Scalar::Float(5.0),
        )];
        assert_eq!(store.count_with_filter(0, 127, &covered).unwrap(), 128);
        assert_eq!(store.count_with_filter(0, 127, &partial).unwrap(), 63);
        let covered_buckets = store.histogram_with_filter(0, 127, 1, &covered).unwrap();
        assert_eq!(
            covered_buckets
                .iter()
                .map(|bucket| bucket.count)
                .sum::<u64>(),
            128
        );
        assert_eq!(
            covered_buckets,
            store.histogram(0, 127, 1).unwrap(),
            "a zone-proved range matches the unfiltered buckets"
        );

        flip_column_slice(&mut bytes, 1);
        fs::write(&path, &bytes).unwrap();
        assert_eq!(
            store.count_with_filter(0, 127, &covered).unwrap(),
            128,
            "a proved float column is not decoded"
        );
        assert!(store.count_with_filter(0, 127, &partial).is_err());
        assert_eq!(
            store
                .histogram_with_filter(0, 127, 1, &covered)
                .unwrap()
                .iter()
                .map(|bucket| bucket.count)
                .sum::<u64>(),
            128,
            "a proved histogram does not decode the float column"
        );
        assert!(store.histogram_with_filter(0, 127, 1, &partial).is_err());
        let above = [Predicate::Cmp(
            "score".into(),
            CmpOp::Gt,
            Scalar::Float(100.0),
        )];
        assert_eq!(
            store
                .histogram_with_filter(0, 127, 1, &above)
                .unwrap()
                .iter()
                .map(|bucket| bucket.count)
                .sum::<u64>(),
            0,
            "a range the zone rejects reads no payload"
        );
        assert!(store
            .count_with_filter(0, 127, &[Predicate::Eq("score".into(), Scalar::Float(0.0))])
            .is_err());
        assert!(store
            .histogram_with_filter(
                0,
                127,
                1,
                &[Predicate::Eq("score".into(), Scalar::Float(0.0))]
            )
            .is_err());
        store.close().unwrap();
    }

    #[test]
    fn count_refuses_an_uncompressed_len_above_the_query_ceiling() {
        let dir = TempDir::new();
        let schema = write_schema(dir.path());
        let data = dir.path().join("data");
        let options = test_options(2);
        let store = Store::open_with(&data, &schema, options.clone()).unwrap();
        store
            .append_json(&event(10, Some(1), "click", None, "1.00"))
            .unwrap();
        store
            .append_json(&event(20, Some(2), "view", None, "1.00"))
            .unwrap();
        store.close().unwrap();

        patch_block_header(&data, 1, Some(u32::MAX), None);

        let store = Store::open_with(&data, &schema, options).unwrap();
        let click = [Predicate::Eq("action".into(), "click".into())];
        for (label, err) in [
            ("count", store.count(10, 20).unwrap_err()),
            (
                "count_with_filter",
                store.count_with_filter(10, 20, &click).unwrap_err(),
            ),
        ] {
            assert!(
                err.to_string()
                    .contains("query response size limit exceeded"),
                "{label}: {err}"
            );
        }
        store.close().unwrap();
    }

    #[test]
    fn count_rejects_a_header_row_count_the_payload_does_not_match() {
        let dir = TempDir::new();
        let schema = write_schema(dir.path());
        let data = dir.path().join("data");
        let options = test_options(3);
        let store = Store::open_with(&data, &schema, options.clone()).unwrap();
        for ts in [10, 20, 30, 40, 50] {
            store
                .append_json(&event(ts, Some(ts), "click", None, "1.00"))
                .unwrap();
        }
        store.close().unwrap();

        patch_block_header(&data, 1, None, Some(100));

        let store = Store::open_with(&data, &schema, options).unwrap();
        let click = [Predicate::Eq("action".into(), "click".into())];
        for (label, err) in [
            ("count", store.count(10, 50).unwrap_err()),
            (
                "count_with_filter",
                store.count_with_filter(10, 50, &click).unwrap_err(),
            ),
        ] {
            assert!(matches!(err, Error::Corrupt(_)), "{label}: {err}");
            assert!(
                err.to_string()
                    .contains("block row count does not match the frame header"),
                "{label}: {err}"
            );
        }
        store.close().unwrap();
    }

    #[test]
    fn equality_zone_skips_blocks_that_cannot_contain_the_value() {
        let dir = TempDir::new();
        let schema = write_schema(dir.path());
        let data = dir.path().join("data");
        let store = Store::open_with(&data, &schema, test_options(32)).unwrap();
        let runs = 40i64;
        let per_run = 32i64;
        for run in 0..runs {
            for seq in 0..per_run {
                let ts = run * per_run + seq;
                let action = format!("run-{run}");
                store
                    .append_json(&event(ts, Some(run), &action, Some("note"), "1.00"))
                    .unwrap();
            }
        }
        store.flush().unwrap();
        assert!(store.stats().blocks >= runs as u64);

        let predicates = [Predicate::Eq("action".into(), "run-7".into())];
        let resolved = resolve_predicates(store.schema(), &predicates).unwrap();
        let candidates = store.blocks_in_range(0, 10_000_000, &resolved);
        assert_eq!(
            candidates.len(),
            1,
            "one run occupies one block and the zone map should keep only that block"
        );
        let rows = store.query_with_filter(0, 10_000_000, &predicates).unwrap();
        assert_eq!(rows.len(), per_run as usize);
        assert!(rows
            .iter()
            .all(|row| row_value(&store, row)["action"] == "run-7"));

        let missing = [Predicate::Eq("action".into(), "run-missing".into())];
        let resolved = resolve_predicates(store.schema(), &missing).unwrap();
        assert!(store.blocks_in_range(0, 10_000_000, &resolved).is_empty());
        assert!(store
            .query_with_filter(0, 10_000_000, &missing)
            .unwrap()
            .is_empty());

        let user = [Predicate::Eq("user_id".into(), Scalar::Int(7))];
        let resolved = resolve_predicates(store.schema(), &user).unwrap();
        assert_eq!(store.blocks_in_range(0, 10_000_000, &resolved).len(), 1);
        assert_eq!(
            store.query_with_filter(0, 10_000_000, &user).unwrap().len(),
            per_run as usize
        );
        let index_on_disk = dir_suffix_bytes(&data, ".idx") + dir_suffix_bytes(&data, ".zon");
        assert_eq!(
            store.stats().index_bytes,
            index_on_disk,
            "zone bytes stay in the index total"
        );
        assert_eq!(
            store.stats().data_bytes,
            dir_suffix_bytes(&data, ".dat") + dir_suffix_bytes(&data, ".dict"),
            "zone bytes are not counted as data"
        );
        store.close().unwrap();

        let zone_file = crate::zone::zone_path(&data, 1);
        assert!(zone_file.exists());
        let compressed = fs::read(&zone_file).unwrap();
        assert_eq!(&compressed[..4], &[0x28, 0xB5, 0x2F, 0xFD]);
        let raw = zstd::stream::decode_all(compressed.as_slice()).unwrap();
        assert!(compressed.len() < raw.len());
        assert_eq!(&raw[..4], b"EVZN");
        fs::write(&zone_file, &raw).unwrap();
        let reopened = Store::open_with(&data, &schema, test_options(32)).unwrap();
        let resolved = resolve_predicates(reopened.schema(), &predicates).unwrap();
        assert_eq!(reopened.blocks_in_range(0, 10_000_000, &resolved).len(), 1);
        assert_eq!(
            reopened
                .query_with_filter(0, 10_000_000, &predicates)
                .unwrap()
                .len(),
            per_run as usize
        );
        assert_eq!(
            fs::read(&zone_file).unwrap(),
            raw,
            "a raw zone file from an older writer is left in place"
        );
        reopened.close().unwrap();

        let level1 = zstd::bulk::compress(&raw, 1).unwrap();
        assert!(level1.len() < raw.len());
        assert_eq!(&level1[..4], &[0x28, 0xB5, 0x2F, 0xFD]);
        fs::write(&zone_file, &level1).unwrap();
        let reopened = Store::open_with(&data, &schema, test_options(32)).unwrap();
        let resolved = resolve_predicates(reopened.schema(), &predicates).unwrap();
        assert_eq!(reopened.blocks_in_range(0, 10_000_000, &resolved).len(), 1);
        assert_eq!(
            reopened
                .query_with_filter(0, 10_000_000, &predicates)
                .unwrap()
                .len(),
            per_run as usize
        );
        assert_eq!(
            fs::read(&zone_file).unwrap(),
            level1,
            "a level-1 zone frame from an older writer is left in place"
        );
        reopened.close().unwrap();

        fs::write(&zone_file, &compressed[..8]).unwrap();
        let reopened = Store::open_with(&data, &schema, test_options(32)).unwrap();
        let resolved = resolve_predicates(reopened.schema(), &predicates).unwrap();
        assert_eq!(reopened.blocks_in_range(0, 10_000_000, &resolved).len(), 1);
        assert_eq!(
            reopened
                .query_with_filter(0, 10_000_000, &predicates)
                .unwrap()
                .len(),
            per_run as usize
        );
        reopened.close().unwrap();

        fs::remove_file(&zone_file).unwrap();
        let reopened = Store::open_with(&data, &schema, test_options(32)).unwrap();
        let resolved = resolve_predicates(reopened.schema(), &predicates).unwrap();
        assert_eq!(reopened.blocks_in_range(0, 10_000_000, &resolved).len(), 1);
        assert_eq!(
            reopened
                .query_with_filter(0, 10_000_000, &predicates)
                .unwrap()
                .len(),
            per_run as usize
        );
        assert!(zone_file.exists(), "open rebuilds a missing zone map");
        reopened.close().unwrap();

        fs::write(&zone_file, b"not a zone map").unwrap();
        let reopened = Store::open_with(&data, &schema, test_options(32)).unwrap();
        assert_eq!(
            reopened
                .query_with_filter(0, 10_000_000, &predicates)
                .unwrap()
                .len(),
            per_run as usize
        );
        let resolved = resolve_predicates(reopened.schema(), &predicates).unwrap();
        assert_eq!(reopened.blocks_in_range(0, 10_000_000, &resolved).len(), 1);
        reopened.close().unwrap();
    }

    #[test]
    fn version1_zone_opens_and_a_flipped_trailer_is_rebuilt() {
        let dir = TempDir::new();
        let schema = write_schema(dir.path());
        let data = dir.path().join("data");
        let options = test_options(4);
        let store = Store::open_with(&data, &schema, options.clone()).unwrap();
        let count = 12i64;
        for ts in 0..count {
            store
                .append_json(&event(ts, Some(ts), "click", Some("note"), "1.00"))
                .unwrap();
        }
        store.flush().unwrap();
        let expected: Vec<_> = store
            .query(0, count)
            .unwrap()
            .iter()
            .map(|row| row_value(&store, row))
            .collect();
        assert_eq!(expected.len(), count as usize);
        store.close().unwrap();

        let zone_file = crate::zone::zone_path(&data, 1);
        let stored = fs::read(&zone_file).unwrap();
        let plain = if stored.len() >= 4 && stored[..4] == [0x28, 0xB5, 0x2F, 0xFD] {
            zstd::stream::decode_all(stored.as_slice()).unwrap()
        } else {
            stored
        };
        assert_eq!(&plain[..4], b"EVZN");
        assert_eq!(u16::from_le_bytes(plain[4..6].try_into().unwrap()), 3);
        let split = plain.len() - 4;
        assert_eq!(
            crc32fast::hash(&plain[..split]),
            u32::from_le_bytes(plain[split..].try_into().unwrap())
        );

        let v1 = crate::zone::legacy_v1_image(&plain).unwrap();
        fs::write(&zone_file, &v1).unwrap();
        let reopened = Store::open_with(&data, &schema, options.clone()).unwrap();
        let rows: Vec<_> = reopened
            .query(0, count)
            .unwrap()
            .iter()
            .map(|row| row_value(&reopened, row))
            .collect();
        assert_eq!(rows, expected);
        assert_eq!(
            fs::read(&zone_file).unwrap(),
            v1,
            "a version-1 zone file is left in place"
        );
        reopened.close().unwrap();

        let mut flipped = plain.clone();
        let last = flipped.len() - 1;
        flipped[last] ^= 0xff;
        fs::write(&zone_file, &flipped).unwrap();
        let rebuilt = Store::open_with(&data, &schema, options).unwrap();
        let rows: Vec<_> = rebuilt
            .query(0, count)
            .unwrap()
            .iter()
            .map(|row| row_value(&rebuilt, row))
            .collect();
        assert_eq!(rows, expected);
        rebuilt.close().unwrap();
    }

    #[test]
    fn timestamp_equality_skips_a_block_outside_the_index_range() {
        let dir = TempDir::new();
        let schema = write_schema(dir.path());
        let data = dir.path().join("data");
        let options = test_options(2);
        let store = Store::open_with(&data, &schema, options.clone()).unwrap();
        for ts in [10i64, 20, 100, 110] {
            store
                .append_json(&event(ts, Some(ts), "click", None, "1.00"))
                .unwrap();
        }
        store.flush().unwrap();
        let full: Vec<_> = store
            .query(0, 1_000)
            .unwrap()
            .iter()
            .map(|row| row.ts)
            .collect();
        assert_eq!(full, vec![10, 20, 100, 110]);

        let early = [Predicate::Eq("ts".into(), Scalar::Timestamp(10))];
        let late = [Predicate::Eq("ts".into(), Scalar::Timestamp(100))];
        let early_rows = store.query_with_filter(0, 1_000, &early).unwrap();
        assert_eq!(early_rows.len(), 1);
        assert_eq!(early_rows[0].ts, 10);
        let resolved = resolve_predicates(store.schema(), &early).unwrap();
        let candidates = store.blocks_in_range(0, 1_000, &resolved);
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].min_ts, 10);

        store.close().unwrap();
        let zone_file = crate::zone::zone_path(&data, 1);
        fs::remove_file(&zone_file).unwrap();
        let reopened = Store::open_with(&data, &schema, options).unwrap();
        let rebuilt: Vec<_> = reopened
            .query(0, 1_000)
            .unwrap()
            .iter()
            .map(|row| row.ts)
            .collect();
        assert_eq!(rebuilt, full);
        assert_eq!(
            reopened
                .query_with_filter(0, 1_000, &early)
                .unwrap()
                .iter()
                .map(|row| row.ts)
                .collect::<Vec<_>>(),
            vec![10]
        );

        let victim = reopened.catalog().segments[0].blocks[1].clone();
        assert!(victim.min_ts >= 100);
        let path = segment::data_path(&data, victim.segment_id);
        let mut bytes = fs::read(&path).unwrap();
        let flip_at = victim.offset as usize + segment::BLOCK_HEADER_LEN + 4;
        bytes[flip_at] ^= 0xff;
        fs::write(&path, &bytes).unwrap();
        assert_eq!(
            reopened
                .query_with_filter(0, 1_000, &early)
                .unwrap()
                .iter()
                .map(|row| row.ts)
                .collect::<Vec<_>>(),
            vec![10]
        );
        assert!(reopened.query_with_filter(0, 1_000, &late).is_err());
        reopened.close().unwrap();
    }

    fn dir_suffix_bytes(dir: &Path, suffix: &str) -> u64 {
        fs::read_dir(dir)
            .unwrap()
            .map(|entry| entry.unwrap())
            .filter(|entry| entry.file_name().to_string_lossy().ends_with(suffix))
            .map(|entry| entry.metadata().unwrap().len())
            .sum()
    }

    fn block_kinds(path: &Path) -> Vec<([u8; 4], i64, i64)> {
        let data = fs::read(path).unwrap();
        let frames = segment::frames_in(&data);
        let indexed = segment::read_index(&path.with_extension("idx")).unwrap();
        assert_eq!(frames.len(), indexed.len());
        frames
            .into_iter()
            .zip(indexed)
            .map(|(frame, meta)| (frame.magic, meta.min_ts, meta.max_ts))
            .collect()
    }

    #[test]
    fn magicless_segment_reopens_and_returns_every_row() {
        let dir = TempDir::new();
        let schema = write_schema(dir.path());
        let data = dir.path().join("data");
        let mut options = test_options(8);
        options.zstd_level = 3;
        let store = Store::open_with(&data, &schema, options.clone()).unwrap();
        let rows = 24i64;
        for ts in 0..rows {
            store
                .append_json(&event(ts, Some(ts), "click", Some("hello"), "1.00"))
                .unwrap();
        }
        store.flush().unwrap();
        store.close().unwrap();

        let data_file = segment::data_path(&data, 1);
        let payloads = compressed_payloads(&data_file);
        assert!(!payloads.is_empty());
        assert!(
            payloads
                .iter()
                .all(|payload| payload.len() < 4 || payload[..4] != [0x28, 0xB5, 0x2F, 0xFD]),
            "new payloads omit the zstd magic"
        );
        let kinds = block_kinds(&data_file);
        assert!(kinds
            .iter()
            .all(|(magic, _, _)| magic == segment::BLOCK_MAGIC));

        let reopened = Store::open_with(&data, &schema, options).unwrap();
        let got = reopened.query(0, rows).unwrap();
        assert_eq!(got.len(), rows as usize);
        assert_eq!(row_value(&reopened, &got[0])["action"], "click");
        assert_eq!(
            row_value(&reopened, got.last().unwrap())["user_id"],
            rows - 1
        );
        reopened.close().unwrap();
    }

    #[test]
    fn legacy_segment_without_dictionary_round_trips() {
        let dir = TempDir::new();
        let schema_path = write_schema(dir.path());
        let data = dir.path().join("data");
        fs::create_dir_all(&data).unwrap();
        let schema = parse_schema(SCHEMA_JSON).unwrap();
        let rows: Vec<Row> = (0..5)
            .map(|ts| {
                crate::value::parse_event(&schema, &event(ts, Some(ts), "click", None, "1.00"))
                    .unwrap()
            })
            .collect();
        let encoded = crate::codec::encode_block(&schema, &rows).unwrap();
        let compressed = zstd::bulk::compress(&encoded.bytes, 3).unwrap();
        let framed = segment::frame_block_v1(
            &compressed,
            encoded.bytes.len() as u32,
            encoded.row_count,
            encoded.min_ts,
            encoded.max_ts,
            false,
        )
        .unwrap();
        assert_eq!(
            framed.len(),
            segment::BLOCK_HEADER_LEN_V1 + compressed.len()
        );
        assert_eq!(&framed[..4], segment::BLOCK_MAGIC);
        let mut active = segment::ActiveSegment::create_new(&data, 1).unwrap();
        let meta = segment::BlockMeta {
            segment_id: 1,
            offset: 0,
            compressed_len: compressed.len() as u32,
            uncompressed_len: encoded.bytes.len() as u32,
            row_count: encoded.row_count,
            min_ts: encoded.min_ts,
            max_ts: encoded.max_ts,
        };
        active.write_framed(&framed, &meta).unwrap();
        active.flush_os(true).unwrap();
        drop(active);
        assert!(
            segment::read_dictionary(&segment::dictionary_path(&data, 1))
                .unwrap()
                .is_none()
        );

        let store = Store::open_with(&data, &schema_path, test_options(8)).unwrap();
        let got = store.query(0, 10).unwrap();
        assert_eq!(got.len(), 5);
        assert_eq!(row_value(&store, &got[0])["user_id"], 0);
        assert_eq!(row_value(&store, &got[4])["action"], "click");
        assert_eq!(row_value(&store, &got[4])["amount"], "1.00");
        assert!(!segment::dictionary_path(&data, 1).exists());
        let segment_bytes = fs::read(segment::data_path(&data, 1)).unwrap();
        let frames = segment::frames_in(&segment_bytes);
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].header_len, segment::BLOCK_HEADER_LEN_V1);
        assert_eq!(frames[0].min_ts, Some(encoded.min_ts));
        assert_eq!(frames[0].max_ts, Some(encoded.max_ts));
        store.close().unwrap();

        fs::remove_file(segment::index_path(&data, 1)).unwrap();
        let store = Store::open_with(&data, &schema_path, test_options(8)).unwrap();
        assert_eq!(store.query(0, 10).unwrap().len(), 5);
        assert_eq!(
            fs::read(segment::data_path(&data, 1)).unwrap(),
            segment_bytes
        );
        let indexed = segment::read_index(&segment::index_path(&data, 1)).unwrap();
        assert_eq!(indexed[0].min_ts, encoded.min_ts);
        assert_eq!(indexed[0].max_ts, encoded.max_ts);
        store.close().unwrap();

        let dict_path = segment::dictionary_path(&data, 1);
        fs::write(&dict_path, [0u8, 1]).unwrap();
        let store = Store::open_with(&data, &schema_path, test_options(8)).unwrap();
        assert_eq!(store.query(0, 10).unwrap().len(), 5);
        store.close().unwrap();

        let oversized = vec![0u8; segment::DICT_HEADER_LEN + segment::DICT_MAX_BYTES + 1];
        fs::write(&dict_path, &oversized).unwrap();
        let store = Store::open_with(&data, &schema_path, test_options(8)).unwrap();
        assert_eq!(store.query(0, 10).unwrap().len(), 5);
        store.close().unwrap();
    }

    #[test]
    fn legacy_dictionary_header_round_trips_after_the_index_is_removed() {
        let dir = TempDir::new();
        let schema_path = write_schema(dir.path());
        let data = dir.path().join("data");
        fs::create_dir_all(&data).unwrap();
        let schema = parse_schema(SCHEMA_JSON).unwrap();
        let rows: Vec<Row> = (0..4)
            .map(|ts| {
                crate::value::parse_event(&schema, &event(10 + ts, Some(ts), "view", None, "2.00"))
                    .unwrap()
            })
            .collect();
        let encoded = crate::codec::encode_block(&schema, &rows).unwrap();
        let dict = vec![0x11u8; 128];
        let mut compressor = zstd::bulk::Compressor::with_dictionary(1, &dict).unwrap();
        let compressed = compressor.compress(&encoded.bytes).unwrap();
        let framed = segment::frame_block_v1(
            &compressed,
            encoded.bytes.len() as u32,
            encoded.row_count,
            encoded.min_ts,
            encoded.max_ts,
            true,
        )
        .unwrap();
        assert_eq!(&framed[..4], segment::BLOCK_MAGIC_DICT);
        segment::write_dictionary(&data, 1, &dict).unwrap();
        let mut active = segment::ActiveSegment::create_new(&data, 1).unwrap();
        let meta = segment::BlockMeta {
            segment_id: 1,
            offset: 0,
            compressed_len: compressed.len() as u32,
            uncompressed_len: encoded.bytes.len() as u32,
            row_count: encoded.row_count,
            min_ts: encoded.min_ts,
            max_ts: encoded.max_ts,
        };
        active.write_framed(&framed, &meta).unwrap();
        active.flush_os(true).unwrap();
        drop(active);

        let segment_bytes = fs::read(segment::data_path(&data, 1)).unwrap();
        let frames = segment::frames_in(&segment_bytes);
        assert_eq!(frames[0].header_len, segment::BLOCK_HEADER_LEN_V1);
        assert_eq!(frames[0].magic, *segment::BLOCK_MAGIC_DICT);

        fs::remove_file(segment::index_path(&data, 1)).unwrap();
        let store = Store::open_with(&data, &schema_path, test_options(8)).unwrap();
        let got = store.query(0, 100).unwrap();
        assert_eq!(got.len(), 4);
        assert_eq!(row_value(&store, &got[0])["ts"], 10);
        assert_eq!(row_value(&store, &got[3])["user_id"], 3);
        assert_eq!(
            fs::read(segment::data_path(&data, 1)).unwrap(),
            segment_bytes
        );
        let indexed = segment::read_index(&segment::index_path(&data, 1)).unwrap();
        assert_eq!(indexed[0].min_ts, encoded.min_ts);
        assert_eq!(indexed[0].max_ts, encoded.max_ts);
        store.close().unwrap();
    }

    fn write_until_dictionary(dir: &Path) -> (PathBuf, StoreOptions, i64) {
        let schema = write_schema(dir);
        let data = dir.join("data");
        let mut options = test_options(2048);
        options.zstd_level = 3;
        let store = Store::open_with(&data, &schema, options.clone()).unwrap();
        let dict_path = segment::dictionary_path(&data, 1);
        let mut n = 0i64;
        while !dict_path.exists() {
            for _ in 0..2048 {
                store
                    .append_json(&event(n, Some(n % 50), "click", Some("hello"), "19.99"))
                    .unwrap();
                n += 1;
            }
            store.flush().unwrap();
            // Compact integer blocks on current main are about 1.6 KiB, so
            // filling DICT_SAMPLE_MAX (256 KiB) takes well past 100k rows.
            assert!(n <= 1_048_576, "dictionary was not trained");
        }
        for _ in 0..2048 {
            store
                .append_json(&event(n, Some(n % 50), "click", Some("hello"), "19.99"))
                .unwrap();
            n += 1;
        }
        store.flush().unwrap();
        store.close().unwrap();
        (schema, options, n)
    }

    #[test]
    fn dictionary_segment_round_trips_across_the_training_boundary() {
        let dir = TempDir::new();
        let (schema, options, n) = write_until_dictionary(dir.path());
        let data = dir.path().join("data");
        let data_file = segment::data_path(&data, 1);
        let dict_file = segment::dictionary_path(&data, 1);
        let kinds = block_kinds(&data_file);
        let first_dict = kinds
            .iter()
            .position(|(magic, _, _)| magic == segment::BLOCK_MAGIC_DICT)
            .expect("dictionary frame");
        assert!(
            first_dict > 0,
            "blocks before the dictionary must stay plain"
        );
        assert!(kinds
            .iter()
            .any(|(magic, _, _)| magic == segment::BLOCK_MAGIC));
        let span_from = kinds[first_dict - 1].1;
        let span_to = kinds[first_dict].2;
        assert!(span_from <= kinds[first_dict].1);

        let store = Store::open_with(&data, &schema, options.clone()).unwrap();
        let spanned = store.query(span_from, span_to).unwrap();
        assert_eq!(spanned.len(), (span_to - span_from + 1) as usize);
        assert_eq!(row_value(&store, &spanned[0])["ts"], span_from);
        assert_eq!(
            row_value(&store, spanned.last().unwrap())["user_id"],
            span_to % 50
        );
        assert_eq!(store.query(0, n).unwrap().len(), n as usize);
        let data_len = fs::metadata(&data_file).unwrap().len();
        let dict_len = fs::metadata(&dict_file).unwrap().len();
        let dict_bytes = fs::read(&dict_file).unwrap();
        assert_eq!(&dict_bytes[..4], &[0x28, 0xB5, 0x2F, 0xFD]);
        assert_eq!(store.stats().data_bytes, data_len + dict_len);
        assert!(dict_len > segment::DICT_HEADER_LEN as u64);
        assert!(dict_len <= (segment::DICT_HEADER_LEN + segment::DICT_MAX_BYTES) as u64);
        store.close().unwrap();

        let reopened = Store::open_with(&data, &schema, options.clone()).unwrap();
        assert_eq!(
            reopened.query(span_from, span_to).unwrap().len(),
            spanned.len()
        );
        assert_eq!(reopened.query(0, n - 1).unwrap().len(), n as usize);
        assert_eq!(reopened.stats().data_bytes, data_len + dict_len);
        reopened.close().unwrap();

        let trained = segment::read_dictionary(&dict_file).unwrap().unwrap().bytes;
        let mut raw_sidecar = Vec::with_capacity(segment::DICT_HEADER_LEN + trained.len());
        raw_sidecar.extend_from_slice(segment::DICT_MAGIC);
        raw_sidecar.extend_from_slice(&segment::DICT_VERSION.to_le_bytes());
        raw_sidecar.extend_from_slice(&0u16.to_le_bytes());
        raw_sidecar.extend_from_slice(&(trained.len() as u32).to_le_bytes());
        raw_sidecar.extend_from_slice(&crc32fast::hash(&trained).to_le_bytes());
        raw_sidecar.extend_from_slice(&trained);
        fs::write(&dict_file, &raw_sidecar).unwrap();
        let legacy = Store::open_with(&data, &schema, options).unwrap();
        assert_eq!(legacy.query(0, n - 1).unwrap().len(), n as usize);
        assert_eq!(
            legacy.stats().data_bytes,
            data_len + raw_sidecar.len() as u64
        );
        legacy.close().unwrap();
    }

    #[test]
    fn flush_frames_training_sample_with_dictionary() {
        let dir = TempDir::new();
        let schema = write_schema(dir.path());
        let data = dir.path().join("data");
        let mut options = test_options(128);
        options.zstd_level = 3;
        let store = Store::open_with(&data, &schema, options.clone()).unwrap();
        // Unique text so the sealed blocks stay large enough to fill the 256 KiB sample.
        let rows = 128 * 16;
        for n in 0..rows {
            let note = format!("{n:04}-{}", "n".repeat(300));
            store
                .append_json(&event(n, Some(n % 50), "click", Some(&note), "19.99"))
                .unwrap();
        }
        store.flush().unwrap();
        store.close().unwrap();

        let data_file = segment::data_path(&data, 1);
        let kinds = block_kinds(&data_file);
        assert!(
            kinds.len() > 1,
            "expected the sample to span more than one block"
        );
        assert!(
            kinds
                .iter()
                .all(|(magic, _, _)| magic == segment::BLOCK_MAGIC_DICT),
            "training-sample blocks must be EVBD after one flush: {kinds:?}"
        );
        let payloads = compressed_payloads(&data_file);
        assert!(
            payloads
                .iter()
                .all(|payload| payload.len() < 4 || payload[..4] != [0x28, 0xB5, 0x2F, 0xFD]),
            "new EVBD payloads are magicless zstd frames"
        );
        let dict_file = segment::dictionary_path(&data, 1);
        let dict_len = fs::metadata(&dict_file).unwrap().len();
        assert!(dict_len > segment::DICT_HEADER_LEN as u64);
        assert!(dict_len <= (segment::DICT_HEADER_LEN + segment::DICT_MAX_BYTES) as u64);

        let store = Store::open_with(&data, &schema, options).unwrap();
        assert_eq!(store.query(0, rows).unwrap().len(), rows as usize);
        let data_len = fs::metadata(&data_file).unwrap().len();
        assert_eq!(store.stats().data_bytes, data_len + dict_len);
        store.close().unwrap();
    }

    #[test]
    fn truncated_or_missing_dictionary_is_corrupt() {
        let dir = TempDir::new();
        let (schema, options, n) = write_until_dictionary(dir.path());
        let data = dir.path().join("data");
        let dict_file = segment::dictionary_path(&data, 1);
        let original = fs::read(&dict_file).unwrap();
        assert!(original.len() > 1);
        fs::write(&dict_file, &original[..original.len() - 1]).unwrap();

        match Store::open_with(&data, &schema, options.clone()) {
            Err(err) => assert!(
                err.to_string().contains("corrupt"),
                "truncated dictionary opened: {err}"
            ),
            Ok(_) => panic!("truncated dictionary opened"),
        }

        fs::write(&dict_file, &original).unwrap();
        let store = Store::open_with(&data, &schema, options.clone()).unwrap();
        fs::write(&dict_file, &original[..8]).unwrap();
        let queried = store.query(0, n).unwrap_err();
        assert!(
            queried.to_string().contains("corrupt"),
            "truncated dictionary queried: {queried}"
        );
        store.close().unwrap();

        fs::write(&dict_file, &original).unwrap();
        let store = Store::open_with(&data, &schema, options.clone()).unwrap();
        fs::remove_file(&dict_file).unwrap();
        let missing = store.query(0, n).unwrap_err();
        assert!(
            missing.to_string().contains("corrupt"),
            "missing dictionary queried: {missing}"
        );
        store.close().unwrap();

        match Store::open_with(&data, &schema, options) {
            Err(err) => assert!(
                err.to_string().contains("corrupt"),
                "missing dictionary opened: {err}"
            ),
            Ok(_) => panic!("missing dictionary opened"),
        }
    }

    fn compressed_payloads(path: &Path) -> Vec<Vec<u8>> {
        let data = fs::read(path).unwrap();
        let mut off = 0usize;
        let mut out = Vec::new();
        for frame in segment::frames_in(&data) {
            let compressed_at = if frame.header_len == segment::BLOCK_HEADER_LEN {
                off + 4
            } else {
                off + 8
            };
            let compressed_len =
                u32::from_le_bytes(data[compressed_at..compressed_at + 4].try_into().unwrap())
                    as usize;
            let start = off + frame.header_len;
            let end = start + compressed_len;
            assert!(end <= data.len());
            out.push(data[start..end].to_vec());
            off = end;
        }
        out
    }

    fn dir_file_lengths(dir: &Path) -> Vec<(String, u64)> {
        let mut out = Vec::new();
        for entry in fs::read_dir(dir).unwrap() {
            let entry = entry.unwrap();
            out.push((
                entry.file_name().to_string_lossy().into_owned(),
                entry.metadata().unwrap().len(),
            ));
        }
        out.sort();
        out
    }

    #[test]
    fn drop_blocks_before_removes_old_blocks_and_keeps_the_rest() {
        let dir = TempDir::new();
        let schema = write_schema(dir.path());
        let data = dir.path().join("data");
        let store = Store::open_with(&data, &schema, test_options(1)).unwrap();
        for ts in [1_000, 2_000, 3_000] {
            store
                .append_json(&event(ts, Some(ts), "click", None, "1.00"))
                .unwrap();
        }
        store.flush().unwrap();
        let data_file = segment::data_path(&data, 1);
        let before = compressed_payloads(&data_file);
        assert_eq!(before.len(), 3);
        let kept = before[1..].to_vec();

        // max_ts of the first block is 1000. A cutoff equal to that max keeps it.
        let unchanged = dir_file_lengths(&data);
        store.drop_blocks_before(1_000).unwrap();
        assert_eq!(dir_file_lengths(&data), unchanged);
        assert_eq!(store.query(0, 10_000).unwrap().len(), 3);

        store.drop_blocks_before(1_001).unwrap();
        let rows = store.query(0, 10_000).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(row_value(&store, &rows[0])["ts"], 2_000);
        assert_eq!(row_value(&store, &rows[1])["ts"], 3_000);
        assert_eq!(compressed_payloads(&data_file), kept);
        assert_eq!(store.stats().blocks, 2);
        assert_eq!(store.stats().rows, 2);
        store.close().unwrap();

        let reopened = Store::open_with(&data, &schema, test_options(1)).unwrap();
        let rows = reopened.query(0, 10_000).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(row_value(&reopened, &rows[0])["ts"], 2_000);
        assert_eq!(row_value(&reopened, &rows[1])["ts"], 3_000);
        assert_eq!(compressed_payloads(&data_file), kept);
        assert!(
            fs::read_dir(&data).unwrap().all(|entry| {
                !entry
                    .unwrap()
                    .file_name()
                    .to_string_lossy()
                    .contains("partial")
            }),
            "a partial temp must not remain as a segment"
        );
        reopened.close().unwrap();
    }

    #[test]
    fn row_older_than_cutoff_stays_when_its_block_is_kept() {
        let dir = TempDir::new();
        let schema = write_schema(dir.path());
        let data = dir.path().join("data");
        let store = Store::open_with(&data, &schema, test_options(2)).unwrap();
        for ts in [10, 30, 40, 50] {
            store
                .append_json(&event(ts, Some(ts), "click", None, "1.00"))
                .unwrap();
        }
        store.flush().unwrap();
        // First block max is 30, second is 50. Cutoff 35 drops only the first block.
        store.drop_blocks_before(35).unwrap();
        let rows = store.query(0, 100).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(row_value(&store, &rows[0])["ts"], 40);
        assert_eq!(row_value(&store, &rows[1])["ts"], 50);

        // 10 is below the cutoff and shares a block with 30, whose max is not eligible.
        store
            .append_json(&event(10, Some(1), "view", None, "1.00"))
            .unwrap();
        store
            .append_json(&event(30, Some(2), "view", None, "1.00"))
            .unwrap();
        store.flush().unwrap();
        store.drop_blocks_before(20).unwrap();
        let kept = store.query(0, 20).unwrap();
        assert!(
            kept.iter().any(|row| row_value(&store, row)["ts"] == 10),
            "a row below the cutoff stays inside a block that is not eligible"
        );
        store.close().unwrap();
    }

    #[test]
    fn drop_blocks_before_merges_slices_of_the_same_segment() {
        // One frame is 90 bytes. Rotating at 100 bytes inside the second flush
        // leaves four catalog entries for two data files: the first commit's
        // block, an empty placeholder, then the two frames of that flush.
        let open = |dir: &TempDir| {
            let schema = write_schema(dir.path());
            let data = dir.path().join("data");
            let mut options = test_options(1);
            options.segment_bytes = 100;
            let store = Store::open_with(&data, &schema, options).unwrap();
            store
                .append_json(&event(1_000, Some(1), "click", None, "1.00"))
                .unwrap();
            store.flush().unwrap();
            store
                .append_json(&event(2_000, Some(2), "view", None, "2.00"))
                .unwrap();
            store
                .append_json(&event(3_000, Some(3), "buy", None, "3.00"))
                .unwrap();
            store.flush().unwrap();
            assert_eq!(store.stats().segments, 4, "split catalog entries");
            assert!(segment::data_path(&data, 1).exists());
            assert!(segment::data_path(&data, 2).exists());
            assert!(!segment::data_path(&data, 3).exists());
            (schema, data, store)
        };

        let dir = TempDir::new();
        let (schema, data, store) = open(&dir);
        store.drop_blocks_before(i64::MIN).unwrap();
        assert_eq!(store.query(0, 10_000).unwrap().len(), 3);
        assert_eq!(store.stats().segments, 2);
        store.close().unwrap();
        let reopened = Store::open_with(&data, &schema, test_options(1)).unwrap();
        assert_eq!(reopened.query(0, 10_000).unwrap().len(), 3);
        reopened.close().unwrap();

        let dir = TempDir::new();
        let (schema, data, store) = open(&dir);
        store.drop_blocks_before(1_001).unwrap();
        assert!(
            segment::data_path(&data, 1).exists(),
            "segment 1 still holds the block at ts 2000"
        );
        let rows = store.query(0, 10_000).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(row_value(&store, &rows[0])["ts"], 2_000);
        assert_eq!(row_value(&store, &rows[1])["ts"], 3_000);
        store.close().unwrap();
        let reopened = Store::open_with(&data, &schema, test_options(1)).unwrap();
        assert_eq!(reopened.query(0, 10_000).unwrap().len(), 2);
        reopened.close().unwrap();
    }

    #[test]
    fn drop_blocks_before_deletes_a_fully_expired_segment() {
        let dir = TempDir::new();
        let schema = write_schema(dir.path());
        let data = dir.path().join("data");
        let mut options = test_options(1);
        options.segment_bytes = 1;
        let store = Store::open_with(&data, &schema, options).unwrap();
        store
            .append_json(&event(1_000, Some(1), "click", None, "1.00"))
            .unwrap();
        store
            .append_json(&event(2_000, Some(2), "view", None, "2.00"))
            .unwrap();
        store.flush().unwrap();
        assert!(store.stats().segments >= 2);
        assert!(segment::data_path(&data, 1).exists());
        assert!(segment::data_path(&data, 2).exists());

        store.drop_blocks_before(1_001).unwrap();
        for path in [
            segment::data_path(&data, 1),
            segment::index_path(&data, 1),
            crate::zone::zone_path(&data, 1),
            segment::dictionary_path(&data, 1),
        ] {
            assert!(
                !path.exists(),
                "expired segment file still present: {path:?}"
            );
        }
        assert!(segment::data_path(&data, 2).exists());
        assert_eq!(store.query(0, 10_000).unwrap().len(), 1);

        store
            .append_json(&event(3_000, Some(3), "buy", None, "3.00"))
            .unwrap();
        let rows = store.query(0, 10_000).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(row_value(&store, &rows[0])["ts"], 2_000);
        assert_eq!(row_value(&store, &rows[1])["ts"], 3_000);
        store.close().unwrap();
    }

    #[test]
    fn drop_blocks_before_keeps_catalog_when_a_later_segment_fails() {
        let dir = TempDir::new();
        let schema = write_schema(dir.path());
        let data = dir.path().join("data");
        let mut options = test_options(1);
        options.segment_bytes = 1;
        let store = Store::open_with(&data, &schema, options).unwrap();
        for ts in [1_000, 2_000, 3_000] {
            store
                .append_json(&event(ts, Some(ts), "click", None, "1.00"))
                .unwrap();
        }
        store.flush().unwrap();
        let present: Vec<u32> = (1..8)
            .filter(|id| segment::data_path(&data, *id).exists())
            .collect();
        assert!(
            present.len() >= 3,
            "expected one data file per event, found {present:?}"
        );
        let blocked = segment::data_path(&data, present[1]);
        fs::remove_file(&blocked).unwrap();
        fs::create_dir(&blocked).unwrap();

        let err = store.drop_blocks_before(i64::MAX).unwrap_err();
        assert!(
            matches!(err, crate::error::Error::Io(_)),
            "expected the blocked unlink to fail the drop, got {err:?}"
        );
        assert!(
            !segment::data_path(&data, present[0]).exists(),
            "the segment published before the error must stay deleted"
        );
        // `query` flushes first and observes the poison flag. Stats read the
        // catalog directly, which is what a query would walk if it ignored poison.
        assert_eq!(
            store.stats().rows,
            1,
            "deleted and failed segments must leave the catalog; the unvisited one stays"
        );
        store.close().unwrap();
    }

    #[test]
    fn drop_blocks_before_respects_cutoffs_outside_the_store() {
        let dir = TempDir::new();
        let schema = write_schema(dir.path());
        let data = dir.path().join("data");
        let store = Store::open_with(&data, &schema, test_options(1)).unwrap();
        for ts in [1_000, 2_000, 3_000] {
            store
                .append_json(&event(ts, Some(ts), "click", None, "1.00"))
                .unwrap();
        }
        store.flush().unwrap();
        let before = dir_file_lengths(&data);
        store.drop_blocks_before(i64::MIN).unwrap();
        assert_eq!(
            dir_file_lengths(&data),
            before,
            "a cutoff older than every block changes no file lengths"
        );
        assert_eq!(store.query(0, 10_000).unwrap().len(), 3);

        store.drop_blocks_before(i64::MAX).unwrap();
        assert!(store.query(0, 10_000).unwrap().is_empty());
        assert_eq!(store.stats().rows, 0);
        assert_eq!(store.stats().segments, 0);
        let names: Vec<_> = fs::read_dir(&data)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        assert!(
            names.iter().all(|name| !name.starts_with("seg-")),
            "expired store still has segment files: {names:?}"
        );
        store
            .append_json(&event(4_000, Some(4), "click", None, "1.00"))
            .unwrap();
        assert_eq!(store.query(0, 10_000).unwrap().len(), 1);
        store.close().unwrap();

        let reopened = Store::open_with(&data, &schema, test_options(1)).unwrap();
        let rows = reopened.query(0, 10_000).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(row_value(&reopened, &rows[0])["ts"], 4_000);
        reopened.close().unwrap();
    }

    #[test]
    fn open_ignores_a_retention_partial_and_keeps_dictionary_frames() {
        let dir = TempDir::new();
        let schema_path = write_schema(dir.path());
        let data = dir.path().join("data");
        fs::create_dir_all(&data).unwrap();
        let schema = parse_schema(SCHEMA_JSON).unwrap();
        let row = |ts: i64| {
            crate::value::parse_event(&schema, &event(ts, Some(ts), "click", None, "1.00")).unwrap()
        };
        let first = crate::codec::encode_block(&schema, &[row(1_000)]).unwrap();
        let second = crate::codec::encode_block(&schema, &[row(2_000)]).unwrap();
        let plain = zstd::bulk::compress(&first.bytes, 1).unwrap();
        let dict = vec![9u8; 128];
        let compressed = zstd::bulk::Compressor::with_dictionary(1, &dict)
            .unwrap()
            .compress(&second.bytes)
            .unwrap();
        let frame1 = segment::frame_block_v1(
            &plain,
            first.bytes.len() as u32,
            first.row_count,
            first.min_ts,
            first.max_ts,
            false,
        )
        .unwrap();
        let frame2 = segment::frame_block_v1(
            &compressed,
            second.bytes.len() as u32,
            second.row_count,
            second.min_ts,
            second.max_ts,
            true,
        )
        .unwrap();
        assert_eq!(&frame2[..4], segment::BLOCK_MAGIC_DICT);
        let mut active = segment::ActiveSegment::create_new(&data, 1).unwrap();
        let meta1 = segment::BlockMeta {
            segment_id: 1,
            offset: 0,
            compressed_len: plain.len() as u32,
            uncompressed_len: first.bytes.len() as u32,
            row_count: first.row_count,
            min_ts: first.min_ts,
            max_ts: first.max_ts,
        };
        let meta2 = segment::BlockMeta {
            segment_id: 1,
            offset: frame1.len() as u64,
            compressed_len: compressed.len() as u32,
            uncompressed_len: second.bytes.len() as u32,
            row_count: second.row_count,
            min_ts: second.min_ts,
            max_ts: second.max_ts,
        };
        active.write_framed(&frame1, &meta1).unwrap();
        active.write_framed(&frame2, &meta2).unwrap();
        active.flush_os(true).unwrap();
        drop(active);
        segment::write_dictionary(&data, 1, &dict).unwrap();
        fs::write(data.join(".seg-000001.dat.partial"), b"torn").unwrap();
        fs::write(data.join("seg-000001.dat.partial"), b"torn").unwrap();

        let store = Store::open_with(&data, &schema_path, test_options(1)).unwrap();
        assert_eq!(store.query(0, 10_000).unwrap().len(), 2);
        let survivor = compressed_payloads(&segment::data_path(&data, 1))[1].clone();
        store.drop_blocks_before(1_001).unwrap();
        assert_eq!(
            compressed_payloads(&segment::data_path(&data, 1)),
            vec![survivor]
        );
        assert!(segment::dictionary_path(&data, 1).exists());
        let rows = store.query(0, 10_000).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(row_value(&store, &rows[0])["ts"], 2_000);
        assert!(
            !data.join(".seg-000001.dat.partial").exists(),
            "rename publishes the segment; the temp file is not left behind"
        );
        assert!(
            data.join("seg-000001.dat.partial").exists(),
            "a name open does not list can sit beside the segment"
        );
        store.close().unwrap();

        let reopened = Store::open_with(&data, &schema_path, test_options(1)).unwrap();
        assert_eq!(reopened.query(0, 10_000).unwrap().len(), 1);
        reopened.drop_blocks_before(i64::MAX).unwrap();
        assert!(!segment::dictionary_path(&data, 1).exists());
        assert!(!segment::data_path(&data, 1).exists());
        reopened.close().unwrap();
    }

    fn snapshot_tree(dir: &Path) -> std::collections::BTreeMap<String, Vec<u8>> {
        fn walk(dir: &Path, prefix: &str, out: &mut std::collections::BTreeMap<String, Vec<u8>>) {
            let mut entries: Vec<_> = fs::read_dir(dir)
                .unwrap()
                .map(|entry| entry.unwrap())
                .collect();
            entries.sort_by_key(|entry| entry.file_name());
            for entry in entries {
                let name = entry.file_name().to_string_lossy().into_owned();
                let rel = if prefix.is_empty() {
                    name
                } else {
                    format!("{prefix}/{}", entry.file_name().to_string_lossy())
                };
                let path = entry.path();
                if path.is_dir() {
                    walk(&path, &rel, out);
                } else {
                    out.insert(rel, fs::read(&path).unwrap());
                }
            }
        }
        let mut out = std::collections::BTreeMap::new();
        if dir.exists() {
            walk(dir, "", &mut out);
        }
        out
    }

    fn frame_v20(compressed: &[u8], uncompressed_len: u32, row_count: u32) -> Vec<u8> {
        let mut out = Vec::with_capacity(segment::BLOCK_HEADER_LEN_V20 + compressed.len());
        out.extend_from_slice(segment::BLOCK_MAGIC);
        out.extend_from_slice(&uncompressed_len.to_le_bytes());
        out.extend_from_slice(&(compressed.len() as u32).to_le_bytes());
        out.extend_from_slice(&row_count.to_le_bytes());
        out.extend_from_slice(&crc32fast::hash(compressed).to_le_bytes());
        out.extend_from_slice(compressed);
        out
    }

    fn seal_rows(schema: &Schema, rows: &[Row]) -> crate::codec::EncodedBlock {
        crate::codec::encode_block(schema, rows).unwrap()
    }

    fn install_schema_lock(dir: &Path, text: &str) {
        fs::create_dir_all(dir).unwrap();
        let schema = parse_schema(text).unwrap();
        fs::write(dir.join("schema.lock"), schema.canonical()).unwrap();
    }

    fn assert_change(report: &RewriteReport) {
        let data = relative_change(
            report.source.stored_data_bytes,
            report.destination.stored_data_bytes,
        );
        let total = relative_change_sum(
            report.source.stored_data_bytes,
            report.source.stored_index_bytes,
            report.destination.stored_data_bytes,
            report.destination.stored_index_bytes,
        );
        assert_eq!(report.data_bytes_change, data);
        assert_eq!(report.stored_bytes_change, total);
    }

    fn query_bytes(dir: &Path, schema: &Path) -> Vec<Vec<u8>> {
        let store = Store::open(dir, schema).unwrap();
        let rows = store.query(i64::MIN, i64::MAX).unwrap();
        let bytes = rows
            .iter()
            .map(|row| row_to_json_bytes(store.schema(), row).unwrap())
            .collect();
        store.close().unwrap();
        bytes
    }

    #[test]
    fn rewrite_directory_turns_a_36_byte_segment_into_the_current_format() {
        let dir = TempDir::new();
        let schema = parse_schema(SCHEMA_JSON).unwrap();
        let data = dir.path().join("data");
        install_schema_lock(&data, SCHEMA_JSON);
        let rows: Vec<Row> = (0..5)
            .map(|ts| {
                crate::value::parse_event(&schema, &event(ts, Some(ts), "click", None, "1.00"))
                    .unwrap()
            })
            .collect();
        let encoded = seal_rows(&schema, &rows);
        let compressed = zstd::bulk::compress(&encoded.bytes, 3).unwrap();
        assert_eq!(&compressed[..4], &[0x28, 0xB5, 0x2F, 0xFD]);
        let framed = segment::frame_block_v1(
            &compressed,
            encoded.bytes.len() as u32,
            encoded.row_count,
            encoded.min_ts,
            encoded.max_ts,
            false,
        )
        .unwrap();
        fs::write(segment::data_path(&data, 1), &framed).unwrap();
        assert!(segment::frames_in(&framed)[0].header_len == segment::BLOCK_HEADER_LEN_V1);
        assert!(!crate::zone::zone_path(&data, 1).exists());
        assert!(!segment::dictionary_path(&data, 1).exists());
        let before = snapshot_tree(&data);
        let expected: Vec<_> = rows
            .iter()
            .map(|row| row_to_json_bytes(&schema, row).unwrap())
            .collect();

        let dest = dir.path().join("rewritten");
        let report = Store::rewrite_directory(&data, &dest).unwrap();
        assert_eq!(snapshot_tree(&data), before, "source files changed");
        assert_eq!(
            report.source,
            directory_stored_bytes(&data).unwrap(),
            "source report does not match the directory"
        );
        assert_eq!(report.destination, directory_stored_bytes(&dest).unwrap());
        assert_change(&report);
        assert!(
            report.data_bytes_change > 0.0
                || report.destination.stored_data_bytes <= report.source.stored_data_bytes
        );

        let schema_file = data.join("schema.lock");
        let got = query_bytes(&dest, &schema_file);
        assert_eq!(got, expected);
        let reopened = query_bytes(&dest, &schema_file);
        assert_eq!(reopened, expected);
        let dest_bytes = fs::read(segment::data_path(&dest, 1)).unwrap();
        let frames = segment::frames_in(&dest_bytes);
        assert!(!frames.is_empty());
        assert!(frames
            .iter()
            .all(|frame| frame.header_len == segment::BLOCK_HEADER_LEN));
        let opened = Store::open(&dest, &schema_file).unwrap();
        assert_eq!(opened.stats().rows, 5);
        assert_eq!(
            opened.stats().data_bytes,
            report.destination.stored_data_bytes
        );
        assert_eq!(
            opened.stats().index_bytes,
            report.destination.stored_index_bytes
        );
        opened.close().unwrap();
    }

    #[test]
    fn rewrite_directory_copies_mixed_headers_without_touching_the_source() {
        let dir = TempDir::new();
        let schema = parse_schema(SCHEMA_JSON).unwrap();
        let data = dir.path().join("data");
        install_schema_lock(&data, SCHEMA_JSON);
        let blocks: Vec<Vec<Row>> = (0..3)
            .map(|block| {
                (0..2)
                    .map(|row| {
                        let ts = block * 10 + row;
                        crate::value::parse_event(
                            &schema,
                            &event(ts, Some(ts), "click", Some("n"), "1.00"),
                        )
                        .unwrap()
                    })
                    .collect()
            })
            .collect();
        let encoded: Vec<_> = blocks.iter().map(|rows| seal_rows(&schema, rows)).collect();
        let magic36 = zstd::bulk::compress(&encoded[0].bytes, 3).unwrap();
        let frame36 = segment::frame_block_v1(
            &magic36,
            encoded[0].bytes.len() as u32,
            encoded[0].row_count,
            encoded[0].min_ts,
            encoded[0].max_ts,
            false,
        )
        .unwrap();
        let magic20 = zstd::bulk::compress(&encoded[1].bytes, 1).unwrap();
        let frame20 = frame_v20(
            &magic20,
            encoded[1].bytes.len() as u32,
            encoded[1].row_count,
        );
        let dict = vec![0x5Au8; 128];
        let mut compressor = segment::block_compressor(3, &dict).unwrap();
        let magicless = compressor.compress(&encoded[2].bytes).unwrap();
        assert!(magicless.len() < 4 || magicless[..4] != [0x28, 0xB5, 0x2F, 0xFD]);
        let frame12 = segment::frame_block(&magicless, true).unwrap();
        assert_eq!(frame12.len(), segment::BLOCK_HEADER_LEN + magicless.len());
        assert_eq!(&frame12[..4], segment::BLOCK_MAGIC_DICT);
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&frame36);
        bytes.extend_from_slice(&frame20);
        bytes.extend_from_slice(&frame12);
        fs::write(segment::data_path(&data, 1), &bytes).unwrap();
        segment::write_dictionary(&data, 1, &dict).unwrap();
        let frames = segment::frames_in(&bytes);
        assert_eq!(frames.len(), 3);
        assert_eq!(frames[0].header_len, segment::BLOCK_HEADER_LEN_V1);
        assert_eq!(frames[1].header_len, segment::BLOCK_HEADER_LEN_V20);
        assert_eq!(frames[2].header_len, segment::BLOCK_HEADER_LEN);
        assert_eq!(frames[2].magic, *segment::BLOCK_MAGIC_DICT);
        let before = snapshot_tree(&data);
        let expected: Vec<_> = blocks
            .iter()
            .flatten()
            .map(|row| row_to_json_bytes(&schema, row).unwrap())
            .collect();

        let dest = dir.path().join("rewritten");
        let report = Store::rewrite_directory(&data, &dest).unwrap();
        assert_eq!(snapshot_tree(&data), before);
        assert_eq!(report.source, directory_stored_bytes(&data).unwrap());
        assert_eq!(report.destination, directory_stored_bytes(&dest).unwrap());
        assert_change(&report);
        let schema_file = data.join("schema.lock");
        assert_eq!(query_bytes(&dest, &schema_file), expected);
        let reopened = Store::open(&dest, &schema_file).unwrap();
        assert_eq!(reopened.query(i64::MIN, i64::MAX).unwrap().len(), 6);
        reopened.close().unwrap();
        assert_eq!(query_bytes(&dest, &schema_file), expected);
    }

    #[test]
    fn rewrite_directory_keeps_json_field_text_and_appended_nulls() {
        let dir = TempDir::new();
        let text = r#"{
            "timestamp_field": "ts",
            "fields": [
                {"name": "ts", "type": "timestamp"},
                {"name": "props", "type": "json"}
            ]
        }"#;
        let schema_path = dir.path().join("schema.json");
        fs::write(&schema_path, text).unwrap();
        let data = dir.path().join("data");
        let store = Store::open_with(&data, &schema_path, test_options(4)).unwrap();
        let raw = br#"{"ts":5,"props":{"a":1,"a":2,"b":{"z":1,"z":3}}}"#;
        store.append_json(raw).unwrap();
        store.flush().unwrap();
        store.close().unwrap();

        let wide = r#"{
            "timestamp_field": "ts",
            "fields": [
                {"name": "ts", "type": "timestamp"},
                {"name": "props", "type": "json"},
                {"name": "region", "type": "string"}
            ]
        }"#;
        let wide_path = dir.path().join("wide.json");
        fs::write(&wide_path, wide).unwrap();
        let store = Store::open_with(&data, &wide_path, test_options(4)).unwrap();
        store
            .append_json(br#"{"ts":9,"props":[1,2,3],"region":"us"}"#)
            .unwrap();
        store.flush().unwrap();
        store.close().unwrap();
        let expected = query_bytes(&data, &wide_path);
        let before = snapshot_tree(&data);

        let dest = dir.path().join("rewritten");
        let report = Store::rewrite_directory(&data, &dest).unwrap();
        assert_eq!(snapshot_tree(&data), before);
        assert_eq!(
            report.source.stored_data_bytes,
            directory_stored_bytes(&data).unwrap().stored_data_bytes
        );
        assert_eq!(
            report.source.stored_index_bytes,
            directory_stored_bytes(&data).unwrap().stored_index_bytes
        );
        assert_eq!(report.destination, directory_stored_bytes(&dest).unwrap());
        assert_change(&report);
        let got = query_bytes(&dest, &wide_path);
        assert_eq!(got, expected);
        assert!(got[0]
            .windows(br#"{"a":1,"a":2,"b":{"z":1,"z":3}}"#.len())
            .any(|window| window == br#"{"a":1,"a":2,"b":{"z":1,"z":3}}"#));
        let reopened = Store::open(&dest, &wide_path).unwrap();
        let rows = reopened.query(i64::MIN, i64::MAX).unwrap();
        assert_eq!(rows.len(), 2);
        match &rows[0].values[1] {
            Scalar::Json(text) => assert_eq!(text, r#"{"a":1,"a":2,"b":{"z":1,"z":3}}"#),
            other => panic!("expected json text, got {other:?}"),
        }
        assert!(row_value(&reopened, &rows[0])["region"].is_null());
        assert_eq!(row_value(&reopened, &rows[1])["region"], "us");
        assert_eq!(
            fs::read_to_string(dest.join("schema.lock")).unwrap(),
            reopened.schema().canonical()
        );
        reopened.close().unwrap();
        assert_eq!(query_bytes(&dest, &wide_path), expected);
    }

    #[test]
    fn rewrite_directory_of_a_current_store_reports_directory_totals() {
        let dir = TempDir::new();
        let schema = write_schema(dir.path());
        let data = dir.path().join("data");
        let store = Store::open_with(&data, &schema, StoreOptions::default()).unwrap();
        for ts in 0..8 {
            store
                .append_json(&event(ts, Some(ts), "click", Some("hello"), "19.99"))
                .unwrap();
        }
        store.flush().unwrap();
        let stats = store.stats();
        store.close().unwrap();
        let expected = query_bytes(&data, &schema);
        let before = snapshot_tree(&data);

        let dest = dir.path().join("rewritten");
        let report = Store::rewrite_directory(&data, &dest).unwrap();
        assert_eq!(snapshot_tree(&data), before);
        assert_eq!(report.source.stored_data_bytes, stats.data_bytes);
        assert_eq!(report.source.stored_index_bytes, stats.index_bytes);
        assert_eq!(report.source, directory_stored_bytes(&data).unwrap());
        let opened = Store::open(&dest, &schema).unwrap();
        assert_eq!(
            opened.stats().data_bytes,
            report.destination.stored_data_bytes
        );
        assert_eq!(
            opened.stats().index_bytes,
            report.destination.stored_index_bytes
        );
        assert_eq!(opened.stats().rows, 8);
        opened.close().unwrap();
        assert_eq!(report.destination, directory_stored_bytes(&dest).unwrap());
        assert_change(&report);
        assert_eq!(query_bytes(&dest, &schema), expected);
        assert_eq!(query_bytes(&dest, &schema), query_bytes(&data, &schema));
    }

    #[test]
    fn rewrite_directory_failure_leaves_the_source_segment_in_place() {
        let dir = TempDir::new();
        let schema = write_schema(dir.path());
        let data = dir.path().join("data");
        let store = Store::open_with(&data, &schema, test_options(2)).unwrap();
        store
            .append_json(&event(1, Some(1), "click", None, "1.00"))
            .unwrap();
        store
            .append_json(&event(2, Some(2), "view", Some("a"), "2.00"))
            .unwrap();
        store.flush().unwrap();
        store.close().unwrap();
        let before = snapshot_tree(&data);

        let blocked = dir.path().join("not-a-directory");
        fs::write(&blocked, b"file").unwrap();
        let err = Store::rewrite_directory(&data, &blocked).unwrap_err();
        assert!(err.to_string().contains("not a directory"), "{err}");
        assert_eq!(snapshot_tree(&data), before);
        assert_eq!(fs::read(&blocked).unwrap(), b"file");

        let existing = dir.path().join("existing");
        let prior = Store::open_with(&existing, &schema, test_options(2)).unwrap();
        prior
            .append_json(&event(9, Some(9), "buy", None, "3.00"))
            .unwrap();
        prior.close().unwrap();
        let existing_before = snapshot_tree(&existing);
        let err = Store::rewrite_directory(&data, &existing).unwrap_err();
        assert!(err.to_string().contains("already holds a store"), "{err}");
        assert_eq!(snapshot_tree(&data), before);
        assert_eq!(snapshot_tree(&existing), existing_before);

        let dest = dir.path().join("unpublished");
        REWRITE_FAIL_BEFORE_PUBLISH.with(|flag| flag.set(true));
        let err = Store::rewrite_directory(&data, &dest).unwrap_err();
        assert!(
            err.to_string()
                .contains("before the destination was published"),
            "{err}"
        );
        assert_eq!(snapshot_tree(&data), before);
        assert!(
            !dest.exists() || !directory_holds_store(&dest).unwrap(),
            "a failed copy published a store"
        );
        let segment = fs::read(segment::data_path(&data, 1)).unwrap();
        assert_eq!(
            snapshot_tree(&data).get("seg-000001.dat").unwrap(),
            &segment
        );
    }

    #[test]
    fn rewrite_directory_keeps_a_row_that_reserializes_past_the_ingest_cap() {
        let dir = TempDir::new();
        let schema = write_schema(dir.path());
        let data = dir.path().join("data");
        let store = Store::open_with(&data, &schema, test_options(1)).unwrap();
        let empty = event(1, None, "click", Some(""), "1");
        let note = "n".repeat(1024 * 1024 - empty.len());
        let raw = event(1, None, "click", Some(&note), "1");
        assert!(raw.len() <= 1024 * 1024, "fixture is {}", raw.len());
        store.append_json(&raw).unwrap();
        store.flush().unwrap();
        store.close().unwrap();
        let before = snapshot_tree(&data);

        let dest = dir.path().join("rewritten");
        let report = Store::rewrite_directory(&data, &dest).unwrap();
        assert_eq!(snapshot_tree(&data), before);
        let opened = Store::open(&dest, &schema).unwrap();
        assert_eq!(opened.stats().rows, 1);
        let rows = opened.query(i64::MIN, i64::MAX).unwrap();
        let amount = &rows[0].values[6];
        assert!(matches!(amount, Scalar::Decimal(_)), "{amount:?}");
        opened.close().unwrap();
        assert_eq!(report.destination, directory_stored_bytes(&dest).unwrap());
    }

    #[test]
    fn rewrite_directory_moves_files_into_an_existing_empty_directory() {
        let dir = TempDir::new();
        let schema = write_schema(dir.path());
        let data = dir.path().join("data");
        let store = Store::open_with(&data, &schema, test_options(2)).unwrap();
        store
            .append_json(&event(1, Some(1), "click", None, "1.00"))
            .unwrap();
        store
            .append_json(&event(2, Some(2), "view", Some("a"), "2.00"))
            .unwrap();
        store.flush().unwrap();
        store.close().unwrap();
        let before = snapshot_tree(&data);
        let expected = query_bytes(&data, &schema);

        let dest = dir.path().join("mounted");
        fs::create_dir(&dest).unwrap();
        let report = Store::rewrite_directory(&data, &dest).unwrap();
        assert_eq!(snapshot_tree(&data), before);
        assert!(dest.join("schema.lock").is_file());
        assert!(
            fs::read_dir(&dest)
                .unwrap()
                .all(|entry| entry.unwrap().file_type().unwrap().is_file()),
            "the private directory was left inside the destination"
        );
        let opened = Store::open(&dest, &schema).unwrap();
        assert_eq!(opened.stats().rows, 2);
        opened.close().unwrap();
        assert_eq!(query_bytes(&dest, &schema), expected);
        assert_eq!(report.destination, directory_stored_bytes(&dest).unwrap());

        let failed = dir.path().join("mounted-failed");
        fs::create_dir(&failed).unwrap();
        REWRITE_FAIL_BEFORE_PUBLISH.with(|flag| flag.set(true));
        let err = Store::rewrite_directory(&data, &failed).unwrap_err();
        assert!(
            err.to_string()
                .contains("before the destination was published"),
            "{err}"
        );
        assert_eq!(snapshot_tree(&data), before);
        assert!(
            !directory_has_entries(&failed).unwrap(),
            "a failed rewrite left files in the existing destination"
        );
    }

    #[test]
    fn open_refuses_an_unfinished_rewrite_and_still_opens_segment_files() {
        let dir = TempDir::new();
        let schema = write_schema(dir.path());
        let data = dir.path().join("data");
        fs::create_dir(&data).unwrap();
        fs::write(data.join(REWRITE_INCOMPLETE_MARKER), b"incomplete\n").unwrap();
        let staging = data.join("eventer-rewrite-1-1").join("staging");
        fs::create_dir_all(&staging).unwrap();
        fs::write(staging.join("seg-000001.dat"), b"partial").unwrap();
        let err = match Store::open(&data, &schema) {
            Ok(_) => panic!("opened a directory that still has the rewrite marker"),
            Err(err) => err,
        };
        assert!(err.to_string().contains("unfinished rewrite"), "{err}");
        assert!(!data.join("schema.lock").exists());
        assert_eq!(
            fs::read(staging.join("seg-000001.dat")).unwrap(),
            b"partial"
        );

        let legacy = dir.path().join("legacy");
        fs::create_dir(&legacy).unwrap();
        fs::write(legacy.join("seg-000001.dat"), b"partial").unwrap();
        let parsed = parse_schema(SCHEMA_JSON).unwrap();
        schema_lock_action(&legacy, &parsed).unwrap();
        assert!(legacy.join("schema.lock").is_file());
        assert_eq!(fs::read(legacy.join("seg-000001.dat")).unwrap(), b"partial");

        let fresh = dir.path().join("fresh");
        let store = Store::open(&fresh, &schema).unwrap();
        store.close().unwrap();
        assert!(fresh.join("schema.lock").is_file());
    }

    #[test]
    fn open_refuses_a_rewrite_marker_when_schema_lock_is_already_present() {
        let dir = TempDir::new();
        let schema = write_schema(dir.path());
        let data = dir.path().join("data");
        let mut options = test_options(1);
        options.segment_bytes = 1;
        let store = Store::open_with(&data, &schema, options).unwrap();
        for ts in 1..=6 {
            store
                .append_json(&event(ts, Some(ts), "click", None, "1.00"))
                .unwrap();
            store.flush().unwrap();
        }
        store.close().unwrap();
        let dest = dir.path().join("partial");
        fs::create_dir(&dest).unwrap();
        fs::copy(data.join("schema.lock"), dest.join("schema.lock")).unwrap();
        fs::copy(data.join("seg-000001.dat"), dest.join("seg-000001.dat")).unwrap();
        fs::write(dest.join(REWRITE_INCOMPLETE_MARKER), b"incomplete\n").unwrap();
        let before = snapshot_tree(&dest);
        let err = match Store::open(&dest, &schema) {
            Ok(_) => panic!("opened a directory that still has the rewrite marker"),
            Err(err) => err,
        };
        assert!(err.to_string().contains("unfinished rewrite"), "{err}");
        assert_eq!(snapshot_tree(&dest), before);
    }

    #[test]
    fn rewrite_directory_replaces_an_unfinished_attempt() {
        let dir = TempDir::new();
        let schema = write_schema(dir.path());
        let data = dir.path().join("data");
        let store = Store::open_with(&data, &schema, test_options(2)).unwrap();
        store
            .append_json(&event(1, Some(1), "click", None, "1.00"))
            .unwrap();
        store
            .append_json(&event(2, Some(2), "view", Some("a"), "2.00"))
            .unwrap();
        store.flush().unwrap();
        store.close().unwrap();
        let expected = query_bytes(&data, &schema);

        let marker_only = dir.path().join("marker-only");
        fs::create_dir(&marker_only).unwrap();
        fs::write(marker_only.join(REWRITE_INCOMPLETE_MARKER), b"incomplete\n").unwrap();
        Store::rewrite_directory(&data, &marker_only).unwrap();
        let opened = Store::open(&marker_only, &schema).unwrap();
        assert_eq!(opened.stats().rows, 2);
        opened.close().unwrap();
        assert_eq!(query_bytes(&marker_only, &schema), expected);
        assert!(!marker_only.join(REWRITE_INCOMPLETE_MARKER).exists());

        let partial = dir.path().join("partial");
        fs::create_dir(&partial).unwrap();
        fs::write(partial.join(REWRITE_INCOMPLETE_MARKER), b"incomplete\n").unwrap();
        fs::write(partial.join("seg-000001.dat"), b"leftover").unwrap();
        let scratch = partial.join("eventer-rewrite-1-1").join("staging");
        fs::create_dir_all(&scratch).unwrap();
        fs::write(scratch.join("seg-000001.dat"), b"scratch").unwrap();
        Store::rewrite_directory(&data, &partial).unwrap();
        let opened = Store::open(&partial, &schema).unwrap();
        assert_eq!(opened.stats().rows, 2);
        opened.close().unwrap();
        assert!(!partial.join("eventer-rewrite-1-1").exists());
        assert!(!partial.join(REWRITE_INCOMPLETE_MARKER).exists());
        assert_eq!(query_bytes(&partial, &schema), expected);

        let finished = dir.path().join("finished");
        Store::rewrite_directory(&data, &finished).unwrap();
        let err = Store::rewrite_directory(&data, &finished).unwrap_err();
        assert!(err.to_string().contains("already holds a store"), "{err}");

        fs::write(finished.join(REWRITE_INCOMPLETE_MARKER), b"incomplete\n").unwrap();
        Store::rewrite_directory(&data, &finished).unwrap();
        let opened = Store::open(&finished, &schema).unwrap();
        assert_eq!(opened.stats().rows, 2);
        opened.close().unwrap();
        assert!(!finished.join(REWRITE_INCOMPLETE_MARKER).exists());
        assert_eq!(query_bytes(&finished, &schema), expected);
    }

    #[test]
    fn rewrite_directory_puts_back_files_when_a_later_publish_rename_fails() {
        let dir = TempDir::new();
        let schema = write_schema(dir.path());
        let data = dir.path().join("data");
        let store = Store::open_with(&data, &schema, test_options(2)).unwrap();
        store
            .append_json(&event(1, Some(1), "click", None, "1.00"))
            .unwrap();
        store
            .append_json(&event(2, Some(2), "view", Some("a"), "2.00"))
            .unwrap();
        store.flush().unwrap();
        store.close().unwrap();
        let before = snapshot_tree(&data);
        let expected = query_bytes(&data, &schema);

        let dest = dir.path().join("mounted");
        fs::create_dir(&dest).unwrap();
        // Allow the first file through, then fail before schema.lock moves.
        REWRITE_FAIL_PUBLISH_RENAME_AFTER.with(|flag| flag.set(2));
        let err = Store::rewrite_directory(&data, &dest).unwrap_err();
        assert!(
            err.to_string()
                .contains("publish rename failed before the rewritten store was complete"),
            "{err}"
        );
        assert_eq!(snapshot_tree(&data), before);
        assert!(
            !directory_has_entries(&dest).unwrap(),
            "a failed publish left files in the destination"
        );
        let report = Store::rewrite_directory(&data, &dest).unwrap();
        assert_eq!(snapshot_tree(&data), before);
        let opened = Store::open(&dest, &schema).unwrap();
        assert_eq!(opened.stats().rows, 2);
        opened.close().unwrap();
        assert_eq!(query_bytes(&dest, &schema), expected);
        assert_eq!(report.destination, directory_stored_bytes(&dest).unwrap());
    }

    #[test]
    fn scratch_dir_is_created_outside_the_source_when_temp_is_the_source() {
        let dir = TempDir::new();
        let source = dir.path().join("source");
        fs::create_dir(&source).unwrap();
        let destination = dir.path().join("destination");
        let bases = vec![source.clone(), dir.path().to_path_buf()];
        let created =
            scratch_dir_in(&bases, "eventer-rewrite-test", &source, &destination).unwrap();
        assert_eq!(created.parent(), Some(dir.path()));
        assert!(!created.starts_with(&source));
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = fs::metadata(&created).unwrap().permissions().mode() & 0o777;
            assert_eq!(
                mode, 0o700,
                "scratch directory is traversable by other users"
            );
        }
        let _ = fs::remove_dir_all(&created);
        assert!(!source.join("eventer-rewrite-test").exists());
    }

    #[test]
    fn publish_across_devices_does_not_keep_a_precreated_sibling() {
        let dir = TempDir::new();
        let schema = write_schema(dir.path());
        let data = dir.path().join("data");
        let store = Store::open_with(&data, &schema, test_options(2)).unwrap();
        store
            .append_json(&event(1, Some(1), "click", None, "1.00"))
            .unwrap();
        store.flush().unwrap();
        store.close().unwrap();
        let before = snapshot_tree(&data);
        let dest = dir.path().join("rewritten");
        let planted = dir
            .path()
            .join(format!(".rewritten-publish-{}", std::process::id()));
        fs::create_dir(&planted).unwrap();
        fs::write(planted.join("seg-000099.dat"), b"leftover").unwrap();

        REWRITE_COPY_ACROSS_DEVICES.with(|flag| flag.set(true));
        let report = Store::rewrite_directory(&data, &dest).unwrap();
        assert_eq!(snapshot_tree(&data), before);
        assert!(planted.join("seg-000099.dat").is_file());
        assert!(!dest.join("seg-000099.dat").exists());
        let opened = Store::open(&dest, &schema).unwrap();
        assert_eq!(opened.stats().rows, 1);
        opened.close().unwrap();
        assert_eq!(report.destination, directory_stored_bytes(&dest).unwrap());
        let _ = fs::remove_dir_all(&planted);
    }

    fn stat_req(field: &str, sum: bool, min: bool, max: bool) -> StatRequest {
        StatRequest {
            field: field.to_string(),
            sum,
            min,
            max,
            var: false,
            stddev: false,
        }
    }

    fn moment_req(field: &str, var: bool, stddev: bool) -> StatRequest {
        StatRequest {
            field: field.to_string(),
            sum: false,
            min: false,
            max: false,
            var,
            stddev,
        }
    }

    fn exact_var(columns: &[ColumnStats], field: &str) -> (String, Option<f64>) {
        let column = columns.iter().find(|column| column.field == field).unwrap();
        let text = match column.var.as_ref().unwrap() {
            Some(StatVar::Exact(text)) => text.clone(),
            other => panic!("expected an exact variance, got {other:?}"),
        };
        let dev = column.stddev.as_ref().map(|value| value.unwrap());
        (text, dev)
    }

    #[test]
    fn population_variance_uses_the_population_moment() {
        let dir = TempDir::new();
        let schema = write_schema(dir.path());
        let data = dir.path().join("data");
        let store = Store::open_with(&data, &schema, test_options(2)).unwrap();
        for (ts, user, score, amount) in [
            (1000i64, Some(1i64), 1.0f64, "1.00"),
            (2000, Some(2), 3.0, "2.00"),
            (3000, Some(3), 1.0, "3.00"),
        ] {
            let obj = serde_json::json!({
                "ts": ts,
                "user_id": user,
                "score": score,
                "ok": true,
                "action": if ts == 3000 { "view" } else { "buy" },
                "amount": amount,
            });
            store
                .append_json(&serde_json::to_vec(&obj).unwrap())
                .unwrap();
        }
        let null_user = serde_json::json!({
            "ts": 4000,
            "score": 1.0,
            "ok": false,
            "action": "other",
            "amount": "1.00",
        });
        store
            .append_json(&serde_json::to_vec(&null_user).unwrap())
            .unwrap();
        store.flush().unwrap();

        let users = store
            .aggregate_with_filter(1000, 3000, &[], &[moment_req("user_id", true, true)])
            .unwrap();
        let (var, dev) = exact_var(&users, "user_id");
        assert_eq!(var, "2/3");
        assert_eq!(dev.unwrap().to_bits(), (2.0f64 / 3.0).sqrt().to_bits());
        assert!(users[0].sum.is_none());
        assert!(users[0].min.is_none());

        let one = store
            .aggregate_with_filter(1000, 1000, &[], &[moment_req("user_id", true, true)])
            .unwrap();
        let (var, dev) = exact_var(&one, "user_id");
        assert_eq!(var, "0");
        assert_eq!(dev.unwrap().to_bits(), 0.0f64.to_bits());

        let pair = store
            .aggregate_with_filter(1000, 2000, &[], &[moment_req("user_id", true, false)])
            .unwrap();
        assert_eq!(exact_var(&pair, "user_id").0, "0.25");
        assert!(pair[0].stddev.is_none());

        let timestamps = store
            .aggregate_with_filter(1000, 3000, &[], &[moment_req("ts", true, true)])
            .unwrap();
        let (var, dev) = exact_var(&timestamps, "ts");
        assert_eq!(var, "2000000/3");
        assert_eq!(
            dev.unwrap().to_bits(),
            (2_000_000.0f64 / 3.0).sqrt().to_bits()
        );

        let amounts = store
            .aggregate_with_filter(1000, 3000, &[], &[moment_req("amount", true, true)])
            .unwrap();
        let (var, dev) = exact_var(&amounts, "amount");
        assert_eq!(var, "2/3");
        assert_eq!(dev.unwrap().to_bits(), (2.0f64 / 3.0).sqrt().to_bits());

        let cents = store
            .aggregate_with_filter(1000, 2000, &[], &[moment_req("amount", true, true)])
            .unwrap();
        let (var, dev) = exact_var(&cents, "amount");
        assert_eq!(var, "0.25");
        assert_eq!(dev.unwrap().to_bits(), 0.5f64.to_bits());

        let scores = store
            .aggregate_with_filter(1000, 2000, &[], &[moment_req("score", true, true)])
            .unwrap();
        match scores[0].var.as_ref().unwrap() {
            Some(StatVar::Float(value)) => assert_eq!(value.to_bits(), 1.0f64.to_bits()),
            other => panic!("float variance {other:?}"),
        }
        assert_eq!(
            scores[0].stddev.unwrap().unwrap().to_bits(),
            1.0f64.to_bits()
        );

        let single_score = store
            .aggregate_with_filter(1000, 1000, &[], &[moment_req("score", true, true)])
            .unwrap();
        match single_score[0].var.as_ref().unwrap() {
            Some(StatVar::Float(value)) => assert_eq!(*value, 0.0),
            other => panic!("single float variance {other:?}"),
        }
        assert_eq!(single_score[0].stddev.unwrap().unwrap(), 0.0);

        let empty = store
            .aggregate_with_filter(9000, 9000, &[], &[moment_req("user_id", true, true)])
            .unwrap();
        assert_eq!(empty[0].var, Some(None));
        assert_eq!(empty[0].stddev, Some(None));
        let only_null = store
            .aggregate_with_filter(4000, 4000, &[], &[moment_req("user_id", true, true)])
            .unwrap();
        assert_eq!(only_null[0].var, Some(None));
        assert_eq!(only_null[0].stddev, Some(None));

        let text = store.aggregate_with_filter(0, 10, &[], &[moment_req("action", true, false)]);
        assert!(matches!(text, Err(Error::Schema(_))), "{text:?}");

        let grouped = store
            .aggregate_grouped(
                1000,
                4000,
                &[],
                &[moment_req("user_id", true, false)],
                "action",
            )
            .unwrap();
        assert_eq!(grouped.len(), 3);
        assert_eq!(grouped[0].key, Scalar::Str("buy".into()));
        assert_eq!(exact_var(&grouped[0].columns, "user_id").0, "0.25");
        assert_eq!(grouped[1].key, Scalar::Str("other".into()));
        assert_eq!(grouped[1].columns[0].var, Some(None));
        assert_eq!(grouped[2].key, Scalar::Str("view".into()));
        assert_eq!(exact_var(&grouped[2].columns, "user_id").0, "0");

        let wide = TempDir::new();
        let schema_path = wide.path().join("schema.json");
        fs::write(
            &schema_path,
            r#"{
                "timestamp_field": "ts",
                "fields": [
                    {"name": "ts", "type": "timestamp"},
                    {"name": "user_id", "type": "int"}
                ]
            }"#,
        )
        .unwrap();
        let store =
            Store::open_with(wide.path().join("data"), &schema_path, test_options(8)).unwrap();
        let huge = i64::MIN;
        for ts in [1i64, 2] {
            store
                .append_json(format!(r#"{{"ts":{ts},"user_id":{huge}}}"#).as_bytes())
                .unwrap();
        }
        store.flush().unwrap();
        let overflow =
            store.aggregate_with_filter(0, 10, &[], &[moment_req("user_id", true, true)]);
        assert!(
            matches!(overflow, Err(Error::Event(ref message)) if message.contains("overflow")),
            "{overflow:?}"
        );
        store.close().unwrap();
    }

    #[test]
    fn negative_float_moment_of_three_tenths_stays_finite() {
        let dir = TempDir::new();
        let schema = write_schema(dir.path());
        let store = Store::open_with(dir.path().join("data"), &schema, test_options(8)).unwrap();
        for ts in [1000i64, 2000, 3000] {
            let obj = serde_json::json!({
                "ts": ts,
                "user_id": 1,
                "score": 0.1,
                "ok": true,
                "action": "buy",
                "amount": "1.00",
            });
            store
                .append_json(&serde_json::to_vec(&obj).unwrap())
                .unwrap();
        }
        store.flush().unwrap();

        let value = 0.1f64;
        let n = 3.0f64;
        let sum = value + value + value;
        let sum_sq = (value * value) + (value * value) + (value * value);
        let expected = (sum_sq / n) - ((sum / n) * (sum / n));
        assert!(expected.is_finite() && expected < 0.0, "{expected}");

        let alone = store
            .aggregate_with_filter(1000, 3000, &[], &[moment_req("score", true, false)])
            .unwrap();
        match alone[0].var.as_ref().unwrap() {
            Some(StatVar::Float(got)) => assert_eq!(got.to_bits(), expected.to_bits()),
            other => panic!("float variance {other:?}"),
        }
        assert!(alone[0].sum.is_none());
        assert!(alone[0].min.is_none());
        assert!(alone[0].stddev.is_none());

        let with_sum = store
            .aggregate_with_filter(
                1000,
                3000,
                &[],
                &[StatRequest {
                    field: "score".to_string(),
                    sum: true,
                    min: true,
                    max: false,
                    var: true,
                    stddev: false,
                }],
            )
            .unwrap();
        match with_sum[0].var.as_ref().unwrap() {
            Some(StatVar::Float(got)) => assert_eq!(got.to_bits(), expected.to_bits()),
            other => panic!("float variance with sum {other:?}"),
        }
        assert_eq!(
            with_sum[0].sum,
            Some(StatSum::Float {
                total: sum,
                contributed: true,
            })
        );
        assert_eq!(with_sum[0].min, Some(Some(Scalar::Float(value))));
        assert!(with_sum[0].stddev.is_none());

        let with_dev = store.aggregate_with_filter(
            1000,
            3000,
            &[],
            &[StatRequest {
                field: "score".to_string(),
                sum: true,
                min: true,
                max: false,
                var: true,
                stddev: true,
            }],
        );
        assert!(
            matches!(with_dev, Err(Error::Event(ref message)) if message.contains("negative")),
            "{with_dev:?}"
        );
        store.close().unwrap();
    }

    #[test]
    fn float_variance_keeps_a_finite_negative_moment() {
        let dir = TempDir::new();
        let schema = write_schema(dir.path());
        let store = Store::open_with(dir.path().join("data"), &schema, test_options(8)).unwrap();
        for (index, offset) in [0.0, 1.0, 2.0, 3.0, 4.0, 5.0].into_iter().enumerate() {
            let score = 1e10 + offset;
            let obj = serde_json::json!({
                "ts": 1000 + index as i64,
                "user_id": 1,
                "score": score,
                "ok": true,
                "action": "buy",
                "amount": "1.00",
            });
            store
                .append_json(&serde_json::to_vec(&obj).unwrap())
                .unwrap();
        }
        store.flush().unwrap();
        let scores = store
            .aggregate_with_filter(1000, 2000, &[], &[moment_req("score", true, false)])
            .unwrap();
        match scores[0].var.as_ref().unwrap() {
            Some(StatVar::Float(value)) => assert_eq!(value.to_bits(), (-16384.0f64).to_bits()),
            other => panic!("negative float variance {other:?}"),
        }
        assert!(scores[0].stddev.is_none());

        let with_dev =
            store.aggregate_with_filter(1000, 2000, &[], &[moment_req("score", true, true)]);
        assert!(
            matches!(with_dev, Err(Error::Event(ref message)) if message.contains("negative")),
            "{with_dev:?}"
        );
        store.close().unwrap();
    }

    #[test]
    fn population_variance_matches_the_stock_distribution() {
        let dir = TempDir::new();
        let schema = write_schema(dir.path());
        let data = dir.path().join("data");
        let store = Store::open_with(&data, &schema, test_options(2048)).unwrap();
        let count = 100_000usize;
        for i in 0..count {
            let ts = 1_700_000_000_000i64 + i as i64;
            let amount_cents = (i % 5_000) as i64;
            let amount = format!("{}.{:02}", amount_cents / 100, amount_cents % 100);
            let obj = serde_json::json!({
                "ts": ts,
                "user_id": (i % 1_000) as i64,
                "score": (i % 100) as f64 / 10.0,
                "ok": i % 2 == 0,
                "action": "click",
                "amount": amount,
            });
            store
                .append_json(&serde_json::to_vec(&obj).unwrap())
                .unwrap();
        }
        store.flush().unwrap();
        let from = 1_700_000_000_000i64;
        let to = from + count as i64;
        let scores = store
            .aggregate_with_filter(from, to, &[], &[moment_req("score", true, true)])
            .unwrap();
        let expected = 8_332_500.0f64 / 1_000_000.0;
        match scores[0].var.as_ref().unwrap() {
            Some(StatVar::Float(value)) => assert_eq!(value.to_bits(), expected.to_bits()),
            other => panic!("stock score variance {other:?}"),
        }
        assert_eq!(
            scores[0].stddev.unwrap().unwrap().to_bits(),
            expected.sqrt().to_bits()
        );
        let sample = {
            let n = count as f64;
            let mut sum = 0.0f64;
            let mut sum_sq = 0.0f64;
            for i in 0..count {
                let value = (i % 100) as f64 / 10.0;
                sum += value;
                sum_sq += value * value;
            }
            (sum_sq - (sum * sum) / n) / (n - 1.0)
        };
        assert_ne!(sample.to_bits(), expected.to_bits());

        let users = store
            .aggregate_with_filter(from, to, &[], &[moment_req("user_id", true, true)])
            .unwrap();
        let (var, dev) = exact_var(&users, "user_id");
        assert_eq!(var, "83333.25");
        assert_eq!(
            dev.unwrap().to_bits(),
            (333_333.0f64 / 4.0).sqrt().to_bits()
        );
        let stored = store.stats();
        assert!(stored.data_bytes > 0);
        store.close().unwrap();
    }

    fn exact_sum(columns: &[ColumnStats], field: &str) -> (i128, bool) {
        let column = columns.iter().find(|column| column.field == field).unwrap();
        match column.sum.unwrap() {
            StatSum::Exact { total, contributed } => (total, contributed),
            StatSum::Float { total, .. } => panic!("float sum {total} for {field}"),
        }
    }

    #[test]
    fn aggregate_sums_filtered_rows_and_skips_a_rejected_zone() {
        let dir = TempDir::new();
        let schema = write_schema(dir.path());
        let data = dir.path().join("data");
        let store = Store::open_with(&data, &schema, test_options(8)).unwrap();
        for (ts, user, score, amount) in [
            (1000i64, Some(1i64), 0.5f64, "1.50"),
            (2000, Some(5), 0.25, "2.25"),
            (5000, Some(9), 1.0, "4.00"),
        ] {
            let obj = serde_json::json!({
                "ts": ts,
                "user_id": user,
                "score": score,
                "ok": true,
                "action": "buy",
                "amount": amount,
            });
            store
                .append_json(&serde_json::to_vec(&obj).unwrap())
                .unwrap();
        }
        let null_user = serde_json::json!({
            "ts": 3000,
            "score": 1.0,
            "ok": false,
            "action": "other",
            "amount": "9.00",
        });
        store
            .append_json(&serde_json::to_vec(&null_user).unwrap())
            .unwrap();
        store.flush().unwrap();

        let columns = store
            .aggregate_with_filter(1000, 3000, &[], &[stat_req("user_id", true, true, true)])
            .unwrap();
        assert_eq!(exact_sum(&columns, "user_id"), (6, true));
        assert_eq!(columns[0].min, Some(Some(Scalar::Int(1))));
        assert_eq!(columns[0].max, Some(Some(Scalar::Int(5))));
        let amount = store
            .aggregate_with_filter(1000, 3000, &[], &[stat_req("amount", true, false, false)])
            .unwrap();
        assert_eq!(exact_sum(&amount, "amount"), (150 + 225 + 900, true));
        let cut = store
            .aggregate_with_filter(4000, 6000, &[], &[stat_req("user_id", false, true, true)])
            .unwrap();
        assert_eq!(cut[0].min, Some(Some(Scalar::Int(9))));
        assert_eq!(cut[0].max, Some(Some(Scalar::Int(9))));
        assert!(cut[0].sum.is_none());

        let below =
            crate::value::scalar_from_literal(FieldType::Decimal { scale: 2 }, "2.00").unwrap();
        let filtered = store
            .aggregate_with_filter(
                1000,
                3000,
                &[Predicate::Cmp("amount".into(), CmpOp::Lt, below)],
                &[stat_req("amount", true, true, true)],
            )
            .unwrap();
        assert_eq!(exact_sum(&filtered, "amount"), (150, true));
        assert_eq!(filtered[0].min, Some(Some(Scalar::Decimal(150))));
        let buy = [Predicate::Eq("action".into(), "buy".into())];
        assert_eq!(store.count_with_filter(1000, 3000, &buy).unwrap(), 2);
        let same = store
            .aggregate_with_filter(1000, 3000, &buy, &[stat_req("user_id", true, false, false)])
            .unwrap();
        assert_eq!(exact_sum(&same, "user_id"), (6, true));

        let empty = store
            .aggregate_with_filter(
                9000,
                9000,
                &[],
                &[
                    stat_req("user_id", true, true, true),
                    stat_req("score", true, true, false),
                    stat_req("amount", true, false, false),
                    stat_req("ts", true, true, true),
                ],
            )
            .unwrap();
        assert_eq!(exact_sum(&empty, "user_id"), (0, false));
        assert_eq!(empty[0].min, Some(None));
        assert_eq!(empty[0].max, Some(None));
        assert_eq!(
            empty[1].sum,
            Some(StatSum::Float {
                total: 0.0,
                contributed: false,
            })
        );
        assert_eq!(empty[1].min, Some(None));
        assert!(empty[1].max.is_none());
        assert_eq!(exact_sum(&empty, "amount"), (0, false));
        assert_eq!(exact_sum(&empty, "ts"), (0, false));

        let scores = store
            .aggregate_with_filter(1000, 2000, &[], &[stat_req("score", true, true, true)])
            .unwrap();
        assert_eq!(
            scores[0].sum,
            Some(StatSum::Float {
                total: 0.75,
                contributed: true,
            })
        );
        let timestamps = store
            .aggregate_with_filter(1000, 2000, &[], &[stat_req("ts", true, true, true)])
            .unwrap();
        assert_eq!(exact_sum(&timestamps, "ts"), (3000, true));
        assert_eq!(timestamps[0].min, Some(Some(Scalar::Timestamp(1000))));
        assert_eq!(timestamps[0].max, Some(Some(Scalar::Timestamp(2000))));

        let missing =
            store.aggregate_with_filter(0, 10, &[], &[stat_req("nope", true, false, false)]);
        assert!(matches!(missing, Err(Error::Schema(_))), "{missing:?}");
        let text =
            store.aggregate_with_filter(0, 10, &[], &[stat_req("action", true, false, false)]);
        assert!(matches!(text, Err(Error::Schema(_))), "{text:?}");
        assert_eq!(store.count(1000, 5000).unwrap(), 4);
        store.close().unwrap();

        let low_dir = TempDir::new();
        let schema = write_schema(low_dir.path());
        let data = low_dir.path().join("data");
        let store = Store::open_with(&data, &schema, test_options(2)).unwrap();
        for (ts, user) in [(10i64, 1i64), (11, 2), (20, 10), (21, 11)] {
            store
                .append_json(&event(ts, Some(user), "click", None, "1.00"))
                .unwrap();
        }
        store.flush().unwrap();
        let gt = [Predicate::Cmp("user_id".into(), CmpOp::Gt, Scalar::Int(5))];
        let resolved = resolve_predicates(store.schema(), &gt).unwrap();
        assert_eq!(store.blocks_in_range(0, 100, &resolved).len(), 1);
        let victim = store.catalog().segments[0]
            .blocks
            .iter()
            .find(|block| block.max_ts <= 11)
            .cloned()
            .unwrap();
        let outside = store.catalog().segments[0]
            .blocks
            .iter()
            .find(|block| block.min_ts >= 20)
            .cloned()
            .unwrap();
        let path = segment::data_path(&data, victim.segment_id);
        let mut bytes = fs::read(&path).unwrap();
        bytes[victim.offset as usize + segment::BLOCK_HEADER_LEN + 4] ^= 0xff;
        fs::write(&path, &bytes).unwrap();
        let kept = store
            .aggregate_with_filter(0, 100, &gt, &[stat_req("user_id", true, true, true)])
            .unwrap();
        assert_eq!(exact_sum(&kept, "user_id"), (21, true));
        assert_eq!(kept[0].min, Some(Some(Scalar::Int(10))));
        assert_eq!(kept[0].max, Some(Some(Scalar::Int(11))));
        let skipped_time = store
            .aggregate_with_filter(
                outside.min_ts,
                outside.max_ts,
                &[],
                &[stat_req("user_id", true, false, false)],
            )
            .unwrap();
        assert_eq!(exact_sum(&skipped_time, "user_id"), (21, true));
        assert!(store
            .aggregate_with_filter(
                victim.min_ts,
                victim.max_ts,
                &[],
                &[stat_req("user_id", true, false, false)]
            )
            .is_err());
        store.close().unwrap();

        let wide = TempDir::new();
        let schema_path = wide.path().join("schema.json");
        fs::write(
            &schema_path,
            r#"{
                "timestamp_field": "ts",
                "fields": [
                    {"name": "ts", "type": "timestamp"},
                    {"name": "amount", "type": "decimal", "scale": 0}
                ]
            }"#,
        )
        .unwrap();
        let store =
            Store::open_with(wide.path().join("data"), &schema_path, test_options(8)).unwrap();
        let huge = i128::MAX.to_string();
        for (ts, amount) in [(1i64, huge.as_str()), (2, "1")] {
            store
                .append_json(format!(r#"{{"ts":{ts},"amount":"{amount}"}}"#).as_bytes())
                .unwrap();
        }
        store.flush().unwrap();
        let overflow =
            store.aggregate_with_filter(0, 10, &[], &[stat_req("amount", true, true, false)]);
        assert!(
            matches!(overflow, Err(Error::Event(ref message)) if message.contains("overflow")),
            "{overflow:?}"
        );
        store.close().unwrap();

        let floats = TempDir::new();
        let schema = write_schema(floats.path());
        let store = Store::open_with(floats.path().join("data"), &schema, test_options(2)).unwrap();
        for (ts, score) in [(1i64, 1e16f64), (2, 1.0), (3, -1e16)] {
            let obj = serde_json::json!({
                "ts": ts,
                "user_id": 1,
                "score": score,
                "ok": true,
                "action": "click",
                "amount": "1.00",
            });
            store
                .append_json(&serde_json::to_vec(&obj).unwrap())
                .unwrap();
        }
        store.flush().unwrap();
        let summed = store
            .aggregate_with_filter(0, 10, &[], &[stat_req("score", true, false, false)])
            .unwrap();
        assert_eq!(
            summed[0].sum,
            Some(StatSum::Float {
                total: 0.0,
                contributed: true,
            })
        );
        store.close().unwrap();

        let overflowed = TempDir::new();
        let schema = write_schema(overflowed.path());
        let store =
            Store::open_with(overflowed.path().join("data"), &schema, test_options(8)).unwrap();
        for (ts, score) in [(1i64, 1e308f64), (2, 1e308)] {
            let obj = serde_json::json!({
                "ts": ts,
                "user_id": 1,
                "score": score,
                "ok": true,
                "action": "click",
                "amount": "1.00",
            });
            store
                .append_json(&serde_json::to_vec(&obj).unwrap())
                .unwrap();
        }
        store.flush().unwrap();
        let overflow = store.aggregate_with_filter(
            0,
            10,
            &[],
            &[
                stat_req("user_id", true, true, true),
                stat_req("score", true, true, true),
            ],
        );
        assert!(
            matches!(overflow, Err(Error::Event(ref message)) if message.contains("overflow")),
            "{overflow:?}"
        );
        for (ts, score) in [(3i64, -1e308f64), (4, -1e308)] {
            let obj = serde_json::json!({
                "ts": ts,
                "user_id": 1,
                "score": score,
                "ok": true,
                "action": "click",
                "amount": "1.00",
            });
            store
                .append_json(&serde_json::to_vec(&obj).unwrap())
                .unwrap();
        }
        store.flush().unwrap();
        let negative =
            store.aggregate_with_filter(3, 4, &[], &[stat_req("score", true, true, true)]);
        assert!(
            matches!(negative, Err(Error::Event(ref message)) if message.contains("overflow")),
            "{negative:?}"
        );
        store.close().unwrap();
    }

    #[test]
    fn float_sum_skips_a_row_outside_a_cut_block_and_keeps_extremes() {
        let dir = TempDir::new();
        let schema = write_schema(dir.path());
        let store = Store::open_with(dir.path().join("data"), &schema, test_options(2)).unwrap();
        for (ts, score) in [(1i64, 1e16f64), (2, 0.0), (3, -1e16), (4, 1.0)] {
            store
                .append_json(&score_event(ts, Some(score), "click"))
                .unwrap();
        }
        store.flush().unwrap();
        let summed = store
            .aggregate_with_filter(1, 4, &[], &[stat_req("score", true, false, false)])
            .unwrap();
        assert_eq!(
            summed[0].sum,
            Some(StatSum::Float {
                total: 1.0,
                contributed: true,
            })
        );
        store.close().unwrap();

        let cut_dir = TempDir::new();
        let schema = write_schema(cut_dir.path());
        let store =
            Store::open_with(cut_dir.path().join("data"), &schema, test_options(4)).unwrap();
        for (ts, score) in [(1i64, 1e16f64), (2, 0.0), (3, -1e16), (4, 1.0)] {
            store
                .append_json(&score_event(ts, Some(score), "click"))
                .unwrap();
        }
        store.flush().unwrap();
        let cut = store
            .aggregate_with_filter(3, 4, &[], &[stat_req("score", true, false, false)])
            .unwrap();
        assert_eq!(
            cut[0].sum,
            Some(StatSum::Float {
                total: -1e16 + 1.0,
                contributed: true,
            })
        );
        store.close().unwrap();

        let extremes = TempDir::new();
        let schema = write_schema(extremes.path());
        let store =
            Store::open_with(extremes.path().join("data"), &schema, test_options(128)).unwrap();
        for i in 0..100 {
            let score = (i % 100) as f64 / 10.0;
            store
                .append_json(&score_event(i, Some(score), "view"))
                .unwrap();
        }
        store.flush().unwrap();
        let both = store
            .aggregate_with_filter(0, 99, &[], &[stat_req("score", true, true, true)])
            .unwrap();
        assert_eq!(both[0].min, Some(Some(Scalar::Float(0.0))));
        assert_eq!(both[0].max, Some(Some(Scalar::Float(9.9))));
        let mut expected = 0.0;
        for i in 0..100 {
            expected += (i % 100) as f64 / 10.0;
        }
        match both[0].sum.unwrap() {
            StatSum::Float { total, contributed } => {
                assert!(contributed);
                assert_eq!(total.to_bits(), expected.to_bits());
            }
            other => panic!("expected a float sum, got {other:?}"),
        }
        let only = store
            .aggregate_with_filter(0, 99, &[], &[stat_req("score", true, false, false)])
            .unwrap();
        match only[0].sum.unwrap() {
            StatSum::Float { total, contributed } => {
                assert!(contributed);
                assert_eq!(total.to_bits(), expected.to_bits());
            }
            other => panic!("expected a float sum, got {other:?}"),
        }
        assert!(only[0].min.is_none());
        assert!(only[0].max.is_none());

        let with_null = TempDir::new();
        let schema = write_schema(with_null.path());
        let store =
            Store::open_with(with_null.path().join("data"), &schema, test_options(4)).unwrap();
        store
            .append_json(&score_event(10, Some(1.0), "click"))
            .unwrap();
        store.append_json(&score_event(20, None, "click")).unwrap();
        store
            .append_json(&score_event(30, Some(2.5), "buy"))
            .unwrap();
        store.flush().unwrap();
        let nullable = store
            .aggregate_with_filter(10, 30, &[], &[stat_req("score", true, false, false)])
            .unwrap();
        assert_eq!(
            nullable[0].sum,
            Some(StatSum::Float {
                total: 3.5,
                contributed: true,
            })
        );
        let clicks = store
            .aggregate_with_filter(
                10,
                30,
                &[Predicate::Eq("action".into(), "click".into())],
                &[stat_req("score", true, false, false)],
            )
            .unwrap();
        assert_eq!(
            clicks[0].sum,
            Some(StatSum::Float {
                total: 1.0,
                contributed: true,
            })
        );
        store.close().unwrap();
    }

    fn score_event(ts: i64, score: Option<f64>, action: &str) -> Vec<u8> {
        let mut obj = serde_json::json!({
            "ts": ts,
            "user_id": 1,
            "ok": true,
            "action": action,
            "amount": "1.00",
        });
        if let Some(score) = score {
            obj["score"] = serde_json::json!(score);
        }
        serde_json::to_vec(&obj).unwrap()
    }

    #[test]
    fn exact_sum_adds_stride_pieces_and_keeps_a_cut_block_on_the_visitor() {
        let dir = TempDir::new();
        let schema = write_schema(dir.path());
        let data = dir.path().join("data");
        let store = Store::open_with(&data, &schema, test_options(2048)).unwrap();
        let mut expected_full = 0i128;
        let mut expected_cut = 0i128;
        for i in 0..5000i64 {
            let user = i % 1000;
            expected_full += i128::from(user);
            if (100..=4500).contains(&i) {
                expected_cut += i128::from(user);
            }
            store
                .append_json(&event(i, Some(user), "click", None, "1.00"))
                .unwrap();
        }
        store.flush().unwrap();
        let full = store
            .aggregate_with_filter(0, 4999, &[], &[stat_req("user_id", true, false, false)])
            .unwrap();
        assert_eq!(exact_sum(&full, "user_id"), (expected_full, true));
        assert_eq!(expected_full, 2_497_500);
        let cut = store
            .aggregate_with_filter(100, 4500, &[], &[stat_req("user_id", true, false, false)])
            .unwrap();
        assert_eq!(exact_sum(&cut, "user_id"), (expected_cut, true));
        let rows = store.query(100, 4500).unwrap();
        let mut outside = 0i128;
        for row in &rows {
            let ts = row_value(&store, row)["ts"].as_i64().unwrap();
            let user = row_value(&store, row)["user_id"].as_i64().unwrap();
            assert!((100..=4500).contains(&ts));
            outside += i128::from(user);
        }
        assert_eq!(outside, expected_cut);
        let extremes = store
            .aggregate_with_filter(0, 4999, &[], &[stat_req("user_id", true, true, true)])
            .unwrap();
        assert_eq!(exact_sum(&extremes, "user_id"), (expected_full, true));
        assert_eq!(extremes[0].min, Some(Some(Scalar::Int(0))));
        assert_eq!(extremes[0].max, Some(Some(Scalar::Int(999))));
        let both = store
            .aggregate_with_filter(
                0,
                4999,
                &[],
                &[
                    stat_req("user_id", true, false, false),
                    stat_req("ts", true, false, false),
                ],
            )
            .unwrap();
        assert_eq!(exact_sum(&both, "user_id"), (expected_full, true));
        let ts_sum = i128::from(4999 * 5000 / 2);
        assert_eq!(exact_sum(&both, "ts"), (ts_sum, true));
        let timestamps = store
            .aggregate_with_filter(0, 4999, &[], &[stat_req("ts", true, false, false)])
            .unwrap();
        assert_eq!(exact_sum(&timestamps, "ts"), (ts_sum, true));
        store.close().unwrap();

        let cut_dir = TempDir::new();
        let schema = write_schema(cut_dir.path());
        let store =
            Store::open_with(cut_dir.path().join("data"), &schema, test_options(4)).unwrap();
        for i in 0..10i64 {
            store
                .append_json(&event(i, Some(i), "click", None, "1.00"))
                .unwrap();
        }
        store.flush().unwrap();
        let partial = store
            .aggregate_with_filter(2, 5, &[], &[stat_req("user_id", true, false, false)])
            .unwrap();
        assert_eq!(exact_sum(&partial, "user_id"), (2 + 3 + 4 + 5, true));
        {
            let mut catalog = store.catalog();
            let block = &mut catalog.segments[0].blocks[0];
            assert_eq!(block.min_ts, 0);
            block.min_ts = 2;
            block.max_ts = 5;
        }
        let indexed = store
            .aggregate_with_filter(2, 5, &[], &[stat_req("user_id", true, false, false)])
            .unwrap();
        assert_eq!(
            exact_sum(&indexed, "user_id"),
            (2 + 3 + 4 + 5, true),
            "the sparse index does not replace the payload timestamps"
        );
        store.close().unwrap();

        let nulls = TempDir::new();
        let schema = write_schema(nulls.path());
        let store = Store::open_with(nulls.path().join("data"), &schema, test_options(8)).unwrap();
        store
            .append_json(&event(1, Some(4), "click", None, "1.00"))
            .unwrap();
        store
            .append_json(&event(2, None, "click", None, "1.00"))
            .unwrap();
        store
            .append_json(&event(3, Some(6), "click", None, "1.00"))
            .unwrap();
        store.flush().unwrap();
        let skipped = store
            .aggregate_with_filter(0, 10, &[], &[stat_req("user_id", true, false, false)])
            .unwrap();
        assert_eq!(exact_sum(&skipped, "user_id"), (10, true));
        store.close().unwrap();

        let amounts = TempDir::new();
        let schema = write_schema(amounts.path());
        let store =
            Store::open_with(amounts.path().join("data"), &schema, test_options(64)).unwrap();
        let mut cents = 0i128;
        for i in 0..64i64 {
            cents += i128::from(i);
            let amount = format!("{}.{:02}", i / 100, i % 100);
            store
                .append_json(&event(i, Some(1), "click", None, &amount))
                .unwrap();
        }
        store.flush().unwrap();
        let summed = store
            .aggregate_with_filter(0, 63, &[], &[stat_req("amount", true, false, false)])
            .unwrap();
        assert_eq!(exact_sum(&summed, "amount"), (cents, true));
        store.close().unwrap();

        let wrapped = TempDir::new();
        let schema = write_schema(wrapped.path());
        let store =
            Store::open_with(wrapped.path().join("data"), &schema, test_options(8)).unwrap();
        let values = [i64::MAX, i64::MIN, i64::MIN.wrapping_add(1)];
        let mut wrapped_sum = 0i128;
        for (index, user) in values.into_iter().enumerate() {
            wrapped_sum += i128::from(user);
            store
                .append_json(&event(index as i64, Some(user), "click", None, "1.00"))
                .unwrap();
        }
        store.flush().unwrap();
        let summed = store
            .aggregate_with_filter(0, 10, &[], &[stat_req("user_id", true, false, false)])
            .unwrap();
        assert_eq!(exact_sum(&summed, "user_id"), (wrapped_sum, true));
        let with_bounds = store
            .aggregate_with_filter(0, 10, &[], &[stat_req("user_id", true, true, true)])
            .unwrap();
        assert_eq!(exact_sum(&with_bounds, "user_id"), (wrapped_sum, true));
        store.close().unwrap();

        let wide = TempDir::new();
        let schema_path = wide.path().join("schema.json");
        fs::write(
            &schema_path,
            r#"{
                "timestamp_field": "ts",
                "fields": [
                    {"name": "ts", "type": "timestamp"},
                    {"name": "amount", "type": "decimal", "scale": 0}
                ]
            }"#,
        )
        .unwrap();
        let store =
            Store::open_with(wide.path().join("data"), &schema_path, test_options(8)).unwrap();
        let huge = i128::MAX.to_string();
        for (ts, amount) in [(1i64, huge.as_str()), (2, huge.as_str())] {
            store
                .append_json(format!(r#"{{"ts":{ts},"amount":"{amount}"}}"#).as_bytes())
                .unwrap();
        }
        store.flush().unwrap();
        let overflow =
            store.aggregate_with_filter(0, 10, &[], &[stat_req("amount", true, false, false)]);
        assert!(
            matches!(overflow, Err(Error::Event(ref message)) if message.contains("overflow")),
            "{overflow:?}"
        );
        let overflow_bounds =
            store.aggregate_with_filter(0, 10, &[], &[stat_req("amount", true, true, true)]);
        assert!(
            matches!(overflow_bounds, Err(Error::Event(ref message)) if message.contains("overflow")),
            "{overflow_bounds:?}"
        );
        store.close().unwrap();

        let carried = TempDir::new();
        let schema_path = carried.path().join("schema.json");
        fs::write(
            &schema_path,
            r#"{
                "timestamp_field": "ts",
                "fields": [
                    {"name": "ts", "type": "timestamp"},
                    {"name": "amount", "type": "decimal", "scale": 0}
                ]
            }"#,
        )
        .unwrap();
        let store =
            Store::open_with(carried.path().join("data"), &schema_path, test_options(8)).unwrap();
        let low = (i128::MIN + 1).to_string();
        let high = i128::MAX.to_string();
        store
            .append_json(format!(r#"{{"ts":1,"amount":"{low}"}}"#).as_bytes())
            .unwrap();
        store.flush().unwrap();
        for ts in [2i64, 3] {
            store
                .append_json(format!(r#"{{"ts":{ts},"amount":"{high}"}}"#).as_bytes())
                .unwrap();
        }
        store.flush().unwrap();
        let with_bounds = store
            .aggregate_with_filter(0, 10, &[], &[stat_req("amount", true, true, true)])
            .unwrap();
        assert_eq!(exact_sum(&with_bounds, "amount"), (i128::MAX, true));
        let alone = store
            .aggregate_with_filter(0, 10, &[], &[stat_req("amount", true, false, false)])
            .unwrap();
        assert_eq!(exact_sum(&alone, "amount"), (i128::MAX, true));
        store.close().unwrap();
    }

    #[test]
    fn minmax_uses_zone_extremes_only_when_every_timestamp_is_inside() {
        let dir = TempDir::new();
        let schema = write_schema(dir.path());
        let data = dir.path().join("data");
        let store = Store::open_with(&data, &schema, test_options(4)).unwrap();
        for (ts, user, score, amount, action) in [
            (10i64, Some(1i64), Some(0.0f64), Some("1.00"), "buy"),
            (20, Some(7), Some(9.9), Some("3.50"), "buy"),
            (25, Some(9), Some(100.0), Some("8.00"), "view"),
            (30, None, None, None, "buy"),
        ] {
            let mut obj = serde_json::json!({
                "ts": ts,
                "ok": true,
                "action": action,
            });
            if let Some(user) = user {
                obj["user_id"] = serde_json::json!(user);
            }
            if let Some(score) = score {
                obj["score"] = serde_json::json!(score);
            }
            if let Some(amount) = amount {
                obj["amount"] = serde_json::json!(amount);
            }
            store
                .append_json(&serde_json::to_vec(&obj).unwrap())
                .unwrap();
        }
        store.flush().unwrap();

        let full = store
            .aggregate_with_filter(
                10,
                30,
                &[],
                &[
                    stat_req("score", false, true, true),
                    stat_req("user_id", false, true, true),
                    stat_req("amount", false, true, true),
                    stat_req("ts", false, true, true),
                ],
            )
            .unwrap();
        assert_eq!(full[0].min, Some(Some(Scalar::Float(0.0))));
        assert_eq!(full[0].max, Some(Some(Scalar::Float(100.0))));
        assert!(full[0].sum.is_none());
        assert_eq!(full[1].min, Some(Some(Scalar::Int(1))));
        assert_eq!(full[1].max, Some(Some(Scalar::Int(9))));
        assert_eq!(full[2].min, Some(Some(Scalar::Decimal(100))));
        assert_eq!(full[2].max, Some(Some(Scalar::Decimal(800))));
        assert_eq!(full[3].min, Some(Some(Scalar::Timestamp(10))));
        assert_eq!(full[3].max, Some(Some(Scalar::Timestamp(30))));

        let cut = store
            .aggregate_with_filter(
                20,
                20,
                &[],
                &[
                    stat_req("score", false, true, true),
                    stat_req("user_id", false, true, true),
                    stat_req("amount", false, true, true),
                ],
            )
            .unwrap();
        assert_eq!(cut[0].min, Some(Some(Scalar::Float(9.9))));
        assert_eq!(cut[0].max, Some(Some(Scalar::Float(9.9))));
        assert_eq!(cut[1].min, Some(Some(Scalar::Int(7))));
        assert_eq!(cut[1].max, Some(Some(Scalar::Int(7))));
        assert_eq!(cut[2].min, Some(Some(Scalar::Decimal(350))));
        assert_eq!(cut[2].max, Some(Some(Scalar::Decimal(350))));

        let with_sum = store
            .aggregate_with_filter(20, 20, &[], &[stat_req("score", true, true, true)])
            .unwrap();
        assert_eq!(
            with_sum[0].sum,
            Some(StatSum::Float {
                total: 9.9,
                contributed: true,
            })
        );
        assert_eq!(with_sum[0].min, Some(Some(Scalar::Float(9.9))));
        assert_eq!(with_sum[0].max, Some(Some(Scalar::Float(9.9))));

        let filtered = store
            .aggregate_with_filter(
                10,
                30,
                &[Predicate::Eq("action".into(), "buy".into())],
                &[stat_req("score", false, true, true)],
            )
            .unwrap();
        assert_eq!(filtered[0].min, Some(Some(Scalar::Float(0.0))));
        assert_eq!(filtered[0].max, Some(Some(Scalar::Float(9.9))));

        {
            let mut catalog = store.catalog();
            let block = &mut catalog.segments[0].blocks[0];
            assert!(block.min_ts <= 10 && block.max_ts >= 30);
            block.min_ts = 10;
            block.max_ts = 10;
        }
        let lied = store
            .aggregate_with_filter(10, 10, &[], &[stat_req("score", false, true, true)])
            .unwrap();
        assert_eq!(lied[0].min, Some(Some(Scalar::Float(0.0))));
        assert_eq!(
            lied[0].max,
            Some(Some(Scalar::Float(0.0))),
            "a sparse index that hides a later row must not publish that row's extreme"
        );
        store.close().unwrap();

        let nulls = TempDir::new();
        let schema = write_schema(nulls.path());
        let store = Store::open_with(nulls.path().join("data"), &schema, test_options(4)).unwrap();
        for ts in [1i64, 2] {
            let obj = serde_json::json!({
                "ts": ts,
                "user_id": 1,
                "ok": true,
                "action": "click",
                "amount": "1.00",
            });
            store
                .append_json(&serde_json::to_vec(&obj).unwrap())
                .unwrap();
        }
        store.flush().unwrap();
        let empty = store
            .aggregate_with_filter(1, 2, &[], &[stat_req("score", false, true, true)])
            .unwrap();
        assert_eq!(empty[0].min, Some(None));
        assert_eq!(empty[0].max, Some(None));
        store.close().unwrap();

        let unknown = TempDir::new();
        let schema = write_schema(unknown.path());
        let data = unknown.path().join("data");
        let store = Store::open_with(&data, &schema, test_options(4)).unwrap();
        for (ts, score) in [(1i64, 1.5f64), (2, 3.5)] {
            let obj = serde_json::json!({
                "ts": ts,
                "user_id": 1,
                "score": score,
                "ok": true,
                "action": "click",
                "amount": "1.00",
            });
            store
                .append_json(&serde_json::to_vec(&obj).unwrap())
                .unwrap();
        }
        store.flush().unwrap();
        let crc = crate::zone::schema_crc(store.schema());
        let fields = crate::zone::field_count(store.schema()).unwrap();
        let bounds: Vec<(i64, i64)> = store.catalog().segments[0]
            .blocks
            .iter()
            .map(|block| (block.min_ts, block.max_ts))
            .collect();
        let zone_path = crate::zone::zone_path(&data, store.catalog().segments[0].id);
        store.close().unwrap();
        crate::zone::rewrite_float_zones_unknown(&zone_path, crc, fields, &bounds).unwrap();
        let store = Store::open(&data, &schema).unwrap();
        let decoded = store
            .aggregate_with_filter(1, 2, &[], &[stat_req("score", false, true, true)])
            .unwrap();
        assert_eq!(decoded[0].min, Some(Some(Scalar::Float(1.5))));
        assert_eq!(decoded[0].max, Some(Some(Scalar::Float(3.5))));
        store.close().unwrap();
    }

    #[test]
    fn aggregate_grouped_orders_one_column_and_skips_unread_blocks() {
        let dir = TempDir::new();
        let schema = write_schema(dir.path());
        let data = dir.path().join("data");
        let store = Store::open_with(&data, &schema, test_options(8)).unwrap();
        let rows = [
            (1000i64, Some(1i64), true, "click", Some("b"), "1.00"),
            (2000, Some(5), false, "click", Some("A"), "2.00"),
            (3000, None, true, "view", Some("a"), "4.00"),
            (4000, Some(-1), false, "view", None, "10.00"),
        ];
        for (ts, user, ok, action, note, amount) in rows {
            let mut obj = serde_json::json!({
                "ts": ts,
                "score": 1.0,
                "ok": ok,
                "action": action,
                "amount": amount,
            });
            if let Some(user) = user {
                obj["user_id"] = serde_json::json!(user);
            }
            if let Some(note) = note {
                obj["note"] = serde_json::json!(note);
            }
            store
                .append_json(&serde_json::to_vec(&obj).unwrap())
                .unwrap();
        }
        let missing_action = serde_json::json!({
            "ts": 2500,
            "user_id": 9,
            "score": 1.0,
            "ok": false,
            "amount": "8.00",
        });
        store
            .append_json(&serde_json::to_vec(&missing_action).unwrap())
            .unwrap();
        store.flush().unwrap();

        let stats = [stat_req("amount", true, true, true)];
        let groups = store
            .aggregate_grouped(1000, 3000, &[], &stats, "action")
            .unwrap();
        assert_eq!(groups.len(), 3);
        assert_eq!(groups[0].key, Scalar::Null);
        assert_eq!(exact_sum(&groups[0].columns, "amount"), (800, true));
        assert_eq!(groups[1].key, Scalar::Str("click".into()));
        assert_eq!(exact_sum(&groups[1].columns, "amount"), (300, true));
        assert_eq!(groups[1].columns[0].min, Some(Some(Scalar::Decimal(100))));
        assert_eq!(groups[1].columns[0].max, Some(Some(Scalar::Decimal(200))));
        assert_eq!(groups[2].key, Scalar::Str("view".into()));
        assert_eq!(exact_sum(&groups[2].columns, "amount"), (400, true));
        assert_eq!(groups[2].columns[0].min, Some(Some(Scalar::Decimal(400))));
        assert_eq!(groups[2].columns[0].max, Some(Some(Scalar::Decimal(400))));

        let ungrouped = store
            .aggregate_with_filter(1000, 3000, &[], &stats)
            .unwrap();
        assert_eq!(exact_sum(&ungrouped, "amount"), (300 + 400 + 800, true));

        let none = store
            .aggregate_grouped(
                1000,
                3000,
                &[Predicate::Eq("action".into(), "missing".into())],
                &stats,
                "action",
            )
            .unwrap();
        assert!(none.is_empty());

        let only_click = store
            .aggregate_grouped(
                1000,
                3000,
                &[Predicate::Eq("action".into(), "click".into())],
                &stats,
                "action",
            )
            .unwrap();
        assert_eq!(only_click.len(), 1);
        assert_eq!(only_click[0].key, Scalar::Str("click".into()));
        assert_eq!(exact_sum(&only_click[0].columns, "amount"), (300, true));

        let cut = store
            .aggregate_grouped(1000, 1000, &[], &stats, "action")
            .unwrap();
        assert_eq!(cut.len(), 1);
        assert_eq!(cut[0].key, Scalar::Str("click".into()));
        assert_eq!(exact_sum(&cut[0].columns, "amount"), (100, true));

        let by_amount = store
            .aggregate_grouped(
                1000,
                4000,
                &[],
                &[stat_req("amount", true, false, false)],
                "amount",
            )
            .unwrap();
        let amount_keys: Vec<_> = by_amount
            .iter()
            .map(|group| match group.key {
                Scalar::Decimal(value) => value,
                _ => panic!("amount key {:?}", group.key),
            })
            .collect();
        assert_eq!(amount_keys, vec![100, 200, 400, 800, 1000]);

        let by_user = store
            .aggregate_grouped(
                1000,
                4000,
                &[],
                &[stat_req("user_id", true, false, false)],
                "user_id",
            )
            .unwrap();
        let user_keys: Vec<_> = by_user.iter().map(|group| group.key.clone()).collect();
        assert_eq!(
            user_keys,
            vec![
                Scalar::Null,
                Scalar::Int(-1),
                Scalar::Int(1),
                Scalar::Int(5),
                Scalar::Int(9),
            ]
        );

        let by_ok = store
            .aggregate_grouped(1000, 4000, &[], &stats, "ok")
            .unwrap();
        assert_eq!(by_ok[0].key, Scalar::Bool(false));
        assert_eq!(by_ok[1].key, Scalar::Bool(true));
        assert_eq!(
            exact_sum(&by_ok[0].columns, "amount"),
            (200 + 800 + 1000, true)
        );
        assert_eq!(exact_sum(&by_ok[1].columns, "amount"), (100 + 400, true));

        let by_note = store
            .aggregate_grouped(1000, 3000, &[], &stats, "note")
            .unwrap();
        let note_keys: Vec<_> = by_note.iter().map(|group| group.key.clone()).collect();
        assert_eq!(
            note_keys,
            vec![
                Scalar::Null,
                Scalar::Str("A".into()),
                Scalar::Str("a".into()),
                Scalar::Str("b".into()),
            ]
        );

        let by_ts = store
            .aggregate_grouped(1000, 2000, &[], &[stat_req("ts", true, true, true)], "ts")
            .unwrap();
        assert_eq!(by_ts[0].key, Scalar::Timestamp(1000));
        assert_eq!(by_ts[1].key, Scalar::Timestamp(2000));
        assert_eq!(exact_sum(&by_ts[0].columns, "ts"), (1000, true));

        let float = store.aggregate_grouped(0, 10, &[], &stats, "score");
        assert!(matches!(float, Err(Error::Schema(_))), "{float:?}");
        let json_schema = dir.path().join("json-schema.json");
        fs::write(
            &json_schema,
            r#"{
                "timestamp_field": "ts",
                "fields": [
                    {"name": "ts", "type": "timestamp"},
                    {"name": "props", "type": "json"},
                    {"name": "amount", "type": "decimal", "scale": 2}
                ]
            }"#,
        )
        .unwrap();
        let json_store =
            Store::open_with(dir.path().join("json-data"), &json_schema, test_options(8)).unwrap();
        let json_group = json_store.aggregate_grouped(0, 10, &[], &stats, "props");
        assert!(
            matches!(json_group, Err(Error::Schema(_))),
            "{json_group:?}"
        );
        let empty_name = store.aggregate_grouped(0, 10, &[], &stats, "");
        assert!(matches!(empty_name, Err(Error::Event(_))), "{empty_name:?}");
        let unknown = store.aggregate_grouped(0, 10, &[], &stats, "missing");
        assert!(matches!(unknown, Err(Error::Schema(_))), "{unknown:?}");
        assert_eq!(store.count(1000, 4000).unwrap(), 5);
        store.close().unwrap();
        json_store.close().unwrap();

        let wide = TempDir::new();
        let schema = write_schema(wide.path());
        let data = wide.path().join("data");
        let store = Store::open_with(&data, &schema, test_options(512)).unwrap();
        for id in 0..4097i64 {
            store
                .append_json(&event(id, Some(id), "k", None, "1.00"))
                .unwrap();
        }
        store.flush().unwrap();
        let capped = store
            .aggregate_grouped(
                0,
                4095,
                &[],
                &[stat_req("user_id", true, false, false)],
                "user_id",
            )
            .unwrap();
        assert_eq!(capped.len(), 4096);
        assert_eq!(capped[0].key, Scalar::Int(0));
        assert_eq!(capped[4095].key, Scalar::Int(4095));
        let overflow = store.aggregate_grouped(
            0,
            4096,
            &[],
            &[stat_req("user_id", true, false, false)],
            "user_id",
        );
        assert!(
            matches!(overflow, Err(Error::Event(ref message)) if message.contains("4096")),
            "{overflow:?}"
        );
        store.close().unwrap();

        let skipped = TempDir::new();
        let schema = write_schema(skipped.path());
        let data = skipped.path().join("data");
        let store = Store::open_with(&data, &schema, test_options(2)).unwrap();
        for (ts, user, action) in [
            (10i64, 1i64, "low"),
            (11, 2, "low"),
            (20, 10, "high"),
            (21, 11, "high"),
        ] {
            store
                .append_json(&event(ts, Some(user), action, None, "1.00"))
                .unwrap();
        }
        store.flush().unwrap();
        let gt = [Predicate::Cmp("user_id".into(), CmpOp::Gt, Scalar::Int(5))];
        let resolved = resolve_predicates(store.schema(), &gt).unwrap();
        assert_eq!(store.blocks_in_range(0, 100, &resolved).len(), 1);
        let victim = store.catalog().segments[0]
            .blocks
            .iter()
            .find(|block| block.max_ts <= 11)
            .cloned()
            .unwrap();
        let path = segment::data_path(&data, victim.segment_id);
        let mut bytes = fs::read(&path).unwrap();
        bytes[victim.offset as usize + segment::BLOCK_HEADER_LEN + 4] ^= 0xff;
        fs::write(&path, &bytes).unwrap();
        let kept = store
            .aggregate_grouped(
                0,
                100,
                &gt,
                &[stat_req("user_id", true, false, false)],
                "action",
            )
            .unwrap();
        assert_eq!(kept.len(), 1);
        assert_eq!(kept[0].key, Scalar::Str("high".into()));
        assert_eq!(exact_sum(&kept[0].columns, "user_id"), (21, true));
        let later = store
            .aggregate_grouped(
                20,
                21,
                &[],
                &[stat_req("user_id", true, false, false)],
                "action",
            )
            .unwrap();
        assert_eq!(later[0].key, Scalar::Str("high".into()));
        let unread = store.aggregate_grouped(
            victim.min_ts,
            victim.max_ts,
            &[],
            &[stat_req("user_id", true, false, false)],
            "score",
        );
        assert!(matches!(unread, Err(Error::Schema(_))), "{unread:?}");
        assert!(store
            .aggregate_with_filter(
                victim.min_ts,
                victim.max_ts,
                &[],
                &[stat_req("user_id", true, false, false)]
            )
            .is_err());
        store.close().unwrap();
    }

    #[test]
    fn grouped_text_keys_stop_at_the_query_byte_ceiling() {
        let dir = TempDir::new();
        let schema = write_schema(dir.path());
        let data = dir.path().join("data");
        let store = Store::open_with(&data, &schema, test_options(1)).unwrap();
        let half = "a".repeat(MAX_QUERY_BYTES / 2);
        let other = "b".repeat(MAX_QUERY_BYTES / 2);
        for (ts, note) in [(1, half.clone()), (2, half), (3, other)] {
            store
                .pipeline
                .append_row(Row {
                    ts,
                    values: vec![
                        Scalar::Timestamp(ts),
                        Scalar::Int(1),
                        Scalar::Float(1.0),
                        Scalar::Bool(true),
                        Scalar::Str("k".into()),
                        Scalar::Str(note),
                        Scalar::Decimal(100),
                    ],
                })
                .unwrap();
        }
        store.flush().unwrap();
        assert_eq!(store.count(1, 3).unwrap(), 3);
        let stats = [stat_req("user_id", true, false, false)];
        let repeated = store.aggregate_grouped(1, 2, &[], &stats, "note").unwrap();
        assert_eq!(repeated.len(), 1);
        assert_eq!(exact_sum(&repeated[0].columns, "user_id"), (2, true));
        let overflow = store.aggregate_grouped(1, 3, &[], &stats, "note");
        assert!(
            matches!(overflow, Err(Error::Event(ref message)) if message.contains("query response size limit exceeded")),
            "{overflow:?}"
        );
        store.close().unwrap();
    }

    #[test]
    fn quantile_rank_matches_the_exact_index() {
        let rank = |text: &str, n: usize| quantile_rank(parse_quantile_p(text).unwrap(), n);
        assert_eq!(rank("0", 100_000), Some(0));
        assert_eq!(rank("0.0", 100_000), Some(0));
        assert_eq!(rank("0.5", 100_000), Some(50_000));
        assert_eq!(rank("0.50", 100_000), Some(50_000));
        assert_eq!(rank("0.95", 100_000), Some(95_000));
        assert_eq!(rank("1", 100_000), Some(99_999));
        assert_eq!(rank("1.0", 100_000), Some(99_999));
        assert_eq!(rank("1.000", 4), Some(3));
        assert_eq!(rank("0.1", 10), Some(1));
        assert_eq!(rank("0.1", 3), Some(0));
        assert_eq!(rank(".5", 2), Some(1));
        assert_eq!(rank("0", 0), None);
        assert!(parse_quantile_p("1.1").is_err());
        assert!(parse_quantile_p("-0.1").is_err());
        assert!(parse_quantile_p("2").is_err());
        assert!(parse_quantile_p("0.95e0").is_err());
        assert!(parse_quantile_p("").is_err());
        let almost = parse_quantile_p(&format!("0.{}", "9".repeat(38))).unwrap();
        assert_eq!(quantile_rank(almost, usize::MAX), Some(usize::MAX - 1));
    }

    #[test]
    fn unit_stride_quantile_merges_runs_and_keeps_a_cut_on_the_sample() {
        let level = |text: &str| parse_quantile_p(text).unwrap();
        let overlap = [
            UnitRun {
                start: 0,
                end: 2,
                mult: 1,
            },
            UnitRun {
                start: 1,
                end: 1,
                mult: 1,
            },
        ];
        assert_eq!(select_unit_rank(&overlap, 0).unwrap(), 0);
        assert_eq!(select_unit_rank(&overlap, 1).unwrap(), 1);
        assert_eq!(select_unit_rank(&overlap, 2).unwrap(), 1);
        assert_eq!(select_unit_rank(&overlap, 3).unwrap(), 2);
        let top = [UnitRun {
            start: i128::MAX - 1,
            end: i128::MAX,
            mult: 1,
        }];
        assert_eq!(select_unit_rank(&top, 0).unwrap(), i128::MAX - 1);
        assert_eq!(select_unit_rank(&top, 1).unwrap(), i128::MAX);

        let dir = TempDir::new();
        let schema = write_schema(dir.path());
        let store = Store::open_with(dir.path().join("data"), &schema, test_options(1000)).unwrap();
        let count = 2_000usize;
        for i in 0..count {
            let amount = format!("{}.{:02}", (i % 100) / 100, i % 100);
            let obj = serde_json::json!({
                "ts": i as i64,
                "user_id": (i % 1_000) as i64,
                "score": (i % 100) as f64 / 10.0,
                "ok": true,
                "action": if i % 3 == 0 { "click" } else { "view" },
                "amount": amount,
            });
            store
                .append_json(&serde_json::to_vec(&obj).unwrap())
                .unwrap();
        }
        store.flush().unwrap();
        let rank = |field: &str, p: &str| {
            store
                .quantile_with_filter(0, count as i64 - 1, &[], field, level(p))
                .unwrap()
        };
        assert_eq!(rank("user_id", "0"), Some(Scalar::Int(0)));
        assert_eq!(rank("user_id", "0.5"), Some(Scalar::Int(500)));
        assert_eq!(rank("user_id", "0.95"), Some(Scalar::Int(950)));
        assert_eq!(rank("user_id", "1"), Some(Scalar::Int(999)));
        assert_eq!(rank("ts", "0"), Some(Scalar::Timestamp(0)));
        assert_eq!(rank("ts", "0.5"), Some(Scalar::Timestamp(1000)));
        assert_eq!(rank("ts", "1"), Some(Scalar::Timestamp(1999)));
        assert_eq!(rank("score", "0.5"), Some(Scalar::Float(5.0)));
        assert_eq!(rank("amount", "0"), Some(Scalar::Decimal(0)));
        assert_eq!(rank("amount", "1"), Some(Scalar::Decimal(99)));
        assert_eq!(
            store
                .quantile_with_filter(100, 200, &[], "user_id", level("0"))
                .unwrap(),
            Some(Scalar::Int(100)),
            "a cut block stays on the sample"
        );
        assert_eq!(
            store
                .quantile_with_filter(
                    0,
                    count as i64 - 1,
                    &[Predicate::Eq("action".into(), "click".into())],
                    "ts",
                    level("0.5"),
                )
                .unwrap(),
            Some(Scalar::Timestamp(999))
        );
        assert_eq!(
            store
                .quantile_with_filter(
                    0,
                    count as i64 - 1,
                    &[Predicate::Eq("action".into(), "click".into())],
                    "ts",
                    level("1"),
                )
                .unwrap(),
            Some(Scalar::Timestamp(1998))
        );
        store.close().unwrap();

        let down = TempDir::new();
        let schema = write_schema(down.path());
        let store = Store::open_with(down.path().join("data"), &schema, test_options(32)).unwrap();
        for (ts, user) in (0..10).map(|i| (i, 10 - i)) {
            store
                .append_json(&event(ts, Some(user), "click", None, "1.00"))
                .unwrap();
        }
        store.flush().unwrap();
        assert_eq!(
            store
                .quantile_with_filter(0, 9, &[], "user_id", level("0"))
                .unwrap(),
            Some(Scalar::Int(1))
        );
        assert_eq!(
            store
                .quantile_with_filter(0, 9, &[], "user_id", level("0.5"))
                .unwrap(),
            Some(Scalar::Int(6))
        );
        assert_eq!(
            store
                .quantile_with_filter(0, 9, &[], "user_id", level("1"))
                .unwrap(),
            Some(Scalar::Int(10))
        );
        store.close().unwrap();

        let wrapped = TempDir::new();
        let schema = write_schema(wrapped.path());
        let store =
            Store::open_with(wrapped.path().join("data"), &schema, test_options(8)).unwrap();
        for (ts, user) in [(1i64, i64::MAX), (2, i64::MIN), (3, i64::MIN + 1)] {
            store
                .append_json(&event(ts, Some(user), "click", None, "1.00"))
                .unwrap();
        }
        store.flush().unwrap();
        assert_eq!(
            store
                .quantile_with_filter(1, 3, &[], "user_id", level("0"))
                .unwrap(),
            Some(Scalar::Int(i64::MIN))
        );
        assert_eq!(
            store
                .quantile_with_filter(1, 3, &[], "user_id", level("0.5"))
                .unwrap(),
            Some(Scalar::Int(i64::MIN + 1))
        );
        assert_eq!(
            store
                .quantile_with_filter(1, 3, &[], "user_id", level("1"))
                .unwrap(),
            Some(Scalar::Int(i64::MAX))
        );
        store.close().unwrap();

        let constant = TempDir::new();
        let schema = write_schema(constant.path());
        let store =
            Store::open_with(constant.path().join("data"), &schema, test_options(8)).unwrap();
        for ts in 0..6 {
            store
                .append_json(&event(ts, Some(7), "click", None, "1.00"))
                .unwrap();
        }
        store.flush().unwrap();
        assert_eq!(
            store
                .quantile_with_filter(0, 5, &[], "user_id", level("0"))
                .unwrap(),
            Some(Scalar::Int(7))
        );
        assert_eq!(
            store
                .quantile_with_filter(0, 5, &[], "user_id", level("1"))
                .unwrap(),
            Some(Scalar::Int(7))
        );
        store.close().unwrap();

        let holes = TempDir::new();
        let schema = write_schema(holes.path());
        let store = Store::open_with(holes.path().join("data"), &schema, test_options(8)).unwrap();
        store
            .append_json(&event(1, Some(1), "click", None, "1.00"))
            .unwrap();
        store
            .append_json(&event(2, None, "click", None, "1.00"))
            .unwrap();
        store
            .append_json(&event(3, Some(3), "click", None, "1.00"))
            .unwrap();
        store.flush().unwrap();
        assert_eq!(
            store
                .quantile_with_filter(1, 3, &[], "user_id", level("0"))
                .unwrap(),
            Some(Scalar::Int(1))
        );
        assert_eq!(
            store
                .quantile_with_filter(1, 3, &[], "user_id", level("1"))
                .unwrap(),
            Some(Scalar::Int(3))
        );
        assert_eq!(
            store
                .quantile_with_filter(2, 2, &[], "user_id", level("0.5"))
                .unwrap(),
            None
        );
        store.close().unwrap();

        let stride = TempDir::new();
        let schema = write_schema(stride.path());
        let store = Store::open_with(stride.path().join("data"), &schema, test_options(8)).unwrap();
        for i in 0..5 {
            store
                .append_json(&event(i, Some(i * 2), "click", None, "1.00"))
                .unwrap();
        }
        store.flush().unwrap();
        assert_eq!(
            store
                .quantile_with_filter(0, 4, &[], "user_id", level("0.5"))
                .unwrap(),
            Some(Scalar::Int(4))
        );
        store.close().unwrap();
    }

    #[test]
    fn unit_stride_quantile_keeps_a_qualified_prefix_when_a_later_block_declines() {
        let level = |text: &str| parse_quantile_p(text).unwrap();
        let cut = TempDir::new();
        let schema = write_schema(cut.path());
        let store = Store::open_with(cut.path().join("data"), &schema, test_options(100)).unwrap();
        for i in 0..200i64 {
            store
                .append_json(&event(i, Some(i), "click", None, "1.00"))
                .unwrap();
        }
        store.flush().unwrap();
        assert_eq!(store.stats().blocks, 2);
        assert_eq!(
            store
                .quantile_with_filter(0, 149, &[], "user_id", level("0"))
                .unwrap(),
            Some(Scalar::Int(0))
        );
        assert_eq!(
            store
                .quantile_with_filter(0, 149, &[], "user_id", level("0.5"))
                .unwrap(),
            Some(Scalar::Int(75))
        );
        assert_eq!(
            store
                .quantile_with_filter(0, 149, &[], "user_id", level("1"))
                .unwrap(),
            Some(Scalar::Int(149))
        );
        store.close().unwrap();

        let tail = TempDir::new();
        let schema = write_schema(tail.path());
        let store = Store::open_with(tail.path().join("data"), &schema, test_options(32)).unwrap();
        for i in 0..40i64 {
            store
                .append_json(&event(i, Some(i), "click", None, "1.00"))
                .unwrap();
        }
        store.flush().unwrap();
        assert_eq!(store.stats().blocks, 2);
        assert_eq!(
            store
                .quantile_with_filter(0, 39, &[], "ts", level("0"))
                .unwrap(),
            Some(Scalar::Timestamp(0))
        );
        assert_eq!(
            store
                .quantile_with_filter(0, 39, &[], "ts", level("0.5"))
                .unwrap(),
            Some(Scalar::Timestamp(20))
        );
        assert_eq!(
            store
                .quantile_with_filter(0, 39, &[], "ts", level("1"))
                .unwrap(),
            Some(Scalar::Timestamp(39))
        );
        store.close().unwrap();
    }

    #[test]
    fn quantile_refuses_samples_past_the_query_byte_ceiling() {
        let width = std::mem::size_of::<i64>();
        let fit = MAX_QUERY_BYTES / width;
        retain_sample_bytes(fit, width).unwrap_err();
        retain_sample_bytes(fit - 1, width).unwrap();

        let width = std::mem::size_of::<i128>();
        let fit = MAX_QUERY_BYTES / width;
        let overflow = retain_sample_bytes(fit, width);
        assert!(
            matches!(overflow, Err(Error::Event(ref message)) if message.contains("query response size limit exceeded")),
            "{overflow:?}"
        );
        retain_sample_bytes(fit - 1, width).unwrap();
    }

    #[test]
    fn quantile_refuses_samples_past_the_query_row_ceiling() {
        let mut ints = QuantileSamples::new(FieldType::Int);
        for value in 0..MAX_QUERY_ROWS as i64 {
            ints.push(Scalar::Int(value)).unwrap();
        }
        let overflow = ints.push(Scalar::Int(0));
        assert!(
            matches!(overflow, Err(Error::Event(ref message)) if message.contains("query row limit exceeded")),
            "{overflow:?}"
        );
    }

    #[test]
    fn quantile_skips_nulls_outside_rows_and_rejected_blocks() {
        let level = |text: &str| parse_quantile_p(text).unwrap();
        let dir = TempDir::new();
        let schema = write_schema(dir.path());
        let data = dir.path().join("data");
        let store = Store::open_with(&data, &schema, test_options(8)).unwrap();
        for (ts, user, score, amount) in [
            (1000i64, Some(1i64), 5.0, "1.50"),
            (2000, Some(5), 0.0, "9.00"),
            (3000, Some(9), 6.0, "2.25"),
        ] {
            let obj = serde_json::json!({
                "ts": ts,
                "user_id": user,
                "score": score,
                "ok": true,
                "action": "buy",
                "amount": amount,
            });
            store
                .append_json(&serde_json::to_vec(&obj).unwrap())
                .unwrap();
        }
        let null_score = serde_json::json!({
            "ts": 2500,
            "user_id": 4,
            "ok": false,
            "action": "other",
            "amount": "4.00",
        });
        store
            .append_json(&serde_json::to_vec(&null_score).unwrap())
            .unwrap();
        store.flush().unwrap();

        let score = |from, to, p: &str| {
            store
                .quantile_with_filter(from, to, &[], "score", level(p))
                .unwrap()
        };
        assert_eq!(score(1000, 3000, "0"), Some(Scalar::Float(0.0)));
        assert_eq!(score(1000, 3000, "0.5"), Some(Scalar::Float(5.0)));
        assert_eq!(score(1000, 3000, "1"), Some(Scalar::Float(6.0)));
        assert_eq!(score(1000, 1000, "1"), Some(Scalar::Float(5.0)));
        assert_eq!(score(9000, 9000, "0.5"), None);
        assert_eq!(
            store
                .quantile_with_filter(2500, 2500, &[], "score", level("1"))
                .unwrap(),
            None,
            "a null score is not a sample"
        );
        assert_eq!(
            store
                .quantile_with_filter(1000, 3000, &[], "user_id", level("0"))
                .unwrap(),
            Some(Scalar::Int(1))
        );
        assert_eq!(
            store
                .quantile_with_filter(1000, 3000, &[], "user_id", level("1"))
                .unwrap(),
            Some(Scalar::Int(9))
        );
        assert_eq!(
            store
                .quantile_with_filter(1000, 2000, &[], "ts", level("1"))
                .unwrap(),
            Some(Scalar::Timestamp(2000))
        );
        assert_eq!(
            store
                .quantile_with_filter(1000, 3000, &[], "amount", level("0"))
                .unwrap(),
            Some(Scalar::Decimal(150))
        );
        let below =
            crate::value::scalar_from_literal(FieldType::Decimal { scale: 2 }, "3.00").unwrap();
        assert_eq!(
            store
                .quantile_with_filter(
                    1000,
                    3000,
                    &[Predicate::Cmp("amount".into(), CmpOp::Lt, below)],
                    "amount",
                    level("1"),
                )
                .unwrap(),
            Some(Scalar::Decimal(225))
        );
        assert!(matches!(
            store.quantile_with_filter(0, 10, &[], "action", level("0.5")),
            Err(Error::Schema(_))
        ));
        assert!(matches!(
            store.quantile_with_filter(0, 10, &[], "missing", level("0.5")),
            Err(Error::Schema(_))
        ));
        assert!(matches!(
            store.quantile_with_filter(0, 10, &[], "", level("0.5")),
            Err(Error::Event(_))
        ));

        let copies = 10usize;
        for index in 0..(copies * 100) {
            let score = (index % 100) as f64 / 10.0;
            let obj = serde_json::json!({
                "ts": 10_000 + index as i64,
                "user_id": 1,
                "score": score,
                "ok": true,
                "action": "view",
                "amount": "1.00",
            });
            store
                .append_json(&serde_json::to_vec(&obj).unwrap())
                .unwrap();
        }
        store.flush().unwrap();
        let tenths = |p: &str| {
            store
                .quantile_with_filter(
                    10_000,
                    10_000 + (copies * 100) as i64,
                    &[],
                    "score",
                    level(p),
                )
                .unwrap()
        };
        assert_eq!(tenths("0"), Some(Scalar::Float(0.0)));
        assert_eq!(tenths("0.5"), Some(Scalar::Float(5.0)));
        assert_eq!(tenths("0.95"), Some(Scalar::Float(9.5)));
        assert_eq!(tenths("1"), Some(Scalar::Float(9.9)));
        store.close().unwrap();

        let low_dir = TempDir::new();
        let schema = write_schema(low_dir.path());
        let data = low_dir.path().join("data");
        let store = Store::open_with(&data, &schema, test_options(2)).unwrap();
        for (ts, user, score) in [
            (10i64, 1i64, 1.0),
            (11, 2, 2.0),
            (20, 10, 8.0),
            (21, 11, 9.0),
        ] {
            let obj = serde_json::json!({
                "ts": ts,
                "user_id": user,
                "score": score,
                "ok": true,
                "action": "click",
                "amount": "1.00",
            });
            store
                .append_json(&serde_json::to_vec(&obj).unwrap())
                .unwrap();
        }
        store.flush().unwrap();
        let gt = [Predicate::Cmp("user_id".into(), CmpOp::Gt, Scalar::Int(5))];
        let resolved = resolve_predicates(store.schema(), &gt).unwrap();
        assert_eq!(store.blocks_in_range(0, 100, &resolved).len(), 1);
        let victim = store.catalog().segments[0]
            .blocks
            .iter()
            .find(|block| block.max_ts <= 11)
            .cloned()
            .unwrap();
        let outside = store.catalog().segments[0]
            .blocks
            .iter()
            .find(|block| block.min_ts >= 20)
            .cloned()
            .unwrap();
        let path = segment::data_path(&data, victim.segment_id);
        let mut bytes = fs::read(&path).unwrap();
        bytes[victim.offset as usize + segment::BLOCK_HEADER_LEN + 4] ^= 0xff;
        fs::write(&path, &bytes).unwrap();
        assert_eq!(
            store
                .quantile_with_filter(0, 100, &gt, "score", level("0"))
                .unwrap(),
            Some(Scalar::Float(8.0))
        );
        assert_eq!(
            store
                .quantile_with_filter(0, 100, &gt, "score", level("1"))
                .unwrap(),
            Some(Scalar::Float(9.0))
        );
        assert_eq!(
            store
                .quantile_with_filter(outside.min_ts, outside.max_ts, &[], "user_id", level("0"))
                .unwrap(),
            Some(Scalar::Int(10))
        );
        assert!(store
            .quantile_with_filter(victim.min_ts, victim.max_ts, &[], "user_id", level("0"))
            .is_err());
        store.close().unwrap();
    }

    #[test]
    fn latest_keeps_the_newest_row_and_orders_keys_like_grouped_stats() {
        let dir = TempDir::new();
        let schema = write_schema(dir.path());
        let data = dir.path().join("data");
        let store = Store::open_with(&data, &schema, test_options(2)).unwrap();
        let rows = [
            (1000i64, Some(1i64), true, "click", Some("b"), "1.00"),
            (5000, Some(9), true, "click", Some("b"), "9.00"),
            (4000, Some(3), false, "click", Some("b"), "8.00"),
            (5000, Some(2), true, "click", Some("Z"), "3.00"),
            (2000, None, false, "", None, "4.00"),
            (1500, Some(4), true, "view", Some("a"), "2.00"),
            (1100, Some(4), false, "view", Some("A"), "7.00"),
        ];
        for (ts, user, ok, action, note, amount) in rows {
            let mut obj = serde_json::json!({
                "ts": ts,
                "score": 1.5,
                "ok": ok,
                "amount": amount,
            });
            if let Some(user) = user {
                obj["user_id"] = serde_json::json!(user);
            }
            if !action.is_empty() {
                obj["action"] = serde_json::json!(action);
            }
            if let Some(note) = note {
                obj["note"] = serde_json::json!(note);
            }
            store
                .append_json(&serde_json::to_vec(&obj).unwrap())
                .unwrap();
        }
        store.flush().unwrap();

        let body = store.latest_json(1000, 5000, &[], "action", None).unwrap();
        let listed: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let listed = listed.as_array().unwrap();
        assert_eq!(listed.len(), 3);
        assert!(listed[0]["action"].is_null());
        assert_eq!(listed[0]["ts"], 2000);
        assert!(listed[0]["user_id"].is_null());
        assert_eq!(listed[0]["amount"], "4.00");
        assert_eq!(listed[1]["action"], "click");
        assert_eq!(listed[1]["ts"], 5000);
        assert_eq!(
            listed[1]["user_id"], 2,
            "a timestamp tie keeps the later row"
        );
        assert_eq!(listed[1]["note"], "Z");
        assert_eq!(listed[2]["action"], "view");
        assert_eq!(
            listed[2]["ts"], 1500,
            "a later ingest with an earlier timestamp does not win"
        );
        assert_eq!(listed[2]["user_id"], 4);

        let grouped = store
            .aggregate_grouped(
                1000,
                5000,
                &[],
                &[stat_req("user_id", true, false, false)],
                "action",
            )
            .unwrap();
        let group_keys: Vec<_> = grouped.iter().map(|group| group.key.clone()).collect();
        assert_eq!(
            group_keys,
            vec![
                Scalar::Null,
                Scalar::Str("click".into()),
                Scalar::Str("view".into()),
            ]
        );

        let one = store.query_json(5000, 5000).unwrap();
        let one: serde_json::Value = serde_json::from_slice(&one).unwrap();
        let tied = one
            .as_array()
            .unwrap()
            .iter()
            .find(|row| row["user_id"] == 2)
            .unwrap();
        assert_eq!(&listed[1], tied);

        let projected = store
            .latest_json(
                1000,
                5000,
                &[Predicate::Eq("note".into(), Scalar::Str("b".into()))],
                "user_id",
                Some(&[4, 0]),
            )
            .unwrap();
        assert_eq!(
            projected,
            br#"[{"ts":1000,"action":"click"},{"ts":4000,"action":"click"},{"ts":5000,"action":"click"}]"#
        );

        let ranged = store
            .latest_json(
                1000,
                5000,
                &[Predicate::Cmp("user_id".into(), CmpOp::Gt, Scalar::Int(3))],
                "action",
                None,
            )
            .unwrap();
        let ranged: serde_json::Value = serde_json::from_slice(&ranged).unwrap();
        assert_eq!(ranged.as_array().unwrap().len(), 2);
        assert_eq!(ranged[0]["action"], "click");
        assert_eq!(ranged[0]["user_id"], 9);
        assert_eq!(ranged[1]["action"], "view");
        assert_eq!(ranged[1]["user_id"], 4);

        let empty = store
            .latest_json(
                1000,
                5000,
                &[Predicate::Eq(
                    "action".into(),
                    Scalar::Str("missing".into()),
                )],
                "action",
                None,
            )
            .unwrap();
        assert_eq!(empty, b"[]");

        let by_user = store.latest_json(1000, 5000, &[], "user_id", None).unwrap();
        let by_user: serde_json::Value = serde_json::from_slice(&by_user).unwrap();
        let user_keys: Vec<_> = by_user
            .as_array()
            .unwrap()
            .iter()
            .map(|row| {
                if row["user_id"].is_null() {
                    None
                } else {
                    Some(row["user_id"].as_i64().unwrap())
                }
            })
            .collect();
        assert_eq!(
            user_keys,
            vec![None, Some(1), Some(2), Some(3), Some(4), Some(9)]
        );
        assert_eq!(by_user[1]["ts"], 1000, "user 1 keeps the only earlier row");

        let by_ok = store.latest_json(1000, 5000, &[], "ok", None).unwrap();
        let by_ok: serde_json::Value = serde_json::from_slice(&by_ok).unwrap();
        assert_eq!(by_ok[0]["ok"], false);
        assert_eq!(by_ok[0]["ts"], 4000);
        assert_eq!(by_ok[1]["ok"], true);
        assert_eq!(by_ok[1]["ts"], 5000);
        assert_eq!(by_ok[1]["user_id"], 2);

        let by_amount = store.latest_json(1000, 5000, &[], "amount", None).unwrap();
        let by_amount: serde_json::Value = serde_json::from_slice(&by_amount).unwrap();
        let amounts: Vec<_> = by_amount
            .as_array()
            .unwrap()
            .iter()
            .map(|row| row["amount"].as_str().unwrap().to_string())
            .collect();
        assert_eq!(
            amounts,
            vec!["1.00", "2.00", "3.00", "4.00", "7.00", "8.00", "9.00"]
        );

        let by_note = store.latest_json(1000, 2000, &[], "note", None).unwrap();
        let by_note: serde_json::Value = serde_json::from_slice(&by_note).unwrap();
        let notes: Vec<_> = by_note
            .as_array()
            .unwrap()
            .iter()
            .map(|row| {
                if row["note"].is_null() {
                    None
                } else {
                    Some(row["note"].as_str().unwrap().to_string())
                }
            })
            .collect();
        assert_eq!(
            notes,
            vec![None, Some("A".into()), Some("a".into()), Some("b".into())]
        );

        let float = store.latest_json(0, 10, &[], "score", None);
        assert!(matches!(float, Err(Error::Schema(_))), "{float:?}");
        let empty_name = store.latest_json(0, 10, &[], "", None);
        assert!(matches!(empty_name, Err(Error::Event(_))), "{empty_name:?}");
        let unknown = store.latest_json(0, 10, &[], "missing", None);
        assert!(matches!(unknown, Err(Error::Schema(_))), "{unknown:?}");
        store.close().unwrap();

        let wide = TempDir::new();
        let schema = write_schema(wide.path());
        let store =
            Store::open_with(wide.path().join("data"), &schema, test_options(8192)).unwrap();
        for id in 0..4097i64 {
            store
                .append_json(&event(id, Some(id), "k", None, "1.00"))
                .unwrap();
        }
        store.flush().unwrap();
        let capped = store.latest_json(0, 4095, &[], "user_id", None).unwrap();
        let capped: serde_json::Value = serde_json::from_slice(&capped).unwrap();
        assert_eq!(capped.as_array().unwrap().len(), 4096);
        assert_eq!(capped[0]["user_id"], 0);
        assert_eq!(capped[4095]["user_id"], 4095);
        let overflow = store.latest_json(0, 4096, &[], "user_id", None);
        assert!(
            matches!(overflow, Err(Error::Event(ref message)) if message.contains("4096")),
            "{overflow:?}"
        );
        assert!(overflow.as_ref().ok().is_none());
        store.close().unwrap();

        let skipped = TempDir::new();
        let schema = write_schema(skipped.path());
        let data = skipped.path().join("data");
        let store = Store::open_with(&data, &schema, test_options(2)).unwrap();
        for (ts, user, action) in [
            (10i64, 1i64, "low"),
            (11, 2, "low"),
            (20, 10, "high"),
            (21, 11, "high"),
        ] {
            store
                .append_json(&event(ts, Some(user), action, None, "1.00"))
                .unwrap();
        }
        store.flush().unwrap();
        let gt = [Predicate::Cmp("user_id".into(), CmpOp::Gt, Scalar::Int(5))];
        let resolved = resolve_predicates(store.schema(), &gt).unwrap();
        assert_eq!(store.blocks_in_range(0, 100, &resolved).len(), 1);
        let victim = store.catalog().segments[0]
            .blocks
            .iter()
            .find(|block| block.max_ts <= 11)
            .cloned()
            .unwrap();
        let path = segment::data_path(&data, victim.segment_id);
        let mut bytes = fs::read(&path).unwrap();
        bytes[victim.offset as usize + segment::BLOCK_HEADER_LEN + 4] ^= 0xff;
        fs::write(&path, &bytes).unwrap();
        let kept = store.latest_json(0, 100, &gt, "action", None).unwrap();
        let kept: serde_json::Value = serde_json::from_slice(&kept).unwrap();
        assert_eq!(kept.as_array().unwrap().len(), 1);
        assert_eq!(kept[0]["action"], "high");
        assert_eq!(kept[0]["user_id"], 11);
        let unread = store.latest_json(victim.min_ts, victim.max_ts, &[], "score", None);
        assert!(matches!(unread, Err(Error::Schema(_))), "{unread:?}");
        store.close().unwrap();
    }
    #[test]
    fn distinct_count_ignores_nulls_and_rows_outside_the_range() {
        let dir = TempDir::new();
        let schema = write_schema(dir.path());
        let data = dir.path().join("data");
        let store = Store::open_with(&data, &schema, test_options(2)).unwrap();
        store
            .append_json(&event(10, Some(1), "click", Some("A"), "1.00"))
            .unwrap();
        store
            .append_json(&event(11, None, "click", Some("a"), "1.00"))
            .unwrap();
        store
            .append_json(&event(20, Some(2), "view", Some("b"), "2.00"))
            .unwrap();
        store
            .append_json(&event(21, Some(2), "buy", None, "2.00"))
            .unwrap();
        store.flush().unwrap();

        assert_eq!(store.distinct_count(10, 21, &[], "user_id").unwrap(), 2);
        assert_eq!(store.distinct_count(10, 11, &[], "user_id").unwrap(), 1);
        assert_eq!(store.distinct_count(10, 21, &[], "action").unwrap(), 3);
        assert_eq!(store.distinct_count(10, 11, &[], "action").unwrap(), 1);
        assert_eq!(store.distinct_count(10, 21, &[], "note").unwrap(), 3);
        assert_eq!(store.distinct_count(10, 21, &[], "amount").unwrap(), 2);
        assert_eq!(store.distinct_count(10, 21, &[], "ok").unwrap(), 2);
        assert_eq!(store.distinct_count(10, 21, &[], "ts").unwrap(), 4);
        assert_eq!(store.distinct_count(10, 10, &[], "ts").unwrap(), 1);
        assert_eq!(store.distinct_count(12, 19, &[], "action").unwrap(), 0);
        assert_eq!(store.distinct_count(30, 10, &[], "action").unwrap(), 0);

        let clicked = [Predicate::Eq("action".into(), "click".into())];
        assert_eq!(
            store.distinct_count(10, 21, &clicked, "user_id").unwrap(),
            1
        );
        let above = [Predicate::Cmp("user_id".into(), CmpOp::Gt, Scalar::Int(1))];
        assert_eq!(store.distinct_count(10, 21, &above, "action").unwrap(), 2);
        assert_eq!(
            store
                .distinct_count(
                    10,
                    21,
                    &[Predicate::Eq("action".into(), "missing".into())],
                    "user_id"
                )
                .unwrap(),
            0
        );

        let zeros = TempDir::new();
        let schema = write_schema(zeros.path());
        let store = Store::open_with(zeros.path().join("data"), &schema, test_options(4)).unwrap();
        for (ts, score) in [(1i64, "0.0"), (2, "-0.0"), (3, "0"), (4, "1.5")] {
            store
                .append_json(
                    format!(
                        r#"{{"ts":{ts},"user_id":1,"score":{score},"ok":true,"action":"click","amount":"1.00"}}"#
                    )
                    .as_bytes(),
                )
                .unwrap();
        }
        store.flush().unwrap();
        assert_eq!(
            store.distinct_count(1, 4, &[], "score").unwrap(),
            3,
            "+0.0 and -0.0 are different bits; the integer lexeme 0 is +0.0"
        );
        assert_eq!(store.distinct_count(1, 1, &[], "score").unwrap(), 1);

        let mut row = value::parse_event(
            store.schema(),
            br#"{"ts":5,"user_id":1,"score":1.0,"ok":true,"action":"click","amount":"1.00"}"#,
        )
        .unwrap();
        row.values[2] = Scalar::Float(f64::NAN);
        store.pipeline.append_row(row).unwrap();
        store.flush().unwrap();
        let err = store.distinct_count(5, 5, &[], "score").unwrap_err();
        assert!(
            matches!(err, Error::Corrupt(_)) && err.to_string().contains("not finite"),
            "{err}"
        );
        assert_eq!(store.distinct_count(1, 4, &[], "score").unwrap(), 3);
        let overlap = store.distinct_count(1, 5, &[], "score").unwrap_err();
        assert!(
            matches!(overlap, Error::Corrupt(_)) && overlap.to_string().contains("not finite"),
            "{overlap}"
        );
        store.close().unwrap();

        let skipped = TempDir::new();
        let schema = write_schema(skipped.path());
        let data = skipped.path().join("data");
        let store = Store::open_with(&data, &schema, test_options(2)).unwrap();
        for (ts, user, action) in [
            (10i64, 1i64, "low"),
            (11, 2, "low"),
            (20, 10, "high"),
            (21, 11, "high"),
        ] {
            store
                .append_json(&event(ts, Some(user), action, None, "1.00"))
                .unwrap();
        }
        store.flush().unwrap();
        let gt = [Predicate::Cmp("user_id".into(), CmpOp::Gt, Scalar::Int(5))];
        let resolved = resolve_predicates(store.schema(), &gt).unwrap();
        assert_eq!(store.blocks_in_range(0, 100, &resolved).len(), 1);
        let victim = store.catalog().segments[0]
            .blocks
            .iter()
            .find(|block| block.max_ts <= 11)
            .cloned()
            .unwrap();
        let path = segment::data_path(&data, victim.segment_id);
        let mut bytes = fs::read(&path).unwrap();
        bytes[victim.offset as usize + segment::BLOCK_HEADER_LEN + 4] ^= 0xff;
        fs::write(&path, &bytes).unwrap();
        assert_eq!(store.distinct_count(0, 100, &gt, "action").unwrap(), 1);
        assert_eq!(store.distinct_count(20, 21, &[], "user_id").unwrap(), 2);
        let unread = store.distinct_count(0, 100, &[], "props");
        assert!(matches!(unread, Err(Error::Schema(_))), "{unread:?}");
        let empty_name = store.distinct_count(0, 100, &[], "");
        assert!(matches!(empty_name, Err(Error::Event(_))), "{empty_name:?}");
        store.close().unwrap();
    }

    #[test]
    fn unit_stride_distinct_counts_the_union_of_the_runs() {
        let count_users = |rows: &[(i64, i64)], block_rows: usize, from: i64, to: i64| -> u64 {
            let dir = TempDir::new();
            let schema = write_schema(dir.path());
            let store =
                Store::open_with(dir.path().join("data"), &schema, test_options(block_rows))
                    .unwrap();
            for (ts, user) in rows {
                store
                    .append_json(&event(*ts, Some(*user), "click", None, "1.00"))
                    .unwrap();
            }
            store.flush().unwrap();
            let count = store.distinct_count(from, to, &[], "user_id").unwrap();
            store.close().unwrap();
            count
        };

        assert_eq!(count_users(&[(1, 0), (2, 1), (3, 2), (4, 3)], 8, 1, 4), 4);
        assert_eq!(
            count_users(&[(1, 0), (2, 1), (3, 2), (4, 3)], 8, 2, 3),
            2,
            "a cut block stays on the row set"
        );
        assert_eq!(count_users(&[(1, 5), (2, 4), (3, 3), (4, 2)], 8, 1, 4), 4);
        assert_eq!(count_users(&[(1, 7), (2, 7), (3, 7)], 8, 1, 3), 1);
        assert_eq!(
            count_users(&[(1, 0), (2, 2)], 8, 1, 2),
            2,
            "stride 2 is not the closed interval"
        );
        assert_eq!(count_users(&[(1, 0), (2, 2), (3, 4)], 8, 1, 3), 3);
        assert_eq!(
            count_users(&[(1, 0), (2, 1), (3, 1), (4, 2), (5, 0), (6, 2)], 2, 1, 6),
            3,
            "a later stride of 2 is unioned with the unit runs"
        );
        assert_eq!(
            count_users(&[(1, 0), (2, 1), (3, 3), (4, 4)], 2, 1, 4),
            4,
            "a gap between unit runs is not filled"
        );
        assert_eq!(
            count_users(&[(1, i64::MAX), (2, i64::MIN), (3, i64::MIN + 1)], 8, 1, 3),
            3,
            "wrapping arithmetic is not the closed span of the endpoints"
        );

        let dir = TempDir::new();
        let schema = write_schema(dir.path());
        let store = Store::open_with(dir.path().join("data"), &schema, test_options(8)).unwrap();
        for (ts, amount) in [(1i64, "0.00"), (2, "0.01"), (3, "0.02")] {
            store
                .append_json(&event(ts, Some(1), "click", None, amount))
                .unwrap();
        }
        store.flush().unwrap();
        assert_eq!(store.distinct_count(1, 3, &[], "amount").unwrap(), 3);
        store.close().unwrap();

        let dir = TempDir::new();
        let schema = write_schema(dir.path());
        let store = Store::open_with(dir.path().join("data"), &schema, test_options(8)).unwrap();
        for (ts, amount) in [(1i64, "0.00"), (2, "0.02")] {
            store
                .append_json(&event(ts, Some(1), "click", None, amount))
                .unwrap();
        }
        store.flush().unwrap();
        assert_eq!(store.distinct_count(1, 2, &[], "amount").unwrap(), 2);
        store.close().unwrap();

        let dir = TempDir::new();
        let schema = write_schema(dir.path());
        let store = Store::open_with(dir.path().join("data"), &schema, test_options(8)).unwrap();
        for ts in [10i64, 20, 30] {
            store
                .append_json(&event(ts, Some(1), "click", None, "1.00"))
                .unwrap();
        }
        store.flush().unwrap();
        assert_eq!(store.distinct_count(10, 30, &[], "ts").unwrap(), 3);
        assert_eq!(store.distinct_count(10, 10, &[], "ts").unwrap(), 1);
        store.close().unwrap();
    }

    #[test]
    fn distinct_count_unions_proved_zones_and_walks_the_rest() {
        let dir = TempDir::new();
        let schema = write_schema(dir.path());
        let store = Store::open_with(dir.path().join("data"), &schema, test_options(2)).unwrap();
        let row = |ts: i64, ok: Option<bool>, action: &str, note: Option<&str>| {
            let mut value = serde_json::json!({
                "ts": ts,
                "user_id": 1,
                "score": 1.0,
                "action": action,
                "amount": "1.00",
            });
            if let Some(ok) = ok {
                value["ok"] = serde_json::json!(ok);
            }
            if let Some(note) = note {
                value["note"] = serde_json::json!(note);
            }
            serde_json::to_vec(&value).unwrap()
        };
        store
            .append_json(&row(10, Some(true), "click", Some("a")))
            .unwrap();
        store
            .append_json(&row(11, Some(true), "view", None))
            .unwrap();
        store
            .append_json(&row(20, Some(false), "buy", Some("b")))
            .unwrap();
        store
            .append_json(&row(21, Some(true), "scroll", Some("c")))
            .unwrap();
        store.flush().unwrap();

        assert_eq!(store.distinct_count(10, 21, &[], "action").unwrap(), 4);
        assert_eq!(store.distinct_count(10, 20, &[], "action").unwrap(), 3);
        assert_eq!(store.distinct_count(10, 11, &[], "action").unwrap(), 2);
        assert_eq!(store.distinct_count(20, 20, &[], "action").unwrap(), 1);
        assert_eq!(store.distinct_count(10, 11, &[], "note").unwrap(), 1);
        assert_eq!(store.distinct_count(10, 21, &[], "note").unwrap(), 3);
        assert_eq!(store.distinct_count(10, 11, &[], "ok").unwrap(), 1);
        assert_eq!(store.distinct_count(10, 21, &[], "ok").unwrap(), 2);
        assert_eq!(store.distinct_count(10, 20, &[], "ok").unwrap(), 2);
        let ok_true = [Predicate::Eq("ok".into(), Scalar::Bool(true))];
        assert_eq!(store.distinct_count(10, 21, &ok_true, "action").unwrap(), 3);
        store.close().unwrap();

        let wide = TempDir::new();
        let schema = write_schema(wide.path());
        let store = Store::open_with(wide.path().join("data"), &schema, test_options(40)).unwrap();
        for id in 0..40i64 {
            store
                .append_json(&row(id, Some(true), &format!("run-{id}"), Some("n")))
                .unwrap();
        }
        store
            .append_json(&row(40, Some(false), "click", None))
            .unwrap();
        store
            .append_json(&row(41, Some(false), "click", None))
            .unwrap();
        store.flush().unwrap();
        assert_eq!(store.catalog().segments[0].blocks.len(), 2);
        assert!(
            matches!(
                store.catalog().segments[0].zones[0].columns[4],
                crate::zone::ColumnZone::Bloom { .. }
            ),
            "forty actions are a bloom, so that block still walks"
        );
        assert!(matches!(
            store.catalog().segments[0].zones[1].columns[4],
            crate::zone::ColumnZone::Exact { .. }
        ));
        assert!(matches!(
            store.catalog().segments[0].zones[1].columns[5],
            crate::zone::ColumnZone::AllNull
        ));
        assert_eq!(store.distinct_count(0, 41, &[], "action").unwrap(), 41);
        assert_eq!(store.distinct_count(40, 41, &[], "action").unwrap(), 1);
        assert_eq!(store.distinct_count(40, 41, &[], "note").unwrap(), 0);
        assert_eq!(store.distinct_count(0, 41, &[], "user_id").unwrap(), 1);
        store.close().unwrap();
    }

    #[test]
    fn prefix_matches_string_bytes_and_the_zone_skips_the_rest() {
        let dir = TempDir::new();
        let schema = write_schema(dir.path());
        let data = dir.path().join("data");
        let store = Store::open_with(&data, &schema, test_options(2)).unwrap();
        store
            .append_json(&event(10, Some(1), "click", Some("landing"), "1.00"))
            .unwrap();
        store
            .append_json(&event(11, Some(2), "click", None, "2.00"))
            .unwrap();
        store.flush().unwrap();
        store
            .append_json(&event(20, Some(3), "view", Some("nullary"), "3.00"))
            .unwrap();
        store
            .append_json(&event(21, None, "view", Some("checkout"), "4.00"))
            .unwrap();
        store.flush().unwrap();
        assert!(store.stats().blocks >= 2);

        let click = [Predicate::Prefix("action".into(), "c".into())];
        let resolved = resolve_predicates(store.schema(), &click).unwrap();
        assert_eq!(store.blocks_in_range(0, 100, &resolved).len(), 1);
        assert_eq!(store.count_with_filter(0, 100, &click).unwrap(), 2);
        let rows = store.query_with_filter(0, 100, &click).unwrap();
        assert_eq!(rows.len(), 2);
        assert!(rows
            .iter()
            .all(|row| row_value(&store, row)["action"] == "click"));

        let missing = [Predicate::Prefix("action".into(), "zzz".into())];
        let resolved = resolve_predicates(store.schema(), &missing).unwrap();
        assert!(store.blocks_in_range(0, 100, &resolved).is_empty());
        assert_eq!(store.count_with_filter(0, 100, &missing).unwrap(), 0);
        assert!(store
            .query_with_filter(0, 100, &missing)
            .unwrap()
            .is_empty());

        let land = [Predicate::Prefix("note".into(), "land".into())];
        assert_eq!(store.count_with_filter(0, 100, &land).unwrap(), 1);
        let word_null = [Predicate::Prefix("note".into(), "null".into())];
        assert_eq!(store.count_with_filter(0, 100, &word_null).unwrap(), 1);
        let null_note = store.query_with_filter(0, 100, &word_null).unwrap();
        assert_eq!(row_value(&store, &null_note[0])["note"], "nullary");

        let both = [
            Predicate::Prefix("action".into(), "c".into()),
            Predicate::Prefix("note".into(), "land".into()),
        ];
        assert_eq!(store.count_with_filter(0, 100, &both).unwrap(), 1);
        let with_ok = [
            Predicate::Prefix("action".into(), "c".into()),
            Predicate::Eq("ok".into(), Scalar::Bool(true)),
        ];
        assert_eq!(store.count_with_filter(0, 100, &with_ok).unwrap(), 2);
        let with_in = [
            Predicate::Prefix("action".into(), "c".into()),
            Predicate::In("action".into(), vec!["click".into(), "view".into()]),
        ];
        assert_eq!(store.count_with_filter(0, 100, &with_in).unwrap(), 2);
        let with_range = [
            Predicate::Prefix("action".into(), "v".into()),
            Predicate::Cmp("user_id".into(), CmpOp::Ge, Scalar::Int(3)),
        ];
        assert_eq!(store.count_with_filter(0, 100, &with_range).unwrap(), 1);
        let conflict = [
            Predicate::Prefix("action".into(), "c".into()),
            Predicate::Prefix("action".into(), "s".into()),
        ];
        let resolved = resolve_predicates(store.schema(), &conflict).unwrap();
        assert!(resolved.impossible);
        assert_eq!(store.count_with_filter(0, 100, &conflict).unwrap(), 0);

        let buckets = store.histogram_with_filter(0, 100, 10, &click).unwrap();
        assert_eq!(buckets.iter().map(|bucket| bucket.count).sum::<u64>(), 2);
        let stats = store
            .aggregate_with_filter(
                0,
                100,
                &click,
                &[StatRequest {
                    field: "user_id".into(),
                    sum: false,
                    min: true,
                    max: false,
                    var: false,
                    stddev: false,
                }],
            )
            .unwrap();
        assert_eq!(stats[0].min, Some(Some(Scalar::Int(1))));
        assert_eq!(store.distinct_count(0, 100, &click, "action").unwrap(), 1);
        assert_eq!(store.topk(0, 100, &click, "note", 2).unwrap()[0].count, 1);
        assert_eq!(
            store
                .quantile_with_filter(0, 100, &click, "user_id", parse_quantile_p("0").unwrap())
                .unwrap(),
            Some(Scalar::Int(1))
        );
        let latest = store.latest_json(0, 100, &click, "action", None).unwrap();
        assert!(std::str::from_utf8(&latest).unwrap().contains("click"));

        for (name, prefix) in [
            ("user_id", "1"),
            ("ok", "true"),
            ("ts", "1"),
            ("score", "1"),
            ("amount", "1"),
        ] {
            let err = store
                .count_with_filter(0, 100, &[Predicate::Prefix(name.into(), prefix.into())])
                .unwrap_err();
            assert!(matches!(err, Error::Schema(_)), "{name}: {err}");
        }
        let err = store
            .count_with_filter(0, 100, &[Predicate::Prefix("action".into(), String::new())])
            .unwrap_err();
        assert!(matches!(err, Error::Schema(_)), "{err}");
        let err = store
            .count_with_filter(0, 100, &[Predicate::Prefix("missing".into(), "a".into())])
            .unwrap_err();
        assert!(matches!(err, Error::Schema(_)), "{err}");
        store.close().unwrap();
    }

    #[test]
    fn stock_events_have_the_distinct_counts_of_the_bench() {
        let dir = TempDir::new();
        let schema = write_schema(dir.path());
        let store = Store::open_with(dir.path().join("data"), &schema, test_options(2048)).unwrap();
        let actions = ["click", "view", "buy", "scroll"];
        let notes = ["landing", "checkout", "search", ""];
        let count = 100_000usize;
        for i in 0..count {
            let ts = 1_700_000_000_000i64 + i as i64 * 10;
            let amount_cents = (i % 5_000) as i64;
            let amount = format!("{}.{:02}", amount_cents / 100, amount_cents % 100);
            let note = notes[i % notes.len()];
            let mut value = serde_json::json!({
                "ts": ts,
                "user_id": (i % 1_000) as i64,
                "score": (i % 100) as f64 / 10.0,
                "ok": i % 2 == 0,
                "action": actions[i % actions.len()],
                "amount": amount,
            });
            if !note.is_empty() {
                value["note"] = serde_json::json!(note);
            }
            store
                .append_json(&serde_json::to_vec(&value).unwrap())
                .unwrap();
        }
        store.flush().unwrap();
        let start = 1_700_000_000_000i64;
        let end = start + (count as i64 - 1) * 10;
        let click_prefix = [Predicate::Prefix("action".into(), "c".into())];
        let scroll_prefix = [Predicate::Prefix("action".into(), "s".into())];
        let missing_prefix = [Predicate::Prefix("action".into(), "zzz".into())];
        let land_prefix = [Predicate::Prefix("note".into(), "land".into())];
        let click_ok = [
            Predicate::Prefix("action".into(), "c".into()),
            Predicate::Eq("ok".into(), Scalar::Bool(true)),
        ];
        assert_eq!(
            store.count_with_filter(start, end, &click_prefix).unwrap(),
            25_000
        );
        assert_eq!(
            store.count_with_filter(start, end, &scroll_prefix).unwrap(),
            25_000
        );
        assert_eq!(
            store
                .count_with_filter(start, end, &missing_prefix)
                .unwrap(),
            0
        );
        assert_eq!(
            store.count_with_filter(start, end, &land_prefix).unwrap(),
            25_000
        );
        // click is i % 4 == 0, which is also ok, so the conjunction is every click.
        assert_eq!(
            store.count_with_filter(start, end, &click_ok).unwrap(),
            25_000
        );
        assert!(store
            .blocks_in_range(
                start,
                end,
                &resolve_predicates(store.schema(), &missing_prefix).unwrap(),
            )
            .is_empty());
        assert_eq!(
            store.distinct_count(start, end, &[], "user_id").unwrap(),
            1000
        );
        assert_eq!(store.distinct_count(start, end, &[], "action").unwrap(), 4);
        assert_eq!(store.distinct_count(start, end, &[], "ok").unwrap(), 2);
        assert_eq!(store.distinct_count(start, end, &[], "note").unwrap(), 3);
        assert_eq!(store.distinct_count(start, end, &[], "score").unwrap(), 100);
        assert_eq!(
            store
                .distinct_count(start, start + 10, &[], "action")
                .unwrap(),
            2,
            "i in 0..=1 is click and view, not the four values on that block's zone"
        );
        let ok_true = [Predicate::Eq("ok".into(), Scalar::Bool(true))];
        assert_eq!(
            store
                .distinct_count(start, end, &ok_true, "action")
                .unwrap(),
            2,
            "ok=true is click and buy"
        );
        assert_eq!(
            store.distinct_count(start, end, &[], "ts").unwrap(),
            100_000
        );
        assert_eq!(
            store
                .distinct_count(start + 100 * 10, start + 200 * 10, &[], "user_id")
                .unwrap(),
            101
        );
        let even = [Predicate::Eq("ok".into(), Scalar::Bool(true))];
        assert_eq!(
            store.distinct_count(start, end, &even, "user_id").unwrap(),
            500
        );
        assert_eq!(
            store.distinct_count(start, start, &[], "user_id").unwrap(),
            1
        );
        assert_eq!(
            store
                .distinct_count(start + 1, start + 1, &[], "action")
                .unwrap(),
            0
        );
        let one = store.topk(start, end, &[], "action", 1).unwrap();
        assert_eq!(one.len(), 1);
        assert_eq!(one[0].value, Scalar::Str("buy".into()));
        assert_eq!(one[0].count, 25_000);
        let actions = store.topk(start, end, &[], "action", 4).unwrap();
        assert_eq!(
            actions
                .iter()
                .map(|hit| match &hit.value {
                    Scalar::Str(text) => text.as_str(),
                    _ => "",
                })
                .collect::<Vec<_>>(),
            vec!["buy", "click", "scroll", "view"]
        );
        assert!(actions.iter().all(|hit| hit.count == 25_000));
        let user = store.topk(start, end, &[], "user_id", 1).unwrap();
        assert_eq!(user.len(), 1);
        assert_eq!(user[0].value, Scalar::Int(0));
        assert_eq!(user[0].count, 100);
        let first = store.topk(start, start, &[], "user_id", 1).unwrap();
        assert_eq!(first[0].value, Scalar::Int(0));
        assert_eq!(first[0].count, 1);
        assert!(store
            .topk(start + 1, start + 1, &[], "action", 1)
            .unwrap()
            .is_empty());
        store.close().unwrap();
    }

    #[test]
    fn distinct_count_stops_past_one_million_values() {
        let dir = TempDir::new();
        let schema = write_schema(dir.path());
        let store = Store::open_with(dir.path().join("data"), &schema, test_options(8192)).unwrap();
        for id in 0..=1_000_000i64 {
            store
                .append_json(&event(id, Some(id), "click", None, "1.00"))
                .unwrap();
        }
        store.flush().unwrap();
        let capped = store.distinct_count(0, 999_999, &[], "user_id").unwrap();
        assert_eq!(capped, 1_000_000);
        let overflow = store.distinct_count(0, 1_000_000, &[], "user_id");
        assert!(
            matches!(overflow, Err(Error::Event(ref message)) if message == "distinct value limit exceeded"),
            "{overflow:?}"
        );
        store.close().unwrap();
    }

    #[test]
    fn distinct_count_charges_text_bytes_before_keeping_a_value() {
        let dir = TempDir::new();
        let schema = write_schema(dir.path());
        let mut options = test_options(4);
        options.segment_bytes = 256 * 1024 * 1024;
        let store = Store::open_with(dir.path().join("data"), &schema, options).unwrap();
        let width = 512 * 1024;
        let rows = (MAX_QUERY_BYTES / width) + 1;
        let start = 1_000_000i64;
        for id in 0..rows {
            let mut note = vec![b'x'; width];
            note[..8].copy_from_slice(format!("{id:08}").as_bytes());
            let note = String::from_utf8(note).unwrap();
            store
                .append_json(&event(
                    start + id as i64,
                    Some(1),
                    "click",
                    Some(&note),
                    "1.00",
                ))
                .unwrap();
        }
        store.flush().unwrap();
        let end = start + rows as i64 - 1;
        let kept = store.distinct_count(start, end - 1, &[], "note");
        assert_eq!(kept.unwrap(), (rows - 1) as u64);
        let overflow = store.distinct_count(start, end, &[], "note");
        assert!(
            matches!(
                overflow,
                Err(Error::Event(ref message)) if message == "query response size limit exceeded"
            ),
            "{overflow:?}"
        );
        store.close().unwrap();
    }

    #[test]
    fn topk_keeps_nulls_and_orders_ties_like_grouped_stats() {
        let dir = TempDir::new();
        let schema = write_schema(dir.path());
        let data = dir.path().join("data");
        let store = Store::open_with(&data, &schema, test_options(2)).unwrap();
        store
            .append_json(&event(10, Some(1), "click", Some("b"), "2.00"))
            .unwrap();
        store
            .append_json(&event(11, None, "click", Some("a"), "1.00"))
            .unwrap();
        store
            .append_json(&event(20, Some(1), "view", Some("a"), "2.00"))
            .unwrap();
        store
            .append_json(&event(21, Some(5), "buy", None, "1.00"))
            .unwrap();
        store
            .append_json(&event(30, Some(9), "buy", Some("a"), "9.00"))
            .unwrap();
        store.flush().unwrap();

        let users = store.topk(10, 21, &[], "user_id", 3).unwrap();
        assert_eq!(users[0].value, Scalar::Int(1));
        assert_eq!(users[0].count, 2);
        assert_eq!(users[1].value, Scalar::Null);
        assert_eq!(users[1].count, 1);
        assert_eq!(users[2].value, Scalar::Int(5));
        assert_eq!(users[2].count, 1);
        assert_eq!(store.topk(10, 21, &[], "user_id", 1).unwrap()[0].count, 2);
        let outside = store.topk(10, 21, &[], "action", 1).unwrap();
        assert_eq!(outside[0].value, Scalar::Str("click".into()));
        assert_eq!(outside[0].count, 2);
        let with_later = store.topk(10, 30, &[], "action", 2).unwrap();
        assert_eq!(with_later[0].value, Scalar::Str("buy".into()));
        assert_eq!(with_later[0].count, 2);
        assert_eq!(with_later[1].value, Scalar::Str("click".into()));
        assert_eq!(with_later[1].count, 2);

        let notes = store.topk(10, 21, &[], "note", 4).unwrap();
        assert_eq!(notes[0].value, Scalar::Str("a".into()));
        assert_eq!(notes[0].count, 2);
        assert_eq!(notes[1].value, Scalar::Null);
        assert_eq!(notes[2].value, Scalar::Str("b".into()));
        let amounts = store.topk(10, 21, &[], "amount", 2).unwrap();
        assert_eq!(amounts[0].count, 2);
        assert_eq!(amounts[0].value, Scalar::Decimal(100));
        assert_eq!(amounts[1].value, Scalar::Decimal(200));
        let flags = store.topk(10, 21, &[], "ok", 2).unwrap();
        assert_eq!(flags[0].value, Scalar::Bool(true));
        assert_eq!(flags[1].value, Scalar::Bool(false));
        let tied_flags = store.topk(10, 11, &[], "ok", 2).unwrap();
        assert_eq!(tied_flags[0].value, Scalar::Bool(false));
        assert_eq!(tied_flags[0].count, 1);
        assert_eq!(tied_flags[1].value, Scalar::Bool(true));
        assert_eq!(store.topk(12, 19, &[], "action", 4).unwrap(), Vec::new());
        assert!(store.topk(30, 10, &[], "action", 1).unwrap().is_empty());

        let clicked = [Predicate::Eq("action".into(), "click".into())];
        let clicked_users = store.topk(10, 21, &clicked, "user_id", 2).unwrap();
        assert_eq!(clicked_users.len(), 2);
        assert_eq!(clicked_users[0].count, 1);
        assert_eq!(clicked_users[0].value, Scalar::Null);
        assert_eq!(clicked_users[1].value, Scalar::Int(1));
        let above = [Predicate::Cmp("user_id".into(), CmpOp::Gt, Scalar::Int(1))];
        let above_actions = store.topk(10, 21, &above, "action", 2).unwrap();
        assert_eq!(above_actions.len(), 1);
        assert_eq!(above_actions[0].value, Scalar::Str("buy".into()));
        assert_eq!(above_actions[0].count, 1);

        let zeros = TempDir::new();
        let schema = write_schema(zeros.path());
        let store = Store::open_with(zeros.path().join("data"), &schema, test_options(4)).unwrap();
        for (ts, score) in [(1i64, "0.0"), (2, "-0.0"), (3, "0"), (4, "1.5")] {
            store
                .append_json(
                    format!(
                        r#"{{"ts":{ts},"user_id":1,"score":{score},"ok":true,"action":"click","amount":"1.00"}}"#
                    )
                    .as_bytes(),
                )
                .unwrap();
        }
        store.flush().unwrap();
        let scores = store.topk(1, 4, &[], "score", 4).unwrap();
        assert_eq!(scores.len(), 3);
        assert_eq!(scores[0].value, Scalar::Float(0.0));
        assert_eq!(scores[0].count, 2);
        assert_eq!(scores[1].count, 1);
        assert_eq!(scores[2].count, 1);
        let tied = [scores[1].value.clone(), scores[2].value.clone()];
        assert!(
            tied.iter().any(|value| matches!(value, Scalar::Float(bits) if bits.to_bits() == (-0.0f64).to_bits()))
                && tied.iter().any(|value| matches!(value, Scalar::Float(bits) if bits.to_bits() == 1.5f64.to_bits())),
            "{tied:?}"
        );
        let equal = store.topk(2, 4, &[], "score", 3).unwrap();
        assert!(matches!(
            &equal[0].value,
            Scalar::Float(value) if value.to_bits() == (-0.0f64).to_bits()
        ));
        assert!(matches!(
            &equal[1].value,
            Scalar::Float(value) if value.to_bits() == 0.0f64.to_bits()
        ));

        let mut row = value::parse_event(
            store.schema(),
            br#"{"ts":5,"user_id":1,"score":1.0,"ok":true,"action":"click","amount":"1.00"}"#,
        )
        .unwrap();
        row.values[2] = Scalar::Float(f64::NAN);
        store.pipeline.append_row(row).unwrap();
        store.flush().unwrap();
        let err = store.topk(5, 5, &[], "score", 1).unwrap_err();
        assert!(
            matches!(err, Error::Corrupt(_)) && err.to_string().contains("not finite"),
            "{err}"
        );
        assert_eq!(store.topk(1, 4, &[], "score", 1).unwrap()[0].count, 2);
        store.close().unwrap();

        let skipped = TempDir::new();
        let schema = write_schema(skipped.path());
        let data = skipped.path().join("data");
        let store = Store::open_with(&data, &schema, test_options(2)).unwrap();
        for (ts, user, action) in [
            (10i64, 1i64, "low"),
            (11, 2, "low"),
            (20, 10, "high"),
            (21, 11, "high"),
        ] {
            store
                .append_json(&event(ts, Some(user), action, None, "1.00"))
                .unwrap();
        }
        store.flush().unwrap();
        let gt = [Predicate::Cmp("user_id".into(), CmpOp::Gt, Scalar::Int(5))];
        let resolved = resolve_predicates(store.schema(), &gt).unwrap();
        assert_eq!(store.blocks_in_range(0, 100, &resolved).len(), 1);
        let victim = store.catalog().segments[0]
            .blocks
            .iter()
            .find(|block| block.max_ts <= 11)
            .cloned()
            .unwrap();
        let path = segment::data_path(&data, victim.segment_id);
        let mut bytes = fs::read(&path).unwrap();
        bytes[victim.offset as usize + segment::BLOCK_HEADER_LEN + 4] ^= 0xff;
        fs::write(&path, &bytes).unwrap();
        let kept = store.topk(0, 100, &gt, "action", 2).unwrap();
        assert_eq!(kept.len(), 1);
        assert_eq!(kept[0].value, Scalar::Str("high".into()));
        assert_eq!(kept[0].count, 2);
        let unread = store.topk(0, 100, &[], "props", 1);
        assert!(matches!(unread, Err(Error::Schema(_))), "{unread:?}");
        let empty_name = store.topk(0, 100, &[], "", 1);
        assert!(matches!(empty_name, Err(Error::Event(_))), "{empty_name:?}");
        let bad_k = store.topk(0, 100, &[], "action", 0);
        assert!(
            matches!(bad_k, Err(Error::Event(ref message)) if message.contains("1..=4096")),
            "{bad_k:?}"
        );
        store.close().unwrap();
    }

    #[test]
    fn topk_stops_past_one_million_values_and_the_text_ceiling() {
        let dir = TempDir::new();
        let schema = write_schema(dir.path());
        let store = Store::open_with(dir.path().join("data"), &schema, test_options(8192)).unwrap();
        for id in 0..=1_000_000i64 {
            store
                .append_json(&event(id, Some(id), "click", None, "1.00"))
                .unwrap();
        }
        store.flush().unwrap();
        let capped = store.topk(0, 999_999, &[], "user_id", 1).unwrap();
        assert_eq!(capped.len(), 1);
        assert_eq!(capped[0].count, 1);
        let overflow = store.topk(0, 1_000_000, &[], "user_id", 1);
        assert!(
            matches!(overflow, Err(Error::Event(ref message)) if message == "distinct value limit exceeded"),
            "{overflow:?}"
        );
        store.close().unwrap();

        let wide = TempDir::new();
        let schema = write_schema(wide.path());
        let mut options = test_options(4);
        options.segment_bytes = 256 * 1024 * 1024;
        let store = Store::open_with(wide.path().join("data"), &schema, options).unwrap();
        let width = 512 * 1024;
        let rows = (MAX_QUERY_BYTES / width) + 1;
        let start = 1_000_000i64;
        for id in 0..rows {
            let mut note = vec![b'x'; width];
            note[..8].copy_from_slice(format!("{id:08}").as_bytes());
            let note = String::from_utf8(note).unwrap();
            store
                .append_json(&event(
                    start + id as i64,
                    Some(1),
                    "click",
                    Some(&note),
                    "1.00",
                ))
                .unwrap();
        }
        store.flush().unwrap();
        let end = start + rows as i64 - 1;
        let kept = store.topk(start, end - 1, &[], "note", 2).unwrap();
        assert_eq!(kept.len(), 2);
        assert!(kept.iter().all(|hit| hit.count == 1));
        let overflow = store.topk(start, end, &[], "note", 1);
        assert!(
            matches!(
                overflow,
                Err(Error::Event(ref message)) if message == "query response size limit exceeded"
            ),
            "{overflow:?}"
        );
        store.close().unwrap();
    }
}
