use std::collections::HashMap;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use crate::codec::{
    block_row_count, decode_rows_in_range_filtered, read_timestamp_column, ColumnPredicate,
};
use crate::error::{Error, Result};
use crate::pipeline::{self, Pipeline, PipelineConfig};
use crate::schema::{self, Schema};
use crate::segment::{self, Catalog};
use crate::value::{self, row_to_json_bytes, Row, Scalar};

/// Maximum JSON bytes a single query may materialize in the response buffer.
pub const MAX_QUERY_BYTES: usize = 64 * 1024 * 1024;

/// Maximum rows a single query may return.
pub const MAX_QUERY_ROWS: usize = 1_000_000;

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

/// Equality constraint pushed into block decoding.
///
/// Predicates on one query are AND-ed. [`Predicate::In`] matches any listed value.
/// [`Scalar::Null`] matches a null column. An empty [`Predicate::In`] matches nothing.
#[derive(Debug, Clone, PartialEq)]
pub enum Predicate {
    /// `field == value`, for example `Predicate::Eq("type".into(), "assistant".into())`.
    Eq(String, Scalar),
    /// `field` equals one of `values`.
    In(String, Vec<Scalar>),
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

    /// Inclusive time range plus equality predicates.
    ///
    /// Blocks whose zone map cannot contain the requested values are not read.
    /// Each predicate on a block that is read is applied while the block is decoded.
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
                resolved.is_empty(),
                contained,
                skipped,
                offset,
                limit,
                rows_out.len(),
                block.row_count,
            );
            if block_exceeds_read_budget(block.uncompressed_len, whole, string_budget) {
                return Err(Error::event("query response size limit exceeded"));
            }
            let payload = self.read_block_bytes(&block, &mut dictionaries)?;
            let nrows = block_row_count(&payload)?;
            require_header_row_count(block.row_count, nrows)?;
            if resolved.is_empty()
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
            let rows = decode_rows_in_range_filtered(
                &self.schema,
                &payload,
                from_ms,
                to_ms,
                string_budget,
                &resolved,
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
        if limit == Some(0) {
            return Err(Error::event("query limit must be a positive integer"));
        }
        let Some(resolved) = self.prepare_scan(from_ms, to_ms, predicates)? else {
            return Ok(b"[]".to_vec());
        };
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
                resolved.is_empty(),
                contained,
                skipped,
                offset,
                limit,
                row_count,
                block.row_count,
            );
            if block_exceeds_read_budget(block.uncompressed_len, whole, string_budget) {
                return Err(Error::event("query response size limit exceeded"));
            }
            let payload = self.read_block_bytes(&block, &mut dictionaries)?;
            let nrows = block_row_count(&payload)?;
            require_header_row_count(block.row_count, nrows)?;
            if resolved.is_empty()
                && contained
                && offset_covers_block(offset, skipped, block.row_count)
            {
                if every_timestamp_in_range(&self.schema, &payload, from_ms, to_ms)?
                    && skip_whole_block(offset, block.row_count, &mut skipped)
                {
                    continue;
                }
            }
            if whole && row_count + nrows > MAX_QUERY_ROWS {
                return Err(Error::event("query row limit exceeded"));
            }
            let rows = decode_rows_in_range_filtered(
                &self.schema,
                &payload,
                from_ms,
                to_ms,
                string_budget,
                &resolved,
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
                    let row_bytes = row_to_json_bytes(&self.schema, &row)?;
                    let next_len = out
                        .len()
                        .checked_add(row_bytes.len())
                        .and_then(|n| n.checked_add(if wrote { 1 } else { 0 }))
                        .and_then(|n| n.checked_add(1))
                        .ok_or_else(|| Error::event("query response size overflow"))?;
                    if next_len > MAX_QUERY_BYTES {
                        return Err(Error::event("query response size limit exceeded"));
                    }
                    if wrote {
                        out.push(b',');
                    }
                    wrote = true;
                    row_count += 1;
                    out.extend_from_slice(&row_bytes);
                }
            }
            if page_done {
                break;
            }
        }
        out.push(b']');
        Ok(out)
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
    ) -> Result<Option<Vec<ColumnPredicate>>> {
        if from_ms > to_ms {
            return Ok(None);
        }
        let resolved = resolve_predicates(&self.schema, predicates)?;
        if resolved.iter().any(|pred| pred.allowed.is_empty()) {
            return Ok(None);
        }
        self.flush()?;
        Ok(Some(resolved))
    }

    fn read_block_bytes(
        &self,
        block: &segment::BlockMeta,
        dictionaries: &mut HashMap<u32, Option<Vec<u8>>>,
    ) -> Result<Vec<u8>> {
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
        segment::read_block_payload(&data_path, block, dictionary)
    }

    fn blocks_in_range(
        &self,
        from_ms: i64,
        to_ms: i64,
        resolved: &[ColumnPredicate],
    ) -> Vec<segment::BlockMeta> {
        let catalog = self.catalog();
        let mut blocks = Vec::new();
        for segment in &catalog.segments {
            for (index, block) in segment.blocks.iter().enumerate() {
                if block.max_ts < from_ms || block.min_ts > to_ms {
                    continue;
                }
                if segment
                    .zones
                    .get(index)
                    .is_some_and(|zone| !crate::zone::may_match(zone, resolved))
                {
                    continue;
                }
                blocks.push(block.clone());
            }
        }
        blocks
    }

    fn catalog(&self) -> std::sync::MutexGuard<'_, Catalog> {
        self.pipeline
            .catalog
            .lock()
            .unwrap_or_else(|err| err.into_inner())
    }
}

impl Drop for Store {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

/// Bytes already written, including the opening `[`. A further row needs at
/// least one payload byte plus the closing `]`, so a full buffer stops the
/// next block before it is read or decoded.
fn resolve_predicates(schema: &Schema, predicates: &[Predicate]) -> Result<Vec<ColumnPredicate>> {
    let mut grouped: Vec<ColumnPredicate> = Vec::new();
    for predicate in predicates {
        let (name, values) = match predicate {
            Predicate::Eq(name, value) => (name.as_str(), vec![value.clone()]),
            Predicate::In(name, values) => (name.as_str(), values.clone()),
        };
        let index = schema
            .fields
            .iter()
            .position(|field| field.name == name)
            .ok_or_else(|| Error::schema(format!("unknown filter field `{name}`")))?;
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
    }
    Ok(grouped)
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
/// returned in full also refuses a length the shrinking response budget cannot hold.
fn block_exceeds_read_budget(uncompressed_len: u32, whole: bool, string_budget: usize) -> bool {
    let declared = uncompressed_len as usize;
    declared > MAX_QUERY_BYTES || (whole && declared > string_budget)
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
        let stored = fs::read(&index_path).unwrap();
        let mut index = if stored.len() >= 4 && stored[..4] == [0x28, 0xB5, 0x2F, 0xFD] {
            zstd::bulk::decompress(&stored, 1024 * 1024).unwrap()
        } else {
            stored
        };
        assert_eq!(&index[..4], segment::INDEX_MAGIC);
        let entry = segment::INDEX_HEADER_LEN;
        if let Some(len) = uncompressed_len {
            index[entry + 12..entry + 16].copy_from_slice(&len.to_le_bytes());
        }
        if let Some(rows) = row_count {
            index[entry + 16..entry + 20].copy_from_slice(&rows.to_le_bytes());
        }
        fs::write(&index_path, &index).unwrap();
    }

    /// A 20-byte frame does not store min/max. Open keeps the index values when
    /// the other header fields match, so a lying zone can hide an outside timestamp.
    fn patch_index_minmax(dir: &Path, segment_id: u32, min_ts: i64, max_ts: i64) {
        let index_path = segment::index_path(dir, segment_id);
        let stored = fs::read(&index_path).unwrap();
        let mut index = if stored.len() >= 4 && stored[..4] == [0x28, 0xB5, 0x2F, 0xFD] {
            zstd::bulk::decompress(&stored, 1024 * 1024).unwrap()
        } else {
            stored
        };
        assert_eq!(&index[..4], segment::INDEX_MAGIC);
        let entry = segment::INDEX_HEADER_LEN;
        index[entry + 24..entry + 32].copy_from_slice(&min_ts.to_le_bytes());
        index[entry + 32..entry + 40].copy_from_slice(&max_ts.to_le_bytes());
        fs::write(&index_path, &index).unwrap();
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
}
