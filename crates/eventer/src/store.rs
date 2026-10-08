use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use crate::codec::{block_row_count, decode_rows_in_range_filtered, ColumnPredicate};
use crate::error::{Error, Result};
use crate::pipeline::{self, Pipeline, PipelineConfig};
use crate::schema::{self, Schema};
use crate::segment::{self, BlockMeta, Catalog};
use crate::summary::{self, SummaryPredicate};
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
    decompressed_blocks: AtomicU64,
    ignore_summaries: AtomicBool,
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
        ensure_schema_lock(&dir, &schema)?;
        let catalog = Arc::new(std::sync::Mutex::new(segment::load_catalog(&dir, &schema)?));
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
            decompressed_blocks: AtomicU64::new(0),
            ignore_summaries: AtomicBool::new(false),
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

    /// Force a partial block out and fsync it.
    pub fn flush(&self) -> Result<()> {
        self.pipeline.flush()
    }

    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    /// Compressed blocks whose bodies were decompressed by queries on this store.
    pub fn decompressed_blocks(&self) -> u64 {
        self.decompressed_blocks.load(Ordering::Relaxed)
    }

    /// When set, queries ignore version-2 presence summaries and decompress every
    /// time-overlapping block. The exact predicate still runs.
    pub fn set_ignore_summaries(&self, ignore: bool) {
        self.ignore_summaries.store(ignore, Ordering::Relaxed);
    }

    /// Inclusive range on the schema timestamp, in unix milliseconds. Results are in ingest order.
    pub fn query(&self, from_ms: i64, to_ms: i64) -> Result<Vec<Row>> {
        self.query_with_filter(from_ms, to_ms, &[])
    }

    /// Inclusive time range plus equality predicates.
    ///
    /// Each predicate is applied while the block is decoded. If a filter column's
    /// constant or dictionary cannot contain the requested value, the rest of that
    /// block is not decoded. Unknown fields and values of the wrong column type
    /// return [`Error::Schema`]. Checks run before [`Store::flush`].
    pub fn query_with_filter(
        &self,
        from_ms: i64,
        to_ms: i64,
        predicates: &[Predicate],
    ) -> Result<Vec<Row>> {
        let Some(resolved) = self.prepare_scan(from_ms, to_ms, predicates)? else {
            return Ok(Vec::new());
        };
        let summary_preds = summary_predicates(&resolved);
        let blocks = self.blocks_overlapping(from_ms, to_ms);
        let mut rows_out = Vec::new();
        let mut response_bytes = 1usize;
        for block in blocks {
            if rows_out.len() >= MAX_QUERY_ROWS {
                return Err(Error::event("query row limit exceeded"));
            }
            let string_budget = remaining_query_bytes(response_bytes)?;
            let contained = block.min_ts >= from_ms && block.max_ts <= to_ms;
            if resolved.is_empty() && contained && block.uncompressed_len as usize > string_budget {
                return Err(Error::event("query response size limit exceeded"));
            }
            if self.summary_miss(&block, &summary_preds) {
                continue;
            }
            let payload = self.read_block(&block)?;
            let nrows = block_row_count(&payload)?;
            if resolved.is_empty() && contained && rows_out.len() + nrows > MAX_QUERY_ROWS {
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
            for row in rows {
                if row.ts >= from_ms && row.ts <= to_ms {
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
        let Some(resolved) = self.prepare_scan(from_ms, to_ms, predicates)? else {
            return Ok(b"[]".to_vec());
        };
        let summary_preds = summary_predicates(&resolved);
        let blocks = self.blocks_overlapping(from_ms, to_ms);
        let mut out = Vec::from(b"[");
        let mut wrote = false;
        let mut row_count = 0usize;
        for block in blocks {
            if row_count >= MAX_QUERY_ROWS {
                return Err(Error::event("query row limit exceeded"));
            }
            let string_budget = remaining_query_bytes(out.len())?;
            let contained = block.min_ts >= from_ms && block.max_ts <= to_ms;
            if resolved.is_empty() && contained && block.uncompressed_len as usize > string_budget {
                return Err(Error::event("query response size limit exceeded"));
            }
            if self.summary_miss(&block, &summary_preds) {
                continue;
            }
            let payload = self.read_block(&block)?;
            let nrows = block_row_count(&payload)?;
            if resolved.is_empty() && contained && row_count + nrows > MAX_QUERY_ROWS {
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
            for row in rows {
                if row.ts >= from_ms && row.ts <= to_ms {
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
        }
        out.push(b']');
        Ok(out)
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

    fn summary_miss(&self, block: &BlockMeta, predicates: &[SummaryPredicate<'_>]) -> bool {
        if self.ignore_summaries.load(Ordering::Relaxed) || block.summary.is_empty() {
            return false;
        }
        !summary::might_match(&block.summary, predicates)
    }

    fn read_block(&self, block: &BlockMeta) -> Result<Vec<u8>> {
        let payload =
            segment::read_block_payload(&segment::data_path(&self.dir, block.segment_id), block)?;
        self.decompressed_blocks.fetch_add(1, Ordering::Relaxed);
        Ok(payload)
    }

    fn blocks_overlapping(&self, from_ms: i64, to_ms: i64) -> Vec<BlockMeta> {
        let catalog = self.catalog();
        catalog
            .segments
            .iter()
            .flat_map(|segment| {
                segment
                    .blocks
                    .iter()
                    .filter(|block| block.max_ts >= from_ms && block.min_ts <= to_ms)
                    .cloned()
            })
            .collect()
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
fn summary_predicates(predicates: &[ColumnPredicate]) -> Vec<SummaryPredicate<'_>> {
    predicates
        .iter()
        .map(|predicate| SummaryPredicate {
            field_index: predicate.index,
            allowed: predicate.allowed.as_slice(),
        })
        .collect()
}

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

fn ensure_schema_lock(dir: &Path, schema: &Schema) -> Result<()> {
    let path = dir.join("schema.lock");
    let canonical = schema.canonical();
    if path.exists() {
        let existing = fs::read_to_string(&path)?;
        if existing != canonical {
            return Err(Error::schema(
                "schema.lock does not match the supplied schema; this directory was created with a different schema",
            ));
        }
    } else {
        fs::write(&path, canonical)?;
    }
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

        let index = segment::index_path(&data, 1);
        fs::write(&index, b"EVIX").unwrap();
        let schema_text = parse_schema(SCHEMA_JSON).unwrap();
        assert!(schema_text.canonical().contains("timestamp"));

        let store = Store::open_with(&data, &schema, test_options(8)).unwrap();
        let rows = store.query(0, 10_000).unwrap();
        assert_eq!(rows.len(), 10);
        assert_eq!(row_value(&store, &rows[9])["user_id"], 9);
        assert!(segment::index_path(&data, 1).exists());
        store.close().unwrap();
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
    fn presence_summary_skips_blocks_and_keeps_matching_rows() {
        let dir = TempDir::new();
        let schema = write_schema(dir.path());
        let data = dir.path().join("data");
        let store = Store::open_with(&data, &schema, test_options(4)).unwrap();
        for block in 0..8 {
            let action = ["click", "view", "buy", "scroll"][block % 4];
            for row in 0..4 {
                let ts = (block * 4 + row) as i64;
                let user = if row == 0 {
                    None
                } else {
                    Some(block as i64 * 10 + row as i64)
                };
                store
                    .append_json(&event(ts, user, action, Some("note"), "1.50"))
                    .unwrap();
            }
        }
        store.flush().unwrap();
        assert_eq!(store.stats().blocks, 8);

        let all = store.query(0, 100).unwrap();
        assert_eq!(all.len(), 32);
        let decompressed_full = store.decompressed_blocks();
        assert_eq!(decompressed_full, 8);

        let clicks = store
            .query_with_filter(0, 100, &[Predicate::Eq("action".into(), "click".into())])
            .unwrap();
        assert_eq!(clicks.len(), 8);
        assert!(clicks
            .iter()
            .all(|row| row_value(&store, row)["action"] == "click"));
        let click_blocks = store.decompressed_blocks() - decompressed_full;
        assert!(
            click_blocks * 10 <= 8 * 3,
            "decompressed {click_blocks} of 8"
        );

        let outside = store
            .query_with_filter(0, 100, &[Predicate::Eq("user_id".into(), Scalar::Int(99))])
            .unwrap();
        assert!(outside.is_empty());
        assert_eq!(
            store.decompressed_blocks(),
            decompressed_full + click_blocks
        );

        let null_users = store
            .query_with_filter(0, 100, &[Predicate::Eq("user_id".into(), Scalar::Null)])
            .unwrap();
        assert_eq!(null_users.len(), 8);
        assert!(null_users
            .iter()
            .all(|row| row_value(&store, row)["user_id"].is_null()));

        let before_amount = store.decompressed_blocks();
        let amount = store
            .query_with_filter(
                0,
                100,
                &[Predicate::Eq(
                    "amount".into(),
                    crate::value::scalar_from_literal(
                        crate::schema::FieldType::Decimal { scale: 2 },
                        "9.99",
                    )
                    .unwrap(),
                )],
            )
            .unwrap();
        assert!(amount.is_empty());
        assert_eq!(store.decompressed_blocks(), before_amount);

        store.set_ignore_summaries(true);
        let before_ignore = store.decompressed_blocks();
        let ignored = store
            .query_with_filter(0, 100, &[Predicate::Eq("action".into(), "click".into())])
            .unwrap();
        assert_eq!(ignored.len(), clicks.len());
        assert_eq!(store.decompressed_blocks() - before_ignore, 8);
        store.close().unwrap();
    }

    #[test]
    fn version_1_segment_queries_without_rewriting_the_index() {
        let dir = TempDir::new();
        let schema = write_schema(dir.path());
        let data = dir.path().join("data");
        let store = Store::open_with(&data, &schema, test_options(1)).unwrap();
        store
            .append_json(&event(1, Some(1), "click", None, "1.00"))
            .unwrap();
        store
            .append_json(&event(2, Some(2), "view", None, "2.00"))
            .unwrap();
        store.flush().unwrap();
        let skipped = store
            .query_with_filter(0, 10, &[Predicate::Eq("user_id".into(), Scalar::Int(1))])
            .unwrap();
        assert_eq!(skipped.len(), 1);
        assert_eq!(store.decompressed_blocks(), 1);
        store.close().unwrap();

        let index = segment::index_path(&data, 1);
        let loaded = segment::read_index(&index).unwrap();
        assert_eq!(loaded.version, 2);
        assert!(loaded.blocks.iter().all(|block| !block.summary.is_empty()));
        segment::write_index(&index, 1, &loaded.blocks).unwrap();
        let v1 = fs::read(&index).unwrap();
        assert_eq!(v1[4], 1);
        assert_eq!(v1[5], 0);

        let store = Store::open_with(&data, &schema, test_options(1)).unwrap();
        let rows = store
            .query_with_filter(0, 10, &[Predicate::Eq("user_id".into(), Scalar::Int(1))])
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(row_value(&store, &rows[0])["action"], "click");
        assert_eq!(row_value(&store, &rows[0])["user_id"], 1);
        assert_eq!(store.decompressed_blocks(), 2);
        let missing = store
            .query_with_filter(0, 10, &[Predicate::Eq("action".into(), "buy".into())])
            .unwrap();
        assert!(missing.is_empty());
        store.close().unwrap();
        assert_eq!(fs::read(&index).unwrap(), v1);
    }

    #[test]
    fn bloom_false_positive_still_drops_non_matching_rows() {
        let dir = TempDir::new();
        let schema = write_schema(dir.path());
        let data = dir.path().join("data");
        let store = Store::open_with(&data, &schema, test_options(80)).unwrap();
        for index in 0..80 {
            store
                .append_json(&event(
                    index,
                    Some(index),
                    &format!("action-{index}"),
                    None,
                    "1.00",
                ))
                .unwrap();
        }
        store.flush().unwrap();
        assert_eq!(store.stats().blocks, 1);
        let catalog = segment::load_catalog(&data, store.schema()).unwrap();
        let summary = &catalog.segments[0].blocks[0].summary;
        let action_index = store
            .schema()
            .fields
            .iter()
            .position(|field| field.name == "action")
            .unwrap();
        let mut probe = None;
        for index in 0..20_000 {
            let text = format!("missing-{index}");
            if summary::bloom_may_contain(summary, action_index, &text) == Some(true) {
                probe = Some(text);
                break;
            }
        }
        let probe = probe.expect("bloom false positive");
        let rows = store
            .query_with_filter(0, 100, &[Predicate::Eq("action".into(), probe.into())])
            .unwrap();
        assert!(rows.is_empty());
        assert_eq!(store.decompressed_blocks(), 1);
        store.close().unwrap();
    }

    #[test]
    fn round_robin_actions_do_not_have_to_skip_blocks() {
        let dir = TempDir::new();
        let schema = write_schema(dir.path());
        let data = dir.path().join("data");
        let store = Store::open_with(&data, &schema, test_options(8)).unwrap();
        let actions = ["click", "view", "buy", "scroll"];
        for index in 0..16 {
            store
                .append_json(&event(
                    index,
                    Some(index),
                    actions[index as usize % 4],
                    None,
                    "1.00",
                ))
                .unwrap();
        }
        store.flush().unwrap();
        let clicks = store
            .query_with_filter(0, 100, &[Predicate::Eq("action".into(), "click".into())])
            .unwrap();
        assert_eq!(clicks.len(), 4);
        assert_eq!(store.decompressed_blocks(), store.stats().blocks);
        store.close().unwrap();
    }
}
