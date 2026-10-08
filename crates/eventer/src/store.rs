use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use serde_json::Value;

use crate::codec::{decode_rows_in_range, ColumnPredicate};
use crate::error::{Error, Result};
use crate::pipeline::{self, Pipeline, PipelineConfig};
use crate::schema::{self, Schema};
use crate::segment::{self, Catalog};
use crate::value::{self, row_to_json, Scalar};

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
        ensure_schema_lock(&dir, &schema)?;
        let catalog = Arc::new(std::sync::Mutex::new(segment::load_catalog(&dir)?));
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

    /// Force a partial block out and fsync it.
    pub fn flush(&self) -> Result<()> {
        self.pipeline.flush()
    }

    /// Schema this store was opened with.
    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    /// Inclusive range on the schema timestamp, in unix milliseconds. Results are in ingest order.
    pub fn query(&self, from_ms: i64, to_ms: i64) -> Result<Vec<Value>> {
        self.query_with_filter(from_ms, to_ms, &[])
    }

    /// Inclusive time range plus equality predicates.
    ///
    /// Each predicate is applied while the block is decoded. If a filter column's
    /// constant or dictionary cannot contain the requested value, the rest of that
    /// block is not decoded. Unknown fields and values of the wrong column type
    /// return [`Error::Schema`].
    pub fn query_with_filter(
        &self,
        from_ms: i64,
        to_ms: i64,
        predicates: &[Predicate],
    ) -> Result<Vec<Value>> {
        if from_ms > to_ms {
            return Ok(Vec::new());
        }
        let resolved = resolve_predicates(&self.schema, predicates)?;
        if resolved.iter().any(|pred| pred.allowed.is_empty()) {
            return Ok(Vec::new());
        }
        self.flush()?;
        let blocks = {
            let catalog = self.catalog();
            catalog
                .segments
                .iter()
                .flat_map(|segment| segment.blocks.iter().cloned())
                .filter(|block| block.max_ts >= from_ms && block.min_ts <= to_ms)
                .collect::<Vec<_>>()
        };
        let mut values = Vec::new();
        for block in blocks {
            let payload = segment::read_block_payload(
                &segment::data_path(&self.dir, block.segment_id),
                &block,
            )?;
            let rows = decode_rows_in_range(&self.schema, &payload, from_ms, to_ms, &resolved)?;
            for row in rows {
                values.push(row_to_json(&self.schema, &row)?);
            }
        }
        Ok(values)
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
        let values = self.query_with_filter(from_ms, to_ms, predicates)?;
        Ok(serde_json::to_vec(&values)?)
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
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::thread;
    use std::time::{SystemTime, UNIX_EPOCH};

    static TEMP_SEQ: AtomicU64 = AtomicU64::new(0);

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
        assert_eq!(middle[0]["ts"], 2000);
        assert_eq!(middle[1]["amount"], "0.05");
        assert!(middle[1]["user_id"].is_null());
        assert!(middle[1].get("note").unwrap().as_str().unwrap() == "b");

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
        assert_eq!(rows[9]["user_id"], 9);
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
        assert_eq!(clicks[0]["ts"], 1000);
        assert_eq!(clicks[1]["ts"], 4000);
        assert!(clicks[1]["note"].is_null());

        let narrowed = store
            .query_with_filter(
                1500,
                4000,
                &[Predicate::Eq("action".into(), "click".into())],
            )
            .unwrap();
        assert_eq!(narrowed.len(), 1);
        assert_eq!(narrowed[0]["user_id"], 4);

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
        assert_eq!(either[0]["action"], "view");
        assert_eq!(either[1]["action"], "buy");

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
        assert_eq!(both[0]["ts"], 3000);

        let null_user = store
            .query_with_filter(0, 10_000, &[Predicate::Eq("user_id".into(), Scalar::Null)])
            .unwrap();
        assert_eq!(null_user.len(), 1);
        assert_eq!(null_user[0]["action"], "buy");

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
}
