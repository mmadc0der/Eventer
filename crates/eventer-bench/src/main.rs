use std::env;
use std::fs;
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::{Duration, Instant};

use eventer::{Store, StoreOptions};

const SCHEMA: &str = r#"{
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

fn main() -> ExitCode {
    let count: usize = env::args()
        .nth(1)
        .and_then(|value| value.parse().ok())
        .unwrap_or(100_000);
    let root = env::args()
        .nth(2)
        .map(PathBuf::from)
        .unwrap_or_else(|| env::temp_dir().join("eventer-bench"));
    if root.exists() {
        fs::remove_dir_all(&root).expect("clear bench directory");
    }
    fs::create_dir_all(&root).expect("create bench directory");
    let schema_path = root.join("schema.json");
    fs::write(&schema_path, SCHEMA).expect("write schema");

    let options = StoreOptions {
        block_rows: 2048,
        zstd_level: 3,
        linger: Duration::from_secs(30),
        ..StoreOptions::default()
    };
    let store = match Store::open_with(root.join("data"), &schema_path, options) {
        Ok(store) => store,
        Err(err) => {
            eprintln!("open failed: {err}");
            return ExitCode::from(1);
        }
    };

    let actions = ["click", "view", "buy", "scroll"];
    let notes = ["landing", "checkout", "search", ""];
    let mut events = Vec::with_capacity(count);
    let mut raw_bytes = 0usize;
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
        let bytes = serde_json::to_vec(&value).expect("event json");
        raw_bytes += bytes.len();
        events.push(bytes);
    }

    let started = Instant::now();
    for event in &events {
        if let Err(err) = store.append_json(event) {
            eprintln!("append failed: {err}");
            return ExitCode::from(1);
        }
    }
    if let Err(err) = store.flush() {
        eprintln!("flush failed: {err}");
        return ExitCode::from(1);
    }
    let elapsed = started.elapsed();

    let stats = store.stats();
    let queried = match store.query(1_700_000_000_000, 1_700_000_000_000 + count as i64 * 10) {
        Ok(rows) => rows.len(),
        Err(err) => {
            eprintln!("query failed: {err}");
            return ExitCode::from(1);
        }
    };
    if queried != count {
        eprintln!("query returned {queried} rows, expected {count}");
        return ExitCode::from(1);
    }

    let seconds = elapsed.as_secs_f64();
    let raw_per = raw_bytes as f64 / count as f64;
    let stored_per = stats.data_bytes as f64 / count as f64;
    let ratio = raw_per / stored_per;
    println!("events: {count}");
    println!("raw_bytes: {raw_bytes}");
    println!("raw_bytes_per_event: {raw_per:.2}");
    println!("stored_data_bytes: {}", stats.data_bytes);
    println!("stored_index_bytes: {}", stats.index_bytes);
    println!("stored_bytes_per_event: {stored_per:.2}");
    println!("ratio_raw_over_stored: {ratio:.2}");
    println!("blocks: {}", stats.blocks);
    println!("segments: {}", stats.segments);
    println!("elapsed_sec: {seconds:.4}");
    println!("events_per_sec: {:.0}", count as f64 / seconds);
    let schema = eventer::parse_schema(SCHEMA).expect("bench schema");
    let sample_end = count.min(2048);
    match eventer::uncompressed_column_sizes(&schema, &events[..sample_end]) {
        Ok(sizes) => {
            for (name, size) in sizes {
                if matches!(name.as_str(), "ts" | "user_id" | "amount" | "score") {
                    println!("uncompressed_column_bytes {name}: {size}");
                }
            }
            // Kind 2 is one null flag, the kind byte, and 8 bytes per present
            // score. The bench writes a score on every row.
            println!("uncompressed_score_kind2_bytes: {}", 1 + 1 + sample_end * 8);
        }
        Err(err) => {
            eprintln!("column size measurement failed: {err}");
            return ExitCode::from(1);
        }
    }
    let _ = store.close();
    ExitCode::SUCCESS
}
