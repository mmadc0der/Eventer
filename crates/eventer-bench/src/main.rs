use std::env;
use std::fs;
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::{Duration, Instant};

use eventer::{Predicate, Store, StoreOptions};

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
    let clustered = env::var("EVENTER_BENCH_CLUSTERED").ok().as_deref() == Some("1");
    let ignore_summary = env::var("EVENTER_BENCH_IGNORE_SUMMARY").ok().as_deref() == Some("1");
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
        let action = if clustered {
            actions[(i / 2048) % actions.len()]
        } else {
            actions[i % actions.len()]
        };
        let mut value = serde_json::json!({
            "ts": ts,
            "user_id": (i % 1_000) as i64,
            "score": (i % 100) as f64 / 10.0,
            "ok": i % 2 == 0,
            "action": action,
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
    let from = 1_700_000_000_000;
    let to = 1_700_000_000_000 + count as i64 * 10;
    // Time the action predicate before the full scan so decompression is not
    // already warm from reading every block.
    store.set_ignore_summaries(ignore_summary);
    let before_filter = store.decompressed_blocks();
    let filter_started = Instant::now();
    let filtered = match store.query_with_filter(
        from,
        to,
        &[Predicate::Eq("action".into(), "click".into())],
    ) {
        Ok(rows) => rows.len(),
        Err(err) => {
            eprintln!("filtered query failed: {err}");
            return ExitCode::from(1);
        }
    };
    let filter_elapsed = filter_started.elapsed();
    let filter_decompressed = store.decompressed_blocks() - before_filter;
    store.set_ignore_summaries(false);
    let queried = match store.query(from, to) {
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
    if clustered {
        let expected = clustered_click_rows(count);
        if filtered != expected {
            eprintln!("clustered query returned {filtered} rows, expected {expected}");
            return ExitCode::from(1);
        }
    } else if filtered != count.div_ceil(4) {
        eprintln!(
            "filtered query returned {filtered} rows, expected {}",
            count.div_ceil(4)
        );
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
    println!("query_elapsed_sec: {:.6}", filter_elapsed.as_secs_f64());
    println!("decompressed_blocks: {filter_decompressed}");
    println!("time_selected_blocks: {}", stats.blocks);
    println!("clustered: {clustered}");
    println!("ignore_summary: {ignore_summary}");
    let _ = store.close();
    ExitCode::SUCCESS
}

fn clustered_click_rows(count: usize) -> usize {
    let full = count / 2048;
    let rem = count % 2048;
    let mut rows = 0;
    for block in 0..full {
        if block % 4 == 0 {
            rows += 2048;
        }
    }
    if rem > 0 && full % 4 == 0 {
        rows += rem;
    }
    rows
}
