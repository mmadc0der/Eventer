# Eventer

Append-only store for JSON events. A typed schema fixes the columns. Rows are packed into columnar blocks, compressed with zstd, and written to segment files. A sparse index records each block's minimum and maximum timestamp so a time-range read can skip blocks that cannot match. A zone map next to that index records equality stats for each column, so a filtered read can skip a block whose payload cannot contain the value.

The library is both an rlib and a cdylib (`libeventer.so` / `eventer.dll`) with a small C API. `eventer-server` links the rlib and serves two routes.

## Layout

| Path | Role |
| --- | --- |
| `crates/eventer` | Store library (`rlib` + `cdylib`) |
| `crates/eventer-server` | HTTP server |
| `crates/eventer-bench` | Write benchmark |
| `examples/schema.json` | Sample schema |
| `include/eventer.h` | C header |
| `examples/demo.c` | Small C caller |

## Schema

Events are JSON objects. The schema names the timestamp field and the column types. Field order is the on-disk column order. Unknown JSON fields are ignored. Missing values become null, except the timestamp, which is required.

Types: `int` (i64), `float` (f64), `bool`, `string` (dictionary-encoded when that is smaller), `text` (dictionary-encoded when that is smaller), `decimal` (JSON string, stored as i128 with a schema scale), `timestamp` (unix milliseconds, or an RFC3339 string such as `2024-01-02T03:04:05.123Z`), `json` (any JSON value other than null). A `json` column returns the nested value as JSON, so one field can hold an object on one row, an array on the next, and a scalar after that. The stored form keeps the original field text from ingest (including key order, whitespace inside the value, and duplicate object keys). Dictionary encoding is used only when repeated values in a block make it smaller. JSON null and a missing field are both column nulls; a repeated top-level field follows last-wins semantics like `serde_json`.

```json
{
  "timestamp_field": "ts",
  "fields": [
    {"name": "ts", "type": "timestamp"},
    {"name": "user_id", "type": "int"},
    {"name": "score", "type": "float"},
    {"name": "ok", "type": "bool"},
    {"name": "action", "type": "string"},
    {"name": "note", "type": "text"},
    {"name": "amount", "type": "decimal", "scale": 2},
    {"name": "props", "type": "json"}
  ]
}
```

The first open writes `schema.lock` into the data directory. A later open may append fields at the end of that locked list. The lock file is rewritten to the new canonical schema, and rows stored before the append read null for the new columns. Old segment files stay as they were written. A different timestamp field, a type or decimal-scale change, a removed field, a renamed field, or a reordered field is rejected.

## Build

The repo pins Rust 1.99 in `rust-toolchain.toml`. With rustup installed, `cargo` downloads that toolchain on its own.

```bash
cargo test --workspace
cargo build -p eventer --release
cargo build -p eventer-server --release
```

## Run the server

```bash
mkdir -p data
cargo run -p eventer-server -- --data ./data --schema examples/schema.json --bind 127.0.0.1:43123
```

`POST /events` takes one JSON object and returns after that event is fsynced.

```bash
curl -sS -X POST http://127.0.0.1:43123/events \
  -H 'content-type: application/json' \
  --data '{"ts":1700000000000,"user_id":7,"score":1.5,"ok":true,"action":"click","note":"demo","amount":"19.99","props":{"plan":"pro","flags":["a",1]}}'
```

`GET /events?from=&to=` returns a JSON array. `from` and `to` are inclusive unix milliseconds. Repeat `eq=field=value` to keep rows where that column equals the value. Filters are AND-ed. Blocks the zone map can reject are not read; the rest are filtered while they are decoded.

```bash
curl -sS 'http://127.0.0.1:43123/events?from=1700000000000&to=1700000000000'
curl -sS --get 'http://127.0.0.1:43123/events' \
  --data-urlencode 'from=1700000000000' \
  --data-urlencode 'to=1700000000000' \
  --data-urlencode 'eq=action=click'
```

`GET /health` returns `{"status":"ok"}`. The server listens on localhost and has no authentication. Point it at a directory used by only one process.

## C API

```bash
cargo build -p eventer --release
gcc -O2 -o demo examples/demo.c -I include -L target/release -leventer
LD_LIBRARY_PATH=target/release ./demo ./data examples/schema.json
```

| Function | Behavior |
| --- | --- |
| `eventer_open` | Open or create a data directory |
| `eventer_append` | Copy one JSON event into the pipeline |
| `eventer_flush` | Write and fsync queued events |
| `eventer_query` | Inclusive time range as a JSON array |
| `eventer_query_filtered` | Time range plus one exact string or text column match |
| `eventer_last_error` | Last error string for this handle |
| `eventer_close` | Flush and free the handle |

Return codes: `0` ok, `-1` bad argument, `-2` I/O, `-3` event or schema, `-4` query buffer too small (`*out_len` is the required size), `-5` closed. `eventer_append` returns after the event is queued. `eventer_query` and `eventer_close` flush first.

## How a write moves

1. Parser threads turn JSON into typed rows.
2. One encoder packs rows into a columnar block (default 2048 rows, or sooner on flush).
3. Compress threads run zstd (default level 3).
4. One writer thread batches blocks, appends them to the current segment, then appends sparse-index entries. It fsyncs on flush, close, and durable appends.

Segments rotate after 64 MiB (`seg-000001.dat` plus `seg-000001.idx`). The sparse index stores each block's min/max timestamp. New block frames do not repeat those two fields. A crash can leave a torn tail; the next open scans complete blocks, truncates the tear, and rebuilds the index if it disagrees. Rebuilding a frame that has no timestamps decodes the payload and rewrites only the index. Older 36-byte `EVBK` and `EVBD` headers that still contain the timestamps still scan and decode.

Inside a block, integers, timestamps, and decimals use a constant, a constant stride, up to eight constant-stride pieces, an exact-width bit packing of the delta from the minimum, or a byte-width frame of reference, whichever is smaller. Kinds already written by older blocks still decode. Bools are bit-packed. Repeated strings use a dictionary when that encoding is smaller. JSON columns use that same string encoding on the preserved field text. Nulls are a bitmap.

## Benchmark

`eventer-bench` generates events, times `append` plus one final `flush`, then checks that a full-range query returns every row. Generation happens before the timer. The single flush is the only fsync, so the rate is pipeline throughput for a batched load, not the latency of one durable HTTP post.

```bash
cargo run --release -p eventer-bench -- 100000
```

One host, three release runs of 100,000 events, zstd level 3, 2048-row blocks:

| | |
| --- | --- |
| Raw JSON | 102.44 bytes/event |
| Stored segment data | 3.94 bytes/event |
| Sparse index | 1,968 bytes total |
| Size ratio | 26.0× smaller than JSON |
| Throughput | 969k–1.03M events/sec |

Stored size was identical across the runs (394,005 data bytes, 49 blocks, one segment). Elapsed time was 0.097–0.103 seconds.

## Rust API

`Store::append_json` queues an event. `Store::flush` and `Store::query` make queued events durable and visible. `Store::append_json_durable` waits until that event's block is fsynced; the HTTP `POST` uses it. `Store::query(from_ms, to_ms)` returns matching [`Row`] values in ingest order; use [`RowSerializable`] or [`row_to_json_bytes`] when you need the original JSON lexemes (including duplicate keys). `Store::query_json` returns the same data as one JSON array for HTTP.

`Store::query_with_filter` adds equality predicates. Several predicates are AND-ed. `Predicate::Eq("type".into(), "assistant".into())` keeps one value. `Predicate::In` keeps any listed value. `Scalar::Null` matches null. The zone map skips a block that cannot contain those values. A block that is read, and whose filter column is a constant or dictionary that does not contain the value, is not fully decoded, so later text columns stay unread. The C equivalent is `eventer_query_filtered` for one string or text column.

`Store::drop_blocks_before(cutoff_ms)` deletes blocks whose maximum timestamp is strictly less than `cutoff_ms`. The cutoff is an argument, not the newest timestamp stored, so one future event cannot expire the table. A block is kept or dropped as a whole: a row older than the cutoff stays when a later row in the same block is still inside the window. A segment file (`.dat`, `.idx`, `.zon`, and `.dict`) is removed only when every block in it is eligible. A mixed segment is rewritten by copying the surviving compressed frames unchanged and publishing that file with `rename`, so a crash leaves either the old segment or the new one. The segment dictionary stays when any surviving frame was compressed with it. After the call, a query no longer returns a row from a block whose maximum timestamp is below the cutoff, including after the directory is opened again. Rows that shared a kept block stay.
