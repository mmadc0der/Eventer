# Eventer

Append-only store for JSON events. A typed schema fixes the columns. Rows are packed into columnar blocks, compressed with zstd, and written to segment files. A sparse index records each block's minimum and maximum timestamp so a time-range read can skip blocks that cannot match. A zone map next to that index records equality stats for the other columns, so a filtered read can skip a block whose payload cannot contain the value. The timestamp column is the sparse-index range, so a new zone file stores that column as a one-byte tag.

The library is both an rlib and a cdylib (`libeventer.so` / `eventer.dll`) with a small C API. `eventer-server` links the rlib and serves the HTTP routes.

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

`POST /events` takes one JSON object, or a JSON array of objects, and returns after that write is fsynced. An object responds with `{"ok":true}`. An array is validated in full before any row is queued, then stored with one fsync, and responds with `{"ok":true,"count":N}`. An empty array, or an array containing a value that is not an object or does not match the schema, is rejected and leaves the store unchanged.

```bash
curl -sS -X POST http://127.0.0.1:43123/events \
  -H 'content-type: application/json' \
  --data '{"ts":1700000000000,"user_id":7,"score":1.5,"ok":true,"action":"click","note":"demo","amount":"19.99","props":{"plan":"pro","flags":["a",1]}}'
```

`GET /events?from=&to=` returns a JSON array. `from` and `to` are inclusive unix milliseconds. Repeat `eq=field=value` to keep rows where that column equals the value. Repeat `gt`, `ge`, `lt`, and `le` as `field=value` to keep rows on one side of a numeric threshold. `gt` and `lt` are exclusive. `ge` and `le` are inclusive. An int bound is a base-10 integer in the `i64` range. A decimal bound uses the same text as `eq` for that column. A float bound is a finite number. A bool, string, text, or json column, a missing field, a missing value, or a value the column cannot parse is status `400` and does not read the store. Nulls do not match a range. Every filter is AND-ed. Blocks the zone map can reject are not read; an integer, decimal, or float zone whose maximum is below a lower bound, or whose minimum is above an upper bound, is one of those, including the exclusive cases. A float equality still reads a block whose range contains the value. JSON is not a skip. The rest are filtered while they are decoded. `limit` is a positive integer: the response is the first that many matching rows, in ingest order. `offset` is a non-negative integer and skips that many matching rows before `limit` is applied. `offset` without `limit` returns the rest, and an offset past the last match returns `[]`. Omitting both returns every match. `limit=0`, a negative limit, a negative offset, or a non-integer is status `400`.

Repeat `select=field` to keep only those fields. The objects contain the selected fields in schema order, and naming a field twice selects it once. A selected null is JSON `null`. A field that was not selected is absent. Selecting every column matches the keys of the response without `select`. An empty name, an unknown field, or a parameter other than `from`, `to`, `limit`, `offset`, `eq`, `gt`, `ge`, `lt`, `le`, and `select` is status `400` and does not read the store. Filters still apply, including a filter on a column that is not selected. A block the sparse index or the zone map can reject is not read. A column that is neither selected nor needed by a filter is not decompressed. An older single-frame block still decodes, and the omitted values are dropped after decode. `select` on `GET /events/count`, `GET /events/histogram`, `GET /events/stats`, and `POST /events/drop` is status `400`.

`GET /events/count?from=&to=` returns `{"count":N}` for that same range and the same `eq` and range filters. `N` is the number of rows the unpaged `GET /events` response would contain. `limit` and `offset` are rejected. A block the zone map can reject is not read. A block with no predicates counts payload timestamps and does not decode the other columns. Columns after the last predicate are not decompressed.

`GET /events/stats?from=&to=` returns `{"columns":{"<field>":{...}}}` for that same range and the same `eq`, `gt`, `ge`, `lt`, and `le` filters. Repeat `sum=field`, `min=field`, and `max=field`. At least one is required, and the same field may be named by more than one of them. Each column object contains only the stats that were requested. `min` and `max` use the same JSON form as that column in `GET /events`. `sum` of an int, a decimal, or a timestamp is a JSON string of the exact total. `sum` of a float is a JSON number. Nulls do not contribute. When every matching value is null, or the filter matches no rows, `min` and `max` are JSON `null` and `sum` is `"0"` for an int, decimal, or timestamp, or `0` for a float. An int, decimal, or timestamp sum that does not fit in 128 bits is status `400` and returns no partial object. A float sum is IEEE-754 addition in ingest order. `limit`, `offset`, and `bucket_ms` are rejected. A missing list, an empty field name, an unknown field, or a bool, string, text, or json column is status `400` and does not read the store. A block the sparse index places outside the range is not read. A block the zone map can reject is not read. `min` and `max` are the min and max of the rows that survive the filters. `by=field` returns `{"groups":[{"key":...,"columns":{...}}]}` instead, one group per value of that single column. A second `by`, an empty name, an unknown field, a float, or a json column is status `400` and does not read the store. `by` may name a column that is also a `sum`, `min`, or `max` target. `key` uses the same JSON form as that column in `GET /events`, and a null key is JSON `null`. Groups are ordered by the key: null first, then numeric order for int, timestamp, and decimal, `false` then `true` for bool, and byte order for string and text. More than 4096 groups is status `400` and the body is not a partial group list. Nulls still do not contribute to the aggregates, and a null key is still a group. A filter that matches no rows returns `{"groups":[]}`. Keys come from the rows that survive the filters.

`GET /events/histogram?from=&to=&bucket_ms=N` returns `{"buckets":[{"start_ms":S,"count":C}, ...]}` for that same range and the same `eq` and range filters. `N` is a positive `i64`. Each bucket starts at `floor(ts / N) * N` using integer division that rounds toward negative infinity, so the starts are aligned to the unix epoch. The list is every bucket that intersects the inclusive range, in increasing `S`, including a bucket whose count is `0`. A row is counted in the bucket that contains its own timestamp, and the sum of the counts is the matching `GET /events/count`. A request that would emit more than 4096 buckets is status `400` and does not scan the store. A missing `bucket_ms`, zero, a negative, a non-integer, `limit`, or `offset` is the same `400`. A block the sparse index places outside the range is not read. A block the zone map can reject is not read.

```bash
curl -sS 'http://127.0.0.1:43123/events?from=1700000000000&to=1700000000000'
curl -sS --get 'http://127.0.0.1:43123/events' \
  --data-urlencode 'from=1700000000000' \
  --data-urlencode 'to=1700000000000' \
  --data-urlencode 'eq=action=click'
```

`POST /events/drop` deletes blocks whose maximum timestamp is strictly less than a caller-supplied cutoff. The body is `{"before_ms": N}` where `N` is a signed integer in the `i64` range. The response is `{"ok":true}`. `N` is not taken from the newest stored timestamp, so one future event cannot expire the table. A cutoff equal to a block's maximum keeps that block. A row older than `N` stays when a later row in the same block is still inside the window. A segment's `.dat`, `.idx`, `.zon`, and `.dict` files are removed only when every block in that segment is eligible. A missing `before_ms`, a non-integer, a JSON array, or a value that is not an object is status `400` and leaves the store unchanged.

```bash
curl -sS -X POST http://127.0.0.1:43123/events/drop \
  -H 'content-type: application/json' \
  --data '{"before_ms":1700000000000}'
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

Each column can be compressed as its own frame. That container is kept when it is at most a quarter larger than one frame for the whole block, and a count or a filter then decompresses frames only through the predicate column. A query that returns rows still reads every column of a block the zone map could not reject. Older blocks are one frame and still decode.

## Benchmark

`eventer-bench` generates events, times `append` plus one final `flush`, then checks that a full-range query returns every row. Generation happens before the timer. The single flush is the only fsync, so the rate is pipeline throughput for a batched load, not the latency of one durable HTTP post.

```bash
cargo run --release -p eventer-bench -- 100000
```

The same 100,000 events, zstd level 3, and 2048-row blocks are what [CHANGELOG.md](CHANGELOG.md) compares across format versions. On `96e1ceb` that run stores 3,639 data bytes and 674 index bytes (49 blocks, one segment): 2,815× smaller than the 10,244,000 bytes of raw JSON. The first columnar store, on the same events, stored 394,005 data bytes and 1,968 index bytes (26×). Throughput depends on the host and is not part of that table.

## Rust API

`Store::append_json` queues an event. `Store::flush` and `Store::query` make queued events durable and visible. `Store::append_json_durable` waits until that event's block is fsynced. `Store::append_json_batch_durable` parses every event before queueing any of them, then fsyncs the batch once. A one-object HTTP `POST` uses the single-event path. An array `POST` uses the batch path. `Store::query(from_ms, to_ms)` returns matching [`Row`] values in ingest order; use [`RowSerializable`] or [`row_to_json_bytes`] when you need the original JSON lexemes (including duplicate keys). `Store::query_json` returns the same data as one JSON array for HTTP.

`Store::query_with_filter` adds equality predicates and numeric ranges. Several predicates are AND-ed. `Predicate::Eq("type".into(), "assistant".into())` keeps one value. `Predicate::In` keeps any listed value. `Scalar::Null` matches null. `Predicate::Cmp("user_id".into(), CmpOp::Ge, Scalar::Int(5))` keeps ints greater than or equal to 5. `CmpOp::Gt` and `CmpOp::Lt` are exclusive. Ranges apply to int, timestamp, decimal, and float columns. Nulls do not match a range. The zone map skips a block that cannot contain those values, including an integer, decimal, or float block whose min/max miss the range. A float equality still reads a block whose range contains the value. JSON is not a skip. A block that is read, and whose filter column is a constant or dictionary that does not contain the value, is not fully decoded, so later text columns stay unread. The C equivalent is `eventer_query_filtered` for one string or text column. `Store::query_window` and `Store::query_json_window` apply the same filters, then skip `offset` matches and stop after `limit` rows. The HTTP `limit` and `offset` parameters use that path. `Store::query_json_columns` is that JSON array with a column list: selected fields only, in schema order. A sliced block does not decompress a column that is neither selected nor required by a filter. The C query functions are unchanged.

`Store::count` and `Store::count_with_filter` return how many rows the matching unpaged query would. They do not build a JSON array. A block with no predicates counts timestamps only. `Store::histogram_with_filter` buckets those same rows. `Store::histogram` and `Store::histogram_with_filter` split that same count into epoch-aligned buckets of `bucket_ms` milliseconds. More than 4096 buckets is an error and does not read a block. `Store::aggregate_with_filter` returns `sum`, `min`, and `max` for int, timestamp, decimal, and float columns over that same window. Nulls do not contribute. An exact sum uses a 128-bit accumulator. `Store::aggregate_grouped` splits that result by one bool, string, text, int, timestamp, or decimal column, at most 4096 groups. `GET /events/stats` is those calls. The C API is unchanged.

`Store::rewrite_directory` reads every row of an existing data directory in ingest order and writes them with the current encoder into a destination that is not already a finished store. The source files are not modified. The report gives `stored_data_bytes` and `stored_index_bytes` for both directories, the same totals as `Store::stats`, and `(source − destination) / source` for data bytes and for data plus index. A destination left unfinished by a crash stays unreadable and a later call replaces it. There is no HTTP route and no C API change.

`Store::drop_blocks_before(cutoff_ms)` deletes blocks whose maximum timestamp is strictly less than `cutoff_ms`. The cutoff is an argument, not the newest timestamp stored, so one future event cannot expire the table. A block is kept or dropped as a whole: a row older than the cutoff stays when a later row in the same block is still inside the window. A segment file (`.dat`, `.idx`, `.zon`, and `.dict`) is removed only when every block in it is eligible. A mixed segment is rewritten by copying the surviving compressed frames unchanged and publishing that file with `rename`, so a crash leaves either the old segment or the new one. The segment dictionary stays when any surviving frame was compressed with it. After the call, a query no longer returns a row from a block whose maximum timestamp is below the cutoff, including after the directory is opened again. Rows that shared a kept block stay. `POST /events/drop` is that call with `before_ms` as the cutoff.
