# Changelog

Relative size of the stock benchmark across shipped formats. Every row is one `cargo run --release -p eventer-bench -- 100000` on that commit. The generator, schema, block size (2048), and zstd level (3) are the same, so the event bytes are the same: 100,000 events, 10,244,000 bytes of raw JSON, 49 blocks, one segment. Stored size does not move between runs. Throughput does, and it is not compared here.

`stored_data_bytes` is the segment data file plus the dictionary sidecar. `stored_index_bytes` is the sparse index plus, from the zone-map version on, the zone file. "Smaller" is `(previous − this) / previous`. A negative index change is growth.

The first store is the baseline for the last column. Current `main` (`305a4d5`) keeps 3,639 data bytes against that baseline of 394,005 (108× fewer data bytes, 99.1% smaller) and 4,524 bytes of data plus index against 395,973 (87.5× fewer, 98.9% smaller).

| Commit | What changed | Data | vs previous data | Index | vs previous index | Data + index | vs previous total | Data vs first store |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `2b75769` | Initial columnar store | 394005 | — | 1968 | — | 395973 | — | — |
| `4712da5` | JSON columns | 394005 | 0% | 1968 | 0% | 395973 | 0% | 0% |
| `ffcd202` | Integer stride and exact bit width | 143012 | 63.7% smaller | 1968 | 0% | 144980 | 63.4% smaller | 63.7% smaller |
| `5ba9f9f` | One zstd dictionary per segment | 112295 | 21.5% smaller | 1968 | 0% | 114263 | 21.2% smaller | 71.5% smaller |
| `ae3079a` | Dictionary encoding for text | 87103 | 22.4% smaller | 1968 | 0% | 89071 | 22.0% smaller | 77.9% smaller |
| `05b392f` | Wrapped integers as a few strides | 17200 | 80.3% smaller | 1968 | 0% | 19168 | 78.5% smaller | 95.6% smaller |
| `55ee9ef` | Dictionary sample framed with the segment dictionary | 13665 | 20.6% smaller | 1968 | 0% | 15633 | 18.4% smaller | 96.5% smaller |
| `7e38dc6` | Zone maps (index total now includes the zone file) | 13665 | 0% | 9628 | 389% larger | 23293 | 49.0% larger | 96.5% smaller |
| `e9138c8` | Repeated decimal stride for floats | 10425 | 23.7% smaller | 9628 | 0% | 20053 | 13.9% smaller | 97.4% smaller |
| `3cca663` | One period of string dictionary codes | 9124 | 12.5% smaller | 9628 | 0% | 18752 | 6.5% smaller | 97.7% smaller |
| `ccce182` | Dictionary sidecar compressed when smaller | 5596 | 38.7% smaller | 9628 | 0% | 15224 | 18.8% smaller | 98.6% smaller |
| `4f3422c` | Zone file compressed when smaller | 5596 | 0% | 2839 | 70.5% smaller | 8435 | 44.6% smaller | 98.6% smaller |
| `e891646` | Sparse index compressed when smaller | 5596 | 0% | 1400 | 50.7% smaller | 6996 | 17.1% smaller | 98.6% smaller |
| `21dcc2a` | Block timestamps removed from new frames | 4812 | 14.0% smaller | 1401 | 1 byte larger | 6213 | 11.2% smaller | 98.8% smaller |
| `9ee4ed3` | Magicless zstd payloads | 4371 | 9.2% smaller | 1399 | 2 bytes smaller | 5770 | 7.1% smaller | 98.9% smaller |
| `8b49771` | 12-byte block headers | 3979 | 9.0% smaller | 1399 | 0% | 5378 | 6.8% smaller | 99.0% smaller |
| `11d9f09` | One period of null and bool bitmaps | 3639 | 8.5% smaller | 1388 | 0.8% smaller | 5027 | 6.5% smaller | 99.1% smaller |
| `10ae7fc` | One checksum for the zone file | 3639 | 0% | 1091 | 21.4% smaller | 4730 | 5.9% smaller | 99.1% smaller |
| `3147df3` | Delta-coded sparse index | 3639 | 0% | 885 | 18.9% smaller | 4524 | 4.4% smaller | 99.1% smaller |
| `305a4d5` | Time-bucket counts | 3639 | 0% | 885 | 0% | 4524 | 0% | 99.1% smaller |

JSON columns did not change this workload: the bench schema has no `json` field. Equality filters, schema append, retention, batch ingest, `limit`/`offset`, and `GET /events/count` sit between these commits and do not change the encoder. The two endpoints that were measured again, HTTP drop (`d194464`) and time-bucket counts (`305a4d5`), match the format commit they sit on.

Zone maps make the index total larger because the zone file did not exist before. The sparse index underneath was still 1,968 bytes until the later index compression.

A directory written by an older commit still opens. Nothing in this table rewrites those files. The bytes above are what a new write of the same events produces on that version.
