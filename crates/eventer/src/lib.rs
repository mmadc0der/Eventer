//! Append-only JSON event store.
//!
//! Events are parsed against a typed schema, packed into columnar blocks, compressed
//! with zstd, and written by a single thread. A sparse index stores each block's
//! time range so queries skip blocks that cannot match. Version 2 stores the
//! first entry as absolute fields and each later entry as signed deltas of those
//! fields. That index file is a plain zstd frame when the frame is smaller than
//! the raw bytes, and an older raw index still opens. A zone map stores per-column
//! equality stats (exact sets or a bloom filter for strings, min/max for numbers)
//! so a filtered query can skip the payload as well. Equality predicates and
//! numeric ranges that still need the block are applied while it is decoded.
//! An integer, decimal, or float zone whose min/max cannot overlap the range is
//! not read. When those bounds sit inside every range and the null flag is
//! clear, [`Store::count_with_filter`](store::Store::count_with_filter) counts
//! the block from its timestamps and does not decode the proved column. A
//! float equality still reads a block whose range contains the value. JSON and
//! a float column stored as the old unknown tag stay unpruned.
//! [`Store::count_with_filter`](store::Store::count_with_filter) counts those
//! rows without building the JSON array. A block with no predicates reads
//! timestamps only. One string or text equality, with no other predicate and
//! no numeric range, counts a constant or one dictionary-code period from the
//! column header when every payload timestamp is inside the range. A null, a
//! full code vector, raw strings, or a timestamp outside the range still walks
//! rows. One bool equality, with no other predicate and no numeric range,
//! counts a constant or one bitmap period the same way, and does not expand a
//! bool per row. A null or a full per-row bitmap still walks. Each column is
//! its own zstd frame when that container is
//! at most a quarter larger than one frame, so a filter does not decompress
//! columns after the predicate.
//! [`Store::query_json_columns`](store::Store::query_json_columns) returns
//! the same rows with only the requested fields. A column that is neither
//! selected nor needed by a filter stays compressed.
//! [`Store::histogram_with_filter`](store::Store::histogram_with_filter) counts
//! the same rows into fixed epoch-aligned time buckets. A constant or positive
//! non-wrapping timestamp stride with no nulls, and with both endpoints inside
//! the range, fills those buckets from the column header.
//! [`Store::aggregate_with_filter`](store::Store::aggregate_with_filter) sums
//! and finds the minimum and maximum of numeric columns over that same filtered
//! window, without building the rows. One float sum with no minimum, maximum,
//! or row predicate adds a fully contained block from the encoded column.
//! One int, timestamp, or decimal `sum`, with no `min`, no `max`, and no row
//! predicate, adds a null-free constant, stride, or stride pieces from the
//! column body when every payload timestamp is inside the range. `min` and
//! `max` with no `sum` and no row predicate take int, decimal, and float
//! extremes from the zone when every payload timestamp in the block is inside
//! the range.
//! [`Store::aggregate_grouped`](store::Store::aggregate_grouped) splits that
//! result by one column.
//! [`Store::quantile_with_filter`](store::Store::quantile_with_filter) returns
//! one exact rank of a numeric column over that same window. Nulls are left
//! out of the sample. One int, timestamp, or decimal with no row predicate
//! selects that rank from a null-free constant, stride, or stride pieces whose
//! stride is -1, 0, or 1 and whose values do not wrap, when every payload
//! timestamp is inside the range. A null, any other encoding, a float, a cut
//! block, or a predicate still sorts the sample. The encoder and the on-disk
//! files are unchanged.
//! [`Store::latest_json`](store::Store::latest_json) returns the newest matching
//! row for each value of one column, in that same key order.
//! [`Store::distinct_count`](store::Store::distinct_count) counts distinct
//! non-null values of one column over that same filtered window.
//! [`Store::topk`](store::Store::topk) returns the values of one column that
//! occur most often in that window, including null, with the same exact counts.
//! [`Store::drop_blocks_before`](store::Store::drop_blocks_before) deletes blocks that
//! are entirely older than a caller-supplied cutoff and removes a segment file only
//! when every block in it is gone. A row older than the cutoff stays when it shares
//! a block with a row that is still inside the window.
//! [`Store::rewrite_directory`](store::Store::rewrite_directory) copies an existing
//! directory into a new one written with the current encoder and reports how the
//! stored data and index bytes changed. The source files stay as they were.

mod c_api;
mod codec;
mod error;
mod json_scan;
mod pipeline;
mod schema;
mod segment;
mod store;
mod value;
mod zone;

pub use error::{Error, Result};
pub use schema::{parse_schema, FieldType, Schema};
pub use store::{
    parse_quantile_p, CmpOp, ColumnStats, HistogramBucket, Predicate, QuantileP, RewriteReport,
    StatGroup, StatRequest, StatSum, Stats, Store, StoreOptions, StoredBytes, TopValue,
    MAX_DISTINCT_VALUES, MAX_HISTOGRAM_BUCKETS,
};
pub use value::format_decimal;
pub use value::{scalar_from_literal, Scalar};

/// Uncompressed size of each column for one block, including its null bitmap.
pub fn uncompressed_column_sizes(
    schema: &Schema,
    json_events: &[impl AsRef<[u8]>],
) -> Result<Vec<(String, usize)>> {
    let mut rows = Vec::with_capacity(json_events.len());
    for event in json_events {
        rows.push(value::parse_event(schema, event.as_ref())?);
    }
    codec::uncompressed_column_sizes(schema, &rows)
}
