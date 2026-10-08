//! Append-only JSON event store.
//!
//! Events are parsed against a typed schema, packed into columnar blocks, compressed
//! with zstd, and written by a single thread. A sparse index stores each block's
//! time range so queries skip blocks that cannot match.

mod c_api;
mod codec;
mod error;
mod json_scan;
mod pipeline;
mod schema;
mod segment;
mod store;
mod value;

pub use error::{Error, Result};
pub use schema::{parse_schema, FieldType, Schema};
pub use store::{Stats, Store, StoreOptions};

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
