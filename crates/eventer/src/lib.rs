//! Append-only JSON event store.
//!
//! Events are parsed against a typed schema, packed into columnar blocks, compressed
//! with zstd, and written by a single thread. A sparse index stores each block's
//! time range so queries skip blocks that cannot match. Equality predicates are
//! applied while a block is decoded, so a column that cannot contain the requested
//! value stops the rest of that block from being materialized.

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
pub use store::{Predicate, Stats, Store, StoreOptions};
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

/// Uncompressed size of one float column before dictionary encoding, then the size
/// that is stored. Both include the null bitmap.
pub fn float_column_uncompressed_bytes(
    schema: &Schema,
    json_events: &[impl AsRef<[u8]>],
    field: &str,
) -> Result<(usize, usize)> {
    let index = schema
        .fields
        .iter()
        .position(|candidate| candidate.name == field)
        .ok_or_else(|| Error::event("float column is missing"))?;
    let mut rows = Vec::with_capacity(json_events.len());
    for event in json_events {
        rows.push(value::parse_event(schema, event.as_ref())?);
    }
    codec::float_column_uncompressed_sizes(schema, &rows, index)
}
