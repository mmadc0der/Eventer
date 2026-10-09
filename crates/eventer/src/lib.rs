//! Append-only JSON event store.
//!
//! Events are parsed against a typed schema, packed into columnar blocks, compressed
//! with zstd, and written by a single thread. A sparse index stores each block's
//! time range so queries skip blocks that cannot match. A zone map stores per-column
//! equality stats (exact sets or a bloom filter for strings, min/max for numbers)
//! so a filtered query can skip the payload as well. Equality predicates that still
//! need the block are applied while it is decoded.
//! [`Store::drop_blocks_before`](store::Store::drop_blocks_before) deletes blocks that
//! are entirely older than a caller-supplied cutoff and removes a segment file only
//! when every block in it is gone. A row older than the cutoff stays when it shares
//! a block with a row that is still inside the window.

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
