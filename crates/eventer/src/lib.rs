//! Append-only JSON event store.
//!
//! Events are parsed against a typed schema, packed into columnar blocks, compressed
//! with zstd, and written by a single thread. A sparse index stores each block's
//! time range so queries skip blocks that cannot match. Version-2 entries also
//! store a per-column presence summary, so an equality predicate can skip the
//! block before it is decompressed. Equality predicates that survive that check
//! are applied while the block is decoded.

mod c_api;
mod codec;
mod error;
mod json_scan;
mod pipeline;
mod schema;
mod segment;
mod store;
mod summary;
mod value;

pub use error::{Error, Result};
pub use schema::{FieldType, Schema};
pub use store::{Predicate, Stats, Store, StoreOptions};
pub use value::{scalar_from_literal, Scalar};
