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
mod pipeline;
mod schema;
mod segment;
mod store;
mod value;

pub use error::{Error, Result};
pub use schema::{FieldType, Schema};
pub use store::{Predicate, Stats, Store, StoreOptions};
pub use value::{scalar_from_literal, Scalar};
