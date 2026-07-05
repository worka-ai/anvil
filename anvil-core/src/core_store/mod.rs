//! CoreStore is the single durable storage boundary for Anvil.
//!
//! The current implementation is deliberately introduced as a breaking internal
//! boundary: feature code should move to these primitives instead of writing its
//! own durable journal files. The local backend below is the first backend used
//! by tests and single-node development; distributed placement/repair will live
//! behind the same API.

mod encoding;
mod local;
mod types;

pub use encoding::*;
pub use local::CoreStore;
pub use types::*;
