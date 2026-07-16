//! Executable ANVIL-0007 CoreStore protocol model.
//!
//! This crate is deliberately independent from the Anvil server/runtime code. It
//! models roots, generations, CoreMeta quorum evidence, explicit single-root
//! transactions, and stale owner fences as a small Stateright state machine.

mod model;

pub use model::*;

#[cfg(test)]
mod tests;
