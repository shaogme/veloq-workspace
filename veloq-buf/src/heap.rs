//! Global Slot Pool Management
//!
//! This module provides the `GlobalSlotPool`, which manages the entire system memory
//! using a Buddy Allocator over a set of 4KB Slots.
//!
//! # Scalability Update
//! To avoid global lock contention, the pool is partitioned into multiple **Shards**.
//! Each shard manages a distinct slice of the global memory.
//!
//! # Dynamic Extension (Phase 1)
//! The pool now supports multiple `Chunk`s. Startups with one chunk, but can grow.

pub mod buddy;
mod cache;
mod pool;
mod units;

pub use pool::*;
pub use units::*;
