//! Global Slot Pool Management
//!
//! This module provides the `GlobalSlotPool`, which manages the entire system memory
//! using a Buddy Allocator over a set of 4KB Slots.
//!
//! # Scalability Update
//! To avoid global lock contention, the pool is partitioned into multiple **Shards**.
//! Each shard manages a distinct slice of the global memory.
//!
//! # Fixed Startup Capacity / 启动时固定容量
//! Runtime pools create their chunks during initialization and do not grow on exhaustion.
//! 运行时 pool 在初始化期间创建全部 chunk，耗尽后不会扩容。

pub mod buddy;
mod cache;
mod pool;
mod units;

pub use pool::*;
pub use units::*;
