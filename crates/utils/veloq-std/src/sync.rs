mod mutex;
mod once;
mod once_lock;
mod raw_mutex;
mod raw_rwlock;
mod rwlock;

pub mod atomic;

pub use mutex::{Mutex, MutexGuard, const_mutex};
pub use once::{Once, OnceState};
pub use once_lock::OnceLock;
pub use raw_mutex::RawMutex;
pub use raw_rwlock::RawRwLock;
pub use rwlock::{RwLock, RwLockReadGuard, RwLockWriteGuard, const_rwlock};

#[cfg(not(feature = "loom"))]
pub use alloc::sync::Arc;

#[cfg(feature = "loom")]
pub use loom::sync::Arc;
