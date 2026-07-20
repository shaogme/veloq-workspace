mod condvar;
mod mutex;
mod once;
mod once_lock;
#[cfg(not(feature = "loom"))]
mod raw_mutex;
#[cfg(not(feature = "loom"))]
mod raw_rwlock;
mod rwlock;
pub(crate) mod sys;

pub mod atomic;
pub mod mpsc;
mod spin_lock;

pub use condvar::{Condvar, WaitTimeoutResult};
#[cfg(not(feature = "loom"))]
pub use mutex::const_mutex;
pub use mutex::{Mutex, MutexGuard};
pub use once::{Once, OnceState};
pub use once_lock::OnceLock;
#[cfg(not(feature = "loom"))]
pub use raw_mutex::RawMutex;
#[cfg(not(feature = "loom"))]
pub use raw_rwlock::RawRwLock;
#[cfg(not(feature = "loom"))]
pub use rwlock::const_rwlock;
pub use rwlock::{RwLock, RwLockReadGuard, RwLockWriteGuard};
pub use spin_lock::{SpinLock, SpinLockGuard};

#[cfg(not(feature = "loom"))]
pub use alloc_crate::sync::Arc;

#[cfg(feature = "loom")]
pub use loom::sync::Arc;
