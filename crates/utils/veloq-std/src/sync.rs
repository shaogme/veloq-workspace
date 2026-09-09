mod condvar;
mod mutex;
mod once;
mod once_lock;
mod reentrant_mutex;
mod rwlock;
mod spin_lock;
pub(crate) mod sys;

pub mod atomic;
pub mod mpsc;

pub use condvar::{Condvar, WaitTimeoutResult};
pub use mutex::{Mutex, MutexGuard, RawMutex};
pub use once::{Once, OnceState};
pub use once_lock::OnceLock;
pub use reentrant_mutex::{ReentrantMutex, ReentrantMutexGuard};
pub use rwlock::{RawRwLock, RwLock, RwLockReadGuard, RwLockWriteGuard};
pub use spin_lock::{SpinLock, SpinLockGuard};

#[cfg(not(feature = "loom"))]
pub use alloc_crate::sync::Arc;
#[cfg(not(feature = "loom"))]
pub use mutex::const_mutex;
#[cfg(not(feature = "loom"))]
pub use reentrant_mutex::const_reentrant_mutex;
#[cfg(not(feature = "loom"))]
pub use rwlock::const_rwlock;

#[cfg(feature = "loom")]
pub use loom::sync::Arc;
