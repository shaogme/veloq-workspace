mod barrier;
mod condvar;
mod mutex;
mod once;
mod once_lock;
mod poison;
mod reentrant_mutex;
mod rwlock;
mod spin_lock;
pub(crate) mod sys;
mod unpoisoned_mutex;
mod unpoisoned_rwlock;

pub mod atomic;
pub mod mpsc;

pub use barrier::{Barrier, BarrierWaitResult};
pub use condvar::{Condvar, UnpoisonedCondvar, WaitTimeoutResult};
pub use mutex::raw::RawMutex;
pub use mutex::{Mutex, MutexGuard};
pub use once::{NativeOnce, Once, OnceState};
pub use once_lock::{NativeOnceLock, OnceLock};
pub use poison::{LockResult, PoisonError, TryLockError, TryLockResult};
pub use reentrant_mutex::{ReentrantMutex, ReentrantMutexGuard};
pub use rwlock::raw::RawRwLock;
pub use rwlock::{RwLock, RwLockReadGuard, RwLockWriteGuard};
pub use spin_lock::{SpinLock, SpinLockGuard};
pub use unpoisoned_mutex::{UnpoisonedMutex, UnpoisonedMutexGuard};
pub use unpoisoned_rwlock::{
    UnpoisonedRwLock, UnpoisonedRwLockReadGuard, UnpoisonedRwLockWriteGuard,
};

#[cfg(not(feature = "loom"))]
pub use alloc_crate::sync::Arc;
#[cfg(not(feature = "loom"))]
pub use mutex::const_mutex;
#[cfg(not(feature = "loom"))]
pub use reentrant_mutex::const_reentrant_mutex;
#[cfg(not(feature = "loom"))]
pub use rwlock::const_rwlock;
#[cfg(not(feature = "loom"))]
pub use unpoisoned_mutex::const_unpoisoned_mutex;
#[cfg(not(feature = "loom"))]
pub use unpoisoned_rwlock::const_unpoisoned_rwlock;

#[cfg(feature = "loom")]
pub use loom::sync::Arc;
#[cfg(feature = "loom")]
pub use once::LoomOnce;
#[cfg(feature = "loom")]
pub use once_lock::LoomOnceLock;
