mod barrier;
mod condvar;
mod lazy_lock;
pub mod mutex;
mod once;
mod once_lock;
mod poison;
mod reentrant_mutex;
pub mod rwlock;
mod spin_lock;
pub(crate) mod sys;
pub mod unpoisoned_mutex;
pub mod unpoisoned_rwlock;

pub mod atomic;
pub mod mpsc;

pub use barrier::{BarrierWaitResult, NativeBarrier, const_native_barrier};
pub use condvar::{
    NativeCondvar, NativeUnpoisonedCondvar, WaitTimeoutResult, const_native_condvar,
    const_native_unpoisoned_condvar,
};
pub use lazy_lock::{LazyLock, NativeLazyLock};
pub use once::{NativeOnce, Once, OnceState};
pub use once_lock::{NativeOnceLock, OnceLock};
pub use poison::{LockResult, PoisonError, TryLockError, TryLockResult};
pub use reentrant_mutex::{
    NativeReentrantMutex, NativeReentrantMutexGuard, const_native_reentrant_mutex,
};
pub use spin_lock::{NativeSpinLock, NativeSpinLockGuard, const_native_spin_lock};

#[cfg(not(feature = "loom"))]
pub use alloc_crate::sync::Arc;

pub use mutex::raw::NativeRawMutex;
pub use mutex::{NativeMutex, NativeMutexGuard, const_native_mutex};

pub use rwlock::raw::NativeRawRwLock;
pub use rwlock::{
    NativeRwLock, NativeRwLockReadGuard, NativeRwLockWriteGuard, const_native_rwlock,
};

pub use unpoisoned_mutex::const_native_unpoisoned_mutex;
pub use unpoisoned_mutex::{NativeUnpoisonedMutex, NativeUnpoisonedMutexGuard};

pub use unpoisoned_rwlock::{
    NativeUnpoisonedRwLock, NativeUnpoisonedRwLockReadGuard, NativeUnpoisonedRwLockWriteGuard,
    const_native_unpoisoned_rwlock,
};

#[cfg(not(feature = "loom"))]
pub use mutex::{Mutex, MutexGuard, raw::RawMutex};

#[cfg(not(feature = "loom"))]
pub use rwlock::{RwLock, RwLockReadGuard, RwLockWriteGuard, raw::RawRwLock};

#[cfg(not(feature = "loom"))]
pub use unpoisoned_mutex::{UnpoisonedMutex, UnpoisonedMutexGuard};

#[cfg(not(feature = "loom"))]
pub use unpoisoned_rwlock::{
    UnpoisonedRwLock, UnpoisonedRwLockReadGuard, UnpoisonedRwLockWriteGuard,
};

#[cfg(not(feature = "loom"))]
pub use barrier::{Barrier, const_barrier};

#[cfg(not(feature = "loom"))]
pub use condvar::{Condvar, UnpoisonedCondvar, const_condvar, const_unpoisoned_condvar};

#[cfg(not(feature = "loom"))]
pub use reentrant_mutex::{ReentrantMutex, ReentrantMutexGuard, const_reentrant_mutex};

#[cfg(not(feature = "loom"))]
pub use spin_lock::{SpinLock, SpinLockGuard, const_spin_lock};

#[cfg(not(feature = "loom"))]
pub use mutex::const_mutex;

#[cfg(not(feature = "loom"))]
pub use rwlock::const_rwlock;

#[cfg(not(feature = "loom"))]
pub use unpoisoned_mutex::const_unpoisoned_mutex;

#[cfg(not(feature = "loom"))]
pub use unpoisoned_rwlock::const_unpoisoned_rwlock;

#[cfg(feature = "loom")]
pub use loom::sync::Arc;

#[cfg(feature = "loom")]
pub use mutex::{LoomMutex, LoomMutexGuard, raw::LoomRawMutex};

#[cfg(feature = "loom")]
pub use mutex::{Mutex, MutexGuard, raw::RawMutex};

#[cfg(feature = "loom")]
pub use rwlock::{LoomRwLock, LoomRwLockReadGuard, LoomRwLockWriteGuard, raw::LoomRawRwLock};

#[cfg(feature = "loom")]
pub use rwlock::{RwLock, RwLockReadGuard, RwLockWriteGuard, raw::RawRwLock};

#[cfg(feature = "loom")]
pub use unpoisoned_mutex::{LoomUnpoisonedMutex, LoomUnpoisonedMutexGuard};

#[cfg(feature = "loom")]
pub use unpoisoned_mutex::{UnpoisonedMutex, UnpoisonedMutexGuard};

#[cfg(feature = "loom")]
pub use unpoisoned_rwlock::{
    LoomUnpoisonedRwLock, LoomUnpoisonedRwLockReadGuard, LoomUnpoisonedRwLockWriteGuard,
};

#[cfg(feature = "loom")]
pub use unpoisoned_rwlock::{
    UnpoisonedRwLock, UnpoisonedRwLockReadGuard, UnpoisonedRwLockWriteGuard,
};

#[cfg(feature = "loom")]
pub use lazy_lock::LoomLazyLock;

#[cfg(feature = "loom")]
pub use once::LoomOnce;

#[cfg(feature = "loom")]
pub use once_lock::LoomOnceLock;

#[cfg(feature = "loom")]
pub use barrier::{Barrier, LoomBarrier};

#[cfg(feature = "loom")]
pub use condvar::{Condvar, LoomCondvar, LoomUnpoisonedCondvar, UnpoisonedCondvar};

#[cfg(feature = "loom")]
pub use reentrant_mutex::{
    LoomReentrantMutex, LoomReentrantMutexGuard, ReentrantMutex, ReentrantMutexGuard,
};

#[cfg(feature = "loom")]
pub use spin_lock::{LoomSpinLock, LoomSpinLockGuard, SpinLock, SpinLockGuard};
