mod arc;
pub mod atomic;
mod barrier;
mod condvar;
mod lazy_lock;
pub mod mpsc;
mod mutex;
mod once;
mod once_lock;
mod poison;
mod reentrant_mutex;
mod rwlock;
mod spin_lock;
mod unpoisoned_mutex;
mod unpoisoned_rwlock;
mod weak;

mod sys;

pub(crate) use sys::Wait;

pub use barrier::{BarrierWaitResult, NativeBarrier};
pub use condvar::{NativeCondvar, NativeUnpoisonedCondvar, WaitTimeoutResult};
pub use lazy_lock::{LazyLock, NativeLazyLock};
pub use once::{NativeOnce, Once, OnceState};
pub use once_lock::{NativeOnceLock, OnceLock};
pub use poison::{LockResult, PoisonError, TryLockError, TryLockResult};
pub use reentrant_mutex::{NativeReentrantMutex, NativeReentrantMutexGuard};
pub use spin_lock::{NativeSpinLock, NativeSpinLockGuard};

pub use arc::NativeArc;
pub use weak::NativeWeak;

pub use mutex::{NativeMutex, NativeMutexGuard, NativeRawMutex};

pub use rwlock::{NativeRawRwLock, NativeRwLock, NativeRwLockReadGuard, NativeRwLockWriteGuard};

pub use unpoisoned_mutex::{NativeUnpoisonedMutex, NativeUnpoisonedMutexGuard};

pub use unpoisoned_rwlock::{
    NativeUnpoisonedRwLock, NativeUnpoisonedRwLockReadGuard, NativeUnpoisonedRwLockWriteGuard,
};

#[cfg(not(feature = "loom"))]
pub use mutex::{Mutex, MutexGuard, RawMutex};

#[cfg(not(feature = "loom"))]
pub use rwlock::{RawRwLock, RwLock, RwLockReadGuard, RwLockWriteGuard};

#[cfg(not(feature = "loom"))]
pub use unpoisoned_mutex::{UnpoisonedMutex, UnpoisonedMutexGuard};

#[cfg(not(feature = "loom"))]
pub use unpoisoned_rwlock::{
    UnpoisonedRwLock, UnpoisonedRwLockReadGuard, UnpoisonedRwLockWriteGuard,
};

#[cfg(not(feature = "loom"))]
pub use barrier::Barrier;

#[cfg(not(feature = "loom"))]
pub use condvar::{Condvar, UnpoisonedCondvar};

#[cfg(not(feature = "loom"))]
pub use reentrant_mutex::{ReentrantMutex, ReentrantMutexGuard};

#[cfg(not(feature = "loom"))]
pub use spin_lock::{SpinLock, SpinLockGuard};

#[cfg(not(feature = "loom"))]
pub use arc::Arc;

#[cfg(not(feature = "loom"))]
pub use weak::Weak;

#[cfg(feature = "loom")]
pub use arc::{Arc, LoomArc};

#[cfg(feature = "loom")]
pub use weak::{LoomWeak, Weak};

#[cfg(feature = "loom")]
pub use mutex::{LoomMutex, LoomMutexGuard, LoomRawMutex};

#[cfg(feature = "loom")]
pub use mutex::{Mutex, MutexGuard, RawMutex};

#[cfg(feature = "loom")]
pub use rwlock::{LoomRawRwLock, LoomRwLock, LoomRwLockReadGuard, LoomRwLockWriteGuard};

#[cfg(feature = "loom")]
pub use rwlock::{RawRwLock, RwLock, RwLockReadGuard, RwLockWriteGuard};

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
