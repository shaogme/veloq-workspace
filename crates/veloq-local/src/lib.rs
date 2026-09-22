#![no_std]

pub mod broadcast;
pub mod common;
pub mod condvar;
pub mod mpmc;
pub mod mpsc;
pub mod mutex;
pub mod notify;
pub mod oneshot;
pub mod rwlock;
pub mod semaphore;
pub mod set_once;
pub mod spsc;
pub(crate) mod wait_queue;
pub(crate) mod waker;
pub mod watch;

pub use condvar::{Condvar, Wait};
pub use mutex::{Mutex, MutexGuard, MutexLockFuture};
pub use notify::{Notified, Notify};
pub use rwlock::{RwLock, RwLockReadFuture, RwLockReadGuard, RwLockWriteFuture, RwLockWriteGuard};
pub use semaphore::{
    AcquireError, Semaphore, SemaphoreAcquireFuture, SemaphorePermit, TryAcquireError,
};
pub use set_once::{SetOnce, SetOnceError, SetOnceWait};
