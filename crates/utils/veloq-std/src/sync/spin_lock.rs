#[cfg(not(feature = "loom"))]
use crate::cell::UnsafeCell;

#[cfg(not(feature = "loom"))]
use crate::sync::atomic::{AtomicBool, Ordering};

#[cfg(not(feature = "loom"))]
use crossbeam_utils::Backoff;

#[cfg(feature = "loom")]
use loom::sync::{Mutex, MutexGuard};

#[cfg(not(feature = "loom"))]
pub struct SpinLock<T> {
    locked: AtomicBool,
    data: UnsafeCell<T>,
}

#[cfg(not(feature = "loom"))]
unsafe impl<T: Send> Send for SpinLock<T> {}

#[cfg(not(feature = "loom"))]
unsafe impl<T: Send> Sync for SpinLock<T> {}

#[cfg(not(feature = "loom"))]
impl<T> SpinLock<T> {
    pub const fn new(data: T) -> Self {
        Self {
            locked: AtomicBool::new(false),
            data: UnsafeCell::new(data),
        }
    }

    pub fn lock(&self) -> SpinLockGuard<'_, T> {
        let backoff = Backoff::new();
        while self.locked.swap(true, Ordering::Acquire) {
            backoff.snooze();
        }
        SpinLockGuard { lock: self }
    }
}

#[cfg(not(feature = "loom"))]
pub struct SpinLockGuard<'a, T> {
    lock: &'a SpinLock<T>,
}

#[cfg(not(feature = "loom"))]
impl<'a, T> Drop for SpinLockGuard<'a, T> {
    fn drop(&mut self) {
        self.lock.locked.store(false, Ordering::Release);
    }
}

#[cfg(not(feature = "loom"))]
impl<'a, T> SpinLockGuard<'a, T> {
    pub fn with<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&T) -> R,
    {
        unsafe { self.lock.data.with(f) }
    }

    pub fn with_mut<F, R>(&mut self, f: F) -> R
    where
        F: FnOnce(&mut T) -> R,
    {
        unsafe { self.lock.data.with_mut(f) }
    }
}

// --- Loom Friendly SpinLock Replacement ---
#[cfg(feature = "loom")]
pub struct SpinLock<T> {
    inner: Mutex<T>,
}

#[cfg(feature = "loom")]
impl<T> SpinLock<T> {
    pub fn new(data: T) -> Self {
        Self {
            inner: Mutex::new(data),
        }
    }

    pub fn lock(&self) -> SpinLockGuard<'_, T> {
        SpinLockGuard {
            inner: self.inner.lock().unwrap(),
        }
    }
}

#[cfg(feature = "loom")]
pub struct SpinLockGuard<'a, T> {
    inner: MutexGuard<'a, T>,
}

#[cfg(feature = "loom")]
impl<'a, T> SpinLockGuard<'a, T> {
    pub fn with<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&T) -> R,
    {
        f(&*self.inner)
    }

    pub fn with_mut<F, R>(&mut self, f: F) -> R
    where
        F: FnOnce(&mut T) -> R,
    {
        f(&mut *self.inner)
    }
}
