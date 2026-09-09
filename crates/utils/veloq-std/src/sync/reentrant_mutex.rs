use core::{fmt, marker::PhantomData, ops::Deref};

use crate::{
    cell::UnsafeCell,
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        mutex::raw::RawMutex,
    },
    thread,
    time::{Duration, Instant},
};

pub struct ReentrantMutex<T: ?Sized> {
    raw: RawMutex,
    owner: AtomicU64,
    count: AtomicUsize,
    data: UnsafeCell<T>,
}

unsafe impl<T: ?Sized + Send> Send for ReentrantMutex<T> {}
unsafe impl<T: ?Sized + Send> Sync for ReentrantMutex<T> {}

impl<T> ReentrantMutex<T> {
    #[cfg(not(feature = "loom"))]
    pub const fn new(val: T) -> Self {
        Self {
            raw: RawMutex::new(),
            owner: AtomicU64::new(0),
            count: AtomicUsize::new(0),
            data: UnsafeCell::new(val),
        }
    }

    #[cfg(feature = "loom")]
    pub fn new(val: T) -> Self {
        Self {
            raw: RawMutex::new(),
            owner: AtomicU64::new(0),
            count: AtomicUsize::new(0),
            data: UnsafeCell::new(val),
        }
    }

    #[inline]
    pub fn into_inner(self) -> T {
        self.data.into_inner()
    }
}

#[cfg(not(feature = "loom"))]
pub const fn const_reentrant_mutex<T>(val: T) -> ReentrantMutex<T> {
    ReentrantMutex::new(val)
}

impl<T: ?Sized> ReentrantMutex<T> {
    pub fn lock(&self) -> ReentrantMutexGuard<'_, T> {
        let current_thread = thread::current_id().as_u64();
        if self.owner.load(Ordering::Relaxed) == current_thread {
            let cur = self.count.load(Ordering::Relaxed);
            self.count.store(
                cur.checked_add(1).expect("reentrant lock count overflow"),
                Ordering::Relaxed,
            );
        } else {
            self.raw.lock();
            self.owner.store(current_thread, Ordering::Relaxed);
            self.count.store(1, Ordering::Relaxed);
        }
        ReentrantMutexGuard {
            lock: self,
            _marker: PhantomData,
        }
    }

    pub fn try_lock(&self) -> Option<ReentrantMutexGuard<'_, T>> {
        let current_thread = thread::current_id().as_u64();
        if self.owner.load(Ordering::Relaxed) == current_thread {
            let cur = self.count.load(Ordering::Relaxed);
            self.count.store(
                cur.checked_add(1).expect("reentrant lock count overflow"),
                Ordering::Relaxed,
            );
            Some(ReentrantMutexGuard {
                lock: self,
                _marker: PhantomData,
            })
        } else if self.raw.try_lock() {
            self.owner.store(current_thread, Ordering::Relaxed);
            self.count.store(1, Ordering::Relaxed);
            Some(ReentrantMutexGuard {
                lock: self,
                _marker: PhantomData,
            })
        } else {
            None
        }
    }

    pub fn try_lock_for(&self, timeout: Duration) -> Option<ReentrantMutexGuard<'_, T>> {
        let current_thread = thread::current_id().as_u64();
        if self.owner.load(Ordering::Relaxed) == current_thread {
            let cur = self.count.load(Ordering::Relaxed);
            self.count.store(
                cur.checked_add(1).expect("reentrant lock count overflow"),
                Ordering::Relaxed,
            );
            Some(ReentrantMutexGuard {
                lock: self,
                _marker: PhantomData,
            })
        } else if self.raw.try_lock_for(timeout) {
            self.owner.store(current_thread, Ordering::Relaxed);
            self.count.store(1, Ordering::Relaxed);
            Some(ReentrantMutexGuard {
                lock: self,
                _marker: PhantomData,
            })
        } else {
            None
        }
    }

    pub fn try_lock_until(&self, timeout: Instant) -> Option<ReentrantMutexGuard<'_, T>> {
        let current_thread = thread::current_id().as_u64();
        if self.owner.load(Ordering::Relaxed) == current_thread {
            let cur = self.count.load(Ordering::Relaxed);
            self.count.store(
                cur.checked_add(1).expect("reentrant lock count overflow"),
                Ordering::Relaxed,
            );
            Some(ReentrantMutexGuard {
                lock: self,
                _marker: PhantomData,
            })
        } else if self.raw.try_lock_until(timeout) {
            self.owner.store(current_thread, Ordering::Relaxed);
            self.count.store(1, Ordering::Relaxed);
            Some(ReentrantMutexGuard {
                lock: self,
                _marker: PhantomData,
            })
        } else {
            None
        }
    }

    #[inline]
    pub fn is_locked(&self) -> bool {
        self.owner.load(Ordering::Relaxed) != 0
    }

    #[inline]
    pub fn is_owned_by_current_thread(&self) -> bool {
        self.owner.load(Ordering::Relaxed) == thread::current_id().as_u64()
    }

    #[inline]
    pub fn get_mut(&mut self) -> &mut T {
        unsafe { &mut *self.data.with_mut(|p| p as *mut T) }
    }

    #[inline]
    pub fn reentrancy_count(&self) -> usize {
        if self.is_owned_by_current_thread() {
            self.count.load(Ordering::Relaxed)
        } else {
            0
        }
    }

    #[inline]
    pub fn raw(&self) -> &RawMutex {
        &self.raw
    }
}

pub struct ReentrantMutexGuard<'a, T: ?Sized> {
    lock: &'a ReentrantMutex<T>,
    _marker: PhantomData<*const ()>,
}

unsafe impl<T: ?Sized + Sync> Sync for ReentrantMutexGuard<'_, T> {}

impl<'a, T: ?Sized> ReentrantMutexGuard<'a, T> {
    #[inline]
    pub fn mutex(guard: &Self) -> &'a ReentrantMutex<T> {
        guard.lock
    }
}

impl<T: ?Sized> Deref for ReentrantMutexGuard<'_, T> {
    type Target = T;

    #[inline]
    fn deref(&self) -> &Self::Target {
        unsafe { &*self.lock.data.with(|p| p as *const T) }
    }
}

impl<T: ?Sized> Drop for ReentrantMutexGuard<'_, T> {
    #[inline]
    fn drop(&mut self) {
        let cur = self.lock.count.load(Ordering::Relaxed);
        if cur > 1 {
            self.lock.count.store(cur - 1, Ordering::Relaxed);
        } else {
            self.lock.count.store(0, Ordering::Relaxed);
            self.lock.owner.store(0, Ordering::Relaxed);
            unsafe {
                self.lock.raw.unlock();
            }
        }
    }
}

impl<T: Default> Default for ReentrantMutex<T> {
    #[inline]
    fn default() -> Self {
        Self::new(T::default())
    }
}

impl<T> From<T> for ReentrantMutex<T> {
    #[inline]
    fn from(val: T) -> Self {
        Self::new(val)
    }
}

impl<T: ?Sized + fmt::Debug> fmt::Debug for ReentrantMutex<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut d = f.debug_struct("ReentrantMutex");
        if let Some(guard) = self.try_lock() {
            d.field("data", &&*guard);
        } else {
            d.field("data", &"<locked>");
        }
        d.finish_non_exhaustive()
    }
}

impl<T: ?Sized + fmt::Debug> fmt::Debug for ReentrantMutexGuard<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&**self, f)
    }
}

impl<T: ?Sized + fmt::Display> fmt::Display for ReentrantMutexGuard<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&**self, f)
    }
}
