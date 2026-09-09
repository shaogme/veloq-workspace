pub(crate) mod raw;

use core::{
    fmt,
    ops::{Deref, DerefMut},
};

use crate::{
    cell::UnsafeCell,
    time::{Duration, Instant},
};

pub use self::raw::RawRwLock;

pub struct RwLock<T: ?Sized> {
    raw: RawRwLock,
    data: UnsafeCell<T>,
}

unsafe impl<T: ?Sized + Send> Send for RwLock<T> {}
unsafe impl<T: ?Sized + Send + Sync> Sync for RwLock<T> {}

impl<T> RwLock<T> {
    #[cfg(not(feature = "loom"))]
    pub const fn new(val: T) -> Self {
        Self {
            raw: RawRwLock::new(),
            data: UnsafeCell::new(val),
        }
    }

    #[cfg(feature = "loom")]
    pub fn new(val: T) -> Self {
        Self {
            raw: RawRwLock::new(),
            data: UnsafeCell::new(val),
        }
    }

    #[inline]
    pub fn into_inner(self) -> T {
        self.data.into_inner()
    }
}

#[cfg(not(feature = "loom"))]
pub const fn const_rwlock<T>(val: T) -> RwLock<T> {
    RwLock::new(val)
}

impl<T: ?Sized> RwLock<T> {
    #[inline]
    pub fn read(&self) -> RwLockReadGuard<'_, T> {
        self.raw.lock_shared();
        RwLockReadGuard { rwlock: self }
    }

    #[inline]
    pub fn try_read(&self) -> Option<RwLockReadGuard<'_, T>> {
        if self.raw.try_lock_shared() {
            Some(RwLockReadGuard { rwlock: self })
        } else {
            None
        }
    }

    #[inline]
    pub fn try_read_for(&self, timeout: Duration) -> Option<RwLockReadGuard<'_, T>> {
        if self.raw.try_lock_shared_for(timeout) {
            Some(RwLockReadGuard { rwlock: self })
        } else {
            None
        }
    }

    #[inline]
    pub fn try_read_until(&self, timeout: Instant) -> Option<RwLockReadGuard<'_, T>> {
        if self.raw.try_lock_shared_until(timeout) {
            Some(RwLockReadGuard { rwlock: self })
        } else {
            None
        }
    }

    #[inline]
    pub fn write(&self) -> RwLockWriteGuard<'_, T> {
        self.raw.lock_exclusive();
        RwLockWriteGuard { rwlock: self }
    }

    #[inline]
    pub fn try_write(&self) -> Option<RwLockWriteGuard<'_, T>> {
        if self.raw.try_lock_exclusive() {
            Some(RwLockWriteGuard { rwlock: self })
        } else {
            None
        }
    }

    #[inline]
    pub fn try_write_for(&self, timeout: Duration) -> Option<RwLockWriteGuard<'_, T>> {
        if self.raw.try_lock_exclusive_for(timeout) {
            Some(RwLockWriteGuard { rwlock: self })
        } else {
            None
        }
    }

    #[inline]
    pub fn try_write_until(&self, timeout: Instant) -> Option<RwLockWriteGuard<'_, T>> {
        if self.raw.try_lock_exclusive_until(timeout) {
            Some(RwLockWriteGuard { rwlock: self })
        } else {
            None
        }
    }

    #[inline]
    pub fn is_locked(&self) -> bool {
        self.raw.is_locked()
    }

    #[inline]
    pub fn is_locked_exclusive(&self) -> bool {
        self.raw.is_locked_exclusive()
    }

    #[inline]
    pub fn get_mut(&mut self) -> &mut T {
        unsafe { &mut *self.data.with_mut(|p| p as *mut T) }
    }

    #[inline]
    pub fn raw(&self) -> &RawRwLock {
        &self.raw
    }
}

impl<T: Default> Default for RwLock<T> {
    #[inline]
    fn default() -> Self {
        Self::new(T::default())
    }
}

impl<T> From<T> for RwLock<T> {
    #[inline]
    fn from(val: T) -> Self {
        Self::new(val)
    }
}

impl<T: ?Sized + fmt::Debug> fmt::Debug for RwLock<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut d = f.debug_struct("RwLock");
        if let Some(guard) = self.try_read() {
            d.field("data", &&*guard);
        } else {
            d.field("data", &"<locked>");
        }
        d.finish_non_exhaustive()
    }
}

pub struct RwLockReadGuard<'a, T: ?Sized> {
    rwlock: &'a RwLock<T>,
}

unsafe impl<T: ?Sized + Sync> Sync for RwLockReadGuard<'_, T> {}

impl<'a, T: ?Sized> RwLockReadGuard<'a, T> {
    #[inline]
    pub fn rwlock(guard: &Self) -> &'a RwLock<T> {
        guard.rwlock
    }
}

impl<T: ?Sized> Deref for RwLockReadGuard<'_, T> {
    type Target = T;

    #[inline]
    fn deref(&self) -> &Self::Target {
        unsafe { &*self.rwlock.data.with(|p| p as *const T) }
    }
}

impl<T: ?Sized> Drop for RwLockReadGuard<'_, T> {
    #[inline]
    fn drop(&mut self) {
        unsafe { self.rwlock.raw.unlock_shared() };
    }
}

pub struct RwLockWriteGuard<'a, T: ?Sized> {
    rwlock: &'a RwLock<T>,
}

unsafe impl<T: ?Sized + Sync> Sync for RwLockWriteGuard<'_, T> {}

impl<'a, T: ?Sized> RwLockWriteGuard<'a, T> {
    #[inline]
    pub fn rwlock(guard: &Self) -> &'a RwLock<T> {
        guard.rwlock
    }

    #[inline]
    pub fn downgrade(guard: Self) -> RwLockReadGuard<'a, T> {
        let rwlock = guard.rwlock;
        core::mem::forget(guard);
        unsafe { rwlock.raw.downgrade() };
        RwLockReadGuard { rwlock }
    }
}

impl<T: ?Sized> Deref for RwLockWriteGuard<'_, T> {
    type Target = T;

    #[inline]
    fn deref(&self) -> &Self::Target {
        unsafe { &*self.rwlock.data.with(|p| p as *const T) }
    }
}

impl<T: ?Sized> DerefMut for RwLockWriteGuard<'_, T> {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { &mut *self.rwlock.data.with_mut(|p| p as *mut T) }
    }
}

impl<T: ?Sized> Drop for RwLockWriteGuard<'_, T> {
    #[inline]
    fn drop(&mut self) {
        unsafe { self.rwlock.raw.unlock_exclusive() };
    }
}

impl<T: ?Sized + fmt::Debug> fmt::Debug for RwLockReadGuard<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&**self, f)
    }
}

impl<T: ?Sized + fmt::Display> fmt::Display for RwLockReadGuard<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&**self, f)
    }
}

impl<T: ?Sized + fmt::Debug> fmt::Debug for RwLockWriteGuard<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&**self, f)
    }
}

impl<T: ?Sized + fmt::Display> fmt::Display for RwLockWriteGuard<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&**self, f)
    }
}

#[cfg(all(test, not(feature = "loom")))]
mod tests {
    use core::time::Duration;

    use crate::{
        sync::{Arc, rwlock::RwLock},
        thread,
        time::Instant,
        vec::Vec,
    };

    #[test]
    fn test_rwlock_basic() {
        let lock = RwLock::new(0);
        {
            let r1 = lock.read();
            let r2 = lock.read();
            assert_eq!(*r1, 0);
            assert_eq!(*r2, 0);
        }
        {
            let mut w = lock.write();
            *w = 42;
        }
        assert_eq!(*lock.read(), 42);
    }

    #[test]
    fn test_rwlock_threads() {
        let lock = Arc::new(RwLock::new(0));
        let num_threads = 4;
        let mut handles = Vec::new();

        for _ in 0..num_threads {
            let l = lock.clone();
            let handle = thread::spawn(move || {
                for _ in 0..100 {
                    let mut guard = l.write();
                    *guard += 1;
                }
            })
            .unwrap();
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }

        assert_eq!(*lock.read(), num_threads * 100);
    }

    #[test]
    fn test_rwlock_timed() {
        let lock = Arc::new(RwLock::new(0));
        let l = lock.clone();
        let guard = lock.write();

        let handle = thread::spawn(move || {
            let start = Instant::now();
            let res = l.try_read_for(Duration::from_millis(10));
            assert!(res.is_none());
            assert!(start.elapsed() >= Duration::from_millis(10));
        })
        .unwrap();

        handle.join().unwrap();
        drop(guard);
    }
}
