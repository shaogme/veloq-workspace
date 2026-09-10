use core::{
    fmt,
    ops::{Deref, DerefMut},
};

use crate::{
    cell::UnsafeCell,
    time::{Duration, Instant},
};

use super::rwlock::raw::RawRwLock;

/// 不会记录 panic 中毒状态的读写锁。
///
/// 该类型保留底层原始锁的直接访问语义；需要标准库兼容的中毒行为时，
/// 请使用 [`super::RwLock`]。
pub struct UnpoisonedRwLock<T: ?Sized> {
    raw: RawRwLock,
    data: UnsafeCell<T>,
}

unsafe impl<T: ?Sized + Send> Send for UnpoisonedRwLock<T> {}
unsafe impl<T: ?Sized + Send + Sync> Sync for UnpoisonedRwLock<T> {}

impl<T> UnpoisonedRwLock<T> {
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
pub const fn const_unpoisoned_rwlock<T>(val: T) -> UnpoisonedRwLock<T> {
    UnpoisonedRwLock::new(val)
}

impl<T: ?Sized> UnpoisonedRwLock<T> {
    #[inline]
    pub fn read(&self) -> UnpoisonedRwLockReadGuard<'_, T> {
        self.raw.lock_shared();
        UnpoisonedRwLockReadGuard { rwlock: self }
    }

    #[inline]
    pub fn try_read(&self) -> Option<UnpoisonedRwLockReadGuard<'_, T>> {
        if self.raw.try_lock_shared() {
            Some(UnpoisonedRwLockReadGuard { rwlock: self })
        } else {
            None
        }
    }

    #[inline]
    pub fn try_read_for(&self, timeout: Duration) -> Option<UnpoisonedRwLockReadGuard<'_, T>> {
        if self.raw.try_lock_shared_for(timeout) {
            Some(UnpoisonedRwLockReadGuard { rwlock: self })
        } else {
            None
        }
    }

    #[inline]
    pub fn try_read_until(&self, timeout: Instant) -> Option<UnpoisonedRwLockReadGuard<'_, T>> {
        if self.raw.try_lock_shared_until(timeout) {
            Some(UnpoisonedRwLockReadGuard { rwlock: self })
        } else {
            None
        }
    }

    #[inline]
    pub fn write(&self) -> UnpoisonedRwLockWriteGuard<'_, T> {
        self.raw.lock_exclusive();
        UnpoisonedRwLockWriteGuard { rwlock: self }
    }

    #[inline]
    pub fn try_write(&self) -> Option<UnpoisonedRwLockWriteGuard<'_, T>> {
        if self.raw.try_lock_exclusive() {
            Some(UnpoisonedRwLockWriteGuard { rwlock: self })
        } else {
            None
        }
    }

    #[inline]
    pub fn try_write_for(&self, timeout: Duration) -> Option<UnpoisonedRwLockWriteGuard<'_, T>> {
        if self.raw.try_lock_exclusive_for(timeout) {
            Some(UnpoisonedRwLockWriteGuard { rwlock: self })
        } else {
            None
        }
    }

    #[inline]
    pub fn try_write_until(&self, timeout: Instant) -> Option<UnpoisonedRwLockWriteGuard<'_, T>> {
        if self.raw.try_lock_exclusive_until(timeout) {
            Some(UnpoisonedRwLockWriteGuard { rwlock: self })
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

impl<T: Default> Default for UnpoisonedRwLock<T> {
    #[inline]
    fn default() -> Self {
        Self::new(T::default())
    }
}

impl<T> From<T> for UnpoisonedRwLock<T> {
    #[inline]
    fn from(val: T) -> Self {
        Self::new(val)
    }
}

impl<T: ?Sized + fmt::Debug> fmt::Debug for UnpoisonedRwLock<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut d = f.debug_struct("UnpoisonedRwLock");
        if let Some(guard) = self.try_read() {
            d.field("data", &&*guard);
        } else {
            d.field("data", &"<locked>");
        }
        d.finish_non_exhaustive()
    }
}

pub struct UnpoisonedRwLockReadGuard<'a, T: ?Sized> {
    rwlock: &'a UnpoisonedRwLock<T>,
}

unsafe impl<T: ?Sized + Sync> Sync for UnpoisonedRwLockReadGuard<'_, T> {}

impl<'a, T: ?Sized> UnpoisonedRwLockReadGuard<'a, T> {
    #[inline]
    pub fn rwlock(guard: &Self) -> &'a UnpoisonedRwLock<T> {
        guard.rwlock
    }
}

impl<T: ?Sized> Deref for UnpoisonedRwLockReadGuard<'_, T> {
    type Target = T;

    #[inline]
    fn deref(&self) -> &Self::Target {
        unsafe { &*self.rwlock.data.with(|p| p as *const T) }
    }
}

impl<T: ?Sized> Drop for UnpoisonedRwLockReadGuard<'_, T> {
    #[inline]
    fn drop(&mut self) {
        unsafe { self.rwlock.raw.unlock_shared() };
    }
}

pub struct UnpoisonedRwLockWriteGuard<'a, T: ?Sized> {
    rwlock: &'a UnpoisonedRwLock<T>,
}

unsafe impl<T: ?Sized + Sync> Sync for UnpoisonedRwLockWriteGuard<'_, T> {}

impl<'a, T: ?Sized> UnpoisonedRwLockWriteGuard<'a, T> {
    #[inline]
    pub fn rwlock(guard: &Self) -> &'a UnpoisonedRwLock<T> {
        guard.rwlock
    }

    #[inline]
    pub fn downgrade(guard: Self) -> UnpoisonedRwLockReadGuard<'a, T> {
        let rwlock = guard.rwlock;
        core::mem::forget(guard);
        unsafe { rwlock.raw.downgrade() };
        UnpoisonedRwLockReadGuard { rwlock }
    }
}

impl<T: ?Sized> Deref for UnpoisonedRwLockWriteGuard<'_, T> {
    type Target = T;

    #[inline]
    fn deref(&self) -> &Self::Target {
        unsafe { &*self.rwlock.data.with(|p| p as *const T) }
    }
}

impl<T: ?Sized> DerefMut for UnpoisonedRwLockWriteGuard<'_, T> {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { &mut *self.rwlock.data.with_mut(|p| p as *mut T) }
    }
}

impl<T: ?Sized> Drop for UnpoisonedRwLockWriteGuard<'_, T> {
    #[inline]
    fn drop(&mut self) {
        unsafe { self.rwlock.raw.unlock_exclusive() };
    }
}

impl<T: ?Sized + fmt::Debug> fmt::Debug for UnpoisonedRwLockReadGuard<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&**self, f)
    }
}

impl<T: ?Sized + fmt::Display> fmt::Display for UnpoisonedRwLockReadGuard<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&**self, f)
    }
}

impl<T: ?Sized + fmt::Debug> fmt::Debug for UnpoisonedRwLockWriteGuard<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&**self, f)
    }
}

impl<T: ?Sized + fmt::Display> fmt::Display for UnpoisonedRwLockWriteGuard<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&**self, f)
    }
}

#[cfg(all(test, not(feature = "loom")))]
mod tests {
    use core::time::Duration;

    use crate::{
        sync::{Arc, UnpoisonedRwLock},
        thread,
        time::Instant,
        vec::Vec,
    };

    #[test]
    fn test_rwlock_basic() {
        let lock = UnpoisonedRwLock::new(0);
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
        let lock = Arc::new(UnpoisonedRwLock::new(0));
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
        let lock = Arc::new(UnpoisonedRwLock::new(0));
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
