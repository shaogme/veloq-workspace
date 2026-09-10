use core::{
    fmt,
    ops::{Deref, DerefMut},
};

use crate::{
    cell::UnsafeCell,
    time::{Duration, Instant},
};

use super::mutex::raw::RawMutex;

/// 不会记录 panic 中毒状态的互斥锁。
///
/// 该类型保留底层原始锁的直接访问语义；需要标准库兼容的中毒行为时，
/// 请使用 [`super::Mutex`]。
pub struct UnpoisonedMutex<T: ?Sized> {
    raw: RawMutex,
    data: UnsafeCell<T>,
}

unsafe impl<T: ?Sized + Send> Send for UnpoisonedMutex<T> {}
unsafe impl<T: ?Sized + Send> Sync for UnpoisonedMutex<T> {}

impl<T> UnpoisonedMutex<T> {
    #[cfg(not(feature = "loom"))]
    pub const fn new(val: T) -> Self {
        Self {
            raw: RawMutex::new(),
            data: UnsafeCell::new(val),
        }
    }

    #[cfg(feature = "loom")]
    pub fn new(val: T) -> Self {
        Self {
            raw: RawMutex::new(),
            data: UnsafeCell::new(val),
        }
    }

    #[inline]
    pub fn into_inner(self) -> T {
        self.data.into_inner()
    }
}

#[cfg(not(feature = "loom"))]
pub const fn const_unpoisoned_mutex<T>(val: T) -> UnpoisonedMutex<T> {
    UnpoisonedMutex::new(val)
}

impl<T: ?Sized> UnpoisonedMutex<T> {
    #[inline]
    pub fn lock(&self) -> UnpoisonedMutexGuard<'_, T> {
        self.raw.lock();
        UnpoisonedMutexGuard { mutex: self }
    }

    #[inline]
    pub fn try_lock(&self) -> Option<UnpoisonedMutexGuard<'_, T>> {
        if self.raw.try_lock() {
            Some(UnpoisonedMutexGuard { mutex: self })
        } else {
            None
        }
    }

    #[inline]
    pub fn try_lock_for(&self, timeout: Duration) -> Option<UnpoisonedMutexGuard<'_, T>> {
        if self.raw.try_lock_for(timeout) {
            Some(UnpoisonedMutexGuard { mutex: self })
        } else {
            None
        }
    }

    #[inline]
    pub fn try_lock_until(&self, timeout: Instant) -> Option<UnpoisonedMutexGuard<'_, T>> {
        if self.raw.try_lock_until(timeout) {
            Some(UnpoisonedMutexGuard { mutex: self })
        } else {
            None
        }
    }

    #[inline]
    pub fn is_locked(&self) -> bool {
        self.raw.is_locked()
    }

    #[inline]
    pub fn get_mut(&mut self) -> &mut T {
        unsafe { &mut *self.data.with_mut(|p| p as *mut T) }
    }

    #[inline]
    pub fn raw(&self) -> &RawMutex {
        &self.raw
    }
}

impl<T: Default> Default for UnpoisonedMutex<T> {
    #[inline]
    fn default() -> Self {
        Self::new(T::default())
    }
}

impl<T> From<T> for UnpoisonedMutex<T> {
    #[inline]
    fn from(val: T) -> Self {
        Self::new(val)
    }
}

impl<T: ?Sized + fmt::Debug> fmt::Debug for UnpoisonedMutex<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut d = f.debug_struct("UnpoisonedMutex");
        if let Some(guard) = self.try_lock() {
            d.field("data", &&*guard);
        } else {
            d.field("data", &"<locked>");
        }
        d.finish_non_exhaustive()
    }
}

pub struct UnpoisonedMutexGuard<'a, T: ?Sized> {
    mutex: &'a UnpoisonedMutex<T>,
}

unsafe impl<T: ?Sized + Sync> Sync for UnpoisonedMutexGuard<'_, T> {}

impl<'a, T: ?Sized> UnpoisonedMutexGuard<'a, T> {
    #[inline]
    pub fn mutex(guard: &Self) -> &'a UnpoisonedMutex<T> {
        guard.mutex
    }
}

impl<T: ?Sized> Deref for UnpoisonedMutexGuard<'_, T> {
    type Target = T;

    #[inline]
    fn deref(&self) -> &Self::Target {
        unsafe { &*self.mutex.data.with(|p| p as *const T) }
    }
}

impl<T: ?Sized> DerefMut for UnpoisonedMutexGuard<'_, T> {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { &mut *self.mutex.data.with_mut(|p| p as *mut T) }
    }
}

impl<T: ?Sized> Drop for UnpoisonedMutexGuard<'_, T> {
    #[inline]
    fn drop(&mut self) {
        unsafe { self.mutex.raw.unlock() };
    }
}

impl<T: ?Sized + fmt::Debug> fmt::Debug for UnpoisonedMutexGuard<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&**self, f)
    }
}

#[cfg(all(test, not(feature = "loom")))]
mod tests {
    use crate::{
        sync::{Arc, UnpoisonedMutex},
        thread,
        time::{Duration, Instant},
        vec::Vec,
    };

    #[test]
    fn test_mutex_basic() {
        let m = UnpoisonedMutex::new(0);
        {
            let mut guard = m.lock();
            *guard = 42;
        }
        assert_eq!(*m.lock(), 42);
        assert!(m.try_lock().is_some());
    }

    #[test]
    fn test_mutex_threads() {
        let mutex = Arc::new(UnpoisonedMutex::new(0));
        let num_threads = 4;
        let mut handles = Vec::new();

        for _ in 0..num_threads {
            let m = mutex.clone();
            let handle = thread::spawn(move || {
                for _ in 0..100 {
                    let mut guard = m.lock();
                    *guard += 1;
                }
            })
            .unwrap();
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }

        assert_eq!(*mutex.lock(), num_threads * 100);
    }

    #[test]
    fn test_mutex_timed() {
        let mutex = Arc::new(UnpoisonedMutex::new(0));
        let m = mutex.clone();
        let guard = mutex.lock();

        let handle = thread::spawn(move || {
            let start = Instant::now();
            let res = m.try_lock_for(Duration::from_millis(10));
            assert!(res.is_none()); // Should fail to acquire because guard is held
            assert!(start.elapsed() >= Duration::from_millis(10));
        })
        .unwrap();

        handle.join().unwrap();
        drop(guard);
    }
}
