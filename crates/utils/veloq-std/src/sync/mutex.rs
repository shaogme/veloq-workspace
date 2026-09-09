pub mod raw;

use core::{
    fmt,
    ops::{Deref, DerefMut},
};

use crate::{
    cell::UnsafeCell,
    time::{Duration, Instant},
};

pub use self::raw::RawMutex;

pub struct Mutex<T: ?Sized> {
    raw: RawMutex,
    data: UnsafeCell<T>,
}

unsafe impl<T: ?Sized + Send> Send for Mutex<T> {}
unsafe impl<T: ?Sized + Send> Sync for Mutex<T> {}

impl<T> Mutex<T> {
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
pub const fn const_mutex<T>(val: T) -> Mutex<T> {
    Mutex::new(val)
}

impl<T: ?Sized> Mutex<T> {
    #[inline]
    pub fn lock(&self) -> MutexGuard<'_, T> {
        self.raw.lock();
        MutexGuard { mutex: self }
    }

    #[inline]
    pub fn try_lock(&self) -> Option<MutexGuard<'_, T>> {
        if self.raw.try_lock() {
            Some(MutexGuard { mutex: self })
        } else {
            None
        }
    }

    #[inline]
    pub fn try_lock_for(&self, timeout: Duration) -> Option<MutexGuard<'_, T>> {
        if self.raw.try_lock_for(timeout) {
            Some(MutexGuard { mutex: self })
        } else {
            None
        }
    }

    #[inline]
    pub fn try_lock_until(&self, timeout: Instant) -> Option<MutexGuard<'_, T>> {
        if self.raw.try_lock_until(timeout) {
            Some(MutexGuard { mutex: self })
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

impl<T: Default> Default for Mutex<T> {
    #[inline]
    fn default() -> Self {
        Self::new(T::default())
    }
}

impl<T> From<T> for Mutex<T> {
    #[inline]
    fn from(val: T) -> Self {
        Self::new(val)
    }
}

impl<T: ?Sized + fmt::Debug> fmt::Debug for Mutex<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut d = f.debug_struct("Mutex");
        if let Some(guard) = self.try_lock() {
            d.field("data", &&*guard);
        } else {
            d.field("data", &"<locked>");
        }
        d.finish_non_exhaustive()
    }
}

pub struct MutexGuard<'a, T: ?Sized> {
    mutex: &'a Mutex<T>,
}

unsafe impl<T: ?Sized + Sync> Sync for MutexGuard<'_, T> {}

impl<'a, T: ?Sized> MutexGuard<'a, T> {
    #[inline]
    pub fn mutex(guard: &Self) -> &'a Mutex<T> {
        guard.mutex
    }
}

impl<T: ?Sized> Deref for MutexGuard<'_, T> {
    type Target = T;

    #[inline]
    fn deref(&self) -> &Self::Target {
        unsafe { &*self.mutex.data.with(|p| p as *const T) }
    }
}

impl<T: ?Sized> DerefMut for MutexGuard<'_, T> {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { &mut *self.mutex.data.with_mut(|p| p as *mut T) }
    }
}

impl<T: ?Sized> Drop for MutexGuard<'_, T> {
    #[inline]
    fn drop(&mut self) {
        unsafe { self.mutex.raw.unlock() };
    }
}

impl<T: ?Sized + fmt::Debug> fmt::Debug for MutexGuard<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&**self, f)
    }
}

#[cfg(all(test, not(feature = "loom")))]
mod tests {
    use crate::{
        sync::{Arc, Mutex},
        thread,
        time::{Duration, Instant},
        vec::Vec,
    };

    #[test]
    fn test_mutex_basic() {
        let m = Mutex::new(0);
        {
            let mut guard = m.lock();
            *guard = 42;
        }
        assert_eq!(*m.lock(), 42);
        assert!(m.try_lock().is_some());
    }

    #[test]
    fn test_mutex_threads() {
        let mutex = Arc::new(Mutex::new(0));
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
        let mutex = Arc::new(Mutex::new(0));
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
