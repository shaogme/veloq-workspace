#[cfg(not(feature = "loom"))]
mod std_impl {
    use crate::sync::raw_rwlock::RawRwLock;
    use lock_api::RawRwLock as RawRwLockTrait;

    pub type RwLock<T> = lock_api::RwLock<RawRwLock, T>;
    pub type RwLockReadGuard<'a, T> = lock_api::RwLockReadGuard<'a, RawRwLock, T>;
    pub type RwLockWriteGuard<'a, T> = lock_api::RwLockWriteGuard<'a, RawRwLock, T>;

    pub const fn const_rwlock<T>(val: T) -> RwLock<T> {
        RwLock::const_new(<RawRwLock as RawRwLockTrait>::INIT, val)
    }
}

#[cfg(not(feature = "loom"))]
pub use std_impl::*;

#[cfg(feature = "loom")]
mod loom_impl {
    use crate::time::{Duration, Instant};

    pub use loom::sync::{RwLock as LoomRwLock, RwLockReadGuard, RwLockWriteGuard};

    #[derive(Debug)]
    pub struct RwLock<T> {
        inner: LoomRwLock<T>,
    }

    impl<T> RwLock<T> {
        #[inline]
        pub fn new(val: T) -> Self {
            Self {
                inner: LoomRwLock::new(val),
            }
        }

        #[inline]
        pub fn read(&self) -> RwLockReadGuard<'_, T> {
            self.inner.read().unwrap()
        }

        #[inline]
        pub fn write(&self) -> RwLockWriteGuard<'_, T> {
            self.inner.write().unwrap()
        }

        #[inline]
        pub fn try_read(&self) -> Option<RwLockReadGuard<'_, T>> {
            self.inner.try_read().ok()
        }

        #[inline]
        pub fn try_write(&self) -> Option<RwLockWriteGuard<'_, T>> {
            self.inner.try_write().ok()
        }

        #[inline]
        pub fn try_read_for(&self, _timeout: Duration) -> Option<RwLockReadGuard<'_, T>> {
            self.try_read()
        }

        #[inline]
        pub fn try_read_until(&self, _timeout: Instant) -> Option<RwLockReadGuard<'_, T>> {
            self.try_read()
        }

        #[inline]
        pub fn try_write_for(&self, _timeout: Duration) -> Option<RwLockWriteGuard<'_, T>> {
            self.try_write()
        }

        #[inline]
        pub fn try_write_until(&self, _timeout: Instant) -> Option<RwLockWriteGuard<'_, T>> {
            self.try_write()
        }
    }
}

#[cfg(feature = "loom")]
pub use loom_impl::*;

#[cfg(test)]
mod tests {
    use crate::sync::rwlock::RwLock;
    use crate::thread;
    use alloc::sync::Arc;
    use core::time::Duration;

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
        let mut handles = alloc::vec::Vec::new();

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
            let start = crate::time::Instant::now();
            let res = l.try_read_for(Duration::from_millis(10));
            assert!(res.is_none());
            assert!(start.elapsed() >= Duration::from_millis(10));
        })
        .unwrap();

        handle.join().unwrap();
        drop(guard);
    }
}
