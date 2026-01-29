pub use veloq_shim::*;

pub mod queue {
    #[cfg(not(feature = "loom"))]
    pub use crossbeam_queue::{ArrayQueue, SegQueue};

    #[cfg(feature = "loom")]
    pub use self::loom_queues::{ArrayQueue, SegQueue};

    #[cfg(feature = "loom")]
    mod loom_queues {
        use loom::sync::Mutex;
        use std::collections::VecDeque;

        pub struct SegQueue<T> {
            inner: Mutex<VecDeque<T>>,
        }

        impl<T> SegQueue<T> {
            pub fn new() -> Self {
                Self {
                    inner: Mutex::new(VecDeque::new()),
                }
            }

            pub fn push(&self, t: T) {
                self.inner.lock().unwrap().push_back(t);
            }

            pub fn pop(&self) -> Option<T> {
                self.inner.lock().unwrap().pop_front()
            }

            pub fn is_empty(&self) -> bool {
                self.inner.lock().unwrap().is_empty()
            }
        }

        pub struct ArrayQueue<T> {
            inner: Mutex<VecDeque<T>>,
            cap: usize,
        }

        impl<T> ArrayQueue<T> {
            pub fn new(cap: usize) -> Self {
                Self {
                    inner: Mutex::new(VecDeque::new()),
                    cap,
                }
            }

            pub fn push(&self, t: T) -> Result<(), T> {
                let mut lock = self.inner.lock().unwrap();
                if lock.len() >= self.cap {
                    return Err(t);
                }
                lock.push_back(t);
                Ok(())
            }

            pub fn pop(&self) -> Option<T> {
                self.inner.lock().unwrap().pop_front()
            }

            pub fn is_full(&self) -> bool {
                self.inner.lock().unwrap().len() >= self.cap
            }
        }
    }
}

pub mod lock {
    #[cfg(not(feature = "loom"))]
    use super::atomic::{AtomicBool, Ordering};

    #[cfg(not(feature = "loom"))]
    pub struct SpinLock<T> {
        locked: AtomicBool,
        data: std::cell::UnsafeCell<T>,
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
                data: std::cell::UnsafeCell::new(data),
            }
        }

        pub fn lock(&self) -> SpinLockGuard<'_, T> {
            let backoff = crossbeam_utils::Backoff::new();
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
    impl<'a, T> std::ops::Deref for SpinLockGuard<'a, T> {
        type Target = T;
        fn deref(&self) -> &Self::Target {
            unsafe { &*self.lock.data.get() }
        }
    }

    #[cfg(not(feature = "loom"))]
    impl<'a, T> std::ops::DerefMut for SpinLockGuard<'a, T> {
        fn deref_mut(&mut self) -> &mut Self::Target {
            unsafe { &mut *self.lock.data.get() }
        }
    }

    // --- Loom Friendly SpinLock Replacement ---
    #[cfg(feature = "loom")]
    pub struct SpinLock<T> {
        inner: loom::sync::Mutex<T>,
    }

    #[cfg(feature = "loom")]
    impl<T> SpinLock<T> {
        pub fn new(data: T) -> Self {
            Self {
                inner: loom::sync::Mutex::new(data),
            }
        }

        pub fn lock(&self) -> loom::sync::MutexGuard<'_, T> {
            self.inner.lock().unwrap()
        }
    }
}
