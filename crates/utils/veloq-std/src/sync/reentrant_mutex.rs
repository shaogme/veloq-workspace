use core::fmt;

#[cfg(not(feature = "loom"))]
mod std_impl {
    use core::{
        cell::UnsafeCell,
        marker::PhantomData,
        ops::Deref,
        sync::atomic::{AtomicU64, AtomicUsize, Ordering},
        time::Duration,
    };

    use crate::{sync::mutex::raw::RawMutex, thread, time::Instant};
    use lock_api::{RawMutex as RawMutexTrait, RawMutexTimed};

    pub struct ReentrantMutex<T: ?Sized> {
        raw: RawMutex,
        owner: AtomicU64,
        count: AtomicUsize,
        data: UnsafeCell<T>,
    }

    unsafe impl<T: ?Sized + Send> Send for ReentrantMutex<T> {}
    unsafe impl<T: ?Sized + Send> Sync for ReentrantMutex<T> {}

    pub const fn const_reentrant_mutex<T>(val: T) -> ReentrantMutex<T> {
        ReentrantMutex {
            raw: <RawMutex as RawMutexTrait>::INIT,
            owner: AtomicU64::new(0),
            count: AtomicUsize::new(0),
            data: UnsafeCell::new(val),
        }
    }

    impl<T> ReentrantMutex<T> {
        #[inline]
        pub const fn new(val: T) -> Self {
            const_reentrant_mutex(val)
        }

        #[inline]
        pub fn into_inner(self) -> T {
            self.data.into_inner()
        }
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
            self.data.get_mut()
        }

        #[inline]
        pub fn reentrancy_count(&self) -> usize {
            if self.is_owned_by_current_thread() {
                self.count.load(Ordering::Relaxed)
            } else {
                0
            }
        }
    }

    pub struct ReentrantMutexGuard<'a, T: ?Sized> {
        pub(super) lock: &'a ReentrantMutex<T>,
        pub(super) _marker: PhantomData<*const ()>,
    }

    unsafe impl<T: ?Sized + Sync> Sync for ReentrantMutexGuard<'_, T> {}

    impl<T: ?Sized> Deref for ReentrantMutexGuard<'_, T> {
        type Target = T;

        #[inline]
        fn deref(&self) -> &Self::Target {
            unsafe { &*self.lock.data.get() }
        }
    }

    impl<T: ?Sized> Drop for ReentrantMutexGuard<'_, T> {
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
}

#[cfg(not(feature = "loom"))]
pub use std_impl::*;

#[cfg(feature = "loom")]
mod loom_impl {
    use core::{
        marker::PhantomData, mem::transmute, ops::Deref, sync::atomic::Ordering, time::Duration,
    };

    use crate::{thread, time::Instant};
    use loom::{
        cell::{ConstPtr as LoomConstPtr, UnsafeCell as LoomUnsafeCell},
        sync::{
            Mutex as LoomMutex, MutexGuard as LoomMutexGuard,
            atomic::{AtomicU64 as LoomAtomicU64, AtomicUsize as LoomAtomicUsize},
        },
    };

    pub struct ReentrantMutex<T: ?Sized> {
        raw: LoomMutex<()>,
        raw_guard: LoomUnsafeCell<Option<LoomMutexGuard<'static, ()>>>,
        owner: LoomAtomicU64,
        count: LoomAtomicUsize,
        data: LoomUnsafeCell<T>,
    }

    unsafe impl<T: ?Sized + Send> Send for ReentrantMutex<T> {}
    unsafe impl<T: ?Sized + Send> Sync for ReentrantMutex<T> {}

    impl<T> ReentrantMutex<T> {
        pub fn new(val: T) -> Self {
            Self {
                raw: LoomMutex::new(()),
                raw_guard: LoomUnsafeCell::new(None),
                owner: LoomAtomicU64::new(0),
                count: LoomAtomicUsize::new(0),
                data: LoomUnsafeCell::new(val),
            }
        }

        pub fn into_inner(self) -> T {
            self.data.into_inner()
        }
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
                let g = self.raw.lock().unwrap();
                let static_g: LoomMutexGuard<'static, ()> = unsafe { transmute(g) };
                self.raw_guard.with_mut(|opt| unsafe {
                    *opt = Some(static_g);
                });
                self.owner.store(current_thread, Ordering::Relaxed);
                self.count.store(1, Ordering::Relaxed);
            }
            ReentrantMutexGuard {
                lock: self,
                _ptr: self.data.get(),
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
                    _ptr: self.data.get(),
                    _marker: PhantomData,
                })
            } else if let Ok(g) = self.raw.try_lock() {
                let static_g: LoomMutexGuard<'static, ()> = unsafe { transmute(g) };
                self.raw_guard.with_mut(|opt| unsafe {
                    *opt = Some(static_g);
                });
                self.owner.store(current_thread, Ordering::Relaxed);
                self.count.store(1, Ordering::Relaxed);
                Some(ReentrantMutexGuard {
                    lock: self,
                    _ptr: self.data.get(),
                    _marker: PhantomData,
                })
            } else {
                None
            }
        }

        pub fn try_lock_for(&self, _timeout: Duration) -> Option<ReentrantMutexGuard<'_, T>> {
            self.try_lock()
        }

        pub fn try_lock_until(&self, _timeout: Instant) -> Option<ReentrantMutexGuard<'_, T>> {
            self.try_lock()
        }

        pub fn is_locked(&self) -> bool {
            self.owner.load(Ordering::Relaxed) != 0
        }

        pub fn is_owned_by_current_thread(&self) -> bool {
            self.owner.load(Ordering::Relaxed) == thread::current_id().as_u64()
        }

        pub fn reentrancy_count(&self) -> usize {
            if self.is_owned_by_current_thread() {
                self.count.load(Ordering::Relaxed)
            } else {
                0
            }
        }

        pub fn get_mut(&mut self) -> &mut T {
            self.data.with_mut(|p| unsafe { &mut *p })
        }
    }

    pub struct ReentrantMutexGuard<'a, T: ?Sized> {
        pub(super) lock: &'a ReentrantMutex<T>,
        pub(super) _ptr: LoomConstPtr<T>,
        pub(super) _marker: PhantomData<*const ()>,
    }

    unsafe impl<T: ?Sized + Sync> Sync for ReentrantMutexGuard<'_, T> {}

    impl<T: ?Sized> Deref for ReentrantMutexGuard<'_, T> {
        type Target = T;

        #[inline]
        fn deref(&self) -> &Self::Target {
            unsafe { self._ptr.deref() }
        }
    }

    impl<T: ?Sized> Drop for ReentrantMutexGuard<'_, T> {
        fn drop(&mut self) {
            let cur = self.lock.count.load(Ordering::Relaxed);
            if cur > 1 {
                self.lock.count.store(cur - 1, Ordering::Relaxed);
            } else {
                self.lock.count.store(0, Ordering::Relaxed);
                self.lock.owner.store(0, Ordering::Relaxed);
                let g = self.lock.raw_guard.with_mut(|opt| unsafe { (*opt).take() });
                drop(g);
            }
        }
    }
}

#[cfg(feature = "loom")]
pub use loom_impl::*;

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
