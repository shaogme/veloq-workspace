use core::{
    fmt,
    ops::{Deref, DerefMut},
};

use crate::{
    cell::NativeUnsafeCell,
    time::{Duration, Instant},
};

use super::mutex::raw::NativeRawMutex;

#[cfg(feature = "loom")]
use crate::{cell::LoomUnsafeCell, sync::mutex::raw::LoomRawMutex};

/// 不会记录 panic 中毒状态的互斥锁。
///
/// 该类型保留底层原始锁的直接访问语义；需要标准库兼容的中毒行为时，
/// 请使用 [`super::Mutex`]。
macro_rules! impl_unpoisoned_mutex {
    (
        $(#[$meta:meta])*
        struct $mutex_name:ident,
        guard: $guard_name:ident,
        raw: $raw_ty:ident,
        cell: $cell_ty:ident,
        $(const_new: $is_const:ident)?
    ) => {
        $(#[$meta])*
        pub struct $mutex_name<T: ?Sized> {
            raw: $raw_ty,
            data: $cell_ty<T>,
        }

        unsafe impl<T: ?Sized + Send> Send for $mutex_name<T> {}
        unsafe impl<T: ?Sized + Send> Sync for $mutex_name<T> {}

        impl<T> $mutex_name<T> {
            $(impl_unpoisoned_mutex!(@new_fn $is_const, $raw_ty, $cell_ty);)?

            #[inline]
            pub fn into_inner(self) -> T {
                self.data.into_inner()
            }
        }

        impl<T: ?Sized> $mutex_name<T> {
            #[inline]
            pub fn lock(&self) -> $guard_name<'_, T> {
                self.raw.lock();
                $guard_name { mutex: self }
            }

            #[inline]
            pub fn try_lock(&self) -> Option<$guard_name<'_, T>> {
                if self.raw.try_lock() {
                    Some($guard_name { mutex: self })
                } else {
                    None
                }
            }

            #[inline]
            pub fn try_lock_for(&self, timeout: Duration) -> Option<$guard_name<'_, T>> {
                if self.raw.try_lock_for(timeout) {
                    Some($guard_name { mutex: self })
                } else {
                    None
                }
            }

            #[inline]
            pub fn try_lock_until(&self, timeout: Instant) -> Option<$guard_name<'_, T>> {
                if self.raw.try_lock_until(timeout) {
                    Some($guard_name { mutex: self })
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
            pub fn raw(&self) -> &$raw_ty {
                &self.raw
            }
        }

        impl<T: Default> Default for $mutex_name<T> {
            #[inline]
            fn default() -> Self {
                Self::new(T::default())
            }
        }

        impl<T> From<T> for $mutex_name<T> {
            #[inline]
            fn from(value: T) -> Self {
                Self::new(value)
            }
        }

        impl<T: ?Sized + fmt::Debug> fmt::Debug for $mutex_name<T> {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                let mut debug = f.debug_struct(stringify!($mutex_name));
                if let Some(guard) = self.try_lock() {
                    debug.field("data", &&*guard);
                } else {
                    debug.field("data", &"<locked>");
                }
                debug.finish_non_exhaustive()
            }
        }

        pub struct $guard_name<'a, T: ?Sized> {
            mutex: &'a $mutex_name<T>,
        }

        unsafe impl<T: ?Sized + Sync> Sync for $guard_name<'_, T> {}

        impl<'a, T: ?Sized> $guard_name<'a, T> {
            #[inline]
            pub fn mutex(guard: &Self) -> &'a $mutex_name<T> {
                guard.mutex
            }
        }

        impl<T: ?Sized> Deref for $guard_name<'_, T> {
            type Target = T;

            #[inline]
            fn deref(&self) -> &Self::Target {
                unsafe { &*self.mutex.data.with(|p| p as *const T) }
            }
        }

        impl<T: ?Sized> DerefMut for $guard_name<'_, T> {
            #[inline]
            fn deref_mut(&mut self) -> &mut Self::Target {
                unsafe { &mut *self.mutex.data.with_mut(|p| p as *mut T) }
            }
        }

        impl<T: ?Sized> Drop for $guard_name<'_, T> {
            #[inline]
            fn drop(&mut self) {
                unsafe { self.mutex.raw.unlock() };
            }
        }

        impl<T: ?Sized + fmt::Debug> fmt::Debug for $guard_name<'_, T> {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                fmt::Debug::fmt(&**self, f)
            }
        }
    };

    (@new_fn const, $raw_ty:ident, $cell_ty:ident) => {
        #[inline]
        pub const fn new(value: T) -> Self {
            Self {
                raw: $raw_ty::new(),
                data: $cell_ty::new(value),
            }
        }
    };

    (@new_fn non_const, $raw_ty:ident, $cell_ty:ident) => {
        #[inline]
        #[track_caller]
        pub fn new(value: T) -> Self {
            Self {
                raw: $raw_ty::new(),
                data: $cell_ty::new(value),
            }
        }
    };
}

impl_unpoisoned_mutex!(
    /// 不会记录 panic 中毒状态的原生互斥锁。
    struct NativeUnpoisonedMutex,
    guard: NativeUnpoisonedMutexGuard,
    raw: NativeRawMutex,
    cell: NativeUnsafeCell,
    const_new: const
);

#[cfg(feature = "loom")]
impl_unpoisoned_mutex!(
    /// 由 Loom 运行时跟踪、不会记录 panic 中毒状态的互斥锁。
    struct LoomUnpoisonedMutex,
    guard: LoomUnpoisonedMutexGuard,
    raw: LoomRawMutex,
    cell: LoomUnsafeCell,
    const_new: non_const
);

#[cfg(not(feature = "loom"))]
pub type UnpoisonedMutex<T> = NativeUnpoisonedMutex<T>;

#[cfg(not(feature = "loom"))]
pub type UnpoisonedMutexGuard<'a, T> = NativeUnpoisonedMutexGuard<'a, T>;

#[cfg(feature = "loom")]
pub type UnpoisonedMutex<T> = LoomUnpoisonedMutex<T>;

#[cfg(feature = "loom")]
pub type UnpoisonedMutexGuard<'a, T> = LoomUnpoisonedMutexGuard<'a, T>;

#[inline]
pub const fn const_native_unpoisoned_mutex<T>(value: T) -> NativeUnpoisonedMutex<T> {
    NativeUnpoisonedMutex::new(value)
}

#[cfg(not(feature = "loom"))]
#[inline]
pub const fn const_unpoisoned_mutex<T>(value: T) -> UnpoisonedMutex<T> {
    const_native_unpoisoned_mutex(value)
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
        let mutex = UnpoisonedMutex::new(0);
        {
            let mut guard = mutex.lock();
            *guard = 42;
        }
        assert_eq!(*mutex.lock(), 42);
        assert!(mutex.try_lock().is_some());
    }

    #[test]
    fn test_mutex_threads() {
        let mutex = Arc::new(UnpoisonedMutex::new(0));
        let num_threads = 4;
        let mut handles = Vec::new();

        for _ in 0..num_threads {
            let mutex = mutex.clone();
            let handle = thread::spawn(move || {
                for _ in 0..100 {
                    let mut guard = mutex.lock();
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
        let worker_mutex = mutex.clone();
        let guard = mutex.lock();

        let handle = thread::spawn(move || {
            let start = Instant::now();
            let result = worker_mutex.try_lock_for(Duration::from_millis(10));
            assert!(result.is_none());
            assert!(start.elapsed() >= Duration::from_millis(10));
        })
        .unwrap();

        handle.join().unwrap();
        drop(guard);
    }
}
