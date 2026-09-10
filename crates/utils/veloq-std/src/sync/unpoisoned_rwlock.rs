use core::{
    fmt,
    ops::{Deref, DerefMut},
};

use crate::{
    cell::NativeUnsafeCell,
    time::{Duration, Instant},
};

use super::rwlock::raw::NativeRawRwLock;

#[cfg(feature = "loom")]
use crate::{cell::LoomUnsafeCell, sync::rwlock::raw::LoomRawRwLock};

/// 不会记录 panic 中毒状态的读写锁。
///
/// 该类型保留底层原始锁的直接访问语义；需要标准库兼容的中毒行为时，
/// 请使用 [`super::RwLock`]。
macro_rules! impl_unpoisoned_rwlock {
    (
        $(#[$meta:meta])*
        struct $lock_name:ident,
        read_guard: $read_guard_name:ident,
        write_guard: $write_guard_name:ident,
        raw: $raw_ty:ident,
        cell: $cell_ty:ident,
        $(const_new: $is_const:ident)?
    ) => {
        $(#[$meta])*
        pub struct $lock_name<T: ?Sized> {
            raw: $raw_ty,
            data: $cell_ty<T>,
        }

        unsafe impl<T: ?Sized + Send> Send for $lock_name<T> {}
        unsafe impl<T: ?Sized + Send + Sync> Sync for $lock_name<T> {}

        impl<T> $lock_name<T> {
            $(impl_unpoisoned_rwlock!(@new_fn $is_const, $raw_ty, $cell_ty);)?

            #[inline]
            pub fn into_inner(self) -> T {
                self.data.into_inner()
            }
        }

        impl<T: ?Sized> $lock_name<T> {
            #[inline]
            pub fn read(&self) -> $read_guard_name<'_, T> {
                self.raw.lock_shared();
                $read_guard_name { rwlock: self }
            }

            #[inline]
            pub fn try_read(&self) -> Option<$read_guard_name<'_, T>> {
                if self.raw.try_lock_shared() {
                    Some($read_guard_name { rwlock: self })
                } else {
                    None
                }
            }

            #[inline]
            pub fn try_read_for(&self, timeout: Duration) -> Option<$read_guard_name<'_, T>> {
                if self.raw.try_lock_shared_for(timeout) {
                    Some($read_guard_name { rwlock: self })
                } else {
                    None
                }
            }

            #[inline]
            pub fn try_read_until(&self, timeout: Instant) -> Option<$read_guard_name<'_, T>> {
                if self.raw.try_lock_shared_until(timeout) {
                    Some($read_guard_name { rwlock: self })
                } else {
                    None
                }
            }

            #[inline]
            pub fn write(&self) -> $write_guard_name<'_, T> {
                self.raw.lock_exclusive();
                $write_guard_name { rwlock: self }
            }

            #[inline]
            pub fn try_write(&self) -> Option<$write_guard_name<'_, T>> {
                if self.raw.try_lock_exclusive() {
                    Some($write_guard_name { rwlock: self })
                } else {
                    None
                }
            }

            #[inline]
            pub fn try_write_for(&self, timeout: Duration) -> Option<$write_guard_name<'_, T>> {
                if self.raw.try_lock_exclusive_for(timeout) {
                    Some($write_guard_name { rwlock: self })
                } else {
                    None
                }
            }

            #[inline]
            pub fn try_write_until(&self, timeout: Instant) -> Option<$write_guard_name<'_, T>> {
                if self.raw.try_lock_exclusive_until(timeout) {
                    Some($write_guard_name { rwlock: self })
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
            pub fn raw(&self) -> &$raw_ty {
                &self.raw
            }
        }

        impl<T: Default> Default for $lock_name<T> {
            #[inline]
            fn default() -> Self {
                Self::new(T::default())
            }
        }

        impl<T> From<T> for $lock_name<T> {
            #[inline]
            fn from(value: T) -> Self {
                Self::new(value)
            }
        }

        impl<T: ?Sized + fmt::Debug> fmt::Debug for $lock_name<T> {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                let mut debug = f.debug_struct(stringify!($lock_name));
                if let Some(guard) = self.try_read() {
                    debug.field("data", &&*guard);
                } else {
                    debug.field("data", &"<locked>");
                }
                debug.finish_non_exhaustive()
            }
        }

        pub struct $read_guard_name<'a, T: ?Sized> {
            rwlock: &'a $lock_name<T>,
        }

        unsafe impl<T: ?Sized + Sync> Sync for $read_guard_name<'_, T> {}

        impl<'a, T: ?Sized> $read_guard_name<'a, T> {
            #[inline]
            pub fn rwlock(guard: &Self) -> &'a $lock_name<T> {
                guard.rwlock
            }
        }

        impl<T: ?Sized> Deref for $read_guard_name<'_, T> {
            type Target = T;

            #[inline]
            fn deref(&self) -> &Self::Target {
                unsafe { &*self.rwlock.data.with(|p| p as *const T) }
            }
        }

        impl<T: ?Sized> Drop for $read_guard_name<'_, T> {
            #[inline]
            fn drop(&mut self) {
                unsafe { self.rwlock.raw.unlock_shared() };
            }
        }

        pub struct $write_guard_name<'a, T: ?Sized> {
            rwlock: &'a $lock_name<T>,
        }

        unsafe impl<T: ?Sized + Sync> Sync for $write_guard_name<'_, T> {}

        impl<'a, T: ?Sized> $write_guard_name<'a, T> {
            #[inline]
            pub fn rwlock(guard: &Self) -> &'a $lock_name<T> {
                guard.rwlock
            }

            /// Downgrades an exclusive guard to a shared guard atomically.
            #[inline]
            pub fn downgrade(guard: Self) -> $read_guard_name<'a, T> {
                let rwlock = guard.rwlock;
                core::mem::forget(guard);
                unsafe { rwlock.raw.downgrade() };
                $read_guard_name { rwlock }
            }
        }

        impl<T: ?Sized> Deref for $write_guard_name<'_, T> {
            type Target = T;

            #[inline]
            fn deref(&self) -> &Self::Target {
                unsafe { &*self.rwlock.data.with(|p| p as *const T) }
            }
        }

        impl<T: ?Sized> DerefMut for $write_guard_name<'_, T> {
            #[inline]
            fn deref_mut(&mut self) -> &mut Self::Target {
                unsafe { &mut *self.rwlock.data.with_mut(|p| p as *mut T) }
            }
        }

        impl<T: ?Sized> Drop for $write_guard_name<'_, T> {
            #[inline]
            fn drop(&mut self) {
                unsafe { self.rwlock.raw.unlock_exclusive() };
            }
        }

        impl<T: ?Sized + fmt::Debug> fmt::Debug for $read_guard_name<'_, T> {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                fmt::Debug::fmt(&**self, f)
            }
        }

        impl<T: ?Sized + fmt::Display> fmt::Display for $read_guard_name<'_, T> {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                fmt::Display::fmt(&**self, f)
            }
        }

        impl<T: ?Sized + fmt::Debug> fmt::Debug for $write_guard_name<'_, T> {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                fmt::Debug::fmt(&**self, f)
            }
        }

        impl<T: ?Sized + fmt::Display> fmt::Display for $write_guard_name<'_, T> {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                fmt::Display::fmt(&**self, f)
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

impl_unpoisoned_rwlock!(
    /// 不会记录 panic 中毒状态的原生读写锁。
    struct NativeUnpoisonedRwLock,
    read_guard: NativeUnpoisonedRwLockReadGuard,
    write_guard: NativeUnpoisonedRwLockWriteGuard,
    raw: NativeRawRwLock,
    cell: NativeUnsafeCell,
    const_new: const
);

#[cfg(feature = "loom")]
impl_unpoisoned_rwlock!(
    /// 由 Loom 运行时跟踪、不会记录 panic 中毒状态的读写锁。
    struct LoomUnpoisonedRwLock,
    read_guard: LoomUnpoisonedRwLockReadGuard,
    write_guard: LoomUnpoisonedRwLockWriteGuard,
    raw: LoomRawRwLock,
    cell: LoomUnsafeCell,
    const_new: non_const
);

#[cfg(not(feature = "loom"))]
pub type UnpoisonedRwLock<T> = NativeUnpoisonedRwLock<T>;

#[cfg(not(feature = "loom"))]
pub type UnpoisonedRwLockReadGuard<'a, T> = NativeUnpoisonedRwLockReadGuard<'a, T>;

#[cfg(not(feature = "loom"))]
pub type UnpoisonedRwLockWriteGuard<'a, T> = NativeUnpoisonedRwLockWriteGuard<'a, T>;

#[cfg(feature = "loom")]
pub type UnpoisonedRwLock<T> = LoomUnpoisonedRwLock<T>;

#[cfg(feature = "loom")]
pub type UnpoisonedRwLockReadGuard<'a, T> = LoomUnpoisonedRwLockReadGuard<'a, T>;

#[cfg(feature = "loom")]
pub type UnpoisonedRwLockWriteGuard<'a, T> = LoomUnpoisonedRwLockWriteGuard<'a, T>;

#[inline]
pub const fn const_native_unpoisoned_rwlock<T>(value: T) -> NativeUnpoisonedRwLock<T> {
    NativeUnpoisonedRwLock::new(value)
}

#[cfg(not(feature = "loom"))]
#[inline]
pub const fn const_unpoisoned_rwlock<T>(value: T) -> UnpoisonedRwLock<T> {
    const_native_unpoisoned_rwlock(value)
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
            let mut writer = lock.write();
            *writer = 42;
        }
        assert_eq!(*lock.read(), 42);
    }

    #[test]
    fn test_rwlock_threads() {
        let lock = Arc::new(UnpoisonedRwLock::new(0));
        let num_threads = 4;
        let mut handles = Vec::new();

        for _ in 0..num_threads {
            let lock = lock.clone();
            let handle = thread::spawn(move || {
                for _ in 0..100 {
                    let mut writer = lock.write();
                    *writer += 1;
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
        let worker_lock = lock.clone();
        let guard = lock.write();

        let handle = thread::spawn(move || {
            let start = Instant::now();
            let result = worker_lock.try_read_for(Duration::from_millis(10));
            assert!(result.is_none());
            assert!(start.elapsed() >= Duration::from_millis(10));
        })
        .unwrap();

        handle.join().unwrap();
        drop(guard);
    }
}
