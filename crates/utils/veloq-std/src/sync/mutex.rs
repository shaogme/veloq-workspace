pub mod raw;

use core::{
    fmt,
    ops::{Deref, DerefMut},
};

use crate::{
    cell::NativeUnsafeCell,
    sync::{
        LockResult, PoisonError, TryLockError, TryLockResult,
        poison::NativePoisonState,
        unpoisoned_mutex::{NativeUnpoisonedMutex, NativeUnpoisonedMutexGuard},
    },
    thread,
    time::{Duration, Instant},
};

use self::raw::NativeRawMutex;

#[cfg(feature = "loom")]
use crate::{
    cell::LoomUnsafeCell,
    sync::unpoisoned_mutex::{LoomUnpoisonedMutex, LoomUnpoisonedMutexGuard},
};

#[cfg(feature = "loom")]
use self::raw::LoomRawMutex;

#[cfg(feature = "loom")]
use crate::sync::poison::LoomPoisonState;

/// A mutex with poisoning behavior compatible with `std::sync::Mutex`.
///
/// The native implementation is always available. With the `loom` feature,
/// the default [`Mutex`] alias selects the Loom implementation so accesses to
/// the protected data are tracked by Loom.
macro_rules! impl_mutex {
    (
        $(#[$meta:meta])*
        struct $mutex_name:ident,
        guard: $guard_name:ident,
        unpoisoned_mutex: $unpoisoned_mutex_ty:ident,
        unpoisoned_guard: $unpoisoned_guard_ty:ident,
        raw_mutex: $raw_mutex_ty:ident,
        cell: $cell_ty:ident,
        poison: $poison_ty:ident,
        $(const_new: $is_const:ident)?
    ) => {
        $(#[$meta])*
        pub struct $mutex_name<T: ?Sized> {
            inner: $unpoisoned_mutex_ty<()>,
            poison: $poison_ty,
            data: $cell_ty<T>,
        }

        unsafe impl<T: ?Sized + Send> Send for $mutex_name<T> {}
        unsafe impl<T: ?Sized + Send> Sync for $mutex_name<T> {}

        impl<T> $mutex_name<T> {
            $(impl_mutex!(@new_fn $is_const, $unpoisoned_mutex_ty, $cell_ty, $poison_ty);)?

            #[inline]
            pub fn into_inner(self) -> LockResult<T> {
                let poisoned = self.poison.is_poisoned();
                let data = self.data.into_inner();
                if poisoned {
                    Err(PoisonError::new(data))
                } else {
                    Ok(data)
                }
            }
        }

        impl<T: ?Sized> $mutex_name<T> {
            #[inline]
            fn guard<'a>(
                &'a self,
                inner: $unpoisoned_guard_ty<'a, ()>,
            ) -> LockResult<$guard_name<'a, T>> {
                let guard = $guard_name {
                    mutex: self,
                    _inner: inner,
                };
                if self.poison.is_poisoned() {
                    Err(PoisonError::new(guard))
                } else {
                    Ok(guard)
                }
            }

            #[inline]
            pub fn lock(&self) -> LockResult<$guard_name<'_, T>> {
                self.guard(self.inner.lock())
            }

            #[inline]
            pub fn try_lock(&self) -> TryLockResult<$guard_name<'_, T>> {
                match self.inner.try_lock() {
                    Some(inner) => self.guard(inner).map_err(TryLockError::Poisoned),
                    None => Err(TryLockError::WouldBlock),
                }
            }

            #[inline]
            pub fn try_lock_for(&self, timeout: Duration) -> TryLockResult<$guard_name<'_, T>> {
                match self.inner.try_lock_for(timeout) {
                    Some(inner) => self.guard(inner).map_err(TryLockError::Poisoned),
                    None => Err(TryLockError::WouldBlock),
                }
            }

            #[inline]
            pub fn try_lock_until(&self, timeout: Instant) -> TryLockResult<$guard_name<'_, T>> {
                match self.inner.try_lock_until(timeout) {
                    Some(inner) => self.guard(inner).map_err(TryLockError::Poisoned),
                    None => Err(TryLockError::WouldBlock),
                }
            }

            #[inline]
            pub fn is_poisoned(&self) -> bool {
                self.poison.is_poisoned()
            }

            #[inline]
            pub fn clear_poison(&self) {
                self.poison.clear();
            }

            #[inline]
            pub fn is_locked(&self) -> bool {
                self.inner.is_locked()
            }

            #[inline]
            pub fn get_mut(&mut self) -> LockResult<&mut T> {
                let data = unsafe { &mut *self.data.with_mut(|p| p as *mut T) };
                if self.poison.is_poisoned() {
                    Err(PoisonError::new(data))
                } else {
                    Ok(data)
                }
            }

            #[inline]
            pub fn raw(&self) -> &$raw_mutex_ty {
                self.inner.raw()
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
                match self.try_lock() {
                    Ok(guard) => debug.field("data", &&*guard),
                    Err(TryLockError::Poisoned(error)) => {
                        debug.field("data", &&**error.get_ref())
                    }
                    Err(TryLockError::WouldBlock) => debug.field("data", &"<locked>"),
                };
                debug.finish_non_exhaustive()
            }
        }

        pub struct $guard_name<'a, T: ?Sized> {
            mutex: &'a $mutex_name<T>,
            _inner: $unpoisoned_guard_ty<'a, ()>,
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
                if thread::panicking() {
                    self.mutex.poison.poison();
                }
            }
        }

        impl<T: ?Sized + fmt::Debug> fmt::Debug for $guard_name<'_, T> {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                fmt::Debug::fmt(&**self, f)
            }
        }
    };

    (@new_fn const, $unpoisoned_mutex_ty:ident, $cell_ty:ident, $poison_ty:ident) => {
        #[inline]
        pub const fn new(value: T) -> Self {
            Self {
                inner: $unpoisoned_mutex_ty::new(()),
                poison: $poison_ty::new(),
                data: $cell_ty::new(value),
            }
        }
    };

    (@new_fn non_const, $unpoisoned_mutex_ty:ident, $cell_ty:ident, $poison_ty:ident) => {
        #[inline]
        #[track_caller]
        pub fn new(value: T) -> Self {
            Self {
                inner: $unpoisoned_mutex_ty::new(()),
                poison: $poison_ty::new(),
                data: $cell_ty::new(value),
            }
        }
    };
}

impl_mutex!(
    /// A native mutex with standard-library-compatible poisoning.
    struct NativeMutex,
    guard: NativeMutexGuard,
    unpoisoned_mutex: NativeUnpoisonedMutex,
    unpoisoned_guard: NativeUnpoisonedMutexGuard,
    raw_mutex: NativeRawMutex,
    cell: NativeUnsafeCell,
    poison: NativePoisonState,
    const_new: const
);

#[cfg(feature = "loom")]
impl_mutex!(
    /// A Loom-tracked mutex with standard-library-compatible poisoning.
    struct LoomMutex,
    guard: LoomMutexGuard,
    unpoisoned_mutex: LoomUnpoisonedMutex,
    unpoisoned_guard: LoomUnpoisonedMutexGuard,
    raw_mutex: LoomRawMutex,
    cell: LoomUnsafeCell,
    poison: LoomPoisonState,
    const_new: non_const
);

#[cfg(not(feature = "loom"))]
pub type Mutex<T> = NativeMutex<T>;

#[cfg(not(feature = "loom"))]
pub type MutexGuard<'a, T> = NativeMutexGuard<'a, T>;

#[cfg(feature = "loom")]
pub type Mutex<T> = LoomMutex<T>;

#[cfg(feature = "loom")]
pub type MutexGuard<'a, T> = LoomMutexGuard<'a, T>;

#[inline]
pub const fn const_native_mutex<T>(value: T) -> NativeMutex<T> {
    NativeMutex::new(value)
}

#[cfg(not(feature = "loom"))]
#[inline]
pub const fn const_mutex<T>(value: T) -> Mutex<T> {
    const_native_mutex(value)
}
