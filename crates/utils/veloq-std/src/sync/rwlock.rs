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
        unpoisoned_rwlock::{
            NativeUnpoisonedRwLock, NativeUnpoisonedRwLockReadGuard,
            NativeUnpoisonedRwLockWriteGuard,
        },
    },
    thread,
    time::{Duration, Instant},
};

use self::raw::NativeRawRwLock;

#[cfg(feature = "loom")]
use crate::{
    cell::LoomUnsafeCell,
    sync::unpoisoned_rwlock::{
        LoomUnpoisonedRwLock, LoomUnpoisonedRwLockReadGuard, LoomUnpoisonedRwLockWriteGuard,
    },
};

#[cfg(feature = "loom")]
use self::raw::LoomRawRwLock;

#[cfg(feature = "loom")]
use crate::sync::poison::LoomPoisonState;

pub use self::raw::RawRwLock;

/// A reader-writer lock with poisoning behavior compatible with
/// `std::sync::RwLock`.
///
/// The native implementation is always available. With the `loom` feature,
/// the default [`RwLock`] alias selects the Loom implementation so accesses to
/// the protected data are tracked by Loom.
macro_rules! impl_rwlock {
    (
        $(#[$meta:meta])*
        struct $lock_name:ident,
        read_guard: $read_guard_name:ident,
        write_guard: $write_guard_name:ident,
        unpoisoned_lock: $unpoisoned_lock_ty:ident,
        unpoisoned_read_guard: $unpoisoned_read_guard_ty:ident,
        unpoisoned_write_guard: $unpoisoned_write_guard_ty:ident,
        raw_lock: $raw_lock_ty:ident,
        cell: $cell_ty:ident,
        poison: $poison_ty:ident,
        $(const_new: $is_const:ident)?
    ) => {
        $(#[$meta])*
        pub struct $lock_name<T: ?Sized> {
            inner: $unpoisoned_lock_ty<()>,
            poison: $poison_ty,
            data: $cell_ty<T>,
        }

        unsafe impl<T: ?Sized + Send> Send for $lock_name<T> {}
        unsafe impl<T: ?Sized + Send + Sync> Sync for $lock_name<T> {}

        impl<T> $lock_name<T> {
            $(impl_rwlock!(@new_fn $is_const, $unpoisoned_lock_ty, $cell_ty, $poison_ty);)?

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

        impl<T: ?Sized> $lock_name<T> {
            #[inline]
            fn read_guard<'a>(
                &'a self,
                inner: $unpoisoned_read_guard_ty<'a, ()>,
            ) -> LockResult<$read_guard_name<'a, T>> {
                let guard = $read_guard_name {
                    rwlock: self,
                    _inner: inner,
                };
                if self.poison.is_poisoned() {
                    Err(PoisonError::new(guard))
                } else {
                    Ok(guard)
                }
            }

            #[inline]
            fn write_guard<'a>(
                &'a self,
                inner: $unpoisoned_write_guard_ty<'a, ()>,
            ) -> LockResult<$write_guard_name<'a, T>> {
                let guard = $write_guard_name {
                    rwlock: self,
                    inner: Some(inner),
                };
                if self.poison.is_poisoned() {
                    Err(PoisonError::new(guard))
                } else {
                    Ok(guard)
                }
            }

            #[inline]
            pub fn read(&self) -> LockResult<$read_guard_name<'_, T>> {
                self.read_guard(self.inner.read())
            }

            #[inline]
            pub fn try_read(&self) -> TryLockResult<$read_guard_name<'_, T>> {
                match self.inner.try_read() {
                    Some(inner) => self.read_guard(inner).map_err(TryLockError::Poisoned),
                    None => Err(TryLockError::WouldBlock),
                }
            }

            #[inline]
            pub fn try_read_for(&self, timeout: Duration) -> TryLockResult<$read_guard_name<'_, T>> {
                match self.inner.try_read_for(timeout) {
                    Some(inner) => self.read_guard(inner).map_err(TryLockError::Poisoned),
                    None => Err(TryLockError::WouldBlock),
                }
            }

            #[inline]
            pub fn try_read_until(&self, timeout: Instant) -> TryLockResult<$read_guard_name<'_, T>> {
                match self.inner.try_read_until(timeout) {
                    Some(inner) => self.read_guard(inner).map_err(TryLockError::Poisoned),
                    None => Err(TryLockError::WouldBlock),
                }
            }

            #[inline]
            pub fn write(&self) -> LockResult<$write_guard_name<'_, T>> {
                self.write_guard(self.inner.write())
            }

            #[inline]
            pub fn try_write(&self) -> TryLockResult<$write_guard_name<'_, T>> {
                match self.inner.try_write() {
                    Some(inner) => self.write_guard(inner).map_err(TryLockError::Poisoned),
                    None => Err(TryLockError::WouldBlock),
                }
            }

            #[inline]
            pub fn try_write_for(
                &self,
                timeout: Duration,
            ) -> TryLockResult<$write_guard_name<'_, T>> {
                match self.inner.try_write_for(timeout) {
                    Some(inner) => self.write_guard(inner).map_err(TryLockError::Poisoned),
                    None => Err(TryLockError::WouldBlock),
                }
            }

            #[inline]
            pub fn try_write_until(
                &self,
                timeout: Instant,
            ) -> TryLockResult<$write_guard_name<'_, T>> {
                match self.inner.try_write_until(timeout) {
                    Some(inner) => self.write_guard(inner).map_err(TryLockError::Poisoned),
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
            pub fn is_locked_exclusive(&self) -> bool {
                self.inner.is_locked_exclusive()
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
            pub fn raw(&self) -> &$raw_lock_ty {
                self.inner.raw()
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
                match self.try_read() {
                    Ok(guard) => debug.field("data", &&*guard),
                    Err(TryLockError::Poisoned(error)) => {
                        debug.field("data", &&**error.get_ref())
                    }
                    Err(TryLockError::WouldBlock) => debug.field("data", &"<locked>"),
                };
                debug.finish_non_exhaustive()
            }
        }

        pub struct $read_guard_name<'a, T: ?Sized> {
            rwlock: &'a $lock_name<T>,
            _inner: $unpoisoned_read_guard_ty<'a, ()>,
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

        pub struct $write_guard_name<'a, T: ?Sized> {
            rwlock: &'a $lock_name<T>,
            inner: Option<$unpoisoned_write_guard_ty<'a, ()>>,
        }

        unsafe impl<T: ?Sized + Sync> Sync for $write_guard_name<'_, T> {}

        impl<'a, T: ?Sized> $write_guard_name<'a, T> {
            #[inline]
            pub fn rwlock(guard: &Self) -> &'a $lock_name<T> {
                guard.rwlock
            }

            /// Downgrades an exclusive guard to a shared guard without
            /// poisoning the lock while the guard is being transformed.
            #[inline]
            pub fn downgrade(mut guard: Self) -> $read_guard_name<'a, T> {
                let rwlock = guard.rwlock;
                let inner = guard
                    .inner
                    .take()
                    .expect("RwLockWriteGuard inner guard missing");
                drop(guard);
                $read_guard_name {
                    rwlock,
                    _inner: $unpoisoned_write_guard_ty::downgrade(inner),
                }
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
                if self.inner.is_some() && thread::panicking() {
                    self.rwlock.poison.poison();
                }
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

    (@new_fn const, $unpoisoned_lock_ty:ident, $cell_ty:ident, $poison_ty:ident) => {
        #[inline]
        pub const fn new(value: T) -> Self {
            Self {
                inner: $unpoisoned_lock_ty::new(()),
                poison: $poison_ty::new(),
                data: $cell_ty::new(value),
            }
        }
    };

    (@new_fn non_const, $unpoisoned_lock_ty:ident, $cell_ty:ident, $poison_ty:ident) => {
        #[inline]
        #[track_caller]
        pub fn new(value: T) -> Self {
            Self {
                inner: $unpoisoned_lock_ty::new(()),
                poison: $poison_ty::new(),
                data: $cell_ty::new(value),
            }
        }
    };
}

impl_rwlock!(
    /// A native reader-writer lock with standard-library-compatible poisoning.
    struct NativeRwLock,
    read_guard: NativeRwLockReadGuard,
    write_guard: NativeRwLockWriteGuard,
    unpoisoned_lock: NativeUnpoisonedRwLock,
    unpoisoned_read_guard: NativeUnpoisonedRwLockReadGuard,
    unpoisoned_write_guard: NativeUnpoisonedRwLockWriteGuard,
    raw_lock: NativeRawRwLock,
    cell: NativeUnsafeCell,
    poison: NativePoisonState,
    const_new: const
);

#[cfg(feature = "loom")]
impl_rwlock!(
    /// A Loom-tracked reader-writer lock with standard-library-compatible poisoning.
    struct LoomRwLock,
    read_guard: LoomRwLockReadGuard,
    write_guard: LoomRwLockWriteGuard,
    unpoisoned_lock: LoomUnpoisonedRwLock,
    unpoisoned_read_guard: LoomUnpoisonedRwLockReadGuard,
    unpoisoned_write_guard: LoomUnpoisonedRwLockWriteGuard,
    raw_lock: LoomRawRwLock,
    cell: LoomUnsafeCell,
    poison: LoomPoisonState,
    const_new: non_const
);

#[cfg(not(feature = "loom"))]
pub type RwLock<T> = NativeRwLock<T>;

#[cfg(not(feature = "loom"))]
pub type RwLockReadGuard<'a, T> = NativeRwLockReadGuard<'a, T>;

#[cfg(not(feature = "loom"))]
pub type RwLockWriteGuard<'a, T> = NativeRwLockWriteGuard<'a, T>;

#[cfg(feature = "loom")]
pub type RwLock<T> = LoomRwLock<T>;

#[cfg(feature = "loom")]
pub type RwLockReadGuard<'a, T> = LoomRwLockReadGuard<'a, T>;

#[cfg(feature = "loom")]
pub type RwLockWriteGuard<'a, T> = LoomRwLockWriteGuard<'a, T>;

#[inline]
pub const fn const_native_rwlock<T>(value: T) -> NativeRwLock<T> {
    NativeRwLock::new(value)
}

#[cfg(not(feature = "loom"))]
#[inline]
pub const fn const_rwlock<T>(value: T) -> RwLock<T> {
    const_native_rwlock(value)
}
