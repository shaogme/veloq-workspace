pub mod raw;

use core::{
    fmt,
    ops::{Deref, DerefMut},
};

use crate::{
    cell::UnsafeCell,
    sync::{
        LockResult, PoisonError, TryLockError, TryLockResult, UnpoisonedMutex,
        UnpoisonedMutexGuard, poison::PoisonState,
    },
    thread,
    time::{Duration, Instant},
};

use self::raw::RawMutex;

/// A mutex with poisoning behavior compatible with `std::sync::Mutex`.
///
/// The `std` and `loom` feature configurations observe panic unwinding and set
/// the poison flag when a mutable guard is dropped during a panic. In a
/// `no_std` build, [`crate::thread::panicking`] cannot observe a portable panic
/// state, so the flag is not set automatically during unwinding.
pub struct Mutex<T: ?Sized> {
    inner: UnpoisonedMutex<()>,
    poison: PoisonState,
    data: UnsafeCell<T>,
}

unsafe impl<T: ?Sized + Send> Send for Mutex<T> {}
unsafe impl<T: ?Sized + Send> Sync for Mutex<T> {}

impl<T> Mutex<T> {
    #[cfg(not(feature = "loom"))]
    pub const fn new(val: T) -> Self {
        Self {
            inner: UnpoisonedMutex::new(()),
            poison: PoisonState::new(),
            data: UnsafeCell::new(val),
        }
    }

    #[cfg(feature = "loom")]
    pub fn new(val: T) -> Self {
        Self {
            inner: UnpoisonedMutex::new(()),
            poison: PoisonState::new(),
            data: UnsafeCell::new(val),
        }
    }

    /// Consumes the mutex and returns its protected value.
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

#[cfg(not(feature = "loom"))]
pub const fn const_mutex<T>(val: T) -> Mutex<T> {
    Mutex::new(val)
}

impl<T: ?Sized> Mutex<T> {
    #[inline]
    fn guard<'a>(&'a self, inner: UnpoisonedMutexGuard<'a, ()>) -> LockResult<MutexGuard<'a, T>> {
        let guard = MutexGuard {
            mutex: self,
            _inner: inner,
        };
        if self.poison.is_poisoned() {
            Err(PoisonError::new(guard))
        } else {
            Ok(guard)
        }
    }

    /// Acquires the mutex, returning a poisoned guard when necessary.
    #[inline]
    pub fn lock(&self) -> LockResult<MutexGuard<'_, T>> {
        self.guard(self.inner.lock())
    }

    /// Attempts to acquire the mutex without blocking.
    #[inline]
    pub fn try_lock(&self) -> TryLockResult<MutexGuard<'_, T>> {
        match self.inner.try_lock() {
            Some(inner) => self.guard(inner).map_err(TryLockError::Poisoned),
            None => Err(TryLockError::WouldBlock),
        }
    }

    /// Attempts to acquire the mutex for at most `timeout`.
    #[inline]
    pub fn try_lock_for(&self, timeout: Duration) -> TryLockResult<MutexGuard<'_, T>> {
        match self.inner.try_lock_for(timeout) {
            Some(inner) => self.guard(inner).map_err(TryLockError::Poisoned),
            None => Err(TryLockError::WouldBlock),
        }
    }

    /// Attempts to acquire the mutex until `timeout`.
    #[inline]
    pub fn try_lock_until(&self, timeout: Instant) -> TryLockResult<MutexGuard<'_, T>> {
        match self.inner.try_lock_until(timeout) {
            Some(inner) => self.guard(inner).map_err(TryLockError::Poisoned),
            None => Err(TryLockError::WouldBlock),
        }
    }

    /// Returns a concurrent snapshot of the poison state.
    #[inline]
    pub fn is_poisoned(&self) -> bool {
        self.poison.is_poisoned()
    }

    /// Clears the poison flag after the protected data has been repaired.
    #[inline]
    pub fn clear_poison(&self) {
        self.poison.clear();
    }

    /// Returns whether the underlying mutex is currently locked.
    #[inline]
    pub fn is_locked(&self) -> bool {
        self.inner.is_locked()
    }

    /// Returns the protected value through exclusive access to the mutex.
    #[inline]
    pub fn get_mut(&mut self) -> LockResult<&mut T> {
        let data = unsafe { &mut *self.data.with_mut(|p| p as *mut T) };
        if self.poison.is_poisoned() {
            Err(PoisonError::new(data))
        } else {
            Ok(data)
        }
    }

    /// Returns the underlying raw mutex.
    #[inline]
    pub fn raw(&self) -> &RawMutex {
        self.inner.raw()
    }
}

impl<T: Default> Default for Mutex<T> {
    fn default() -> Self {
        Self::new(T::default())
    }
}

impl<T> From<T> for Mutex<T> {
    fn from(value: T) -> Self {
        Self::new(value)
    }
}

impl<T: ?Sized + fmt::Debug> fmt::Debug for Mutex<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut debug = f.debug_struct("Mutex");
        match self.try_lock() {
            Ok(guard) => debug.field("data", &&*guard),
            Err(TryLockError::Poisoned(error)) => debug.field("data", &&**error.get_ref()),
            Err(TryLockError::WouldBlock) => debug.field("data", &"<locked>"),
        };
        debug.finish_non_exhaustive()
    }
}

/// A RAII guard returned by [`Mutex::lock`] and its try-lock variants.
pub struct MutexGuard<'a, T: ?Sized> {
    mutex: &'a Mutex<T>,
    _inner: UnpoisonedMutexGuard<'a, ()>,
}

unsafe impl<T: ?Sized + Sync> Sync for MutexGuard<'_, T> {}

impl<'a, T: ?Sized> MutexGuard<'a, T> {
    /// Returns the mutex associated with this guard.
    #[inline]
    pub fn mutex(guard: &Self) -> &'a Mutex<T> {
        guard.mutex
    }
}

impl<T: ?Sized> Deref for MutexGuard<'_, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        unsafe { &*self.mutex.data.with(|p| p as *const T) }
    }
}

impl<T: ?Sized> DerefMut for MutexGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { &mut *self.mutex.data.with_mut(|p| p as *mut T) }
    }
}

impl<T: ?Sized> Drop for MutexGuard<'_, T> {
    fn drop(&mut self) {
        if thread::panicking() {
            self.mutex.poison.poison();
        }
    }
}

impl<T: ?Sized + fmt::Debug> fmt::Debug for MutexGuard<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&**self, f)
    }
}
