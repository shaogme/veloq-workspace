pub(crate) mod raw;

use core::{
    fmt,
    ops::{Deref, DerefMut},
};

use crate::{
    cell::UnsafeCell,
    sync::{
        LockResult, PoisonError, TryLockError, TryLockResult, UnpoisonedRwLock,
        UnpoisonedRwLockReadGuard, UnpoisonedRwLockWriteGuard, poison::PoisonState,
    },
    thread,
    time::{Duration, Instant},
};

pub use self::raw::RawRwLock;

/// A reader-writer lock with poisoning behavior compatible with
/// `std::sync::RwLock`.
///
/// Only an exclusive write guard dropped during panic unwinding poisons the
/// lock. Read guards, including guards produced by [`RwLockWriteGuard::downgrade`],
/// never set the poison flag. In a `no_std` build, [`crate::thread::panicking`]
/// cannot observe a portable panic state, so automatic poisoning is unavailable.
pub struct RwLock<T: ?Sized> {
    inner: UnpoisonedRwLock<()>,
    poison: PoisonState,
    data: UnsafeCell<T>,
}

unsafe impl<T: ?Sized + Send> Send for RwLock<T> {}
unsafe impl<T: ?Sized + Send + Sync> Sync for RwLock<T> {}

impl<T> RwLock<T> {
    #[cfg(not(feature = "loom"))]
    pub const fn new(val: T) -> Self {
        Self {
            inner: UnpoisonedRwLock::new(()),
            poison: PoisonState::new(),
            data: UnsafeCell::new(val),
        }
    }

    #[cfg(feature = "loom")]
    pub fn new(val: T) -> Self {
        Self {
            inner: UnpoisonedRwLock::new(()),
            poison: PoisonState::new(),
            data: UnsafeCell::new(val),
        }
    }

    /// Consumes the lock and returns its protected value.
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
pub const fn const_rwlock<T>(val: T) -> RwLock<T> {
    RwLock::new(val)
}

impl<T: ?Sized> RwLock<T> {
    #[inline]
    fn read_guard<'a>(
        &'a self,
        inner: UnpoisonedRwLockReadGuard<'a, ()>,
    ) -> LockResult<RwLockReadGuard<'a, T>> {
        let guard = RwLockReadGuard {
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
        inner: UnpoisonedRwLockWriteGuard<'a, ()>,
    ) -> LockResult<RwLockWriteGuard<'a, T>> {
        let guard = RwLockWriteGuard {
            rwlock: self,
            inner: Some(inner),
        };
        if self.poison.is_poisoned() {
            Err(PoisonError::new(guard))
        } else {
            Ok(guard)
        }
    }

    /// Acquires a shared read guard.
    #[inline]
    pub fn read(&self) -> LockResult<RwLockReadGuard<'_, T>> {
        self.read_guard(self.inner.read())
    }

    /// Attempts to acquire a shared read guard without blocking.
    #[inline]
    pub fn try_read(&self) -> TryLockResult<RwLockReadGuard<'_, T>> {
        match self.inner.try_read() {
            Some(inner) => self.read_guard(inner).map_err(TryLockError::Poisoned),
            None => Err(TryLockError::WouldBlock),
        }
    }

    /// Attempts to acquire a shared read guard for at most `timeout`.
    #[inline]
    pub fn try_read_for(&self, timeout: Duration) -> TryLockResult<RwLockReadGuard<'_, T>> {
        match self.inner.try_read_for(timeout) {
            Some(inner) => self.read_guard(inner).map_err(TryLockError::Poisoned),
            None => Err(TryLockError::WouldBlock),
        }
    }

    /// Attempts to acquire a shared read guard until `timeout`.
    #[inline]
    pub fn try_read_until(&self, timeout: Instant) -> TryLockResult<RwLockReadGuard<'_, T>> {
        match self.inner.try_read_until(timeout) {
            Some(inner) => self.read_guard(inner).map_err(TryLockError::Poisoned),
            None => Err(TryLockError::WouldBlock),
        }
    }

    /// Acquires an exclusive write guard.
    #[inline]
    pub fn write(&self) -> LockResult<RwLockWriteGuard<'_, T>> {
        self.write_guard(self.inner.write())
    }

    /// Attempts to acquire an exclusive write guard without blocking.
    #[inline]
    pub fn try_write(&self) -> TryLockResult<RwLockWriteGuard<'_, T>> {
        match self.inner.try_write() {
            Some(inner) => self.write_guard(inner).map_err(TryLockError::Poisoned),
            None => Err(TryLockError::WouldBlock),
        }
    }

    /// Attempts to acquire an exclusive write guard for at most `timeout`.
    #[inline]
    pub fn try_write_for(&self, timeout: Duration) -> TryLockResult<RwLockWriteGuard<'_, T>> {
        match self.inner.try_write_for(timeout) {
            Some(inner) => self.write_guard(inner).map_err(TryLockError::Poisoned),
            None => Err(TryLockError::WouldBlock),
        }
    }

    /// Attempts to acquire an exclusive write guard until `timeout`.
    #[inline]
    pub fn try_write_until(&self, timeout: Instant) -> TryLockResult<RwLockWriteGuard<'_, T>> {
        match self.inner.try_write_until(timeout) {
            Some(inner) => self.write_guard(inner).map_err(TryLockError::Poisoned),
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

    /// Returns whether any reader or writer currently holds the lock.
    #[inline]
    pub fn is_locked(&self) -> bool {
        self.inner.is_locked()
    }

    /// Returns whether a writer currently holds the lock.
    #[inline]
    pub fn is_locked_exclusive(&self) -> bool {
        self.inner.is_locked_exclusive()
    }

    /// Returns the protected value through exclusive access to the lock.
    #[inline]
    pub fn get_mut(&mut self) -> LockResult<&mut T> {
        let data = unsafe { &mut *self.data.with_mut(|p| p as *mut T) };
        if self.poison.is_poisoned() {
            Err(PoisonError::new(data))
        } else {
            Ok(data)
        }
    }

    /// Returns the underlying raw reader-writer lock.
    #[inline]
    pub fn raw(&self) -> &RawRwLock {
        self.inner.raw()
    }
}

impl<T: Default> Default for RwLock<T> {
    fn default() -> Self {
        Self::new(T::default())
    }
}

impl<T> From<T> for RwLock<T> {
    fn from(value: T) -> Self {
        Self::new(value)
    }
}

impl<T: ?Sized + fmt::Debug> fmt::Debug for RwLock<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut debug = f.debug_struct("RwLock");
        match self.try_read() {
            Ok(guard) => debug.field("data", &&*guard),
            Err(TryLockError::Poisoned(error)) => debug.field("data", &&**error.get_ref()),
            Err(TryLockError::WouldBlock) => debug.field("data", &"<locked>"),
        };
        debug.finish_non_exhaustive()
    }
}

/// A shared guard returned by [`RwLock::read`] and its try-lock variants.
pub struct RwLockReadGuard<'a, T: ?Sized> {
    rwlock: &'a RwLock<T>,
    _inner: UnpoisonedRwLockReadGuard<'a, ()>,
}

unsafe impl<T: ?Sized + Sync> Sync for RwLockReadGuard<'_, T> {}

impl<'a, T: ?Sized> RwLockReadGuard<'a, T> {
    /// Returns the lock associated with this guard.
    #[inline]
    pub fn rwlock(guard: &Self) -> &'a RwLock<T> {
        guard.rwlock
    }
}

impl<T: ?Sized> Deref for RwLockReadGuard<'_, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        unsafe { &*self.rwlock.data.with(|p| p as *const T) }
    }
}

/// An exclusive guard returned by [`RwLock::write`] and its try-lock variants.
pub struct RwLockWriteGuard<'a, T: ?Sized> {
    rwlock: &'a RwLock<T>,
    inner: Option<UnpoisonedRwLockWriteGuard<'a, ()>>,
}

unsafe impl<T: ?Sized + Sync> Sync for RwLockWriteGuard<'_, T> {}

impl<'a, T: ?Sized> RwLockWriteGuard<'a, T> {
    /// Returns the lock associated with this guard.
    #[inline]
    pub fn rwlock(guard: &Self) -> &'a RwLock<T> {
        guard.rwlock
    }

    /// Downgrades an exclusive guard to a shared guard without poisoning.
    #[inline]
    pub fn downgrade(mut guard: Self) -> RwLockReadGuard<'a, T> {
        let rwlock = guard.rwlock;
        let inner = guard
            .inner
            .take()
            .expect("RwLockWriteGuard inner guard missing");
        drop(guard);
        RwLockReadGuard {
            rwlock,
            _inner: UnpoisonedRwLockWriteGuard::downgrade(inner),
        }
    }
}

impl<T: ?Sized> Deref for RwLockWriteGuard<'_, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        unsafe { &*self.rwlock.data.with(|p| p as *const T) }
    }
}

impl<T: ?Sized> DerefMut for RwLockWriteGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { &mut *self.rwlock.data.with_mut(|p| p as *mut T) }
    }
}

impl<T: ?Sized> Drop for RwLockWriteGuard<'_, T> {
    fn drop(&mut self) {
        if self.inner.is_some() && thread::panicking() {
            self.rwlock.poison.poison();
        }
    }
}

impl<T: ?Sized + fmt::Debug> fmt::Debug for RwLockReadGuard<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&**self, f)
    }
}

impl<T: ?Sized + fmt::Display> fmt::Display for RwLockReadGuard<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&**self, f)
    }
}

impl<T: ?Sized + fmt::Debug> fmt::Debug for RwLockWriteGuard<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&**self, f)
    }
}

impl<T: ?Sized + fmt::Display> fmt::Display for RwLockWriteGuard<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&**self, f)
    }
}
