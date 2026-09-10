use core::fmt;

use crate::sync::atomic::{AtomicBool, Ordering};

/// A value returned when a lock was acquired after its protected data may have
/// been left inconsistent by a panic.
pub struct PoisonError<T> {
    data: T,
}

impl<T> PoisonError<T> {
    pub fn new(data: T) -> Self {
        Self { data }
    }

    /// Consumes the error and returns the lock guard or value it contains.
    pub fn into_inner(self) -> T {
        self.data
    }

    /// Returns a shared reference to the lock guard or value it contains.
    pub fn get_ref(&self) -> &T {
        &self.data
    }

    /// Returns a mutable reference to the lock guard or value it contains.
    pub fn get_mut(&mut self) -> &mut T {
        &mut self.data
    }
}

impl<T> fmt::Debug for PoisonError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PoisonError").finish_non_exhaustive()
    }
}

impl<T> fmt::Display for PoisonError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("lock poisoned")
    }
}

impl<T> core::error::Error for PoisonError<T> {}

/// The result of acquiring a lock, including the guard when the lock is
/// poisoned.
pub type LockResult<T> = Result<T, PoisonError<T>>;

/// The result of attempting to acquire a lock without blocking.
pub type TryLockResult<T> = Result<T, TryLockError<T>>;

/// An error returned by a non-blocking or timed lock operation.
pub enum TryLockError<T> {
    /// The lock could not be acquired before the operation returned.
    WouldBlock,
    /// The lock was acquired, but its poison flag is set.
    Poisoned(PoisonError<T>),
}

impl<T> TryLockError<T> {
    /// Returns the guard or value carried by a poisoned-lock error.
    pub fn into_inner(self) -> Option<T> {
        match self {
            Self::WouldBlock => None,
            Self::Poisoned(error) => Some(error.into_inner()),
        }
    }
}

impl<T> fmt::Debug for TryLockError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::WouldBlock => f.write_str("WouldBlock"),
            Self::Poisoned(_) => f.write_str("Poisoned(..)"),
        }
    }
}

impl<T> fmt::Display for TryLockError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::WouldBlock => f.write_str("try-lock operation would block"),
            Self::Poisoned(error) => error.fmt(f),
        }
    }
}

impl<T> core::error::Error for TryLockError<T> {}

pub(crate) struct PoisonState {
    poisoned: AtomicBool,
}

impl PoisonState {
    #[cfg(not(feature = "loom"))]
    pub(crate) const fn new() -> Self {
        Self {
            poisoned: AtomicBool::new(false),
        }
    }

    #[cfg(feature = "loom")]
    pub(crate) fn new() -> Self {
        Self {
            poisoned: AtomicBool::new(false),
        }
    }

    #[inline]
    pub(crate) fn is_poisoned(&self) -> bool {
        self.poisoned.load(Ordering::Acquire)
    }

    #[inline]
    pub(crate) fn poison(&self) {
        self.poisoned.store(true, Ordering::Release);
    }

    #[inline]
    pub(crate) fn clear(&self) {
        self.poisoned.store(false, Ordering::Release);
    }
}
