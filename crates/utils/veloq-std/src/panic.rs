//! Panic and unwind primitives exposed by `veloq-std`.
//!
//! The facade deliberately keeps the public type shape identical across the
//! `std` and `no_std` configurations. A `no_std` build does not provide a
//! panic catcher: [`catch_unwind`] executes its closure directly and therefore
//! cannot intercept a panic from a target configured for unwinding.

use crate::fmt::{self, Formatter, Result as FmtResult};

/// The payload produced by a caught panic.
///
/// In a `no_std` build this is an opaque marker. It exists so generic code can
/// use the same result type, but it does not represent a recoverable payload.
#[cfg(feature = "std")]
pub struct PanicPayload(std::boxed::Box<dyn std::any::Any + Send + 'static>);

#[cfg(not(feature = "std"))]
pub struct PanicPayload(());

/// The result type returned by [`catch_unwind`].
pub type PanicResult<T> = Result<T, PanicPayload>;

/// Marks a closure as safe to pass to [`catch_unwind`].
pub struct AssertUnwindSafe<F>(F);

impl<F> AssertUnwindSafe<F> {
    /// Wraps a closure for use with [`catch_unwind`].
    #[inline]
    pub const fn new(f: F) -> Self {
        Self(f)
    }

    /// Returns the wrapped closure.
    #[inline]
    pub fn into_inner(self) -> F {
        self.0
    }
}

impl fmt::Debug for PanicPayload {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.write_str("PanicPayload(..)")
    }
}

#[cfg(feature = "std")]
impl PanicPayload {
    #[inline]
    fn from_std(payload: std::boxed::Box<dyn std::any::Any + Send + 'static>) -> Self {
        Self(payload)
    }

    #[inline]
    fn into_std(self) -> std::boxed::Box<dyn std::any::Any + Send + 'static> {
        self.0
    }

    /// Returns a reference to the underlying standard-library payload.
    #[inline]
    pub fn as_any(&self) -> &(dyn std::any::Any + Send + 'static) {
        &*self.0
    }

    /// Attempts to view the payload as `T`.
    #[inline]
    pub fn downcast_ref<T: 'static>(&self) -> Option<&T> {
        self.0.downcast_ref()
    }
}

/// Executes `f`, catching its panic only when the `std` feature is enabled.
#[inline]
pub fn catch_unwind<F, R>(f: AssertUnwindSafe<F>) -> PanicResult<R>
where
    F: FnOnce() -> R,
{
    #[cfg(feature = "std")]
    {
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(f.into_inner()))
            .map_err(PanicPayload::from_std)
    }

    #[cfg(not(feature = "std"))]
    {
        Ok(f.into_inner()())
    }
}

/// Resumes a caught panic on a `std` target.
///
/// A `no_std` target has no stable general-purpose unwind runtime contract, so
/// the payload cannot be restored. The function therefore aborts the current
/// control flow with a fixed diagnostic panic instead of silently discarding
/// the caller's error.
#[inline]
pub fn resume_unwind(payload: PanicPayload) -> ! {
    #[cfg(feature = "std")]
    {
        std::panic::resume_unwind(payload.into_std())
    }

    #[cfg(not(feature = "std"))]
    {
        let _ = payload;
        panic!("veloq-std no_std target cannot resume a panic payload")
    }
}
