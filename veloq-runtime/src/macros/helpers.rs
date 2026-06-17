use crate::error::Result;
use crate::scope::{AsyncScope, LocalAsyncScope};
use std::ops::AsyncFnOnce;

#[doc(hidden)]
pub fn _constrain<'g, R, F, TExtra>(f: F) -> F
where
    F: for<'r> AsyncFnOnce(&'r AsyncScope<'r, 'g, TExtra>) -> R,
{
    f
}

#[doc(hidden)]
pub fn _constrain_local<'g, R, F, TExtra>(f: F) -> F
where
    F: for<'r> AsyncFnOnce(&'r LocalAsyncScope<'r, 'g, TExtra>) -> R,
{
    f
}

#[doc(hidden)]
pub fn _constrain_result<T>(r: Result<T>) -> Result<T> {
    r
}
