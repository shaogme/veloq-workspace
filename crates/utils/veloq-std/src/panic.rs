#[cfg(feature = "std")]
pub use std::panic::*;

#[cfg(feature = "std")]
use std::panic::{AssertUnwindSafe as StdAssertUnwindSafe, catch_unwind as std_catch_unwind};

#[cfg(not(feature = "std"))]
pub use core::panic::*;

#[cfg(feature = "std")]
use crate::boxed::Box;

/// 统一的 catch_unwind 封装，如果是 std feature 则捕获 panic，否则直接执行。
#[inline]
#[cfg(feature = "std")]
pub fn catch_unwind_safe<F, R>(f: F) -> Result<R, Option<Box<dyn core::any::Any + Send + 'static>>>
where
    F: FnOnce() -> R + Send,
{
    #[cfg(feature = "std")]
    {
        std_catch_unwind(StdAssertUnwindSafe(f)).map_err(Some)
    }
}

#[inline]
#[cfg(not(feature = "std"))]
pub fn catch_unwind_safe<F, R>(f: F) -> Result<R, core::convert::Infallible>
where
    F: FnOnce() -> R + Send,
{
    let r = f();
    Ok(r)
}
