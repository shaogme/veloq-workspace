#[cfg(feature = "std")]
pub use std::panic::*;

#[cfg(not(feature = "std"))]
pub use core::panic::*;

/// 统一的 catch_unwind 封装，如果是 std feature 则捕获 panic，否则直接执行。
#[inline]
pub fn catch_unwind_safe<F, R>(f: F) -> Result<R, Option<Box<dyn core::any::Any + Send + 'static>>>
where
    F: FnOnce() -> R + Send,
{
    #[cfg(feature = "std")]
    {
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(f)).map_err(Some)
    }
    #[cfg(not(feature = "std"))]
    {
        let r = f();
        Ok(r)
    }
}
