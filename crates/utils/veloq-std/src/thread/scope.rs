pub mod raw;

use crate::thread::{Platform, ThreadError};
use raw::{RawScope, RawScopedJoinHandle, scope as raw_scope};

/// 结构化并发的作用域封装结构体
#[repr(transparent)]
pub struct Scope<'scope, 'env> {
    pub(crate) inner: RawScope<'scope, 'env, Platform>,
}

impl<'scope, 'env> Scope<'scope, 'env> {
    /// 检查当前作用域是否已被取消
    pub fn is_cancelled(&self) -> bool {
        self.inner.is_cancelled()
    }

    /// 在当前作用域内生成一个新线程并执行闭包 `f`，返回 `ThreadError` 错误类型
    pub fn spawn<F, R>(&'scope self, f: F) -> Result<ScopedJoinHandle<'scope, R>, ThreadError>
    where
        F: FnOnce() -> R + Send + 'env,
        R: Send + 'env,
    {
        self.inner
            .spawn(f)
            .map(|inner| ScopedJoinHandle { inner })
            .map_err(ThreadError::new)
    }
}

/// 作用域内生成的线程加入句柄的封装结构体
pub struct ScopedJoinHandle<'scope, R: Send + 'scope> {
    pub(crate) inner: RawScopedJoinHandle<'scope, Platform, R>,
}

unsafe impl<'scope, R: Send + 'scope> Send for ScopedJoinHandle<'scope, R> {}
unsafe impl<'scope, R: Send + Sync + 'scope> Sync for ScopedJoinHandle<'scope, R> {}

impl<'scope, R: Send + 'scope> ScopedJoinHandle<'scope, R> {
    /// 等待子线程执行结束并返回其结果，返回 `ThreadError` 错误类型
    pub fn join(self) -> Result<R, ThreadError> {
        self.inner.join().map_err(ThreadError::new)
    }

    /// 中止子线程的执行，返回 `ThreadError` 错误类型
    pub fn abort(&self) -> Result<(), ThreadError> {
        self.inner.abort().map_err(ThreadError::new)
    }
}

/// 创建一个结构化并发作用域，并在其中执行闭包 `f`。
pub fn scope<'env, F, T>(f: F) -> T
where
    F: for<'scope> FnOnce(&'scope Scope<'scope, 'env>) -> T,
{
    raw_scope::<Platform, _, _>(|raw_scope| {
        let wrapper = unsafe {
            &*(raw_scope as *const RawScope<'_, 'env, Platform> as *const Scope<'_, 'env>)
        };
        f(wrapper)
    })
}
