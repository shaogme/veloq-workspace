pub mod raw;

use crate::{
    string::String,
    thread::{Builder, Systerm, ThreadError},
};
use raw::{RawScope, RawScopedJoinHandle, scope as raw_scope};

/// 结构化并发的作用域封装结构体
#[repr(transparent)]
pub struct Scope<'scope, 'env> {
    pub(crate) inner: RawScope<'scope, 'env, Systerm>,
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

    /// 产生一个绑定了当前作用域的 `ScopeBuilder`。
    pub fn builder(&'scope self) -> ScopeBuilder<'scope, 'env> {
        ScopeBuilder {
            scope: self,
            builder: Builder::new(),
        }
    }
}

/// 绑定的作用域线程工厂，可用于配置新线程的属性并在作用域内启动。
#[must_use = "must eventually spawn the thread"]
pub struct ScopeBuilder<'scope, 'env> {
    scope: &'scope Scope<'scope, 'env>,
    builder: Builder,
}

impl<'scope, 'env> crate::fmt::Debug for ScopeBuilder<'scope, 'env> {
    fn fmt(&self, f: &mut crate::fmt::Formatter<'_>) -> crate::fmt::Result {
        f.debug_struct("ScopeBuilder")
            .field("builder", &self.builder)
            .finish()
    }
}

impl<'scope, 'env> ScopeBuilder<'scope, 'env> {
    /// 设置新线程的名称。
    pub fn name(mut self, name: String) -> Self {
        self.builder = self.builder.name(name);
        self
    }

    /// 设置新线程的栈大小（字节）。
    pub fn stack_size(mut self, size: usize) -> Self {
        self.builder = self.builder.stack_size(size);
        self
    }

    /// 使用配置在当前作用域内启动一个新线程。
    pub fn spawn<F, R>(self, f: F) -> Result<ScopedJoinHandle<'scope, R>, ThreadError>
    where
        F: FnOnce() -> R + Send + 'env,
        R: Send + 'env,
    {
        self.scope
            .inner
            .spawn_with(self.builder.name, self.builder.stack_size, f)
            .map(|inner| ScopedJoinHandle { inner })
            .map_err(ThreadError::new)
    }
}

/// 作用域内生成的线程加入句柄的封装结构体
pub struct ScopedJoinHandle<'scope, R: Send + 'scope> {
    pub(crate) inner: RawScopedJoinHandle<'scope, Systerm, R>,
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
    raw_scope::<Systerm, _, _>(|raw_scope| {
        let wrapper = unsafe {
            &*(raw_scope as *const RawScope<'_, 'env, Systerm> as *const Scope<'_, 'env>)
        };
        f(wrapper)
    })
}
