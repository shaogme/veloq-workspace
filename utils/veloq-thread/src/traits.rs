use crate::scope::{Scope, scope_in};

/// 线程 Parker 的抽象接口
pub trait ThreadParkerTrait: Send + Sync {
    /// 创建一个新的 Parker
    fn new() -> Self;

    /// 阻塞当前线程
    fn park(&self);

    /// 唤醒被阻塞的线程
    fn unpark(&self);
}

/// 平台线程实现的统一抽象接口
pub trait PlatformImpl: Sized {
    /// 错误类型
    type Error: core::error::Error;

    /// Parker 类型
    type Parker: ThreadParkerTrait;

    /// 线程关联类型，带有生命周期约束
    type Thread<'a>;

    /// 产生一个新线程，并执行传入的 `f` 闭包
    fn spawn<'a, F>(f: F) -> Result<Self::Thread<'a>, Self::Error>
    where
        F: FnOnce() + Send + 'a;

    /// 等待线程执行结束 (Join)
    fn join<'a>(thread: Self::Thread<'a>) -> Result<(), Self::Error>;

    /// 中止 (Abort) 线程的执行
    fn abort<'a>(thread: &Self::Thread<'a>) -> Result<(), Self::Error>;

    /// 让出当前线程 of CPU 执行时间片。
    fn yield_now();

    /// 创建一个结构化并发作用域，并在其中执行闭包 `f`。
    fn scope<'env, F, R>(f: F) -> R
    where
        F: for<'scope> FnOnce(&'scope Scope<'scope, 'env, Self>) -> R,
    {
        scope_in::<'env, Self, F, R>(f)
    }
}
