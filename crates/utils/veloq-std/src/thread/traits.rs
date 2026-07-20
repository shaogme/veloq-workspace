use core::num::NonZeroUsize;

use crate::{
    error::Error,
    thread::{
        AbortedError, ThreadErrorKind, ThreadId,
        scope::raw::{RawScope, scope},
    },
    time::Duration,
};

/// 原始线程错误接口，供平台特有错误实现
pub trait RawThreadErrorTrait: Error + Send + Sync + 'static {
    /// 获取线程错误种类
    fn kind(&self) -> ThreadErrorKind;

    /// 从原始 panic payload 构造错误实例
    fn from_panic(payload: super::platform::ThreadPanicPayload) -> Self;

    /// 提取原始的 panic payload 并在原处留下 None
    fn take_panic(&mut self) -> super::platform::ThreadPanicPayload {
        Default::default()
    }
}

/// 原始线程加入句柄的抽象接口
pub trait RawJoinHandleTrait<T: Send>: Send + Sync {
    /// 错误类型
    type Error: RawThreadErrorTrait;

    /// 等待线程执行结束 (Join)
    fn join(self) -> Result<T, Self::Error>;

    /// 中止 (Abort) 线程的执行
    fn abort(&self) -> Result<(), Self::Error>;
}

/// 平台线程实现的统一抽象接口
pub trait PlatformImpl: Sized {
    /// 错误类型
    type Error: RawThreadErrorTrait;

    /// 原始加入句柄类型，带有生命周期约束
    type RawJoinHandle<'a, T: Send>: RawJoinHandleTrait<T, Error = Self::Error>
    where
        T: 'a;

    /// 产生一个新线程，并执行传入的 `f` 闭包
    fn spawn<'a, F, T>(f: F) -> Result<Self::RawJoinHandle<'a, T>, Self::Error>
    where
        F: FnOnce() -> T + Send + 'a,
        T: Send + 'a;

    /// 让出当前线程的 CPU 执行时间片。
    ///
    /// 如果成功让出或切换到了另一个线程，返回 `Ok(true)`；否则返回 `Ok(false)`。
    /// 如果检测到当前线程已被中止，则返回 `Err(AbortedError)`。
    fn yield_now() -> Result<bool, AbortedError>;

    /// 创建一个结构化并发作用域，并在其中执行闭包 `f`。
    fn scope<'env, F, R>(f: F) -> R
    where
        F: for<'scope> FnOnce(&'scope RawScope<'scope, 'env, Self>) -> R,
    {
        scope::<'env, Self, F, R>(f)
    }

    /// 使当前线程睡眠指定的时长。
    ///
    /// 如果检测到当前线程已被中止，则返回 `Err(AbortedError)`。
    fn sleep(dur: Duration) -> Result<(), AbortedError>;

    /// 获取当前线程的 ID
    fn current_id() -> ThreadId;

    /// 获取当前系统的可用并行度 (逻辑 CPU 核心数)
    fn available_parallelism() -> Result<NonZeroUsize, Self::Error>;
}
