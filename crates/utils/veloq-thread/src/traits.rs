#[cfg(feature = "std")]
use alloc::boxed::Box;
#[cfg(feature = "std")]
use core::any::Any;

use crate::{
    AbortedError, ThreadErrorKind,
    scope::raw::{RawScope, scope},
};
use core::{error::Error, sync::atomic::AtomicU32, time::Duration};

/// 原始线程错误接口，供平台特有错误实现
pub trait RawThreadErrorTrait: Error + Send + Sync + 'static {
    /// 获取线程错误种类
    fn kind(&self) -> ThreadErrorKind;

    /// 从原始 panic payload 构造错误实例
    #[cfg(feature = "std")]
    fn from_panic(payload: Box<dyn Any + Send + 'static>) -> Self;

    /// 提取原始的 panic payload 并在原处留下 None
    #[cfg(feature = "std")]
    fn take_panic(&mut self) -> Option<Box<dyn Any + Send + 'static>> {
        None
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

    /// 让出当前线程 of CPU 执行时间片。
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

    /// 在指定的 `AtomicU32` 地址上等待，直到其值不再等于 `expected`
    fn wait_on_address(address: &AtomicU32, expected: u32);

    /// 在指定的 `AtomicU32` 地址上等待，直到其值不再等于 `expected`，或者超时
    /// 返回 `true` 表示超时，`false` 表示未超时（被唤醒或值已改变）
    fn wait_on_address_timeout(
        address: &AtomicU32,
        expected: u32,
        timeout: Option<Duration>,
    ) -> bool;

    /// 唤醒在指定的 `AtomicU32` 地址上等待 of 线程
    fn wake_by_address(address: &AtomicU32);

    /// 唤醒所有在指定的 `AtomicU32` 地址上等待的线程
    fn wake_all_by_address(address: &AtomicU32);

    /// 使当前线程睡眠指定的时长。
    ///
    /// 如果检测到当前线程已被中止，则返回 `Err(AbortedError)`。
    fn sleep(dur: Duration) -> Result<(), AbortedError>;
}
