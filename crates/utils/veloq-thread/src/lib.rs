#![cfg_attr(not(feature = "std"), no_std)]

extern crate alloc;

mod traits;
pub use traits::{PlatformImpl, ThreadParkerTrait};

mod platform;

pub use platform::{Platform, Thread, ThreadError};

mod scope;

pub type Scope<'scope, 'env> = scope::Scope<'scope, 'env, Platform>;
pub type ScopedJoinHandle<'scope, T> = scope::ScopedJoinHandle<'scope, Platform, T>;

/// 创建一个结构化并发作用域，并在其中执行闭包 `f`。
pub fn scope<'env, F, T>(f: F) -> T
where
    F: for<'scope> FnOnce(&'scope Scope<'scope, 'env>) -> T,
{
    Platform::scope(f)
}

/// 让出当前线程的 CPU 执行时间片。
///
/// 如果成功让出或切换到了另一个线程，返回 `true`；否则返回 `false`。
pub fn yield_now() -> bool {
    Platform::yield_now()
}
