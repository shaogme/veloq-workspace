use veloq_std::{future::Future, ops::AsyncFnOnce};

use crate::{
    error::Result,
    outcome::{IntoOutcome, Outcome},
    scope::{AsyncScope, GenericAsyncScope, LocalAsyncScope, ScopeExitGuard, ScopeProvider},
    task::ScopeStorage,
    utils::ownership::Ownership,
};

/// 把 `async |s| ..` 字面量的参数类型钉成一个作用域引用。
///
/// 闭包在宏体内被立刻调用，所以除 `'r` 之外的生命周期可以自由推导 —— 这也正是宏无法直接
/// 转发给 [`RuntimeCtx::scope`](crate::runtime::RuntimeCtx::scope) 的原因，详见 `scope!`。
#[doc(hidden)]
pub fn _constrain<'g, 'env, O, F, TExtra>(f: F) -> F
where
    O: IntoOutcome,
    F: for<'r> AsyncFnOnce(&'r AsyncScope<'r, 'g, 'env, TExtra>) -> O,
{
    f
}

#[doc(hidden)]
pub fn _constrain_local<'g, 'env, O, F, TExtra>(f: F) -> F
where
    O: IntoOutcome,
    F: for<'r> AsyncFnOnce(&'r LocalAsyncScope<'r, 'g, 'env, TExtra>) -> O,
{
    f
}

/// 钉住宏展开结果的错误类型（否则 `Ok(res)` 里的 `E` 无从推导）。
#[doc(hidden)]
pub fn _constrain_result<T>(r: Result<T>) -> Result<T> {
    r
}

/// Runs a scope body and keeps cancellation/join in one unwind-safe guard.
#[doc(hidden)]
pub async fn run_scope_eval<
    'rt,
    'scope,
    'env: 'scope,
    S: ScopeStorage,
    O: Ownership + 'static,
    TExtra,
    Fut: Future<Output = Body>,
    Body: IntoOutcome,
>(
    scope: &GenericAsyncScope<'rt, 'scope, 'env, S, O, TExtra>,
    fut: Fut,
) -> Result<Outcome<Body::Output, Body::Error>> {
    let mut guard = ScopeExitGuard::new(scope);
    let outcome = fut.await.into_outcome();

    if matches!(&outcome, Outcome::Err(_)) {
        scope.completion().cancel();
    }

    scope.wait_all().await?;
    guard.disarm();
    Ok(outcome)
}
