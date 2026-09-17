use veloq_std::{future::Future, ops::AsyncFnOnce};

use crate::{
    error::Result,
    outcome::{IntoOutcome, Outcome},
    scope::{AsyncScope, GenericAsyncScope, LocalAsyncScope, ScopeExitGuard, ScopeProvider},
    task::ScopeStorage,
    utils::ownership::Ownership,
};

/// 将异步作用域闭包限制到当前作用域实例。
///
/// 这里只量化传入 `&scope` 的引用生命周期；runtime、scope 和环境生命周期保持为
/// 独立参数，避免把 runtime extra 中的借用错误地推广成同一个高阶生命周期。
#[doc(hidden)]
pub fn _constrain<'rt, 'scope, 'env: 'scope, O, F, TExtra>(f: F) -> F
where
    O: IntoOutcome,
    F: for<'scope_ref> AsyncFnOnce(&'scope_ref AsyncScope<'rt, 'scope, 'env, TExtra>) -> O,
{
    f
}

/// [`_constrain`] 的线程本地作用域版本。
#[doc(hidden)]
pub fn _constrain_local<'rt, 'scope, 'env: 'scope, O, F, TExtra>(f: F) -> F
where
    O: IntoOutcome,
    F: for<'scope_ref> AsyncFnOnce(&'scope_ref LocalAsyncScope<'rt, 'scope, 'env, TExtra>) -> O,
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
