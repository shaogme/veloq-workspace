#![no_std]

mod error;
pub mod macros;
mod outcome;
mod runtime;
mod scope;
mod task;
mod utils;

#[doc(hidden)]
pub use veloq_std as __veloq_std;

pub use error::*;
pub use outcome::{IntoOutcome, Outcome};
pub use runtime::*;
pub use scope::*;
pub use task::*;
pub use utils::*;
pub use veloq_storage as storage;

use crate::{
    error::Result as MacroResult,
    outcome::{IntoOutcome as MacroIntoOutcome, Outcome as MacroOutcome},
    scope::{
        AsyncScope as MacroAsyncScope, GenericAsyncScope as MacroGenericAsyncScope,
        LocalAsyncScope as MacroLocalAsyncScope, ScopeExitGuard as MacroScopeExitGuard,
    },
    task::ScopeStorage as MacroScopeStorage,
    utils::Ownership as MacroOwnership,
};
use veloq_std::{future::Future, ops::AsyncFnOnce};

pub fn yield_now() -> YieldNow {
    YieldNow::new()
}

#[doc(hidden)]
pub struct MacroHelpers;

impl MacroHelpers {
    #[doc(hidden)]
    pub fn _constrain<'rt, 'scope, 'env: 'scope, O, F, TExtra>(f: F) -> F
    where
        O: MacroIntoOutcome,
        F: for<'scope_ref> AsyncFnOnce(&'scope_ref MacroAsyncScope<'rt, 'scope, 'env, TExtra>) -> O,
    {
        f
    }

    #[doc(hidden)]
    pub fn _constrain_local<'rt, 'scope, 'env: 'scope, O, F, TExtra>(f: F) -> F
    where
        O: MacroIntoOutcome,
        F: for<'scope_ref> AsyncFnOnce(
            &'scope_ref MacroLocalAsyncScope<'rt, 'scope, 'env, TExtra>,
        ) -> O,
    {
        f
    }

    #[doc(hidden)]
    pub fn _constrain_result<T>(result: MacroResult<T>) -> MacroResult<T> {
        result
    }

    #[doc(hidden)]
    pub async fn run_scope_eval<
        'rt,
        'scope,
        'env: 'scope,
        S: MacroScopeStorage,
        O: MacroOwnership + 'static,
        TExtra,
        Fut: Future<Output = Body>,
        Body: MacroIntoOutcome,
    >(
        scope: &MacroGenericAsyncScope<'rt, 'scope, 'env, S, O, TExtra>,
        fut: Fut,
    ) -> MacroResult<MacroOutcome<Body::Output, Body::Error>> {
        let mut guard = MacroScopeExitGuard::new(scope);
        let outcome = fut.await.into_outcome();

        if matches!(&outcome, MacroOutcome::Err(_)) {
            scope.completion().cancel();
        }

        scope.wait_all().await?;
        guard.disarm();
        Ok(outcome)
    }
}
