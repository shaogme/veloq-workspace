pub mod helpers;

/// A macro to wait on multiple futures simultaneously, returning the output of the first one that completes.
///
/// The first argument must be a [`RuntimeCtx`](crate::runtime::RuntimeCtx).
///
/// By default, branches are polled in a **fair** order: each `select!` invocation picks a random
/// starting branch via the worker TLS [`FastRand`](crate::utils::FastRand), then polls in ring order.
/// When multiple branches are ready in the same poll, each branch has an equal chance to win.
///
/// Use `biased;` immediately after `ctx;` to poll branches strictly in declaration order. This is more efficient
/// for I/O completion models where polling may trigger a syscall or resource allocation.
///
/// # Example
/// ```ignore
/// select! {
///     ctx;
///     val = rx.recv() => {
///         println!("Received: {:?}", val);
///     },
///     _ = timer.tick() => {
///         println!("Timed out");
///     }
/// }
/// ```
#[macro_export]
macro_rules! select {
    { $ctx:expr; $pat:pat = $fut:expr => $handler:expr $(,)? } => {
        {
            let _ = $ctx;
            #[allow(clippy::let_unit_value)]
            let $pat = $fut.await;
            $handler
        }
    };

    { $ctx:expr; biased; $pat:pat = $fut:expr => $handler:expr $(,)? } => {
        {
            let _ = $ctx;
            #[allow(clippy::let_unit_value)]
            let $pat = $fut.await;
            $handler
        }
    };

    { $ctx:expr; biased; $pat:pat = $fut:expr => $handler:expr, $($r_pat:pat = $r_fut:expr => $r_handler:expr),+ $(,)? } => {
        {
            let _ = $ctx;
            use std::future::{poll_fn, Future, IntoFuture};
            use std::pin::Pin;
            use std::task::Poll;
            use $crate::task::RuntimeContextExt;

            const BRANCHES: usize = $crate::select!(@branch_len $pat = $fut => $handler $(, $r_pat = $r_fut => $r_handler)*);

            let mut __futures = (
                IntoFuture::into_future($fut),
                $($crate::select!(@into_future $r_fut)),+
            );
            let mut __futures = &mut __futures;

            poll_fn(move |cx| {
                if cx.is_cancelled() {
                    use std::panic::panic_any;
                    use $crate::task::TaskError;
                    panic_any(TaskError::Cancelled);
                }

                for __i in 0..BRANCHES {
                    $crate::select!(@poll_branches cx, __futures, __i, [], $pat => $handler, $($r_pat => $r_handler),+);
                }

                Poll::Pending
            })
            .await
        }
    };

    { $ctx:expr; $pat:pat = $fut:expr => $handler:expr, $($r_pat:pat = $r_fut:expr => $r_handler:expr),+ $(,)? } => {
        {
            use std::future::{poll_fn, Future, IntoFuture};
            use std::pin::Pin;
            use std::task::Poll;
            use $crate::task::RuntimeContextExt;

            const BRANCHES: usize = $crate::select!(@branch_len $pat = $fut => $handler $(, $r_pat = $r_fut => $r_handler)*);

            let mut __futures = (
                IntoFuture::into_future($fut),
                $($crate::select!(@into_future $r_fut)),+
            );
            let mut __futures = &mut __futures;
            let __start = $ctx.select_poll_start(BRANCHES as u32) as usize;

            poll_fn(move |cx| {
                if cx.is_cancelled() {
                    use std::panic::panic_any;
                    use $crate::task::TaskError;
                    panic_any(TaskError::Cancelled);
                }


                 for __i in 0..BRANCHES {
                    let __branch = (__start + __i) % BRANCHES;
                    $crate::select!(@poll_branches cx, __futures, __branch, [], $pat => $handler, $($r_pat => $r_handler),+);
                }

                Poll::Pending
            })
            .await
        }
    };

    (@into_future $fut:expr) => {
        {
            use std::future::IntoFuture;
            IntoFuture::into_future($fut)
        }
    };

    (@branch_len $pat:pat = $fut:expr => $handler:expr) => {
        1usize
    };
    (@branch_len $pat:pat = $fut:expr => $handler:expr, $($r_pat:pat = $r_fut:expr => $r_handler:expr),+ $(,)?) => {
        1usize + $crate::select!(@branch_len $($r_pat = $r_fut => $r_handler),+)
    };

    (@poll_branches $cx:ident, $futures:ident, $branch:ident, [ $($idx:tt)* ], $pat:pat => $handler:expr) => {
        if $branch == $crate::select!(@as_val $($idx)*) {
            let __f = unsafe { Pin::new_unchecked($crate::select!(@tuple_field $futures, $($idx)*)) };
            if let Poll::Ready(__out) = __f.poll($cx) {
                #[allow(unreachable_code)]
                match __out {
                    $pat => return Poll::Ready($handler),
                }
            }
        }
    };

    (@poll_branches $cx:ident, $futures:ident, $branch:ident, [ $($idx:tt)* ], $pat:pat => $handler:expr, $next_pat:pat => $next_handler:expr $(, $rest_pat:pat => $rest_handler:expr)*) => {
        if $branch == $crate::select!(@as_val $($idx)*) {
            let __f = unsafe { Pin::new_unchecked($crate::select!(@tuple_field $futures, $($idx)*)) };
            if let Poll::Ready(__out) = __f.poll($cx) {
                #[allow(unreachable_code)]
                match __out {
                    $pat => return Poll::Ready($handler),
                }
            }
        } else {
            $crate::select!(@poll_branches $cx, $futures, $branch, [ ~ $($idx)* ], $next_pat => $next_handler $(, $rest_pat => $rest_handler)*)
        }
    };

    (@as_val) => { 0 };
    (@as_val ~) => { 1 };
    (@as_val ~ ~) => { 2 };
    (@as_val ~ ~ ~) => { 3 };
    (@as_val ~ ~ ~ ~) => { 4 };
    (@as_val ~ ~ ~ ~ ~) => { 5 };
    (@as_val ~ ~ ~ ~ ~ ~) => { 6 };
    (@as_val ~ ~ ~ ~ ~ ~ ~) => { 7 };
    (@as_val ~ ~ ~ ~ ~ ~ ~ ~) => { 8 };
    (@as_val ~ ~ ~ ~ ~ ~ ~ ~ ~) => { 9 };
    (@as_val ~ ~ ~ ~ ~ ~ ~ ~ ~ ~) => { 10 };
    (@as_val ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~) => { 11 };
    (@as_val ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~) => { 12 };
    (@as_val ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~) => { 13 };
    (@as_val ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~) => { 14 };
    (@as_val ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~) => { 15 };
    (@as_val ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~) => { 16 };

    (@tuple_field $tuple:ident, ) => { &mut $tuple.0 };
    (@tuple_field $tuple:ident, ~) => { &mut $tuple.1 };
    (@tuple_field $tuple:ident, ~ ~) => { &mut $tuple.2 };
    (@tuple_field $tuple:ident, ~ ~ ~) => { &mut $tuple.3 };
    (@tuple_field $tuple:ident, ~ ~ ~ ~) => { &mut $tuple.4 };
    (@tuple_field $tuple:ident, ~ ~ ~ ~ ~) => { &mut $tuple.5 };
    (@tuple_field $tuple:ident, ~ ~ ~ ~ ~ ~) => { &mut $tuple.6 };
    (@tuple_field $tuple:ident, ~ ~ ~ ~ ~ ~ ~) => { &mut $tuple.7 };
    (@tuple_field $tuple:ident, ~ ~ ~ ~ ~ ~ ~ ~) => { &mut $tuple.8 };
    (@tuple_field $tuple:ident, ~ ~ ~ ~ ~ ~ ~ ~ ~) => { &mut $tuple.9 };
    (@tuple_field $tuple:ident, ~ ~ ~ ~ ~ ~ ~ ~ ~ ~) => { &mut $tuple.10 };
    (@tuple_field $tuple:ident, ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~) => { &mut $tuple.11 };
    (@tuple_field $tuple:ident, ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~) => { &mut $tuple.12 };
    (@tuple_field $tuple:ident, ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~) => { &mut $tuple.13 };
    (@tuple_field $tuple:ident, ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~) => { &mut $tuple.14 };
    (@tuple_field $tuple:ident, ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~) => { &mut $tuple.15 };
}

#[macro_export]
macro_rules! scope {
    ($ctx:expr, $closure:expr) => {
        async {
            use std::future::poll_fn;
            use std::task::Poll;
            use $crate::macros::helpers::{_constrain, _constrain_result};
            use $crate::scope::{AsyncScope, AsyncScopeGuard};
            use $crate::task::RuntimeContextExt;

            let parent = poll_fn(|cx| Poll::Ready(RuntimeContextExt::scope_completion(cx))).await;
            let guard = AsyncScopeGuard;
            let scope = AsyncScope::new(
                $crate::runtime::AsRuntimeCtx::as_runtime_ctx($ctx),
                parent,
                &guard,
            );
            let s_ref = &scope;
            let res = _constrain($closure)(s_ref).await;
            scope.wait_all().await?;
            _constrain_result(Ok(res))
        }
    };
}

#[macro_export]
macro_rules! scope_local {
    ($ctx:expr, $closure:expr) => {
        async {
            use std::future::poll_fn;
            use std::task::Poll;
            use $crate::macros::helpers::{_constrain_local, _constrain_result};
            use $crate::scope::{AsyncScopeGuard, LocalAsyncScope};
            use $crate::task::RuntimeContextExt;

            let parent = poll_fn(|cx| Poll::Ready(RuntimeContextExt::scope_completion(cx))).await;
            let guard = AsyncScopeGuard;
            let scope = LocalAsyncScope::new(
                $crate::runtime::AsRuntimeCtx::as_runtime_ctx($ctx),
                parent,
                &guard,
            );
            let s_ref = &scope;
            let res = _constrain_local($closure)(s_ref).await;
            scope.wait_all().await?;
            _constrain_result(Ok(res))
        }
    };
}
