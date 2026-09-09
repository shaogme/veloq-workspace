use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use veloq_runtime::{
    runtime::{Runtime, RuntimeBuilder},
    scope,
    scope::JoinOutcome,
    task::{TaskError, yield_now},
};
use veloq_std::nz;

#[test]
fn test_join_handle_waits_for_task_completion_on_cancel() {
    Runtime::<(), _>::scope(async |ctx| {
        scope!(ctx, async |scope| {
            let mut handle = scope.spawn_boxed(async {
                for _ in 0..8 {
                    yield_now().await;
                }
                42
            });

            yield_now().await;
            handle.cancel();

            assert!(handle.is_cancel_requested());
            assert!(!handle.is_finished());

            let res = handle.await;
            assert!(matches!(res, JoinOutcome::TaskErr(TaskError::Cancelled)));
        })
        .await
        .unwrap();
    })
    .unwrap();
}

#[test]
fn test_join_handle_cancelled_before_await() {
    Runtime::<(), _>::scope(async |ctx| {
        scope!(ctx, async |scope| {
            let mut handle = scope.spawn_boxed(async {
                loop {
                    yield_now().await;
                }
            });

            yield_now().await;
            handle.cancel();

            handle.cancelled().await;
            assert!(!handle.is_finished());

            let res = handle.await;
            assert!(matches!(res, JoinOutcome::TaskErr(TaskError::Cancelled)));
        })
        .await
        .unwrap();
    })
    .unwrap();
}

#[test]
fn test_join_handle_scope_cancel_waits_for_completion() {
    Runtime::<(), _>::scope(async |ctx| {
        scope!(ctx, async |scope| {
            let token = scope.cancel_token().clone();
            let handle = scope.spawn_boxed(async {
                loop {
                    yield_now().await;
                }
            });

            yield_now().await;
            token.cancel();

            assert!(handle.is_cancel_requested());
            assert!(!handle.is_finished());

            let res = handle.await;
            assert!(matches!(res, JoinOutcome::TaskErr(TaskError::Cancelled)));
        })
        .await
        .unwrap();
    })
    .unwrap();
}

#[test]
fn routed_handle_can_cancel_before_remote_job_is_published() {
    RuntimeBuilder::new()
        .with_worker_count(Some(nz!(2)))
        .scope(async |ctx| {
            scope!(ctx, async |scope| {
                let mut handle = scope.spawn_boxed_to(1, async || 7usize);
                for _ in 0..8 {
                    handle.cancel();
                }

                assert!(handle.is_cancel_requested());
                assert!(matches!(
                    handle.await,
                    JoinOutcome::TaskErr(TaskError::Cancelled)
                ));
            })
            .await
            .unwrap();
        })
        .unwrap();
}

#[test]
fn routed_handle_can_cancel_after_remote_job_is_published() {
    RuntimeBuilder::new()
        .with_worker_count(Some(nz!(2)))
        .scope(async |ctx| {
            scope!(ctx, async |scope| {
                let started = Arc::new(AtomicBool::new(false));
                let task_started = started.clone();
                let mut handle = scope.spawn_boxed_to(1, async move || {
                    task_started.store(true, Ordering::Release);
                    std::future::pending::<usize>().await
                });

                while !started.load(Ordering::Acquire) {
                    yield_now().await;
                }
                for _ in 0..8 {
                    handle.cancel();
                }

                assert!(handle.is_cancel_requested());
                assert!(matches!(
                    handle.await,
                    JoinOutcome::TaskErr(TaskError::Cancelled)
                ));
            })
            .await
            .unwrap();
        })
        .unwrap();
}
