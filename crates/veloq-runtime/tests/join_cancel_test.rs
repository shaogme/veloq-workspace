use veloq_std::sync::{
    NativeArc as Arc,
    atomic::{NativeAtomicBool as AtomicBool, Ordering},
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
fn handle_can_cancel_before_target_poll_without_calling_job() {
    RuntimeBuilder::new()
        .with_worker_count(Some(nz!(2)))
        .with_queue_capacity(nz!(2))
        .scope(async |ctx| {
            scope!(ctx, async |scope| {
                let blocker_started = Arc::new(AtomicBool::new(false));
                let blocker_started_for_job = blocker_started.clone();
                let mut blocker = scope.spawn_boxed_to(1, async move || {
                    blocker_started_for_job.store(true, Ordering::Release);
                    veloq_std::future::pending::<usize>().await
                });
                while !blocker_started.load(Ordering::Acquire) {
                    yield_now().await;
                }

                let job_called = Arc::new(AtomicBool::new(false));
                let job_called_for_job = job_called.clone();
                let mut handle = scope.spawn_boxed_to(1, async move || {
                    job_called_for_job.store(true, Ordering::Release);
                    7usize
                });
                handle.cancel();
                assert!(handle.is_cancel_requested());
                assert!(!handle.is_finished());

                blocker.cancel();
                assert!(matches!(
                    blocker.await,
                    JoinOutcome::TaskErr(TaskError::Cancelled)
                ));

                assert!(matches!(
                    handle.await,
                    JoinOutcome::TaskErr(TaskError::Cancelled)
                ));
                assert!(!job_called.load(Ordering::Acquire));
            })
            .await
            .unwrap();
        })
        .unwrap();
}

#[test]
fn handle_can_cancel_after_target_poll_begins() {
    RuntimeBuilder::new()
        .with_worker_count(Some(nz!(2)))
        .scope(async |ctx| {
            scope!(ctx, async |scope| {
                let started = Arc::new(AtomicBool::new(false));
                let task_started = started.clone();
                let mut handle = scope.spawn_boxed_to(1, async move || {
                    task_started.store(true, Ordering::Release);
                    veloq_std::future::pending::<usize>().await
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
