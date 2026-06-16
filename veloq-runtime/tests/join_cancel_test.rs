use veloq_runtime::{
    runtime::Runtime,
    scope,
    scope::JoinOutcome,
    task::{TaskError, yield_now},
};

#[test]
fn test_join_handle_waits_for_task_completion_on_cancel() {
    let rt = Runtime::<(), _>::new();
    rt.block_on(async |ctx| {
        scope!(ctx, async |scope| {
            let handle = scope.spawn_boxed(async {
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
    let rt = Runtime::<(), _>::new();
    rt.block_on(async |ctx| {
        scope!(ctx, async |scope| {
            let handle = scope.spawn_boxed(async {
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
    let rt = Runtime::<(), _>::new();
    rt.block_on(async |ctx| {
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
