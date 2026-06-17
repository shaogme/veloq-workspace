use veloq_runtime::{
    LifetimeGuard,
    runtime::Runtime,
    scope,
    scope::JoinOutcome,
    scope_local,
    task::{TaskError, yield_now},
    task_local,
};

#[test]
fn test_scope_local_basic() {
    let guard = LifetimeGuard;
    let rt = Runtime::<(), _>::new(&guard);
    rt.block_on(async |ctx| {
        task_local!(t2, async { 2 + 2 });
        scope_local!(ctx, async |local_scope| {
            let h1 = local_scope.spawn_boxed_local(async { 1 + 1 });
            let h2 = local_scope.spawn_local(&t2);
            assert_eq!(h1.await.unwrap(), 2);
            assert_eq!(h2.await.unwrap(), 4);
        })
        .await
        .unwrap();
    })
    .unwrap();
}

#[test]
fn test_scope_local_nested() {
    let guard = LifetimeGuard;
    let rt = Runtime::<(), _>::new(&guard);
    rt.block_on(async |ctx| {
        scope_local!(ctx, async move |outer| {
            let h1 = outer.spawn_boxed_local(async move {
                scope_local!(ctx, async |inner| {
                    let h2 = inner.spawn_boxed_local(async { 10 });
                    h2.await.unwrap()
                })
                .await
                .unwrap()
            });
            assert_eq!(h1.await.unwrap(), 10);
        })
        .await
        .unwrap();
    })
    .unwrap();
}

#[test]
fn test_scope_local_nested_in_async_scope_cancellation() {
    let guard = LifetimeGuard;
    let rt = Runtime::<(), _>::new(&guard);
    rt.block_on(async |ctx| {
        scope!(ctx, async |parent_scope| {
            let parent_token = parent_scope.cancel_token().clone();

            let handle = parent_scope.spawn_boxed_local(async move {
                scope_local!(ctx, async move |child_scope| {
                    let cancel_token = child_scope.cancel_token().clone();
                    let child_handle = child_scope.spawn_boxed_local(async move {
                        cancel_token.cancelled().await;
                        42
                    });

                    // Child handle should be cancelled
                    let res = child_handle.await;
                    assert!(matches!(res, JoinOutcome::TaskErr(TaskError::Cancelled)));
                })
                .await
                .unwrap();
            });

            // Let the tasks start and suspend
            yield_now().await;
            yield_now().await;

            // Trigger parent cancellation
            parent_token.cancel();

            let res = handle.await;
            assert!(matches!(res, JoinOutcome::TaskErr(TaskError::Cancelled)));
        })
        .await
        .unwrap();
    })
    .unwrap();
}
