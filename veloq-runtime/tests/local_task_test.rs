use veloq_runtime::{runtime::Runtime, scope, task::yield_now, task_local};

#[test]
fn test_local_task_execution() {
    let rt = Runtime::<(), _>::new();
    rt.block_on(async |ctx| {
        task_local!(t, async { 1 + 1 });
        scope!(ctx, async |s| {
            let handle = s.spawn_local(&t);
            assert_eq!(handle.await.unwrap(), 2);
        })
        .await
        .unwrap();
    })
    .unwrap();
}

#[test]
fn test_local_task_with_yield() {
    let rt = Runtime::<(), _>::new();
    rt.block_on(async |ctx| {
        task_local!(t, async {
            yield_now().await;
            42
        });
        scope!(ctx, async |s| {
            let handle = s.spawn_local(&t);
            assert_eq!(handle.await.unwrap(), 42);
        })
        .await
        .unwrap();
    })
    .unwrap();
}
