use veloq_runtime::runtime::Runtime;
use veloq_runtime::task_local;

#[test]
fn test_local_task_execution() {
    let rt = Runtime::new();
    rt.block_on(async |ctx| {
        ctx.scope(async |s| {
            task_local!(t, async { 1 + 1 });
            let handle = s.spawn_local(&t);
            assert_eq!(handle.await.unwrap(), 2);
        })
        .await;
    });
}

#[test]
fn test_local_task_with_yield() {
    let rt = Runtime::new();
    rt.block_on(async |ctx| {
        ctx.scope(async |s| {
            task_local!(t, async {
                veloq_runtime::task::yield_now().await;
                42
            });
            let handle = s.spawn_local(&t);
            assert_eq!(handle.await.unwrap(), 42);
        })
        .await;
    });
}
