use veloq_runtime::runtime::Runtime;
use veloq_runtime::task_local;

#[test]
fn test_local_task_execution() {
    let rt = Runtime::<(), _>::new();
    rt.block_on(async |ctx| {
        task_local!(t, async { 1 + 1 });
        ctx.scope(async |s| {
            let handle = s.spawn_local(&t);
            assert_eq!(handle.await.unwrap(), 2);
        })
        .await;
    })
    .unwrap();
}

#[test]
fn test_local_task_with_yield() {
    let rt = Runtime::<(), _>::new();
    rt.block_on(async |ctx| {
        task_local!(t, async {
            veloq_runtime::task::yield_now().await;
            42
        });
        ctx.scope(async |s| {
            let handle = s.spawn_local(&t);
            assert_eq!(handle.await.unwrap(), 42);
        })
        .await;
    })
    .unwrap();
}
