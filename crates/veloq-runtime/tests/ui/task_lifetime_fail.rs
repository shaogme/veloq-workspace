use veloq_runtime::{Runtime, scope, task, task_local};

fn main() {
    Runtime::<(), _>::scope(async |ctx| {
        scope!(ctx, async |s| {
            task_local!(t, async {
                veloq_runtime::yield_now().await;
                veloq_runtime::yield_now().await;
                veloq_runtime::yield_now().await;
                veloq_runtime::yield_now().await;
                42
            });
            let _ = s.spawn_local(&t);
        })
        .await
        .unwrap();
        scope!(ctx, async |s| {
            task!(t, async {
                veloq_runtime::yield_now().await;
                veloq_runtime::yield_now().await;
                veloq_runtime::yield_now().await;
                veloq_runtime::yield_now().await;
                42
            });
            let _ = s.spawn(&t);
        })
        .await
        .unwrap();
    }).unwrap();
}
