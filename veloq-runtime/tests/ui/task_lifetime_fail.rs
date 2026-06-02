use veloq_runtime::runtime::Runtime;
use veloq_runtime::{task, task_local};

fn main() {
    let rt = Runtime::<_, (), _>::new();
    rt.block_on(async |ctx| {
        ctx.scope(async |s| {
            task_local!(t, async {
                veloq_runtime::task::yield_now().await;
                veloq_runtime::task::yield_now().await;
                veloq_runtime::task::yield_now().await;
                veloq_runtime::task::yield_now().await;
                42
            });
            let _ = s.spawn_local(&t);
        })
        .await;
        ctx.scope(async |s| {
            task!(t, async {
                veloq_runtime::task::yield_now().await;
                veloq_runtime::task::yield_now().await;
                veloq_runtime::task::yield_now().await;
                veloq_runtime::task::yield_now().await;
                42
            });
            let _ = s.spawn(&t);
        })
        .await;
    });
}
