use std::num::NonZeroUsize;

use veloq_runtime_next::runtime::Runtime;
use veloq_runtime_next::scope;
use veloq_runtime_next::task_local;

#[test]
fn test_local_task_execution() {
    let rt = Runtime::new(NonZeroUsize::new(1).unwrap());
    rt.block_on(async {
        scope!(s, {
            task_local!(t, async { 1 + 1 });
            let handle = s.spawn_local(&t);
            assert_eq!(handle.await.unwrap(), 2);
        });
    });
}

#[test]
fn test_local_task_with_yield() {
    let rt = Runtime::new(NonZeroUsize::new(2).unwrap());
    rt.block_on(async {
        scope!(s, {
            task_local!(t, async {
                veloq_runtime_next::task::yield_now().await;
                42
            });
            let handle = s.spawn_local(&t);
            assert_eq!(handle.await.unwrap(), 42);
        });
    });
}
