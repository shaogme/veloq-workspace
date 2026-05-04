use veloq_runtime_next::runtime::Runtime;
use veloq_runtime_next::{scope_local, spawn_boxed_local, spawn_local, task_local};

#[test]
fn test_scope_local_basic() {
    let rt = Runtime::new(1);
    rt.block_on(async {
        scope_local!(local_scope, {
            let h1 = spawn_boxed_local!(local_scope, async { 1 + 1 });
            task_local!(t2, async { 2 + 2 });
            spawn_local!(local_scope, h2, t2);
            assert_eq!(h1.await.unwrap(), 2);
            assert_eq!(h2.await.unwrap(), 4);
        });
    });
}

#[test]
fn test_scope_local_nested() {
    let rt = Runtime::new(1);
    rt.block_on(async {
        scope_local!(outer, {
            let h1 = spawn_boxed_local!(outer, async {
                scope_local!(inner, {
                    let h2 = spawn_boxed_local!(inner, async { 10 });
                    h2.await.unwrap()
                })
            });
            assert_eq!(h1.await.unwrap(), 10);
        });
    });
}
