use std::num::NonZeroUsize;

use veloq_runtime_next::runtime::Runtime;
use veloq_runtime_next::{scope_local, task_local};

#[test]
fn test_scope_local_basic() {
    let rt = Runtime::new(NonZeroUsize::new(1).unwrap());
    rt.block_on(async {
        scope_local!(local_scope, {
            let h1 = local_scope.spawn_boxed_local(async { 1 + 1 });
            task_local!(t2, async { 2 + 2 });
            let h2 = local_scope.spawn_local(&t2);
            assert_eq!(h1.await.unwrap(), 2);
            assert_eq!(h2.await.unwrap(), 4);
        });
    });
}

#[test]
fn test_scope_local_nested() {
    let rt = Runtime::new(NonZeroUsize::new(1).unwrap());
    rt.block_on(async {
        scope_local!(outer, {
            let h1 = outer.spawn_boxed_local(async {
                scope_local!(inner, {
                    let h2 = inner.spawn_boxed_local(async { 10 });
                    h2.await.unwrap()
                })
            });
            assert_eq!(h1.await.unwrap(), 10);
        });
    });
}
