use veloq_runtime::runtime::Runtime;

#[test]
fn test_nested_scope_local_build() {
    let rt = Runtime::<(), _>::new();
    rt.block_on(async |ctx| {
        ctx.scope_local(async |parent_scope| {
            parent_scope.spawn_boxed_local(async {
                ctx.scope_local(async |child_scope| {
                    child_scope.spawn_boxed_local(async {});
                })
                .await;
            });
        })
        .await;
    })
    .unwrap();
}

#[test]
fn test_nested_scope_build_1() {
    let rt = Runtime::<(), _>::new();
    rt.block_on(async |ctx| {
        ctx.scope_local(async |parent_scope| {
            parent_scope.spawn_boxed(async {
                ctx.scope(async |child_scope| {
                    child_scope.spawn_boxed(async {});
                })
                .await;
            });
        })
        .await;
    })
    .unwrap();
}

#[test]
fn test_nested_scope_build_2() {
    let rt = Runtime::<(), _>::new();
    rt.block_on(async |ctx| {
        ctx.scope(async |parent_scope| {
            parent_scope.spawn_boxed_local(async {
                ctx.scope_local(async |child_scope| {
                    child_scope.spawn_boxed(async {});
                })
                .await;
            });
        })
        .await;
    })
    .unwrap();
}

#[test]
fn test_nested_scope_build_3() {
    let rt = Runtime::<(), _>::new();
    rt.block_on(async |ctx| {
        ctx.scope(async |_parent_scope| {
            ctx.scope_local(async |child_scope| {
                child_scope.spawn_boxed(async {});
            })
            .await;
        })
        .await;
    })
    .unwrap();
}

#[test]
fn test_nested_scope_build_4() {
    let rt = Runtime::<(), _>::new();
    rt.block_on(async |ctx| {
        ctx.scope(async |_parent_scope| {
            ctx.scope(async |child_scope| {
                child_scope.spawn_boxed(async {});
            })
            .await;
        })
        .await;
    })
    .unwrap();
}

#[test]
fn test_nested_scope_build_5() {
    let rt = Runtime::<(), _>::new();
    rt.block_on(async |ctx| {
        ctx.scope(async |parent_scope| {
            async fn run_child_scope(child_scope: &veloq_runtime::scope::AsyncScope<'_, '_, ()>) {
                child_scope.spawn_boxed(async {});
            }
            parent_scope.spawn_boxed(async {
                ctx.scope(run_child_scope).await;
            });
        })
        .await;
    })
    .unwrap();
}

// This test fails to compile due to limitations in Rust's current lifetime inference mechanism.
// Error Cause: `ctx.scope` expects an async closure that accepts a `GenericAsyncScope` reference with any lifetime (Higher-Rank Trait Bound, HRTB).
// However, when the nested async closure (`async |child_scope|`) is defined inside the async block spawned by `parent_scope.spawn_boxed`,
// the compiler fails to generalize the closure's parameter lifetime over all possible inputs (i.e. to implement `AsyncFnOnce` generically).
// Instead, it infers a single concrete lifetime, causing the compilation error:
// "implementation of `AsyncFnOnce` is not general enough".
// Workaround: Refer to `test_nested_scope_build_5` where a named `async fn` is used to explicitly express the lifetime bounds.
// #[test]
// fn test_nested_scope_build_6() {
//     let rt = Runtime::<(), _>::new();
//     rt.block_on(async |ctx| {
//         ctx.scope(async |parent_scope| {
//             parent_scope.spawn_boxed(async {
//                 ctx.scope(async |child_scope| {
//                     child_scope.spawn_boxed(async {});
//                 })
//                 .await;
//             });
//         })
//         .await;
//     }).unwrap();
// }
