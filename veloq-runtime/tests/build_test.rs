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
    });
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
    });
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
    });
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
    });
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
    });
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
    });
}

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
//     });
// }
