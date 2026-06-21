use veloq_runtime::{runtime::Runtime, scope, scope_local};

#[test]
fn test_nested_scope_local_build() {
    Runtime::<(), _>::scope(async |ctx| {
        scope_local!(ctx, async |parent_scope| {
            parent_scope.spawn_boxed_local(async move {
                scope_local!(ctx, async move |child_scope| {
                    child_scope.spawn_boxed_local(async {});
                })
                .await
                .unwrap();
            });
        })
        .await
        .unwrap();
    })
    .unwrap();
}

#[test]
fn test_nested_scope_build_1() {
    Runtime::<(), _>::scope(async |ctx| {
        scope_local!(ctx, async |parent_scope| {
            parent_scope.spawn_boxed(async move {
                scope!(ctx, async move |child_scope| {
                    child_scope.spawn_boxed(async {});
                })
                .await
                .unwrap();
            });
        })
        .await
        .unwrap();
    })
    .unwrap();
}

#[test]
fn test_nested_scope_build_2() {
    Runtime::<(), _>::scope(async |ctx| {
        scope!(ctx, async move |parent_scope| {
            parent_scope.spawn_boxed_local(async move {
                scope_local!(ctx, async |child_scope| {
                    child_scope.spawn_boxed(async {});
                })
                .await
                .unwrap();
            });
        })
        .await
        .unwrap();
    })
    .unwrap();
}

#[test]
fn test_nested_scope_build_3() {
    Runtime::<(), _>::scope(async |ctx| {
        scope!(ctx, async |_parent_scope| {
            scope_local!(ctx, async |child_scope| {
                child_scope.spawn_boxed(async {});
            })
            .await
            .unwrap();
        })
        .await
        .unwrap();
    })
    .unwrap();
}

#[test]
fn test_nested_scope_build_4() {
    Runtime::<(), _>::scope(async |ctx| {
        scope!(ctx, async |_parent_scope| {
            scope!(ctx, async |child_scope| {
                child_scope.spawn_boxed(async {});
            })
            .await
            .unwrap();
        })
        .await
        .unwrap();
    })
    .unwrap();
}

#[test]
fn test_nested_scope_build_5() {
    Runtime::<(), _>::scope(async |ctx| {
        scope!(ctx, async move |parent_scope| {
            async fn run_child_scope(child_scope: &AsyncScope<'_, '_, '_, ()>) {
                child_scope.spawn_boxed(async {});
            }
            parent_scope.spawn_boxed(async move {
                scope!(ctx, run_child_scope).await.unwrap();
            });
        })
        .await
        .unwrap();
    })
    .unwrap();
}

#[test]
fn test_nested_scope_build_6() {
    Runtime::<(), _>::scope(async |ctx| {
        ctx.scope(async move |parent_scope| {
            parent_scope.spawn_boxed(async move {
                scope!(ctx, async move |child_scope| {
                    child_scope.spawn_boxed(async {});
                })
                .await
                .unwrap();
            });
        })
        .await
        .unwrap();
    })
    .unwrap();
}
