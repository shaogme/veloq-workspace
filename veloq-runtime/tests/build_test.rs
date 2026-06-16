#[allow(unused_imports)]
use veloq_runtime::{runtime::Runtime, scope, scope::AsyncScope, scope_local};

#[test]
fn test_nested_scope_local_build() {
    let rt = Runtime::<(), _>::new();
    rt.block_on(async |ctx| {
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
    let rt = Runtime::<(), _>::new();
    rt.block_on(async |ctx| {
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
    let rt = Runtime::<(), _>::new();
    rt.block_on(async |ctx| {
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
    let rt = Runtime::<(), _>::new();
    rt.block_on(async |ctx| {
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
    let rt = Runtime::<(), _>::new();
    rt.block_on(async |ctx| {
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
    let rt = Runtime::<(), _>::new();
    rt.block_on(async |ctx| {
        scope!(ctx, async move |parent_scope| {
            async fn run_child_scope(child_scope: &AsyncScope<'_, '_, ()>) {
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
    let rt = Runtime::<(), _>::new();
    rt.block_on(async |ctx| {
        scope!(ctx, async move |parent_scope| {
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
