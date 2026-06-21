#[allow(unused_imports)]
use veloq_runtime::{LifetimeGuard, runtime::Runtime, scope, scope::AsyncScope, scope_local};

#[test]
fn test_nested_scope_local_build() {
    let guard = LifetimeGuard;
    let rt = Runtime::<(), _>::new(&guard);
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
    let guard = LifetimeGuard;
    let rt = Runtime::<(), _>::new(&guard);
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
    let guard = LifetimeGuard;
    let rt = Runtime::<(), _>::new(&guard);
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
    let guard = LifetimeGuard;
    let rt = Runtime::<(), _>::new(&guard);
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
    let guard = LifetimeGuard;
    let rt = Runtime::<(), _>::new(&guard);
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
    let guard = LifetimeGuard;
    let rt = Runtime::<(), _>::new(&guard);
    rt.block_on(async |ctx| {
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
    let guard = LifetimeGuard;
    let rt = Runtime::<(), _>::new(&guard);
    rt.block_on(async |ctx| {
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
