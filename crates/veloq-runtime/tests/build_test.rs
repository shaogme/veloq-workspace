use veloq_runtime::{
    runtime::{Runtime, RuntimeBuilder},
    scope, scope_local,
};
use veloq_std::num::NonZeroUsize;

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

#[test]
fn test_nested_scope_join_handles_build_on_single_and_multiple_workers() {
    for worker_count in [1, 2] {
        RuntimeBuilder::new()
            .with_worker_count(Some(NonZeroUsize::new(worker_count).unwrap()))
            .scope(async |ctx| {
                scope!(ctx, async |outer| {
                    let first = outer.spawn_boxed(async { 1usize });
                    let second = outer.spawn_boxed(async { 2usize });

                    assert_eq!(second.await.unwrap(), 2);
                    assert_eq!(first.await.unwrap(), 1);

                    scope!(ctx, async |inner| {
                        let first = inner.spawn_boxed(async { 3usize });
                        let second = inner.spawn_boxed(async { 4usize });

                        assert_eq!(first.await.unwrap(), 3);
                        assert_eq!(second.await.unwrap(), 4);
                    })
                    .await
                    .unwrap();
                })
                .await
                .unwrap();
            })
            .unwrap();
    }
}
