use veloq_runtime::runtime::Runtime;

#[test]
fn test_nested_scope_local_build() {
    let rt = Runtime::<_, (), _>::new();
    rt.block_on(async |ctx| {
        ctx.scope_local(async |parent_scope| {
            parent_scope.spawn_boxed_local(async {
                ctx.scope_local(async move |child_scope| {
                    child_scope.spawn_boxed_local(async {});
                })
                .await;
            });
        })
        .await;
    });
}

// #[test]
// fn test_nested_scope_build() {
//     let rt = Runtime::<_, (), _>::new();
//     rt.block_on(async |ctx| {
//         ctx.scope(async |parent_scope| {
//             parent_scope.spawn_boxed(async move {
//                 ctx.scope(async move |child_scope| {
//                     child_scope.spawn_boxed(async {});
//                 })
//                 .await;
//             });
//         })
//         .await;
//     });
// }
