use veloq_runtime::{runtime::Runtime, scope};

fn main() {
    Runtime::<(), _>::scope(async |ctx| {
        scope!(ctx, async |scope| {
            let handle = scope.spawn_boxed(async { 42usize });
            let _: &dyn Sync = &handle;
        })
        .await
        .unwrap();
    })
    .unwrap();
}
