use veloq_runtime::runtime::Runtime;
use veloq_runtime::scope;

fn main() {
    let rt = Runtime::<(), _>::new();
    rt.block_on(async |ctx| {
        scope!(ctx, async |s| {
            let x = 42;
            let _ = s.spawn_boxed(async {
                let _y = &x;
            });
        })
        .await
        .unwrap();

        scope!(ctx, async |s| {
            let x = 42;
            let _ = s.spawn_boxed_local(async {
                let _y = &x;
            });
        })
        .await
        .unwrap();
    }).unwrap();
}
