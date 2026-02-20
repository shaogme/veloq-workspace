use crate::runtime::{Runtime, spawn_to};

fn current_worker_id() -> Option<usize> {
    let ctx = crate::runtime::context::try_current();
    ctx.map(|c| c.handle.id())
}

#[test]
fn test_spawn_to_worker() {
    let config = crate::config::Config::default().worker_threads(4);

    let runtime = Runtime::builder().config(config).build().unwrap();

    runtime.block_on(async move {
        // Target worker 2
        let target_id = 2;

        let handle = spawn_to(target_id, || async move { current_worker_id() });

        let executed_id = handle.await;

        assert_eq!(
            executed_id,
            Some(target_id),
            "Task should run on target worker 2"
        );

        // Target worker 3
        let target_id_3 = 3;
        let handle3 = spawn_to(target_id_3, || async move { current_worker_id() });

        assert_eq!(
            handle3.await,
            Some(target_id_3),
            "Task should run on target worker 3"
        );
    });
}
