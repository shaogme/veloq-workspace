use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use veloq_runtime::runtime::{Runtime, WorkerInitContext};

#[test]
fn worker_init_runs_for_each_worker() {
    let worker_init_calls = Arc::new(AtomicUsize::new(0));

    let runtime = Runtime::<_, (), _>::builder()
        .worker_count(NonZeroUsize::new(3).unwrap())
        .with_worker_init({
            let worker_init_calls = Arc::clone(&worker_init_calls);
            async move |ctx: WorkerInitContext<'_, ()>| {
                assert!(ctx.worker_id() < ctx.worker_count().get());
                worker_init_calls.fetch_add(1, Ordering::AcqRel);
            }
        })
        .build();

    runtime.block_on(async |_ctx| {});

    assert_eq!(worker_init_calls.load(Ordering::Acquire), 3);
}
