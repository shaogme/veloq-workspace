use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use veloq_runtime_next::runtime::{Runtime, WorkerInitContext, current_worker_id};

#[test]
fn worker_init_runs_for_each_worker() {
    let worker_init_calls = Arc::new(AtomicUsize::new(0));

    let runtime = Runtime::builder()
        .worker_count(NonZeroUsize::new(3).unwrap())
        .with_worker_init({
            let worker_init_calls = Arc::clone(&worker_init_calls);
            move |ctx: WorkerInitContext| {
                let worker_init_calls = Arc::clone(&worker_init_calls);
                async move {
                    assert_eq!(current_worker_id(), ctx.worker_id());
                    assert!(ctx.worker_id() < ctx.worker_count().get());
                    worker_init_calls.fetch_add(1, Ordering::AcqRel);
                }
            }
        })
        .build();

    runtime.block_on(async {});

    assert_eq!(worker_init_calls.load(Ordering::Acquire), 3);
}
