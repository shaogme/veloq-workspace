//! Global injector 关停协议的黑盒故障注入覆盖。
//!
//! 通过可控的 runtime waker 触发 wake failure，并让作用域同时持有一批挂起任务。
//! 运行时必须在停止发布后完成统一收尾，不能因为任务仍在队列或作用域中而永久等待。

use veloq_std::{
    future::{pending, poll_fn},
    num::NonZeroUsize,
    sync::{
        NativeArc as Arc,
        atomic::{NativeAtomicBool as AtomicBool, Ordering},
    },
    task::Poll,
};

use veloq_runtime::{
    error::RuntimeWakeError,
    runtime::primitives::RuntimeWaker,
    runtime::{RuntimeBuilder, RuntimeShared},
    scope,
};

struct ControlledWaker {
    fail: Arc<AtomicBool>,
}

impl RuntimeWaker for ControlledWaker {
    fn wake(&self) -> Result<(), RuntimeWakeError> {
        if self.fail.load(Ordering::Acquire) {
            Err(RuntimeWakeError {
                backend: "test",
                worker_id: 0,
                operation: "global-injector-shutdown",
                detail: "injected wake failure".into(),
            })
        } else {
            Ok(())
        }
    }
}

#[test]
fn wake_failure_drains_pending_scope_without_hanging() {
    let fail = Arc::new(AtomicBool::new(false));
    let result = RuntimeBuilder::new()
        .with_worker_count(NonZeroUsize::new(2))
        .with_queue_capacity(NonZeroUsize::new(1).expect("non-zero queue capacity"))
        .with_worker_factory({
            let fail = fail.clone();
            move |worker_id: usize, shared: &RuntimeShared<()>| {
                shared.unparkers()[worker_id]
                    .bind(Arc::new(ControlledWaker { fail: fail.clone() }))
                    .expect("worker waker must be bound once");
            }
        })
        .scope(async |ctx| {
            let _ = scope!(ctx, async |scope| {
                let _handles = (0..16)
                    .map(|_| {
                        scope.spawn_boxed(async {
                            pending::<()>().await;
                            1usize
                        })
                    })
                    .collect::<Vec<_>>();

                let mut triggered = false;
                poll_fn(move |_| {
                    if !triggered {
                        triggered = true;
                        fail.store(true, Ordering::Release);
                        let _ = ctx.shared().unparkers()[0].unpark();
                    }
                    Poll::<()>::Pending
                })
                .await;
            })
            .await;
        });

    assert!(result.is_err(), "wake failure must stop the runtime");
}

#[test]
fn worker_initialization_panic_closes_runtime_without_waiting_for_missing_worker() {
    let result = RuntimeBuilder::new()
        .with_worker_count(NonZeroUsize::new(2))
        .with_worker_factory(|worker_id: usize, _shared: &RuntimeShared<()>| {
            assert_eq!(worker_id, 0, "only worker 0 should finish initialization");
        })
        .scope(async |_ctx| ());

    assert!(
        result.is_err(),
        "a worker initialization panic must be reported as a runtime error"
    );
}

#[test]
fn main_worker_initialization_panic_runs_shutdown_completion() {
    let result = RuntimeBuilder::new()
        .with_worker_count(NonZeroUsize::new(2))
        .with_worker_factory(|worker_id: usize, _shared: &RuntimeShared<()>| {
            assert_eq!(worker_id, 1, "only worker 1 should finish initialization");
        })
        .scope(async |_ctx| ());

    assert!(
        result.is_err(),
        "a main worker initialization panic must be reported as a runtime error"
    );
}
