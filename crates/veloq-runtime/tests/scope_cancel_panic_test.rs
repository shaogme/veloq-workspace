use veloq_runtime::{
    Outcome,
    runtime::{RuntimeBuilder, RuntimeShared},
    scope, scope_local,
};
use veloq_std::{
    future::Future,
    num::NonZeroUsize,
    panic::{AssertUnwindSafe, catch_unwind},
    pin::Pin,
    sync::atomic::{NativeAtomicBool as AtomicBool, Ordering},
    task::{Context, Poll},
};

fn with_workers(count: usize) -> RuntimeBuilder<(), fn(usize, &RuntimeShared<()>)> {
    RuntimeBuilder::new().with_worker_count(NonZeroUsize::new(count))
}

struct Park;

impl Future for Park {
    type Output = ();
    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<()> {
        Poll::Pending
    }
}

#[test]
fn panic_in_scope_cancels_and_joins_child_tasks() {
    let child_cancelled = AtomicBool::new(false);

    let result = catch_unwind(AssertUnwindSafe::new(|| {
        with_workers(2)
            .scope(async |ctx| {
                scope!(ctx, async |s| -> () {
                    s.spawn_boxed(async {
                        Park.await;
                    });
                    child_cancelled.store(true, Ordering::Release);
                    panic!("scope body panic");
                })
                .await
                .unwrap();
            })
            .unwrap();
    }));

    assert!(result.is_err(), "Scope panic should unwind out");
    assert!(child_cancelled.load(Ordering::Acquire));
}

#[test]
fn early_err_in_scope_cancels_and_joins_child_tasks() {
    let child_executed = AtomicBool::new(false);

    with_workers(2)
        .scope(async |ctx| {
            let res: Outcome<(), &'static str> = scope!(ctx, async |s| {
                s.spawn_boxed(async {
                    Park.await;
                });
                child_executed.store(true, Ordering::Release);
                Err("early failure")
            })
            .await
            .unwrap();

            assert_eq!(res, Outcome::Err("early failure"));
            assert!(child_executed.load(Ordering::Acquire));
        })
        .unwrap();
}

#[test]
fn ctx_scope_early_err_cancels_child_tasks() {
    let child_executed = AtomicBool::new(false);

    with_workers(2)
        .scope(async |ctx| {
            let res: Outcome<(), &'static str> = ctx
                .scope(async |s| {
                    s.spawn_boxed(async {
                        Park.await;
                    });
                    child_executed.store(true, Ordering::Release);
                    Err("ctx scope error")
                })
                .await
                .unwrap();

            assert_eq!(res, Outcome::Err("ctx scope error"));
            assert!(child_executed.load(Ordering::Acquire));
        })
        .unwrap();
}

#[test]
fn scope_local_early_err_cancels_child_tasks() {
    let child_executed = AtomicBool::new(false);

    with_workers(1)
        .scope(async |ctx| {
            let res: Outcome<(), &'static str> = scope_local!(ctx, async |s| {
                s.spawn_boxed(async {
                    Park.await;
                });
                child_executed.store(true, Ordering::Release);
                Err("scope_local error")
            })
            .await
            .unwrap();

            assert_eq!(res, Outcome::Err("scope_local error"));
            assert!(child_executed.load(Ordering::Acquire));
        })
        .unwrap();
}
