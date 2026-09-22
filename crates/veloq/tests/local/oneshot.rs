use veloq_std::ops::AsyncFnOnce;

use veloq::{
    local::oneshot,
    nz,
    runtime::{Runtime, context::Ctx, scope_local},
};
use veloq_buf::{UniformSlot, heap::ThreadMemoryMultiplier};

fn run_test<F, R>(f: F) -> R
where
    F: for<'s> AsyncFnOnce(Ctx<'s>) -> R,
{
    Runtime::builder(UniformSlot::new(ThreadMemoryMultiplier(nz!(4))))
        .worker_count(Some(nz!(1)))
        .scope(f)
        .expect("failed to run scope")
}

#[test]
fn test_send_recv() {
    run_test(async |ctx| {
        oneshot::with_borrowed_channel(async |tx, rx| {
            scope_local!(ctx, async |s| {
                s.spawn_boxed_local(async move {
                    tx.send(42).unwrap();
                });

                assert_eq!(rx.await.unwrap(), 42);
            })
            .await
            .unwrap();
        })
        .await;
    });
}

#[test]
fn test_tx_closed() {
    run_test(async |_ctx| {
        oneshot::with_borrowed_channel::<i32, _, _>(async |tx, rx| {
            drop(tx);
            assert!(rx.await.is_err());
        })
        .await;
    });
}

#[test]
fn test_rx_closed() {
    run_test(async |_ctx| {
        oneshot::with_borrowed_channel::<i32, _, _>(async |tx, rx| {
            assert!(!tx.is_closed());
            drop(rx);
            assert!(tx.is_closed());

            // Attempt to send should fail
            assert_eq!(tx.send(10), Err(10));
        })
        .await;
    });
}

#[test]
fn test_try_recv() {
    run_test(async |_ctx| {
        oneshot::with_borrowed_channel(async |tx, rx| {
            assert_eq!(rx.try_recv(), Err(oneshot::TryRecvError::Empty));

            tx.send(100).unwrap();

            assert_eq!(rx.try_recv(), Ok(100));

            assert_eq!(rx.try_recv(), Err(oneshot::TryRecvError::Closed));
        })
        .await;
    });
}

#[test]
fn test_drop_tx_notify() {
    run_test(async |ctx| {
        oneshot::with_borrowed_channel::<i32, _, _>(async |tx, rx| {
            scope_local!(ctx, async |s| {
                let handle = s.spawn_boxed_local(rx);

                // Drop tx without sending
                drop(tx);

                let res = handle.await.unwrap();
                assert!(res.is_err());
            })
            .await
            .unwrap();
        })
        .await;
    });
}

#[test]
fn test_send_before_recv() {
    run_test(async |_ctx| {
        oneshot::with_borrowed_channel(async |tx, rx| {
            tx.send("hello").unwrap();
            assert_eq!(rx.await.unwrap(), "hello");
        })
        .await;
    });
}

#[test]
fn test_owned_oneshot() {
    run_test(async |ctx| {
        let (tx, rx) = oneshot::channel();

        scope_local!(ctx, async |s| {
            s.spawn_boxed_local(async move {
                tx.send(42).unwrap();
            });

            assert_eq!(rx.await.unwrap(), 42);
        })
        .await
        .unwrap();
    });
}
