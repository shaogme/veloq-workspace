use veloq::{
    nz,
    runtime::{Runtime, context::Ctx, scope},
};
use veloq_buf::{UniformSlot, heap::ThreadMemoryMultiplier};
use veloq_std::ops::AsyncFnOnce;
use veloq_sync::oneshot;

fn run_test<F, R>(f: F) -> R
where
    F: for<'s> AsyncFnOnce(Ctx<'s>) -> R,
{
    Runtime::builder(UniformSlot::new(ThreadMemoryMultiplier(nz!(4))))
        .worker_count(Some(nz!(2)))
        .scope(f)
        .expect("failed to run scope")
}

#[test]
fn test_sync_oneshot_send_recv() {
    run_test(async |ctx| {
        oneshot::with_borrowed_channel(async |tx, rx| {
            scope!(ctx, async |s| {
                s.spawn_boxed(async move {
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
fn test_sync_oneshot_drop_sender() {
    run_test(async |ctx| {
        oneshot::with_borrowed_channel::<i32, _, _>(async |tx, rx| {
            scope!(ctx, async |s| {
                s.spawn_boxed(async move {
                    drop(tx);
                });

                let err = rx.await.unwrap_err();
                assert!(matches!(err, oneshot::error::RecvError(_)));
            })
            .await
            .unwrap();
        })
        .await;
    });
}

#[test]
fn test_sync_oneshot_try_recv() {
    run_test(async |_ctx| {
        oneshot::with_borrowed_channel(async |tx, mut rx| {
            assert_eq!(rx.try_recv(), Err(oneshot::error::TryRecvError::Empty));

            tx.send(100).unwrap();

            assert_eq!(rx.try_recv(), Ok(100));
            assert_eq!(rx.try_recv(), Err(oneshot::error::TryRecvError::Closed));
        })
        .await;
    });
}

#[test]
fn test_sync_oneshot_drop_receiver_notify() {
    run_test(async |ctx| {
        oneshot::with_borrowed_channel::<i32, _, _>(async |tx, rx| {
            scope!(ctx, async |s| {
                s.spawn_boxed(async move {
                    drop(rx);
                });
            })
            .await
            .unwrap();

            assert!(tx.is_closed());
            assert_eq!(tx.send(1), Err(1));
        })
        .await;
    });
}

#[test]
fn test_sync_oneshot_poll_closed() {
    run_test(async |ctx| {
        oneshot::with_borrowed_channel::<(), _, _>(async |mut tx, rx| {
            scope!(ctx, async |s| {
                s.spawn_boxed(async move {
                    drop(rx);
                });

                tx.closed().await;
                assert!(tx.is_closed());
            })
            .await
            .unwrap();
        })
        .await;
    });
}

#[test]
fn test_sync_owned_oneshot() {
    run_test(async |ctx| {
        let (tx, rx) = oneshot::channel();

        scope!(ctx, async |s| {
            s.spawn_boxed(async move {
                tx.send(42).unwrap();
            });

            assert_eq!(rx.await.unwrap(), 42);
        })
        .await
        .unwrap();
    });
}
