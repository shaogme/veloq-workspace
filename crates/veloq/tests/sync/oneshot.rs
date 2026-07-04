use std::ops::AsyncFnOnce;
use veloq::{
    nz,
    runtime::{Runtime, context::Ctx, scope},
};
use veloq_buf::{UniformSlot, heap::ThreadMemoryMultiplier};
use veloq_sync::oneshot;

fn run_test<F, R>(f: F) -> R
where
    F: for<'s1, 's2> AsyncFnOnce(Ctx<'s1, 's2>) -> R,
{
    Runtime::builder(UniformSlot::new(ThreadMemoryMultiplier(nz!(4))))
        .worker_count(Some(nz!(2)))
        .scope(f)
        .expect("failed to run scope")
}

#[test]
fn test_sync_oneshot_send_recv() {
    run_test(async |ctx| {
        let state = oneshot::channel();
        let (tx, rx) = state.split();

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

#[test]
fn test_sync_oneshot_drop_sender() {
    run_test(async |ctx| {
        let state = oneshot::channel::<i32>();
        let (tx, rx) = state.split();

        scope!(ctx, async |s| {
            s.spawn_boxed(async move {
                drop(tx);
            });

            let err = rx.await.unwrap_err();
            assert!(matches!(err, oneshot::error::RecvError(_)));
        })
        .await
        .unwrap();
    });
}

#[test]
fn test_sync_oneshot_try_recv() {
    run_test(async |_ctx| {
        let state = oneshot::channel();
        let (tx, mut rx) = state.split();

        assert_eq!(rx.try_recv(), Err(oneshot::error::TryRecvError::Empty));

        tx.send(100).unwrap();

        assert_eq!(rx.try_recv(), Ok(100));
        assert_eq!(rx.try_recv(), Err(oneshot::error::TryRecvError::Closed));
    });
}

#[test]
fn test_sync_oneshot_drop_receiver_notify() {
    run_test(async |ctx| {
        let state = oneshot::channel::<i32>();
        let (tx, rx) = state.split();

        scope!(ctx, async |s| {
            s.spawn_boxed(async move {
                drop(rx);
            });
        })
        .await
        .unwrap();

        assert!(tx.is_closed());
        assert_eq!(tx.send(1), Err(1));
    });
}

#[test]
fn test_sync_oneshot_poll_closed() {
    run_test(async |ctx| {
        let state = oneshot::channel::<()>();
        let (mut tx, rx) = state.split();

        scope!(ctx, async |s| {
            s.spawn_boxed(async move {
                drop(rx);
            });

            tx.closed().await;
            assert!(tx.is_closed());
        })
        .await
        .unwrap();
    });
}

#[test]
fn test_sync_owned_oneshot() {
    run_test(async |ctx| {
        let (tx, rx) = oneshot::owned_channel();

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
