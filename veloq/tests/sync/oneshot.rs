use veloq::runtime::{Runtime, scope};
use veloq_buf::UniformSlot;
use veloq_buf::heap::ThreadMemoryMultiplier;
use veloq_buf::nz;
use veloq_sync::oneshot;

fn create_runtime() -> Runtime<UniformSlot> {
    Runtime::builder(UniformSlot::new(ThreadMemoryMultiplier(nz!(4))))
        .worker_count(Some(nz!(2)))
        .build()
        .expect("failed to build runtime")
}

#[test]
fn test_sync_oneshot_send_recv() {
    let runtime = create_runtime();
    runtime
        .block_on(async |ctx| {
            let state = oneshot::channel();
            let (tx, rx) = state.split();

            scope!(ctx.scope, async |s| {
                s.spawn_boxed(async move {
                    tx.send(42).unwrap();
                });

                assert_eq!(rx.await.unwrap(), 42);
            })
            .await
            .unwrap();
        })
        .unwrap();
}

#[test]
fn test_sync_oneshot_drop_sender() {
    let runtime = create_runtime();
    runtime
        .block_on(async |ctx| {
            let state = oneshot::channel::<i32>();
            let (tx, rx) = state.split();

            scope!(ctx.scope, async |s| {
                s.spawn_boxed(async move {
                    drop(tx);
                });

                let err = rx.await.unwrap_err();
                assert!(matches!(err, oneshot::error::RecvError(_)));
            })
            .await
            .unwrap();
        })
        .unwrap();
}

#[test]
fn test_sync_oneshot_try_recv() {
    let runtime = create_runtime();
    runtime
        .block_on(async |_ctx| {
            let state = oneshot::channel();
            let (tx, mut rx) = state.split();

            assert_eq!(rx.try_recv(), Err(oneshot::error::TryRecvError::Empty));

            tx.send(100).unwrap();

            assert_eq!(rx.try_recv(), Ok(100));
            assert_eq!(rx.try_recv(), Err(oneshot::error::TryRecvError::Closed));
        })
        .unwrap();
}

#[test]
fn test_sync_oneshot_drop_receiver_notify() {
    let runtime = create_runtime();
    runtime
        .block_on(async |ctx| {
            let state = oneshot::channel::<i32>();
            let (tx, rx) = state.split();

            scope!(ctx.scope, async |s| {
                s.spawn_boxed(async move {
                    drop(rx);
                });
            })
            .await
            .unwrap();

            assert!(tx.is_closed());
            assert_eq!(tx.send(1), Err(1));
        })
        .unwrap();
}

#[test]
fn test_sync_oneshot_poll_closed() {
    let runtime = create_runtime();
    runtime
        .block_on(async |ctx| {
            let state = oneshot::channel::<()>();
            let (mut tx, rx) = state.split();

            scope!(ctx.scope, async |s| {
                s.spawn_boxed(async move {
                    drop(rx);
                });

                tx.closed().await;
                assert!(tx.is_closed());
            })
            .await
            .unwrap();
        })
        .unwrap();
}

#[test]
fn test_sync_owned_oneshot() {
    let runtime = create_runtime();
    runtime
        .block_on(async |ctx| {
            let (tx, rx) = oneshot::owned_channel();

            scope!(ctx.scope, async |s| {
                s.spawn_boxed(async move {
                    tx.send(42).unwrap();
                });

                assert_eq!(rx.await.unwrap(), 42);
            })
            .await
            .unwrap();
        })
        .unwrap();
}
