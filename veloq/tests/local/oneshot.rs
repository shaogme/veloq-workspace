use veloq::local::oneshot;
use veloq::runtime::Runtime;
use veloq_buf::UniformSlot;
use veloq_buf::heap::ThreadMemoryMultiplier;
use veloq_buf::nz;

fn create_runtime() -> Runtime<UniformSlot> {
    Runtime::builder(UniformSlot::new(ThreadMemoryMultiplier(nz!(4))))
        .worker_count(Some(nz!(1)))
        .build()
        .expect("failed to build runtime")
}

#[test]
fn test_send_recv() {
    let runtime = create_runtime();
    runtime
        .block_on(async |ctx| {
            let state = oneshot::channel();
            let (tx, rx) = state.split();

            ctx.scope(async |s| {
                s.spawn_boxed_local(async move {
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
fn test_tx_closed() {
    let runtime = create_runtime();
    runtime
        .block_on(async |_ctx| {
            let state = oneshot::channel::<i32>();
            let (tx, rx) = state.split();
            drop(tx);
            assert!(rx.await.is_err());
        })
        .unwrap();
}

#[test]
fn test_rx_closed() {
    let runtime = create_runtime();
    runtime
        .block_on(async |_ctx| {
            let state = oneshot::channel::<i32>();
            let (tx, rx) = state.split();

            assert!(!tx.is_closed());
            drop(rx);
            assert!(tx.is_closed());

            // Attempt to send should fail
            assert_eq!(tx.send(10), Err(10));
        })
        .unwrap();
}

#[test]
fn test_try_recv() {
    let runtime = create_runtime();
    runtime
        .block_on(async |_ctx| {
            let state = oneshot::channel();
            let (tx, rx) = state.split();

            assert_eq!(rx.try_recv(), Err(oneshot::TryRecvError::Empty));

            tx.send(100).unwrap();

            assert_eq!(rx.try_recv(), Ok(100));

            assert_eq!(rx.try_recv(), Err(oneshot::TryRecvError::Closed));
        })
        .unwrap();
}

#[test]
fn test_drop_tx_notify() {
    let runtime = create_runtime();
    runtime
        .block_on(async |ctx| {
            let state = oneshot::channel::<i32>();
            let (tx, rx) = state.split();

            ctx.scope(async |s| {
                let handle = s.spawn_boxed_local(rx);

                // Drop tx without sending
                drop(tx);

                let res = handle.await.unwrap();
                assert!(res.is_err());
            })
            .await
            .unwrap();
        })
        .unwrap();
}

#[test]
fn test_send_before_recv() {
    let runtime = create_runtime();
    runtime
        .block_on(async |_ctx| {
            let state = oneshot::channel();
            let (tx, rx) = state.split();
            tx.send("hello").unwrap();
            assert_eq!(rx.await.unwrap(), "hello");
        })
        .unwrap();
}

#[test]
fn test_owned_oneshot() {
    let runtime = create_runtime();
    runtime
        .block_on(async |ctx| {
            let (tx, rx) = oneshot::owned_channel();

            ctx.scope(async |s| {
                s.spawn_boxed_local(async move {
                    tx.send(42).unwrap();
                });

                assert_eq!(rx.await.unwrap(), 42);
            })
            .await
            .unwrap();
        })
        .unwrap();
}
