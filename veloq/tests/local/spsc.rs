use veloq::local::spsc;
use veloq::runtime::Runtime;
use veloq_buf::heap::ThreadMemoryMultiplier;
use veloq_buf::{UniformSlot, nz};
use veloq_runtime::task::yield_now;

fn create_runtime() -> Runtime<UniformSlot> {
    Runtime::builder(UniformSlot::new(ThreadMemoryMultiplier(nz!(4))))
        .worker_count(Some(nz!(1)))
        .build()
        .expect("failed to build runtime")
}

#[test]
fn test_unbounded_basic() {
    let runtime = create_runtime();
    runtime
        .block_on(async |ctx| {
            let state = spsc::unbounded();
            let (tx, rx) = state.split();

            ctx.scope(async |s| {
                s.spawn_boxed_local(async move {
                    for i in 0..10 {
                        tx.send(i).await.unwrap();
                    }
                });

                let mut expected = 0;
                while let Some(val) = rx.recv().await {
                    assert_eq!(val, expected);
                    expected += 1;
                }
                assert_eq!(expected, 10);
            })
            .await
            .unwrap();
        })
        .unwrap();
}

#[test]
fn test_bounded_basic() {
    let runtime = create_runtime();
    runtime
        .block_on(async |ctx| {
            let state = spsc::bounded(5);
            let (tx, rx) = state.split();

            ctx.scope(async |s| {
                s.spawn_boxed_local(async move {
                    for i in 0..10 {
                        tx.send(i).await.unwrap();
                    }
                });

                for i in 0..10 {
                    let val = rx.recv().await.expect("Should receive value");
                    assert_eq!(val, i);
                }
                assert!(rx.recv().await.is_none());
            })
            .await
            .unwrap();
        })
        .unwrap();
}

#[test]
fn test_sender_drop_closes_channel() {
    let runtime = create_runtime();
    runtime
        .block_on(async |ctx| {
            let state = spsc::unbounded::<()>();
            let (tx, rx) = state.split();
            ctx.scope(async |s| {
                s.spawn_boxed_local(async move {
                    drop(tx);
                });

                assert!(rx.recv().await.is_none());
            })
            .await
            .unwrap();
        })
        .unwrap();
}

#[test]
fn test_receiver_drop_errors_sender() {
    let runtime = create_runtime();
    runtime
        .block_on(async |ctx| {
            let state = spsc::bounded::<i32>(1);
            let (tx, rx) = state.split();

            tx.send(1).await.unwrap();

            ctx.scope(async |s| {
                s.spawn_boxed_local(async move {
                    drop(rx);
                });

                yield_now().await;

                match tx.send(2).await {
                    Err(spsc::SendError::Closed(val)) => assert_eq!(val, 2),
                    _ => panic!("Should return Closed error"),
                }
            })
            .await
            .unwrap();
        })
        .unwrap();
}

#[test]
fn test_bounded_backpressure() {
    let runtime = create_runtime();
    runtime
        .block_on(async |ctx| {
            let state = spsc::bounded(1);
            let (tx, rx) = state.split();

            ctx.scope(async |s| {
                s.spawn_boxed_local(async move {
                    tx.send(1).await.unwrap();
                    tx.send(2).await.unwrap();
                });

                yield_now().await;

                let val1 = rx.recv().await;
                assert_eq!(val1, Some(1));

                yield_now().await;

                let val2 = rx.recv().await;
                assert_eq!(val2, Some(2));
            })
            .await
            .unwrap();
        })
        .unwrap();
}

#[test]
fn test_stream_conversion() {
    use futures_util::Stream;
    use std::pin::Pin;

    let runtime = create_runtime();
    runtime
        .block_on(async |ctx| {
            let state = spsc::unbounded();
            let (tx, rx) = state.split();

            ctx.scope(async |s| {
                s.spawn_boxed_local(async move {
                    tx.send(100).await.unwrap();
                    tx.send(200).await.unwrap();
                });

                let mut stream = Box::pin(rx.stream());

                async fn next_item<S: Stream<Item = i32> + Unpin>(s: &mut S) -> Option<i32> {
                    use std::future::poll_fn;
                    poll_fn(|cx| Pin::new(&mut *s).poll_next(cx)).await
                }

                assert_eq!(next_item(&mut stream).await, Some(100));
                assert_eq!(next_item(&mut stream).await, Some(200));
                assert_eq!(next_item(&mut stream).await, None);
            })
            .await
            .unwrap();
        })
        .unwrap();
}

#[test]
fn test_zst() {
    let runtime = create_runtime();
    runtime
        .block_on(async |ctx| {
            let state = spsc::unbounded::<()>();
            let (tx, rx) = state.split();

            ctx.scope(async |s| {
                s.spawn_boxed_local(async move {
                    for _ in 0..100 {
                        tx.send(()).await.unwrap();
                    }
                });

                for _ in 0..100 {
                    assert_eq!(rx.recv().await, Some(()));
                }
                assert!(rx.recv().await.is_none());
            })
            .await
            .unwrap();
        })
        .unwrap();
}

#[test]
fn test_try_recv() {
    let runtime = create_runtime();
    runtime
        .block_on(async |_ctx| {
            let state = spsc::unbounded();
            let (tx, rx) = state.split();

            assert_eq!(rx.try_recv(), Err(spsc::TryRecvError::Empty));

            tx.send(100).await.unwrap();

            assert_eq!(rx.try_recv(), Ok(100));

            // After consuming, it should be empty again
            assert_eq!(rx.try_recv(), Err(spsc::TryRecvError::Empty));

            drop(tx);
            // After drop, it should be closed
            assert_eq!(rx.try_recv(), Err(spsc::TryRecvError::Closed));
        })
        .unwrap();
}

#[test]
fn test_owned_spsc() {
    let runtime = create_runtime();
    runtime
        .block_on(async |ctx| {
            let (tx, rx) = spsc::owned_bounded(5);

            ctx.scope(async |s| {
                s.spawn_boxed_local(async move {
                    for i in 0..10 {
                        tx.send(i).await.unwrap();
                    }
                });

                for i in 0..10 {
                    let val = rx.recv().await.expect("Should receive value");
                    assert_eq!(val, i);
                }
            })
            .await
            .unwrap();
        })
        .unwrap();
}
