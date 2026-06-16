use std::time::Duration;
use veloq_sync::mpsc;
use veloq::runtime::Runtime;
use veloq_buf::UniformSlot;
use veloq_buf::heap::ThreadMemoryMultiplier;
use veloq_buf::nz;
use veloq_runtime::task::yield_now;

fn create_runtime() -> Runtime<UniformSlot> {
    Runtime::builder(UniformSlot::new(ThreadMemoryMultiplier(nz!(4))))
        .worker_count(Some(nz!(2)))
        .build()
        .expect("failed to build runtime")
}

#[test]
fn test_sync_unbounded_simple() {
    let runtime = create_runtime();
    runtime
        .block_on(async |ctx| {
            let state = mpsc::unbounded();
            let (tx, mut rx) = state.split();

            tx.send(1).unwrap();
            tx.send(2).unwrap();

            assert_eq!(rx.recv().await, Some(1));
            assert_eq!(rx.recv().await, Some(2));
        })
        .unwrap();
}

#[test]
fn test_sync_unbounded_multi_thread() {
    let runtime = create_runtime();
    runtime
        .block_on(async |ctx| {
            let state = mpsc::unbounded();
            let (tx, mut rx) = state.split();

            ctx.scope(async |s| {
                for i in 0..10 {
                    let tx = tx.clone();
                    s.spawn_boxed(async move {
                        tx.send(i).unwrap();
                    });
                }
                drop(tx);

                let mut sum = 0;
                for _ in 0..10 {
                    if let Some(val) = rx.recv().await {
                        sum += val;
                    }
                }
                assert_eq!(sum, 45);
            })
            .await
            .unwrap();
        })
        .unwrap();
}

#[test]
fn test_sync_unbounded_stream() {
    use futures_util::Stream;
    use std::pin::Pin;

    let runtime = create_runtime();
    runtime
        .block_on(async |ctx| {
            let state = mpsc::unbounded();
            let (tx, rx) = state.split();

            ctx.scope(async |s| {
                s.spawn_boxed(async move {
                    for i in 0..5 {
                        tx.send(i).unwrap();
                    }
                });

                let mut stream = Box::pin(rx);
                async fn next_item<S: Stream<Item = i32> + Unpin>(s: &mut S) -> Option<i32> {
                    use std::future::poll_fn;
                    poll_fn(|cx| Pin::new(&mut *s).poll_next(cx)).await
                }

                assert_eq!(next_item(&mut stream).await, Some(0));
                assert_eq!(next_item(&mut stream).await, Some(1));
                assert_eq!(next_item(&mut stream).await, Some(2));
                assert_eq!(next_item(&mut stream).await, Some(3));
                assert_eq!(next_item(&mut stream).await, Some(4));
            })
            .await
            .unwrap();
        })
        .unwrap();
}

#[test]
fn test_sync_bounded_capacity() {
    let runtime = create_runtime();
    runtime
        .block_on(async |ctx| {
            let state = mpsc::bounded(1);
            let (tx, mut rx) = state.split();

            tx.send(1).await.unwrap();

            ctx.scope(async |s| {
                let tx_clone = tx.clone();
                s.spawn_boxed(async move {
                    tx_clone.send(2).await.unwrap();
                });

                yield_now().await;

                assert_eq!(rx.recv().await, Some(1));
                assert_eq!(rx.recv().await, Some(2));
            })
            .await
            .unwrap();
        })
        .unwrap();
}

#[test]
fn test_sync_bounded_drop_receiver() {
    let runtime = create_runtime();
    runtime
        .block_on(async |ctx| {
            let state = mpsc::bounded(1);
            let (tx, rx) = state.split();
            tx.send(1).await.unwrap();

            ctx.scope(async |s| {
                s.spawn_boxed(async move {
                    let result = tx.send(2).await;
                    assert!(result.is_err());
                });

                yield_now().await;
                drop(rx);
            })
            .await
            .unwrap();
        })
        .unwrap();
}

#[test]
fn test_sync_owned_mpsc() {
    let runtime = create_runtime();
    runtime
        .block_on(async |ctx| {
            let (tx, rx) = mpsc::owned_bounded(5);

            ctx.scope(async |s| {
                s.spawn_boxed(async move {
                    for i in 0..10 {
                        tx.send(i).await.unwrap();
                    }
                });

                let mut rx = rx;
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
