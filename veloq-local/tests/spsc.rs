use tokio::task;
use veloq_local::spsc;

#[tokio::test]
async fn test_unbounded_basic() {
    let local = task::LocalSet::new();
    local
        .run_until(async move {
            let (tx, rx) = spsc::unbounded();

            task::spawn_local(async move {
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
        .await;
}

#[tokio::test]
async fn test_bounded_basic() {
    let local = task::LocalSet::new();
    local
        .run_until(async move {
            let (tx, rx) = spsc::bounded(5);

            task::spawn_local(async move {
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
        .await;
}

#[tokio::test]
async fn test_sender_drop_closes_channel() {
    let local = task::LocalSet::new();
    local
        .run_until(async move {
            let (tx, rx) = spsc::unbounded::<()>();
            task::spawn_local(async move {
                drop(tx);
            });

            assert!(rx.recv().await.is_none());
        })
        .await;
}

#[tokio::test]
async fn test_receiver_drop_errors_sender() {
    let local = task::LocalSet::new();
    local
        .run_until(async move {
            let (tx, rx) = spsc::bounded::<i32>(1);

            tx.send(1).await.unwrap();

            task::spawn_local(async move {
                drop(rx);
            });

            task::yield_now().await;

            match tx.send(2).await {
                Err(spsc::SendError::Closed(val)) => assert_eq!(val, 2),
                _ => panic!("Should return Closed error"),
            }
        })
        .await;
}

#[tokio::test]
async fn test_bounded_backpressure() {
    let local = task::LocalSet::new();
    local
        .run_until(async move {
            let (tx, rx) = spsc::bounded(1);

            task::spawn_local(async move {
                tx.send(1).await.unwrap();
                tx.send(2).await.unwrap();
            });

            task::yield_now().await;

            let val1 = rx.recv().await;
            assert_eq!(val1, Some(1));

            task::yield_now().await;

            let val2 = rx.recv().await;
            assert_eq!(val2, Some(2));
        })
        .await;
}

#[tokio::test]
async fn test_stream_conversion() {
    use futures_core::Stream;
    use std::pin::Pin;

    let local = task::LocalSet::new();
    local
        .run_until(async move {
            let (tx, rx) = spsc::unbounded();

            task::spawn_local(async move {
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
        .await;
}

#[tokio::test]
async fn test_zst() {
    let local = task::LocalSet::new();
    local
        .run_until(async move {
            let (tx, rx) = spsc::unbounded::<()>();

            task::spawn_local(async move {
                for _ in 0..100 {
                    tx.send(()).await.unwrap();
                }
            });

            for _ in 0..100 {
                assert_eq!(rx.recv().await, Some(()));
            }
            assert!(rx.recv().await.is_none());
        })
        .await;
}

#[tokio::test]
async fn test_try_recv() {
    let (tx, rx) = spsc::unbounded();

    assert_eq!(rx.try_recv(), Err(spsc::TryRecvError::Empty));

    tx.send(100).await.unwrap();

    assert_eq!(rx.try_recv(), Ok(100));

    // After consuming, it should be empty again
    assert_eq!(rx.try_recv(), Err(spsc::TryRecvError::Empty));

    drop(tx);
    // After drop, it should be closed
    assert_eq!(rx.try_recv(), Err(spsc::TryRecvError::Closed));
}
