use tokio::task;
use veloq_local::mpsc;

#[tokio::test]
async fn test_unbounded_basic() {
    let local = task::LocalSet::new();
    local
        .run_until(async {
            let (tx, rx) = mpsc::new_unbounded();

            task::spawn_local(async move {
                for i in 0..10 {
                    tx.send(i).await.unwrap();
                }
            });

            let mut expected = 0;
            while let Some(val) = rx.recv().await {
                assert_eq!(val, expected);
                expected += 1;
                if expected == 10 {
                    break;
                }
            }
        })
        .await;
}

#[tokio::test]
async fn test_bounded_basic() {
    let local = task::LocalSet::new();
    local
        .run_until(async {
            let (tx, rx) = mpsc::new_bounded(5);

            task::spawn_local(async move {
                for i in 0..10 {
                    tx.send(i).await.unwrap();
                }
            });

            for i in 0..10 {
                let val = rx.recv().await.expect("Should receive value");
                assert_eq!(val, i);
            }
        })
        .await;
}

#[tokio::test]
async fn test_multiple_senders() {
    let local = task::LocalSet::new();
    local
        .run_until(async {
            let (tx, rx) = mpsc::new_unbounded();

            for i in 0..5 {
                let tx = tx.clone();
                task::spawn_local(async move {
                    tx.send(i).await.unwrap();
                });
            }
            drop(tx); // Close the original sender

            let mut count = 0;
            while rx.recv().await.is_some() {
                count += 1;
            }
            assert_eq!(count, 5);
        })
        .await;
}

#[tokio::test]
async fn test_sender_drop_closes_channel() {
    let local = task::LocalSet::new();
    local
        .run_until(async {
            let (tx, rx) = mpsc::new_unbounded::<()>();
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
        .run_until(async {
            let (tx, rx) = mpsc::new_bounded::<i32>(1);

            // Fill the channel first to make sure next send might block or wait
            tx.send(1).await.unwrap();

            task::spawn_local(async move {
                drop(rx);
            });

            task::yield_now().await;

            match tx.send(2).await {
                Err(mpsc::SendError::Closed(val)) => assert_eq!(val, 2),
                _ => panic!("Should return Closed error"),
            }
        })
        .await;
}

#[tokio::test]
async fn test_bounded_backpressure() {
    let local = task::LocalSet::new();
    local
        .run_until(async {
            let (tx, rx) = mpsc::new_bounded(1);

            // This task will fill the channel and then block on the next send
            let tx_clone = tx.clone();
            task::spawn_local(async move {
                tx_clone.send(1).await.unwrap();
                // This one should block until receiver pops
                tx_clone.send(2).await.unwrap();
            });

            // Allow the spawned task to run and fill the channel
            task::yield_now().await;

            // Receiver takes one
            let val1 = rx.recv().await;
            assert_eq!(val1, Some(1));

            // Now the second send can proceed.
            let val2 = rx.recv().await;
            assert_eq!(val2, Some(2));
        })
        .await;
}

#[tokio::test]
async fn test_stream_conversion() {
    use futures_core::Stream; // Ensure we have the trait in scope
    use std::pin::Pin;

    let local = task::LocalSet::new();
    local
        .run_until(async {
            let (tx, rx) = mpsc::new_unbounded();

            task::spawn_local(async move {
                tx.send(100).await.unwrap();
                tx.send(200).await.unwrap();
            });

            // Manually poll the stream since we don't have StreamExt easily available
            let mut stream = Box::pin(rx.stream());

            // Let's utilize a simple helper generic function to await the stream next
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
