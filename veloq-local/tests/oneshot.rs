use tokio::task;
use veloq_local::oneshot;

#[tokio::test]
async fn test_send_recv() {
    let local = task::LocalSet::new();
    local
        .run_until(async move {
            let (tx, rx) = oneshot::channel();

            task::spawn_local(async move {
                tx.send(42).unwrap();
            });

            assert_eq!(rx.await.unwrap(), 42);
        })
        .await;
}

#[tokio::test]
async fn test_tx_closed() {
    let (tx, rx) = oneshot::channel::<i32>();
    drop(tx);
    assert!(rx.await.is_err());
}

#[tokio::test]
async fn test_rx_closed() {
    let (tx, rx) = oneshot::channel::<i32>();

    assert!(!tx.is_closed());
    drop(rx);
    assert!(tx.is_closed());

    // Attempt to send should fail
    assert_eq!(tx.send(10), Err(10));
}

#[tokio::test]
async fn test_try_recv() {
    let (tx, mut rx) = oneshot::channel();

    assert!(rx.try_recv().unwrap().is_none());

    tx.send(100).unwrap();

    assert_eq!(rx.try_recv().unwrap(), Some(100));

    assert!(rx.try_recv().is_err());
}

#[tokio::test]
async fn test_drop_tx_notify() {
    let local = task::LocalSet::new();
    local
        .run_until(async move {
            let (tx, rx) = oneshot::channel::<i32>();

            let handle = task::spawn_local(rx);

            // Drop tx without sending
            drop(tx);

            let res = handle.await.unwrap();
            assert!(res.is_err());
        })
        .await;
}

#[tokio::test]
async fn test_send_before_recv() {
    let (tx, rx) = oneshot::channel();
    tx.send("hello").unwrap();
    assert_eq!(rx.await.unwrap(), "hello");
}
