#![cfg(not(feature = "loom"))]
use std::time::Duration;
use veloq_sync::mpsc;

#[tokio::test]
async fn test_unbounded_simple() {
    let (tx, mut rx) = mpsc::unbounded();

    tx.send(1).unwrap();
    tx.send(2).unwrap();

    assert_eq!(rx.recv().await, Some(1));
    assert_eq!(rx.recv().await, Some(2));
}

#[tokio::test]
async fn test_unbounded_multi_thread() {
    let (tx, mut rx) = mpsc::unbounded();

    for i in 0..10 {
        let tx = tx.clone();
        tokio::spawn(async move {
            tx.send(i).unwrap();
        });
    }

    let mut sum = 0;
    for _ in 0..10 {
        if let Some(val) = rx.recv().await {
            sum += val;
        }
    }
    assert_eq!(sum, 45);
}

#[tokio::test]
async fn test_unbounded_stream() {
    let (tx, mut rx) = mpsc::unbounded();

    tokio::spawn(async move {
        for i in 0..5 {
            tx.send(i).unwrap();
        }
    }); // tx drops here

    let mut v = Vec::new();
    while let Some(msg) = rx.recv().await {
        v.push(msg);
    }

    assert_eq!(v, vec![0, 1, 2, 3, 4]);
}

#[tokio::test]
async fn test_bounded_capacity() {
    let (tx, mut rx) = mpsc::bounded(1);

    // First send should be instant
    tx.send(1).await.unwrap();

    // Second send should block
    let tx_clone = tx.clone();
    let handle = tokio::spawn(async move {
        tx_clone.send(2).await.unwrap();
    });

    // Give generic time for the spawn to hit the full channel
    tokio::time::sleep(Duration::from_millis(10)).await;

    // Should verify it hasn't sent yet? (hard to deterministic test without mocking, but if we recv we should get 1 then 2)
    assert_eq!(rx.recv().await, Some(1));

    // Now receiving 1 should wake up the second sender
    assert_eq!(rx.recv().await, Some(2));

    handle.await.unwrap();
}

#[tokio::test]
async fn test_bounded_drop_receiver() {
    let (tx, rx) = mpsc::bounded(1);
    tx.send(1).await.unwrap();

    // Fill it so next send blocks
    let handle = tokio::spawn(async move {
        let result = tx.send(2).await;
        assert!(result.is_err()); // Receiver dropped
    });

    tokio::time::sleep(Duration::from_millis(10)).await;
    drop(rx);

    handle.await.unwrap();
}

#[tokio::test]
async fn test_bounded_stream() {
    let (tx, mut rx) = mpsc::bounded(2);

    tokio::spawn(async move {
        for i in 0..10 {
            tx.send(i).await.unwrap();
        }
    });

    let mut received = 0;
    while rx.recv().await.is_some() {
        received += 1;
    }
    // Receiver keeps going until all senders drop.
    // Wait, the sender loop finishes, tx drops. rx.recv() returns None.
    assert_eq!(received, 10);
}
