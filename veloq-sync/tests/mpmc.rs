#![cfg(not(feature = "loom"))]
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use veloq_sync::mpmc;

#[tokio::test]
async fn test_mpmc_unbounded_simple() {
    let (tx, rx) = mpmc::unbounded();

    tx.send(1).await.unwrap();
    tx.send(2).await.unwrap();

    let rx2 = rx.clone();

    assert_eq!(rx.recv().await.unwrap(), 1);
    assert_eq!(rx2.recv().await.unwrap(), 2);
}

#[tokio::test]
async fn test_mpmc_unbounded_concurrent() {
    let (tx, rx) = mpmc::unbounded();
    let count = 1000;

    // Multiple senders
    for _ in 0..10 {
        let tx = tx.clone();
        tokio::spawn(async move {
            for i in 0..count {
                tx.send(i).await.unwrap();
            }
        });
    }
    drop(tx); // drop the original sender so receiver knows when to stop eventually? 
    // Wait, GenericReceiver.recv() only returns Error if disconnected.
    // Disconnection happens when all senders drop.

    // Multiple receivers
    let mut tasks = vec![];
    let total_received = Arc::new(AtomicUsize::new(0));

    for _ in 0..10 {
        let rx = rx.clone();
        let total = total_received.clone();
        tasks.push(tokio::spawn(async move {
            while rx.recv().await.is_ok() {
                total.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }

    for task in tasks {
        task.await.unwrap();
    }

    assert_eq!(total_received.load(Ordering::Relaxed), count * 10);
}

#[tokio::test]
async fn test_mpmc_bounded_capacity() {
    let (tx, rx) = mpmc::bounded(1);

    tx.send(1).await.unwrap();

    let tx_clone = tx.clone();
    let handle = tokio::spawn(async move {
        tx_clone.send(2).await.unwrap();
    });

    tokio::time::sleep(Duration::from_millis(10)).await;
    // Should verify tx_clone hasn't finished?

    assert_eq!(rx.recv().await.unwrap(), 1);

    handle.await.unwrap(); // Now it should finish
    assert_eq!(rx.recv().await.unwrap(), 2);
}

#[tokio::test]
async fn test_mpmc_bounded_multi_consumer() {
    let (tx, rx) = mpmc::bounded(5);

    // Fill buffer
    for i in 0..5 {
        tx.send(i).await.unwrap();
    }

    // Spawn consumers
    let c1 = rx.clone();
    let c2 = rx.clone();

    let h1 = tokio::spawn(async move { c1.recv().await.unwrap() });

    let h2 = tokio::spawn(async move { c2.recv().await.unwrap() });

    // They should pick up values
    let r1 = h1.await.unwrap();
    let r2 = h2.await.unwrap();

    assert!(r1 < 5);
    assert!(r2 < 5);
    assert_ne!(r1, r2);
}

#[tokio::test]
async fn test_mpmc_bounded_close_sender() {
    let (tx, rx) = mpmc::bounded::<i32>(10);
    let h = tokio::spawn(async move { rx.recv().await });

    drop(tx);
    // rx should return error
    h.await.unwrap().unwrap_err();
}

#[tokio::test]
async fn test_mpmc_bounded_close_receiver() {
    let (tx, rx) = mpmc::bounded(1);
    tx.send(1).await.unwrap();

    let h = tokio::spawn(async move {
        tx.send(2).await.unwrap_err(); // Should fail
    });

    // Drop all receivers
    drop(rx);

    h.await.unwrap();
}

#[tokio::test]
async fn test_mpmc_try_send_recv() {
    let (tx, rx) = mpmc::bounded(1);

    tx.try_send(1).unwrap();
    assert!(tx.try_send(2).is_err());

    assert_eq!(rx.try_recv().unwrap(), 1);
    assert!(rx.try_recv().is_err());
}
