#![cfg(not(feature = "loom"))]

use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
use std::time::Duration;
use tokio::time::sleep;
use veloq_std::sync::Arc;
use veloq_sync::semaphore::{AcquireError, Semaphore, TryAcquireError};

#[tokio::test]
async fn test_semaphore_basic() {
    let sem = Semaphore::new(3);
    assert_eq!(sem.available_permits(), 3);
    assert!(!sem.is_closed());

    {
        let permit = sem.acquire().await.unwrap();
        assert_eq!(sem.available_permits(), 2);
        assert_eq!(permit.num_permits(), 1);
    }
    assert_eq!(sem.available_permits(), 3);

    let p1 = sem.try_acquire().unwrap();
    let p2 = sem.try_acquire_many(2).unwrap();
    assert_eq!(sem.available_permits(), 0);

    assert_eq!(sem.try_acquire().unwrap_err(), TryAcquireError::NoPermits);
    assert_eq!(
        sem.try_acquire_many(1).unwrap_err(),
        TryAcquireError::NoPermits
    );

    drop(p1);
    assert_eq!(sem.available_permits(), 1);
    drop(p2);
    assert_eq!(sem.available_permits(), 3);
}

#[tokio::test]
async fn test_semaphore_acquire_many() {
    let sem = Semaphore::new(5);

    let p1 = sem.acquire_many(3).await.unwrap();
    assert_eq!(sem.available_permits(), 2);
    assert_eq!(p1.num_permits(), 3);

    let p2 = sem.acquire_many(2).await.unwrap();
    assert_eq!(sem.available_permits(), 0);
    assert_eq!(p2.num_permits(), 2);

    let p0 = sem.acquire_many(0).await.unwrap();
    assert_eq!(p0.num_permits(), 0);
    assert_eq!(sem.available_permits(), 0);

    drop(p1);
    assert_eq!(sem.available_permits(), 3);
    drop(p2);
    assert_eq!(sem.available_permits(), 5);
    drop(p0);
    assert_eq!(sem.available_permits(), 5);
}

#[tokio::test]
async fn test_semaphore_contention() {
    let max_permits = 3;
    let sem = Arc::new(Semaphore::new(max_permits));
    let active = Arc::new(AtomicUsize::new(0));
    let mut handles = Vec::new();

    for _ in 0..10 {
        let sem = sem.clone();
        let active = active.clone();
        handles.push(tokio::spawn(async move {
            for _ in 0..20 {
                let permit = sem.acquire().await.unwrap();
                let count = active.fetch_add(1, AtomicOrdering::SeqCst) + 1;
                assert!(
                    count <= max_permits,
                    "Concurrency limit exceeded: {}",
                    count
                );
                sleep(Duration::from_millis(1)).await;
                active.fetch_sub(1, AtomicOrdering::SeqCst);
                drop(permit);
            }
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    assert_eq!(sem.available_permits(), max_permits);
    assert_eq!(active.load(AtomicOrdering::SeqCst), 0);
}

#[tokio::test]
async fn test_semaphore_fifo_order() {
    let sem = Arc::new(Semaphore::new(0));
    let order = Arc::new(tokio::sync::Mutex::new(Vec::new()));
    let mut handles = Vec::new();

    for i in 1..=3 {
        let sem = sem.clone();
        let order = order.clone();
        let (tx, rx) = tokio::sync::oneshot::channel();
        handles.push(tokio::spawn(async move {
            let fut = sem.acquire();
            let _ = tx.send(());
            let _permit = fut.await.unwrap();
            order.lock().await.push(i);
        }));
        rx.await.unwrap();
        tokio::task::yield_now().await;
    }

    // Sequentially release permits
    sem.add_permits(1);
    sleep(Duration::from_millis(10)).await;
    sem.add_permits(1);
    sleep(Duration::from_millis(10)).await;
    sem.add_permits(1);

    for h in handles {
        h.await.unwrap();
    }

    let recorded = order.lock().await.clone();
    assert_eq!(recorded, vec![1, 2, 3]);
}

#[tokio::test]
async fn test_semaphore_cancellation_waiting() {
    let sem = Arc::new(Semaphore::new(1));
    let guard = sem.acquire().await.unwrap();

    let sem1 = sem.clone();
    let (tx1, rx1) = tokio::sync::oneshot::channel();
    let t1 = tokio::spawn(async move {
        let fut = sem1.acquire();
        let _ = tx1.send(());
        fut.await.unwrap();
    });

    rx1.await.unwrap();
    tokio::task::yield_now().await;

    let sem2 = sem.clone();
    let (tx2, rx2) = tokio::sync::oneshot::channel();
    let t2 = tokio::spawn(async move {
        let fut = sem2.acquire();
        let _ = tx2.send(());
        let permit = fut.await.unwrap();
        permit.num_permits()
    });

    rx2.await.unwrap();
    tokio::task::yield_now().await;

    // t1 is waiting, cancel it
    t1.abort();
    let _ = t1.await;

    // Release guard, t2 should acquire it
    drop(guard);

    let num = t2.await.unwrap();
    assert_eq!(num, 1);
}

#[tokio::test]
async fn test_semaphore_cancellation_granted() {
    let sem = Arc::new(Semaphore::new(1));
    let guard = sem.acquire().await.unwrap();

    let sem1 = sem.clone();
    let sem2 = sem.clone();

    let (tx1, rx1) = tokio::sync::oneshot::channel();
    let t1 = tokio::spawn(async move {
        let fut = sem1.acquire();
        let _ = tx1.send(());
        fut.await.unwrap();
    });

    rx1.await.unwrap();
    tokio::task::yield_now().await;

    let (tx2, rx2) = tokio::sync::oneshot::channel();
    let t2 = tokio::spawn(async move {
        let fut = sem2.acquire();
        let _ = tx2.send(());
        let _permit = fut.await.unwrap();
    });

    rx2.await.unwrap();
    tokio::task::yield_now().await;

    // Dropping guard grants permit to t1
    drop(guard);

    // Abort t1 while holding STATE_GRANTED
    t1.abort();
    let _ = t1.await;

    // t2 should receive the cascaded permit
    t2.await.unwrap();
    assert_eq!(sem.available_permits(), 1);
}

#[tokio::test]
async fn test_semaphore_cancellation_head_unblocks_tail() {
    let sem = Arc::new(Semaphore::new(0));

    let sem1 = sem.clone();
    let (tx1, rx1) = tokio::sync::oneshot::channel();
    let t1 = tokio::spawn(async move {
        let fut = sem1.acquire_many(5);
        let _ = tx1.send(());
        fut.await.unwrap();
    });

    rx1.await.unwrap();
    tokio::task::yield_now().await;

    let sem2 = sem.clone();
    let (tx2, rx2) = tokio::sync::oneshot::channel();
    let t2 = tokio::spawn(async move {
        let fut = sem2.acquire_many(2);
        let _ = tx2.send(());
        let permit = fut.await.unwrap();
        permit.num_permits()
    });

    rx2.await.unwrap();
    tokio::task::yield_now().await;

    // Add 3 permits: not enough for t1 (needs 5), but enough for t2 (needs 2)
    sem.add_permits(3);
    sleep(Duration::from_millis(10)).await;

    // t1 is canceled, freeing up the front of the queue
    t1.abort();
    let _ = t1.await;

    // t2 should now be unblocked with the existing 3 permits!
    let num = t2.await.unwrap();
    assert_eq!(num, 2);
    assert_eq!(sem.available_permits(), 3);
}

#[tokio::test]
async fn test_semaphore_close() {
    let sem = Arc::new(Semaphore::new(1));
    let permit = sem.acquire().await.unwrap();

    let sem1 = sem.clone();
    let (tx, rx) = tokio::sync::oneshot::channel();
    let (tx_res, rx_res) = tokio::sync::oneshot::channel();
    let t1 = tokio::spawn(async move {
        let fut = sem1.acquire();
        let _ = tx.send(());
        let res = fut.await;
        let _ = tx_res.send(res.is_err());
    });

    rx.await.unwrap();
    tokio::task::yield_now().await;

    // Close the semaphore
    sem.close();
    assert!(sem.is_closed());

    // t1 should wake up with AcquireError
    t1.await.unwrap();
    assert!(rx_res.await.unwrap());

    // New acquires should fail immediately
    assert_eq!(sem.acquire().await.unwrap_err(), AcquireError);
    assert_eq!(sem.try_acquire().unwrap_err(), TryAcquireError::Closed);

    // Dropping existing permit releases permit back, but semaphore remains closed
    drop(permit);
    assert_eq!(sem.available_permits(), 1);
    assert_eq!(sem.try_acquire().unwrap_err(), TryAcquireError::Closed);
}

#[tokio::test]
async fn test_semaphore_forget() {
    let sem = Semaphore::new(5);
    let permit = sem.acquire_many(2).await.unwrap();
    assert_eq!(sem.available_permits(), 3);

    permit.forget();
    assert_eq!(sem.available_permits(), 3);

    sem.forget_permits(1);
    assert_eq!(sem.available_permits(), 2);
}

#[tokio::test]
async fn test_semaphore_add_permits() {
    let sem = Arc::new(Semaphore::new(0));
    let sem2 = sem.clone();

    let handle = tokio::spawn(async move {
        let permit = sem2.acquire_many(3).await.unwrap();
        assert_eq!(permit.num_permits(), 3);
    });

    sleep(Duration::from_millis(10)).await;
    sem.add_permits(3);

    handle.await.unwrap();
    assert_eq!(sem.available_permits(), 3);
}
