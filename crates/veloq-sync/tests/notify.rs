#![cfg(not(feature = "loom"))]

use std::future::pending;
use std::pin::pin;
use std::time::Duration;
use tokio::time::sleep;
use veloq_std::sync::Arc;
use veloq_std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use veloq_sync::notify::Notify;

#[tokio::test]
async fn test_notify_one_basic() {
    let notify = Arc::new(Notify::new());
    let notify2 = notify.clone();

    let handle = tokio::spawn(async move {
        notify2.notified().await;
        42
    });

    sleep(Duration::from_millis(10)).await;
    notify.notify_one();

    let res = handle.await.unwrap();
    assert_eq!(res, 42);
}

#[tokio::test]
async fn test_notify_one_permit() {
    let notify = Notify::new();

    // Notify before waiting
    notify.notify_one();

    // Should complete immediately without blocking
    notify.notified().await;
}

#[tokio::test]
async fn test_notify_one_single_permit() {
    let notify = Arc::new(Notify::new());

    // Multiple notify_one calls should only produce a single permit
    notify.notify_one();
    notify.notify_one();
    notify.notify_one();

    // First await consumes the permit
    notify.notified().await;

    // Second await should block because only 1 permit was stored
    let notify2 = notify.clone();
    let completed = Arc::new(AtomicBool::new(false));
    let completed2 = completed.clone();

    let handle = tokio::spawn(async move {
        notify2.notified().await;
        completed2.store(true, Ordering::Release);
    });

    sleep(Duration::from_millis(20)).await;
    assert!(!completed.load(Ordering::Acquire));

    // Now unpark it
    notify.notify_one();
    handle.await.unwrap();
    assert!(completed.load(Ordering::Acquire));
}

#[tokio::test]
async fn test_notify_multi_waiters_fifo() {
    let notify = Arc::new(Notify::new());
    let counter = Arc::new(AtomicUsize::new(0));

    let mut handles = Vec::new();
    for i in 0..3 {
        let n = notify.clone();
        let c = counter.clone();
        handles.push(tokio::spawn(async move {
            n.notified().await;
            c.store(i + 1, Ordering::Release);
        }));
        // Small sleep to ensure deterministic arrival order
        sleep(Duration::from_millis(10)).await;
    }

    notify.notify_one();
    sleep(Duration::from_millis(10)).await;
    assert_eq!(counter.load(Ordering::Acquire), 1);

    notify.notify_one();
    sleep(Duration::from_millis(10)).await;
    assert_eq!(counter.load(Ordering::Acquire), 2);

    notify.notify_one();
    sleep(Duration::from_millis(10)).await;
    assert_eq!(counter.load(Ordering::Acquire), 3);

    for h in handles {
        h.await.unwrap();
    }
}

#[tokio::test]
async fn test_notify_waiters() {
    let notify = Arc::new(Notify::new());
    let count = Arc::new(AtomicUsize::new(0));

    let mut handles = Vec::new();
    for _ in 0..5 {
        let n = notify.clone();
        let c = count.clone();
        handles.push(tokio::spawn(async move {
            n.notified().await;
            c.fetch_add(1, Ordering::Relaxed);
        }));
    }

    sleep(Duration::from_millis(20)).await;
    notify.notify_waiters();

    for h in handles {
        h.await.unwrap();
    }

    assert_eq!(count.load(Ordering::Relaxed), 5);
}

#[tokio::test]
async fn test_notify_waiters_no_permit() {
    let notify = Arc::new(Notify::new());

    // Calling notify_waiters when there are no waiters should not store a permit
    notify.notify_waiters();

    let n2 = notify.clone();
    let completed = Arc::new(AtomicBool::new(false));
    let completed2 = completed.clone();

    let handle = tokio::spawn(async move {
        n2.notified().await;
        completed2.store(true, Ordering::Release);
    });

    sleep(Duration::from_millis(20)).await;
    assert!(!completed.load(Ordering::Acquire));

    notify.notify_one();
    handle.await.unwrap();
    assert!(completed.load(Ordering::Acquire));
}

#[tokio::test]
async fn test_notify_drop_notified_transfer() {
    let notify = Arc::new(Notify::new());
    let n1 = notify.clone();
    let n2 = notify.clone();

    let second_woken = Arc::new(AtomicBool::new(false));
    let second_woken2 = second_woken.clone();

    // Task 1 will be cancelled
    let t1 = tokio::spawn(async move {
        let notified = n1.notified();
        let mut pinned = pin!(notified);
        pinned.as_mut().enable();
        // Wait forever until cancelled
        pending::<()>().await;
    });

    // Task 2 will wait
    let t2 = tokio::spawn(async move {
        n2.notified().await;
        second_woken2.store(true, Ordering::Release);
    });

    sleep(Duration::from_millis(20)).await;

    // Wake one: t1 gets the notification
    notify.notify_one();

    // Cancel t1: its drop should transfer the notification to t2
    t1.abort();
    let _ = t1.await;

    sleep(Duration::from_millis(20)).await;
    assert!(second_woken.load(Ordering::Acquire));
    t2.await.unwrap();
}

#[tokio::test]
async fn test_notify_drop_notified_restore_permit() {
    let notify = Arc::new(Notify::new());
    let n1 = notify.clone();

    let t1 = tokio::spawn(async move {
        let notified = n1.notified();
        let mut pinned = pin!(notified);
        pinned.as_mut().enable();
        pending::<()>().await;
    });

    sleep(Duration::from_millis(20)).await;

    notify.notify_one();

    // Abort t1 before completion
    t1.abort();
    let _ = t1.await;

    // The notification should be restored as a permit
    notify.notified().await;
}

#[tokio::test]
async fn test_notify_enable() {
    let notify = Notify::new();
    let notified = notify.notified();
    let mut pinned = pin!(notified);
    pinned.as_mut().enable();

    notify.notify_one();

    pinned.await;
}

#[tokio::test]
async fn test_notify_concurrent() {
    let notify = Arc::new(Notify::new());
    let mut handles = Vec::new();

    for _ in 0..10 {
        let n = notify.clone();
        handles.push(tokio::spawn(async move {
            for _ in 0..50 {
                n.notified().await;
            }
        }));
    }

    for _ in 0..500 {
        notify.notify_one();
        tokio::task::yield_now().await;
    }

    for h in handles {
        h.await.unwrap();
    }
}
