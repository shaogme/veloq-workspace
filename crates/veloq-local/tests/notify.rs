use std::cell::Cell;
use std::future::pending;
use std::pin::pin;
use std::rc::Rc;
use std::time::Duration;
use tokio::task::LocalSet;
use tokio::time::sleep;
use veloq_local::notify::Notify;

#[tokio::test]
async fn test_local_notify_one_basic() {
    let local = LocalSet::new();

    local
        .run_until(async {
            let notify = Rc::new(Notify::new());
            let n2 = notify.clone();

            let handle = tokio::task::spawn_local(async move {
                n2.notified().await;
                42
            });

            sleep(Duration::from_millis(10)).await;
            notify.notify_one();

            let res = handle.await.unwrap();
            assert_eq!(res, 42);
        })
        .await;
}

#[tokio::test]
async fn test_local_notify_one_permit() {
    let notify = Notify::new();

    // Notify before waiting
    notify.notify_one();

    // Should complete immediately without blocking
    notify.notified().await;
}

#[tokio::test]
async fn test_local_notify_one_single_permit() {
    let local = LocalSet::new();

    local
        .run_until(async {
            let notify = Rc::new(Notify::new());

            // Multiple calls to notify_one should only produce 1 permit
            notify.notify_one();
            notify.notify_one();
            notify.notify_one();

            // First await consumes the permit
            notify.notified().await;

            // Second await should block because only 1 permit was stored
            let n2 = notify.clone();
            let completed = Rc::new(Cell::new(false));
            let completed2 = completed.clone();

            let handle = tokio::task::spawn_local(async move {
                n2.notified().await;
                completed2.set(true);
            });

            sleep(Duration::from_millis(20)).await;
            assert!(!completed.get());

            // Now unpark it
            notify.notify_one();
            handle.await.unwrap();
            assert!(completed.get());
        })
        .await;
}

#[tokio::test]
async fn test_local_notify_multi_waiters_fifo() {
    let local = LocalSet::new();

    local
        .run_until(async {
            let notify = Rc::new(Notify::new());
            let counter = Rc::new(Cell::new(0));

            let mut handles = Vec::new();
            for i in 0..3 {
                let n = notify.clone();
                let c = counter.clone();
                handles.push(tokio::task::spawn_local(async move {
                    n.notified().await;
                    c.set(i + 1);
                }));
                sleep(Duration::from_millis(10)).await;
            }

            notify.notify_one();
            sleep(Duration::from_millis(10)).await;
            assert_eq!(counter.get(), 1);

            notify.notify_one();
            sleep(Duration::from_millis(10)).await;
            assert_eq!(counter.get(), 2);

            notify.notify_one();
            sleep(Duration::from_millis(10)).await;
            assert_eq!(counter.get(), 3);

            for h in handles {
                h.await.unwrap();
            }
        })
        .await;
}

#[tokio::test]
async fn test_local_notify_waiters() {
    let local = LocalSet::new();

    local
        .run_until(async {
            let notify = Rc::new(Notify::new());
            let count = Rc::new(Cell::new(0));

            let mut handles = Vec::new();
            for _ in 0..5 {
                let n = notify.clone();
                let c = count.clone();
                handles.push(tokio::task::spawn_local(async move {
                    n.notified().await;
                    c.set(c.get() + 1);
                }));
            }

            sleep(Duration::from_millis(20)).await;
            notify.notify_waiters();

            for h in handles {
                h.await.unwrap();
            }

            assert_eq!(count.get(), 5);
        })
        .await;
}

#[tokio::test]
async fn test_local_notify_waiters_no_permit() {
    let local = LocalSet::new();

    local
        .run_until(async {
            let notify = Rc::new(Notify::new());

            // Calling notify_waiters when there are no waiters should not store a permit
            notify.notify_waiters();

            let n2 = notify.clone();
            let completed = Rc::new(Cell::new(false));
            let completed2 = completed.clone();

            let handle = tokio::task::spawn_local(async move {
                n2.notified().await;
                completed2.set(true);
            });

            sleep(Duration::from_millis(20)).await;
            assert!(!completed.get());

            notify.notify_one();
            handle.await.unwrap();
            assert!(completed.get());
        })
        .await;
}

#[tokio::test]
async fn test_local_notify_drop_notified_transfer() {
    let local = LocalSet::new();

    local
        .run_until(async {
            let notify = Rc::new(Notify::new());
            let n1 = notify.clone();
            let n2 = notify.clone();

            let second_woken = Rc::new(Cell::new(false));
            let second_woken2 = second_woken.clone();

            // Task 1 will be cancelled
            let t1 = tokio::task::spawn_local(async move {
                let notified = n1.notified();
                let mut pinned = pin!(notified);
                pinned.as_mut().enable();
                pending::<()>().await;
            });

            // Task 2 will wait
            let t2 = tokio::task::spawn_local(async move {
                n2.notified().await;
                second_woken2.set(true);
            });

            sleep(Duration::from_millis(20)).await;

            // Wake one: t1 gets the notification
            notify.notify_one();

            // Cancel t1: its drop should transfer the notification to t2
            t1.abort();
            let _ = t1.await;

            sleep(Duration::from_millis(20)).await;
            assert!(second_woken.get());
            t2.await.unwrap();
        })
        .await;
}

#[tokio::test]
async fn test_local_notify_drop_notified_restore_permit() {
    let local = LocalSet::new();

    local
        .run_until(async {
            let notify = Rc::new(Notify::new());
            let n1 = notify.clone();

            let t1 = tokio::task::spawn_local(async move {
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
        })
        .await;
}

#[tokio::test]
async fn test_local_notify_enable() {
    let notify = Notify::new();
    let notified = notify.notified();
    let mut pinned = pin!(notified);
    pinned.as_mut().enable();

    notify.notify_one();

    pinned.await;
}
