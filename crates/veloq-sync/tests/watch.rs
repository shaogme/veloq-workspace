#![cfg(not(feature = "loom"))]

use std::time::Duration;

use tokio::time::sleep;
use veloq_sync::{
    RecvError, SendError,
    watch::{self, State},
};

#[tokio::test]
async fn test_watch_basic() {
    let (tx, mut rx) = watch::channel(10);
    assert_eq!(*rx.borrow(), 10);
    assert_eq!(*tx.borrow(), 10);
    assert!(!rx.has_changed().unwrap());

    tx.send(20).unwrap();
    assert!(rx.has_changed().unwrap());

    rx.changed().await.unwrap();
    assert_eq!(*rx.borrow(), 20);
    assert!(!rx.has_changed().unwrap());
}

#[tokio::test]
async fn test_watch_borrow_and_update() {
    let (tx, mut rx) = watch::channel("hello");
    tx.send("world").unwrap();

    // borrow_and_update marks the version as seen
    assert_eq!(*rx.borrow_and_update(), "world");
    assert!(!rx.has_changed().unwrap());
}

#[tokio::test]
async fn test_watch_mark_changed_unchanged() {
    let (_tx, mut rx) = watch::channel(100);
    assert!(!rx.has_changed().unwrap());

    rx.mark_changed();
    assert!(rx.has_changed().unwrap());

    rx.mark_unchanged();
    assert!(!rx.has_changed().unwrap());
}

#[tokio::test]
async fn test_watch_send_modify() {
    let (tx, mut rx) = watch::channel(5);

    let old = tx.send_modify(|val| {
        let prev = *val;
        *val += 10;
        prev
    });
    assert_eq!(old, 5);
    assert_eq!(*rx.borrow(), 15);

    rx.changed().await.unwrap();
    assert_eq!(*rx.borrow(), 15);

    // send_if_modified returns false -> no notification
    let modified = tx.send_if_modified(|val| {
        if *val > 100 {
            *val = 0;
            true
        } else {
            false
        }
    });
    assert!(!modified);
    assert!(!rx.has_changed().unwrap());

    // send_if_modified returns true -> triggers notification
    let modified = tx.send_if_modified(|val| {
        *val = 42;
        true
    });
    assert!(modified);
    assert!(rx.has_changed().unwrap());
    rx.changed().await.unwrap();
    assert_eq!(*rx.borrow(), 42);
}

#[tokio::test]
async fn test_watch_multi_receiver() {
    let (tx, mut rx1) = watch::channel(1);
    let mut rx2 = rx1.clone();
    let mut rx3 = tx.subscribe();

    assert_eq!(tx.receiver_count(), 3);
    assert!(rx1.same_channel(&rx2));
    assert!(rx1.same_channel(&rx3));

    tx.send(2).unwrap();

    rx1.changed().await.unwrap();
    rx2.changed().await.unwrap();
    rx3.changed().await.unwrap();

    assert_eq!(*rx1.borrow(), 2);
    assert_eq!(*rx2.borrow(), 2);
    assert_eq!(*rx3.borrow(), 2);

    drop(rx1);
    drop(rx2);
    assert_eq!(tx.receiver_count(), 1);
}

#[tokio::test]
async fn test_watch_sender_drop() {
    let (tx, mut rx) = watch::channel(10);
    tx.send(20).unwrap();
    drop(tx);

    // First changed() sees the pending new value
    rx.changed().await.unwrap();
    assert_eq!(*rx.borrow(), 20);

    // Subsequent changed() returns RecvError
    assert_eq!(rx.changed().await, Err(RecvError));
    assert_eq!(rx.has_changed(), Err(RecvError));
}

#[tokio::test]
async fn test_watch_all_receivers_dropped() {
    let (mut tx, rx) = watch::channel(1);
    assert!(!tx.is_closed());

    let handle = tokio::spawn(async move {
        sleep(Duration::from_millis(20)).await;
        drop(rx);
    });

    tx.closed().await;
    assert!(tx.is_closed());
    handle.await.unwrap();

    let err = tx.send(99).unwrap_err();
    assert_eq!(err, SendError(99));
}

#[tokio::test]
async fn test_watch_concurrent() {
    let (tx, mut rx) = watch::channel(0);

    let sender_handle = tokio::spawn(async move {
        for i in 1..=50 {
            let _ = tx.send(i);
            sleep(Duration::from_millis(1)).await;
        }
    });

    let receiver_handle = tokio::spawn(async move {
        let mut last_seen = 0;
        while last_seen < 50 {
            if rx.changed().await.is_err() {
                break;
            }
            let val = *rx.borrow_and_update();
            assert!(val >= last_seen);
            last_seen = val;
        }
        last_seen
    });

    sender_handle.await.unwrap();
    let final_val = receiver_handle.await.unwrap();
    assert_eq!(final_val, 50);
}

#[tokio::test]
async fn test_watch_borrowed_state() {
    let state = State::new(100);
    let (tx, mut rx) = state.split();
    assert_eq!(*rx.borrow(), 100);

    let mut rx2 = rx.clone();
    let mut rx3 = tx.subscribe();
    assert_eq!(tx.receiver_count(), 3);
    assert!(rx.same_channel(&rx2));
    assert!(rx.same_channel(&rx3));

    tx.send(200).unwrap();
    rx.changed().await.unwrap();
    rx2.changed().await.unwrap();
    rx3.changed().await.unwrap();

    assert_eq!(*rx.borrow(), 200);
    assert_eq!(*rx2.borrow(), 200);
    assert_eq!(*rx3.borrow(), 200);
}

#[tokio::test]
async fn test_watch_cancellation() {
    let (tx, mut rx) = watch::channel(1);

    // Start waiting, then cancel the future
    tokio::select! {
        _ = rx.changed() => {
            panic!("should not resolve");
        }
        _ = sleep(Duration::from_millis(10)) => {}
    }

    // Now send and wait again; must still successfully receive
    tx.send(2).unwrap();
    rx.changed().await.unwrap();
    assert_eq!(*rx.borrow(), 2);
}
