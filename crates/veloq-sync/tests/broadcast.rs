#![cfg(not(feature = "loom"))]

use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};
use std::time::Duration;

use tokio::time::sleep;
use veloq_sync::broadcast::{self, RecvError, SendError, TryRecvError};

#[tokio::test]
async fn test_broadcast_basic() {
    let (tx, mut rx) = broadcast::channel(16);
    assert_eq!(tx.capacity(), 16);
    assert_eq!(tx.receiver_count(), 1);
    assert_eq!(tx.sender_count(), 1);
    assert_eq!(tx.len(), 0);
    assert!(tx.is_empty());
    assert!(!tx.is_closed());

    let count = tx.send(10).unwrap();
    assert_eq!(count, 1);
    assert_eq!(tx.len(), 1);
    assert_eq!(rx.len(), 1);

    let val = rx.recv().await.unwrap();
    assert_eq!(val, 10);
    assert_eq!(rx.len(), 0);
    assert!(rx.is_empty());
}

#[tokio::test]
async fn test_broadcast_multi_receiver() {
    let (tx, mut rx1) = broadcast::channel(16);
    let mut rx2 = tx.subscribe();
    let mut rx3 = rx1.clone();

    assert_eq!(tx.receiver_count(), 3);
    assert!(rx1.same_channel(&rx2));
    assert!(rx1.same_channel(&rx3));

    let count = tx.send(42).unwrap();
    assert_eq!(count, 3);

    assert_eq!(rx1.recv().await.unwrap(), 42);
    assert_eq!(rx2.recv().await.unwrap(), 42);
    assert_eq!(rx3.recv().await.unwrap(), 42);

    drop(rx1);
    drop(rx2);
    assert_eq!(tx.receiver_count(), 1);
}

#[tokio::test]
async fn test_broadcast_lagged() {
    let (tx, mut rx) = broadcast::channel(2);

    tx.send(1).unwrap();
    tx.send(2).unwrap();
    tx.send(3).unwrap();
    tx.send(4).unwrap();

    // The buffer capacity is 2, so values 1 and 2 were dropped (lagged by 2)
    let err = rx.recv().await.unwrap_err();
    assert_eq!(err, RecvError::Lagged(2));

    // Next receive should yield the oldest remaining message (3)
    assert_eq!(rx.recv().await.unwrap(), 3);
    assert_eq!(rx.recv().await.unwrap(), 4);
}

#[tokio::test]
async fn test_broadcast_try_recv() {
    let (tx, mut rx) = broadcast::channel(2);

    assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));

    tx.send(10).unwrap();
    assert_eq!(rx.try_recv(), Ok(10));
    assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));

    tx.send(20).unwrap();
    tx.send(30).unwrap();
    tx.send(40).unwrap();

    assert_eq!(rx.try_recv(), Err(TryRecvError::Lagged(1)));
    assert_eq!(rx.try_recv(), Ok(30));
    assert_eq!(rx.try_recv(), Ok(40));

    drop(tx);
    assert_eq!(rx.try_recv(), Err(TryRecvError::Closed));
}

#[tokio::test]
async fn test_broadcast_sender_drop() {
    let (tx, mut rx) = broadcast::channel(16);
    tx.send(100).unwrap();
    tx.send(200).unwrap();
    drop(tx);

    // Remaining messages can still be consumed
    assert_eq!(rx.recv().await.unwrap(), 100);
    assert_eq!(rx.recv().await.unwrap(), 200);

    // After all messages are read, it returns Closed
    assert_eq!(rx.recv().await, Err(RecvError::Closed));
    assert_eq!(rx.try_recv(), Err(TryRecvError::Closed));
}

#[tokio::test]
async fn test_broadcast_no_receiver() {
    let (tx, rx) = broadcast::channel::<i32>(16);
    drop(rx);

    assert!(tx.is_closed());
    let err = tx.send(999).unwrap_err();
    assert_eq!(err, SendError(999));

    // Subscribing again allows sends
    let mut new_rx = tx.subscribe();
    assert!(!tx.is_closed());
    assert_eq!(tx.send(1000).unwrap(), 1);
    assert_eq!(new_rx.recv().await.unwrap(), 1000);
}

#[tokio::test]
async fn test_broadcast_resubscribe_and_clone() {
    let (tx, mut rx1) = broadcast::channel(16);
    tx.send(1).unwrap();
    tx.send(2).unwrap();

    // clone() continues from the same position as rx1
    let mut rx_cloned = rx1.clone();
    assert_eq!(rx_cloned.recv().await.unwrap(), 1);

    // resubscribe() only receives future messages
    let mut rx_resub = rx1.resubscribe();

    tx.send(3).unwrap();

    assert_eq!(rx1.recv().await.unwrap(), 1);
    assert_eq!(rx1.recv().await.unwrap(), 2);
    assert_eq!(rx1.recv().await.unwrap(), 3);

    assert_eq!(rx_resub.recv().await.unwrap(), 3);
}

#[tokio::test]
async fn test_broadcast_borrowed_state() {
    let (tx, mut rx) = broadcast::channel(16);

    let mut rx2 = tx.subscribe();
    let mut rx3 = rx.clone();

    assert_eq!(tx.receiver_count(), 3);
    assert!(rx.same_channel(&rx2));
    assert!(rx.same_channel(&rx3));

    tx.send(555).unwrap();
    assert_eq!(rx.recv().await.unwrap(), 555);
    assert_eq!(rx2.recv().await.unwrap(), 555);
    assert_eq!(rx3.recv().await.unwrap(), 555);
}

#[tokio::test]
async fn test_broadcast_concurrent() {
    let (tx, mut rx1) = broadcast::channel(100);
    let mut rx2 = tx.subscribe();

    let tx_clone = tx.clone();
    let sender_handle = tokio::spawn(async move {
        for i in 1..=50 {
            let _ = tx_clone.send(i);
            sleep(Duration::from_millis(1)).await;
        }
    });

    let receiver1_handle = tokio::spawn(async move {
        let mut count = 0;
        while count < 50 {
            match rx1.recv().await {
                Ok(_) => count += 1,
                Err(RecvError::Closed) => break,
                Err(RecvError::Lagged(_)) => {}
            }
        }
        count
    });

    let receiver2_handle = tokio::spawn(async move {
        let mut count = 0;
        while count < 50 {
            match rx2.recv().await {
                Ok(_) => count += 1,
                Err(RecvError::Closed) => break,
                Err(RecvError::Lagged(_)) => {}
            }
        }
        count
    });

    sender_handle.await.unwrap();
    assert_eq!(receiver1_handle.await.unwrap(), 50);
    assert_eq!(receiver2_handle.await.unwrap(), 50);
}

#[tokio::test]
async fn test_broadcast_cancellation() {
    let (tx, mut rx) = broadcast::channel(16);

    tokio::select! {
        _ = rx.recv() => {
            panic!("should not resolve");
        }
        _ = sleep(Duration::from_millis(10)) => {}
    }

    tx.send(777).unwrap();
    assert_eq!(rx.recv().await.unwrap(), 777);
}

#[tokio::test]
async fn test_broadcast_send_count_linearization_order() {
    let (tx, mut rx) = broadcast::channel(8);

    assert_eq!(tx.send(1).unwrap(), 1);
    let mut late = tx.subscribe();
    assert_eq!(late.try_recv(), Err(TryRecvError::Empty));
    assert_eq!(rx.recv().await.unwrap(), 1);

    assert_eq!(tx.send(2).unwrap(), 2);
    assert_eq!(rx.recv().await.unwrap(), 2);
    assert_eq!(late.recv().await.unwrap(), 2);
}

#[tokio::test]
async fn test_broadcast_send_count_after_receiver_drop() {
    let (tx, rx) = broadcast::channel::<i32>(8);
    drop(rx);

    assert_eq!(tx.send(1), Err(SendError(1)));
    assert_eq!(tx.len(), 0);

    let mut rx = tx.subscribe();
    assert_eq!(tx.send(2).unwrap(), 1);
    assert_eq!(rx.recv().await.unwrap(), 2);
}

#[tokio::test]
async fn test_broadcast_borrowed_send_count() {
    broadcast::with_borrowed_channel(8, async |tx, mut rx| {
        let mut cloned = rx.clone();
        let mut resubscribed = rx.resubscribe();
        assert_eq!(tx.receiver_count(), 3);
        assert_eq!(tx.send(7).unwrap(), 3);
        assert_eq!(rx.recv().await.unwrap(), 7);
        assert_eq!(cloned.recv().await.unwrap(), 7);
        assert_eq!(resubscribed.recv().await.unwrap(), 7);
    })
    .await;
}

#[derive(Clone)]
struct ReentrantDrop {
    sender: Arc<Mutex<Option<broadcast::Sender<ReentrantDrop>>>>,
    fired: Arc<AtomicBool>,
}

impl Drop for ReentrantDrop {
    fn drop(&mut self) {
        if self.fired.swap(true, Ordering::SeqCst) {
            return;
        }
        if let Some(sender) = self.sender.lock().unwrap().as_ref().cloned() {
            let _ = sender.send(Self {
                sender: Arc::clone(&self.sender),
                fired: Arc::clone(&self.fired),
            });
        }
    }
}

#[test]
fn test_broadcast_evicted_drop_reenters_after_state_unlock() {
    let (tx, _rx) = broadcast::channel(1);
    let sender = Arc::new(Mutex::new(None));
    let fired = Arc::new(AtomicBool::new(false));
    *sender.lock().unwrap() = Some(tx.clone());

    tx.send(ReentrantDrop {
        sender: Arc::clone(&sender),
        fired: Arc::clone(&fired),
    })
    .unwrap();
    tx.send(ReentrantDrop {
        sender: Arc::clone(&sender),
        fired: Arc::clone(&fired),
    })
    .unwrap();

    assert!(fired.load(Ordering::SeqCst));
    assert_eq!(tx.len(), 1);
}
