#![cfg(feature = "loom")]

use loom::{future::block_on, thread};
use veloq_sync::broadcast::{self, RecvError};

#[test]
fn loom_broadcast_send_recv() {
    let mut builder = loom::model::Builder::new();
    builder.preemption_bound = Some(2);
    builder.check(|| {
        let (tx, mut rx) = broadcast::channel(2);

        let h1 = thread::spawn(move || {
            let _ = tx.send(42);
        });

        let h2 = thread::spawn(move || {
            block_on(async move {
                if let Ok(val) = rx.recv().await {
                    assert_eq!(val, 42);
                }
            });
        });

        h1.join().unwrap();
        h2.join().unwrap();
    });
}

#[test]
fn loom_broadcast_drop_sender() {
    let mut builder = loom::model::Builder::new();
    builder.preemption_bound = Some(2);
    builder.check(|| {
        let (tx, mut rx) = broadcast::channel::<i32>(2);

        let h1 = thread::spawn(move || {
            drop(tx);
        });

        let h2 = thread::spawn(move || {
            block_on(async move {
                let res = rx.recv().await;
                assert!(res.is_err());
            });
        });

        h1.join().unwrap();
        h2.join().unwrap();
    });
}

#[test]
fn loom_broadcast_drop_receiver() {
    let mut builder = loom::model::Builder::new();
    builder.preemption_bound = Some(2);
    builder.check(|| {
        let (tx, rx) = broadcast::channel::<i32>(2);

        let h1 = thread::spawn(move || {
            drop(rx);
        });

        let h2 = thread::spawn(move || {
            let _ = tx.send(10);
        });

        h1.join().unwrap();
        h2.join().unwrap();
    });
}

#[test]
fn loom_broadcast_lagged() {
    let mut builder = loom::model::Builder::new();
    builder.preemption_bound = Some(2);
    builder.check(|| {
        let (tx, mut rx) = broadcast::channel(1);

        let h1 = thread::spawn(move || {
            let _ = tx.send(1);
            let _ = tx.send(2);
        });

        let h2 = thread::spawn(move || {
            block_on(async move {
                match rx.recv().await {
                    Ok(val) => assert!(val == 1 || val == 2),
                    Err(RecvError::Lagged(_)) => {
                        if let Ok(val) = rx.recv().await {
                            assert_eq!(val, 2);
                        }
                    }
                    Err(_) => {}
                }
            });
        });

        h1.join().unwrap();
        h2.join().unwrap();
    });
}

#[test]
fn loom_broadcast_two_receivers() {
    let mut builder = loom::model::Builder::new();
    builder.preemption_bound = Some(2);
    builder.check(|| {
        let (tx, mut rx1) = broadcast::channel(2);
        let mut rx2 = tx.subscribe();

        let h1 = thread::spawn(move || {
            let _ = tx.send(99);
        });

        let h2 = thread::spawn(move || {
            block_on(async move {
                if let Ok(val) = rx1.recv().await {
                    assert_eq!(val, 99);
                }
            });
        });

        let h3 = thread::spawn(move || {
            block_on(async move {
                if let Ok(val) = rx2.recv().await {
                    assert_eq!(val, 99);
                }
            });
        });

        h1.join().unwrap();
        h2.join().unwrap();
        h3.join().unwrap();
    });
}

#[test]
fn loom_broadcast_send_subscribe_uses_one_snapshot() {
    let mut builder = loom::model::Builder::new();
    builder.preemption_bound = Some(3);
    builder.check(|| {
        let (tx, mut rx) = broadcast::channel(2);
        let tx_send = tx.clone();
        let tx_subscribe = tx.clone();

        let send_handle = thread::spawn(move || tx_send.send(42));
        let subscribe_handle = thread::spawn(move || tx_subscribe.subscribe());

        let result = send_handle.join().unwrap();
        let mut late = subscribe_handle.join().unwrap();
        if result == Ok(1) {
            assert_eq!(late.try_recv().unwrap_err().to_string(), "channel empty");
        }
        if result == Ok(2) {
            let _ = late.try_recv();
        }
        assert!(rx.try_recv().is_ok() || result == Ok(1));
    });
}

#[test]
fn loom_broadcast_send_clone_uses_one_snapshot() {
    let mut builder = loom::model::Builder::new();
    builder.preemption_bound = Some(3);
    builder.check(|| {
        let (tx, rx) = broadcast::channel(2);
        let tx_send = tx.clone();
        let rx_clone = rx.clone();

        let send_handle = thread::spawn(move || tx_send.send(7));
        let clone_handle = thread::spawn(move || rx_clone);

        let result = send_handle.join().unwrap();
        let mut cloned = clone_handle.join().unwrap();
        if result == Ok(1) {
            assert_eq!(cloned.try_recv().unwrap_err().to_string(), "channel empty");
        }
    });
}

#[test]
fn loom_broadcast_send_drop_receiver_has_no_phantom_message() {
    let mut builder = loom::model::Builder::new();
    builder.preemption_bound = Some(3);
    builder.check(|| {
        let (tx, rx) = broadcast::channel::<i32>(2);
        let tx_send = tx.clone();

        let send_handle = thread::spawn(move || tx_send.send(9));
        let drop_handle = thread::spawn(move || drop(rx));

        let result = send_handle.join().unwrap();
        drop_handle.join().unwrap();
        match result {
            Ok(1) => assert_eq!(tx.len(), 1),
            Err(_) => assert_eq!(tx.len(), 0),
            Ok(count) => panic!("unexpected receiver count: {count}"),
        }
    });
}
