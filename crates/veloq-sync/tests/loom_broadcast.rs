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
