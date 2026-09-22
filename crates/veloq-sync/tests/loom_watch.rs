#![cfg(feature = "loom")]

use loom::{future::block_on, thread};
use veloq_sync::watch;

#[test]
fn loom_watch_send_recv() {
    let mut builder = loom::model::Builder::new();
    builder.preemption_bound = Some(2);
    builder.check(|| {
        let (tx, mut rx) = watch::channel(1);

        let h1 = thread::spawn(move || {
            let _ = tx.send(42);
        });

        let h2 = thread::spawn(move || {
            block_on(async move {
                if rx.changed().await.is_ok() {
                    assert_eq!(*rx.borrow(), 42);
                }
            });
        });

        h1.join().unwrap();
        h2.join().unwrap();
    });
}

#[test]
fn loom_watch_drop_sender() {
    let mut builder = loom::model::Builder::new();
    builder.preemption_bound = Some(2);
    builder.check(|| {
        let (tx, mut rx) = watch::channel(1);

        let h1 = thread::spawn(move || {
            drop(tx);
        });

        let h2 = thread::spawn(move || {
            block_on(async move {
                let res = rx.changed().await;
                assert!(res.is_err());
            });
        });

        h1.join().unwrap();
        h2.join().unwrap();
    });
}

#[test]
fn loom_watch_drop_receiver() {
    let mut builder = loom::model::Builder::new();
    builder.preemption_bound = Some(2);
    builder.check(|| {
        let (mut tx, rx) = watch::channel(1);

        let h1 = thread::spawn(move || {
            drop(rx);
        });

        let h2 = thread::spawn(move || {
            block_on(async move {
                tx.closed().await;
                assert!(tx.is_closed());
            });
        });

        h1.join().unwrap();
        h2.join().unwrap();
    });
}

#[test]
fn loom_watch_two_receivers() {
    let mut builder = loom::model::Builder::new();
    builder.preemption_bound = Some(2);
    builder.check(|| {
        let (tx, mut rx1) = watch::channel(0);
        let mut rx2 = rx1.clone();

        let h1 = thread::spawn(move || {
            let _ = tx.send(99);
        });

        let h2 = thread::spawn(move || {
            block_on(async move {
                if rx1.changed().await.is_ok() {
                    assert_eq!(*rx1.borrow(), 99);
                }
            });
        });

        let h3 = thread::spawn(move || {
            block_on(async move {
                if rx2.changed().await.is_ok() {
                    assert_eq!(*rx2.borrow(), 99);
                }
            });
        });

        h1.join().unwrap();
        h2.join().unwrap();
        h3.join().unwrap();
    });
}

#[test]
fn loom_watch_send_modify() {
    let mut builder = loom::model::Builder::new();
    builder.preemption_bound = Some(2);
    builder.check(|| {
        let (tx, mut rx) = watch::channel(10);

        let h1 = thread::spawn(move || {
            tx.send_modify(|v| *v += 5);
        });

        let h2 = thread::spawn(move || {
            block_on(async move {
                if rx.changed().await.is_ok() {
                    assert_eq!(*rx.borrow(), 15);
                }
            });
        });

        h1.join().unwrap();
        h2.join().unwrap();
    });
}
