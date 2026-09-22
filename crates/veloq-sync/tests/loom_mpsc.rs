#![cfg(feature = "loom")]

use loom::future::block_on;
use loom::thread;
use veloq_sync::mpsc;

#[test]
fn loom_mpsc_unbounded_recv_async() {
    let mut builder = loom::model::Builder::new();
    builder.preemption_bound = Some(3);
    builder.check(|| {
        let (tx, mut rx) = mpsc::unbounded();

        // Thread 1: Sends data
        let h1 = thread::spawn(move || {
            let _ = tx.send(1);
        });

        // Thread 2: Receives data
        let h2 = thread::spawn(move || {
            block_on(async move {
                let val = rx.recv().await;
                assert_eq!(val, Some(1));
            });
        });

        h1.join().unwrap();
        h2.join().unwrap();
    });
}

#[test]
fn loom_mpsc_bounded_async_send() {
    let mut builder = loom::model::Builder::new();
    builder.preemption_bound = Some(3);
    builder.check(|| {
        let (tx, mut rx) = mpsc::bounded::<usize>(1);

        let h1 = thread::spawn(move || {
            block_on(async move {
                tx.send(10).await.unwrap();
                tx.send(20).await.unwrap();
            });
        });

        let h2 = thread::spawn(move || {
            block_on(async move {
                let val1 = rx.recv().await;
                assert_eq!(val1, Some(10));
                let val2 = rx.recv().await;
                assert_eq!(val2, Some(20));
            });
        });

        h1.join().unwrap();
        h2.join().unwrap();
    });
}
