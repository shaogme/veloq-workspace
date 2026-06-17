#![cfg(feature = "loom")]

use loom::future::block_on;
use loom::sync::Arc;
use loom::thread;
use veloq_sync::mpmc;

#[test]
fn loom_mpmc_unbounded_send_recv_async() {
    loom::model(|| {
        let (tx, rx) = mpmc::owned_unbounded();

        let tx = Arc::new(tx);
        let rx = Arc::new(rx);

        // Sender Thread
        let tx1 = tx.clone();
        let h1 = thread::spawn(move || {
            block_on(async move {
                tx1.send(100).await.unwrap();
            });
        });

        // Receiver Thread
        let rx1 = rx.clone();
        let h2 = thread::spawn(move || {
            block_on(async move {
                let val = rx1.recv().await.unwrap();
                assert_eq!(val, 100);
            });
        });

        h1.join().unwrap();
        h2.join().unwrap();
    });
}
