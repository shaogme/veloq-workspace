#![cfg(feature = "loom")]

use loom::sync::Arc;
use loom::sync::atomic::{AtomicBool, Ordering};
use loom::thread;
use std::task::Wake;
use veloq_atomic_waker::AtomicWaker;

struct TestWaker(Arc<AtomicBool>);

impl Wake for TestWaker {
    fn wake(self: std::sync::Arc<Self>) {
        self.0.store(true, Ordering::Relaxed);
    }
}

#[test]
fn concurrent_register() {
    loom::model(|| {
        let atomic_waker = Arc::new(AtomicWaker::new());

        let woken1 = Arc::new(AtomicBool::new(false));
        let woken2 = Arc::new(AtomicBool::new(false));

        let w1 = std::sync::Arc::new(TestWaker(woken1.clone()));
        let w2 = std::sync::Arc::new(TestWaker(woken2.clone()));
        let waker1 = std::task::Waker::from(w1);
        let waker2 = std::task::Waker::from(w2);

        let aw1 = atomic_waker.clone();
        // We need to move wakers into threads but Waker is !Send in loom?
        // No, Waker is Send.
        // But we need to clone them.
        let waker1_clone = waker1.clone();
        let t1 = thread::spawn(move || {
            aw1.register(&waker1_clone);
        });

        let aw2 = atomic_waker.clone();
        let waker2_clone = waker2.clone();
        let t2 = thread::spawn(move || {
            aw2.register(&waker2_clone);
        });

        t1.join().unwrap();
        t2.join().unwrap();

        // Ensure no panic and basic state consistency
        atomic_waker.wake();
    });
}
