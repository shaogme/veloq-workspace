#![cfg(feature = "loom")]

use loom::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread,
};
use std::{
    sync::Arc as StdArc,
    task::{Wake, Waker},
};
use veloq_waker::AtomicWaker;

struct TestWaker(Arc<AtomicBool>);

impl Wake for TestWaker {
    fn wake(self: StdArc<Self>) {
        self.0.store(true, Ordering::Relaxed);
    }
}

#[test]
fn concurrent_register() {
    loom::model(|| {
        let atomic_waker = Arc::new(AtomicWaker::new());

        let woken1 = Arc::new(AtomicBool::new(false));
        let woken2 = Arc::new(AtomicBool::new(false));

        let w1 = StdArc::new(TestWaker(woken1.clone()));
        let w2 = StdArc::new(TestWaker(woken2.clone()));
        let waker1 = Waker::from(w1);
        let waker2 = Waker::from(w2);

        let aw1 = atomic_waker.clone();
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

        // Ensure no panic and basic state consistency.
        atomic_waker.wake();

        let woke1 = woken1.load(Ordering::Relaxed);
        let woke2 = woken2.load(Ordering::Relaxed);
        assert!(
            woke1 || woke2,
            "at least one concurrent registration must remain wakeable"
        );
    });
}
