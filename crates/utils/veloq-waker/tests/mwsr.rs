use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    task::{Wake, Waker},
    thread,
};
use veloq_waker::MwsrWaker;

struct TestWaker(Arc<AtomicBool>);

impl Wake for TestWaker {
    fn wake(self: std::sync::Arc<Self>) {
        self.0.store(true, Ordering::Release);
    }
}

#[test]
fn test_basic_register_wake() {
    let waker_state = Arc::new(AtomicBool::new(false));
    let custom_waker = Waker::from(Arc::new(TestWaker(waker_state.clone())));

    let mpsc_waker = MwsrWaker::new();
    unsafe {
        mpsc_waker.register(&custom_waker);
    }

    assert!(!waker_state.load(Ordering::Acquire));
    mpsc_waker.wake();
    assert!(waker_state.load(Ordering::Acquire));
}

#[test]
fn test_concurrent_wake_register() {
    // Under loom or normal execution, testing concurrent register (from single thread)
    // and wake (from another thread).
    for _ in 0..10 {
        let mpsc_waker = Arc::new(MwsrWaker::new());
        let woken = Arc::new(AtomicBool::new(false));
        let custom_waker = Waker::from(Arc::new(TestWaker(woken.clone())));

        let waker_clone = mpsc_waker.clone();
        let handle = thread::spawn(move || {
            waker_clone.wake();
        });

        unsafe {
            mpsc_waker.register(&custom_waker);
        }
        handle.join().unwrap();

        // Since wake and register happened concurrently,
        // either register finished first and wake woke it,
        // or wake happened first/concurrently and register immediately woke it.
        // In either case, the waker must be woken.
        mpsc_waker.wake(); // Make sure it's woken
        assert!(woken.load(Ordering::Acquire));
    }
}

#[cfg(feature = "loom")]
#[test]
fn test_mpsc_waker_loom() {
    loom::model(|| {
        let mpsc_waker = Arc::new(MwsrWaker::new());
        let woken = Arc::new(AtomicBool::new(false));
        let custom_waker = Waker::from(Arc::new(TestWaker(woken.clone())));

        let waker_clone = mpsc_waker.clone();
        let handle = thread::spawn(move || {
            waker_clone.wake();
        });

        unsafe {
            mpsc_waker.register(&custom_waker);
        }
        handle.join().unwrap();

        mpsc_waker.wake();
        assert!(woken.load(Ordering::Acquire));
    });
}
