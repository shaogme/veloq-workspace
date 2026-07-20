#[cfg(not(feature = "loom"))]
mod normal_tests {
    use std::{
        sync::{
            Arc,
            atomic::{AtomicBool, AtomicUsize, Ordering},
        },
        task::{Wake, Waker},
        thread,
    };
    use veloq_waker::MwsrWaker;

    struct TestWaker {
        woken: Arc<AtomicBool>,
        wake_count: Arc<AtomicUsize>,
    }

    impl TestWaker {
        fn new(woken: Arc<AtomicBool>, wake_count: Arc<AtomicUsize>) -> Self {
            Self { woken, wake_count }
        }
    }

    impl Wake for TestWaker {
        fn wake(self: Arc<Self>) {
            self.woken.store(true, Ordering::Release);
            self.wake_count.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[test]
    fn test_basic_register_wake() {
        let woken = Arc::new(AtomicBool::new(false));
        let count = Arc::new(AtomicUsize::new(0));
        let custom_waker = Waker::from(Arc::new(TestWaker::new(woken.clone(), count.clone())));

        let mpsc_waker = MwsrWaker::new();
        unsafe {
            mpsc_waker.register(&custom_waker);
        }

        assert!(!woken.load(Ordering::Acquire));
        mpsc_waker.wake();
        assert!(woken.load(Ordering::Acquire));
        assert_eq!(count.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn test_multiple_wake() {
        let woken = Arc::new(AtomicBool::new(false));
        let count = Arc::new(AtomicUsize::new(0));
        let custom_waker = Waker::from(Arc::new(TestWaker::new(woken.clone(), count.clone())));

        let mpsc_waker = MwsrWaker::new();
        unsafe {
            mpsc_waker.register(&custom_waker);
        }

        mpsc_waker.wake();
        mpsc_waker.wake();
        assert!(woken.load(Ordering::Acquire));
        assert_eq!(count.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn test_reregister_different_waker() {
        let woken1 = Arc::new(AtomicBool::new(false));
        let count1 = Arc::new(AtomicUsize::new(0));
        let waker1 = Waker::from(Arc::new(TestWaker::new(woken1.clone(), count1.clone())));

        let woken2 = Arc::new(AtomicBool::new(false));
        let count2 = Arc::new(AtomicUsize::new(0));
        let waker2 = Waker::from(Arc::new(TestWaker::new(woken2.clone(), count2.clone())));

        let mpsc_waker = MwsrWaker::new();
        unsafe {
            mpsc_waker.register(&waker1);
            mpsc_waker.register(&waker2);
        }

        mpsc_waker.wake();
        assert!(!woken1.load(Ordering::Acquire));
        assert!(woken2.load(Ordering::Acquire));
        assert_eq!(count1.load(Ordering::SeqCst), 0);
        assert_eq!(count2.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn test_drop_waker() {
        let woken = Arc::new(AtomicBool::new(false));
        let count = Arc::new(AtomicUsize::new(0));
        let custom_waker = Waker::from(Arc::new(TestWaker::new(woken.clone(), count.clone())));

        {
            let mpsc_waker = MwsrWaker::new();
            unsafe {
                mpsc_waker.register(&custom_waker);
            }
            // mpsc_waker goes out of scope, registered waker must be dropped
        }

        // Drop should not trigger waking
        assert!(!woken.load(Ordering::Acquire));
        assert_eq!(count.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn test_take_waker() {
        let woken = Arc::new(AtomicBool::new(false));
        let count = Arc::new(AtomicUsize::new(0));
        let custom_waker = Waker::from(Arc::new(TestWaker::new(woken.clone(), count.clone())));

        let mpsc_waker = MwsrWaker::new();
        assert!(mpsc_waker.take().is_none());

        unsafe {
            mpsc_waker.register(&custom_waker);
        }

        let taken = mpsc_waker.take();
        assert!(taken.is_some());
        assert!(mpsc_waker.take().is_none());

        taken.unwrap().wake();
        assert!(woken.load(Ordering::Acquire));
        assert_eq!(count.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn test_concurrent_wake_register() {
        for _ in 0..10 {
            let mpsc_waker = Arc::new(MwsrWaker::new());
            let woken = Arc::new(AtomicBool::new(false));
            let count = Arc::new(AtomicUsize::new(0));
            let custom_waker = Waker::from(Arc::new(TestWaker::new(woken.clone(), count.clone())));

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
        }
    }
}

#[cfg(feature = "loom")]
mod loom_tests {
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
    use veloq_waker::MwsrWaker;

    struct TestWaker(Arc<AtomicBool>);

    impl Wake for TestWaker {
        fn wake(self: StdArc<Self>) {
            self.0.store(true, Ordering::Release);
        }
    }

    #[test]
    fn test_mpsc_waker_loom() {
        loom::model(|| {
            let mpsc_waker = Arc::new(MwsrWaker::new());
            let woken = Arc::new(AtomicBool::new(false));
            let custom_waker = Waker::from(StdArc::new(TestWaker(woken.clone())));

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

    #[test]
    fn test_mpsc_waker_take_loom() {
        loom::model(|| {
            let mpsc_waker = Arc::new(MwsrWaker::new());
            let woken = Arc::new(AtomicBool::new(false));
            let custom_waker = Waker::from(StdArc::new(TestWaker(woken.clone())));

            unsafe {
                mpsc_waker.register(&custom_waker);
            }

            let waker_clone = mpsc_waker.clone();
            let handle = thread::spawn(move || {
                if let Some(w) = waker_clone.take() {
                    w.wake();
                }
            });

            if let Some(w) = mpsc_waker.take() {
                w.wake();
            }

            handle.join().unwrap();
            assert!(woken.load(Ordering::Acquire));
        });
    }
}
