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
    use veloq_waker::AtomicWaker;

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

        let atomic_waker = AtomicWaker::new();
        atomic_waker.register(&custom_waker);

        assert!(!woken.load(Ordering::Acquire));
        atomic_waker.wake();
        assert!(woken.load(Ordering::Acquire));
        assert_eq!(count.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn test_multiple_wake() {
        let woken = Arc::new(AtomicBool::new(false));
        let count = Arc::new(AtomicUsize::new(0));
        let custom_waker = Waker::from(Arc::new(TestWaker::new(woken.clone(), count.clone())));

        let atomic_waker = AtomicWaker::new();
        atomic_waker.register(&custom_waker);

        atomic_waker.wake();
        atomic_waker.wake();
        assert!(woken.load(Ordering::Acquire));
        assert_eq!(count.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn test_concurrent_register() {
        let atomic_waker = Arc::new(AtomicWaker::new());

        let woken1 = Arc::new(AtomicBool::new(false));
        let count1 = Arc::new(AtomicUsize::new(0));
        let w1 = Arc::new(TestWaker::new(woken1.clone(), count1.clone()));

        let woken2 = Arc::new(AtomicBool::new(false));
        let count2 = Arc::new(AtomicUsize::new(0));
        let w2 = Arc::new(TestWaker::new(woken2.clone(), count2.clone()));

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

        atomic_waker.wake();

        let woke1 = woken1.load(Ordering::Acquire);
        let woke2 = woken2.load(Ordering::Acquire);
        assert!(
            woke1 || woke2,
            "at least one concurrent registration must remain wakeable"
        );
    }

    #[test]
    fn test_reregister_different_waker() {
        let woken1 = Arc::new(AtomicBool::new(false));
        let count1 = Arc::new(AtomicUsize::new(0));
        let waker1 = Waker::from(Arc::new(TestWaker::new(woken1.clone(), count1.clone())));

        let woken2 = Arc::new(AtomicBool::new(false));
        let count2 = Arc::new(AtomicUsize::new(0));
        let waker2 = Waker::from(Arc::new(TestWaker::new(woken2.clone(), count2.clone())));

        let atomic_waker = AtomicWaker::new();
        atomic_waker.register(&waker1);
        atomic_waker.register(&waker2);

        atomic_waker.wake();
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
            let atomic_waker = AtomicWaker::new();
            atomic_waker.register(&custom_waker);
        }

        assert!(!woken.load(Ordering::Acquire));
        assert_eq!(count.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn test_take_waker() {
        let woken = Arc::new(AtomicBool::new(false));
        let count = Arc::new(AtomicUsize::new(0));
        let custom_waker = Waker::from(Arc::new(TestWaker::new(woken.clone(), count.clone())));

        let atomic_waker = AtomicWaker::new();
        assert!(atomic_waker.take().is_none());

        atomic_waker.register(&custom_waker);

        let taken = atomic_waker.take();
        assert!(taken.is_some());
        assert!(atomic_waker.take().is_none());

        taken.unwrap().wake();
        assert!(woken.load(Ordering::Acquire));
        assert_eq!(count.load(Ordering::SeqCst), 1);
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
    use veloq_waker::AtomicWaker;

    struct TestWaker(Arc<AtomicBool>);

    impl Wake for TestWaker {
        fn wake(self: StdArc<Self>) {
            self.0.store(true, Ordering::Release);
        }
    }

    #[test]
    fn test_atomic_waker_loom_concurrent_register() {
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

            atomic_waker.wake();

            let woke1 = woken1.load(Ordering::Acquire);
            let woke2 = woken2.load(Ordering::Acquire);
            assert!(
                woke1 || woke2,
                "at least one concurrent registration must remain wakeable"
            );
        });
    }

    #[test]
    fn test_atomic_waker_loom_register_wake() {
        loom::model(|| {
            let atomic_waker = Arc::new(AtomicWaker::new());
            let woken = Arc::new(AtomicBool::new(false));
            let custom_waker = Waker::from(StdArc::new(TestWaker(woken.clone())));

            let waker_clone = atomic_waker.clone();
            let handle = thread::spawn(move || {
                waker_clone.wake();
            });

            atomic_waker.register(&custom_waker);
            handle.join().unwrap();

            atomic_waker.wake();
            assert!(woken.load(Ordering::Acquire));
        });
    }
}
