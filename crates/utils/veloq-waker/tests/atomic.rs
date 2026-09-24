#[cfg(not(feature = "loom"))]
mod normal_tests {
    use veloq_std::{
        sync::{
            Arc, Barrier, Weak,
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
        fn wake(&self) {
            self.woken.store(true, Ordering::Release);
            self.wake_count.fetch_add(1, Ordering::SeqCst);
        }
    }

    struct ReentrantWaker {
        target: Arc<AtomicWaker>,
        replacement: Waker,
        reentered: Arc<AtomicBool>,
    }

    impl Wake for ReentrantWaker {
        fn wake(&self) {
            self.reentered.store(true, Ordering::Release);
            self.target.register(&self.replacement);
        }
    }

    struct DropReentrantWaker {
        target: Weak<AtomicWaker>,
        drop_count: Arc<AtomicUsize>,
        reentered: Arc<AtomicBool>,
    }

    impl Wake for DropReentrantWaker {
        fn wake(&self) {}
    }

    impl Drop for DropReentrantWaker {
        fn drop(&mut self) {
            self.drop_count.fetch_add(1, Ordering::SeqCst);
            if let Some(target) = self.target.upgrade() {
                let _ = target.take();
                self.reentered.store(true, Ordering::Release);
            }
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
        })
        .expect("thread spawn failed");

        let aw2 = atomic_waker.clone();
        let waker2_clone = waker2.clone();
        let t2 = thread::spawn(move || {
            aw2.register(&waker2_clone);
        })
        .expect("thread spawn failed");

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

    #[test]
    fn test_replacement_register_and_multiple_take_concurrent() {
        let atomic_waker = Arc::new(AtomicWaker::new());
        let first_count = Arc::new(AtomicUsize::new(0));
        let first_waker = Waker::from(Arc::new(TestWaker::new(
            Arc::new(AtomicBool::new(false)),
            first_count.clone(),
        )));
        atomic_waker.register(&first_waker);

        let replacement_count = Arc::new(AtomicUsize::new(0));
        let replacement_waker = Waker::from(Arc::new(TestWaker::new(
            Arc::new(AtomicBool::new(false)),
            replacement_count.clone(),
        )));
        let barrier = Arc::new(Barrier::new(4));

        let register_slot = atomic_waker.clone();
        let register_barrier = barrier.clone();
        let register_handle = thread::spawn(move || {
            register_barrier.wait();
            register_slot.register(&replacement_waker);
        })
        .expect("thread spawn failed");

        let take_slot = atomic_waker.clone();
        let take_barrier = barrier.clone();
        let take_handle = thread::spawn(move || {
            take_barrier.wait();
            if let Some(waker) = take_slot.take() {
                waker.wake();
            }
        })
        .expect("thread spawn failed");

        let second_take_slot = atomic_waker.clone();
        let second_take_barrier = barrier.clone();
        let second_take_handle = thread::spawn(move || {
            second_take_barrier.wait();
            if let Some(waker) = second_take_slot.take() {
                waker.wake();
            }
        })
        .expect("thread spawn failed");

        barrier.wait();
        register_handle.join().unwrap();
        take_handle.join().unwrap();
        second_take_handle.join().unwrap();

        if let Some(waker) = atomic_waker.take() {
            waker.wake();
        }

        let first_wakes = first_count.load(Ordering::SeqCst);
        let replacement_wakes = replacement_count.load(Ordering::SeqCst);
        assert!(first_wakes <= 1);
        assert!(replacement_wakes <= 1);
        assert!((1..=2).contains(&(first_wakes + replacement_wakes)));
    }

    #[test]
    fn test_waker_wake_can_reenter_and_register() {
        let atomic_waker = Arc::new(AtomicWaker::new());
        let replacement_count = Arc::new(AtomicUsize::new(0));
        let replacement = Waker::from(Arc::new(TestWaker::new(
            Arc::new(AtomicBool::new(false)),
            replacement_count.clone(),
        )));
        let reentered = Arc::new(AtomicBool::new(false));
        let reentrant = Waker::from(Arc::new(ReentrantWaker {
            target: atomic_waker.clone(),
            replacement,
            reentered: reentered.clone(),
        }));

        atomic_waker.register(&reentrant);
        atomic_waker.take().unwrap().wake();

        assert!(reentered.load(Ordering::Acquire));
        atomic_waker.take().unwrap().wake();
        assert_eq!(replacement_count.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn test_replacement_drop_can_reenter_take() {
        let drop_count = Arc::new(AtomicUsize::new(0));
        let reentered = Arc::new(AtomicBool::new(false));
        let atomic_waker = Arc::new(AtomicWaker::new());
        let first = Waker::from(Arc::new(DropReentrantWaker {
            target: Arc::downgrade(&atomic_waker),
            drop_count: drop_count.clone(),
            reentered: reentered.clone(),
        }));
        atomic_waker.register(&first);
        drop(first);

        let replacement = Waker::noop().clone();
        atomic_waker.register(&replacement);
        drop(replacement);
        if let Some(waker) = atomic_waker.take() {
            waker.wake();
        }
        drop(atomic_waker);

        assert_eq!(drop_count.load(Ordering::SeqCst), 1);
        assert!(reentered.load(Ordering::Acquire));
    }
}

#[cfg(feature = "loom")]
mod loom_tests {
    use veloq_std::{
        sync::{
            Arc, NativeArc as StdArc, Weak,
            atomic::{AtomicBool, AtomicUsize, Ordering},
        },
        task::{Wake, Waker},
        thread,
    };
    use veloq_waker::AtomicWaker;

    struct TestWaker(Arc<AtomicBool>);

    impl Wake for TestWaker {
        fn wake(&self) {
            self.0.store(true, Ordering::Release);
        }
    }

    struct CountWaker(Arc<AtomicUsize>);

    impl Wake for CountWaker {
        fn wake(&self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    struct ReentrantWaker {
        target: Arc<AtomicWaker>,
        replacement: Waker,
        reentered: Arc<AtomicBool>,
    }

    impl Wake for ReentrantWaker {
        fn wake(&self) {
            self.reentered.store(true, Ordering::Release);
            self.target.register(&self.replacement);
        }
    }

    struct DropReentrantWaker {
        target: Weak<AtomicWaker>,
        drop_count: Arc<AtomicUsize>,
        reentered: Arc<AtomicBool>,
    }

    impl Wake for DropReentrantWaker {
        fn wake(&self) {}
    }

    impl Drop for DropReentrantWaker {
        fn drop(&mut self) {
            self.drop_count.fetch_add(1, Ordering::SeqCst);
            if let Some(target) = self.target.upgrade() {
                let _ = target.take();
                self.reentered.store(true, Ordering::Release);
            }
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
            })
            .expect("thread spawn failed");

            let aw2 = atomic_waker.clone();
            let waker2_clone = waker2.clone();
            let t2 = thread::spawn(move || {
                aw2.register(&waker2_clone);
            })
            .expect("thread spawn failed");

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
            })
            .expect("thread spawn failed");

            atomic_waker.register(&custom_waker);
            handle.join().unwrap();

            atomic_waker.wake();
            assert!(woken.load(Ordering::Acquire));
        });
    }

    #[test]
    fn test_atomic_waker_loom_replacement_register_and_multiple_take() {
        let mut builder = loom::model::Builder::new();
        builder.preemption_bound = Some(3);
        builder.check(|| {
            let atomic_waker = Arc::new(AtomicWaker::new());
            let first_count = Arc::new(AtomicUsize::new(0));
            let first_waker = Waker::from(StdArc::new(CountWaker(first_count.clone())));
            atomic_waker.register(&first_waker);

            let replacement_count = Arc::new(AtomicUsize::new(0));
            let replacement_waker = Waker::from(StdArc::new(CountWaker(replacement_count.clone())));

            let take_slot = atomic_waker.clone();
            let take_handle = thread::spawn(move || {
                if let Some(waker) = take_slot.take() {
                    waker.wake();
                }
            })
            .expect("thread spawn failed");

            let second_take_slot = atomic_waker.clone();
            let second_take_handle = thread::spawn(move || {
                if let Some(waker) = second_take_slot.take() {
                    waker.wake();
                }
            })
            .expect("thread spawn failed");

            thread::yield_now().unwrap();
            atomic_waker.register(&replacement_waker);
            take_handle.join().unwrap();
            second_take_handle.join().unwrap();
            if let Some(waker) = atomic_waker.take() {
                waker.wake();
            }

            let first_wakes = first_count.load(Ordering::SeqCst);
            let replacement_wakes = replacement_count.load(Ordering::SeqCst);
            assert!(first_wakes <= 1);
            assert!(replacement_wakes <= 1);
            assert!((1..=2).contains(&(first_wakes + replacement_wakes)));
        });
    }

    #[test]
    fn test_atomic_waker_loom_wake_can_reenter_and_register() {
        loom::model(|| {
            let atomic_waker = Arc::new(AtomicWaker::new());
            let replacement_count = Arc::new(AtomicUsize::new(0));
            let replacement = Waker::from(StdArc::new(CountWaker(replacement_count.clone())));
            let reentered = Arc::new(AtomicBool::new(false));
            let reentrant = Waker::from(StdArc::new(ReentrantWaker {
                target: atomic_waker.clone(),
                replacement,
                reentered: reentered.clone(),
            }));

            atomic_waker.register(&reentrant);
            atomic_waker.take().unwrap().wake();

            assert!(reentered.load(Ordering::Acquire));
            atomic_waker.take().unwrap().wake();
            assert_eq!(replacement_count.load(Ordering::SeqCst), 1);
        });
    }

    #[test]
    fn test_atomic_waker_loom_replacement_drop_can_reenter_take() {
        let mut builder = loom::model::Builder::new();
        builder.preemption_bound = Some(3);
        builder.check(|| {
            let atomic_waker = Arc::new(AtomicWaker::new());
            let drop_count = Arc::new(AtomicUsize::new(0));
            let first = Waker::from(StdArc::new(DropReentrantWaker {
                target: Arc::downgrade(&atomic_waker),
                drop_count: drop_count.clone(),
                reentered: Arc::new(AtomicBool::new(false)),
            }));
            atomic_waker.register(&first);
            drop(first);

            let replacement = Waker::noop().clone();
            let take_slot = atomic_waker.clone();
            let take_handle = thread::spawn(move || {
                let _ = take_slot.take();
            })
            .expect("thread spawn failed");
            atomic_waker.register(&replacement);
            take_handle.join().unwrap();
            drop(replacement);
            if let Some(waker) = atomic_waker.take() {
                waker.wake();
            }
            assert_eq!(drop_count.load(Ordering::SeqCst), 1);
        });
    }
}
