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
        fn wake(&self) {
            self.woken.store(true, Ordering::Release);
            self.wake_count.fetch_add(1, Ordering::SeqCst);
        }
    }

    struct ReentrantWaker {
        target: Arc<MwsrWaker>,
        replacement: Waker,
        reentered: Arc<AtomicBool>,
    }

    impl Wake for ReentrantWaker {
        fn wake(&self) {
            self.reentered.store(true, Ordering::Release);
            unsafe {
                self.target.register(&self.replacement);
            }
        }
    }

    struct DropReentrantWaker {
        target: Weak<MwsrWaker>,
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

    fn wake(waker: &MwsrWaker) {
        if let Some(waker) = waker.take() {
            waker.wake();
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
        wake(&mpsc_waker);
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

        wake(&mpsc_waker);
        wake(&mpsc_waker);
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

        wake(&mpsc_waker);
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
                wake(&waker_clone);
            })
            .expect("thread spawn failed");

            unsafe {
                mpsc_waker.register(&custom_waker);
            }
            handle.join().unwrap();

            wake(&mpsc_waker);
            assert!(woken.load(Ordering::Acquire));
        }
    }

    #[test]
    fn test_replacement_register_and_multiple_take_concurrent() {
        let mpsc_waker = Arc::new(MwsrWaker::new());
        let first_count = Arc::new(AtomicUsize::new(0));
        let first_waker = Waker::from(Arc::new(TestWaker::new(
            Arc::new(AtomicBool::new(false)),
            first_count.clone(),
        )));
        unsafe {
            mpsc_waker.register(&first_waker);
        }

        let replacement_count = Arc::new(AtomicUsize::new(0));
        let replacement_waker = Waker::from(Arc::new(TestWaker::new(
            Arc::new(AtomicBool::new(false)),
            replacement_count.clone(),
        )));
        let barrier = Arc::new(Barrier::new(4));

        let register_slot = mpsc_waker.clone();
        let register_barrier = barrier.clone();
        let register_handle = thread::spawn(move || {
            register_barrier.wait();
            unsafe {
                register_slot.register(&replacement_waker);
            }
        })
        .expect("thread spawn failed");

        let take_slot = mpsc_waker.clone();
        let take_barrier = barrier.clone();
        let take_handle = thread::spawn(move || {
            take_barrier.wait();
            wake(&take_slot);
        })
        .expect("thread spawn failed");

        let second_take_slot = mpsc_waker.clone();
        let second_take_barrier = barrier.clone();
        let second_take_handle = thread::spawn(move || {
            second_take_barrier.wait();
            wake(&second_take_slot);
        })
        .expect("thread spawn failed");

        barrier.wait();
        register_handle.join().unwrap();
        take_handle.join().unwrap();
        second_take_handle.join().unwrap();
        wake(&mpsc_waker);

        let first_wakes = first_count.load(Ordering::SeqCst);
        let replacement_wakes = replacement_count.load(Ordering::SeqCst);
        assert!(first_wakes <= 1);
        assert!(replacement_wakes <= 1);
        assert!((1..=2).contains(&(first_wakes + replacement_wakes)));
    }

    #[test]
    fn test_waker_wake_can_reenter_and_register() {
        let mpsc_waker = Arc::new(MwsrWaker::new());
        let replacement_count = Arc::new(AtomicUsize::new(0));
        let replacement = Waker::from(Arc::new(TestWaker::new(
            Arc::new(AtomicBool::new(false)),
            replacement_count.clone(),
        )));
        let reentered = Arc::new(AtomicBool::new(false));
        let reentrant = Waker::from(Arc::new(ReentrantWaker {
            target: mpsc_waker.clone(),
            replacement,
            reentered: reentered.clone(),
        }));

        unsafe {
            mpsc_waker.register(&reentrant);
        }
        mpsc_waker.take().unwrap().wake();

        assert!(reentered.load(Ordering::Acquire));
        mpsc_waker.take().unwrap().wake();
        assert_eq!(replacement_count.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn test_replacement_drop_can_reenter_take() {
        let drop_count = Arc::new(AtomicUsize::new(0));
        let reentered = Arc::new(AtomicBool::new(false));
        let mpsc_waker = Arc::new(MwsrWaker::new());
        let first = Waker::from(Arc::new(DropReentrantWaker {
            target: Arc::downgrade(&mpsc_waker),
            drop_count: drop_count.clone(),
            reentered: reentered.clone(),
        }));
        unsafe {
            mpsc_waker.register(&first);
        }
        drop(first);

        let replacement = Waker::noop().clone();
        unsafe {
            mpsc_waker.register(&replacement);
        }
        drop(replacement);
        wake(&mpsc_waker);
        drop(mpsc_waker);

        assert_eq!(drop_count.load(Ordering::SeqCst), 1);
        assert!(reentered.load(Ordering::Acquire));
    }
}

#[cfg(feature = "loom")]
mod loom_tests {
    use loom::future::block_on;
    use veloq_std::{
        sync::{
            Arc, NativeArc as StdArc, Weak,
            atomic::{AtomicBool, AtomicUsize, Ordering},
        },
        task::{Wake, Waker},
        thread,
    };
    use veloq_waker::MwsrWaker;

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
        target: Arc<MwsrWaker>,
        replacement: Waker,
        reentered: Arc<AtomicBool>,
    }

    impl Wake for ReentrantWaker {
        fn wake(&self) {
            self.reentered.store(true, Ordering::Release);
            unsafe {
                self.target.register(&self.replacement);
            }
        }
    }

    struct DropReentrantWaker {
        target: Weak<MwsrWaker>,
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

    fn wake(waker: &MwsrWaker) {
        if let Some(waker) = waker.take() {
            waker.wake();
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
                wake(&waker_clone);
            })
            .expect("thread spawn failed");

            unsafe {
                mpsc_waker.register(&custom_waker);
            }
            handle.join().unwrap();

            wake(&mpsc_waker);
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
            })
            .expect("thread spawn failed");

            if let Some(w) = mpsc_waker.take() {
                w.wake();
            }

            handle.join().unwrap();
            assert!(woken.load(Ordering::Acquire));
        });
    }

    #[test]
    fn test_mpsc_waker_block_on_waker_ownership() {
        loom::model(|| {
            let mpsc_waker = MwsrWaker::new();
            block_on(async {
                veloq_std::future::poll_fn(|cx| {
                    unsafe {
                        mpsc_waker.register(cx.waker());
                    }
                    if let Some(waker) = mpsc_waker.take() {
                        drop(waker);
                        veloq_std::task::Poll::Ready(())
                    } else {
                        veloq_std::task::Poll::Pending
                    }
                })
                .await;
            });
        });
    }

    #[test]
    fn test_mpsc_waker_loom_replacement_register_and_multiple_take() {
        let mut builder = loom::model::Builder::new();
        builder.preemption_bound = Some(3);
        builder.check(|| {
            let mpsc_waker = Arc::new(MwsrWaker::new());
            let first_count = Arc::new(AtomicUsize::new(0));
            let first_waker = Waker::from(StdArc::new(CountWaker(first_count.clone())));
            unsafe {
                mpsc_waker.register(&first_waker);
            }

            let replacement_count = Arc::new(AtomicUsize::new(0));
            let replacement_waker = Waker::from(StdArc::new(CountWaker(replacement_count.clone())));

            let take_slot = mpsc_waker.clone();
            let take_handle = thread::spawn(move || {
                wake(&take_slot);
            })
            .expect("thread spawn failed");

            let second_take_slot = mpsc_waker.clone();
            let second_take_handle = thread::spawn(move || {
                wake(&second_take_slot);
            })
            .expect("thread spawn failed");

            thread::yield_now().unwrap();
            unsafe {
                mpsc_waker.register(&replacement_waker);
            }
            take_handle.join().unwrap();
            second_take_handle.join().unwrap();
            wake(&mpsc_waker);

            let first_wakes = first_count.load(Ordering::SeqCst);
            let replacement_wakes = replacement_count.load(Ordering::SeqCst);
            assert!(first_wakes <= 1);
            assert!(replacement_wakes <= 1);
            assert!((1..=2).contains(&(first_wakes + replacement_wakes)));
        });
    }

    #[test]
    fn test_mpsc_waker_loom_wake_can_reenter_and_register() {
        loom::model(|| {
            let mpsc_waker = Arc::new(MwsrWaker::new());
            let replacement_count = Arc::new(AtomicUsize::new(0));
            let replacement = Waker::from(StdArc::new(CountWaker(replacement_count.clone())));
            let reentered = Arc::new(AtomicBool::new(false));
            let reentrant = Waker::from(StdArc::new(ReentrantWaker {
                target: mpsc_waker.clone(),
                replacement,
                reentered: reentered.clone(),
            }));

            unsafe {
                mpsc_waker.register(&reentrant);
            }
            mpsc_waker.take().unwrap().wake();

            assert!(reentered.load(Ordering::Acquire));
            mpsc_waker.take().unwrap().wake();
            assert_eq!(replacement_count.load(Ordering::SeqCst), 1);
        });
    }

    #[test]
    fn test_mpsc_waker_loom_replacement_drop_can_reenter_take() {
        let mut builder = loom::model::Builder::new();
        builder.preemption_bound = Some(3);
        builder.check(|| {
            let mpsc_waker = Arc::new(MwsrWaker::new());
            let drop_count = Arc::new(AtomicUsize::new(0));
            let reentered = Arc::new(AtomicBool::new(false));
            let first = Waker::from(StdArc::new(DropReentrantWaker {
                target: Arc::downgrade(&mpsc_waker),
                drop_count: drop_count.clone(),
                reentered: reentered.clone(),
            }));
            unsafe {
                mpsc_waker.register(&first);
            }
            drop(first);

            let replacement = Waker::noop().clone();
            let take_slot = mpsc_waker.clone();
            let take_handle = thread::spawn(move || {
                let _ = take_slot.take();
            })
            .expect("thread spawn failed");
            unsafe {
                mpsc_waker.register(&replacement);
            }
            take_handle.join().unwrap();
            drop(replacement);
            wake(&mpsc_waker);
            assert_eq!(drop_count.load(Ordering::SeqCst), 1);
            assert!(reentered.load(Ordering::Acquire));
        });
    }
}
