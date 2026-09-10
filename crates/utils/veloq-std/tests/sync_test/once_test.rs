#[cfg(not(feature = "loom"))]
mod normal_tests {
    use std::panic;

    use veloq_std::{
        sync::{
            NativeOnce, Once,
            atomic::{NativeAtomicU32, Ordering},
        },
        thread,
        time::Duration,
    };

    static STATIC_NATIVE_ONCE: NativeOnce = NativeOnce::new();
    static STATIC_ONCE: Once = Once::new();

    #[test]
    fn test_static_once() {
        assert!(!STATIC_NATIVE_ONCE.is_completed());
        assert!(!STATIC_ONCE.is_completed());
    }

    #[test]
    fn test_native_once_concurrency() {
        static ONCE: NativeOnce = NativeOnce::new();
        static CALL_COUNT: NativeAtomicU32 = NativeAtomicU32::new(0);

        thread::scope(|s| {
            for _ in 0..10 {
                s.spawn(|| {
                    ONCE.call_once(|| {
                        CALL_COUNT.fetch_add(1, Ordering::SeqCst);
                    });
                })
                .unwrap();
            }
        });

        assert_eq!(CALL_COUNT.load(Ordering::SeqCst), 1);
        assert!(ONCE.is_completed());
    }

    #[test]
    fn test_once_basic() {
        let once = Once::new();
        assert!(!once.is_completed());

        let mut counter = 0;
        once.call_once(|| {
            counter += 1;
        });
        assert_eq!(counter, 1);
        assert!(once.is_completed());

        once.call_once(|| {
            counter += 1;
        });
        assert_eq!(counter, 1);
    }

    #[test]
    fn test_once_panic_poison() {
        let once = Once::new();

        let res = panic::catch_unwind(panic::AssertUnwindSafe(|| {
            once.call_once(|| {
                panic!("poisoning");
            });
        }));
        assert!(res.is_err());
        assert!(!once.is_completed());

        // Subsequent call_once should panic due to poison
        let res2 = panic::catch_unwind(panic::AssertUnwindSafe(|| {
            once.call_once(|| {});
        }));
        assert!(res2.is_err());
    }

    #[test]
    fn test_once_force_recovery() {
        let once = Once::new();

        let _ = panic::catch_unwind(panic::AssertUnwindSafe(|| {
            once.call_once(|| {
                panic!("poisoning");
            });
        }));

        let mut recovered = false;
        once.call_once_force(|state| {
            assert!(state.is_poisoned());
            recovered = true;
        });
        assert!(recovered);
        assert!(once.is_completed());
    }

    #[test]
    fn test_once_wait() {
        let once = Once::new();
        thread::scope(|s| {
            s.spawn(|| {
                once.call_once(|| {
                    thread::sleep(Duration::from_millis(50)).unwrap();
                });
            })
            .unwrap();

            s.spawn(|| {
                once.wait();
                assert!(once.is_completed());
            })
            .unwrap();
        });
    }

    #[test]
    fn test_once_wait_force() {
        let once = Once::new();
        let _ = panic::catch_unwind(panic::AssertUnwindSafe(|| {
            once.call_once(|| {
                panic!("poisoning");
            });
        }));

        // wait should panic
        let res = panic::catch_unwind(panic::AssertUnwindSafe(|| {
            once.wait();
        }));
        assert!(res.is_err());

        // Spawn a thread to initialize it and wake up wait_force
        thread::scope(|s| {
            s.spawn(|| {
                thread::sleep(Duration::from_millis(50)).unwrap();
                once.call_once_force(|state| {
                    assert!(state.is_poisoned());
                });
            })
            .unwrap();

            // wait_force should block until the other thread completes the initialization
            once.wait_force();
        });
        assert!(once.is_completed());
    }

    #[test]
    fn test_once_debug() {
        let once = Once::new();
        let format_str = format!("{once:?}");
        assert!(format_str.contains("Once"));
    }
}

#[cfg(feature = "loom")]
mod loom_tests {
    use core::sync::atomic::Ordering;
    use std::panic;

    use loom::{
        sync::{Arc, atomic::AtomicU32},
        thread,
    };
    use veloq_std::sync::{LoomOnce, NativeOnce, Once};

    static STATIC_NATIVE_ONCE: NativeOnce = NativeOnce::new();

    #[test]
    fn test_loom_type_exports() {
        assert!(!STATIC_NATIVE_ONCE.is_completed());
        loom::model(|| {
            let _once = Once::new();
            let _loom_once = LoomOnce::new();
        });
    }

    #[test]
    fn test_loom_once_concurrency() {
        loom::model(|| {
            let once = Arc::new(LoomOnce::new());
            let executed = Arc::new(AtomicU32::new(0));

            let mut handles = Vec::new();
            for _ in 0..2 {
                let o = once.clone();
                let e = executed.clone();
                handles.push(thread::spawn(move || {
                    o.call_once(|| {
                        e.fetch_add(1, Ordering::SeqCst);
                    });
                }));
            }

            for h in handles {
                h.join().unwrap();
            }

            assert_eq!(executed.load(Ordering::SeqCst), 1);
            assert!(once.is_completed());
        });
    }

    #[test]
    fn test_loom_once_poison_and_recovery() {
        loom::model(|| {
            let once = Arc::new(LoomOnce::new());
            let o1 = once.clone();
            let h1 = thread::spawn(move || {
                let _ = panic::catch_unwind(panic::AssertUnwindSafe(|| {
                    o1.call_once(|| {
                        panic!("loom poison");
                    });
                }));
            });
            h1.join().unwrap();

            assert!(!once.is_completed());

            let mut recovered = false;
            once.call_once_force(|state| {
                assert!(state.is_poisoned());
                recovered = true;
            });
            assert!(recovered);
            assert!(once.is_completed());
        });
    }
}
