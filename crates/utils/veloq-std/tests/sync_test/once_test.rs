#[cfg(not(feature = "loom"))]
mod normal_tests {
    use std::panic;
    use veloq_std::{sync::Once, thread, time::Duration};

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
