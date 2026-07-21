#![cfg(not(feature = "loom"))]

use veloq_std::{
    sync::atomic::{CoreAtomicU32, Ordering},
    sync::{Once, OnceLock},
    thread,
    time::Duration,
};

use std::panic;

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

#[test]
fn test_once_lock_basic() {
    let lock = OnceLock::new();
    assert_eq!(lock.get(), None);

    assert_eq!(lock.set(42), Ok(()));
    assert_eq!(lock.get(), Some(&42));
    assert_eq!(lock.set(100), Err(100));
}

#[test]
fn test_once_lock_try_insert() {
    let lock = OnceLock::new();
    assert_eq!(lock.try_insert(42), Ok(&42));
    assert_eq!(lock.try_insert(100), Err((&42, 100)));
}

#[test]
fn test_once_lock_get_or_init() {
    let lock = OnceLock::new();
    let val = lock.get_or_init(|| 42);
    assert_eq!(*val, 42);

    let val2 = lock.get_or_init(|| 100);
    assert_eq!(*val2, 42);
}

#[test]
fn test_once_lock_get_mut() {
    let mut lock = OnceLock::new();
    assert_eq!(lock.get_mut(), None);

    lock.set(42).unwrap();
    assert_eq!(lock.get_mut(), Some(&mut 42));

    *lock.get_mut().unwrap() = 100;
    assert_eq!(lock.get(), Some(&100));
}

#[test]
fn test_once_lock_get_mut_or_init() {
    let mut lock = OnceLock::new();
    let val = lock.get_mut_or_init(|| 42);
    assert_eq!(*val, 42);
    *val = 100;

    let val2 = lock.get_mut_or_init(|| 200);
    assert_eq!(*val2, 100);
}

#[test]
fn test_once_lock_get_or_try_init() {
    let lock = OnceLock::new();

    // Failed init
    let res: Result<&i32, &str> = lock.get_or_try_init(|| Err("error"));
    assert_eq!(res, Err("error"));
    assert_eq!(lock.get(), None);

    // Successful init after failure
    let res2: Result<&i32, &str> = lock.get_or_try_init(|| Ok(42));
    assert_eq!(res2, Ok(&42));
    assert_eq!(lock.get(), Some(&42));
}

#[test]
fn test_once_lock_get_mut_or_try_init() {
    let mut lock = OnceLock::new();

    // Failed init
    let res: Result<&mut i32, &str> = lock.get_mut_or_try_init(|| Err("error"));
    assert_eq!(res, Err("error"));
    assert_eq!(lock.get_mut(), None);

    // Successful init after failure
    let res2: Result<&mut i32, &str> = lock.get_mut_or_try_init(|| Ok(42));
    assert_eq!(res2, Ok(&mut 42));
    assert_eq!(lock.get(), Some(&42));
}

#[test]
fn test_once_lock_into_inner() {
    let lock: OnceLock<i32> = OnceLock::new();
    assert_eq!(lock.into_inner(), None);

    let lock2 = OnceLock::from(42);
    assert_eq!(lock2.into_inner(), Some(42));
}

#[test]
fn test_once_lock_take() {
    let mut lock = OnceLock::new();
    lock.set(42).unwrap();
    assert_eq!(lock.take(), Some(42));
    assert_eq!(lock.get(), None);
}

#[test]
fn test_once_lock_wait() {
    let lock = OnceLock::new();
    thread::scope(|s| {
        s.spawn(|| {
            lock.get_or_init(|| {
                thread::sleep(Duration::from_millis(50)).unwrap();
                42
            });
        })
        .unwrap();

        s.spawn(|| {
            assert_eq!(*lock.wait(), 42);
        })
        .unwrap();
    });
}

#[test]
fn test_once_lock_traits() {
    // Default
    let lock: OnceLock<i32> = Default::default();
    assert_eq!(lock.get(), None);

    // From
    let lock_from = OnceLock::from(42);
    assert_eq!(lock_from.get(), Some(&42));

    // Clone
    let lock_clone = lock_from.clone();
    assert_eq!(lock_clone.get(), Some(&42));

    // PartialEq / Eq
    assert_eq!(lock_from, lock_clone);
    let lock_empty: OnceLock<i32> = OnceLock::new();
    assert_ne!(lock_from, lock_empty);

    // Debug
    let debug_empty = format!("{lock_empty:?}");
    assert!(debug_empty.contains("<uninit>"));
    let debug_full = format!("{lock_from:?}");
    assert!(debug_full.contains("42"));
}

#[test]
fn test_once_lock_drop() {
    static DROP_COUNTER: CoreAtomicU32 = CoreAtomicU32::new(0);
    struct Detector;
    impl Drop for Detector {
        fn drop(&mut self) {
            DROP_COUNTER.fetch_add(1, Ordering::SeqCst);
        }
    }

    {
        let lock = OnceLock::new();
        let _ = lock.set(Detector);
    }
    assert_eq!(DROP_COUNTER.load(Ordering::SeqCst), 1);
}

#[cfg(not(feature = "loom"))]
mod condvar_tests {
    use std::vec::Vec;
    use veloq_std::sync::{Arc, Condvar, Mutex};
    use veloq_std::thread;
    use veloq_std::time::Duration;

    #[test]
    fn test_condvar_basic() {
        let pair = Arc::new((Mutex::new(false), Condvar::new()));
        let pair2 = pair.clone();

        thread::spawn(move || {
            let (lock, cvar) = &*pair2;
            let mut started = lock.lock();
            *started = true;
            cvar.notify_one();
        })
        .unwrap()
        .join()
        .unwrap();

        let (lock, cvar) = &*pair;
        let mut started = lock.lock();
        while !*started {
            started = cvar.wait(started);
        }
        assert!(*started);
    }

    #[test]
    fn test_condvar_timeout() {
        let pair = Arc::new((Mutex::new(false), Condvar::new()));
        let pair2 = pair.clone();

        let handle = thread::spawn(move || {
            let (lock, cvar) = &*pair2;
            let mut started = lock.lock();
            *started = true;
            cvar.notify_one();
        })
        .unwrap();

        let (lock, cvar) = &*pair;
        let mut started = lock.lock();
        while !*started {
            let (g, res) = cvar.wait_timeout(started, Duration::from_millis(100));
            started = g;
            if res.timed_out() {
                break;
            }
        }
        assert!(*started);
        handle.join().unwrap();
    }

    #[test]
    fn test_condvar_timeout_expired() {
        let pair = Arc::new((Mutex::new(false), Condvar::new()));
        let (lock, cvar) = &*pair;
        let started = lock.lock();
        let (_g, res) = cvar.wait_timeout(started, Duration::from_millis(10));
        assert!(res.timed_out());
    }

    #[test]
    fn test_condvar_notify_all() {
        let pair = Arc::new((Mutex::new(0), Condvar::new()));
        let mut handles = Vec::new();

        for _ in 0..3 {
            let pair_clone = pair.clone();
            let handle = thread::spawn(move || {
                let (lock, cvar) = &*pair_clone;
                let mut count = lock.lock();
                while *count == 0 {
                    count = cvar.wait(count);
                }
                *count += 1;
            })
            .unwrap();
            handles.push(handle);
        }

        thread::sleep(Duration::from_millis(20)).unwrap();

        let (lock, cvar) = &*pair;
        {
            let mut count = lock.lock();
            *count = 1;
            cvar.notify_all();
        }

        for handle in handles {
            handle.join().unwrap();
        }

        assert_eq!(*lock.lock(), 4);
    }
}

#[cfg(feature = "loom")]
mod loom_tests {
    use loom::thread;
    use veloq_std::sync::{Arc, Condvar, Mutex};

    #[test]
    fn test_loom_condvar() {
        loom::model(|| {
            let pair = Arc::new((Mutex::new(false), Condvar::new()));
            let pair2 = pair.clone();

            thread::spawn(move || {
                let (lock, cvar) = &*pair2;
                let mut started = lock.lock();
                *started = true;
                cvar.notify_one();
            });

            let (lock, cvar) = &*pair;
            let mut started = lock.lock();
            while !*started {
                started = cvar.wait(started);
            }
            assert!(*started);
        });
    }
}
