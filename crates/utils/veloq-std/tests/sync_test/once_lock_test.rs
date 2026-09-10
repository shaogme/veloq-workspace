#[cfg(not(feature = "loom"))]
mod normal_tests {
    use veloq_std::{
        sync::{
            OnceLock,
            atomic::{NativeAtomicU32, Ordering},
        },
        thread,
        time::Duration,
    };

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
        static DROP_COUNTER: NativeAtomicU32 = NativeAtomicU32::new(0);
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
}
