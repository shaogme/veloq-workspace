#[cfg(not(feature = "loom"))]
mod normal_tests {
    use std::panic::{AssertUnwindSafe, catch_unwind};

    use veloq_std::{
        sync::{
            LazyLock, NativeLazyLock,
            atomic::{NativeAtomicU32, Ordering},
        },
        thread,
    };

    static STATIC_NATIVE_LAZY: NativeLazyLock<i32> = NativeLazyLock::new(|| 7);
    static STATIC_LAZY: LazyLock<i32> = LazyLock::new(|| 9);

    #[test]
    fn test_static_lazy_lock() {
        assert_eq!(NativeLazyLock::get(&STATIC_NATIVE_LAZY), None);
        assert_eq!(LazyLock::get(&STATIC_LAZY), None);

        assert_eq!(*NativeLazyLock::force(&STATIC_NATIVE_LAZY), 7);
        assert_eq!(*LazyLock::force(&STATIC_LAZY), 9);
    }

    #[test]
    fn test_lazy_lock_initializes_once_concurrently() {
        let count = veloq_std::sync::Arc::new(NativeAtomicU32::new(0));
        let init_count = count.clone();
        let lazy = LazyLock::new(move || {
            init_count.fetch_add(1, Ordering::SeqCst);
            42
        });

        thread::scope(|scope| {
            for _ in 0..10 {
                scope
                    .spawn(|| {
                        assert_eq!(*lazy, 42);
                        assert_eq!(LazyLock::force(&lazy), &42);
                    })
                    .unwrap();
            }
        });

        assert_eq!(count.load(Ordering::SeqCst), 1);
        assert_eq!(LazyLock::get(&lazy), Some(&42));
    }

    #[test]
    fn test_lazy_lock_force_mut_and_get_mut() {
        let mut lazy = LazyLock::new(|| 92);
        assert_eq!(LazyLock::get(&lazy), None);
        assert_eq!(LazyLock::get_mut(&mut lazy), None);

        assert_eq!(*LazyLock::force_mut(&mut lazy), 92);
        *LazyLock::get_mut(&mut lazy).unwrap() = 44;
        assert_eq!(LazyLock::get(&lazy), Some(&44));

        *lazy = 45;
        assert_eq!(*lazy, 45);
    }

    #[test]
    fn test_lazy_lock_panic_is_unrecoverable() {
        let mut lazy = LazyLock::new(|| -> i32 { panic!("initializer failed") });

        let first = catch_unwind(AssertUnwindSafe(|| LazyLock::force(&lazy)));
        assert!(first.is_err());
        assert_eq!(LazyLock::get(&lazy), None);
        assert_eq!(LazyLock::get_mut(&mut lazy), None);

        let second = catch_unwind(AssertUnwindSafe(|| LazyLock::force(&lazy)));
        assert!(second.is_err());
    }

    #[test]
    fn test_lazy_lock_into_inner_and_from() {
        let lazy = LazyLock::new(|| 42);
        let initializer = LazyLock::into_inner(lazy).unwrap_err();
        assert_eq!(initializer(), 42);

        let lazy = LazyLock::new(|| 92);
        assert_eq!(LazyLock::force(&lazy), &92);
        assert_eq!(LazyLock::into_inner(lazy).ok(), Some(92));

        let lazy: LazyLock<i32> = 123.into();
        assert_eq!(LazyLock::get(&lazy), Some(&123));
        assert_eq!(LazyLock::into_inner(lazy), Ok(123));
    }

    #[test]
    fn test_lazy_lock_default_debug_and_drop() {
        let lazy: LazyLock<Vec<i32>> = Default::default();
        assert!(format!("{lazy:?}").contains("<uninit>"));
        assert_eq!(&*lazy, &[]);
        assert!(format!("{lazy:?}").contains("[]"));

        static DROP_COUNT: NativeAtomicU32 = NativeAtomicU32::new(0);

        struct DropProbe(&'static NativeAtomicU32);

        impl Drop for DropProbe {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }

        DROP_COUNT.store(0, Ordering::SeqCst);
        {
            let probe = DropProbe(&DROP_COUNT);
            let lazy = LazyLock::new(move || {
                let _ = &probe;
                1
            });
            drop(lazy);
        }
        assert_eq!(DROP_COUNT.load(Ordering::SeqCst), 1);

        DROP_COUNT.store(0, Ordering::SeqCst);
        {
            let lazy = LazyLock::new(|| DropProbe(&DROP_COUNT));
            assert!(LazyLock::get(&lazy).is_none());
            let _ = &*lazy;
        }
        assert_eq!(DROP_COUNT.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn test_lazy_lock_into_inner_poison_panics() {
        let lazy = LazyLock::new(|| -> i32 { panic!("initializer failed") });
        let _ = catch_unwind(AssertUnwindSafe(|| LazyLock::force(&lazy)));

        let result = catch_unwind(AssertUnwindSafe(|| LazyLock::into_inner(lazy)));
        assert!(result.is_err());
    }
}

#[cfg(feature = "loom")]
mod loom_tests {
    use core::sync::atomic::Ordering;
    use std::panic::{AssertUnwindSafe, catch_unwind};

    use loom::{
        sync::{Arc, atomic::AtomicU32},
        thread,
    };
    use veloq_std::sync::{LazyLock, LoomLazyLock, NativeLazyLock};

    static STATIC_NATIVE_LAZY: NativeLazyLock<i32> = NativeLazyLock::new(|| 7);

    #[test]
    fn test_loom_type_exports() {
        assert_eq!(NativeLazyLock::get(&STATIC_NATIVE_LAZY), None);
        loom::model(|| {
            let _lazy: LazyLock<i32> = LoomLazyLock::new(|| 9);
        });
    }

    #[test]
    fn test_loom_lazy_lock_concurrency() {
        loom::model(|| {
            let count = Arc::new(AtomicU32::new(0));
            let init_count = count.clone();
            let lazy = Arc::new(LoomLazyLock::new(move || {
                init_count.fetch_add(1, Ordering::SeqCst);
                42
            }));

            let mut handles = Vec::new();
            for _ in 0..2 {
                let lazy = lazy.clone();
                handles.push(thread::spawn(move || {
                    assert_eq!(**lazy, 42);
                }));
            }

            for handle in handles {
                handle.join().unwrap();
            }

            assert_eq!(count.load(Ordering::SeqCst), 1);
            assert_eq!(LoomLazyLock::get(&lazy), Some(&42));
        });
    }

    #[test]
    fn test_loom_lazy_lock_panic_is_unrecoverable() {
        loom::model(|| {
            let mut lazy = LoomLazyLock::new(|| -> i32 { panic!("initializer failed") });
            let first = catch_unwind(AssertUnwindSafe(|| LoomLazyLock::force(&lazy)));
            assert!(first.is_err());
            assert_eq!(LoomLazyLock::get(&lazy), None);
            assert_eq!(LoomLazyLock::get_mut(&mut lazy), None);

            let second = catch_unwind(AssertUnwindSafe(|| LoomLazyLock::force(&lazy)));
            assert!(second.is_err());
        });
    }

    #[test]
    fn test_loom_lazy_lock_from_and_into_inner() {
        loom::model(|| {
            let lazy: LoomLazyLock<i32> = 123.into();
            assert_eq!(LoomLazyLock::get(&lazy), Some(&123));
            assert_eq!(LoomLazyLock::into_inner(lazy), Ok(123));
        });
    }
}
