#[path = "sync_test/arc_test.rs"]
mod arc_test;
#[path = "sync_test/barrier_test.rs"]
mod barrier_test;
#[path = "sync_test/condvar_test.rs"]
mod condvar_test;
#[path = "sync_test/lazy_lock_test.rs"]
mod lazy_lock_test;
#[path = "sync_test/mpsc_test.rs"]
mod mpsc_test;
#[path = "sync_test/mutex_test.rs"]
mod mutex_test;
#[path = "sync_test/once_lock_test.rs"]
mod once_lock_test;
#[path = "sync_test/once_test.rs"]
mod once_test;
#[path = "sync_test/poison_test.rs"]
mod poison_test;
#[path = "sync_test/reentrant_mutex_test.rs"]
mod reentrant_mutex_test;
#[path = "sync_test/rwlock_test.rs"]
mod rwlock_test;
#[path = "sync_test/weak_test.rs"]
mod weak_test;
mod spin_lock_test {
    #[cfg(not(feature = "loom"))]
    mod native_tests {
        use veloq_std::{
            sync::{Arc, NativeSpinLock, SpinLock},
            thread::ThreadApi,
        };

        #[test]
        fn native_spin_lock_protects_mutation_and_reports_state() {
            let lock = NativeSpinLock::new(0);
            assert!(!lock.is_locked());
            {
                let mut guard = lock.lock();
                assert!(lock.is_locked());
                assert_eq!(guard.with(|value| *value), 0);
                guard.with_mut(|value| *value = 42);
            }
            assert!(!lock.is_locked());
            assert_eq!(lock.lock().with(|value| *value), 42);

            let lock = SpinLock::new(7);
            assert_eq!(lock.lock().with(|value| *value), 7);
        }

        #[test]
        fn native_spin_lock_serializes_threads() {
            const THREADS: usize = 4;
            const ITERATIONS: usize = 100;

            let lock = Arc::new(SpinLock::new(0));
            let mut handles = Vec::new();
            for _ in 0..THREADS {
                let lock = lock.clone();
                handles.push(
                    ThreadApi::spawn(move || {
                        for _ in 0..ITERATIONS {
                            let mut guard = lock.lock();
                            guard.with_mut(|value| *value += 1);
                        }
                    })
                    .unwrap(),
                );
            }

            for handle in handles {
                handle.join().unwrap();
            }

            assert_eq!(lock.lock().with(|value| *value), THREADS * ITERATIONS);
        }

        #[test]
        fn native_spin_lock_try_lock_and_debug_are_available() {
            let lock = NativeSpinLock::new(11);
            let guard = lock.try_lock().unwrap();
            assert!(lock.try_lock().is_none());
            assert!(format!("{lock:?}").contains("<locked>"));
            drop(guard);
            assert!(format!("{lock:?}").contains("11"));
        }
    }

    #[cfg(feature = "loom")]
    mod loom_tests {
        use loom::{sync::Arc, thread};
        use veloq_std::sync::{LoomSpinLock, SpinLock};

        #[test]
        fn loom_spin_lock_replacement_serializes_access() {
            loom::model(|| {
                let lock = Arc::new(SpinLock::new(0));
                let lock_for_thread = lock.clone();
                let handle = thread::spawn(move || {
                    let mut guard = lock_for_thread.lock();
                    guard.with_mut(|value| *value += 1);
                });

                {
                    let mut guard = lock.lock();
                    guard.with_mut(|value| *value += 1);
                }

                handle.join().unwrap();
                assert_eq!(lock.lock().with(|value| *value), 2);
            });
        }

        #[test]
        fn loom_spin_lock_exposes_symmetric_api() {
            loom::model(|| {
                let lock = LoomSpinLock::new(3);
                assert!(!lock.is_locked());
                let guard = lock.try_lock().unwrap();
                assert!(lock.is_locked());
                assert_eq!(guard.with(|value| *value), 3);
                drop(guard);
                assert!(!lock.is_locked());
                assert!(format!("{lock:?}").contains("3"));
            });
        }
    }
}
