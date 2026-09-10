#[cfg(not(feature = "loom"))]
mod normal_tests {
    use std::{sync::Arc, thread, time::Duration};

    use veloq_std::sync::{
        Mutex, RwLock, RwLockWriteGuard, TryLockError, UnpoisonedMutex, UnpoisonedRwLock,
    };

    #[test]
    fn mutex_poison_carries_a_recoverable_guard() {
        let lock = Arc::new(Mutex::new(0));
        let worker_lock = lock.clone();
        let worker = thread::spawn(move || {
            let mut guard = worker_lock.lock().unwrap();
            *guard = 41;
            panic!("poison mutex");
        });
        assert!(worker.join().is_err());

        assert!(lock.is_poisoned());
        let mut guard = lock.lock().unwrap_err().into_inner();
        assert_eq!(*guard, 41);
        *guard = 42;
        drop(guard);

        assert!(lock.try_lock().is_err());
        lock.clear_poison();
        assert_eq!(*lock.lock().unwrap(), 42);
    }

    #[test]
    fn mutex_try_lock_distinguishes_busy_and_poisoned() {
        let lock = Arc::new(Mutex::new(0));
        let guard = lock.lock().unwrap();
        assert!(matches!(lock.try_lock(), Err(TryLockError::WouldBlock)));
        drop(guard);

        let worker_lock = lock.clone();
        let worker = thread::spawn(move || {
            let _guard = worker_lock.lock().unwrap();
            panic!("poison mutex");
        });
        assert!(worker.join().is_err());
        assert!(matches!(lock.try_lock(), Err(TryLockError::Poisoned(_))));
        assert!(matches!(
            lock.try_lock_for(Duration::ZERO),
            Err(TryLockError::Poisoned(_))
        ));
        assert!(matches!(
            lock.try_lock_until(veloq_std::time::Instant::now()),
            Err(TryLockError::Poisoned(_))
        ));
    }

    #[test]
    fn mutex_get_mut_and_into_inner_observe_poison() {
        let mut lock = Mutex::new(1);
        assert_eq!(*lock.get_mut().unwrap(), 1);

        let shared = Arc::new(lock);
        let worker_lock = shared.clone();
        let worker = thread::spawn(move || {
            let _guard = worker_lock.lock().unwrap();
            panic!("poison mutex");
        });
        assert!(worker.join().is_err());

        let mut lock = Arc::try_unwrap(shared).unwrap();
        let mut error = lock.get_mut().unwrap_err();
        assert_eq!(**error.get_ref(), 1);
        **error.get_mut() = 2;
        let _ = error.into_inner();
        assert_eq!(lock.into_inner().unwrap_err().into_inner(), 2);
    }

    #[test]
    fn rwlock_only_write_guards_poison() {
        let lock = Arc::new(RwLock::new(0));
        let read_lock = lock.clone();
        let read_panic = thread::spawn(move || {
            let _guard = read_lock.read().unwrap();
            panic!("read guard panic");
        });
        assert!(read_panic.join().is_err());
        assert!(!lock.is_poisoned());

        let write_lock = lock.clone();
        let write_panic = thread::spawn(move || {
            let mut guard = write_lock.write().unwrap();
            *guard = 7;
            panic!("write guard panic");
        });
        assert!(write_panic.join().is_err());
        assert!(lock.is_poisoned());
        assert_eq!(*lock.read().unwrap_err().into_inner(), 7);
        lock.clear_poison();
        assert_eq!(*lock.read().unwrap(), 7);
    }

    #[test]
    fn rwlock_downgrade_does_not_poison() {
        let lock = Arc::new(RwLock::new(0));
        let worker_lock = lock.clone();
        let worker = thread::spawn(move || {
            let mut write = worker_lock.write().unwrap();
            *write = 9;
            let _read = RwLockWriteGuard::downgrade(write);
            panic!("downgraded read guard panic");
        });
        assert!(worker.join().is_err());
        assert!(!lock.is_poisoned());
        assert_eq!(*lock.read().unwrap(), 9);
    }

    #[test]
    fn unpoisoned_locks_remain_direct_and_panic_tolerant() {
        let mutex = Arc::new(UnpoisonedMutex::new(0));
        let mutex_worker = mutex.clone();
        assert!(
            thread::spawn(move || {
                let mut guard = mutex_worker.lock();
                *guard = 1;
                panic!("unpoisoned mutex");
            })
            .join()
            .is_err()
        );
        assert_eq!(*mutex.lock(), 1);

        let rwlock = Arc::new(UnpoisonedRwLock::new(0));
        let rwlock_worker = rwlock.clone();
        assert!(
            thread::spawn(move || {
                let mut guard = rwlock_worker.write();
                *guard = 2;
                panic!("unpoisoned rwlock");
            })
            .join()
            .is_err()
        );
        assert_eq!(*rwlock.read(), 2);
    }
}

#[cfg(feature = "loom")]
mod loom_tests {
    use std::panic::{AssertUnwindSafe, catch_unwind};

    use loom::{sync::Arc, thread};

    use veloq_std::sync::{Mutex, RwLock};

    #[test]
    fn mutex_poison_is_visible_after_panic() {
        loom::model(|| {
            let lock = Arc::new(Mutex::new(0));
            let worker_lock = lock.clone();
            let worker = thread::spawn(move || {
                assert!(
                    catch_unwind(AssertUnwindSafe(|| {
                        let mut guard = worker_lock.lock().unwrap();
                        *guard = 1;
                        panic!("poison mutex");
                    }))
                    .is_err()
                );
            });
            worker.join().unwrap();
            assert!(lock.is_poisoned());
            assert_eq!(*lock.lock().unwrap_err().into_inner(), 1);
        });
    }

    #[test]
    fn rwlock_read_panic_does_not_poison() {
        loom::model(|| {
            let lock = Arc::new(RwLock::new(0));
            let worker_lock = lock.clone();
            let worker = thread::spawn(move || {
                assert!(
                    catch_unwind(AssertUnwindSafe(|| {
                        let _guard = worker_lock.read().unwrap();
                        panic!("read guard panic");
                    }))
                    .is_err()
                );
            });
            worker.join().unwrap();
            assert!(!lock.is_poisoned());
            assert_eq!(*lock.read().unwrap(), 0);
        });
    }
}
