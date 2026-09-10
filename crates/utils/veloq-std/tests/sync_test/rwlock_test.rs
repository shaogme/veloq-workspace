#[cfg(feature = "loom")]
mod loom_tests {
    use loom::{sync::Arc, thread};
    use veloq_std::sync::{RawRwLock, UnpoisonedRwLock, UnpoisonedRwLockWriteGuard};

    #[test]
    fn test_loom_rwlock_read_write() {
        loom::model(|| {
            let lock = Arc::new(UnpoisonedRwLock::new(0));
            let l1 = lock.clone();
            let l2 = lock.clone();

            let h1 = thread::spawn(move || {
                let r = l1.read();
                assert!(*r == 0 || *r == 1);
            });

            let h2 = thread::spawn(move || {
                let mut w = l2.write();
                *w = 1;
            });

            h1.join().unwrap();
            h2.join().unwrap();
            assert_eq!(*lock.read(), 1);
        });
    }

    #[test]
    fn test_loom_rwlock_downgrade() {
        loom::model(|| {
            let lock = Arc::new(UnpoisonedRwLock::new(0));
            let l1 = lock.clone();

            let h = thread::spawn(move || {
                let mut w = l1.write();
                *w = 42;
                let r = UnpoisonedRwLockWriteGuard::downgrade(w);
                assert_eq!(*r, 42);
            });

            let r = lock.read();
            assert!(*r == 0 || *r == 42);
            drop(r);

            h.join().unwrap();
            assert_eq!(*lock.read(), 42);
        });
    }

    #[test]
    fn test_loom_raw_rwlock_direct() {
        loom::model(|| {
            let lock = Arc::new(RawRwLock::new());
            let l1 = lock.clone();

            let h = thread::spawn(move || {
                l1.lock_exclusive();
                unsafe { l1.downgrade() };
                unsafe { l1.unlock_shared() };
            });

            lock.lock_shared();
            unsafe { lock.unlock_shared() };

            h.join().unwrap();
            assert!(!lock.is_locked());
        });
    }
}
