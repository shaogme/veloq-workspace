#[cfg(feature = "loom")]
mod loom_tests {
    use loom::{sync::Arc, thread};
    use veloq_std::sync::{RawMutex, UnpoisonedMutex};

    #[test]
    fn test_loom_mutex_simple() {
        loom::model(|| {
            let lock = Arc::new(UnpoisonedMutex::new(0));
            let lock2 = lock.clone();
            let h = thread::spawn(move || {
                let mut g = lock2.lock();
                *g += 1;
            });
            {
                let mut g = lock.lock();
                *g += 1;
            }
            h.join().unwrap();
            assert_eq!(*lock.lock(), 2);
        });
    }

    #[test]
    fn test_loom_raw_mutex_concurrency() {
        loom::model(|| {
            let lock = Arc::new(RawMutex::new());
            let l2 = lock.clone();

            let h = thread::spawn(move || {
                l2.lock();
                unsafe { l2.unlock() };
            });

            lock.lock();
            unsafe { lock.unlock() };

            h.join().unwrap();
            assert!(!lock.is_locked());
        });
    }
}
