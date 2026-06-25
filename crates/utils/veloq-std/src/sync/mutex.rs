use crate::sync::raw_mutex::RawMutex;
use lock_api::RawMutex as RawMutexTrait;

pub type Mutex<T> = lock_api::Mutex<RawMutex, T>;
pub type MutexGuard<'a, T> = lock_api::MutexGuard<'a, RawMutex, T>;

pub const fn const_mutex<T>(val: T) -> Mutex<T> {
    Mutex::const_new(<RawMutex as RawMutexTrait>::INIT, val)
}

#[cfg(test)]
mod tests {
    use crate::sync::mutex::Mutex;
    use crate::thread;
    use alloc::sync::Arc;
    use core::time::Duration;

    #[test]
    fn test_mutex_basic() {
        let m = Mutex::new(0);
        {
            let mut guard = m.lock();
            *guard = 42;
        }
        assert_eq!(*m.lock(), 42);
        assert!(m.try_lock().is_some());
    }

    #[test]
    fn test_mutex_threads() {
        let mutex = Arc::new(Mutex::new(0));
        let num_threads = 4;
        let mut handles = alloc::vec::Vec::new();

        for _ in 0..num_threads {
            let m = mutex.clone();
            let handle = thread::spawn(move || {
                for _ in 0..100 {
                    let mut guard = m.lock();
                    *guard += 1;
                }
            })
            .unwrap();
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }

        assert_eq!(*mutex.lock(), num_threads * 100);
    }

    #[test]
    fn test_mutex_timed() {
        let mutex = Arc::new(Mutex::new(0));
        let m = mutex.clone();
        let guard = mutex.lock();

        let handle = thread::spawn(move || {
            let start = crate::time::Instant::now();
            let res = m.try_lock_for(Duration::from_millis(10));
            assert!(res.is_none()); // Should fail to acquire because guard is held
            assert!(start.elapsed() >= Duration::from_millis(10));
        })
        .unwrap();

        handle.join().unwrap();
        drop(guard);
    }
}
