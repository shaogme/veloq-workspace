#[cfg(not(feature = "loom"))]
mod normal_tests {
    use std::sync::mpsc::channel;
    use std::vec::Vec;

    use veloq_std::{
        sync::{Arc, Condvar, Mutex},
        thread,
        time::Duration,
    };

    #[test]
    fn test_condvar_basic() {
        let pair = Arc::new((Mutex::new(false), Condvar::new()));
        let pair2 = pair.clone();

        thread::spawn(move || {
            let (lock, cvar) = &*pair2;
            let mut started = lock.lock().unwrap();
            *started = true;
            cvar.notify_one();
        })
        .unwrap()
        .join()
        .unwrap();

        let (lock, cvar) = &*pair;
        let mut started = lock.lock().unwrap();
        while !*started {
            started = cvar.wait(started).unwrap();
        }
        assert!(*started);
    }

    #[test]
    fn test_condvar_notify_one_does_not_leak_to_future_waiter() {
        let pair = Arc::new((Mutex::new(()), Condvar::new()));
        pair.1.notify_one();
        let (ready_tx, ready_rx) = channel();
        let (done_tx, done_rx) = channel();
        let pair2 = pair.clone();

        let handle = thread::spawn(move || {
            let (lock, cvar) = &*pair2;
            let guard = lock.lock().unwrap();
            ready_tx.send(()).unwrap();
            let _guard = cvar.wait(guard).unwrap();
            done_tx.send(()).unwrap();
        })
        .unwrap();

        ready_rx.recv().unwrap();
        let (lock, cvar) = &*pair;
        let guard = lock.lock().unwrap();
        assert!(done_rx.try_recv().is_err());
        cvar.notify_one();
        drop(guard);

        done_rx.recv().unwrap();
        handle.join().unwrap();
    }

    #[test]
    fn test_condvar_timeout() {
        let pair = Arc::new((Mutex::new(false), Condvar::new()));
        let pair2 = pair.clone();

        let handle = thread::spawn(move || {
            let (lock, cvar) = &*pair2;
            let mut started = lock.lock().unwrap();
            *started = true;
            cvar.notify_one();
        })
        .unwrap();

        let (lock, cvar) = &*pair;
        let mut started = lock.lock().unwrap();
        while !*started {
            let (g, res) = cvar.wait_timeout(started, Duration::from_millis(100));
            started = g.unwrap();
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
        let started = lock.lock().unwrap();
        let (g, res) = cvar.wait_timeout(started, Duration::from_millis(10));
        drop(g.unwrap());
        assert!(res.timed_out());
    }

    #[test]
    fn test_condvar_timeout_removes_waiter_before_notify() {
        let pair = Arc::new((Mutex::new(()), Condvar::new()));
        let (ready_tx, ready_rx) = channel();
        let (done_tx, done_rx) = channel();
        let pair2 = pair.clone();

        let handle = thread::spawn(move || {
            let (lock, cvar) = &*pair2;
            let guard = lock.lock().unwrap();
            ready_tx.send(()).unwrap();
            let (guard, result) = cvar.wait_timeout(guard, Duration::from_millis(5));
            drop(guard.unwrap());
            done_tx.send(result.timed_out()).unwrap();
        })
        .unwrap();

        ready_rx.recv().unwrap();
        let (lock, cvar) = &*pair;
        let guard = lock.lock().unwrap();
        drop(guard);
        assert!(done_rx.recv().unwrap());
        cvar.notify_one();
        handle.join().unwrap();
    }

    #[test]
    fn test_condvar_notify_all() {
        let pair = Arc::new((Mutex::new(0), Condvar::new()));
        let (ready_tx, ready_rx) = channel();
        let mut handles = Vec::new();

        for _ in 0..3 {
            let pair_clone = pair.clone();
            let ready_tx = ready_tx.clone();
            let handle = thread::spawn(move || {
                let (lock, cvar) = &*pair_clone;
                let mut count = lock.lock().unwrap();
                ready_tx.send(()).unwrap();
                while *count == 0 {
                    count = cvar.wait(count).unwrap();
                }
                *count += 1;
            })
            .unwrap();
            handles.push(handle);
        }

        for _ in 0..3 {
            ready_rx.recv().unwrap();
        }

        let (lock, cvar) = &*pair;
        {
            let mut count = lock.lock().unwrap();
            *count = 1;
            cvar.notify_all();
        }

        for handle in handles {
            handle.join().unwrap();
        }

        assert_eq!(*lock.lock().unwrap(), 4);
    }
}

#[cfg(feature = "loom")]
mod loom_tests {
    use loom::{sync::Arc, thread};
    use veloq_std::sync::{Condvar, Mutex};

    #[test]
    fn test_loom_condvar() {
        loom::model(|| {
            let pair = Arc::new((Mutex::new(false), Condvar::new()));
            let pair2 = pair.clone();

            let handle = thread::spawn(move || {
                let (lock, cvar) = &*pair2;
                let mut started = lock.lock().unwrap();
                *started = true;
                cvar.notify_one();
            });

            let (lock, cvar) = &*pair;
            let mut started = lock.lock().unwrap();
            while !*started {
                started = cvar.wait(started).unwrap();
            }
            assert!(*started);
            handle.join().unwrap();
        });
    }

    #[test]
    fn test_loom_condvar_notify_one_selects_one_waiter() {
        loom::model(|| {
            let pair = Arc::new((Mutex::new(()), Condvar::new()));
            let registered = Arc::new(AtomicUsize::new(0));
            let returned = Arc::new(AtomicUsize::new(0));

            let pair1 = pair.clone();
            let registered1 = registered.clone();
            let returned1 = returned.clone();
            let first = thread::spawn(move || {
                let (lock, cvar) = &*pair1;
                let guard = lock.lock().unwrap();
                registered1.fetch_add(1, Ordering::Release);
                let guard = cvar.wait(guard).unwrap();
                returned1.fetch_add(1, Ordering::Release);
                drop(guard);
            });

            let pair2 = pair.clone();
            let registered2 = registered.clone();
            let returned2 = returned.clone();
            let second = thread::spawn(move || {
                let (lock, cvar) = &*pair2;
                let guard = lock.lock().unwrap();
                registered2.fetch_add(1, Ordering::Release);
                let guard = cvar.wait(guard).unwrap();
                returned2.fetch_add(1, Ordering::Release);
                drop(guard);
            });

            while registered.load(Ordering::Acquire) != 2 {
                thread::yield_now();
            }

            let (lock, cvar) = &*pair;
            let guard = lock.lock().unwrap();
            cvar.notify_one();
            drop(guard);

            while returned.load(Ordering::Acquire) == 0 {
                thread::yield_now();
            }
            assert_eq!(returned.load(Ordering::Acquire), 1);

            let guard = lock.lock().unwrap();
            cvar.notify_one();
            drop(guard);
            first.join().unwrap();
            second.join().unwrap();
            assert_eq!(returned.load(Ordering::Acquire), 2);
        });
    }

    #[test]
    fn test_loom_condvar_notify_all() {
        loom::model(|| {
            let pair = Arc::new((Mutex::new(false), Condvar::new()));
            let pair2 = pair.clone();

            let handle = thread::spawn(move || {
                let (lock, cvar) = &*pair2;
                let mut started = lock.lock().unwrap();
                *started = true;
                cvar.notify_all();
            });

            let (lock, cvar) = &*pair;
            let mut started = lock.lock().unwrap();
            while !*started {
                started = cvar.wait(started).unwrap();
            }
            assert!(*started);
            handle.join().unwrap();
        });
    }
}
