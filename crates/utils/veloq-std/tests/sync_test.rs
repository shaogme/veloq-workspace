#[cfg(not(feature = "loom"))]
mod normal_tests {
    use core::cell::Cell;
    use std::{panic, sync::mpsc::channel, vec::Vec};

    use veloq_std::{
        io::{_eprint, _print, stderr, stdout},
        sync::{
            Arc, Condvar, Mutex, Once, OnceLock, ReentrantMutex,
            atomic::{CoreAtomicU32, Ordering},
        },
        thread,
        time::{Duration, Instant},
    };

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

    #[test]
    fn test_reentrant_mutex_basic() {
        let m = ReentrantMutex::new(42);
        assert!(!m.is_locked());
        assert_eq!(m.reentrancy_count(), 0);

        {
            let g1 = m.lock();
            assert_eq!(*g1, 42);
            assert!(m.is_locked());
            assert!(m.is_owned_by_current_thread());
            assert_eq!(m.reentrancy_count(), 1);

            {
                let g2 = m.lock();
                assert_eq!(*g2, 42);
                assert_eq!(m.reentrancy_count(), 2);

                {
                    let g3 = m.lock();
                    assert_eq!(*g3, 42);
                    assert_eq!(m.reentrancy_count(), 3);
                }
                assert_eq!(m.reentrancy_count(), 2);
                assert!(m.is_locked());
            }
            assert_eq!(m.reentrancy_count(), 1);
            assert!(m.is_locked());
        }

        assert_eq!(m.reentrancy_count(), 0);
        assert!(!m.is_locked());
    }

    #[test]
    fn test_reentrant_mutex_depth() {
        let m = ReentrantMutex::new(100);
        let mut guards = Vec::new();
        for i in 1..=15 {
            guards.push(m.lock());
            assert_eq!(m.reentrancy_count(), i);
            assert!(m.is_locked());
        }
        for expected in (0..15).rev() {
            guards.pop();
            assert_eq!(m.reentrancy_count(), expected);
            if expected > 0 {
                assert!(m.is_locked());
            }
        }
        assert!(!m.is_locked());
    }

    #[test]
    fn test_reentrant_mutex_threads() {
        let m = Arc::new(ReentrantMutex::new(Cell::new(0)));
        let num_threads = 4;
        let iters = 100;
        let mut handles = Vec::new();

        for _ in 0..num_threads {
            let m = m.clone();
            let h = thread::spawn(move || {
                for _ in 0..iters {
                    let g1 = m.lock();
                    let g2 = m.lock();
                    let val = g1.get();
                    g2.set(val + 1);
                }
            })
            .unwrap();
            handles.push(h);
        }

        for h in handles {
            h.join().unwrap();
        }

        assert_eq!(m.lock().get(), num_threads * iters);
    }

    #[test]
    fn test_reentrant_mutex_try_lock() {
        let m = Arc::new(ReentrantMutex::new(10));
        let g1 = m.try_lock();
        assert!(g1.is_some());
        let g1 = g1.unwrap();

        // 同一线程多次 try_lock 成功重入
        let g2 = m.try_lock();
        assert!(g2.is_some());
        drop(g2);

        // 异线程 try_lock 互斥排他失败
        let m_clone = m.clone();
        let handle = thread::spawn(move || {
            let res = m_clone.try_lock();
            assert!(res.is_none());
        })
        .unwrap();
        handle.join().unwrap();

        drop(g1);

        // 释放后异线程成功获取
        let m_clone = m.clone();
        let handle = thread::spawn(move || {
            let res = m_clone.try_lock();
            assert!(res.is_some());
        })
        .unwrap();
        handle.join().unwrap();
    }

    #[test]
    fn test_reentrant_mutex_timed() {
        let m = Arc::new(ReentrantMutex::new(0));
        let g = m.lock();

        // 同一线程超时尝试成功
        let reentered = m.try_lock_for(Duration::from_millis(10));
        assert!(reentered.is_some());
        drop(reentered);

        // 异线程超时返回 None
        let m_clone = m.clone();
        let handle = thread::spawn(move || {
            let start = Instant::now();
            let res = m_clone.try_lock_for(Duration::from_millis(50));
            assert!(res.is_none());
            assert!(start.elapsed() >= Duration::from_millis(40));
        })
        .unwrap();
        handle.join().unwrap();
        drop(g);
    }

    #[test]
    fn test_reentrant_mutex_stdio_reentrant() {
        // 嵌套获取 stdout lock 并输出
        {
            let lock1 = stdout().lock();
            let lock2 = stdout().lock();
            _print(format_args!("testing stdout reentrancy\n"));
            drop(lock2);
            drop(lock1);
        }

        // 嵌套获取 stderr lock 并输出
        {
            let lock1 = stderr().lock();
            let lock2 = stderr().lock();
            _eprint(format_args!("testing stderr reentrancy\n"));
            drop(lock2);
            drop(lock1);
        }
    }

    #[test]
    fn test_reentrant_mutex_api() {
        let mut m = ReentrantMutex::new(5);
        *m.get_mut() = 10;
        assert_eq!(m.into_inner(), 10);

        let m2 = ReentrantMutex::new(20);
        let debug_str = format!("{m2:?}");
        assert!(debug_str.contains("20"));

        let _guard = m2.lock();
        let debug_locked = format!("{m2:?}");
        assert!(debug_locked.contains("20"));
    }
}

#[cfg(feature = "loom")]
mod loom_tests {
    use loom::{
        cell::Cell,
        sync::atomic::{AtomicUsize, Ordering},
        thread,
    };
    use veloq_std::sync::{
        Arc, Condvar, Mutex, RawMutex, RawRwLock, ReentrantMutex, UnpoisonedMutex,
        UnpoisonedRwLock, UnpoisonedRwLockWriteGuard,
    };

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

    #[test]
    fn test_loom_reentrant_mutex_basic() {
        loom::model(|| {
            let m = ReentrantMutex::new(42);
            assert!(!m.is_locked());
            assert_eq!(m.reentrancy_count(), 0);

            {
                let g1 = m.lock();
                assert_eq!(*g1, 42);
                assert!(m.is_locked());
                assert!(m.is_owned_by_current_thread());
                assert_eq!(m.reentrancy_count(), 1);

                {
                    let g2 = m.lock();
                    assert_eq!(*g2, 42);
                    assert_eq!(m.reentrancy_count(), 2);
                }

                assert_eq!(m.reentrancy_count(), 1);
                assert!(m.is_locked());
            }

            assert_eq!(m.reentrancy_count(), 0);
            assert!(!m.is_locked());
        });
    }

    #[test]
    fn test_loom_reentrant_mutex_concurrency() {
        loom::model(|| {
            let m = Arc::new(ReentrantMutex::new(Cell::new(0)));
            let m2 = m.clone();

            let h = thread::spawn(move || {
                let g1 = m2.lock();
                let g2 = m2.lock();
                let val = g1.get();
                g2.set(val + 1);
            });

            {
                let g1 = m.lock();
                let g2 = m.lock();
                let val = g1.get();
                g2.set(val + 1);
            }

            h.join().unwrap();

            assert_eq!(m.lock().get(), 2);
        });
    }

    #[test]
    fn test_loom_reentrant_mutex_nested_contention() {
        loom::model(|| {
            let m = Arc::new(ReentrantMutex::new(Cell::new(0)));
            let m2 = m.clone();

            let h = thread::spawn(move || {
                let g1 = m2.lock();
                let g2 = m2.lock();
                let g3 = m2.lock();
                g3.set(g3.get() + 1);
                drop(g3);
                drop(g2);
                drop(g1);
            });

            {
                let g = m.lock();
                g.set(g.get() + 10);
            }

            h.join().unwrap();
            let final_val = m.lock().get();
            assert_eq!(final_val, 11);
        });
    }

    #[test]
    fn test_loom_reentrant_mutex_try_lock() {
        loom::model(|| {
            let m = Arc::new(ReentrantMutex::new(10));
            let m2 = m.clone();

            let h = thread::spawn(move || {
                if let Some(g1) = m2.try_lock() {
                    assert!(m2.try_lock().is_some());
                    assert_eq!(*g1, 10);
                }
            });

            if let Some(g1) = m.try_lock() {
                assert!(m.try_lock().is_some());
                assert_eq!(*g1, 10);
            }

            h.join().unwrap();
        });
    }
}
