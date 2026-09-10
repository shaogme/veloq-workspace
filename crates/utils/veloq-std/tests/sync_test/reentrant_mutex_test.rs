#[cfg(not(feature = "loom"))]
mod normal_tests {
    use core::cell::Cell;

    use veloq_std::{
        io::{_eprint, _print, stderr, stdout},
        sync::{Arc, ReentrantMutex},
        thread,
        time::{Duration, Instant},
    };

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
    use loom::{cell::Cell, sync::Arc, thread};
    use veloq_std::sync::{NativeReentrantMutex, ReentrantMutex};

    #[test]
    fn test_native_reentrant_mutex_outside_model() {
        let mutex = NativeReentrantMutex::new(42);
        let guard = mutex.lock();
        let reentrant_guard = mutex.lock();

        assert_eq!(*guard, 42);
        assert_eq!(*reentrant_guard, 42);
        assert_eq!(mutex.reentrancy_count(), 2);
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
