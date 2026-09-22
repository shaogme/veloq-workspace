use std::{cell::Cell, future::pending, pin::pin, rc::Rc, time::Duration};
use tokio::{
    task::{LocalSet, yield_now},
    time::sleep,
};
use veloq_local::RwLock;

#[tokio::test]
async fn test_rwlock_simple() {
    let lock = RwLock::new(0);
    assert!(!lock.is_locked());
    assert_eq!(lock.read_count(), 0);

    {
        let mut guard = lock.write().await;
        assert!(lock.is_write_locked());
        assert!(lock.is_locked());
        *guard += 1;
    }

    assert!(!lock.is_write_locked());
    assert!(!lock.is_locked());

    {
        let guard = lock.read().await;
        assert_eq!(*guard, 1);
        assert_eq!(lock.read_count(), 1);
        assert!(!lock.is_write_locked());
    }

    assert_eq!(lock.read_count(), 0);
}

#[tokio::test]
async fn test_rwlock_try_read_and_try_write() {
    let lock = RwLock::new(100);

    let r_guard = lock.try_read().unwrap();
    assert_eq!(*r_guard, 100);
    assert_eq!(lock.read_count(), 1);

    // Another reader can acquire
    let r_guard2 = lock.try_read().unwrap();
    assert_eq!(*r_guard2, 100);
    assert_eq!(lock.read_count(), 2);

    // Writer cannot acquire while readers exist
    assert!(lock.try_write().is_none());

    drop(r_guard);
    drop(r_guard2);

    // Now writer can acquire
    let mut w_guard = lock.try_write().unwrap();
    *w_guard = 200;
    assert!(lock.is_write_locked());

    // Neither reader nor writer can acquire while write lock is held
    assert!(lock.try_read().is_none());
    assert!(lock.try_write().is_none());

    drop(w_guard);
    assert_eq!(*lock.try_read().unwrap(), 200);
}

#[tokio::test]
async fn test_rwlock_concurrent_readers() {
    let local = LocalSet::new();

    local
        .run_until(async {
            let lock = Rc::new(RwLock::new(42));
            let active_readers = Rc::new(Cell::new(0));
            let max_readers = Rc::new(Cell::new(0));

            let mut handles = Vec::new();
            for _ in 0..5 {
                let l = lock.clone();
                let a = active_readers.clone();
                let m = max_readers.clone();
                handles.push(tokio::task::spawn_local(async move {
                    let guard = l.read().await;
                    let current = a.get() + 1;
                    a.set(current);
                    if current > m.get() {
                        m.set(current);
                    }
                    yield_now().await;
                    assert_eq!(*guard, 42);
                    a.set(a.get() - 1);
                }));
            }

            for handle in handles {
                handle.await.unwrap();
            }

            // All 5 readers ran concurrently
            assert_eq!(max_readers.get(), 5);
            assert_eq!(lock.read_count(), 0);
        })
        .await;
}

#[tokio::test]
async fn test_rwlock_contention() {
    let local = LocalSet::new();

    local
        .run_until(async {
            let lock = Rc::new(RwLock::new(0));
            let mut handles = Vec::new();

            // 5 writers
            for _ in 0..5 {
                let l = lock.clone();
                handles.push(tokio::task::spawn_local(async move {
                    for _ in 0..20 {
                        let mut guard = l.write().await;
                        let val = *guard;
                        yield_now().await;
                        *guard = val + 1;
                    }
                }));
            }

            // 5 readers
            for _ in 0..5 {
                let l = lock.clone();
                handles.push(tokio::task::spawn_local(async move {
                    for _ in 0..20 {
                        let guard = l.read().await;
                        assert!(*guard >= 0);
                        yield_now().await;
                    }
                }));
            }

            for handle in handles {
                handle.await.unwrap();
            }

            assert_eq!(*lock.read().await, 100);
        })
        .await;
}

#[tokio::test]
async fn test_rwlock_downgrade() {
    let local = LocalSet::new();

    local
        .run_until(async {
            let lock = Rc::new(RwLock::new(0));
            let mut w_guard = lock.write().await;
            *w_guard = 42;

            let l_clone = lock.clone();
            let reader_handle = tokio::task::spawn_local(async move {
                let guard = l_clone.read().await;
                *guard
            });

            sleep(Duration::from_millis(20)).await;

            // Downgrade write guard to read guard
            let r_guard = w_guard.downgrade();
            assert_eq!(*r_guard, 42);
            assert_eq!(lock.read_count(), 2);

            let val = reader_handle.await.unwrap();
            assert_eq!(val, 42);

            drop(r_guard);
            assert_eq!(lock.read_count(), 0);
        })
        .await;
}

#[tokio::test]
async fn test_rwlock_writer_fairness() {
    let local = LocalSet::new();

    local
        .run_until(async {
            let lock = Rc::new(RwLock::new(0));
            let order = Rc::new(Cell::new(Vec::new()));

            // 1. Initial reader holds the lock
            let r_guard = lock.read().await;

            // 2. Writer tries to acquire and queues
            let l_w = lock.clone();
            let o_w = order.clone();
            let writer_handle = tokio::task::spawn_local(async move {
                let mut guard = l_w.write().await;
                *guard = 1;
                let mut list = o_w.take();
                list.push("writer");
                o_w.set(list);
            });

            sleep(Duration::from_millis(10)).await;

            // 3. Second reader tries to acquire - should be queued behind writer to prevent starvation
            let l_r = lock.clone();
            let o_r = order.clone();
            let reader_handle = tokio::task::spawn_local(async move {
                let guard = l_r.read().await;
                assert_eq!(*guard, 1);
                let mut list = o_r.take();
                list.push("reader2");
                o_r.set(list);
            });

            sleep(Duration::from_millis(10)).await;

            // 4. Drop initial read guard
            drop(r_guard);

            writer_handle.await.unwrap();
            reader_handle.await.unwrap();

            assert_eq!(order.take(), vec!["writer", "reader2"]);
        })
        .await;
}

#[tokio::test]
async fn test_rwlock_cancellation_waiting_reader() {
    let local = LocalSet::new();

    local
        .run_until(async {
            let lock = Rc::new(RwLock::new(0));
            let w_guard = lock.write().await;

            let l1 = lock.clone();
            let t1 = tokio::task::spawn_local(async move {
                let _g = l1.read().await;
                pending::<()>().await;
            });

            let l2 = lock.clone();
            let t2 = tokio::task::spawn_local(async move {
                let g = l2.read().await;
                *g
            });

            sleep(Duration::from_millis(20)).await;

            // Cancel t1 while waiting in the queue
            t1.abort();
            let _ = t1.await;

            drop(w_guard);

            let val = t2.await.unwrap();
            assert_eq!(val, 0);
        })
        .await;
}

#[tokio::test]
async fn test_rwlock_cancellation_waiting_writer() {
    let local = LocalSet::new();

    local
        .run_until(async {
            let lock = Rc::new(RwLock::new(0));
            let r_guard = lock.read().await;

            let l1 = lock.clone();
            let t1 = tokio::task::spawn_local(async move {
                let mut _g = l1.write().await;
                pending::<()>().await;
            });

            let l2 = lock.clone();
            let t2 = tokio::task::spawn_local(async move {
                let g = l2.read().await;
                *g
            });

            sleep(Duration::from_millis(20)).await;

            // Cancel waiting writer t1; t2 should be unblocked even while r_guard is held!
            t1.abort();
            let _ = t1.await;

            sleep(Duration::from_millis(10)).await;

            // t2 should now be unblocked because the blocking writer was canceled
            let val = t2.await.unwrap();
            assert_eq!(val, 0);

            drop(r_guard);
        })
        .await;
}

#[tokio::test]
async fn test_rwlock_cancellation_granted_reader() {
    let local = LocalSet::new();

    local
        .run_until(async {
            let lock = Rc::new(RwLock::new(10));
            let w_guard = lock.write().await;

            let l1 = lock.clone();
            let t1 = tokio::task::spawn_local(async move {
                let fut = l1.read();
                let mut pinned = pin!(fut);
                let _ = tokio::time::timeout(Duration::from_millis(5), &mut pinned).await;
                pending::<()>().await;
            });

            let l2 = lock.clone();
            let t2 = tokio::task::spawn_local(async move {
                let mut g = l2.write().await;
                *g = 20;
            });

            sleep(Duration::from_millis(20)).await;

            // Releasing write guard grants the read lock to t1
            drop(w_guard);

            // Abort t1 before it consumes the read lock
            t1.abort();
            let _ = t1.await;

            // t2 should now acquire write lock
            t2.await.unwrap();
            assert_eq!(*lock.read().await, 20);
        })
        .await;
}

#[tokio::test]
async fn test_rwlock_cancellation_granted_writer() {
    let local = LocalSet::new();

    local
        .run_until(async {
            let lock = Rc::new(RwLock::new(10));
            let r_guard = lock.read().await;

            let l1 = lock.clone();
            let t1 = tokio::task::spawn_local(async move {
                let fut = l1.write();
                let mut pinned = pin!(fut);
                let _ = tokio::time::timeout(Duration::from_millis(5), &mut pinned).await;
                pending::<()>().await;
            });

            let l2 = lock.clone();
            let t2 = tokio::task::spawn_local(async move {
                let g = l2.read().await;
                *g
            });

            sleep(Duration::from_millis(20)).await;

            // Release r_guard grants write lock to t1
            drop(r_guard);

            // Abort t1 before it consumes the write lock
            t1.abort();
            let _ = t1.await;

            // t2 should now acquire read lock
            let val = t2.await.unwrap();
            assert_eq!(val, 10);
        })
        .await;
}

#[tokio::test]
async fn test_rwlock_get_mut_and_into_inner() {
    let mut lock = RwLock::new(5);
    *lock.get_mut() += 10;
    assert_eq!(lock.into_inner(), 15);

    let default_lock: RwLock<String> = RwLock::default();
    assert_eq!(default_lock.into_inner(), String::new());
}
