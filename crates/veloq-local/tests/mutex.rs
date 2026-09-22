use std::{cell::Cell, future::pending, pin::pin, rc::Rc, time::Duration};
use tokio::{
    task::{LocalSet, yield_now},
    time::sleep,
};
use veloq_local::Mutex;

#[tokio::test]
async fn test_mutex_simple() {
    let mutex = Mutex::new(10);
    assert!(!mutex.is_locked());
    {
        let mut guard = mutex.lock().await;
        assert!(mutex.is_locked());
        *guard += 1;
    }
    assert!(!mutex.is_locked());
    assert_eq!(*mutex.lock().await, 11);
}

#[tokio::test]
async fn test_mutex_try_lock() {
    let mutex = Mutex::new(42);
    let guard = mutex.try_lock();
    assert!(guard.is_some());
    assert!(mutex.is_locked());

    let guard2 = mutex.try_lock();
    assert!(guard2.is_none());

    drop(guard);
    assert!(!mutex.is_locked());

    let guard3 = mutex.try_lock();
    assert!(guard3.is_some());
}

#[tokio::test]
async fn test_mutex_contention() {
    let local = LocalSet::new();

    local
        .run_until(async {
            let mutex = Rc::new(Mutex::new(0));
            let mut handles = Vec::new();

            for _ in 0..10 {
                let m = mutex.clone();
                handles.push(tokio::task::spawn_local(async move {
                    for _ in 0..50 {
                        let mut guard = m.lock().await;
                        let val = *guard;
                        yield_now().await;
                        *guard = val + 1;
                    }
                }));
            }

            for handle in handles {
                handle.await.unwrap();
            }

            assert_eq!(*mutex.lock().await, 500);
        })
        .await;
}

#[tokio::test]
async fn test_mutex_fifo_order() {
    let local = LocalSet::new();

    local
        .run_until(async {
            let mutex = Rc::new(Mutex::new(()));
            let order = Rc::new(Cell::new(Vec::new()));

            let guard = mutex.lock().await;

            let mut handles = Vec::new();
            for i in 1..=3 {
                let m = mutex.clone();
                let o = order.clone();
                handles.push(tokio::task::spawn_local(async move {
                    let _g = m.lock().await;
                    let mut list = o.take();
                    list.push(i);
                    o.set(list);
                }));
                sleep(Duration::from_millis(10)).await;
            }

            drop(guard);

            for handle in handles {
                handle.await.unwrap();
            }

            assert_eq!(order.take(), vec![1, 2, 3]);
        })
        .await;
}

#[tokio::test]
async fn test_mutex_cancellation_waiting() {
    let local = LocalSet::new();

    local
        .run_until(async {
            let mutex = Rc::new(Mutex::new(0));
            let guard = mutex.lock().await;

            let m1 = mutex.clone();
            let t1 = tokio::task::spawn_local(async move {
                let _g = m1.lock().await;
                pending::<()>().await;
            });

            let m2 = mutex.clone();
            let t2 = tokio::task::spawn_local(async move {
                let mut g = m2.lock().await;
                *g = 42;
            });

            sleep(Duration::from_millis(20)).await;

            // t1 is waiting in the queue, cancel it
            t1.abort();
            let _ = t1.await;

            // Now release the initial guard
            drop(guard);

            // t2 should still acquire the lock successfully
            t2.await.unwrap();
            assert_eq!(*mutex.lock().await, 42);
        })
        .await;
}

#[tokio::test]
async fn test_mutex_cancellation_granted() {
    let local = LocalSet::new();

    local
        .run_until(async {
            let mutex = Rc::new(Mutex::new(0));
            let m1 = mutex.clone();
            let m2 = mutex.clone();

            let guard = mutex.lock().await;

            // t1 waits for the lock
            let t1 = tokio::task::spawn_local(async move {
                let lock_fut = m1.lock();
                let mut pinned = pin!(lock_fut);
                // First poll to register in waiter list
                let _ = tokio::time::timeout(Duration::from_millis(5), &mut pinned).await;
                // Wait until aborted
                pending::<()>().await;
            });

            // t2 also waits for the lock
            let t2 = tokio::task::spawn_local(async move {
                let mut g = m2.lock().await;
                *g = 99;
            });

            sleep(Duration::from_millis(20)).await;

            // Releasing the guard grants the lock to t1
            drop(guard);

            // Now cancel t1 before t1 completes consuming the lock
            t1.abort();
            let _ = t1.await;

            // The lock should be transferred to t2
            t2.await.unwrap();
            assert_eq!(*mutex.lock().await, 99);
        })
        .await;
}

#[tokio::test]
async fn test_mutex_get_mut_and_into_inner() {
    let mut mutex = Mutex::new(10);
    *mutex.get_mut() += 5;
    assert_eq!(mutex.into_inner(), 15);

    let default_mutex: Mutex<i32> = Mutex::default();
    assert_eq!(default_mutex.into_inner(), 0);
}
