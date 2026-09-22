#![cfg(not(feature = "loom"))]
use veloq_std::sync::Arc;
use veloq_sync::mutex::Mutex;

#[tokio::test]
async fn test_mutex_simple() {
    let m = Mutex::new(10);
    {
        let mut guard = m.lock().await;
        *guard += 1;
    }
    assert_eq!(*m.lock().await, 11);
}

#[tokio::test]
async fn test_mutex_contention() {
    let m = Arc::new(Mutex::new(0));
    let mut tasks = vec![];

    for _ in 0..10 {
        let m = m.clone();
        tasks.push(tokio::spawn(async move {
            for _ in 0..100 {
                let mut guard = m.lock().await;
                *guard += 1;
            }
        }));
    }

    for t in tasks {
        t.await.unwrap();
    }

    assert_eq!(*m.lock().await, 1000);
}

#[tokio::test]
async fn test_mutex_try_lock() {
    let m = Arc::new(Mutex::new(0));
    let m2 = m.clone();

    let guard = m.try_lock().unwrap();

    // Contention
    let t = tokio::spawn(async move {
        assert!(m2.try_lock().is_none());
    });
    t.await.unwrap();

    drop(guard);
    assert!(m.try_lock().is_some());
}

#[tokio::test]
async fn test_mutex_cancel_cascading() {
    let m = Arc::new(Mutex::new(0));
    let guard = m.lock().await;

    let m1 = m.clone();
    let m2 = m.clone();

    let (tx1, rx1) = tokio::sync::oneshot::channel();
    let t1 = tokio::spawn(async move {
        let fut = m1.lock();
        let _ = tx1.send(());
        fut.await;
    });

    rx1.await.unwrap();
    tokio::task::yield_now().await;

    let (tx2, rx2) = tokio::sync::oneshot::channel();
    let t2 = tokio::spawn(async move {
        let fut = m2.lock();
        let _ = tx2.send(());
        let mut g = fut.await;
        *g += 42;
    });

    rx2.await.unwrap();
    tokio::task::yield_now().await;

    // Drop guard, granting lock to t1
    drop(guard);

    // Abort t1 while holding STATE_GRANTED
    t1.abort();
    let _ = t1.await;

    // t2 should receive the lock cascading from t1
    t2.await.unwrap();

    assert_eq!(*m.lock().await, 42);
}

#[tokio::test]
async fn test_mutex_cancel_reset_unlocked() {
    let m = Arc::new(Mutex::new(0));
    let guard = m.lock().await;

    let m1 = m.clone();
    let (tx, rx) = tokio::sync::oneshot::channel();
    let t1 = tokio::spawn(async move {
        let fut = m1.lock();
        let _ = tx.send(());
        fut.await;
    });

    rx.await.unwrap();
    tokio::task::yield_now().await;

    // Drop guard, granting lock to t1
    drop(guard);

    // Cancel t1 while holding STATE_GRANTED
    t1.abort();
    let _ = t1.await;

    // The lock should be safely reset to UNLOCKED
    assert!(!m.is_locked());
    let mut g = m.try_lock().expect("mutex should be unlocked");
    *g = 100;
    drop(g);
    assert_eq!(*m.lock().await, 100);
}
