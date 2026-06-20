#![cfg(not(feature = "loom"))]
use std::sync::Arc;
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
