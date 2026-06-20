#![cfg(not(feature = "loom"))]
use std::sync::Arc;
use veloq_sync::rwlock::RwLock;

#[tokio::test]
async fn test_rwlock_simple() {
    let lock = RwLock::new(0);
    {
        let mut guard = lock.write().await;
        *guard += 1;
    }
    {
        let guard = lock.read().await;
        assert_eq!(*guard, 1);
    }
}

#[tokio::test]
async fn test_rwlock_contention() {
    let lock = Arc::new(RwLock::new(0));
    let mut tasks = vec![];

    // Writers
    for _ in 0..10 {
        let lock = lock.clone();
        tasks.push(tokio::spawn(async move {
            for _ in 0..100 {
                let mut guard = lock.write().await;
                *guard += 1;
            }
        }));
    }

    // Readers
    for _ in 0..10 {
        let lock = lock.clone();
        tasks.push(tokio::spawn(async move {
            for _ in 0..100 {
                let guard = lock.read().await;
                assert!(*guard >= 0);
            }
        }));
    }

    for t in tasks {
        t.await.unwrap();
    }

    assert_eq!(*lock.read().await, 1000);
}

#[tokio::test]
async fn test_rwlock_downgrade() {
    let lock = Arc::new(RwLock::new(0));

    // 1. Acquire write lock
    let mut w_guard = lock.write().await;
    *w_guard = 42;

    // 2. Spawn a reader that waits
    let lock_clone = lock.clone();
    let reader_handle = tokio::spawn(async move {
        // This should block until downgrade or unlock
        let guard = lock_clone.read().await;
        *guard
    });

    // Ensure reader spawns and likely hits the lock
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // 3. Downgrade
    // This transitions from Write -> Read and should wake the reader.
    let r_guard = w_guard.downgrade();

    // 4. Verification
    assert_eq!(*r_guard, 42);

    // The spawned reader should complete because we are now in shared mode
    let val = reader_handle.await.unwrap();
    assert_eq!(val, 42);
}
