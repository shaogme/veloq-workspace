#![cfg(not(feature = "loom"))]
use std::sync::Arc;
use std::time::Duration;
use veloq_sync::rwlock::RwLock;

#[tokio::test]
async fn test_bug1_writer_wakeup() {
    let lock = Arc::new(RwLock::new(0));

    // 1. Acquire write lock (W1)
    let mut w1 = lock.write().await;
    *w1 = 1;

    // 2. Spawn W2
    let lock2 = lock.clone();
    let h2 = tokio::spawn(async move {
        // This will block until W1 releases
        let mut w2 = lock2.write().await;
        *w2 = 2;
        // W2 releases immediately upon drop
    });

    // Wait for W2 to be queued
    tokio::time::sleep(Duration::from_millis(50)).await;

    // 3. Spawn W3
    let lock3 = lock.clone();
    let h3 = tokio::spawn(async move {
        // This will block until W2 releases
        let mut w3 = lock3.write().await;
        *w3 = 3;
        // W3 releases immediately upon drop
    });

    // Wait for W3 to be queued
    tokio::time::sleep(Duration::from_millis(50)).await;

    // 4. Release W1
    drop(w1);

    // We expect both W2 and W3 to complete within a reasonable time.
    // If the bug is present (W2 fails to set CONTENDED), W2 will release
    // without waking W3, causing W3 to hang.
    let res = tokio::time::timeout(Duration::from_secs(2), async {
        h2.await.unwrap();
        h3.await.unwrap();
    }).await;

    assert!(res.is_ok(), "W3 failed to wake up (Lost Wakeup Bug detected)");
}

#[tokio::test]
async fn test_bug1_reader_barge() {
    // This test verifies that if a Reader acquires the lock while another waiter exists,
    // it sets CONTENDED so subsequent readers cannot barge.
    let lock = Arc::new(RwLock::new(0));

    // 1. Acquire write lock (W1)
    let mut w1 = lock.write().await;
    *w1 = 1;

    // 2. Spawn R2 (Reader)
    let lock2 = lock.clone();
    let h2 = tokio::spawn(async move {
        let r2 = lock2.read().await;
        assert_eq!(*r2, 1);
        // Hold lock for a bit to allow barge attempt
        tokio::time::sleep(Duration::from_millis(100)).await;
    });

    // Wait for R2 to be queued
    tokio::time::sleep(Duration::from_millis(50)).await;

    // 3. Spawn W3 (Writer)
    let lock3 = lock.clone();
    let h3 = tokio::spawn(async move {
        let mut w3 = lock3.write().await;
        *w3 = 3;
    });

    // Wait for W3 to be queued
    tokio::time::sleep(Duration::from_millis(50)).await;

    // 4. Release W1. R2 should wake up and acquire read lock.
    drop(w1);

    // Wait for R2 to acquire (it sleeps inside)
    tokio::time::sleep(Duration::from_millis(20)).await;

    // 5. Try to barge with R4
    let lock4 = lock.clone();
    let barge_result = lock4.try_read();

    // If R2 set CONTENDED correctly (because W3 is waiting), try_read should fail.
    // If bug exists, try_read might succeed.
    assert!(barge_result.is_none(), "Reader barged successfully! Fairness violated.");

    // Cleanup
    let _ = tokio::join!(h2, h3);
}
