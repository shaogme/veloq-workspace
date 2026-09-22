#![cfg(not(feature = "loom"))]

use std::{
    collections::VecDeque,
    future::pending,
    pin::pin,
    sync::atomic::{AtomicBool, Ordering},
    time::Duration,
};
use tokio::time::sleep;
use veloq_std::sync::Arc;
use veloq_sync::{
    condvar::Condvar,
    mutex::{Mutex, MutexGuard},
};

#[tokio::test]
async fn test_condvar_simple() {
    let pair = Arc::new((Mutex::new(false), Condvar::new()));
    let pair2 = pair.clone();

    let handle = tokio::spawn(async move {
        let (lock, cvar) = &*pair2;
        let mut guard = lock.lock().await;
        while !*guard {
            guard = cvar.wait(guard).await;
        }
        *guard = true;
        42
    });

    sleep(Duration::from_millis(10)).await;
    {
        let (lock, cvar) = &*pair;
        let mut guard = lock.lock().await;
        *guard = true;
        cvar.notify_one();
    }

    let res = handle.await.unwrap();
    assert_eq!(res, 42);
}

#[tokio::test]
async fn test_condvar_notify_before_wait() {
    let pair = Arc::new((Mutex::new(false), Condvar::new()));
    let (lock, cvar) = &*pair;

    // Condvar does not store permits.
    cvar.notify_one();
    cvar.notify_all();

    let pair2 = pair.clone();
    let completed = Arc::new(AtomicBool::new(false));
    let completed2 = completed.clone();

    let handle = tokio::spawn(async move {
        let (lock, cvar) = &*pair2;
        let mut guard = lock.lock().await;
        while !*guard {
            guard = cvar.wait(guard).await;
        }
        completed2.store(true, Ordering::Release);
    });

    sleep(Duration::from_millis(20)).await;
    assert!(!completed.load(Ordering::Acquire));

    {
        let mut guard = lock.lock().await;
        *guard = true;
        cvar.notify_one();
    }

    handle.await.unwrap();
    assert!(completed.load(Ordering::Acquire));
}

#[tokio::test]
async fn test_condvar_notify_all() {
    let pair = Arc::new((Mutex::new(0usize), Condvar::new()));
    let mut handles = vec![];

    for _ in 0..5 {
        let pair2 = pair.clone();
        handles.push(tokio::spawn(async move {
            let (lock, cvar) = &*pair2;
            let mut val = lock.lock().await;
            while *val < 10 {
                val = cvar.wait(val).await;
            }
            *val
        }));
    }

    sleep(Duration::from_millis(20)).await;

    {
        let (lock, cvar) = &*pair;
        let mut val = lock.lock().await;
        *val = 10;
        cvar.notify_all();
    }

    for h in handles {
        let val = h.await.unwrap();
        assert_eq!(val, 10);
    }
}

#[tokio::test]
async fn test_condvar_wait_while() {
    let pair = Arc::new((Mutex::new(false), Condvar::new()));
    let pair2 = pair.clone();

    let handle = tokio::spawn(async move {
        let (lock, cvar) = &*pair2;
        let guard = lock.lock().await;
        let mut guard = cvar.wait_while(guard, |flag| !*flag).await;
        assert!(*guard);
        *guard = false;
        100
    });

    sleep(Duration::from_millis(10)).await;
    {
        let (lock, cvar) = &*pair;
        let mut guard = lock.lock().await;
        *guard = true;
        cvar.notify_one();
    }

    let res = handle.await.unwrap();
    assert_eq!(res, 100);
}

#[tokio::test]
async fn test_condvar_wait_while_async() {
    let pair = Arc::new((Mutex::new(0usize), Condvar::new()));
    let pair2 = pair.clone();

    let handle = tokio::spawn(async move {
        let (lock, cvar) = &*pair2;
        let guard = lock.lock().await;
        let guard = cvar
            .wait_while_async(guard, async |val| {
                sleep(Duration::from_millis(1)).await;
                *val < 5
            })
            .await;
        *guard
    });

    sleep(Duration::from_millis(10)).await;
    {
        let (lock, cvar) = &*pair;
        let mut guard = lock.lock().await;
        *guard = 5;
        cvar.notify_one();
    }

    let res = handle.await.unwrap();
    assert_eq!(res, 5);
}

#[tokio::test]
async fn test_condvar_bounded_queue() {
    struct BoundedQueue {
        queue: Mutex<VecDeque<i32>>,
        not_empty: Condvar,
        not_full: Condvar,
        capacity: usize,
    }

    let q = Arc::new(BoundedQueue {
        queue: Mutex::new(VecDeque::new()),
        not_empty: Condvar::new(),
        not_full: Condvar::new(),
        capacity: 3,
    });

    let mut consumers = vec![];
    for _ in 0..3 {
        let q2 = q.clone();
        consumers.push(tokio::spawn(async move {
            let mut sum = 0;
            loop {
                let mut guard = q2.queue.lock().await;
                while guard.is_empty() {
                    guard = q2.not_empty.wait(guard).await;
                }
                let item = guard.pop_front().unwrap();
                q2.not_full.notify_one();
                if item == -1 {
                    break;
                }
                sum += item;
            }
            sum
        }));
    }

    let q3 = q.clone();
    let producer = tokio::spawn(async move {
        for i in 1..=50 {
            let mut guard = q3.queue.lock().await;
            while guard.len() >= q3.capacity {
                guard = q3.not_full.wait(guard).await;
            }
            guard.push_back(i);
            q3.not_empty.notify_one();
        }
        for _ in 0..3 {
            let mut guard = q3.queue.lock().await;
            while guard.len() >= q3.capacity {
                guard = q3.not_full.wait(guard).await;
            }
            guard.push_back(-1);
            q3.not_empty.notify_one();
        }
    });

    producer.await.unwrap();
    let mut total = 0;
    for c in consumers {
        total += c.await.unwrap();
    }
    assert_eq!(total, (1..=50).sum::<i32>());
}

#[tokio::test]
async fn test_condvar_cancellation_waiting() {
    let pair = Arc::new((Mutex::new(false), Condvar::new()));
    let pair2 = pair.clone();

    let handle = tokio::spawn(async move {
        let (lock, cvar) = &*pair2;
        let guard = lock.lock().await;
        let mut wait_fut = pin!(cvar.wait(guard));
        tokio::select! {
            _ = &mut wait_fut => panic!("should not complete"),
            _ = sleep(Duration::from_millis(15)) => {}
        }
    });

    handle.await.unwrap();

    let (lock, cvar) = &*pair;
    let pair3 = pair.clone();
    let handle2 = tokio::spawn(async move {
        let (lock, cvar) = &*pair3;
        let mut guard = lock.lock().await;
        while !*guard {
            guard = cvar.wait(guard).await;
        }
        77
    });

    sleep(Duration::from_millis(10)).await;
    {
        let mut guard = lock.lock().await;
        *guard = true;
        cvar.notify_one();
    }

    let val = handle2.await.unwrap();
    assert_eq!(val, 77);
}

#[tokio::test]
async fn test_condvar_cancellation_transfer_notify() {
    let pair = Arc::new((Mutex::new(0usize), Condvar::new()));
    let pair1 = pair.clone();
    let pair2 = pair.clone();

    let started1 = Arc::new(AtomicBool::new(false));
    let started1_c = started1.clone();
    let started2 = Arc::new(AtomicBool::new(false));
    let started2_c = started2.clone();

    let h1 = tokio::spawn(async move {
        let (lock, cvar) = &*pair1;
        let guard = lock.lock().await;
        started1_c.store(true, Ordering::Release);
        let mut wait_fut = pin!(cvar.wait(guard));
        tokio::select! {
            _ = &mut wait_fut => 1,
            _ = pending::<()>() => 0,
        }
    });

    let h2 = tokio::spawn(async move {
        let (lock, cvar) = &*pair2;
        let guard = lock.lock().await;
        started2_c.store(true, Ordering::Release);
        let mut guard = cvar.wait(guard).await;
        *guard += 1;
        *guard
    });

    while !started1.load(Ordering::Acquire) || !started2.load(Ordering::Acquire) {
        sleep(Duration::from_millis(5)).await;
    }

    sleep(Duration::from_millis(15)).await;

    {
        let (lock, cvar) = &*pair;
        let _guard = lock.lock().await;
        cvar.notify_one();
        h1.abort();
    }

    let val = h2.await.unwrap();
    assert_eq!(val, 1);
}

#[test]
fn test_condvar_mutex_get() {
    let m = Mutex::new(42);
    let guard = m.try_lock().unwrap();
    let source_m = MutexGuard::mutex(&guard);
    assert!(source_m.is_locked());
}
