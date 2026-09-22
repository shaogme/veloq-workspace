use std::{cell::Cell, collections::VecDeque, future::pending, pin::pin, rc::Rc, time::Duration};
use tokio::{
    task::{LocalSet, spawn_local},
    time::sleep,
};
use veloq_local::{
    condvar::Condvar,
    mutex::{Mutex, MutexGuard},
};

#[tokio::test]
async fn test_local_condvar_simple() {
    let local = LocalSet::new();

    local
        .run_until(async {
            let pair = Rc::new((Mutex::new(false), Condvar::new()));
            let pair2 = pair.clone();

            let handle = spawn_local(async move {
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
        })
        .await;
}

#[tokio::test]
async fn test_local_condvar_notify_before_wait() {
    let local = LocalSet::new();

    local
        .run_until(async {
            let pair = Rc::new((Mutex::new(false), Condvar::new()));
            let (lock, cvar) = &*pair;

            // Condvar does not store permits.
            cvar.notify_one();
            cvar.notify_all();

            let pair2 = pair.clone();
            let completed = Rc::new(Cell::new(false));
            let completed2 = completed.clone();

            let handle = spawn_local(async move {
                let (lock, cvar) = &*pair2;
                let mut guard = lock.lock().await;
                while !*guard {
                    guard = cvar.wait(guard).await;
                }
                completed2.set(true);
            });

            sleep(Duration::from_millis(20)).await;
            assert!(!completed.get());

            {
                let mut guard = lock.lock().await;
                *guard = true;
                cvar.notify_one();
            }

            handle.await.unwrap();
            assert!(completed.get());
        })
        .await;
}

#[tokio::test]
async fn test_local_condvar_notify_all() {
    let local = LocalSet::new();

    local
        .run_until(async {
            let pair = Rc::new((Mutex::new(0usize), Condvar::new()));
            let mut handles = vec![];

            for _ in 0..5 {
                let pair2 = pair.clone();
                handles.push(spawn_local(async move {
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
        })
        .await;
}

#[tokio::test]
async fn test_local_condvar_wait_while() {
    let local = LocalSet::new();

    local
        .run_until(async {
            let pair = Rc::new((Mutex::new(false), Condvar::new()));
            let pair2 = pair.clone();

            let handle = spawn_local(async move {
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
        })
        .await;
}

#[tokio::test]
async fn test_local_condvar_queue() {
    let local = LocalSet::new();

    local
        .run_until(async {
            struct BoundedQueue {
                queue: Mutex<VecDeque<i32>>,
                not_empty: Condvar,
                not_full: Condvar,
                capacity: usize,
            }

            let q = Rc::new(BoundedQueue {
                queue: Mutex::new(VecDeque::new()),
                not_empty: Condvar::new(),
                not_full: Condvar::new(),
                capacity: 3,
            });

            let mut consumers = vec![];
            for _ in 0..2 {
                let q2 = q.clone();
                consumers.push(spawn_local(async move {
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
            let producer = spawn_local(async move {
                for i in 1..=20 {
                    let mut guard = q3.queue.lock().await;
                    while guard.len() >= q3.capacity {
                        guard = q3.not_full.wait(guard).await;
                    }
                    guard.push_back(i);
                    q3.not_empty.notify_one();
                }
                for _ in 0..2 {
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
            assert_eq!(total, (1..=20).sum::<i32>());
        })
        .await;
}

#[tokio::test]
async fn test_local_condvar_cancellation_waiting() {
    let local = LocalSet::new();

    local
        .run_until(async {
            let pair = Rc::new((Mutex::new(false), Condvar::new()));
            let pair2 = pair.clone();

            let handle = spawn_local(async move {
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
            let handle2 = spawn_local(async move {
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
        })
        .await;
}

#[tokio::test]
async fn test_local_condvar_cancellation_transfer_notify() {
    let local = LocalSet::new();

    local
        .run_until(async {
            let pair = Rc::new((Mutex::new(0usize), Condvar::new()));
            let pair1 = pair.clone();
            let pair2 = pair.clone();

            let started1 = Rc::new(Cell::new(false));
            let started1_c = started1.clone();
            let started2 = Rc::new(Cell::new(false));
            let started2_c = started2.clone();

            let h1 = spawn_local(async move {
                let (lock, cvar) = &*pair1;
                let guard = lock.lock().await;
                started1_c.set(true);
                let mut wait_fut = pin!(cvar.wait(guard));
                tokio::select! {
                    _ = &mut wait_fut => 1,
                    _ = pending::<()>() => 0,
                }
            });

            let h2 = spawn_local(async move {
                let (lock, cvar) = &*pair2;
                let guard = lock.lock().await;
                started2_c.set(true);
                let mut guard = cvar.wait(guard).await;
                *guard += 1;
                *guard
            });

            while !started1.get() || !started2.get() {
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
        })
        .await;
}

#[test]
fn test_local_condvar_mutex_get() {
    let m = Mutex::new(42);
    let guard = m.try_lock().unwrap();
    let source_m = MutexGuard::mutex(&guard);
    assert!(source_m.is_locked());
}
