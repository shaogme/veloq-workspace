use std::{cell::Cell, future::pending, pin::pin, rc::Rc, time::Duration};
use tokio::{
    task::{LocalSet, yield_now},
    time::sleep,
};
use veloq_local::semaphore::{AcquireError, AddPermitsError, Semaphore, TryAcquireError};

#[tokio::test]
async fn test_local_semaphore_basic() {
    let sem = Semaphore::new(3);
    assert_eq!(sem.available_permits(), 3);
    assert!(!sem.is_closed());

    {
        let permit = sem.acquire().await.unwrap();
        assert_eq!(sem.available_permits(), 2);
        assert_eq!(permit.num_permits(), 1);
    }
    assert_eq!(sem.available_permits(), 3);

    let p1 = sem.try_acquire().unwrap();
    let p2 = sem.try_acquire_many(2).unwrap();
    assert_eq!(sem.available_permits(), 0);

    assert_eq!(sem.try_acquire().unwrap_err(), TryAcquireError::NoPermits);
    assert_eq!(
        sem.try_acquire_many(1).unwrap_err(),
        TryAcquireError::NoPermits
    );

    drop(p1);
    assert_eq!(sem.available_permits(), 1);
    drop(p2);
    assert_eq!(sem.available_permits(), 3);
}

#[tokio::test]
async fn test_local_semaphore_acquire_many() {
    let sem = Semaphore::new(5);

    let p1 = sem.acquire_many(3).await.unwrap();
    assert_eq!(sem.available_permits(), 2);
    assert_eq!(p1.num_permits(), 3);

    let p2 = sem.acquire_many(2).await.unwrap();
    assert_eq!(sem.available_permits(), 0);
    assert_eq!(p2.num_permits(), 2);

    let p0 = sem.acquire_many(0).await.unwrap();
    assert_eq!(p0.num_permits(), 0);
    assert_eq!(sem.available_permits(), 0);

    drop(p1);
    assert_eq!(sem.available_permits(), 3);
    drop(p2);
    assert_eq!(sem.available_permits(), 5);
    drop(p0);
    assert_eq!(sem.available_permits(), 5);
}

#[tokio::test]
async fn test_local_semaphore_contention() {
    let local = LocalSet::new();

    local
        .run_until(async {
            let max_permits = 3;
            let sem = Rc::new(Semaphore::new(max_permits));
            let active = Rc::new(Cell::new(0));
            let mut handles = Vec::new();

            for _ in 0..10 {
                let sem = sem.clone();
                let active = active.clone();
                handles.push(tokio::task::spawn_local(async move {
                    for _ in 0..20 {
                        let permit = sem.acquire().await.unwrap();
                        let count = active.get() + 1;
                        active.set(count);
                        assert!(
                            count <= max_permits,
                            "Concurrency limit exceeded: {}",
                            count
                        );
                        yield_now().await;
                        active.set(active.get() - 1);
                        drop(permit);
                    }
                }));
            }

            for h in handles {
                h.await.unwrap();
            }

            assert_eq!(sem.available_permits(), max_permits);
            assert_eq!(active.get(), 0);
        })
        .await;
}

#[tokio::test]
async fn test_local_semaphore_fifo_order() {
    let local = LocalSet::new();

    local
        .run_until(async {
            let sem = Rc::new(Semaphore::new(0));
            let order = Rc::new(Cell::new(Vec::new()));
            let mut handles = Vec::new();

            for i in 1..=3 {
                let sem = sem.clone();
                let order = order.clone();
                handles.push(tokio::task::spawn_local(async move {
                    let _permit = sem.acquire().await.unwrap();
                    let mut list = order.take();
                    list.push(i);
                    order.set(list);
                }));
                sleep(Duration::from_millis(10)).await;
            }

            sem.add_permits(1).unwrap();
            sleep(Duration::from_millis(10)).await;
            sem.add_permits(1).unwrap();
            sleep(Duration::from_millis(10)).await;
            sem.add_permits(1).unwrap();

            for h in handles {
                h.await.unwrap();
            }

            assert_eq!(order.take(), vec![1, 2, 3]);
        })
        .await;
}

#[tokio::test]
async fn test_local_semaphore_cancellation_waiting() {
    let local = LocalSet::new();

    local
        .run_until(async {
            let sem = Rc::new(Semaphore::new(1));
            let guard = sem.acquire().await.unwrap();

            let sem1 = sem.clone();
            let t1 = tokio::task::spawn_local(async move {
                let _permit = sem1.acquire().await.unwrap();
                pending::<()>().await;
            });

            let sem2 = sem.clone();
            let t2 = tokio::task::spawn_local(async move {
                let permit = sem2.acquire().await.unwrap();
                permit.num_permits()
            });

            sleep(Duration::from_millis(20)).await;

            // t1 is waiting, cancel it
            t1.abort();
            let _ = t1.await;

            // Release guard, t2 should acquire it
            drop(guard);

            let num = t2.await.unwrap();
            assert_eq!(num, 1);
        })
        .await;
}

#[tokio::test]
async fn test_local_semaphore_cancellation_granted() {
    let local = LocalSet::new();

    local
        .run_until(async {
            let sem = Rc::new(Semaphore::new(1));
            let guard = sem.acquire().await.unwrap();

            let sem1 = sem.clone();
            let sem2 = sem.clone();

            let t1 = tokio::task::spawn_local(async move {
                let fut = sem1.acquire();
                let mut pinned = pin!(fut);
                let _ = tokio::time::timeout(Duration::from_millis(5), &mut pinned).await;
                pending::<()>().await;
            });

            let t2 = tokio::task::spawn_local(async move {
                let permit = sem2.acquire().await.unwrap();
                permit.num_permits()
            });

            sleep(Duration::from_millis(20)).await;

            // Dropping guard grants permit to t1
            drop(guard);

            // Abort t1 before it completes consuming the permit
            t1.abort();
            let _ = t1.await;

            // t2 should receive the cascaded permit
            let num = t2.await.unwrap();
            assert_eq!(num, 1);
            assert_eq!(sem.available_permits(), 1);
        })
        .await;
}

#[tokio::test]
async fn test_local_semaphore_cancellation_head_unblocks_tail() {
    let local = LocalSet::new();

    local
        .run_until(async {
            let sem = Rc::new(Semaphore::new(0));

            let sem1 = sem.clone();
            let t1 = tokio::task::spawn_local(async move {
                let _p = sem1.acquire_many(5).await.unwrap();
            });

            let sem2 = sem.clone();
            let t2 = tokio::task::spawn_local(async move {
                let permit = sem2.acquire_many(2).await.unwrap();
                permit.num_permits()
            });

            sleep(Duration::from_millis(20)).await;

            // Add 3 permits: not enough for t1 (needs 5), but enough for t2 (needs 2)
            sem.add_permits(3).unwrap();
            sleep(Duration::from_millis(10)).await;

            // t1 is canceled, freeing up the front of the queue
            t1.abort();
            let _ = t1.await;

            // t2 should now be unblocked with the existing 3 permits!
            let num = t2.await.unwrap();
            assert_eq!(num, 2);
            assert_eq!(sem.available_permits(), 3);
        })
        .await;
}

#[tokio::test]
async fn test_local_semaphore_close() {
    let local = LocalSet::new();

    local
        .run_until(async {
            let sem = Rc::new(Semaphore::new(1));
            let permit = sem.acquire().await.unwrap();

            let sem1 = sem.clone();
            let t1 = tokio::task::spawn_local(async move { sem1.acquire().await.is_err() });

            sleep(Duration::from_millis(20)).await;

            // Close the semaphore
            sem.close();
            assert!(sem.is_closed());

            // t1 should wake up with AcquireError
            let is_err = t1.await.unwrap();
            assert!(is_err);

            // New acquires should fail immediately
            assert_eq!(sem.acquire().await.unwrap_err(), AcquireError::Closed);
            assert_eq!(sem.try_acquire().unwrap_err(), TryAcquireError::Closed);

            // Dropping existing permit releases permit back, but semaphore remains closed
            drop(permit);
            assert_eq!(sem.available_permits(), 1);
            assert_eq!(sem.try_acquire().unwrap_err(), TryAcquireError::Closed);
        })
        .await;
}

#[tokio::test]
async fn test_local_semaphore_forget() {
    let sem = Semaphore::new(5);
    let permit = sem.acquire_many(2).await.unwrap();
    assert_eq!(sem.available_permits(), 3);

    permit.forget();
    assert_eq!(sem.available_permits(), 3);

    sem.forget_permits(1);
    assert_eq!(sem.available_permits(), 2);
}

#[tokio::test]
async fn test_local_semaphore_add_permits() {
    let local = LocalSet::new();

    local
        .run_until(async {
            let sem = Rc::new(Semaphore::new(0));
            let sem2 = sem.clone();

            let handle = tokio::task::spawn_local(async move {
                let permit = sem2.acquire_many(3).await.unwrap();
                assert_eq!(permit.num_permits(), 3);
            });

            sleep(Duration::from_millis(10)).await;
            assert_eq!(sem.add_permits(0), Ok(()));
            sem.add_permits(3).unwrap();

            handle.await.unwrap();
            assert_eq!(sem.available_permits(), 3);
        })
        .await;
}

#[tokio::test]
async fn test_local_semaphore_capacity_and_overflow_boundaries() {
    let sem = Semaphore::with_capacity(3);
    assert_eq!(sem.capacity(), 3);
    assert_eq!(sem.available_permits(), 0);
    assert_eq!(sem.add_permits(3), Ok(()));
    assert_eq!(
        sem.add_permits(1),
        Err(AddPermitsError {
            requested: 1,
            remaining: 0,
        })
    );
    assert_eq!(sem.available_permits(), 3);

    let max = Semaphore::new(usize::MAX);
    assert_eq!(
        max.add_permits(1),
        Err(AddPermitsError {
            requested: 1,
            remaining: 0,
        })
    );

    let near_max = Semaphore::with_capacity_and_permits(usize::MAX, usize::MAX - 1).unwrap();
    let permit = near_max.try_acquire().unwrap();
    near_max.add_permits(1).unwrap();
    drop(permit);
    assert_eq!(near_max.available_permits(), usize::MAX);
}

#[tokio::test]
async fn test_local_semaphore_forget_reuses_capacity() {
    let sem = Semaphore::new(5);
    let permit = sem.acquire_many(2).await.unwrap();
    permit.forget();
    assert_eq!(sem.add_permits(2), Ok(()));
    assert_eq!(sem.available_permits(), 5);

    sem.forget_permits(2);
    assert_eq!(sem.available_permits(), 3);
    assert_eq!(sem.add_permits(2), Ok(()));
    assert_eq!(sem.available_permits(), 5);
}

#[tokio::test]
async fn test_local_semaphore_too_many_permits_and_closed_priority() {
    let sem = Semaphore::with_capacity(3);
    assert_eq!(
        sem.try_acquire_many(4).unwrap_err(),
        TryAcquireError::TooManyPermits
    );
    assert_eq!(
        sem.acquire_many(4).await.unwrap_err(),
        AcquireError::TooManyPermits
    );

    sem.close();
    assert_eq!(sem.add_permits(0), Ok(()));
    assert_eq!(
        sem.try_acquire_many(4).unwrap_err(),
        TryAcquireError::Closed
    );
    assert_eq!(sem.acquire_many(4).await.unwrap_err(), AcquireError::Closed);
}
