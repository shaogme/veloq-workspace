#![cfg(feature = "loom")]

use loom::{future::block_on, sync::Arc, thread};
use veloq_sync::semaphore::{AcquireError, AddPermitsError, Semaphore, TryAcquireError};

#[test]
fn test_loom_semaphore_exclusion() {
    loom::model(|| {
        let sem = Arc::new(Semaphore::new(1));
        let s1 = sem.clone();
        let s2 = sem.clone();

        let t1 = thread::spawn(move || {
            block_on(async move {
                let p = s1.acquire().await.unwrap();
                drop(p);
            });
        });

        let t2 = thread::spawn(move || {
            block_on(async move {
                let p = s2.acquire().await.unwrap();
                drop(p);
            });
        });

        t1.join().unwrap();
        t2.join().unwrap();

        assert_eq!(sem.available_permits(), 1);
    });
}

#[test]
fn test_loom_semaphore_add_permits() {
    loom::model(|| {
        let sem = Arc::new(Semaphore::new(0));
        let s1 = sem.clone();
        let s2 = sem.clone();

        let t1 = thread::spawn(move || {
            block_on(async move {
                let p = s1.acquire().await.unwrap();
                assert_eq!(p.num_permits(), 1);
            });
        });

        let t2 = thread::spawn(move || {
            s2.add_permits(1).unwrap();
        });

        t1.join().unwrap();
        t2.join().unwrap();

        assert_eq!(sem.available_permits(), 1);
    });
}

#[test]
fn test_loom_semaphore_close() {
    loom::model(|| {
        let sem = Arc::new(Semaphore::new(0));
        let s1 = sem.clone();
        let s2 = sem.clone();

        let t1 = thread::spawn(move || {
            block_on(async move {
                let res = s1.acquire().await;
                assert_eq!(res.unwrap_err(), AcquireError::Closed);
            });
        });

        let t2 = thread::spawn(move || {
            s2.close();
        });

        t1.join().unwrap();
        t2.join().unwrap();

        assert!(sem.is_closed());
        assert_eq!(sem.try_acquire().unwrap_err(), TryAcquireError::Closed);
    });
}

#[test]
fn test_loom_semaphore_acquire_many() {
    let mut builder = loom::model::Builder::new();
    builder.preemption_bound = Some(2);
    builder.check(|| {
        let sem = Arc::new(Semaphore::new(2));
        let s1 = sem.clone();
        let s2 = sem.clone();

        let t1 = thread::spawn(move || {
            block_on(async move {
                let p = s1.acquire_many(2).await.unwrap();
                drop(p);
            });
        });

        let t2 = thread::spawn(move || {
            block_on(async move {
                let p = s2.acquire().await.unwrap();
                drop(p);
            });
        });

        t1.join().unwrap();
        t2.join().unwrap();

        assert_eq!(sem.available_permits(), 2);
    });
}

#[test]
fn test_loom_semaphore_cancellation() {
    let mut builder = loom::model::Builder::new();
    builder.preemption_bound = Some(2);
    builder.check(|| {
        let sem = Arc::new(Semaphore::new(1));
        let s1 = sem.clone();
        let s2 = sem.clone();
        let s3 = sem.clone();

        let guard = block_on(async { s1.acquire().await.unwrap() });

        let t1 = thread::spawn(move || {
            block_on(async move {
                // Poll once or drop future
                let fut = s2.acquire();
                drop(fut);
            });
        });

        let t2 = thread::spawn(move || {
            block_on(async move {
                let p = s3.acquire().await.unwrap();
                drop(p);
            });
        });

        drop(guard);

        t1.join().unwrap();
        t2.join().unwrap();

        assert_eq!(sem.available_permits(), 1);
    });
}

#[test]
fn test_loom_semaphore_try_acquire() {
    loom::model(|| {
        let sem = Arc::new(Semaphore::new(1));
        let s1 = sem.clone();
        let s2 = sem.clone();

        let t1 = thread::spawn(move || {
            if let Ok(p) = s1.try_acquire() {
                drop(p);
            }
        });

        let t2 = thread::spawn(move || {
            if let Ok(p) = s2.try_acquire() {
                drop(p);
            }
        });

        t1.join().unwrap();
        t2.join().unwrap();

        assert_eq!(sem.available_permits(), 1);
    });
}

#[test]
fn test_loom_semaphore_near_capacity() {
    loom::model(|| {
        let sem =
            Arc::new(Semaphore::with_capacity_and_permits(usize::MAX, usize::MAX - 1).unwrap());
        let s1 = sem.clone();
        let s2 = sem.clone();

        let t1 = thread::spawn(move || {
            let permit = s1.try_acquire().unwrap();
            drop(permit);
        });

        let t2 = thread::spawn(move || {
            s2.add_permits(1).unwrap();
        });

        t1.join().unwrap();
        t2.join().unwrap();
        assert_eq!(sem.available_permits(), usize::MAX);
    });
}

#[test]
fn test_loom_semaphore_failed_add_and_release() {
    loom::model(|| {
        let sem = Arc::new(Semaphore::new(1));
        let s0 = sem.clone();
        let guard = block_on(async { s0.acquire().await.unwrap() });
        let s1 = sem.clone();

        let t1 = thread::spawn(move || {
            assert_eq!(
                s1.add_permits(usize::MAX),
                Err(AddPermitsError {
                    requested: usize::MAX,
                    remaining: usize::MAX - 1,
                })
            );
        });

        t1.join().unwrap();
        drop(guard);
        assert_eq!(sem.available_permits(), 1);
    });
}
