use std::ops::AsyncFnOnce;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use veloq::{
    nz,
    runtime::{Runtime, context::Ctx, scope},
};
use veloq_buf::{UniformSlot, heap::ThreadMemoryMultiplier};
use veloq_runtime::task::yield_now;
use veloq_sync::mpmc;

fn run_test<F, R>(f: F) -> R
where
    F: for<'s1, 's2> AsyncFnOnce(Ctx<'s1, 's2>) -> R,
{
    Runtime::builder(UniformSlot::new(ThreadMemoryMultiplier(nz!(4))))
        .worker_count(Some(nz!(2)))
        .scope(f)
        .expect("failed to run scope")
}

#[test]
fn test_sync_mpmc_unbounded_simple() {
    run_test(async |_ctx| {
        let state = mpmc::unbounded();
        let (tx, rx) = state.split();

        tx.send(1).await.unwrap();
        tx.send(2).await.unwrap();

        let rx2 = rx.clone();

        assert_eq!(rx.recv().await.unwrap(), 1);
        assert_eq!(rx2.recv().await.unwrap(), 2);
    });
}

#[test]
fn test_sync_mpmc_unbounded_concurrent() {
    run_test(async |ctx| {
        let state = mpmc::unbounded();
        let (tx, rx) = state.split();
        let count = 100;

        scope!(ctx, async |s| {
            for _ in 0..5 {
                let tx = tx.clone();
                s.spawn_boxed(async move {
                    for i in 0..count {
                        tx.send(i).await.unwrap();
                    }
                });
            }
            drop(tx);

            let total_received = Arc::new(AtomicUsize::new(0));
            for _ in 0..5 {
                let rx = rx.clone();
                let total = total_received.clone();
                s.spawn_boxed(async move {
                    while rx.recv().await.is_ok() {
                        total.fetch_add(1, Ordering::Relaxed);
                    }
                });
            }
            drop(rx);
        })
        .await
        .unwrap();
    });
}

#[test]
fn test_sync_mpmc_bounded_capacity() {
    run_test(async |ctx| {
        let state = mpmc::bounded(1);
        let (tx, rx) = state.split();

        tx.send(1).await.unwrap();

        scope!(ctx, async |s| {
            let tx_clone = tx.clone();
            s.spawn_boxed(async move {
                tx_clone.send(2).await.unwrap();
            });

            yield_now().await;

            assert_eq!(rx.recv().await.unwrap(), 1);
            assert_eq!(rx.recv().await.unwrap(), 2);
        })
        .await
        .unwrap();
    });
}

#[test]
fn test_sync_mpmc_bounded_multi_consumer() {
    run_test(async |ctx| {
        let state = mpmc::bounded(5);
        let (tx, rx) = state.split();

        for i in 0..5 {
            tx.send(i).await.unwrap();
        }

        scope!(ctx, async |s| {
            let c1 = rx.clone();
            let c2 = rx.clone();

            let h1 = s.spawn_boxed(async move { c1.recv().await.unwrap() });
            let h2 = s.spawn_boxed(async move { c2.recv().await.unwrap() });

            let r1 = h1.await.unwrap();
            let r2 = h2.await.unwrap();

            assert!(r1 < 5);
            assert!(r2 < 5);
            assert_ne!(r1, r2);
        })
        .await
        .unwrap();
    });
}

#[test]
fn test_sync_mpmc_try_send_recv() {
    run_test(async |_ctx| {
        let state = mpmc::bounded(1);
        let (tx, rx) = state.split();

        tx.try_send(1).unwrap();
        assert!(tx.try_send(2).is_err());

        assert_eq!(rx.try_recv().unwrap(), 1);
        assert!(rx.try_recv().is_err());
    });
}
