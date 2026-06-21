use std::ops::AsyncFnOnce;
use veloq::runtime::{Runtime, context::Ctx, scope};
use veloq_buf::{UniformSlot, heap::ThreadMemoryMultiplier, nz};
use veloq_runtime::task::yield_now;
use veloq_sync::mpsc;

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
fn test_sync_unbounded_simple() {
    run_test(async |_ctx| {
        let state = mpsc::unbounded();
        let (tx, mut rx) = state.split();

        tx.send(1).unwrap();
        tx.send(2).unwrap();

        assert_eq!(rx.recv().await, Some(1));
        assert_eq!(rx.recv().await, Some(2));
    });
}

#[test]
fn test_sync_unbounded_multi_thread() {
    run_test(async |ctx| {
        let state = mpsc::unbounded();
        let (tx, mut rx) = state.split();

        scope!(ctx, async |s| {
            for i in 0..10 {
                let tx = tx.clone();
                s.spawn_boxed(async move {
                    tx.send(i).unwrap();
                });
            }
            drop(tx);

            let mut sum = 0;
            for _ in 0..10 {
                if let Some(val) = rx.recv().await {
                    sum += val;
                }
            }
            assert_eq!(sum, 45);
        })
        .await
        .unwrap();
    });
}

#[test]
fn test_sync_unbounded_stream() {
    use futures_util::Stream;
    use std::pin::Pin;

    run_test(async |ctx| {
        let state = mpsc::unbounded();
        let (tx, rx) = state.split();

        scope!(ctx, async |s| {
            s.spawn_boxed(async move {
                for i in 0..5 {
                    tx.send(i).unwrap();
                }
            });

            let mut stream = Box::pin(rx);
            async fn next_item<S: Stream<Item = i32> + Unpin>(s: &mut S) -> Option<i32> {
                use std::future::poll_fn;
                poll_fn(|cx| Pin::new(&mut *s).poll_next(cx)).await
            }

            assert_eq!(next_item(&mut stream).await, Some(0));
            assert_eq!(next_item(&mut stream).await, Some(1));
            assert_eq!(next_item(&mut stream).await, Some(2));
            assert_eq!(next_item(&mut stream).await, Some(3));
            assert_eq!(next_item(&mut stream).await, Some(4));
        })
        .await
        .unwrap();
    });
}

#[test]
fn test_sync_bounded_capacity() {
    run_test(async |ctx| {
        let state = mpsc::bounded(1);
        let (tx, mut rx) = state.split();

        tx.send(1).await.unwrap();

        scope!(ctx, async |s| {
            let tx_clone = tx.clone();
            s.spawn_boxed(async move {
                tx_clone.send(2).await.unwrap();
            });

            yield_now().await;

            assert_eq!(rx.recv().await, Some(1));
            assert_eq!(rx.recv().await, Some(2));
        })
        .await
        .unwrap();
    });
}

#[test]
fn test_sync_bounded_drop_receiver() {
    run_test(async |ctx| {
        let state = mpsc::bounded(1);
        let (tx, rx) = state.split();
        tx.send(1).await.unwrap();

        scope!(ctx, async |s| {
            s.spawn_boxed(async move {
                let result = tx.send(2).await;
                assert!(result.is_err());
            });

            yield_now().await;
            drop(rx);
        })
        .await
        .unwrap();
    });
}

#[test]
fn test_sync_owned_mpsc() {
    run_test(async |ctx| {
        let (tx, rx) = mpsc::owned_bounded(5);

        scope!(ctx, async |s| {
            s.spawn_boxed(async move {
                for i in 0..10 {
                    tx.send(i).await.unwrap();
                }
            });

            let mut rx = rx;
            for i in 0..10 {
                let val = rx.recv().await.expect("Should receive value");
                assert_eq!(val, i);
            }
        })
        .await
        .unwrap();
    });
}
