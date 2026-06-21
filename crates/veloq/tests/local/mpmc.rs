use std::ops::AsyncFnOnce;

use veloq::{
    local::mpmc,
    runtime::{Runtime, context::Ctx, scope_local},
};
use veloq_buf::{UniformSlot, heap::ThreadMemoryMultiplier, nz};
use veloq_runtime::task::yield_now;

fn run_test<F, R>(f: F) -> R
where
    F: for<'s1, 's2> AsyncFnOnce(Ctx<'s1, 's2>) -> R,
{
    Runtime::builder(UniformSlot::new(ThreadMemoryMultiplier(nz!(4))))
        .worker_count(Some(nz!(1)))
        .scope(f)
        .expect("failed to run scope")
}

#[test]
fn test_unbounded_basic() {
    run_test(async |ctx| {
        let state = mpmc::unbounded();
        let (tx, rx) = state.split();

        scope_local!(ctx, async |s| {
            s.spawn_boxed_local(async move {
                for i in 0..10 {
                    tx.send(i).await.unwrap();
                }
            });

            let mut expected = 0;
            while let Some(val) = rx.recv().await {
                assert_eq!(val, expected);
                expected += 1;
                if expected == 10 {
                    break;
                }
            }
        })
        .await
        .unwrap();
    });
}

#[test]
fn test_bounded_basic() {
    run_test(async |ctx| {
        let state = mpmc::bounded(5);
        let (tx, rx) = state.split();

        scope_local!(ctx, async |s| {
            s.spawn_boxed_local(async move {
                for i in 0..10 {
                    tx.send(i).await.unwrap();
                }
            });

            for i in 0..10 {
                let val = rx.recv().await.expect("Should receive value");
                assert_eq!(val, i);
            }
        })
        .await
        .unwrap();
    });
}

#[test]
fn test_multiple_producers_consumers() {
    run_test(async |ctx| {
        let state = mpmc::bounded(2);
        let (tx, rx) = state.split();

        use std::cell::RefCell;
        use std::rc::Rc;
        let results = Rc::new(RefCell::new(Vec::new()));

        scope_local!(ctx, async |s| {
            // Spawn 3 senders
            for idx in 0..3 {
                let tx_clone = tx.clone();
                s.spawn_boxed_local(async move {
                    for i in 0..5 {
                        tx_clone.send(idx * 10 + i).await.unwrap();
                    }
                });
            }
            // Drop initial sender so channel closing logic is clean when all clones drop
            drop(tx);

            // Spawn 3 receivers
            for _ in 0..3 {
                let rx_clone = rx.clone();
                let results_clone = results.clone();
                s.spawn_boxed_local(async move {
                    while let Some(val) = rx_clone.recv().await {
                        results_clone.borrow_mut().push(val);
                    }
                });
            }
            drop(rx);

            // Wait for all spawned tasks to finish
            // Note: since they are spawned within the local scope, awaiting the scope will join them.
        })
        .await
        .unwrap();

        // Total messages sent: 3 * 5 = 15.
        let res = results.borrow();
        assert_eq!(res.len(), 15);
        // Verify that all sent numbers are present
        let mut sorted = res.clone();
        sorted.sort();
        let mut expected = Vec::new();
        for idx in 0..3 {
            for i in 0..5 {
                expected.push(idx * 10 + i);
            }
        }
        expected.sort();
        assert_eq!(sorted, expected);
    });
}

#[test]
fn test_sender_drop_closes_channel() {
    run_test(async |ctx| {
        let state = mpmc::unbounded::<()>();
        let (tx, rx) = state.split();
        scope_local!(ctx, async |s| {
            s.spawn_boxed_local(async move {
                drop(tx);
            });

            assert!(rx.recv().await.is_none());
        })
        .await
        .unwrap();
    });
}

#[test]
fn test_receiver_drop_errors_sender() {
    run_test(async |ctx| {
        let state = mpmc::bounded::<i32>(1);
        let (tx, rx) = state.split();

        tx.send(1).await.unwrap();

        scope_local!(ctx, async |s| {
            s.spawn_boxed_local(async move {
                drop(rx);
            });

            yield_now().await;

            match tx.send(2).await {
                Err(mpmc::SendError::Closed(val)) => assert_eq!(val, 2),
                _ => panic!("Should return Closed error"),
            }
        })
        .await
        .unwrap();
    });
}

#[test]
fn test_bounded_backpressure() {
    run_test(async |ctx| {
        let state = mpmc::bounded(1);
        let (tx, rx) = state.split();

        scope_local!(ctx, async |s| {
            let tx_clone = tx.clone();
            s.spawn_boxed_local(async move {
                tx_clone.send(1).await.unwrap();
                tx_clone.send(2).await.unwrap();
            });

            yield_now().await;

            let val1 = rx.recv().await;
            assert_eq!(val1, Some(1));

            let val2 = rx.recv().await;
            assert_eq!(val2, Some(2));
        })
        .await
        .unwrap();
    });
}

#[test]
fn test_stream_conversion() {
    use futures_util::Stream;
    use std::pin::Pin;

    run_test(async |ctx| {
        let state = mpmc::unbounded();
        let (tx, rx) = state.split();

        scope_local!(ctx, async |s| {
            s.spawn_boxed_local(async move {
                tx.send(100).await.unwrap();
                tx.send(200).await.unwrap();
            });

            let mut stream = Box::pin(rx.stream());

            async fn next_item<S: Stream<Item = i32> + Unpin>(s: &mut S) -> Option<i32> {
                use std::future::poll_fn;
                poll_fn(|cx| Pin::new(&mut *s).poll_next(cx)).await
            }

            assert_eq!(next_item(&mut stream).await, Some(100));
            assert_eq!(next_item(&mut stream).await, Some(200));
            assert_eq!(next_item(&mut stream).await, None);
        })
        .await
        .unwrap();
    });
}

#[test]
fn test_try_recv() {
    run_test(async |_ctx| {
        let state = mpmc::unbounded();
        let (tx, rx) = state.split();

        assert_eq!(rx.try_recv(), Err(mpmc::TryRecvError::Empty));

        tx.send(100).await.unwrap();

        assert_eq!(rx.try_recv(), Ok(100));
        assert_eq!(rx.try_recv(), Err(mpmc::TryRecvError::Empty));

        drop(tx);
        assert_eq!(rx.try_recv(), Err(mpmc::TryRecvError::Closed));
    });
}

#[test]
fn test_owned_mpmc() {
    run_test(async |ctx| {
        let (tx, rx) = mpmc::owned_bounded(5);

        scope_local!(ctx, async |s| {
            // Clone sender and receiver
            let tx2 = tx.clone();
            let rx2 = rx.clone();

            s.spawn_boxed_local(async move {
                tx.send(1).await.unwrap();
                tx2.send(2).await.unwrap();
            });

            let mut vals = Vec::new();
            vals.push(rx.recv().await.unwrap());
            vals.push(rx2.recv().await.unwrap());
            vals.sort();
            assert_eq!(vals, vec![1, 2]);
        })
        .await
        .unwrap();
    });
}
