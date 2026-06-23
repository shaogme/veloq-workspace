use std::ops::AsyncFnOnce;

use veloq::{
    local::oneshot,
    runtime::{Runtime, context::Ctx, scope_local},
};
use veloq_buf::{UniformSlot, heap::ThreadMemoryMultiplier, nz};

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
fn test_send_recv() {
    run_test(async |ctx| {
        let state = oneshot::channel();
        let (tx, rx) = state.split();

        scope_local!(ctx, async |s| {
            s.spawn_boxed_local(async move {
                tx.send(42).unwrap();
            });

            assert_eq!(rx.await.unwrap(), 42);
        })
        .await
        .unwrap();
    });
}

#[test]
fn test_tx_closed() {
    run_test(async |_ctx| {
        let state = oneshot::channel::<i32>();
        let (tx, rx) = state.split();
        drop(tx);
        assert!(rx.await.is_err());
    });
}

#[test]
fn test_rx_closed() {
    run_test(async |_ctx| {
        let state = oneshot::channel::<i32>();
        let (tx, rx) = state.split();

        assert!(!tx.is_closed());
        drop(rx);
        assert!(tx.is_closed());

        // Attempt to send should fail
        assert_eq!(tx.send(10), Err(10));
    });
}

#[test]
fn test_try_recv() {
    run_test(async |_ctx| {
        let state = oneshot::channel();
        let (tx, rx) = state.split();

        assert_eq!(rx.try_recv(), Err(oneshot::TryRecvError::Empty));

        tx.send(100).unwrap();

        assert_eq!(rx.try_recv(), Ok(100));

        assert_eq!(rx.try_recv(), Err(oneshot::TryRecvError::Closed));
    });
}

#[test]
fn test_drop_tx_notify() {
    run_test(async |ctx| {
        let state = oneshot::channel::<i32>();
        let (tx, rx) = state.split();

        scope_local!(ctx, async |s| {
            let handle = s.spawn_boxed_local(rx);

            // Drop tx without sending
            drop(tx);

            let res = handle.await.unwrap();
            assert!(res.is_err());
        })
        .await
        .unwrap();
    });
}

#[test]
fn test_send_before_recv() {
    run_test(async |_ctx| {
        let state = oneshot::channel();
        let (tx, rx) = state.split();
        tx.send("hello").unwrap();
        assert_eq!(rx.await.unwrap(), "hello");
    });
}

#[test]
fn test_owned_oneshot() {
    run_test(async |ctx| {
        let (tx, rx) = oneshot::owned_channel();

        scope_local!(ctx, async |s| {
            s.spawn_boxed_local(async move {
                tx.send(42).unwrap();
            });

            assert_eq!(rx.await.unwrap(), 42);
        })
        .await
        .unwrap();
    });
}
