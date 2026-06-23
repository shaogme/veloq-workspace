use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};
use veloq_runtime::{runtime::Runtime, scope, scope::JoinOutcome, select, task::TaskError};

struct ReadyFuture<T>(Option<T>);
impl<T: Unpin + Copy> Future for ReadyFuture<T> {
    type Output = T;
    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if let Some(v) = self.0.take() {
            Poll::Ready(v)
        } else {
            Poll::Pending
        }
    }
}

fn ready<T>(t: T) -> ReadyFuture<T> {
    ReadyFuture(Some(t))
}

struct PendingFuture;
impl Future for PendingFuture {
    type Output = i32;
    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        Poll::Pending
    }
}

#[test]
fn test_select_basic() {
    Runtime::<(), _>::scope(async |ctx| {
        let res = select! {
            ctx;
            val = ready(1) => val,
            _ = PendingFuture => 2,
        };
        assert_eq!(res, 1);
    })
    .unwrap();
}

#[test]
fn test_select_biased() {
    Runtime::<(), _>::scope(async |ctx| {
        let res = select! {
            ctx;
            biased;
            val = ready(10) => val,
            val2 = ready(20) => val2,
        };
        assert_eq!(res, 10);
    })
    .unwrap();
}

#[test]
fn test_select_biased_reverse() {
    Runtime::<(), _>::scope(async |ctx| {
        let res = select! {
            ctx;
            biased;
            val = ready(20) => val,
            val2 = ready(10) => val2,
        };
        assert_eq!(res, 20);
    })
    .unwrap();
}

#[test]
fn test_select_fair_distribution() {
    Runtime::<(), _>::scope(async |ctx| {
        let mut saw_first = false;
        let mut saw_second = false;

        for _ in 0..64 {
            let res = select! {
                ctx;
                _ = ready(10) => 0,
                _ = ready(20) => 1,
            };
            match res {
                0 => saw_first = true,
                1 => saw_second = true,
                _ => unreachable!(),
            }
        }

        assert!(
            saw_first,
            "fair select should sometimes choose the first branch"
        );
        assert!(
            saw_second,
            "fair select should sometimes choose the second branch"
        );
    })
    .unwrap();
}

#[test]
fn test_select_expression() {
    Runtime::<(), _>::scope(async |ctx| {
        let res = select! {
            ctx;
            v = async { 5 + 5 } => v,
            _ = PendingFuture => 0,
        };
        assert_eq!(res, 10);
    })
    .unwrap();
}

#[test]
fn test_select_three_branches() {
    Runtime::<(), _>::scope(async |ctx| {
        let res = select! {
            ctx;
            _ = PendingFuture => 1,
            _ = PendingFuture => 2,
            val = ready(3) => val,
        };
        assert_eq!(res, 3);
    })
    .unwrap();
}

#[test]
fn test_select_cancellation() {
    Runtime::<(), _>::scope(async |ctx| {
        scope!(ctx, async |s| {
            let handle = s.spawn_boxed(async move {
                select! {
                    ctx;
                    _ = PendingFuture => (),
                }
            });

            handle.cancel();

            let res = handle.await;
            match res {
                JoinOutcome::TaskErr(TaskError::Cancelled) => {}
                _ => panic!("Expected TaskError::Cancelled, got {:?}", res),
            }
        })
        .await
        .unwrap();
    })
    .unwrap();
}
