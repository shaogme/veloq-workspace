use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use veloq_runtime::runtime::Runtime;
use veloq_runtime::select;

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
    let rt = Runtime::new();
    rt.block_on(async |_| {
        let res = select! {
            val = ready(1) => val,
            _ = PendingFuture => 2,
        };
        assert_eq!(res, 1);
    });
}

#[test]
fn test_select_biased() {
    // Both are ready immediately. First one should win.
    let rt = Runtime::new();
    rt.block_on(async |_| {
        let res = select! {
            val = ready(10) => val,
            val2 = ready(20) => val2,
        };
        assert_eq!(res, 10);
    });
}

#[test]
fn test_select_biased_reverse() {
    // Both are ready immediately. First one declared (which is ready(20)) should win.
    let rt = Runtime::new();
    rt.block_on(async |_| {
        let res = select! {
            val = ready(20) => val,
            val2 = ready(10) => val2,
        };
        assert_eq!(res, 20);
    });
}

#[test]
fn test_select_expression() {
    // Test using complex expressions in select
    let rt = Runtime::new();
    rt.block_on(async |_| {
        let res = select! {
            v = async { 5 + 5 } => v,
            _ = PendingFuture => 0,
        };
        assert_eq!(res, 10);
    });
}

#[test]
fn test_select_three_branches() {
    let rt = Runtime::new();
    rt.block_on(async |_| {
        let res = select! {
            _ = PendingFuture => 1,
            _ = PendingFuture => 2,
            val = ready(3) => val,
        };
        assert_eq!(res, 3);
    });
}

#[test]
fn test_select_cancellation() {
    use veloq_runtime::task::TaskError;

    let rt = Runtime::new();
    rt.block_on(async |ctx| {
        ctx.scope(async |s| {
            let handle = s.spawn_boxed(async {
                select! {
                    _ = PendingFuture => (),
                }
            });

            // Cancel the task
            handle.cancel();

            // Join the task and expect Cancelled error
            let res = handle.await;
            match res {
                Err(TaskError::Cancelled) => {}
                _ => panic!("Expected TaskError::Cancelled, got {:?}", res),
            }
        })
        .await;
    });
}
