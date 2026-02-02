use crate::runtime::executor::LocalExecutor;
use crate::select;
use std::future::Future;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::task::{Context, Poll};

use veloq_buf::global::{GlobalAllocator, GlobalAllocatorConfig};
use veloq_buf::{AnyBufPool, HybridPool, RegisteredPool};

fn create_local_executor() -> LocalExecutor {
    let multiplier = veloq_buf::ThreadMemoryMultiplier(unsafe { NonZeroUsize::new_unchecked(8) });
    let config = GlobalAllocatorConfig {
        multipliers: vec![multiplier],
    };
    let (mut memories, global_info) = GlobalAllocator::new(config).unwrap();
    let memory = memories.pop().unwrap();

    LocalExecutor::builder().build(move |registrar| {
        let pool = HybridPool::new(memory).unwrap();
        AnyBufPool::new(
            RegisteredPool::new(pool, registrar, global_info)
                .expect("Failed to register buffer pool"),
        )
    })
}
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
    let mut exec = create_local_executor();
    exec.block_on(async {
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
    let mut exec = create_local_executor();
    exec.block_on(async {
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
    let mut exec = create_local_executor();
    exec.block_on(async {
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
    let mut exec = create_local_executor();
    exec.block_on(async {
        let res = select! {
            v = async { 5 + 5 } => v,
            _ = PendingFuture => 0,
        };
        assert_eq!(res, 10);
    });
}
