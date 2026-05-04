use super::context::WorkerInitContext;
use super::primitives::{Signal, create_waker};
use std::ops::AsyncFn;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

pub(crate) fn block_on_worker_init<I>(worker_init: &I, ctx: WorkerInitContext)
where
    I: AsyncFn(WorkerInitContext) -> (),
{
    let mut future = worker_init(ctx);
    let mut future = unsafe { Pin::new_unchecked(&mut future) };
    let signal = Arc::new(Signal::new(false));
    let waker = create_waker(signal.clone());
    let mut cx = Context::from_waker(&waker);

    loop {
        match future.as_mut().poll(&mut cx) {
            Poll::Ready(()) => return,
            Poll::Pending => signal.wait(),
        }
    }
}
