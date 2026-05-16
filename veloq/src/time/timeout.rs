use super::error::Elapsed;
use super::sleep::{LocalSleep, Sleep, sleep_until, sleep_until_local};
use crate::runtime::context::RuntimeContext;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

// ============================================================================
// Sync/Send Timeout
// ============================================================================

pub fn timeout<'ctx, T>(
    ctx: RuntimeContext<'ctx>,
    duration: Duration,
    future: T,
) -> Timeout<'ctx, T>
where
    T: Future,
{
    timeout_at(ctx, Instant::now() + duration, future)
}

pub fn timeout_at<'ctx, T>(
    ctx: RuntimeContext<'ctx>,
    deadline: Instant,
    future: T,
) -> Timeout<'ctx, T>
where
    T: Future,
{
    Timeout {
        value: future,
        delay: sleep_until(ctx, deadline),
    }
}

pub struct Timeout<'ctx, T> {
    value: T,
    delay: Sleep<'ctx>,
}

impl<'ctx, T> Future for Timeout<'ctx, T>
where
    T: Future,
{
    type Output = Result<T::Output, Elapsed>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        unsafe {
            let this = self.get_unchecked_mut();

            // Poll value first
            let value_pin = Pin::new_unchecked(&mut this.value);
            if let Poll::Ready(v) = value_pin.poll(cx) {
                return Poll::Ready(Ok(v));
            }

            // Poll delay
            let delay_pin = Pin::new_unchecked(&mut this.delay);
            if delay_pin.poll(cx).is_ready() {
                return Poll::Ready(Err(Elapsed::new()));
            }

            Poll::Pending
        }
    }
}

// ============================================================================
// Local Timeout
// ============================================================================

pub fn timeout_local<'ctx, T>(
    ctx: RuntimeContext<'ctx>,
    duration: Duration,
    future: T,
) -> LocalTimeout<'ctx, T>
where
    T: Future,
{
    timeout_at_local(ctx, Instant::now() + duration, future)
}

pub fn timeout_at_local<'ctx, T>(
    ctx: RuntimeContext<'ctx>,
    deadline: Instant,
    future: T,
) -> LocalTimeout<'ctx, T>
where
    T: Future,
{
    LocalTimeout {
        value: future,
        delay: sleep_until_local(ctx, deadline),
    }
}

pub struct LocalTimeout<'ctx, T> {
    value: T,
    delay: LocalSleep<'ctx>,
}

impl<'ctx, T> Future for LocalTimeout<'ctx, T>
where
    T: Future,
{
    type Output = Result<T::Output, Elapsed>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        unsafe {
            let this = self.get_unchecked_mut();

            // Poll value first
            let value_pin = Pin::new_unchecked(&mut this.value);
            if let Poll::Ready(v) = value_pin.poll(cx) {
                return Poll::Ready(Ok(v));
            }

            // Poll delay
            let delay_pin = Pin::new_unchecked(&mut this.delay);
            if delay_pin.poll(cx).is_ready() {
                return Poll::Ready(Err(Elapsed::new()));
            }

            Poll::Pending
        }
    }
}
