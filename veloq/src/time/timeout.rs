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

pub fn timeout<'a, T>(ctx: &'a RuntimeContext, duration: Duration, future: T) -> Timeout<'a, T>
where
    T: Future,
{
    timeout_at(ctx, Instant::now() + duration, future)
}

pub fn timeout_at<'a, T>(ctx: &'a RuntimeContext, deadline: Instant, future: T) -> Timeout<'a, T>
where
    T: Future,
{
    Timeout {
        value: future,
        delay: sleep_until(ctx, deadline),
    }
}

pub struct Timeout<'a, T> {
    value: T,
    delay: Sleep<'a>,
}

impl<'a, T> Future for Timeout<'a, T>
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

pub fn timeout_local<'a, T>(
    ctx: &'a RuntimeContext,
    duration: Duration,
    future: T,
) -> LocalTimeout<'a, T>
where
    T: Future,
{
    timeout_at_local(ctx, Instant::now() + duration, future)
}

pub fn timeout_at_local<'a, T>(
    ctx: &'a RuntimeContext,
    deadline: Instant,
    future: T,
) -> LocalTimeout<'a, T>
where
    T: Future,
{
    LocalTimeout {
        value: future,
        delay: sleep_until_local(ctx, deadline),
    }
}

pub struct LocalTimeout<'a, T> {
    value: T,
    delay: LocalSleep<'a>,
}

impl<'a, T> Future for LocalTimeout<'a, T>
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
