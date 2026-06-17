use super::error::Elapsed;
use super::sleep::{LocalSleep, Sleep, sleep_until, sleep_until_local};
use crate::runtime::context::Ctx;
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
    time::{Duration, Instant},
};

// ============================================================================
// Sync/Send Timeout
// ============================================================================

pub fn timeout<'rt, 'reg, T>(
    ctx: Ctx<'rt, 'reg>,
    duration: Duration,
    future: T,
) -> Timeout<'rt, 'reg, T>
where
    T: Future,
{
    timeout_at(ctx, Instant::now() + duration, future)
}

pub fn timeout_at<'rt, 'reg, T>(
    ctx: Ctx<'rt, 'reg>,
    deadline: Instant,
    future: T,
) -> Timeout<'rt, 'reg, T>
where
    T: Future,
{
    Timeout {
        value: future,
        delay: sleep_until(ctx, deadline),
    }
}

pub struct Timeout<'rt, 'reg, T> {
    value: T,
    delay: Sleep<'rt, 'reg>,
}

impl<'rt, 'reg, T> Future for Timeout<'rt, 'reg, T>
where
    'reg: 'rt,
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

pub fn timeout_local<'rt, 'reg, T>(
    ctx: Ctx<'rt, 'reg>,
    duration: Duration,
    future: T,
) -> LocalTimeout<'rt, 'reg, T>
where
    T: Future,
{
    timeout_at_local(ctx, Instant::now() + duration, future)
}

pub fn timeout_at_local<'rt, 'reg, T>(
    ctx: Ctx<'rt, 'reg>,
    deadline: Instant,
    future: T,
) -> LocalTimeout<'rt, 'reg, T>
where
    T: Future,
{
    LocalTimeout {
        value: future,
        delay: sleep_until_local(ctx, deadline),
    }
}

pub struct LocalTimeout<'rt, 'reg, T> {
    value: T,
    delay: LocalSleep<'rt, 'reg>,
}

impl<'rt, 'reg, T> Future for LocalTimeout<'rt, 'reg, T>
where
    'reg: 'rt,
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
