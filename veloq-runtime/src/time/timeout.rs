use super::error::Elapsed;
use super::sleep::{LocalSleep, Sleep, sleep_until, sleep_until_local};
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

// ============================================================================
// Sync/Send Timeout
// ============================================================================

pub fn timeout<T>(duration: Duration, future: T) -> Timeout<T>
where
    T: Future,
{
    timeout_at(Instant::now() + duration, future)
}

pub fn timeout_at<T>(deadline: Instant, future: T) -> Timeout<T>
where
    T: Future,
{
    Timeout {
        value: future,
        delay: sleep_until(deadline),
    }
}

pub struct Timeout<T> {
    value: T,
    delay: Sleep,
}

impl<T> Future for Timeout<T>
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
            if let Poll::Ready(_) = delay_pin.poll(cx) {
                return Poll::Ready(Err(Elapsed::new()));
            }

            Poll::Pending
        }
    }
}

// ============================================================================
// Local Timeout
// ============================================================================

pub fn timeout_local<T>(duration: Duration, future: T) -> LocalTimeout<T>
where
    T: Future,
{
    timeout_at_local(Instant::now() + duration, future)
}

pub fn timeout_at_local<T>(deadline: Instant, future: T) -> LocalTimeout<T>
where
    T: Future,
{
    LocalTimeout {
        value: future,
        delay: sleep_until_local(deadline),
    }
}

pub struct LocalTimeout<T> {
    value: T,
    delay: LocalSleep,
}

impl<T> Future for LocalTimeout<T>
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
            if let Poll::Ready(_) = delay_pin.poll(cx) {
                return Poll::Ready(Err(Elapsed::new()));
            }

            Poll::Pending
        }
    }
}
