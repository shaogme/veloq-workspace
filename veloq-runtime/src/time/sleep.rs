use crate::runtime::context;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use veloq_driver::driver::{Driver, PlatformDriver};
use veloq_driver::op::{DetachedOp, LocalOp, Op, Timeout as OpTimeout};

// ============================================================================
// Sync/Send Sleep (uses DetachedOp)
// ============================================================================

/// Waits until `duration` has elapsed.
///
/// This future is `Send` and `Sync`.
pub fn sleep(duration: Duration) -> Sleep {
    sleep_until(Instant::now() + duration)
}

/// Waits until `deadline` is reached.
///
/// This future is `Send` and `Sync`.
pub fn sleep_until(deadline: Instant) -> Sleep {
    Sleep {
        deadline,
        inner: None,
    }
}

pub struct Sleep {
    deadline: Instant,
    inner: Option<
        DetachedOp<
            OpTimeout,
            <PlatformDriver as Driver>::Op,
            <PlatformDriver as Driver>::Completion,
        >,
    >,
}

impl Sleep {
    pub fn deadline(&self) -> Instant {
        self.deadline
    }

    pub fn is_elapsed(&self) -> bool {
        self.inner.is_none() && Instant::now() >= self.deadline
    }

    pub fn reset(&mut self, deadline: Instant) {
        self.deadline = deadline;
        self.inner = None;
    }
}

impl Future for Sleep {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        loop {
            if let Some(ref mut op) = self.inner {
                // Poll existing detached op
                // DetachedOp is Unpin
                let pinned = Pin::new(op);
                match pinned.poll(cx) {
                    Poll::Ready(_) => {
                        self.inner = None;
                        if Instant::now() >= self.deadline {
                            return Poll::Ready(());
                        }
                    }
                    Poll::Pending => return Poll::Pending,
                }
            } else {
                // Initialize
                let now = Instant::now();
                if now >= self.deadline {
                    return Poll::Ready(());
                }

                let duration = self.deadline - now;
                let driver = context::current()
                    .driver()
                    .upgrade()
                    .expect("Runtime driver missing");

                // We need to borrow mutably to submit
                let mut driver_guard = driver.borrow_mut();
                let op = Op::new(OpTimeout { duration }).submit_detached(&mut *driver_guard);

                self.inner = Some(op);
                // Loop to poll immediately
            }
        }
    }
}

// ============================================================================
// Local Sleep (uses LocalOp, !Send)
// ============================================================================

/// Waits until `duration` has elapsed (Local version).
///
/// This future is `!Send`.
pub fn sleep_local(duration: Duration) -> LocalSleep {
    sleep_until_local(Instant::now() + duration)
}

/// Waits until `deadline` is reached (Local version).
///
/// This future is `!Send`.
pub fn sleep_until_local(deadline: Instant) -> LocalSleep {
    LocalSleep {
        deadline,
        inner: None,
    }
}

pub struct LocalSleep {
    deadline: Instant,
    inner: Option<LocalOp<OpTimeout>>,
}

impl LocalSleep {
    pub fn deadline(&self) -> Instant {
        self.deadline
    }

    pub fn is_elapsed(&self) -> bool {
        self.inner.is_none() && Instant::now() >= self.deadline
    }

    pub fn reset(&mut self, deadline: Instant) {
        self.deadline = deadline;
        self.inner = None;
    }
}

impl Future for LocalSleep {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = &mut *self;

        loop {
            if let Some(ref mut op) = this.inner {
                let op_pin = Pin::new(op);
                match op_pin.poll(cx) {
                    Poll::Ready(_) => {
                        if Instant::now() >= this.deadline {
                            return Poll::Ready(());
                        }
                        this.inner = None;
                    }
                    Poll::Pending => return Poll::Pending,
                }
            } else {
                let now = Instant::now();
                if now >= this.deadline {
                    return Poll::Ready(());
                }

                let duration = this.deadline - now;
                let driver = context::current()
                    .driver()
                    .upgrade()
                    .expect("Runtime driver missing");

                let op = Op::new(OpTimeout { duration }).submit_local(driver);
                this.inner = Some(op);
            }
        }
    }
}
