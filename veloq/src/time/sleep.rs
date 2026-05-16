use crate::runtime::context::RuntimeContext;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use veloq_driver_native::driver::{Driver, PlatformDriver};
use veloq_driver_native::op::{DetachedOp, LocalOp, Op, Timeout as OpTimeout};

// ============================================================================
// Sync/Send Sleep (uses DetachedOp)
// ============================================================================

/// Waits until `duration` has elapsed.
///
/// This future is `Send` and `Sync`.
pub fn sleep<'ctx>(ctx: RuntimeContext<'ctx>, duration: Duration) -> Sleep<'ctx> {
    sleep_until(ctx, Instant::now() + duration)
}

/// Waits until `deadline` is reached.
///
/// This future is `Send` and `Sync`.
pub fn sleep_until<'ctx>(ctx: RuntimeContext<'ctx>, deadline: Instant) -> Sleep<'ctx> {
    Sleep {
        ctx,
        deadline,
        inner: None,
    }
}

pub struct Sleep<'ctx> {
    ctx: RuntimeContext<'ctx>,
    deadline: Instant,
    inner: Option<
        DetachedOp<
            OpTimeout,
            <PlatformDriver<'ctx> as Driver<'ctx>>::Op,
            <PlatformDriver<'ctx> as Driver<'ctx>>::Completion,
        >,
    >,
}

impl<'ctx> Sleep<'ctx> {
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

impl<'ctx> Future for Sleep<'ctx> {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = &mut *self;
        loop {
            if let Some(ref mut op) = this.inner {
                let pinned = Pin::new(op);
                match pinned.poll(cx) {
                    Poll::Ready(_) => {
                        this.inner = None;
                        if Instant::now() >= this.deadline {
                            return Poll::Ready(());
                        }
                    }
                    Poll::Pending => return Poll::Pending,
                }
            } else {
                let now = Instant::now();
                if now >= this.deadline {
                    return Poll::Ready(());
                }

                let duration = this.deadline - now;
                let op = this
                    .ctx
                    .driver(|driver| Op::new(OpTimeout { duration }).submit_detached(driver));

                this.inner = Some(op);
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
pub fn sleep_local<'ctx>(ctx: RuntimeContext<'ctx>, duration: Duration) -> LocalSleep<'ctx> {
    sleep_until_local(ctx, Instant::now() + duration)
}

/// Waits until `deadline` is reached (Local version).
///
/// This future is `!Send`.
pub fn sleep_until_local<'ctx>(ctx: RuntimeContext<'ctx>, deadline: Instant) -> LocalSleep<'ctx> {
    LocalSleep {
        ctx,
        deadline,
        inner: None,
    }
}

pub struct LocalSleep<'ctx> {
    ctx: RuntimeContext<'ctx>,
    deadline: Instant,
    inner: Option<LocalOp<'ctx, OpTimeout, RuntimeContext<'ctx>>>,
}

impl<'ctx> LocalSleep<'ctx> {
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

impl<'ctx> Future for LocalSleep<'ctx> {
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
                let op = Op::new(OpTimeout { duration }).submit_local(this.ctx);
                this.inner = Some(op);
            }
        }
    }
}
