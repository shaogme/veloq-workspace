use crate::runtime::context::Ctx;
use veloq_driver_native::{
    driver::{DriverRaw, PlatformDriver},
    op::{DetachedOp, LocalOp, Op, Timeout as OpTimeout},
};
use veloq_std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
    time::{Duration, Instant},
};

type SleepDetachedOp<'rt> = DetachedOp<OpTimeout, <PlatformDriver<'rt> as DriverRaw>::SlotSpec>;

// ============================================================================
// Sync/Send Sleep (uses DetachedOp)
// ============================================================================

/// Waits until `duration` has elapsed.
///
/// This future is `Send` and `Sync`.
pub fn sleep<'rt>(ctx: Ctx<'rt>, duration: Duration) -> Sleep<'rt> {
    sleep_until(ctx, Instant::now() + duration)
}

/// Waits until `deadline` is reached.
///
/// This future is `Send` and `Sync`.
pub fn sleep_until<'rt>(ctx: Ctx<'rt>, deadline: Instant) -> Sleep<'rt> {
    Sleep {
        ctx,
        deadline,
        inner: None,
    }
}

pub struct Sleep<'rt> {
    ctx: Ctx<'rt>,
    deadline: Instant,
    inner: Option<SleepDetachedOp<'rt>>,
}

impl<'rt> Sleep<'rt> {
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

impl<'rt> Future for Sleep<'rt> {
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
                let op = this.ctx.driver(|mut driver| {
                    Op::new(OpTimeout { duration }).submit_detached(&mut driver)
                });

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
pub fn sleep_local<'rt>(ctx: Ctx<'rt>, duration: Duration) -> LocalSleep<'rt> {
    sleep_until_local(ctx, Instant::now() + duration)
}

/// Waits until `deadline` is reached (Local version).
///
/// This future is `!Send`.
pub fn sleep_until_local<'rt>(ctx: Ctx<'rt>, deadline: Instant) -> LocalSleep<'rt> {
    LocalSleep {
        ctx,
        deadline,
        inner: None,
    }
}

pub struct LocalSleep<'rt> {
    ctx: Ctx<'rt>,
    deadline: Instant,
    inner: Option<LocalOp<'rt, OpTimeout, Ctx<'rt>>>,
}

impl<'rt> LocalSleep<'rt> {
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

impl<'rt> Future for LocalSleep<'rt> {
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
