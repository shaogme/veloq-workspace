use super::sleep::{LocalSleep, Sleep, sleep_until, sleep_until_local};
use crate::runtime::context::Ctx;
use veloq_std::{
    future::poll_fn,
    pin::Pin,
    time::{Duration, Instant},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MissedTickBehavior {
    Burst,
    Delay,
    Skip,
}

// ============================================================================
// Sync/Send Interval
// ============================================================================

pub fn interval<'rt>(ctx: Ctx<'rt>, period: Duration) -> Interval<'rt> {
    interval_at(ctx, Instant::now(), period)
}

pub fn interval_at<'rt>(ctx: Ctx<'rt>, start: Instant, period: Duration) -> Interval<'rt> {
    Interval {
        period,
        next_tick: start,
        behavior: MissedTickBehavior::Burst,
        delay: sleep_until(ctx, start),
    }
}

pub struct Interval<'rt> {
    period: Duration,
    next_tick: Instant,
    behavior: MissedTickBehavior,
    delay: Sleep<'rt>,
}

impl<'rt> Interval<'rt> {
    pub fn set_missed_tick_behavior(&mut self, behavior: MissedTickBehavior) {
        self.behavior = behavior;
    }

    pub fn period(&self) -> Duration {
        self.period
    }

    pub async fn tick(&mut self) -> Instant {
        // Wait for current delay to complete
        poll_fn(|cx| Pin::new(&mut self.delay).poll(cx)).await;

        let now = Instant::now();
        let ticked = self.next_tick;

        // Calculate next
        match self.behavior {
            MissedTickBehavior::Burst => {
                self.next_tick += self.period;
            }
            MissedTickBehavior::Delay => {
                self.next_tick = now + self.period;
            }
            MissedTickBehavior::Skip => {
                self.next_tick += self.period;
                if self.period.is_zero() {
                    self.next_tick = now;
                } else if self.next_tick < now {
                    let remain = now - self.next_tick;
                    let skip = (remain.as_nanos() / self.period.as_nanos()) as u32;
                    self.next_tick += self.period * (skip + 1);
                }
            }
        }

        // Reset sleep
        self.delay.reset(self.next_tick);

        ticked
    }
}

// ============================================================================
// Local Interval
// ============================================================================

pub fn interval_local<'rt>(ctx: Ctx<'rt>, period: Duration) -> LocalInterval<'rt> {
    interval_at_local(ctx, Instant::now(), period)
}

pub fn interval_at_local<'rt>(
    ctx: Ctx<'rt>,
    start: Instant,
    period: Duration,
) -> LocalInterval<'rt> {
    LocalInterval {
        period,
        next_tick: start,
        behavior: MissedTickBehavior::Burst,
        delay: sleep_until_local(ctx, start),
    }
}

pub struct LocalInterval<'rt> {
    period: Duration,
    next_tick: Instant,
    behavior: MissedTickBehavior,
    delay: LocalSleep<'rt>,
}

impl<'rt> LocalInterval<'rt> {
    pub fn set_missed_tick_behavior(&mut self, behavior: MissedTickBehavior) {
        self.behavior = behavior;
    }

    pub fn period(&self) -> Duration {
        self.period
    }

    pub async fn tick(&mut self) -> Instant {
        // Wait for current delay to complete
        poll_fn(|cx| Pin::new(&mut self.delay).poll(cx)).await;

        let now = Instant::now();
        let ticked = self.next_tick;

        // Calculate next
        match self.behavior {
            MissedTickBehavior::Burst => {
                self.next_tick += self.period;
            }
            MissedTickBehavior::Delay => {
                self.next_tick = now + self.period;
            }
            MissedTickBehavior::Skip => {
                self.next_tick += self.period;
                if self.period.is_zero() {
                    self.next_tick = now;
                } else if self.next_tick < now {
                    let remain = now - self.next_tick;
                    let skip = (remain.as_nanos() / self.period.as_nanos()) as u32;
                    self.next_tick += self.period * (skip + 1);
                }
            }
        }

        // Reset sleep
        self.delay.reset(self.next_tick);

        ticked
    }
}
