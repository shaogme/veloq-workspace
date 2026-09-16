use veloq_driver_core::driver::OpToken;
use veloq_std::{
    mem,
    time::{Duration, Instant},
    vec::Vec,
};
use veloq_wheel::{Wheel, WheelConfig};

pub(crate) struct UringTimerWheel {
    wheel: Wheel<OpToken>,
    timer_buffer: Vec<OpToken>,
    last_poll: Instant,
}

pub(crate) struct ExpiredBatch {
    tokens: Vec<OpToken>,
    #[cfg(any(test, feature = "test-hooks"))]
    pending_prefix_len: usize,
}

impl ExpiredBatch {
    #[inline]
    pub(crate) fn len(&self) -> usize {
        self.tokens.len()
    }

    #[inline]
    pub(crate) fn iter(&self) -> impl Iterator<Item = &OpToken> {
        self.tokens.iter()
    }

    #[cfg(any(test, feature = "test-hooks"))]
    #[inline]
    pub(crate) fn newly_expired_iter(&self) -> impl Iterator<Item = &OpToken> {
        self.tokens.iter().skip(self.pending_prefix_len)
    }

    fn into_tokens(self) -> Vec<OpToken> {
        self.tokens
    }
}

impl UringTimerWheel {
    pub(crate) fn new() -> Self {
        Self {
            wheel: Wheel::new(WheelConfig::default()),
            timer_buffer: Vec::new(),
            last_poll: Instant::now(),
        }
    }

    #[inline]
    pub(crate) fn wheel_mut(&mut self) -> &mut Wheel<OpToken> {
        &mut self.wheel
    }

    #[inline]
    pub(crate) fn next_timeout(&self) -> Option<Duration> {
        self.wheel.next_timeout().map(|timeout| {
            timeout.saturating_sub(Instant::now().saturating_duration_since(self.last_poll))
        })
    }

    #[inline]
    pub(crate) fn cancel(&mut self, tid: veloq_wheel::TaskId) {
        self.wheel.cancel(tid);
    }

    pub(crate) fn advance_timer_wheel(&mut self, now: Instant) -> ExpiredBatch {
        let elapsed = now.saturating_duration_since(self.last_poll);
        let tick_ms = (self.wheel.tick_duration().as_millis() as u64).max(1);
        let elapsed_ticks = elapsed.as_millis() as u64 / tick_ms;
        if elapsed_ticks > 0 || !self.timer_buffer.is_empty() {
            self.last_poll += Duration::from_millis(elapsed_ticks * tick_ms);
            let mut expired = mem::take(&mut self.timer_buffer);
            #[cfg(any(test, feature = "test-hooks"))]
            let pending_prefix_len = expired.len();
            if elapsed_ticks > 0 {
                self.wheel.advance(elapsed, &mut expired);
            }
            ExpiredBatch {
                tokens: expired,
                #[cfg(any(test, feature = "test-hooks"))]
                pending_prefix_len,
            }
        } else {
            ExpiredBatch {
                tokens: Vec::new(),
                #[cfg(any(test, feature = "test-hooks"))]
                pending_prefix_len: 0,
            }
        }
    }

    /// Returns the expired scratch buffer while retaining entries that were over the current
    /// drive's timer budget for the next drive.
    pub(crate) fn recycle_expired(&mut self, mut batch: ExpiredBatch, processed: usize) {
        if processed != 0 {
            batch.tokens.drain(..processed.min(batch.tokens.len()));
        }
        self.timer_buffer = batch.into_tokens();
    }

    #[inline]
    pub(crate) fn has_pending_expired(&self) -> bool {
        !self.timer_buffer.is_empty()
    }
}
