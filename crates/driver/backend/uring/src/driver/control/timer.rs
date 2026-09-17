use veloq_driver_core::driver::OpToken;
use veloq_std::{
    mem,
    time::{Duration, Instant},
    vec::Vec,
};
use veloq_wheel::{Expired, TimerError, TimerId, Wheel, WheelConfig};

pub(crate) struct UringTimerWheel {
    wheel: Wheel<OpToken>,
    timer_buffer: Vec<Expired<OpToken>>,
    last_poll: Instant,
}

pub(crate) struct ExpiredBatch {
    entries: Vec<Expired<OpToken>>,
    #[cfg(any(test, feature = "test-hooks"))]
    pending_prefix_len: usize,
}

impl ExpiredBatch {
    #[inline]
    pub(crate) fn len(&self) -> usize {
        self.entries.len()
    }

    #[inline]
    pub(crate) fn iter(&self) -> impl Iterator<Item = &Expired<OpToken>> {
        self.entries.iter()
    }

    #[cfg(any(test, feature = "test-hooks"))]
    #[inline]
    pub(crate) fn newly_expired_iter(&self) -> impl Iterator<Item = &Expired<OpToken>> {
        self.entries.iter().skip(self.pending_prefix_len)
    }

    fn into_entries(self) -> Vec<Expired<OpToken>> {
        self.entries
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
    pub(crate) fn next_deadline(&self) -> Result<Option<Duration>, TimerError> {
        self.wheel.next_deadline()
    }

    #[inline]
    pub(crate) fn cancel(&mut self, id: TimerId) {
        let _ = self.wheel.cancel(id);
    }

    pub(crate) fn insert(
        &mut self,
        token: OpToken,
        duration: Duration,
    ) -> Result<TimerId, TimerError> {
        self.wheel.insert(token, duration)
    }

    pub(crate) fn advance_timer_wheel(&mut self, now: Instant) -> Result<ExpiredBatch, TimerError> {
        let elapsed = now.saturating_duration_since(self.last_poll);
        let mut newly_expired = Vec::new();
        self.wheel.advance_by(elapsed, &mut newly_expired)?;
        let mut entries = mem::take(&mut self.timer_buffer);
        #[cfg(any(test, feature = "test-hooks"))]
        let pending_prefix_len = entries.len();
        entries.extend(newly_expired);
        self.last_poll = now;
        Ok(ExpiredBatch {
            entries,
            #[cfg(any(test, feature = "test-hooks"))]
            pending_prefix_len,
        })
    }

    /// Returns the expired scratch buffer while retaining entries that were over the current
    /// drive's timer budget for the next drive.
    pub(crate) fn recycle_expired(&mut self, mut batch: ExpiredBatch, processed: usize) {
        if processed != 0 {
            batch.entries.drain(..processed.min(batch.entries.len()));
        }
        self.timer_buffer = batch.into_entries();
    }

    #[inline]
    pub(crate) fn has_pending_expired(&self) -> bool {
        !self.timer_buffer.is_empty()
    }
}
