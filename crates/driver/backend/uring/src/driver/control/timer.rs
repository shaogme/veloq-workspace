use veloq_driver_core::driver::OpToken;
use veloq_std::{
    time::{Duration, Instant},
    vec::Vec,
};
use veloq_wheel::{Wheel, WheelConfig};

pub(crate) struct UringTimerWheel {
    wheel: Wheel<OpToken>,
    timer_buffer: Vec<OpToken>,
    last_poll: Instant,
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

    pub(crate) fn advance_timer_wheel(&mut self, now: Instant) -> &[OpToken] {
        let elapsed = now.saturating_duration_since(self.last_poll);
        let tick_ms = (self.wheel.tick_duration().as_millis() as u64).max(1);
        let elapsed_ticks = elapsed.as_millis() as u64 / tick_ms;
        if elapsed_ticks > 0 {
            self.last_poll += Duration::from_millis(elapsed_ticks * tick_ms);
            self.timer_buffer.clear();
            self.wheel.advance(elapsed, &mut self.timer_buffer);
            &self.timer_buffer
        } else {
            &[]
        }
    }
}
