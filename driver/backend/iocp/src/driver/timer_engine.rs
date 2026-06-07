use std::time::{Duration, Instant};

use veloq_wheel::{TaskId, Wheel, WheelConfig};

pub(crate) struct TimerEngine {
    wheel: Wheel<usize>,
    buffer: Vec<usize>,
    last_poll: Instant,
}

impl TimerEngine {
    pub(crate) fn new() -> Self {
        Self {
            wheel: Wheel::new(WheelConfig::default()),
            buffer: Vec::new(),
            last_poll: Instant::now(),
        }
    }

    pub(crate) fn wheel_mut(&mut self) -> &mut Wheel<usize> {
        &mut self.wheel
    }

    pub(crate) fn next_timeout(&self) -> Option<Duration> {
        self.wheel.next_timeout()
    }

    pub(crate) fn insert(&mut self, user_data: usize, duration: Duration) -> TaskId {
        self.wheel.insert(user_data, duration)
    }

    pub(crate) fn cancel(&mut self, id: TaskId) {
        self.wheel.cancel(id);
    }

    pub(crate) fn advance_to(&mut self, now: Instant) {
        let elapsed = now.saturating_duration_since(self.last_poll);
        self.wheel.advance(elapsed, &mut self.buffer);
        self.last_poll = now;
    }

    pub(crate) fn take_buffer(&mut self) -> Vec<usize> {
        std::mem::take(&mut self.buffer)
    }

    pub(crate) fn restore_cleared_buffer(&mut self, mut buffer: Vec<usize>) {
        buffer.clear();
        self.buffer = buffer;
    }
}
