use std::time::Instant;

use veloq_wheel::{Wheel, WheelConfig};

pub(crate) struct TimerEngine {
    pub(crate) wheel: Wheel<usize>,
    pub(crate) buffer: Vec<usize>,
    pub(crate) last_poll: Instant,
}

impl TimerEngine {
    pub(crate) fn new() -> Self {
        Self {
            wheel: Wheel::new(WheelConfig::default()),
            buffer: Vec::new(),
            last_poll: Instant::now(),
        }
    }
}
