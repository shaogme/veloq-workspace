use core::{error::Error, fmt};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfigError {
    ZeroTick,
    TickRangeOverflow,
    InvalidLevelCount { count: usize, max: usize },
    InvalidSlotCount { level: usize, slot_count: usize },
    SlotBudgetExceeded { total_slots: usize, max: usize },
    DerivedRangeOverflow { level: usize },
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroTick => f.write_str("base tick must be greater than zero"),
            Self::TickRangeOverflow => {
                f.write_str("base tick is too large to represent in nanoseconds")
            }
            Self::InvalidLevelCount { count, max } => {
                write!(f, "invalid level count {count}; expected 1..={max}")
            }
            Self::InvalidSlotCount { level, slot_count } => write!(
                f,
                "invalid slot count {slot_count} for level {level}; expected a power of two in 2..={}",
                1 << 16
            ),
            Self::SlotBudgetExceeded { total_slots, max } => {
                write!(f, "slot budget exceeded: {total_slots} > {max}")
            }
            Self::DerivedRangeOverflow { level } => {
                write!(f, "derived tick range overflowed at level {level}")
            }
        }
    }
}

impl Error for ConfigError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimerError {
    DelayOverflow,
    ElapsedOverflow,
    ClockOverflow,
    DeadlineOverflow,
    StaleTimerId,
    InvariantViolation,
}

impl fmt::Display for TimerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let message = match self {
            Self::DelayOverflow => "timer delay cannot be represented by the wheel clock",
            Self::ElapsedOverflow => "elapsed duration cannot be represented by the wheel clock",
            Self::ClockOverflow => "wheel clock would overflow its u64 tick range",
            Self::DeadlineOverflow => "timer deadline cannot be represented as a Duration",
            Self::StaleTimerId => "timer ID is stale or does not belong to this wheel",
            Self::InvariantViolation => "timing wheel invariant was violated",
        };
        f.write_str(message)
    }
}

impl Error for TimerError {}
