use alloc::vec::Vec;
use core::time::Duration;

use crate::error::ConfigError;

const DEFAULT_BASE_TICK: Duration = Duration::from_millis(10);
const DEFAULT_LEVEL_SLOTS: &[usize] = &[512, 64, 64];
const MAX_LEVELS: usize = 8;
const MAX_LEVEL_SLOTS: usize = 1 << 16;
const MAX_TOTAL_SLOTS: usize = 300_000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WheelConfig {
    base_tick: Duration,
    base_tick_nanos: u64,
    levels: Vec<LevelConfig>,
    total_slots: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct LevelConfig {
    pub(crate) unit_ticks: u64,
    pub(crate) span_ticks: u64,
    pub(crate) slot_count: usize,
    pub(crate) mask: usize,
}

impl Default for WheelConfig {
    fn default() -> Self {
        let mut builder = Self::builder().base_tick(DEFAULT_BASE_TICK);
        for &slots in DEFAULT_LEVEL_SLOTS {
            builder = builder.level_slots(slots);
        }
        builder
            .build()
            .expect("default wheel configuration must be valid")
    }
}

impl WheelConfig {
    pub fn builder() -> WheelConfigBuilder {
        WheelConfigBuilder::default()
    }

    pub fn base_tick(&self) -> Duration {
        self.base_tick
    }

    pub fn level_count(&self) -> usize {
        self.levels.len()
    }

    pub fn total_slots(&self) -> usize {
        self.total_slots
    }

    pub(crate) fn base_tick_nanos(&self) -> u64 {
        self.base_tick_nanos
    }

    pub(crate) fn levels(&self) -> &[LevelConfig] {
        &self.levels
    }
}

#[derive(Debug, Clone)]
pub struct WheelConfigBuilder {
    base_tick: Duration,
    levels: Vec<usize>,
}

impl Default for WheelConfigBuilder {
    fn default() -> Self {
        Self {
            base_tick: DEFAULT_BASE_TICK,
            levels: Vec::new(),
        }
    }
}

impl WheelConfigBuilder {
    pub fn base_tick(mut self, tick: Duration) -> Self {
        self.base_tick = tick;
        self
    }

    pub fn level_slots(mut self, slots: usize) -> Self {
        self.levels.push(slots);
        self
    }

    pub fn build(self) -> Result<WheelConfig, ConfigError> {
        let base_tick_nanos = self
            .base_tick
            .as_nanos()
            .try_into()
            .map_err(|_| ConfigError::TickRangeOverflow)?;
        if base_tick_nanos == 0 {
            return Err(ConfigError::ZeroTick);
        }

        if self.levels.is_empty() || self.levels.len() > MAX_LEVELS {
            return Err(ConfigError::InvalidLevelCount {
                count: self.levels.len(),
                max: MAX_LEVELS,
            });
        }

        let mut total_slots = 0usize;
        let mut unit_ticks = 1u64;
        let mut derived = Vec::with_capacity(self.levels.len());
        for (level, &slot_count) in self.levels.iter().enumerate() {
            if slot_count < 2 || !slot_count.is_power_of_two() || slot_count > MAX_LEVEL_SLOTS {
                return Err(ConfigError::InvalidSlotCount { level, slot_count });
            }

            total_slots =
                total_slots
                    .checked_add(slot_count)
                    .ok_or(ConfigError::SlotBudgetExceeded {
                        total_slots: usize::MAX,
                        max: MAX_TOTAL_SLOTS,
                    })?;
            if total_slots > MAX_TOTAL_SLOTS {
                return Err(ConfigError::SlotBudgetExceeded {
                    total_slots,
                    max: MAX_TOTAL_SLOTS,
                });
            }

            let span_ticks = unit_ticks
                .checked_mul(slot_count as u64)
                .ok_or(ConfigError::DerivedRangeOverflow { level })?;
            derived.push(LevelConfig {
                unit_ticks,
                span_ticks,
                slot_count,
                mask: slot_count - 1,
            });
            unit_ticks = span_ticks;
        }

        Ok(WheelConfig {
            base_tick: self.base_tick,
            base_tick_nanos,
            levels: derived,
            total_slots,
        })
    }
}
