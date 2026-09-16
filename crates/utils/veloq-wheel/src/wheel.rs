use alloc::vec::Vec;
use core::time::Duration;
use slotmap::{DefaultKey, SlotMap};

use crate::{config::WheelConfig, error::TimerError, id::TimerId, level::Level};

const SMALL_ADVANCE_LIMIT: u64 = 4096;

struct WheelEntry<T> {
    item: T,
    deadline_tick: u64,
    level: u8,
    slot: u32,
    prev: Option<DefaultKey>,
    next: Option<DefaultKey>,
}

pub struct Expired<T> {
    pub id: TimerId,
    pub item: T,
    pub deadline_tick: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AdvanceReport {
    pub elapsed_ticks: u64,
    pub expired_count: usize,
    pub fast_forwarded: bool,
}

pub struct Wheel<T> {
    tasks: SlotMap<DefaultKey, WheelEntry<T>>,
    levels: Vec<Level>,
    current_tick: u64,
    remainder_nanos: u64,
    base_tick_nanos: u64,
}

impl<T> Wheel<T> {
    pub fn new(config: WheelConfig) -> Self {
        let levels = config
            .levels()
            .iter()
            .map(|level| {
                Level::new(
                    level.unit_ticks,
                    level.span_ticks,
                    level.slot_count,
                    level.mask,
                )
            })
            .collect();
        Self {
            tasks: SlotMap::new(),
            levels,
            current_tick: 0,
            remainder_nanos: 0,
            base_tick_nanos: config.base_tick_nanos(),
        }
    }

    pub fn insert(&mut self, item: T, delay: Duration) -> Result<TimerId, TimerError> {
        let deadline_tick = self.deadline_after(delay)?;
        let key = self.tasks.insert(WheelEntry {
            item,
            deadline_tick,
            level: 0,
            slot: 0,
            prev: None,
            next: None,
        });
        let (level, slot) = self.determine_location(deadline_tick);
        if let Err(error) = self.link(key, level, slot) {
            self.tasks.remove(key);
            return Err(error);
        }
        self.debug_assert_invariants();
        Ok(TimerId::from_key(key))
    }

    pub fn reschedule(&mut self, id: TimerId, delay: Duration) -> Result<(), TimerError> {
        let deadline_tick = self.deadline_after(delay)?;
        let key = id.key();
        if !self.tasks.contains_key(key) {
            return Err(TimerError::StaleTimerId);
        }
        self.unlink(key)?;
        self.tasks
            .get_mut(key)
            .ok_or(TimerError::StaleTimerId)?
            .deadline_tick = deadline_tick;
        let (level, slot) = self.determine_location(deadline_tick);
        self.link(key, level, slot)?;
        self.debug_assert_invariants();
        Ok(())
    }

    pub fn cancel(&mut self, id: TimerId) -> Option<T> {
        let key = id.key();
        if !self.tasks.contains_key(key) {
            return None;
        }
        self.unlink(key).ok()?;
        let entry = self.tasks.remove(key)?;
        self.debug_assert_invariants();
        Some(entry.item)
    }

    pub fn advance_by(
        &mut self,
        elapsed: Duration,
        expired: &mut Vec<Expired<T>>,
    ) -> Result<AdvanceReport, TimerError> {
        let elapsed_nanos: u64 = elapsed
            .as_nanos()
            .try_into()
            .map_err(|_| TimerError::ElapsedOverflow)?;
        let total_nanos = self
            .remainder_nanos
            .checked_add(elapsed_nanos)
            .ok_or(TimerError::ElapsedOverflow)?;
        let elapsed_ticks = total_nanos / self.base_tick_nanos;
        let remainder_nanos = total_nanos % self.base_tick_nanos;
        let target_tick = self
            .current_tick
            .checked_add(elapsed_ticks)
            .ok_or(TimerError::ClockOverflow)?;

        self.remainder_nanos = remainder_nanos;
        let expired_start = expired.len();
        let fast_forwarded = elapsed_ticks > SMALL_ADVANCE_LIMIT;
        if fast_forwarded {
            self.fast_forward(target_tick, expired)?;
        } else {
            self.cascade_at_current_tick(expired)?;
            self.process_l0_slot(expired)?;
            while self.current_tick < target_tick {
                self.current_tick += 1;
                self.cascade_at_current_tick(expired)?;
                self.process_l0_slot(expired)?;
            }
        }
        self.debug_assert_invariants();
        Ok(AdvanceReport {
            elapsed_ticks,
            expired_count: expired.len() - expired_start,
            fast_forwarded,
        })
    }

    pub fn next_wakeup(&self) -> Option<Duration> {
        if self.tasks.is_empty() {
            return None;
        }

        let mut best_ticks = None;
        for (level_index, level) in self.levels.iter().enumerate() {
            let level_tick = self.current_tick / level.unit_ticks;
            let cursor = (level_tick as usize) & level.mask;
            let at_current_boundary = self.current_tick.is_multiple_of(level.unit_ticks);
            let current_slot = level.slots[cursor].head.is_some();
            let needs_processing_now = current_slot
                && at_current_boundary
                && (level_index == 0 || self.slot_can_move_down(level_index, cursor));

            let boundary_distance = if needs_processing_now {
                0
            } else {
                let Some(next_level_tick) = level_tick.checked_add(1) else {
                    best_ticks = Some(u64::MAX);
                    continue;
                };
                let next_cursor = (next_level_tick as usize) & level.mask;
                let Some((_, distance)) = level.next_occupied(next_cursor) else {
                    continue;
                };
                let Some(boundary_tick) = next_level_tick
                    .checked_add(distance as u64)
                    .and_then(|value| value.checked_mul(level.unit_ticks))
                else {
                    best_ticks = Some(u64::MAX);
                    continue;
                };
                boundary_tick.saturating_sub(self.current_tick)
            };
            best_ticks =
                Some(best_ticks.map_or(boundary_distance, |best: u64| best.min(boundary_distance)));
        }

        best_ticks.map(|ticks| self.duration_from_ticks(ticks))
    }

    pub fn tick_duration(&self) -> Duration {
        Duration::from_nanos(self.base_tick_nanos)
    }

    pub fn current_tick(&self) -> u64 {
        self.current_tick
    }

    pub fn len(&self) -> usize {
        self.tasks.len()
    }

    pub fn is_empty(&self) -> bool {
        self.tasks.is_empty()
    }

    pub fn clear(&mut self, out: &mut Vec<T>) {
        for level in &mut self.levels {
            level.clear_slots();
        }
        for (_, entry) in self.tasks.drain() {
            out.push(entry.item);
        }
        self.debug_assert_invariants();
    }

    fn deadline_after(&self, delay: Duration) -> Result<u64, TimerError> {
        let delay_ticks = if delay.is_zero() {
            0
        } else {
            let offset = delay.as_nanos() + u128::from(self.remainder_nanos);
            let ticks = offset.div_ceil(u128::from(self.base_tick_nanos));
            ticks.try_into().map_err(|_| TimerError::DelayOverflow)?
        };
        self.current_tick
            .checked_add(delay_ticks)
            .ok_or(TimerError::DelayOverflow)
    }

    fn determine_location(&self, deadline_tick: u64) -> (usize, usize) {
        let delta = deadline_tick.saturating_sub(self.current_tick);
        let mut selected = self.levels.len() - 1;
        for (index, level) in self.levels.iter().enumerate() {
            if delta < level.span_ticks {
                selected = index;
                break;
            }
        }
        let level = &self.levels[selected];
        let slot = ((deadline_tick / level.unit_ticks) as usize) & level.mask;
        (selected, slot)
    }

    fn link(
        &mut self,
        key: DefaultKey,
        level_index: usize,
        slot_index: usize,
    ) -> Result<(), TimerError> {
        let old_head = self.levels[level_index].slots[slot_index].head;
        if old_head.is_none() {
            let slot = &mut self.levels[level_index].slots[slot_index];
            slot.head = Some(key);
            slot.tail = Some(key);
            self.levels[level_index].set_occupied(slot_index);
            let entry = self
                .tasks
                .get_mut(key)
                .ok_or(TimerError::InvariantViolation)?;
            entry.level = level_index as u8;
            entry.slot = slot_index as u32;
            entry.prev = None;
            entry.next = None;
            return Ok(());
        }

        let old_tail = self.levels[level_index].slots[slot_index]
            .tail
            .ok_or(TimerError::InvariantViolation)?;
        self.tasks
            .get_mut(old_tail)
            .ok_or(TimerError::InvariantViolation)?
            .next = Some(key);
        let entry = self
            .tasks
            .get_mut(key)
            .ok_or(TimerError::InvariantViolation)?;
        entry.level = level_index as u8;
        entry.slot = slot_index as u32;
        entry.prev = Some(old_tail);
        entry.next = None;
        self.levels[level_index].slots[slot_index].tail = Some(key);
        self.levels[level_index].set_occupied(slot_index);
        Ok(())
    }

    fn unlink(&mut self, key: DefaultKey) -> Result<(), TimerError> {
        let (level_index, slot_index, prev, next) = {
            let entry = self.tasks.get(key).ok_or(TimerError::StaleTimerId)?;
            (
                usize::from(entry.level),
                entry.slot as usize,
                entry.prev,
                entry.next,
            )
        };
        if level_index >= self.levels.len() || slot_index >= self.levels[level_index].slots.len() {
            return Err(TimerError::InvariantViolation);
        }

        match prev {
            Some(prev) => {
                self.tasks
                    .get_mut(prev)
                    .ok_or(TimerError::InvariantViolation)?
                    .next = next;
            }
            None => self.levels[level_index].slots[slot_index].head = next,
        }
        match next {
            Some(next) => {
                self.tasks
                    .get_mut(next)
                    .ok_or(TimerError::InvariantViolation)?
                    .prev = prev;
            }
            None => self.levels[level_index].slots[slot_index].tail = prev,
        }
        if self.levels[level_index].slots[slot_index].head.is_none() {
            self.levels[level_index].clear_occupied(slot_index);
        }
        let entry = self
            .tasks
            .get_mut(key)
            .ok_or(TimerError::InvariantViolation)?;
        entry.prev = None;
        entry.next = None;
        Ok(())
    }

    fn process_l0_slot(&mut self, expired: &mut Vec<Expired<T>>) -> Result<(), TimerError> {
        let slot_index = (self.current_tick as usize) & self.levels[0].mask;
        self.process_slot(0, slot_index, expired)
    }

    fn cascade_at_current_tick(&mut self, expired: &mut Vec<Expired<T>>) -> Result<(), TimerError> {
        for level_index in (1..self.levels.len()).rev() {
            if self
                .current_tick
                .is_multiple_of(self.levels[level_index].unit_ticks)
            {
                let level_tick = self.current_tick / self.levels[level_index].unit_ticks;
                let slot = (level_tick as usize) & self.levels[level_index].mask;
                self.process_slot(level_index, slot, expired)?;
            }
        }
        Ok(())
    }

    fn process_slot(
        &mut self,
        level_index: usize,
        slot_index: usize,
        expired: &mut Vec<Expired<T>>,
    ) -> Result<(), TimerError> {
        let mut current = {
            let slot = &mut self.levels[level_index].slots[slot_index];
            let head = slot.head.take();
            slot.tail = None;
            self.levels[level_index].clear_occupied(slot_index);
            head
        };
        while let Some(key) = current {
            let next = self
                .tasks
                .get(key)
                .ok_or(TimerError::InvariantViolation)?
                .next;
            self.detach_entry(key)?;
            let deadline_tick = self
                .tasks
                .get(key)
                .ok_or(TimerError::InvariantViolation)?
                .deadline_tick;
            if deadline_tick <= self.current_tick {
                let entry = self
                    .tasks
                    .remove(key)
                    .ok_or(TimerError::InvariantViolation)?;
                expired.push(Expired {
                    id: TimerId::from_key(key),
                    item: entry.item,
                    deadline_tick,
                });
            } else {
                let (new_level, new_slot) = self.determine_location(deadline_tick);
                self.link(key, new_level, new_slot)?;
            }
            current = next;
        }
        Ok(())
    }

    fn detach_entry(&mut self, key: DefaultKey) -> Result<(), TimerError> {
        let entry = self
            .tasks
            .get_mut(key)
            .ok_or(TimerError::InvariantViolation)?;
        entry.prev = None;
        entry.next = None;
        Ok(())
    }

    fn fast_forward(
        &mut self,
        target_tick: u64,
        expired: &mut Vec<Expired<T>>,
    ) -> Result<(), TimerError> {
        let keys: Vec<DefaultKey> = self.tasks.keys().collect();
        for level in &mut self.levels {
            level.clear_slots();
        }
        self.current_tick = target_tick;
        for key in keys {
            let deadline_tick = self
                .tasks
                .get(key)
                .ok_or(TimerError::InvariantViolation)?
                .deadline_tick;
            if deadline_tick <= target_tick {
                let entry = self
                    .tasks
                    .remove(key)
                    .ok_or(TimerError::InvariantViolation)?;
                expired.push(Expired {
                    id: TimerId::from_key(key),
                    item: entry.item,
                    deadline_tick,
                });
            } else {
                let (level, slot) = self.determine_location(deadline_tick);
                self.link(key, level, slot)?;
            }
        }
        Ok(())
    }

    fn slot_can_move_down(&self, level_index: usize, slot_index: usize) -> bool {
        let mut key = self.levels[level_index].slots[slot_index].head;
        while let Some(current) = key {
            let Some(entry) = self.tasks.get(current) else {
                return false;
            };
            if entry.deadline_tick <= self.current_tick
                || entry.deadline_tick.saturating_sub(self.current_tick)
                    < self.levels[level_index - 1].span_ticks
            {
                return true;
            }
            key = entry.next;
        }
        false
    }

    fn duration_from_ticks(&self, ticks: u64) -> Duration {
        let nanos = u128::from(ticks) * u128::from(self.base_tick_nanos);
        let seconds = nanos / 1_000_000_000;
        let remainder = nanos % 1_000_000_000;
        if seconds > u128::from(u64::MAX) {
            Duration::from_secs(u64::MAX)
        } else {
            Duration::from_secs(seconds as u64) + Duration::from_nanos(remainder as u64)
        }
    }

    #[cfg(debug_assertions)]
    fn debug_assert_invariants(&self) {
        let mut linked = 0usize;
        for (level_index, level) in self.levels.iter().enumerate() {
            for (slot_index, slot) in level.slots.iter().enumerate() {
                let occupied = slot.head.is_some();
                debug_assert_eq!(occupied, slot.tail.is_some());
                debug_assert_eq!(occupied, level.is_occupied(slot_index));
                let mut key = slot.head;
                let mut prev = None;
                while let Some(current) = key {
                    let Some(entry) = self.tasks.get(current) else {
                        debug_assert!(false, "linked wheel entry is missing");
                        break;
                    };
                    debug_assert_eq!(entry.level, level_index as u8);
                    debug_assert_eq!(entry.slot, slot_index as u32);
                    debug_assert_eq!(entry.prev, prev);
                    if let Some(next) = entry.next {
                        debug_assert_eq!(
                            self.tasks.get(next).and_then(|item| item.prev),
                            Some(current)
                        );
                    } else {
                        debug_assert_eq!(slot.tail, Some(current));
                    }
                    prev = Some(current);
                    key = entry.next;
                    linked += 1;
                }
            }
        }
        debug_assert_eq!(linked, self.tasks.len());
    }

    #[cfg(not(debug_assertions))]
    fn debug_assert_invariants(&self) {}
}

#[cfg(test)]
mod tests {
    use alloc::{vec, vec::Vec};
    use core::time::Duration;

    use crate::{ConfigError, TimerError, WheelConfig};

    use super::{Expired, Wheel};

    fn config(base_tick: u64, slots: &[usize]) -> WheelConfig {
        let mut builder = WheelConfig::builder().base_tick(Duration::from_nanos(base_tick));
        for &slot_count in slots {
            builder = builder.level_slots(slot_count);
        }
        builder.build().expect("test wheel configuration")
    }

    fn expired_items(expired: &[Expired<&'static str>]) -> Vec<&'static str> {
        expired.iter().map(|entry| entry.item).collect()
    }

    #[test]
    fn validates_builder_and_derives_aligned_levels() {
        assert_eq!(
            WheelConfig::builder()
                .base_tick(Duration::ZERO)
                .level_slots(4)
                .build(),
            Err(ConfigError::ZeroTick)
        );
        assert!(matches!(
            WheelConfig::builder().build(),
            Err(ConfigError::InvalidLevelCount { .. })
        ));
        assert!(matches!(
            WheelConfig::builder().level_slots(3).build(),
            Err(ConfigError::InvalidSlotCount { level: 0, .. })
        ));
        assert!(matches!(
            WheelConfig::builder()
                .level_slots(1 << 16)
                .level_slots(1 << 16)
                .level_slots(1 << 16)
                .level_slots(1 << 16)
                .level_slots(2)
                .build(),
            Err(ConfigError::DerivedRangeOverflow { level: 3 })
        ));

        let config = config(10_000_000, &[4, 8, 2]);
        assert_eq!(config.base_tick(), Duration::from_millis(10));
        assert_eq!(config.level_count(), 3);
        assert_eq!(config.total_slots(), 14);
    }

    #[test]
    fn rounds_delays_up_without_early_expiration() {
        let mut wheel = Wheel::new(config(10_000_000, &[16, 4]));
        for (item, delay) in [
            ("1ms", 1),
            ("9ms", 9),
            ("10ms", 10),
            ("19ms", 19),
            ("20ms", 20),
        ] {
            wheel
                .insert(item, Duration::from_millis(delay))
                .expect("timer insertion");
        }

        let mut expired = Vec::new();
        wheel
            .advance_by(Duration::from_millis(10), &mut expired)
            .expect("wheel advance");
        assert_eq!(expired_items(&expired), vec!["1ms", "9ms", "10ms"]);
        expired.clear();
        wheel
            .advance_by(Duration::from_millis(9), &mut expired)
            .expect("wheel advance");
        assert!(expired.is_empty());
        wheel
            .advance_by(Duration::from_millis(1), &mut expired)
            .expect("wheel advance");
        assert_eq!(expired_items(&expired), vec!["19ms", "20ms"]);
    }

    #[test]
    fn accumulates_sub_tick_time_and_expires_zero_delay() {
        let mut wheel = Wheel::new(config(10_000_000, &[16]));
        wheel
            .insert("delayed", Duration::from_millis(10))
            .expect("timer insertion");
        let mut expired = Vec::new();
        for _ in 0..9 {
            wheel
                .advance_by(Duration::from_millis(1), &mut expired)
                .expect("wheel advance");
        }
        assert!(expired.is_empty());
        wheel
            .advance_by(Duration::from_millis(1), &mut expired)
            .expect("wheel advance");
        assert_eq!(expired_items(&expired), vec!["delayed"]);

        wheel
            .insert("immediate", Duration::ZERO)
            .expect("timer insertion");
        expired.clear();
        wheel
            .advance_by(Duration::ZERO, &mut expired)
            .expect("zero advance");
        assert_eq!(expired_items(&expired), vec!["immediate"]);
    }

    #[test]
    fn cancel_and_reschedule_are_immediate_and_generation_safe() {
        let mut wheel = Wheel::new(config(10_000_000, &[4, 4, 4]));
        let canceled = wheel
            .insert("canceled", Duration::from_secs(10))
            .expect("timer insertion");
        assert_eq!(wheel.len(), 1);
        assert_eq!(wheel.cancel(canceled), Some("canceled"));
        assert_eq!(wheel.len(), 0);
        assert!(wheel.is_empty());
        assert!(wheel.next_wakeup().is_none());
        assert_eq!(wheel.cancel(canceled), None);

        let reused = wheel
            .insert("reused", Duration::from_secs(10))
            .expect("timer insertion");
        assert_eq!(wheel.cancel(canceled), None);
        assert_eq!(wheel.len(), 1);
        wheel
            .reschedule(reused, Duration::from_millis(10))
            .expect("timer reschedule");
        let mut expired = Vec::new();
        wheel
            .advance_by(Duration::from_millis(10), &mut expired)
            .expect("wheel advance");
        assert_eq!(expired_items(&expired), vec!["reused"]);
        assert_eq!(
            wheel.reschedule(reused, Duration::ZERO),
            Err(TimerError::StaleTimerId)
        );
    }

    #[test]
    fn cascades_multiple_levels_and_handles_rounds() {
        let mut wheel = Wheel::new(config(10_000_000, &[4, 4, 4]));
        wheel
            .insert("level-one", Duration::from_millis(150))
            .expect("timer insertion");
        wheel
            .insert("level-two-round", Duration::from_millis(700))
            .expect("timer insertion");
        let mut expired = Vec::new();
        wheel
            .advance_by(Duration::from_millis(149), &mut expired)
            .expect("wheel advance");
        assert!(expired.is_empty());
        wheel
            .advance_by(Duration::from_millis(1), &mut expired)
            .expect("wheel advance");
        assert_eq!(expired_items(&expired), vec!["level-one"]);
        expired.clear();
        wheel
            .advance_by(Duration::from_millis(549), &mut expired)
            .expect("wheel advance");
        assert!(expired.is_empty());
        wheel
            .advance_by(Duration::from_millis(1), &mut expired)
            .expect("wheel advance");
        assert_eq!(expired_items(&expired), vec!["level-two-round"]);
    }

    #[test]
    fn fast_forwards_large_jumps_and_keeps_future_timers() {
        let mut wheel = Wheel::new(config(10_000_000, &[4, 4, 4]));
        wheel
            .insert("expired", Duration::from_secs(1))
            .expect("timer insertion");
        wheel
            .insert("future", Duration::from_secs(1_000))
            .expect("timer insertion");
        let mut expired = Vec::new();
        let report = wheel
            .advance_by(Duration::from_secs(100), &mut expired)
            .expect("wheel advance");
        assert!(report.fast_forwarded);
        assert_eq!(expired_items(&expired), vec!["expired"]);
        assert_eq!(wheel.len(), 1);
        assert!(wheel.next_wakeup().is_some());
    }

    #[test]
    fn next_wakeup_is_only_a_boundary_hint() {
        let mut wheel = Wheel::new(config(10_000_000, &[4, 4]));
        wheel
            .insert("upper", Duration::from_millis(50))
            .expect("timer insertion");
        assert_eq!(wheel.next_wakeup(), Some(Duration::from_millis(40)));
        let mut expired = Vec::new();
        wheel
            .advance_by(Duration::from_millis(40), &mut expired)
            .expect("wheel advance");
        assert!(expired.is_empty());
        assert_eq!(wheel.next_wakeup(), Some(Duration::from_millis(10)));
        wheel
            .advance_by(Duration::ZERO, &mut expired)
            .expect("zero advance");
        assert!(expired.is_empty());
        wheel
            .advance_by(Duration::from_millis(10), &mut expired)
            .expect("wheel advance");
        assert_eq!(expired_items(&expired), vec!["upper"]);
    }

    #[test]
    fn advance_overflow_does_not_change_clock() {
        let mut wheel: Wheel<()> = Wheel::new(config(1, &[4]));
        let mut expired = Vec::new();
        wheel
            .advance_by(Duration::from_nanos(u64::MAX - 1), &mut expired)
            .expect("wheel advance");
        assert_eq!(wheel.current_tick(), u64::MAX - 1);
        wheel
            .advance_by(Duration::from_nanos(1), &mut expired)
            .expect("wheel may advance to the maximum tick");
        assert_eq!(
            wheel.advance_by(Duration::from_nanos(1), &mut expired),
            Err(TimerError::ClockOverflow)
        );
        assert_eq!(wheel.current_tick(), u64::MAX);
    }
}
