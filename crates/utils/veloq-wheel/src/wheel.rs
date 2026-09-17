use alloc::vec::Vec;
use core::time::Duration;
use slotmap::{DefaultKey, Key, SlotMap};

use crate::{
    config::WheelConfig, deadline::DeadlineHeap, error::TimerError, id::TimerId, level::Level,
};

const SMALL_ADVANCE_LIMIT: u64 = 4096;

struct WheelEntry<T> {
    item: T,
    deadline_tick: u64,
    level: u8,
    slot: u32,
    prev: Option<DefaultKey>,
    next: Option<DefaultKey>,
    heap_index: usize,
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
    deadline_heap: DeadlineHeap,
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
            deadline_heap: DeadlineHeap::new(),
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
            heap_index: 0,
        });
        let (level, slot) = self.determine_location(deadline_tick);
        if let Err(error) = self.link(key, level, slot) {
            self.tasks.remove(key);
            return Err(error);
        }
        if let Err(error) = self.insert_deadline(key, deadline_tick) {
            let _ = self.unlink(key);
            self.tasks.remove(key);
            return Err(error);
        }
        self.debug_assert_invariants();
        Ok(TimerId::from_key(key))
    }

    pub fn reschedule(&mut self, id: TimerId, delay: Duration) -> Result<(), TimerError> {
        let key = id.key();
        if !self.tasks.contains_key(key) {
            return Err(TimerError::StaleTimerId);
        }
        let deadline_tick = self.deadline_after(delay)?;
        self.unlink(key)?;
        self.tasks
            .get_mut(key)
            .ok_or(TimerError::StaleTimerId)?
            .deadline_tick = deadline_tick;
        let (level, slot) = self.determine_location(deadline_tick);
        self.link(key, level, slot)?;
        let heap_index = self
            .tasks
            .get(key)
            .ok_or(TimerError::InvariantViolation)?
            .heap_index;
        self.update_deadline(key, heap_index, deadline_tick)?;
        self.debug_assert_invariants();
        Ok(())
    }

    pub fn cancel(&mut self, id: TimerId) -> Option<T> {
        let key = id.key();
        if !self.tasks.contains_key(key) {
            return None;
        }
        self.unlink(key).ok()?;
        let heap_index = self.tasks.get(key)?.heap_index;
        self.remove_deadline(key, heap_index).ok()?;
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

    pub fn next_deadline(&self) -> Result<Option<Duration>, TimerError> {
        let Some(node) = self.deadline_heap.peek() else {
            return if self.tasks.is_empty() {
                Ok(None)
            } else {
                Err(TimerError::InvariantViolation)
            };
        };
        if self
            .tasks
            .get(node.key)
            .is_none_or(|entry| entry.deadline_tick != node.deadline_tick)
        {
            return Err(TimerError::InvariantViolation);
        }
        self.duration_until_tick(node.deadline_tick).map(Some)
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
        self.deadline_heap.clear();
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
                let heap_index = self
                    .tasks
                    .get(key)
                    .ok_or(TimerError::InvariantViolation)?
                    .heap_index;
                self.remove_deadline(key, heap_index)?;
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
                let heap_index = self
                    .tasks
                    .get(key)
                    .ok_or(TimerError::InvariantViolation)?
                    .heap_index;
                self.remove_deadline(key, heap_index)?;
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

    fn insert_deadline(&mut self, key: DefaultKey, deadline_tick: u64) -> Result<(), TimerError> {
        let (tasks, deadline_heap) = (&mut self.tasks, &mut self.deadline_heap);
        deadline_heap.insert(key, deadline_tick, |key, index| {
            tasks
                .get_mut(key)
                .map(|entry| entry.heap_index = index)
                .ok_or(TimerError::InvariantViolation)
        })
    }

    fn update_deadline(
        &mut self,
        key: DefaultKey,
        heap_index: usize,
        deadline_tick: u64,
    ) -> Result<(), TimerError> {
        let (tasks, deadline_heap) = (&mut self.tasks, &mut self.deadline_heap);
        deadline_heap.update(key, heap_index, deadline_tick, |key, index| {
            tasks
                .get_mut(key)
                .map(|entry| entry.heap_index = index)
                .ok_or(TimerError::InvariantViolation)
        })
    }

    fn remove_deadline(&mut self, key: DefaultKey, heap_index: usize) -> Result<(), TimerError> {
        let (tasks, deadline_heap) = (&mut self.tasks, &mut self.deadline_heap);
        deadline_heap.remove(key, heap_index, |key, index| {
            tasks
                .get_mut(key)
                .map(|entry| entry.heap_index = index)
                .ok_or(TimerError::InvariantViolation)
        })
    }

    fn duration_until_tick(&self, deadline_tick: u64) -> Result<Duration, TimerError> {
        if deadline_tick <= self.current_tick {
            return Ok(Duration::ZERO);
        }
        let ticks = deadline_tick - self.current_tick;
        let nanos = u128::from(ticks)
            .checked_mul(u128::from(self.base_tick_nanos))
            .ok_or(TimerError::DeadlineOverflow)?
            .checked_sub(u128::from(self.remainder_nanos))
            .ok_or(TimerError::DeadlineOverflow)?;
        let seconds = nanos / 1_000_000_000;
        let remainder = nanos % 1_000_000_000;
        let seconds = u64::try_from(seconds).map_err(|_| TimerError::DeadlineOverflow)?;
        Ok(Duration::new(seconds, remainder as u32))
    }

    #[cfg(debug_assertions)]
    fn debug_assert_invariants(&self) {
        self.debug_assert_level_invariants();
        self.debug_assert_deadline_invariants();
    }

    #[cfg(debug_assertions)]
    fn debug_assert_level_invariants(&self) {
        let mut linked = 0usize;
        for (level_index, level) in self.levels.iter().enumerate() {
            for (slot_index, slot) in level.slots.iter().enumerate() {
                let occupied = slot.head.is_some();
                debug_assert_eq!(occupied, slot.tail.is_some());
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

    #[cfg(debug_assertions)]
    fn debug_assert_deadline_invariants(&self) {
        debug_assert_eq!(self.deadline_heap.len(), self.tasks.len());
        for index in 0..self.deadline_heap.len() {
            let Some(node) = self.deadline_heap.node(index) else {
                debug_assert!(false, "deadline heap node is missing");
                continue;
            };
            let Some(entry) = self.tasks.get(node.key) else {
                debug_assert!(false, "deadline heap entry is missing");
                continue;
            };
            debug_assert_eq!(entry.heap_index, index);
            debug_assert_eq!(entry.deadline_tick, node.deadline_tick);
            if index != 0 {
                let parent = (index - 1) / 2;
                let Some(parent_node) = self.deadline_heap.node(parent) else {
                    debug_assert!(false, "deadline heap parent is missing");
                    continue;
                };
                debug_assert!(
                    (parent_node.deadline_tick, parent_node.key.data().as_ffi())
                        <= (node.deadline_tick, node.key.data().as_ffi())
                );
            }
        }
        for (key, entry) in &self.tasks {
            let Some(node) = self.deadline_heap.node(entry.heap_index) else {
                debug_assert!(false, "task points to a missing deadline heap node");
                continue;
            };
            debug_assert_eq!(node.key, key);
            debug_assert_eq!(node.deadline_tick, entry.deadline_tick);
        }
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
        assert_eq!(wheel.next_deadline(), Ok(None));
        assert_eq!(wheel.cancel(canceled), None);
        assert_eq!(
            wheel.reschedule(canceled, Duration::MAX),
            Err(TimerError::StaleTimerId)
        );

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
        assert_eq!(wheel.next_deadline(), Ok(Some(Duration::from_secs(900))));
    }

    #[test]
    fn next_deadline_returns_the_global_earliest_deadline() {
        let mut wheel = Wheel::new(config(10_000_000, &[4, 4]));
        wheel
            .insert("upper", Duration::from_millis(50))
            .expect("timer insertion");
        assert_eq!(wheel.next_deadline(), Ok(Some(Duration::from_millis(50))));
        let mut expired = Vec::new();
        wheel
            .advance_by(Duration::from_millis(40), &mut expired)
            .expect("wheel advance");
        assert!(expired.is_empty());
        assert_eq!(wheel.next_deadline(), Ok(Some(Duration::from_millis(10))));
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
    fn next_deadline_subtracts_sub_tick_remainder() {
        let mut wheel = Wheel::new(config(10_000_000, &[4]));
        wheel
            .insert("deadline", Duration::from_millis(20))
            .expect("timer insertion");
        let mut expired = Vec::new();
        wheel
            .advance_by(Duration::from_millis(5), &mut expired)
            .expect("wheel advance");
        assert!(expired.is_empty());
        assert_eq!(wheel.next_deadline(), Ok(Some(Duration::from_millis(15))));
        wheel
            .advance_by(Duration::from_millis(1), &mut expired)
            .expect("wheel advance");
        assert_eq!(wheel.next_deadline(), Ok(Some(Duration::from_millis(14))));
    }

    #[test]
    fn next_deadline_switches_root_after_insert_reschedule_and_cancel() {
        let mut wheel = Wheel::new(config(10_000_000, &[4, 4, 4]));
        let later = wheel
            .insert("later", Duration::from_millis(30))
            .expect("timer insertion");
        let earliest = wheel
            .insert("earliest", Duration::from_millis(10))
            .expect("timer insertion");
        assert_eq!(wheel.next_deadline(), Ok(Some(Duration::from_millis(10))));

        wheel
            .reschedule(earliest, Duration::from_millis(40))
            .expect("timer reschedule");
        assert_eq!(wheel.next_deadline(), Ok(Some(Duration::from_millis(30))));

        let newest = wheel
            .insert("newest", Duration::from_millis(10))
            .expect("timer insertion");
        assert_eq!(wheel.next_deadline(), Ok(Some(Duration::from_millis(10))));
        assert_eq!(wheel.cancel(newest), Some("newest"));
        assert_eq!(wheel.next_deadline(), Ok(Some(Duration::from_millis(30))));
        assert_eq!(wheel.cancel(later), Some("later"));
        assert_eq!(wheel.next_deadline(), Ok(Some(Duration::from_millis(40))));
    }

    #[test]
    fn next_deadline_returns_zero_without_expiring_timer() {
        let mut wheel = Wheel::new(config(10_000_000, &[4]));
        wheel
            .insert("immediate", Duration::ZERO)
            .expect("timer insertion");
        assert_eq!(wheel.next_deadline(), Ok(Some(Duration::ZERO)));
        let mut expired = Vec::new();
        assert_eq!(wheel.len(), 1);
        wheel
            .advance_by(Duration::ZERO, &mut expired)
            .expect("zero advance");
        assert_eq!(expired_items(&expired), vec!["immediate"]);
    }

    #[test]
    fn next_deadline_reports_duration_overflow_without_mutating_state() {
        let mut wheel = Wheel::new(config(u64::MAX, &[2]));
        wheel
            .insert("overflow", Duration::new(u64::MAX, 999_999_999))
            .expect("timer insertion");
        assert_eq!(wheel.next_deadline(), Err(TimerError::DeadlineOverflow));
        assert_eq!(wheel.len(), 1);
        assert!(!wheel.is_empty());
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
