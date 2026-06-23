use crate::config::WheelConfig;
use crate::task::TaskId;
use slotmap::{DefaultKey, SlotMap};
use std::time::Duration;

/// Internal entry in the slotmap.
/// Acts as a node in a singly-linked list (lazy cancellation).
///
/// Slotmap 中的内部条目。
/// 作为单向链表中的一个节点（惰性取消）。
struct WheelEntry<T> {
    /// The actual item/data stored in the wheel.
    /// Wrapped in Option for lazy cancellation (take() leaves None).
    ///
    /// 时间轮中存储的实际项目/数据。
    /// 包装在 Option 中以支持惰性取消（take() 留下 None）。
    item: Option<T>,

    /// Absolute tick timestamp when this item expires.
    ///
    /// 该项目过期的绝对 tick 时间戳。
    deadline: u64,

    // Linked list pointers (Singly linked now)
    // 链表指针（现在是单向的）
    /// Key of the next entry in the list.
    ///
    /// 链表中后一个条目的 Key。
    next: Option<DefaultKey>,

    // Location for Head update
    // 用于 Head 更新的位置信息
    /// The hierarchy level this entry is currently in (0 or 1).
    ///
    /// 该条目当前所在的层级（0 或 1）。
    level: u8,

    /// The slot index within the level.
    ///
    /// 层级内的槽位索引。
    slot_index: u32,
}

/// Represents a single level in the hierarchical timing wheel.
///
/// 表示分层时间轮中的单个层级。
struct Level {
    /// Heads of the linked lists for each slot.
    /// Each slot contains the head key of a singly-linked list of tasks.
    ///
    /// 每个槽位的链表头。
    /// 每个槽位包含一个任务单向链表的头 Key。
    slots: Vec<Option<DefaultKey>>,

    /// Bitmask for fast modulo operations (slot_count - 1).
    ///
    /// 用于快速模运算的位掩码 (slot_count - 1)。
    mask: usize,
}

impl Level {
    /// Create a new level with the specified number of slots.
    ///
    /// 创建一个具有指定槽位数量的新层级。
    fn new(slot_count: usize) -> Self {
        // Initialize all slots to None (empty list)
        // 将所有槽位初始化为 None（空链表）
        let slots = vec![None; slot_count];
        Self {
            slots,
            mask: slot_count - 1,
        }
    }
}

/// A hierarchical timing wheel implementation.
/// Uses `SlotMap` for O(1) task addressing and singly-linked lists with lazy cancellation.
///
/// 分层时间轮实现。
/// 使用 `SlotMap` 进行 O(1) 的任务寻址，并使用具有惰性取消功能的单向链表。
pub struct Wheel<T> {
    /// Storage for tasks, providing stable keys (TaskId).
    ///
    /// 任务存储，提供稳定的 Key (TaskId)。
    tasks: SlotMap<DefaultKey, WheelEntry<T>>,

    /// Wheel levels (L0, L1).
    ///
    /// 时间轮层级 (L0, L1)。
    levels: Vec<Level>,

    /// Global tick count, strictly increasing.
    ///
    /// 全局 tick 计数，严格递增。
    global_tick: u64,

    /// L0 tick duration in milliseconds.
    ///
    /// L0 层 tick 的持续时间（毫秒）。
    tick_duration_ms: u64,

    /// Ratio of L1 tick to L0 tick (how many L0 ticks make one L1 tick).
    ///
    /// L1 tick 与 L0 tick 的比率（多少个 L0 tick 构成一个 L1 tick）。
    l1_tick_ratio: u64,

    /// Capacity of L0 in ticks (number of slots in L0).
    ///
    /// L0 的容量（以 tick 为单位，即 L0 的槽位数量）。
    l0_capacity_ticks: u64,
}

impl<T> Wheel<T> {
    /// Create a new Timing Wheel with the provided configuration.
    ///
    /// 使用提供的配置创建一个新的时间轮。
    pub fn new(config: WheelConfig) -> Self {
        let l0 = Level::new(config.l0_slot_count);
        let l1 = Level::new(config.l1_slot_count);

        let tick_duration_ms = config.l0_tick_duration.as_millis() as u64;
        let tick_duration_ms = tick_duration_ms.max(1);

        let l1_ms = config.l1_tick_duration.as_millis() as u64;
        let l1_tick_ratio = l1_ms / tick_duration_ms;
        let l0_capacity_ticks = config.l0_slot_count as u64;

        Self {
            tasks: SlotMap::new(),
            levels: vec![l0, l1],
            global_tick: 0,
            tick_duration_ms,
            l1_tick_ratio,
            l0_capacity_ticks,
        }
    }

    /// Convert a duration to a number of ticks based on L0 tick duration.
    ///
    /// 根据 L0 tick 持续时间将持续时间转换为 tick 数量。
    fn delay_to_ticks(&self, delay: Duration) -> u64 {
        let ms = delay.as_millis() as u64;
        ms / self.tick_duration_ms
    }

    /// Insert a task into the wheel with a specified delay.
    /// Returns a `TaskId` that can be used to cancel the task.
    ///
    /// 将任务以指定的延迟插入时间轮。
    /// 返回一个 `TaskId`，可用于取消该任务。
    pub fn insert(&mut self, item: T, delay: Duration) -> TaskId {
        let ticks = self.delay_to_ticks(delay).max(1);
        let deadline = self.global_tick.wrapping_add(ticks);
        let (level, slot_index) = self.determine_location(deadline);

        // Insert into SlotMap
        // 插入到 SlotMap 中
        let key = self.tasks.insert(WheelEntry {
            item: Some(item),
            deadline,
            next: None,
            level: level as u8,
            slot_index: slot_index as u32,
        });

        // Link into the determined slot
        // 链接到确定的槽位中
        self.link(key, level, slot_index);

        TaskId::from_key(key)
    }

    /// Cancel a task by its ID.
    /// Returns `Some(item)` if the task was found and canceled, or `None` otherwise.
    ///
    /// 通过 ID 取消任务。
    /// 如果找到并取消了任务，则返回 `Some(item)`，否则返回 `None`。
    pub fn cancel(&mut self, task_id: TaskId) -> Option<T> {
        let key = task_id.key();

        // Lazy cancellation: just take the item.
        // The entry remains in the slotmap until the wheel advances to its deadline.
        //
        // 惰性取消：只需取出 item。
        // 条目保留在 SlotMap 中，直到时间轮推进到其截止日期。
        self.tasks.get_mut(key).and_then(|entry| entry.item.take())
    }

    /// Advance the wheel by the specified elapsed time and populate `expired` with expired items.
    ///
    /// This method is optimized to handle large time jumps efficiently by
    /// batching L0 processing and jumping to L1 cascade points.
    ///
    /// 将时间轮推进指定的经过时间，并将过期的项目填充到 `expired` 中。
    ///
    /// 该方法经过优化，通过批量处理 L0 并跳转到 L1 级联点，可以高效地处理较大的时间跨度。
    pub fn advance(&mut self, elapsed: Duration, expired: &mut Vec<T>) {
        let ticks = self.delay_to_ticks(elapsed);

        if ticks == 0 {
            return;
        }

        let mut remaining_ticks = ticks;

        while remaining_ticks > 0 {
            // Determine distance to the next L1 cascade point.
            // Cascade happens when global_tick % l1_tick_ratio == 0.
            // We want the distance to the NEXT multiple of ratio.
            //
            // 确定到下一个 L1 级联点的距离。
            // 当 global_tick % l1_tick_ratio == 0 时发生级联。
            // 我们希望得到距离下一个比率倍数的距离。
            let current_mod = self.global_tick % self.l1_tick_ratio;
            let ticks_to_cascade = self.l1_tick_ratio - current_mod;

            // We can advance at most `remaining_ticks`, or until we hit the cascade point.
            // If ticks_to_cascade is small, we process L0 slots logic then cascade.
            //
            // 我们最多可以推进 `remaining_ticks`，或者直到遇到级联点。
            // 如果 ticks_to_cascade 很小，我们将处理 L0 槽位逻辑然后进行级联。
            let step = remaining_ticks.min(ticks_to_cascade);

            // Process L0 slots covered by this step: [global_tick + 1, global_tick + step]
            //
            // 处理此步骤覆盖的 L0 槽位：[global_tick + 1, global_tick + step]
            if step >= self.l0_capacity_ticks {
                // Optimization: If the step covers the entire L0 wheel (and more),
                // we just clear ALL slots in L0. L0 does not get refilled during this step (only at cascade).
                //
                // 优化：如果步骤覆盖了整个 L0 时间轮（甚至更多），
                // 我们只需清除 L0 中的所有槽位。在此步骤期间 L0 不会被重新填充（仅在级联时填充）。

                // Use explicit splitting of borrows to allow drain_list call
                // 使用显式借用拆分以允许 drain_list 调用
                let tasks = &mut self.tasks;
                for slot in &mut self.levels[0].slots {
                    if let Some(head) = slot.take() {
                        Self::drain_list(tasks, head, expired);
                    }
                }
            } else {
                // Process specific slots
                // 处理特定槽位
                for i in 1..=step {
                    let target_tick = self.global_tick.wrapping_add(i);
                    let idx = (target_tick as usize) & self.levels[0].mask;
                    if let Some(head) = self.levels[0].slots[idx].take() {
                        Self::drain_list(&mut self.tasks, head, expired);
                    }
                }
            }

            self.global_tick = self.global_tick.wrapping_add(step);
            remaining_ticks -= step;

            // Check if we hit a cascade point
            // 检查是否遇到级联点
            if self.global_tick.is_multiple_of(self.l1_tick_ratio) {
                let l1_tick = self.global_tick / self.l1_tick_ratio;
                let l1_idx = (l1_tick as usize) & self.levels[1].mask;

                if let Some(head) = self.levels[1].slots[l1_idx].take() {
                    self.cascade_list(head, expired);
                }
            }
        }
    }

    pub fn tick_duration(&self) -> Duration {
        Duration::from_millis(self.tick_duration_ms)
    }

    /// Calculate the duration until the next timed event triggers.
    /// Returns `None` if the wheel is empty.
    ///
    /// 计算距离下一个定时事件触发的持续时间。
    /// 如果时间轮为空，则返回 `None`。
    pub fn next_timeout(&self) -> Option<Duration> {
        if self.tasks.is_empty() {
            return None;
        }

        // Check L0
        // 检查 L0
        let l0_start = (self.global_tick as usize) & self.levels[0].mask;
        for i in 0..self.levels[0].slots.len() {
            let idx = (l0_start + i) & self.levels[0].mask;
            // Check if slot has a head
            // 检查槽位是否有头节点
            if self.levels[0].slots[idx].is_some() {
                return Some(Duration::from_millis(i as u64 * self.tick_duration_ms));
            }
        }

        // Check L1
        // 检查 L1
        let l1_tick = self.global_tick / self.l1_tick_ratio;
        let l1_start = (l1_tick as usize) & self.levels[1].mask;
        for i in 0..self.levels[1].slots.len() {
            let idx = (l1_start + i) & self.levels[1].mask;
            if self.levels[1].slots[idx].is_some() {
                // Approximate time
                // 近似时间
                let slot_start_tick = (l1_tick + i as u64) * self.l1_tick_ratio;
                let delay_ticks = slot_start_tick.saturating_sub(self.global_tick);
                return Some(Duration::from_millis(delay_ticks * self.tick_duration_ms));
            }
        }

        // Should basically not be reached if tasks is not empty, unless tasks are very far in future (overflow?).
        // Or perhaps in higher levels if we had them.
        // Fallback to max L0 capacity to ensure we wake up eventually.
        //
        // 如果 tasks 不为空，基本上不应达到此处，除非任务在很远的未来（溢出？）。
        // 或者如果我们有更高的层级。
        // 回退到最大 L0 容量以确保我们最终会唤醒。
        Some(Duration::from_millis(
            self.l0_capacity_ticks * self.tick_duration_ms,
        ))
    }

    // --- Internal Helpers (内部辅助函数) ---

    /// Determine which level and slot index a deadline belongs to.
    ///
    /// 确定截止日期属于哪个层级和槽位索引。
    fn determine_location(&self, deadline: u64) -> (usize, usize) {
        let remaining = deadline.wrapping_sub(self.global_tick);
        if remaining < self.l0_capacity_ticks {
            let idx = (deadline as usize) & self.levels[0].mask;
            (0, idx)
        } else {
            let l1_tick = deadline / self.l1_tick_ratio;
            let idx = (l1_tick as usize) & self.levels[1].mask;
            (1, idx)
        }
    }

    /// Add a key to the head of the linked list at the specified slot.
    ///
    /// 将 Key 添加到指定槽位的链表头。
    fn link(&mut self, key: DefaultKey, level: usize, slot_index: usize) {
        let old_head = self.levels[level].slots[slot_index];

        // key.next = old_head
        if let Some(entry) = self.tasks.get_mut(key) {
            entry.next = old_head;
            // Also ensure we update the location fields, as we might be re-linking
            // 还要确保更新位置字段，因为我们可能正在重新链接
            entry.level = level as u8;
            entry.slot_index = slot_index as u32;
        }

        // slot head = key
        // 槽位头 = key
        self.levels[level].slots[slot_index] = Some(key);
    }

    /// Drain a linked list starting at `head`, moving all items to `expired`.
    /// This is an associated function to allow borrow splitting.
    ///
    /// 从 `head` 开始清空链表，将所有项目移动到 `expired`。
    /// 这是一个关联函数，以允许借用拆分。
    fn drain_list(
        tasks: &mut SlotMap<DefaultKey, WheelEntry<T>>,
        head: DefaultKey,
        expired: &mut Vec<T>,
    ) {
        let mut current_opt = Some(head);
        while let Some(key) = current_opt {
            if let Some(mut entry) = tasks.remove(key) {
                // Only push if item is Some (not cancelled)
                // 仅当 item 为 Some（未取消）时才推送
                if let Some(item) = entry.item.take() {
                    expired.push(item);
                }
                current_opt = entry.next;
            } else {
                break;
            }
        }
    }

    /// Process a list from L1, either moving items to L0 or expiring them immediately.
    ///
    /// 处理 L1 中的链表，将项目移动到 L0 或立即过期。
    fn cascade_list(&mut self, head: DefaultKey, expired: &mut Vec<T>) {
        let mut current_opt = Some(head);
        while let Some(curr_key) = current_opt {
            let (next_key, deadline, is_cancelled) = {
                let entry = self
                    .tasks
                    .get(curr_key)
                    .expect("L1 Entry must exist during cascade");
                (entry.next, entry.deadline, entry.item.is_none())
            };

            if is_cancelled {
                // Remove ghost task
                // 移除幽灵任务
                self.tasks.remove(curr_key);
            } else {
                // Calculate new location
                // 计算新位置
                if deadline <= self.global_tick {
                    // Expired
                    // 已过期
                    if let Some(mut entry) = self.tasks.remove(curr_key)
                        && let Some(item) = entry.item.take()
                    {
                        expired.push(item);
                    }
                } else {
                    let (new_level, new_slot) = self.determine_location(deadline);
                    // We re-link this item.
                    // 重新链接此项目。
                    self.link(curr_key, new_level, new_slot);
                }
            }

            current_opt = next_key;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_insert_expire() {
        let config = WheelConfig::default();
        let mut wheel = Wheel::new(config);

        // Insert a task for 20ms (2 ticks with default 10ms tick)
        let id = wheel.insert("task1", Duration::from_millis(20));

        // Advance 10ms - nothing should expire
        let mut expired = Vec::new();
        wheel.advance(Duration::from_millis(10), &mut expired);
        assert!(expired.is_empty());

        // Advance another 10ms - should expire
        wheel.advance(Duration::from_millis(10), &mut expired);
        assert_eq!(expired.len(), 1);
        assert_eq!(expired[0], "task1");

        // Verify emptiness
        assert!(wheel.cancel(id).is_none());
    }

    #[test]
    fn test_cancel() {
        let config = WheelConfig::default();
        let mut wheel = Wheel::new(config);

        let id = wheel.insert("task1", Duration::from_millis(100));

        let checked = wheel.cancel(id);
        assert_eq!(checked, Some("task1"));

        // Advance past deadline, should get nothing
        let mut expired = Vec::new();
        wheel.advance(Duration::from_millis(200), &mut expired);
        assert!(expired.is_empty());
    }

    #[test]
    fn test_cascade_l1_to_l0() {
        let config = WheelConfig {
            l0_slot_count: 10, // Small L0 for easy cascade
            l0_tick_duration: Duration::from_millis(10),
            l1_slot_count: 4,
            l1_tick_duration: Duration::from_millis(100), // 10 * 10
        };

        let mut wheel: Wheel<&str> = Wheel::new(config);

        // L0 capacity is 10 * 10ms = 100ms.
        // Insert task for 150ms. Should go to L1.
        wheel.insert("long_task", Duration::from_millis(150));

        // Advance 90ms. Global tick: 9. L0 filled.
        let mut expired = Vec::new();
        wheel.advance(Duration::from_millis(90), &mut expired);
        assert!(expired.is_empty());

        // Advance 10ms. Global tick: 10. Cascade triggered (mod 10 == 0).
        // "long_task" deadline is 15 ticks. Current is 10.
        // It fits in L0 now? remaining = 5 ticks < 10. Yes.
        // It should be moved to L0.
        wheel.advance(Duration::from_millis(10), &mut expired);
        assert!(expired.is_empty()); // Still 50ms to go.

        // Advance 50ms. Global tick: 15. Expire.
        wheel.advance(Duration::from_millis(50), &mut expired);
        assert_eq!(expired.len(), 1);
        assert_eq!(expired[0], "long_task");
    }

    #[test]
    fn test_large_advance() {
        let config = WheelConfig {
            l0_slot_count: 4,
            l0_tick_duration: Duration::from_millis(10),
            l1_slot_count: 4,
            l1_tick_duration: Duration::from_millis(40),
        };

        let mut wheel = Wheel::new(config);

        wheel.insert("short", Duration::from_millis(10));
        wheel.insert("medium", Duration::from_millis(50));
        wheel.insert("long", Duration::from_millis(100));

        // Advance 200ms at once
        let mut expired = Vec::new();
        wheel.advance(Duration::from_millis(200), &mut expired);

        assert_eq!(expired.len(), 3);
        assert!(expired.contains(&"short"));
        assert!(expired.contains(&"medium"));
        assert!(expired.contains(&"long"));
    }

    #[test]
    fn test_next_timeout() {
        let config = WheelConfig {
            l0_tick_duration: Duration::from_millis(10),
            ..Default::default()
        };
        let mut wheel = Wheel::new(config);

        assert!(wheel.next_timeout().is_none());

        wheel.insert("task", Duration::from_millis(30));
        // Should be 30ms (approx)
        let t = wheel.next_timeout();
        assert_eq!(t, Some(Duration::from_millis(30)));

        let mut expired = Vec::new();
        wheel.advance(Duration::from_millis(10), &mut expired);
        let t = wheel.next_timeout();
        assert_eq!(t, Some(Duration::from_millis(20)));
    }

    #[test]
    fn test_heavy_overflow() {
        // Setup a wheel with small capacity to force overflow
        let config = WheelConfig {
            l0_slot_count: 16,
            l0_tick_duration: Duration::from_millis(10),
            l1_slot_count: 4,
            l1_tick_duration: Duration::from_millis(160), // 16 * 10ms
        };

        // Total L1 range = 4 * 160ms = 640ms.
        // If we insert something > 640ms, it wraps safely.

        let mut wheel = Wheel::new(config);

        // Task 1: 800ms.
        // L1 ticks = 800 / 160 = 5.
        // L1 index = 5 & 3 = 1.
        // Rounds = 1.
        let id1 = wheel.insert("overflow_on_round_1", Duration::from_millis(800));

        // Task 2: 1500ms
        // L1 ticks = 1500 / 160 = 9.
        // L1 index = 9 & 3 = 1.
        // Rounds = 2.
        let id2 = wheel.insert("overflow_on_round_2", Duration::from_millis(1500));

        // Advance 160ms (one L1 tick) -> Global tick 16.
        // Index 16/16 & 3 = 1. L1[1] processed.
        // Both tasks are in L1[1].
        // Task 1: target 80, current 16. Remaining 64. 64 > 16 (L0).
        // New L1 tick = 80 / 16 = 5. Index 1. Re-queued to L1[1].
        // Task 2: target 150, current 16. Remaining 134. Re-queued to L1[1].
        let mut expired = Vec::new();
        wheel.advance(Duration::from_millis(160), &mut expired);
        assert!(expired.is_empty());

        // Advance another 640ms (4 full L1 slots).
        // Total advanced: 800ms. Global tick 80.
        // Task 1 matches exactly?
        // At tick 80 (L1 tick 5, index 1).
        // Cascade L1[1]. Task 1 deadline 80. 80 <= 80. Expired!
        wheel.advance(Duration::from_millis(640), &mut expired);

        let has_overflow_1 = expired.contains(&"overflow_on_round_1");
        assert!(has_overflow_1, "Expected overflow_on_round_1 to expire");

        // Task 2 is at 1500ms, should not expire yet
        assert!(!expired.contains(&"overflow_on_round_2"));

        // Task 2 check
        // Current global tick 80. Task 2 deadline 150.
        // Remaining 70.
        // Advance 700ms. Total 1500.
        wheel.advance(Duration::from_millis(700), &mut expired);
        assert!(expired.contains(&"overflow_on_round_2"));

        assert!(wheel.cancel(id1).is_none());
        assert!(wheel.cancel(id2).is_none());
    }
}
