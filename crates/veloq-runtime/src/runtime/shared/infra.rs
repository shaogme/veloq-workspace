use crossbeam_deque::{Injector, Steal, Stealer, Worker};
use crossbeam_queue::ArrayQueue;
use std::{
    result::Result as StdResult,
    sync::Arc,
    sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    thread,
};
use veloq_std::panic::{AssertUnwindSafe, catch_unwind};
use veloq_storage::{AtomicOptionPtr, StateOptionPtr};

use crate::{
    error::{Result, RuntimeWakeError},
    runtime::{
        context::{IdleDecision, IdleWaitStrategy},
        primitives::{EventCount, ShutdownCoordinator, Unparker, WakeFailureState},
        shared::{RuntimeShared, worker_loop::LoopController},
    },
    task::{LocalTaskRef, SendTaskRef, TaskHeader},
    utils::FastRand,
};

pub(crate) struct WorkerQueue {
    pub(crate) remote_queue: ArrayQueue<SendTaskRef>,
    pub(crate) pinned_queue: ArrayQueue<SendTaskRef>,
    pub(crate) local_queue: ArrayQueue<LocalTaskRef>,
    local_capacity: usize,
    pub(crate) remote_count: AtomicUsize,
    pub(crate) pinned_count: AtomicUsize,
    pub(crate) local_count: AtomicUsize,
    /// LIFO slot for high-priority task (cache locality)
    pub(crate) lifo: AtomicOptionPtr<TaskHeader>,
    /// Stealer for work-stealing
    pub(crate) stealer: Stealer<SendTaskRef>,
}

impl WorkerQueue {
    pub(crate) fn new(
        remote_queue: ArrayQueue<SendTaskRef>,
        pinned_queue: ArrayQueue<SendTaskRef>,
        local_queue: ArrayQueue<LocalTaskRef>,
        local_capacity: usize,
        stealer: Stealer<SendTaskRef>,
    ) -> Self {
        Self {
            remote_queue,
            pinned_queue,
            local_queue,
            local_capacity,
            remote_count: AtomicUsize::new(0),
            pinned_count: AtomicUsize::new(0),
            local_count: AtomicUsize::new(0),
            lifo: AtomicOptionPtr::new(None),
            stealer,
        }
    }

    #[inline]
    pub(crate) fn local_capacity(&self) -> usize {
        self.local_capacity
    }
}

unsafe impl Send for WorkerQueue {}
unsafe impl Sync for WorkerQueue {}

pub(crate) struct NUMAGroup {
    pub(crate) worker_ids: Vec<usize>,
    pub(crate) idle_stack: IdleStack,
}

/// 每个 worker 在 idle 栈中的槽位。
///
/// `next` 是 Treiber 栈的后继索引；`in_stack` 标记该 worker 的条目当前是否**物理挂在**
/// 某个栈上。这个标记把「一个 worker 在栈中最多存在一个条目」变成不变式，因此
/// `next[worker]` 不会在条目还留在栈里时被下一次 `push` 覆盖 —— 覆盖正是链表成环、
/// `pop_idle` 死循环的根因。
pub(crate) struct IdleSlots {
    next: Box<[AtomicUsize]>,
    in_stack: AtomicBitset,
}

impl IdleSlots {
    pub(crate) fn new(worker_count: usize) -> Self {
        let mut next = Vec::with_capacity(worker_count);
        for _ in 0..worker_count {
            next.push(AtomicUsize::new(IdleStack::NO_NEXT));
        }
        Self {
            next: next.into_boxed_slice(),
            in_stack: AtomicBitset::new(worker_count),
        }
    }

    /// 声明「本 worker 的条目即将入栈」；条目已在栈中时返回 `false`。
    #[inline]
    fn try_enter(&self, worker_id: usize) -> bool {
        self.in_stack.try_set(worker_id)
    }

    /// 条目已被物理摘除。只允许摘链的一方调用。
    #[inline]
    fn mark_removed(&self, worker_id: usize) {
        self.in_stack.clear(worker_id);
    }
}

/// NUMA 组内的 idle worker 栈：Treiber 栈，节点就是 worker 自身。
pub(crate) struct IdleStack {
    /// 高 32 位是**单调递增**的 generation（防 ABA），低 32 位是栈顶 worker id。
    head: AtomicU64,
}

impl IdleStack {
    /// 低 32 位的空栈哨兵，同时限定了可编码的 worker id 上界。
    const EMPTY_ID: u32 = u32::MAX;
    /// 可编码的 worker 数量上界（不含）。
    pub(crate) const MAX_WORKERS: usize = Self::EMPTY_ID as usize;
    /// `IdleSlots::next` 中表示「无后继」。
    pub(crate) const NO_NEXT: usize = usize::MAX;

    pub(crate) fn new() -> Self {
        Self {
            head: AtomicU64::new(Self::pack(0, Self::EMPTY_ID)),
        }
    }

    #[inline]
    const fn pack(generation: u32, worker_id: u32) -> u64 {
        ((generation as u64) << 32) | worker_id as u64
    }

    #[inline]
    const fn head_id(head: u64) -> u32 {
        head as u32
    }

    #[inline]
    const fn head_generation(head: u64) -> u32 {
        (head >> 32) as u32
    }

    /// 计算摘除栈顶 `top_id` 后的新 head。generation 只递增、不因空栈复位，
    /// 避免重新打开 ABA 窗口。
    #[inline]
    fn head_after_removing(head: u64, slots: &IdleSlots, top_id: usize) -> u64 {
        let next = slots.next[top_id].load(Ordering::Acquire);
        let next_id = if next == Self::NO_NEXT {
            Self::EMPTY_ID
        } else {
            next as u32
        };
        Self::pack(Self::head_generation(head).wrapping_add(1), next_id)
    }

    pub(crate) fn push(&self, worker_id: usize, slots: &IdleSlots) {
        debug_assert!(
            worker_id < Self::MAX_WORKERS,
            "worker id {worker_id} 超出 idle 栈可编码范围"
        );
        if !slots.try_enter(worker_id) {
            // 条目仍在栈中（`leave_idle` 没能摘链留下的陈旧条目），直接复用。
            return;
        }

        let mut head = self.head.load(Ordering::Acquire);
        loop {
            let next = match Self::head_id(head) {
                Self::EMPTY_ID => Self::NO_NEXT,
                top_id => top_id as usize,
            };
            slots.next[worker_id].store(next, Ordering::Release);

            let new_head = Self::pack(
                Self::head_generation(head).wrapping_add(1),
                worker_id as u32,
            );
            match self.head.compare_exchange_weak(
                head,
                new_head,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return,
                Err(h) => head = h,
            }
        }
    }

    pub(crate) fn pop(&self, slots: &IdleSlots) -> Option<usize> {
        let mut head = self.head.load(Ordering::Acquire);
        loop {
            let worker_id = match Self::head_id(head) {
                Self::EMPTY_ID => return None,
                top_id => top_id as usize,
            };
            let new_head = Self::head_after_removing(head, slots, worker_id);
            match self.head.compare_exchange_weak(
                head,
                new_head,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    slots.mark_removed(worker_id);
                    return Some(worker_id);
                }
                Err(h) => head = h,
            }
        }
    }

    /// 仅当栈顶为 `worker_id` 时弹出（`leave_idle` 快路径）。
    pub(crate) fn try_pop_self(&self, worker_id: usize, slots: &IdleSlots) -> bool {
        let mut head = self.head.load(Ordering::Acquire);
        loop {
            if Self::head_id(head) as usize != worker_id {
                return false;
            }
            let new_head = Self::head_after_removing(head, slots, worker_id);
            match self.head.compare_exchange_weak(
                head,
                new_head,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    slots.mark_removed(worker_id);
                    return true;
                }
                Err(h) => head = h,
            }
        }
    }

    /// 弹出仍标记为 idle 的 worker；跳过栈中已失效（stale）的条目。
    ///
    /// 每次 `pop` 都会真正缩短栈（并清掉对应的 `in_stack` 位），所以循环必定终止。
    pub(crate) fn pop_idle(&self, idle_mask: &AtomicBitset, slots: &IdleSlots) -> Option<usize> {
        while let Some(worker_id) = self.pop(slots) {
            if idle_mask.is_set(worker_id) {
                return Some(worker_id);
            }
        }
        None
    }
}

pub(crate) struct AtomicBitset {
    bits: Box<[AtomicU64]>,
}

impl AtomicBitset {
    pub(crate) fn new(size: usize) -> Self {
        let num_u64 = size.div_ceil(64);
        let mut v = Vec::with_capacity(num_u64);
        for _ in 0..num_u64 {
            v.push(AtomicU64::new(0));
        }
        Self {
            bits: v.into_boxed_slice(),
        }
    }

    pub(crate) fn try_set(&self, index: usize) -> bool {
        let word = index / 64;
        let bit = index % 64;
        let mask = 1 << bit;
        let prev = self.bits[word].fetch_or(mask, Ordering::AcqRel);
        prev & mask == 0
    }

    pub(crate) fn clear(&self, index: usize) {
        let word = index / 64;
        let bit = index % 64;
        self.bits[word].fetch_and(!(1 << bit), Ordering::Release);
    }

    pub(crate) fn is_set(&self, index: usize) -> bool {
        let word = index / 64;
        let bit = index % 64;
        self.bits[word].load(Ordering::Acquire) & (1 << bit) != 0
    }
}

pub(crate) struct WorkerRegistry {
    pub(crate) workers: Box<[WorkerQueue]>,
    pub(crate) unparkers: Box<[Unparker]>,
    pub(crate) wake_failure: Arc<WakeFailureState>,
    pub(crate) shutdown: Arc<ShutdownCoordinator>,
}

impl WorkerRegistry {
    #[inline]
    pub(crate) fn unpark(&self, worker_id: usize) -> StdResult<(), RuntimeWakeError> {
        self.unparkers[worker_id].unpark()
    }
}

pub(crate) struct TopologyContext {
    pub(crate) groups: Vec<NUMAGroup>,
    pub(crate) worker_to_group: Vec<usize>,
    pub(crate) group_idle_slots: IdleSlots,
}

impl TopologyContext {
    pub(crate) fn from_worker_to_group(worker_to_group: &[usize]) -> Self {
        let worker_count = worker_to_group.len();
        let group_count = worker_to_group
            .iter()
            .copied()
            .max()
            .map_or(0, |group_idx| group_idx + 1);
        let mut worker_ids = vec![Vec::new(); group_count];
        for (worker_id, &group_idx) in worker_to_group.iter().enumerate() {
            worker_ids[group_idx].push(worker_id);
        }
        let groups = worker_ids
            .into_iter()
            .map(|worker_ids| NUMAGroup {
                worker_ids,
                idle_stack: IdleStack::new(),
            })
            .collect();
        Self {
            groups,
            worker_to_group: worker_to_group.to_vec(),
            group_idle_slots: IdleSlots::new(worker_count),
        }
    }

    pub(crate) fn choose_worker_with_current(
        &self,
        next_worker: &AtomicUsize,
        current: usize,
    ) -> usize {
        let n = self.worker_to_group.len();
        if n <= 1 {
            return 0;
        }

        if current < n {
            let group_idx = self.worker_to_group[current];
            let group = &self.groups[group_idx];
            if group.worker_ids.len() > 1 {
                let idx = next_worker.fetch_add(1, Ordering::Relaxed) % group.worker_ids.len();
                return group.worker_ids[idx];
            }
        }

        next_worker.fetch_add(1, Ordering::Relaxed) % n
    }
}

pub(crate) struct GlobalInjector {
    queue: Injector<SendTaskRef>,
    drained: AtomicBool,
    backlog: AtomicUsize,
}

impl GlobalInjector {
    pub(crate) fn new() -> Self {
        Self {
            queue: Injector::new(),
            drained: AtomicBool::new(false),
            backlog: AtomicUsize::new(0),
        }
    }

    pub(crate) fn push_global(&self, task: SendTaskRef) {
        self.backlog.fetch_add(1, Ordering::Release);
        self.queue.push(task);
    }

    pub(crate) fn pop(&self) -> Option<SendTaskRef> {
        loop {
            match self.queue.steal() {
                Steal::Success(task) => {
                    let previous = self.backlog.fetch_sub(1, Ordering::AcqRel);
                    debug_assert!(previous > 0, "global injector backlog underflow");
                    return Some(task);
                }
                Steal::Retry => continue,
                Steal::Empty => return None,
            }
        }
    }

    pub(crate) fn backlog(&self) -> usize {
        self.backlog.load(Ordering::Acquire)
    }

    /// Drain all task references still owned by the global injector exactly once.
    ///
    /// A panic from a task finalization callback is contained long enough to
    /// process the rest of the injector. The caller records the diagnostic and
    /// completes the shutdown phase after this method returns.
    pub(crate) fn drain_once(&self, mut f: impl FnMut(SendTaskRef)) -> (usize, bool) {
        if self.drained.swap(true, Ordering::AcqRel) {
            return (0, false);
        }

        let mut drained = 0;
        let mut panicked = false;
        while let Some(task) = self.pop() {
            drained += 1;
            if catch_unwind(AssertUnwindSafe::new(|| f(task))).is_err() {
                panicked = true;
            }
        }
        (drained, panicked)
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.backlog() == 0
    }
}

pub(crate) struct TaskScheduler {
    injector: GlobalInjector,
    pub(crate) next_worker: AtomicUsize,
}

impl TaskScheduler {
    pub(crate) fn new() -> Self {
        Self {
            injector: GlobalInjector::new(),
            next_worker: AtomicUsize::new(0),
        }
    }

    pub(super) fn push_global(&self, task: SendTaskRef) {
        self.injector.push_global(task);
    }

    pub(crate) fn pop_global(&self) -> Option<SendTaskRef> {
        self.injector.pop()
    }

    pub(crate) fn has_global_work(&self) -> bool {
        self.injector.backlog() > 0
    }

    pub(crate) fn global_queue_backlog(&self) -> usize {
        self.injector.backlog()
    }

    pub(crate) fn drain_global_once(&self, f: impl FnMut(SendTaskRef)) -> (usize, bool) {
        self.injector.drain_once(f)
    }

    pub(crate) fn is_global_empty(&self) -> bool {
        self.injector.is_empty()
    }

    pub(crate) fn steal_send(
        &self,
        thief_id: usize,
        registry: &WorkerRegistry,
        topo: &TopologyContext,
        rand: &FastRand,
        thief_worker: &Worker<SendTaskRef>,
    ) -> Option<SendTaskRef> {
        let num_workers = registry.workers.len();
        if num_workers <= 1 {
            return self.pop_global();
        }

        let group_idx = topo.worker_to_group[thief_id];
        let group = &topo.groups[group_idx];
        const MAX_STEAL_RETRIES: usize = 32;
        let mut retries = 0;

        loop {
            if retries >= MAX_STEAL_RETRIES {
                return self.pop_global();
            }

            let mut retry_steal = false;

            if group.worker_ids.len() > 1 {
                let start = rand.next_u32(group.worker_ids.len() as u32) as usize;

                for i in 0..group.worker_ids.len() {
                    let victim = group.worker_ids[(start + i) % group.worker_ids.len()];
                    if victim == thief_id {
                        continue;
                    }
                    match registry.workers[victim]
                        .stealer
                        .steal_batch_and_pop(thief_worker)
                    {
                        Steal::Success(item) => {
                            return Some(item);
                        }
                        Steal::Retry => {
                            retry_steal = true;
                            break;
                        }
                        Steal::Empty => continue,
                    }
                }
            }

            if retry_steal {
                retries += 1;
                continue;
            }

            if let Some(task) = self.pop_global() {
                return Some(task);
            }

            let start_group = rand.next_u32(topo.groups.len() as u32) as usize;

            for i in 0..topo.groups.len() {
                let other_group_idx = (start_group + i) % topo.groups.len();
                if other_group_idx == group_idx {
                    continue;
                }
                let other_group = &topo.groups[other_group_idx];
                for &victim in &other_group.worker_ids {
                    match registry.workers[victim]
                        .stealer
                        .steal_batch_and_pop(thief_worker)
                    {
                        Steal::Success(item) => {
                            return Some(item);
                        }
                        Steal::Retry => {
                            retry_steal = true;
                            break;
                        }
                        Steal::Empty => continue,
                    }
                }
                if retry_steal {
                    break;
                }
            }

            if retry_steal {
                retries += 1;
                continue;
            }

            break;
        }

        None
    }
}

pub(crate) struct IdleController {
    pub(crate) idle_mask: AtomicBitset,
    pub(crate) event_count: Arc<EventCount>,
    global_idle_stack: IdleStack,
    global_idle_slots: IdleSlots,
    global_scan_cursor: AtomicUsize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WakeResult {
    Woken,
    NoIdleWorker,
}

impl IdleController {
    pub(crate) fn new(worker_count: usize, event_count: Arc<EventCount>) -> Self {
        Self {
            idle_mask: AtomicBitset::new(worker_count),
            event_count,
            global_idle_stack: IdleStack::new(),
            global_idle_slots: IdleSlots::new(worker_count),
            global_scan_cursor: AtomicUsize::new(0),
        }
    }

    pub(crate) fn enter_idle(&self, worker_id: usize, topo: &TopologyContext) {
        if !self.idle_mask.try_set(worker_id) {
            return;
        }

        let group_idx = topo.worker_to_group[worker_id];
        topo.groups[group_idx]
            .idle_stack
            .push(worker_id, &topo.group_idle_slots);
        self.global_idle_stack
            .push(worker_id, &self.global_idle_slots);
    }

    pub(crate) fn leave_idle(&self, worker_id: usize, topo: &TopologyContext) {
        self.idle_mask.clear(worker_id);
        let group_idx = topo.worker_to_group[worker_id];
        topo.groups[group_idx]
            .idle_stack
            .try_pop_self(worker_id, &topo.group_idle_slots);
        self.global_idle_stack
            .try_pop_self(worker_id, &self.global_idle_slots);
    }

    fn wake_worker(&self, worker_id: usize, registry: &WorkerRegistry) -> WakeResult {
        if registry.unpark(worker_id).is_err() {
            // Unparker 已将故障写入共享 fatal 通道；这里没有可向上传播的结果。
        }
        WakeResult::Woken
    }

    /// 优先唤醒目标 NUMA 组中的一个 worker；目标组没有候选时回退到全局 idle 索引。
    pub(crate) fn wake_for_global_work(
        &self,
        group_idx: usize,
        topo: &TopologyContext,
        registry: &WorkerRegistry,
    ) -> WakeResult {
        let group = &topo.groups[group_idx];
        if let Some(worker_id) = group
            .idle_stack
            .pop_idle(&self.idle_mask, &topo.group_idle_slots)
        {
            return self.wake_worker(worker_id, registry);
        }
        for &worker_id in &group.worker_ids {
            if self.idle_mask.is_set(worker_id) {
                return self.wake_worker(worker_id, registry);
            }
        }

        if let Some(worker_id) = self
            .global_idle_stack
            .pop_idle(&self.idle_mask, &self.global_idle_slots)
        {
            return self.wake_worker(worker_id, registry);
        }

        let worker_count = topo.worker_to_group.len();
        if worker_count == 0 {
            return WakeResult::NoIdleWorker;
        }
        let start = self.global_scan_cursor.fetch_add(1, Ordering::Relaxed) % worker_count;
        for offset in 0..worker_count {
            let worker_id = (start + offset) % worker_count;
            if self.idle_mask.is_set(worker_id) {
                return self.wake_worker(worker_id, registry);
            }
        }
        WakeResult::NoIdleWorker
    }
}

pub(crate) struct RuntimeProgressCoordinator<'a, T> {
    shared: &'a RuntimeShared<T>,
    worker_id: usize,
}

impl<'a, T> RuntimeProgressCoordinator<'a, T> {
    pub(crate) fn new(shared: &'a RuntimeShared<T>, worker_id: usize) -> Self {
        Self { shared, worker_id }
    }

    pub(crate) fn run<C: LoopController>(&self, controller: &C) -> Result<()> {
        let idle_decision = match self.shared.idle_hook {
            Some(h) => h(self.shared)?,
            None => IdleDecision::wait(IdleWaitStrategy::Block),
        };
        let Some(wait_strategy) = idle_decision.into_wait_strategy() else {
            thread::yield_now();
            return Ok(());
        };

        let base = &self.shared.base;
        let seq = base.idle.event_count.load();
        base.idle.enter_idle(self.worker_id, &base.topo);

        if self.should_retry(seq, controller) {
            self.leave_idle();
            return Ok(());
        }

        if let Some(task) = base.scheduler.pop_global() {
            self.leave_idle();
            base.poll_send_task(self.worker_id, task)?;
            return Ok(());
        }

        self.park(wait_strategy)?;
        self.leave_idle();
        Ok(())
    }

    fn should_retry<C: LoopController>(&self, seq: usize, controller: &C) -> bool {
        let base = &self.shared.base;
        base.idle.event_count.load() != seq
            || self.shared.has_work(self.worker_id)
            || base.shutdown.is_shutdown()
            || controller.is_ready()
    }

    /// 真正让线程睡下去。
    ///
    /// 没有 `park_hook` 时**不能**退化成 `thread::yield_now()`：默认构建下 idle 决策就是
    /// `Wait(Block)`，yield 意味着所有空闲 worker 100% 占用 CPU 死转。每个 worker 的
    /// [`Unparker`] 自带一个 futex / `WaitOnAddress` 信号，缺省时就阻塞在它上面 ——
    /// 而 `wake_worker` 一律走同一个 `Unparker`，唤醒路径无需分叉。
    fn park(&self, wait_strategy: IdleWaitStrategy) -> Result<()> {
        if let Some(park_hook) = self.shared.park_hook {
            park_hook(self.shared, wait_strategy)?;
            return Ok(());
        }

        let unparker = self.shared.base.unparker(self.worker_id);
        match wait_strategy {
            IdleWaitStrategy::Block => unparker.park(),
            IdleWaitStrategy::Timeout(timeout) => unparker.park_timeout(timeout),
        }
        Ok(())
    }

    /// 离开 idle 状态。
    ///
    /// 先清 `idle_mask`（这才是「是否可被唤醒」的权威标记），再尝试摘除栈顶的自身条目。
    /// 摘不掉时条目会作为陈旧条目留在栈里，由 `pop_idle` 惰性丢弃 —— `IdleSlots` 的
    /// `in_stack` 位保证它不会被重复入栈。
    fn leave_idle(&self) {
        let base = &self.shared.base;
        base.idle.leave_idle(self.worker_id, &base.topo);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use super::{AtomicBitset, EventCount, IdleController, IdleSlots, IdleStack, WakeResult};
    use crate::{error::RuntimeWakeError, runtime::primitives::RuntimeWaker};
    use std::result::Result as StdResult;

    struct RecordingWaker {
        worker_id: usize,
        calls: Arc<[AtomicUsize]>,
    }

    impl RuntimeWaker for RecordingWaker {
        fn wake(&self) -> StdResult<(), RuntimeWakeError> {
            self.calls[self.worker_id].fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
    }

    fn wake_fixture() -> (
        IdleController,
        super::TopologyContext,
        super::WorkerRegistry,
        Arc<[AtomicUsize]>,
    ) {
        let worker_count = 4;
        let worker_count_nz = std::num::NonZeroUsize::new(worker_count).expect("workers");
        let queue_capacity = std::num::NonZeroUsize::new(1).expect("queue capacity");
        let (registry, _, _) =
            crate::runtime::shared::init_runtime_components(worker_count_nz, queue_capacity);
        let calls: Arc<[AtomicUsize]> = (0..worker_count)
            .map(|_| AtomicUsize::new(0))
            .collect::<Vec<_>>()
            .into();
        for (worker_id, unparker) in registry.unparkers.iter().enumerate() {
            unparker
                .bind(Arc::new(RecordingWaker {
                    worker_id,
                    calls: calls.clone(),
                }))
                .expect("recording waker must bind once");
        }
        let topo = super::TopologyContext::from_worker_to_group(&[0, 0, 1, 1]);
        let idle = IdleController::new(worker_count, Arc::new(EventCount::new()));
        (idle, topo, registry, calls)
    }

    fn fixture(worker_count: usize) -> (IdleStack, IdleSlots, AtomicBitset) {
        (
            IdleStack::new(),
            IdleSlots::new(worker_count),
            AtomicBitset::new(worker_count),
        )
    }

    #[test]
    fn idle_stack_pops_in_lifo_order() {
        let (stack, slots, mask) = fixture(3);
        for id in 0..3 {
            assert!(mask.try_set(id));
            stack.push(id, &slots);
        }

        assert_eq!(stack.pop_idle(&mask, &slots), Some(2));
        assert_eq!(stack.pop_idle(&mask, &slots), Some(1));
        assert_eq!(stack.pop_idle(&mask, &slots), Some(0));
        assert_eq!(stack.pop_idle(&mask, &slots), None);
    }

    #[test]
    fn idle_stack_pop_idle_skips_stale_entries() {
        let (stack, slots, mask) = fixture(3);
        for id in 0..3 {
            assert!(mask.try_set(id));
            stack.push(id, &slots);
        }

        // worker 2 / 0 已离开 idle，只有 worker 1 仍可被唤醒。
        mask.clear(2);
        mask.clear(0);
        assert_eq!(stack.pop_idle(&mask, &slots), Some(1));
        assert_eq!(stack.pop_idle(&mask, &slots), None);
    }

    #[test]
    fn idle_stack_try_pop_self_only_removes_top() {
        let (stack, slots, mask) = fixture(2);
        assert!(mask.try_set(0));
        stack.push(0, &slots);
        assert!(mask.try_set(1));
        stack.push(1, &slots);

        // 栈顶是 1，worker 0 摘不掉自己。
        assert!(!stack.try_pop_self(0, &slots));
        assert!(stack.try_pop_self(1, &slots));
        assert!(stack.try_pop_self(0, &slots));
        assert_eq!(stack.pop(&slots), None);
    }

    /// 回归用例：验证陈旧条目 + 重复入栈不会把链表变成环，避免 `pop_idle` 陷入死循环。
    #[test]
    fn idle_stack_stale_entry_never_forms_cycle() {
        let (stack, slots, mask) = fixture(2);

        // 1) worker 0 进入 idle。
        assert!(mask.try_set(0));
        stack.push(0, &slots);
        // 2) worker 1 进入 idle，栈为 [1 -> 0]。
        assert!(mask.try_set(1));
        stack.push(1, &slots);
        // 3) worker 0 被唤醒：栈顶是 1，条目摘不掉，留在栈中。
        mask.clear(0);
        assert!(!stack.try_pop_self(0, &slots));
        // 4) worker 0 再次进入 idle：条目已在栈中，不会重复入栈覆盖 next 指针。
        assert!(mask.try_set(0));
        stack.push(0, &slots);

        // 两个 worker 都不再 idle：pop_idle 必须能走到栈底并返回 None。
        mask.clear(0);
        mask.clear(1);
        assert_eq!(stack.pop_idle(&mask, &slots), None);
        assert_eq!(stack.pop(&slots), None);
    }

    #[test]
    fn idle_stack_generation_never_resets_on_empty() {
        use std::sync::atomic::Ordering;

        let (stack, slots, _mask) = fixture(1);
        stack.push(0, &slots);
        assert_eq!(stack.pop(&slots), Some(0));

        let generation_after_first_cycle = stack.head.load(Ordering::Acquire) >> 32;
        assert!(generation_after_first_cycle > 0);

        // 空栈后再次入栈：generation 必须继续递增，不能复位到 0 重新打开 ABA 窗口。
        stack.push(0, &slots);
        assert!(stack.head.load(Ordering::Acquire) >> 32 > generation_after_first_cycle);
    }

    #[test]
    fn idle_stack_push_is_idempotent_while_entry_is_linked() {
        let (stack, slots, mask) = fixture(1);
        assert!(mask.try_set(0));
        stack.push(0, &slots);
        stack.push(0, &slots);

        assert_eq!(stack.pop(&slots), Some(0));
        assert_eq!(stack.pop(&slots), None);
    }

    #[test]
    fn idle_worker_has_independent_group_and_global_membership() {
        let (idle, topo, _registry, _calls) = wake_fixture();
        idle.enter_idle(0, &topo);

        assert_eq!(
            topo.groups[0]
                .idle_stack
                .pop_idle(&idle.idle_mask, &topo.group_idle_slots),
            Some(0)
        );
        assert_eq!(
            idle.global_idle_stack
                .pop_idle(&idle.idle_mask, &idle.global_idle_slots),
            Some(0)
        );
        assert!(idle.idle_mask.is_set(0));
    }

    #[test]
    fn global_wake_prefers_target_group() {
        let (idle, topo, registry, calls) = wake_fixture();
        idle.enter_idle(0, &topo);
        idle.enter_idle(2, &topo);

        assert_eq!(
            idle.wake_for_global_work(0, &topo, &registry),
            WakeResult::Woken
        );
        assert_eq!(calls[0].load(Ordering::Relaxed), 1);
        assert_eq!(calls[2].load(Ordering::Relaxed), 0);
    }

    #[test]
    fn global_wake_falls_back_to_other_group() {
        let (idle, topo, registry, calls) = wake_fixture();
        idle.enter_idle(2, &topo);

        assert_eq!(
            idle.wake_for_global_work(0, &topo, &registry),
            WakeResult::Woken
        );
        assert_eq!(calls[2].load(Ordering::Relaxed), 1);
        assert_eq!(
            calls
                .iter()
                .map(|calls| calls.load(Ordering::Relaxed))
                .sum::<usize>(),
            1
        );
    }

    #[test]
    fn global_wake_cleans_stale_entries_and_terminates() {
        let (idle, topo, registry, _calls) = wake_fixture();
        idle.enter_idle(0, &topo);
        idle.enter_idle(1, &topo);
        idle.leave_idle(0, &topo);
        idle.leave_idle(1, &topo);

        assert_eq!(
            idle.wake_for_global_work(0, &topo, &registry),
            WakeResult::NoIdleWorker
        );
        assert_eq!(topo.groups[0].idle_stack.pop(&topo.group_idle_slots), None);
        assert_eq!(idle.global_idle_stack.pop(&idle.global_idle_slots), None);
    }

    #[test]
    fn repeated_idle_lifecycle_is_idempotent() {
        let (idle, topo, registry, calls) = wake_fixture();
        idle.enter_idle(0, &topo);
        idle.enter_idle(0, &topo);
        idle.leave_idle(0, &topo);
        idle.leave_idle(0, &topo);
        assert_eq!(
            idle.wake_for_global_work(0, &topo, &registry),
            WakeResult::NoIdleWorker
        );

        idle.enter_idle(0, &topo);
        assert_eq!(
            idle.wake_for_global_work(0, &topo, &registry),
            WakeResult::Woken
        );
        assert_eq!(calls[0].load(Ordering::Relaxed), 1);
    }
}
