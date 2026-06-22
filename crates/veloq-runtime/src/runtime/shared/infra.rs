use crossbeam_deque::{Injector, Steal, Stealer, Worker};
use crossbeam_queue::ArrayQueue;
use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    thread,
};
use veloq_storage::{AtomicOptionPtr, StateOptionPtr};

use crate::{
    error::Result,
    runtime::{
        context::{IdleDecision, IdleWaitStrategy, WaitBackend},
        primitives::EventCount,
        shared::RuntimeShared,
        wake::WakeCoordinator,
    },
    scope::GenericScopeCompletion,
    task::{LocalTaskRef, ScopeStorage, SendTaskRef, TaskHeader},
    utils::{FastRand, ownership::Ownership},
};

pub(crate) struct WorkerQueue {
    pub(crate) remote_queue: ArrayQueue<SendTaskRef>,
    pub(crate) pinned_queue: ArrayQueue<SendTaskRef>,
    pub(crate) local_queue: ArrayQueue<LocalTaskRef>,
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
        stealer: Stealer<SendTaskRef>,
    ) -> Self {
        Self {
            remote_queue,
            pinned_queue,
            local_queue,
            pinned_count: AtomicUsize::new(0),
            local_count: AtomicUsize::new(0),
            lifo: AtomicOptionPtr::new(None),
            stealer,
        }
    }
}

unsafe impl Send for WorkerQueue {}
unsafe impl Sync for WorkerQueue {}

pub(crate) struct NUMAGroup {
    pub(crate) worker_ids: Vec<usize>,
    pub(crate) idle_stack: IdleStack,
}

pub(crate) struct IdleStack {
    head: AtomicU64,
}

impl IdleStack {
    const EMPTY: u64 = u64::MAX;

    pub(crate) fn new() -> Self {
        Self {
            head: AtomicU64::new(Self::EMPTY),
        }
    }

    pub(crate) fn push(&self, worker_id: usize, next_ptrs: &[AtomicUsize]) {
        let mut head = self.head.load(Ordering::Acquire);
        loop {
            let generation = if head == Self::EMPTY {
                0
            } else {
                (head >> 32) + 1
            };
            let new_head = (generation << 32) | (worker_id as u64);

            let old_top_id = if head == Self::EMPTY {
                usize::MAX
            } else {
                (head & 0xFFFFFFFF) as usize
            };
            next_ptrs[worker_id].store(old_top_id, Ordering::Release);

            match self.head.compare_exchange_weak(
                head,
                new_head,
                Ordering::Release,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(h) => head = h,
            }
        }
    }

    pub(crate) fn pop(&self, next_ptrs: &[AtomicUsize]) -> Option<usize> {
        let mut head = self.head.load(Ordering::Acquire);
        loop {
            if head == Self::EMPTY {
                return None;
            }
            let worker_id = (head & 0xFFFFFFFF) as usize;
            let next_id = next_ptrs[worker_id].load(Ordering::Acquire);

            let new_head = if next_id == usize::MAX {
                Self::EMPTY
            } else {
                let next_gen = (head >> 32) + 1;
                (next_gen << 32) | (next_id as u64)
            };

            match self.head.compare_exchange_weak(
                head,
                new_head,
                Ordering::Release,
                Ordering::Acquire,
            ) {
                Ok(_) => return Some(worker_id),
                Err(h) => head = h,
            }
        }
    }

    /// 仅当栈顶为 `worker_id` 时弹出（`leave_idle` 快路径）。
    pub(crate) fn try_pop_self(&self, worker_id: usize, next_ptrs: &[AtomicUsize]) -> bool {
        let mut head = self.head.load(Ordering::Acquire);
        loop {
            if head == Self::EMPTY {
                return false;
            }
            let top_id = (head & 0xFFFFFFFF) as usize;
            if top_id != worker_id {
                return false;
            }
            let next_id = next_ptrs[worker_id].load(Ordering::Acquire);
            let new_head = if next_id == usize::MAX {
                Self::EMPTY
            } else {
                let next_gen = (head >> 32) + 1;
                (next_gen << 32) | (next_id as u64)
            };
            match self.head.compare_exchange_weak(
                head,
                new_head,
                Ordering::Release,
                Ordering::Acquire,
            ) {
                Ok(_) => return true,
                Err(h) => head = h,
            }
        }
    }

    /// 弹出仍标记为 idle 的 worker；跳过栈中已失效（stale）的条目。
    pub(crate) fn pop_idle(
        &self,
        idle_mask: &AtomicBitset,
        next_ptrs: &[AtomicUsize],
    ) -> Option<usize> {
        while let Some(worker_id) = self.pop(next_ptrs) {
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
    pub(crate) wake_sources: Box<[Arc<WakeCoordinator>]>,
}

impl WorkerRegistry {
    #[inline]
    pub(crate) fn wake_source(&self, worker_id: usize) -> &WakeCoordinator {
        self.wake_sources[worker_id].as_ref()
    }
}

pub(crate) struct TopologyContext {
    pub(crate) groups: Vec<NUMAGroup>,
    pub(crate) worker_to_group: Vec<usize>,
    pub(crate) next_idle: Vec<AtomicUsize>,
}

impl TopologyContext {
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
}

impl GlobalInjector {
    pub(crate) fn new() -> Self {
        Self {
            queue: Injector::new(),
        }
    }

    pub(crate) fn push(&self, task: SendTaskRef) {
        self.queue.push(task);
    }

    pub(crate) fn pop(&self) -> Option<SendTaskRef> {
        loop {
            match self.queue.steal() {
                Steal::Success(task) => return Some(task),
                Steal::Retry => continue,
                Steal::Empty => return None,
            }
        }
    }
}

pub(crate) struct TaskScheduler {
    pub(crate) injector: GlobalInjector,
    pub(crate) next_worker: AtomicUsize,
}

impl TaskScheduler {
    pub(crate) fn pop_global(&self) -> Option<SendTaskRef> {
        self.injector.pop()
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
    pub(crate) event_count: EventCount,
}

impl IdleController {
    /// 唤醒指定 NUMA 组内一个 idle worker；成功返回 true。
    pub(crate) fn wake_idle_in_group(
        &self,
        group_idx: usize,
        topo: &TopologyContext,
        registry: &WorkerRegistry,
    ) -> bool {
        let group = &topo.groups[group_idx];
        if let Some(worker_id) = group.idle_stack.pop_idle(&self.idle_mask, &topo.next_idle) {
            registry.wake_source(worker_id).notify();
            return true;
        }
        for &worker_id in &group.worker_ids {
            if self.idle_mask.is_set(worker_id) {
                registry.wake_source(worker_id).notify();
                return true;
            }
        }
        false
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

    pub(crate) fn run<S: ScopeStorage, O: Ownership>(
        &self,
        completion: Option<&GenericScopeCompletion<S, O>>,
    ) -> Result<()> {
        let idle_decision = match self.shared.idle_hook {
            Some(h) => h(self.shared)?,
            None => IdleDecision::wait(WaitBackend::RuntimePark, IdleWaitStrategy::Block),
        };
        let Some((backend, wait_strategy)) = idle_decision.into_wait() else {
            thread::yield_now();
            return Ok(());
        };

        let base = &self.shared.base;
        let group_idx = base.topo.worker_to_group[self.worker_id];
        let group = &base.topo.groups[group_idx];
        let seq = base.idle.event_count.load();

        if base.idle.idle_mask.try_set(self.worker_id) {
            group.idle_stack.push(self.worker_id, &base.topo.next_idle);
        }

        if self.should_retry(seq, completion) {
            self.leave_idle(group_idx);
            return Ok(());
        }

        if let Some(task) = base.scheduler.pop_global() {
            self.leave_idle(group_idx);
            base.poll_send_task(self.worker_id, task)?;
            return Ok(());
        }

        self.park(backend, wait_strategy, completion)?;
        self.leave_idle(group_idx);
        Ok(())
    }

    fn should_retry<S: ScopeStorage, O: Ownership>(
        &self,
        seq: usize,
        completion: Option<&GenericScopeCompletion<S, O>>,
    ) -> bool {
        let base = &self.shared.base;
        base.idle.event_count.load() != seq
            || self.shared.has_work(self.worker_id)
            || base.shutdown.load(Ordering::Acquire)
            || completion.map(|c| c.is_done()).unwrap_or(false)
    }

    fn park<S: ScopeStorage, O: Ownership>(
        &self,
        backend: WaitBackend,
        wait_strategy: IdleWaitStrategy,
        completion: Option<&GenericScopeCompletion<S, O>>,
    ) -> Result<()> {
        let base = &self.shared.base;
        let wake = base.registry.wake_sources[self.worker_id].clone();
        let epoch = wake.current_epoch();
        wake.wait_worker(
            epoch,
            backend,
            wait_strategy,
            completion.is_some(),
            |strategy| match backend {
                WaitBackend::RuntimePark => Ok(()),
                WaitBackend::Driver => self.shared.drive_wait(strategy),
            },
        )
    }

    fn leave_idle(&self, group_idx: usize) {
        let base = &self.shared.base;
        base.idle.idle_mask.clear(self.worker_id);
        base.topo.groups[group_idx]
            .idle_stack
            .try_pop_self(self.worker_id, &base.topo.next_idle);
    }
}
