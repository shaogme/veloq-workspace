use parking_lot::Mutex;
use std::num::NonZeroUsize;
use std::ptr::NonNull;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc::Sender;
use std::time::Duration;

use crate::runtime::context::{IdleDecision, IdleWaitStrategy};
use crate::runtime::primitives::{EventCount, Parker, ParkerInner, Unparker};
use crate::runtime::shared::RuntimeShared;
use crate::scope::GenericScopeCompletion;
use crate::task::{LocalTaskRef, SendTaskRef, TaskHandleRef, TaskHeader};
use crate::utils::ownership::Ownership;
use crate::utils::storage::{AtomicOptionPtr, StateOptionPtr, Storage};
use crate::utils::{Deque, FastRand, Steal};

pub(crate) struct WorkerQueue {
    pub(crate) remote_tx: Sender<SendTaskRef>,
    pub(crate) pinned_tx: Sender<SendTaskRef>,
    pub(crate) local_tx: Sender<LocalTaskRef>,
    pub(crate) pinned_count: AtomicUsize,
    pub(crate) local_count: AtomicUsize,
    /// LIFO slot for high-priority task (cache locality)
    pub(crate) lifo: AtomicOptionPtr<TaskHeader>,
    /// Chase-Lev Deque for work-stealing
    pub(crate) deque: Deque<SendTaskRef>,
}

impl WorkerQueue {
    pub(crate) fn new(
        remote_tx: Sender<SendTaskRef>,
        pinned_tx: Sender<SendTaskRef>,
        local_tx: Sender<LocalTaskRef>,
        queue_capacity: NonZeroUsize,
    ) -> Self {
        Self {
            remote_tx,
            pinned_tx,
            local_tx,
            pinned_count: AtomicUsize::new(0),
            local_count: AtomicUsize::new(0),
            lifo: AtomicOptionPtr::new(None),
            deque: Deque::new(queue_capacity),
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

    pub(crate) fn set(&self, index: usize) {
        let word = index / 64;
        let bit = index % 64;
        self.bits[word].fetch_or(1 << bit, Ordering::Release);
    }

    pub(crate) fn clear(&self, index: usize) {
        let word = index / 64;
        let bit = index % 64;
        self.bits[word].fetch_and(!(1 << bit), Ordering::Release);
    }
}

pub(crate) struct WorkerRegistry {
    pub(crate) workers: Box<[Arc<WorkerQueue>]>,
    pub(crate) unparkers: Box<[Unparker]>,
    pub(crate) parker_inners: Box<[Arc<ParkerInner>]>,
}

impl WorkerRegistry {
    #[inline]
    pub(crate) fn unpark(&self, worker_id: usize) {
        self.unparkers[worker_id].unpark();
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
    head: Mutex<Option<NonNull<TaskHeader>>>,
}

impl GlobalInjector {
    pub(crate) fn new() -> Self {
        Self {
            head: Mutex::new(None),
        }
    }

    pub(crate) fn push(&self, task: SendTaskRef) {
        let task_ptr = NonNull::from(task.header());
        let mut head = self.head.lock();
        task.header().set_next(*head);
        *head = Some(task_ptr);
    }

    pub(crate) fn pop(&self) -> Option<SendTaskRef> {
        let mut head = self.head.lock();
        let head_ptr = (*head)?;
        let next_ptr = unsafe { head_ptr.as_ref().next() };
        *head = next_ptr;
        unsafe {
            head_ptr.as_ref().set_next(None);
            Some(SendTaskRef::from_header(head_ptr.as_ptr()))
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
    ) -> Option<SendTaskRef> {
        let thief_worker = &registry.workers[thief_id];
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
                        .deque
                        .steal_batch(&thief_worker.deque)
                    {
                        Steal::Success(task) => return Some(task),
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
                        .deque
                        .steal_batch(&thief_worker.deque)
                    {
                        Steal::Success(task) => return Some(task),
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

pub(crate) struct RuntimeProgressCoordinator<'a, T> {
    shared: &'a RuntimeShared<T>,
    worker_id: usize,
}

impl<'a, T> RuntimeProgressCoordinator<'a, T> {
    pub(crate) fn new(shared: &'a RuntimeShared<T>, worker_id: usize) -> Self {
        Self { shared, worker_id }
    }

    pub(crate) fn run<S: Storage, O: Ownership>(
        &self,
        completion: Option<&GenericScopeCompletion<S, O>>,
    ) {
        let idle_decision = self
            .shared
            .idle_hook
            .map(|h| h(self.shared))
            .unwrap_or(IdleDecision::wait(IdleWaitStrategy::Block));
        let Some(wait_strategy) = idle_decision.into_wait_strategy() else {
            std::thread::yield_now();
            return;
        };

        let base = &self.shared.base;
        let group_idx = base.topo.worker_to_group[self.worker_id];
        let group = &base.topo.groups[group_idx];
        let seq = base.idle.event_count.load();

        base.idle.idle_mask.set(self.worker_id);
        group.idle_stack.push(self.worker_id, &base.topo.next_idle);

        if self.should_retry(seq, completion) {
            self.leave_idle(group_idx);
            return;
        }

        if let Some(task) = base.scheduler.pop_global() {
            self.leave_idle(group_idx);
            base.poll_send_task(self.worker_id, task);
            return;
        }

        self.park(wait_strategy, completion);
        self.leave_idle(group_idx);
    }

    fn should_retry<S: Storage, O: Ownership>(
        &self,
        seq: usize,
        completion: Option<&GenericScopeCompletion<S, O>>,
    ) -> bool {
        let base = &self.shared.base;
        base.idle.event_count.load() != seq
            || self.shared.has_work(self.worker_id)
            || base.shutdown.load(std::sync::atomic::Ordering::Acquire)
            || completion.map(|c| c.is_done()).unwrap_or(false)
    }

    fn park<S: Storage, O: Ownership>(
        &self,
        wait_strategy: IdleWaitStrategy,
        completion: Option<&GenericScopeCompletion<S, O>>,
    ) {
        let base = &self.shared.base;
        let parker = Parker::from_inner(base.registry.parker_inners[self.worker_id].clone());
        match wait_strategy {
            IdleWaitStrategy::Timeout(duration) => {
                let _ = parker.park_timeout(duration);
            }
            IdleWaitStrategy::Block => {
                if completion.is_some() {
                    let _ = parker.park_timeout(Duration::from_millis(1));
                } else {
                    parker.park();
                }
            }
        }
    }

    fn leave_idle(&self, group_idx: usize) {
        let base = &self.shared.base;
        let _ = base.topo.groups[group_idx]
            .idle_stack
            .pop(&base.topo.next_idle);
        base.idle.idle_mask.clear(self.worker_id);
    }
}
