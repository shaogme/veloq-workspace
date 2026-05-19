use std::cell::RefCell;
use std::collections::VecDeque;
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
use crate::utils::storage::{AtomicOptionPtr, Storage};
use crate::utils::{Deque, FastRand, Steal};

pub(crate) struct LocalWorkerState {
    pub(crate) worker_id: usize,
    pub(crate) queue: RefCell<VecDeque<LocalTaskRef>>,
}

pub(crate) static LOCAL_WORKER_STATE: veloq_tls::Tls<LocalWorkerState> =
    veloq_tls::Tls::new(|| panic!("LocalWorkerState accessed outside of a worker thread"));

impl LocalWorkerState {
    pub(crate) fn new(worker_id: usize) -> Self {
        Self {
            worker_id,
            queue: RefCell::new(VecDeque::new()),
        }
    }

    #[inline]
    pub(crate) fn push(&self, task: LocalTaskRef) {
        self.queue.borrow_mut().push_back(task);
    }

    #[inline]
    pub(crate) fn pop(&self) -> Option<LocalTaskRef> {
        self.queue.borrow_mut().pop_front()
    }

    #[inline]
    pub(crate) fn is_empty(&self) -> bool {
        self.queue.borrow().is_empty()
    }
}

pub(crate) struct WorkerQueue {
    pub(crate) remote_tx: Sender<SendTaskRef>,
    pub(crate) pinned_tx: Sender<SendTaskRef>,
    pub(crate) pinned_count: AtomicUsize,
    /// LIFO slot for high-priority task (cache locality)
    pub(crate) lifo: AtomicOptionPtr<TaskHeader>,
    /// Chase-Lev Deque for work-stealing
    pub(crate) deque: Deque<SendTaskRef>,
}

impl WorkerQueue {
    pub(crate) fn new(
        remote_tx: Sender<SendTaskRef>,
        pinned_tx: Sender<SendTaskRef>,
        queue_capacity: NonZeroUsize,
    ) -> Self {
        Self {
            remote_tx,
            pinned_tx,
            pinned_count: AtomicUsize::new(0),
            lifo: AtomicOptionPtr::new(None),
            deque: Deque::new(queue_capacity),
        }
    }
}

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
    head: AtomicU64,
}

impl GlobalInjector {
    const EMPTY: u64 = 0;

    pub(crate) fn new() -> Self {
        Self {
            head: AtomicU64::new(Self::EMPTY),
        }
    }

    pub(crate) fn push(&self, task: SendTaskRef) {
        let header_ptr = task.header() as *const _ as u64;
        // Modern x86_64 uses 48-bit virtual addresses.
        debug_assert_eq!(header_ptr & 0xFFFF000000000000, 0);

        let mut head = self.head.load(Ordering::Acquire);
        loop {
            let old_ptr = (head & 0x0000FFFFFFFFFFFF) as *const TaskHeader;
            task.header()
                .injector_next
                .store(NonNull::new(old_ptr as *mut _), Ordering::Release);

            let next_gen = ((head >> 48).wrapping_add(1)) & 0xFFFF;
            let new_head = (next_gen << 48) | header_ptr;

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

    pub(crate) fn pop(&self) -> Option<SendTaskRef> {
        let mut head = self.head.load(Ordering::Acquire);
        loop {
            if head == Self::EMPTY {
                return None;
            }

            let ptr = (head & 0x0000FFFFFFFFFFFF) as *const TaskHeader;
            let next_ptr = unsafe { (&*ptr).injector_next.load(Ordering::Acquire) };

            let next_raw = next_ptr.map(|p| p.as_ptr() as u64).unwrap_or(0);
            let next_gen = ((head >> 48).wrapping_add(1)) & 0xFFFF;
            let new_head = if next_raw == 0 {
                Self::EMPTY
            } else {
                (next_gen << 48) | next_raw
            };

            match self.head.compare_exchange_weak(
                head,
                new_head,
                Ordering::Release,
                Ordering::Acquire,
            ) {
                Ok(_) => return Some(unsafe { SendTaskRef::from_header(ptr) }),
                Err(h) => head = h,
            }
        }
    }
}

pub(crate) struct TaskScheduler {
    pub(crate) injector: GlobalInjector,
    pub(crate) next_worker: AtomicUsize,
    pub(crate) searching_workers: AtomicUsize,
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
                    Steal::Retry => return self.steal_send(thief_id, registry, topo, rand),
                    Steal::Empty => continue,
                }
            }
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
                    Steal::Retry => return self.steal_send(thief_id, registry, topo, rand),
                    Steal::Empty => continue,
                }
            }
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
        if idle_decision.is_continue() {
            std::thread::yield_now();
            return;
        }

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

        self.park(idle_decision, completion);
        self.leave_idle(group_idx);
    }

    fn should_retry<S: Storage, O: Ownership>(
        &self,
        seq: usize,
        completion: Option<&GenericScopeCompletion<S, O>>,
    ) -> bool {
        let base = &self.shared.base;
        base.idle.event_count.load() != seq
            || base.has_work(self.worker_id)
            || base.shutdown.load(std::sync::atomic::Ordering::Acquire)
            || completion.map(|c| c.is_done()).unwrap_or(false)
    }

    fn park<S: Storage, O: Ownership>(
        &self,
        idle_decision: IdleDecision,
        completion: Option<&GenericScopeCompletion<S, O>>,
    ) {
        let base = &self.shared.base;
        let parker = Parker::from_inner(base.registry.parker_inners[self.worker_id].clone());
        match idle_decision {
            IdleDecision::Wait(IdleWaitStrategy::Timeout(duration)) => {
                let _ = parker.park_timeout(duration);
            }
            IdleDecision::Wait(IdleWaitStrategy::Block) => {
                if completion.is_some() {
                    let _ = parker.park_timeout(Duration::from_millis(1));
                } else {
                    parker.park();
                }
            }
            IdleDecision::Continue => unreachable!(),
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
