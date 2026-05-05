use super::context::{CONTEXT, current_worker_id};
use super::primitives::{self, EventCount, Parker, Unparker};
use crate::scope::GenericScopeCompletion;
use crate::task::{LocalTaskRef, SendTaskRef, TaskHandleRef, TaskHeader};
use crate::utils::ownership::Ownership;
use crate::utils::storage::Storage;
use crate::utils::{AtomicOptionPtr, Deque, Steal};
use std::num::NonZeroUsize;
use std::ptr::NonNull;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc::{self, Receiver, Sender};

pub(crate) struct WorkerQueue {
    pub(crate) local_tx: Sender<LocalTaskRef>,
    pub(crate) remote_tx: Sender<SendTaskRef>,
    pub(crate) local_count: AtomicUsize,
    pub(crate) lifo: AtomicOptionPtr<TaskHeader>,
    pub(crate) send: Deque<SendTaskRef>,
}

impl WorkerQueue {
    fn new(
        local_tx: Sender<LocalTaskRef>,
        remote_tx: Sender<SendTaskRef>,
        queue_capacity: NonZeroUsize,
    ) -> Self {
        Self {
            local_tx,
            remote_tx,
            local_count: AtomicUsize::new(0),
            lifo: AtomicOptionPtr::new(None),
            send: Deque::new(queue_capacity),
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

    fn new() -> Self {
        Self {
            head: AtomicU64::new(Self::EMPTY),
        }
    }

    fn push(&self, worker_id: usize, next_ptrs: &[AtomicUsize]) {
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

    fn pop(&self, next_ptrs: &[AtomicUsize]) -> Option<usize> {
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
    fn new(size: usize) -> Self {
        let num_u64 = size.div_ceil(64);
        let mut v = Vec::with_capacity(num_u64);
        for _ in 0..num_u64 {
            v.push(AtomicU64::new(0));
        }
        Self {
            bits: v.into_boxed_slice(),
        }
    }

    fn set(&self, index: usize) {
        let word = index / 64;
        let bit = index % 64;
        self.bits[word].fetch_or(1 << bit, Ordering::Release);
    }

    fn clear(&self, index: usize) {
        let word = index / 64;
        let bit = index % 64;
        self.bits[word].fetch_and(!(1 << bit), Ordering::Release);
    }

    fn is_set(&self, index: usize) -> bool {
        let word = index / 64;
        let bit = index % 64;
        (self.bits[word].load(Ordering::Acquire) & (1 << bit)) != 0
    }
}

pub struct RuntimeShared {
    pub(crate) workers: Vec<Arc<WorkerQueue>>,
    pub(crate) groups: Vec<NUMAGroup>,
    pub(crate) worker_to_group: Vec<usize>,
    pub(crate) next_idle: Vec<AtomicUsize>,
    pub(crate) next_worker: AtomicUsize,
    pub(crate) shutdown: AtomicBool,
    pub(crate) unparkers: Vec<Unparker>,
    pub(crate) idle_mask: AtomicBitset,
    pub(crate) parker_inners: Vec<Arc<primitives::ParkerInner>>,
    pub(crate) event_count: EventCount,
}

impl RuntimeShared {
    pub(crate) fn new(
        worker_count: NonZeroUsize,
        queue_capacity: NonZeroUsize,
    ) -> (
        Self,
        Vec<Receiver<LocalTaskRef>>,
        Vec<Receiver<SendTaskRef>>,
    ) {
        let worker_count_val = worker_count.get();
        let mut unparkers = Vec::with_capacity(worker_count_val);
        let mut parker_inners = Vec::with_capacity(worker_count_val);
        let mut local_receivers = Vec::with_capacity(worker_count_val);
        let mut remote_receivers = Vec::with_capacity(worker_count_val);
        let mut workers = Vec::with_capacity(worker_count_val);
        let mut next_idle = Vec::with_capacity(worker_count_val);

        for _ in 0..worker_count_val {
            let inner = Arc::new(primitives::ParkerInner {
                state: AtomicU32::new(0),
            });
            unparkers.push(Unparker::from_inner(inner.clone()));
            parker_inners.push(inner);

            let (ltx, lrx) = mpsc::channel();
            let (rtx, rrx) = mpsc::channel();
            local_receivers.push(lrx);
            remote_receivers.push(rrx);
            workers.push(Arc::new(WorkerQueue::new(ltx, rtx, queue_capacity)));
            next_idle.push(AtomicUsize::new(usize::MAX));
        }

        // NUMA detection
        let topo = numaperf_topo::Topology::discover().ok();
        let mut groups = Vec::new();
        let mut worker_to_group = vec![0; worker_count_val];

        match topo {
            Some(t) if t.node_count() > 0 => {
                let node_count = t.node_count();
                let mut node_to_workers: Vec<Vec<usize>> = vec![Vec::new(); node_count];

                // Distribute workers among detected NUMA nodes
                for (i, group) in worker_to_group
                    .iter_mut()
                    .enumerate()
                    .take(worker_count_val)
                {
                    let node_idx = i % node_count;
                    node_to_workers[node_idx].push(i);
                    *group = node_idx;
                }

                for worker_ids in node_to_workers.into_iter() {
                    if !worker_ids.is_empty() {
                        groups.push(NUMAGroup {
                            worker_ids,
                            idle_stack: IdleStack::new(),
                        });
                    }
                }
            }
            _ => {
                // Fallback to a single group if no NUMA nodes detected
                groups.push(NUMAGroup {
                    worker_ids: (0..worker_count_val).collect(),
                    idle_stack: IdleStack::new(),
                });
            }
        }

        (
            Self {
                workers,
                groups,
                worker_to_group,
                next_idle,
                next_worker: AtomicUsize::new(0),
                shutdown: AtomicBool::new(false),
                unparkers,
                idle_mask: AtomicBitset::new(worker_count_val),
                parker_inners,
                event_count: EventCount::new(),
            },
            local_receivers,
            remote_receivers,
        )
    }

    pub fn choose_worker(&self) -> usize {
        let n = self.workers.len();
        if n == 1 {
            return 0;
        }

        // Try to stay in the same NUMA group if called from a worker
        let current = current_worker_id();
        if current < n {
            let group_idx = self.worker_to_group[current];
            let group = &self.groups[group_idx];
            if group.worker_ids.len() > 1 {
                // Round-robin within the group
                let idx = self.next_worker.fetch_add(1, Ordering::Relaxed) % group.worker_ids.len();
                return group.worker_ids[idx];
            }
        }

        self.next_worker.fetch_add(1, Ordering::Relaxed) % n
    }

    #[inline]
    pub fn worker_count(&self) -> NonZeroUsize {
        NonZeroUsize::new(self.workers.len()).expect("runtime must have at least one worker")
    }

    pub fn enqueue_local(&self, worker_id: usize, task: LocalTaskRef) {
        if task.header().is_completed() {
            return;
        }
        if task.header().try_mark_queued() {
            let worker = &self.workers[worker_id];
            worker.local_count.fetch_add(1, Ordering::Release);
            let _ = worker.local_tx.send(task);
            self.event_count.notify();
            self.unpark_worker(worker_id);
        }
    }

    fn unpark_worker(&self, worker_id: usize) {
        // If the specific worker is idle, unpark it.
        if self.idle_mask.is_set(worker_id) {
            self.unparkers[worker_id].unpark();
            return;
        }

        // Otherwise, try to unpark any idle worker in the same group
        let group_idx = self.worker_to_group[worker_id];
        if let Some(idle_id) = self.groups[group_idx].idle_stack.pop(&self.next_idle) {
            self.idle_mask.clear(idle_id);
            self.unparkers[idle_id].unpark();
            return;
        }

        // Last resort: try to unpark any idle worker from other groups
        for (i, group) in self.groups.iter().enumerate() {
            if i == group_idx {
                continue;
            }
            if let Some(idle_id) = group.idle_stack.pop(&self.next_idle) {
                self.idle_mask.clear(idle_id);
                self.unparkers[idle_id].unpark();
                return;
            }
        }
    }

    pub fn enqueue_send(&self, worker_id: usize, task: SendTaskRef) {
        if task.header().is_completed() {
            return;
        }
        let worker_id = worker_id % self.workers.len();
        if task.header().try_mark_queued() {
            self.event_count.notify();
            let current = current_worker_id();
            let worker = &self.workers[worker_id];

            if current == worker_id {
                // 尝试放入 LIFO Slot (MPSC)
                let header_ptr = task.header() as *const _ as *mut _;
                if worker
                    .lifo
                    .compare_exchange(
                        None,
                        NonNull::new(header_ptr),
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    )
                    .is_ok()
                {
                    self.unpark_worker(worker_id);
                    return;
                }

                if worker.send.push(task).is_ok() {
                    self.unpark_worker(worker_id);
                    return;
                }
            }
            let _ = worker.remote_tx.send(task);
            self.unpark_worker(worker_id);
        }
    }

    fn pop_local(&self, worker_id: usize, rx: &Receiver<LocalTaskRef>) -> Option<LocalTaskRef> {
        let res = rx.try_recv().ok();
        if res.is_some() {
            self.workers[worker_id]
                .local_count
                .fetch_sub(1, Ordering::Release);
        }
        res
    }

    fn pop_send(&self, worker_id: usize) -> Option<SendTaskRef> {
        let worker = &self.workers[worker_id];
        if let Some(header) = worker.lifo.swap(None, Ordering::AcqRel) {
            return Some(unsafe { SendTaskRef::from_header(header.as_ptr()) });
        }
        worker.send.pop()
    }

    fn steal_send(&self, thief_id: usize) -> Option<SendTaskRef> {
        let thief_queue = &self.workers[thief_id].send;
        let num_workers = self.workers.len();
        if num_workers <= 1 {
            return None;
        }

        let group_idx = self.worker_to_group[thief_id];
        let group = &self.groups[group_idx];

        // 1. Try to steal from the same NUMA group first
        if group.worker_ids.len() > 1 {
            let start = CONTEXT.with(|ctx| {
                ctx.borrow()
                    .as_ref()
                    .map(|c| c.rand.borrow_mut().next_u32(group.worker_ids.len() as u32) as usize)
                    .unwrap_or(0)
            });

            for i in 0..group.worker_ids.len() {
                let victim = group.worker_ids[(start + i) % group.worker_ids.len()];
                if victim == thief_id {
                    continue;
                }
                match self.workers[victim].send.steal_batch(thief_queue) {
                    Steal::Success(task) => return Some(task),
                    Steal::Retry => return self.steal_send(thief_id),
                    Steal::Empty => continue,
                }
            }
        }

        // 2. Try to steal from other groups
        let start_group = CONTEXT.with(|ctx| {
            ctx.borrow()
                .as_ref()
                .map(|c| c.rand.borrow_mut().next_u32(self.groups.len() as u32) as usize)
                .unwrap_or(0)
        });

        for i in 0..self.groups.len() {
            let other_group_idx = (start_group + i) % self.groups.len();
            if other_group_idx == group_idx {
                continue;
            }
            let other_group = &self.groups[other_group_idx];
            for &victim in &other_group.worker_ids {
                match self.workers[victim].send.steal_batch(thief_queue) {
                    Steal::Success(task) => return Some(task),
                    Steal::Retry => return self.steal_send(thief_id),
                    Steal::Empty => continue,
                }
            }
        }

        None
    }

    fn poll_local_task(&self, worker_id: usize, task: LocalTaskRef) {
        task.header().clear_queued();
        let _ = task.poll_task(worker_id);
    }

    fn poll_send_task(&self, worker_id: usize, task: SendTaskRef) {
        task.header().clear_queued();
        let _ = task.poll_task(worker_id);
    }

    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Release);
        for unparker in &self.unparkers {
            unparker.unpark();
        }
    }

    pub fn drive_worker<S: Storage, O: Ownership>(
        &self,
        completion: Option<&GenericScopeCompletion<S, O>>,
    ) {
        let worker_id = current_worker_id();
        CONTEXT.with(|ctx| {
            let ctx = ctx.borrow();
            let ctx = ctx.as_ref().expect("runtime context missing");
            let mut tick = 0u32;
            let mut injector_check_interval = 61;

            while !self.shutdown.load(Ordering::Acquire)
                && completion.map(|c| !c.is_done()).unwrap_or(true)
            {
                let mut progressed = false;
                tick = tick.wrapping_add(1);

                if let Some(task) = self.pop_send(worker_id) {
                    self.poll_send_task(worker_id, task);
                    progressed = true;
                }

                if !progressed && let Some(task) = self.pop_local(worker_id, &ctx.local_rx) {
                    self.poll_local_task(worker_id, task);
                    progressed = true;
                }

                if !progressed && let Ok(task) = ctx.remote_rx.try_recv() {
                    self.poll_send_task(worker_id, task);
                    progressed = true;
                }

                if !progressed || tick.is_multiple_of(injector_check_interval) {
                    if let Some(task) = self.steal_send(worker_id) {
                        self.poll_send_task(worker_id, task);
                        progressed = true;
                        injector_check_interval = 61;
                    } else if tick.is_multiple_of(injector_check_interval) {
                        injector_check_interval = (injector_check_interval * 2 + 1).min(1023);
                    }
                }

                if progressed {
                    continue;
                }

                for _ in 0..64 {
                    if let Some(task) = self.steal_send(worker_id) {
                        self.poll_send_task(worker_id, task);
                        progressed = true;
                        break;
                    }
                    if let Ok(task) = ctx.remote_rx.try_recv() {
                        self.poll_send_task(worker_id, task);
                        progressed = true;
                        break;
                    }
                    std::hint::spin_loop();
                }

                if progressed {
                    continue;
                }

                let seq = self.event_count.load();
                self.idle_mask.set(worker_id);
                let group_idx = self.worker_to_group[worker_id];
                self.groups[group_idx]
                    .idle_stack
                    .push(worker_id, &self.next_idle);

                if self.event_count.load() != seq
                    || self.has_work(worker_id)
                    || self.shutdown.load(Ordering::Acquire)
                    || completion.map(|c| c.is_done()).unwrap_or(false)
                {
                    if self.groups[group_idx]
                        .idle_stack
                        .pop(&self.next_idle)
                        .is_some()
                    {
                        self.idle_mask.clear(worker_id);
                    }
                    continue;
                }

                if completion.is_some() {
                    if self.groups[group_idx]
                        .idle_stack
                        .pop(&self.next_idle)
                        .is_some()
                    {
                        self.idle_mask.clear(worker_id);
                    }
                    std::thread::yield_now();
                    continue;
                }

                let parker = Parker::from_inner(self.parker_inners[worker_id].clone());
                parker.park();

                // If we were unparked, we might have already been popped from the stack by the unparker.
                // But if we timed out or were unparked for other reasons, we might still be in the stack.
                // However, the current unparker logic ALWAYS pops from the stack.
                // So we just need to ensure idle_mask is cleared.
                self.idle_mask.clear(worker_id);
            }
        });
    }

    fn has_work(&self, worker_id: usize) -> bool {
        let worker = &self.workers[worker_id];
        !worker.send.is_empty() || worker.local_count.load(Ordering::Acquire) > 0
    }
}
