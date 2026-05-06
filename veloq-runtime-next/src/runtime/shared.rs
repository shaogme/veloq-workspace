use super::context::{CONTEXT, current_worker_id};
use super::primitives::{self, EventCount, Parker, Unparker};
use crate::scope::GenericScopeCompletion;
use crate::task::{LocalTaskRef, SendTaskRef, TaskHandleRef, TaskHeader};
use crate::utils::ownership::Ownership;
use crate::utils::storage::Storage;
use crate::utils::{AtomicOptionPtr, Deque, Steal};
use veloq_sync::mpmc::{self, Receiver as MpmcReceiver, Sender as MpmcSender};
use std::num::NonZeroUsize;
use std::ptr::NonNull;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::sync::mpsc::{self, Receiver, Sender};
use std::time::Duration;

pub(crate) struct WorkerQueue {
    pub(crate) local_tx: Sender<LocalTaskRef>,
    pub(crate) remote_tx: Sender<SendTaskRef>,
    pub(crate) local_count: AtomicUsize,
    /// LIFO slot for high-priority task (cache locality)
    pub(crate) lifo: AtomicOptionPtr<TaskHeader>,
    /// Chase-Lev Deque for work-stealing
    pub(crate) deque: Deque<SendTaskRef>,
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

pub(crate) struct WorkerRegistry {
    pub(crate) workers: Vec<Arc<WorkerQueue>>,
    pub(crate) unparkers: Vec<Unparker>,
    pub(crate) parker_inners: Vec<Arc<primitives::ParkerInner>>,
}

impl WorkerRegistry {
    #[inline]
    fn unpark(&self, worker_id: usize) {
        self.unparkers[worker_id].unpark();
    }
}

pub(crate) struct TopologyContext {
    pub(crate) groups: Vec<NUMAGroup>,
    pub(crate) worker_to_group: Vec<usize>,
    pub(crate) next_idle: Vec<AtomicUsize>,
}

impl TopologyContext {
    fn choose_worker(&self, next_worker: &AtomicUsize) -> usize {
        let n = self.worker_to_group.len();
        if n <= 1 {
            return 0;
        }

        let current = current_worker_id();
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

    fn pop_idle(&self, preferred_worker_id: usize) -> Option<usize> {
        let group_idx = self.worker_to_group[preferred_worker_id];
        if let Some(idle_id) = self.groups[group_idx].idle_stack.pop(&self.next_idle) {
            return Some(idle_id);
        }

        for (i, group) in self.groups.iter().enumerate() {
            if i == group_idx {
                continue;
            }
            if let Some(idle_id) = group.idle_stack.pop(&self.next_idle) {
                return Some(idle_id);
            }
        }
        None
    }
}

pub(crate) struct TaskScheduler {
    pub(crate) injector: (MpmcSender<SendTaskRef>, MpmcReceiver<SendTaskRef>),
    pub(crate) next_worker: AtomicUsize,
    pub(crate) searching_workers: AtomicUsize,
}

impl TaskScheduler {
    fn pop_global(&self) -> Option<SendTaskRef> {
        self.injector.1.try_recv().ok()
    }

    fn steal_send(
        &self,
        thief_id: usize,
        registry: &WorkerRegistry,
        topo: &TopologyContext,
    ) -> Option<SendTaskRef> {
        let thief_worker = &registry.workers[thief_id];
        let num_workers = registry.workers.len();
        if num_workers <= 1 {
            return self.pop_global();
        }

        let group_idx = topo.worker_to_group[thief_id];
        let group = &topo.groups[group_idx];

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
                match registry.workers[victim].deque.steal_batch(&thief_worker.deque) {
                    Steal::Success(task) => return Some(task),
                    Steal::Retry => return self.steal_send(thief_id, registry, topo),
                    Steal::Empty => continue,
                }
            }
        }

        if let Some(task) = self.pop_global() {
            return Some(task);
        }

        let start_group = CONTEXT.with(|ctx| {
            ctx.borrow()
                .as_ref()
                .map(|c| c.rand.borrow_mut().next_u32(topo.groups.len() as u32) as usize)
                .unwrap_or(0)
        });

        for i in 0..topo.groups.len() {
            let other_group_idx = (start_group + i) % topo.groups.len();
            if other_group_idx == group_idx {
                continue;
            }
            let other_group = &topo.groups[other_group_idx];
            for &victim in &other_group.worker_ids {
                match registry.workers[victim].deque.steal_batch(&thief_worker.deque) {
                    Steal::Success(task) => return Some(task),
                    Steal::Retry => return self.steal_send(thief_id, registry, topo),
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

pub struct RuntimeShared {
    pub(crate) registry: WorkerRegistry,
    pub(crate) topo: TopologyContext,
    pub(crate) scheduler: TaskScheduler,
    pub(crate) idle: IdleController,
    pub(crate) shutdown: AtomicBool,
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
        let topo_info = numaperf_topo::Topology::discover().ok();
        let mut groups = Vec::new();
        let mut worker_to_group = vec![0; worker_count_val];

        match topo_info {
            Some(t) if t.node_count() > 0 => {
                let node_count = t.node_count();
                let mut node_to_workers: Vec<Vec<usize>> = vec![Vec::new(); node_count];

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
                groups.push(NUMAGroup {
                    worker_ids: (0..worker_count_val).collect(),
                    idle_stack: IdleStack::new(),
                });
            }
        }

        (
            Self {
                registry: WorkerRegistry {
                    workers,
                    unparkers,
                    parker_inners,
                },
                topo: TopologyContext {
                    groups,
                    worker_to_group,
                    next_idle,
                },
                scheduler: TaskScheduler {
                    injector: mpmc::unbounded(),
                    next_worker: AtomicUsize::new(0),
                    searching_workers: AtomicUsize::new(0),
                },
                idle: IdleController {
                    idle_mask: AtomicBitset::new(worker_count_val),
                    event_count: EventCount::new(),
                },
                shutdown: AtomicBool::new(false),
            },
            local_receivers,
            remote_receivers,
        )
    }

    pub fn choose_worker(&self) -> usize {
        self.topo.choose_worker(&self.scheduler.next_worker)
    }

    #[inline]
    pub fn worker_count(&self) -> NonZeroUsize {
        NonZeroUsize::new(self.registry.workers.len()).expect("runtime must have at least one worker")
    }

    pub fn enqueue_local(&self, worker_id: usize, task: LocalTaskRef) {
        if task.header().is_completed() {
            return;
        }
        if task.header().try_mark_queued() {
            let worker = &self.registry.workers[worker_id];
            worker.local_count.fetch_add(1, Ordering::Release);
            let _ = worker.local_tx.send(task);
            self.idle.event_count.notify();
            self.notify_work(worker_id);
        }
    }

    fn notify_work(&self, worker_id: usize) {
        if self.scheduler.searching_workers.load(Ordering::Acquire) > 0 {
            return;
        }
        self.unpark_worker(worker_id);
    }

    fn unpark_worker(&self, worker_id: usize) {
        if self.idle.idle_mask.is_set(worker_id) {
            self.idle.idle_mask.clear(worker_id);
            self.registry.unpark(worker_id);
            return;
        }

        if let Some(idle_id) = self.topo.pop_idle(worker_id) {
            self.idle.idle_mask.clear(idle_id);
            self.registry.unpark(idle_id);
        }
    }

    pub fn enqueue_send(&self, worker_id: usize, task: SendTaskRef) {
        if task.header().is_completed() {
            return;
        }
        let worker_count = self.registry.workers.len();
        let worker_id = worker_id % worker_count;
        if task.header().try_mark_queued() {
            self.idle.event_count.notify();

            if task.header().is_affine() {
                let worker = &self.registry.workers[worker_id];
                let _ = worker.remote_tx.send(task);
                self.notify_work(worker_id);
                return;
            }

            let current = current_worker_id();

            if current == worker_id {
                let worker = &self.registry.workers[worker_id];
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
                    self.notify_work(worker_id);
                    return;
                }

                if worker.deque.push(task).is_ok() {
                    self.notify_work(worker_id);
                    return;
                }
            }

            let worker = &self.registry.workers[worker_id];
            if worker.remote_tx.send(task).is_err() {
                let _ = self.scheduler.injector.0.try_send(task);
            }
            self.notify_work(worker_id);
        }
    }

    fn pop_local(&self, worker_id: usize, rx: &Receiver<LocalTaskRef>) -> Option<LocalTaskRef> {
        let res = rx.try_recv().ok();
        if res.is_some() {
            self.registry.workers[worker_id]
                .local_count
                .fetch_sub(1, Ordering::Release);
        }
        res
    }

    fn pop_send(&self, worker_id: usize) -> Option<SendTaskRef> {
        let worker = &self.registry.workers[worker_id];
        if let Some(header) = worker.lifo.swap(None, Ordering::AcqRel) {
            return Some(unsafe { SendTaskRef::from_header(header.as_ptr()) });
        }
        worker.deque.pop()
    }

    fn pop_global(&self) -> Option<SendTaskRef> {
        self.scheduler.pop_global()
    }

    fn steal_send(&self, thief_id: usize) -> Option<SendTaskRef> {
        self.scheduler.steal_send(thief_id, &self.registry, &self.topo)
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
        for i in 0..self.registry.unparkers.len() {
            self.registry.unpark(i);
        }
    }

    pub fn drive_worker<S: Storage, O: Ownership>(
        &self,
        completion: Option<&GenericScopeCompletion<S, O>>,
    ) {
        self.drive_worker_with_init::<S, O, std::future::Ready<()>>(completion, None);
    }

    pub fn drive_worker_with_init<S: Storage, O: Ownership, F: Future<Output = ()>>(
        &self,
        completion: Option<&GenericScopeCompletion<S, O>>,
        mut init_fut: Option<Pin<&mut F>>,
    ) {
        let worker_id = current_worker_id();

        CONTEXT.with(|ctx| {
            let ctx = ctx.borrow();
            let ctx = ctx.as_ref().expect("runtime context missing");

            let waker = primitives::create_unpark_waker(self.registry.unparkers[worker_id].clone());
            let mut init_cx = Context::from_waker(&waker);

            let mut tick = 0u32;
            const INJECTOR_CHECK_INTERVAL: u32 = 61;

            while init_fut.is_some() || !self.shutdown.load(Ordering::Acquire) {
                let mut progressed = false;

                if let Some(fut) = init_fut.as_mut() {
                    match fut.as_mut().poll(&mut init_cx) {
                        Poll::Ready(()) => {
                            init_fut = None;
                            progressed = true;
                            if completion.is_none() && worker_id == 0 {
                                return;
                            }
                        }
                        Poll::Pending => {}
                    }
                }

                if init_fut.is_none() && completion.map(|c| c.is_done()).unwrap_or(false) {
                    return;
                }

                if init_fut.is_none() && completion.is_none() && worker_id == 0 {
                    return;
                }

                tick = tick.wrapping_add(1);

                if let Some(task) = self.pop_send(worker_id) {
                    self.poll_send_task(worker_id, task);
                    progressed = true;
                }

                if !progressed && let Some(task) = self.pop_local(worker_id, &ctx.local_rx) {
                    self.poll_local_task(worker_id, task);
                    progressed = true;
                }

                if !progressed
                    && tick.is_multiple_of(INJECTOR_CHECK_INTERVAL)
                    && let Some(task) = self.pop_global()
                {
                    self.poll_send_task(worker_id, task);
                    progressed = true;
                }

                if !progressed && let Ok(task) = ctx.remote_rx.try_recv() {
                    self.poll_send_task(worker_id, task);
                    progressed = true;
                }

                if progressed {
                    continue;
                }

                self.scheduler.searching_workers.fetch_add(1, Ordering::Relaxed);
                for _ in 0..4 {
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
                self.scheduler.searching_workers.fetch_sub(1, Ordering::Relaxed);

                if progressed {
                    continue;
                }

                let seq = self.idle.event_count.load();
                self.idle.idle_mask.set(worker_id);
                let group_idx = self.topo.worker_to_group[worker_id];
                self.topo.groups[group_idx]
                    .idle_stack
                    .push(worker_id, &self.topo.next_idle);

                let idle_hint = super::context::run_worker_idle_hook();

                if self.idle.event_count.load() != seq
                    || self.has_work(worker_id)
                    || self.shutdown.load(Ordering::Acquire)
                    || completion.map(|c| c.is_done()).unwrap_or(false)
                {
                    if self.topo.groups[group_idx]
                        .idle_stack
                        .pop(&self.topo.next_idle)
                        .is_some()
                    {
                        self.idle.idle_mask.clear(worker_id);
                    }
                    continue;
                }

                if let Ok(task) = ctx.remote_rx.try_recv() {
                    if self.topo.groups[group_idx]
                        .idle_stack
                        .pop(&self.topo.next_idle)
                        .is_some()
                    {
                        self.idle.idle_mask.clear(worker_id);
                    }
                    self.poll_send_task(worker_id, task);
                    continue;
                }
                if let Some(task) = self.pop_global() {
                    if self.topo.groups[group_idx]
                        .idle_stack
                        .pop(&self.topo.next_idle)
                        .is_some()
                    {
                        self.idle.idle_mask.clear(worker_id);
                    }
                    self.poll_send_task(worker_id, task);
                    continue;
                }

                let parker = Parker::from_inner(self.registry.parker_inners[worker_id].clone());
                if let Some(duration) = idle_hint {
                    let _ = parker.park_timeout(duration);
                } else if completion.is_some() {
                    let _ = parker.park_timeout(Duration::from_millis(1));
                } else {
                    parker.park();
                }

                let _ = self.topo.groups[group_idx].idle_stack.pop(&self.topo.next_idle);
                self.idle.idle_mask.clear(worker_id);
            }
        });
    }

    fn has_work(&self, worker_id: usize) -> bool {
        let worker = &self.registry.workers[worker_id];
        worker.lifo.load(Ordering::Acquire).is_some()
            || !worker.deque.is_empty()
            || worker.local_count.load(Ordering::Acquire) > 0
    }
}
