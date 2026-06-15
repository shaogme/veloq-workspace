use std::{
    hint::spin_loop,
    num::NonZeroUsize,
    ptr::NonNull,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU32, AtomicUsize, Ordering},
        mpsc::{self, Receiver},
    },
};

use crossbeam_deque::Worker;
use diagweave::prelude::*;
use numaperf_topo::Topology;
use veloq_storage::StateOptionPtr;
use veloq_tls::Tls;

use super::context::{IdleHook, RuntimeContext, WorkerTickHook};
use crate::{
    error::{Result, RuntimeError},
    runtime::primitives::{EventCount, ParkerInner, Unparker, create_unpark_waker},
    scope::{GenericScopeCompletion, ScopeCompletionRegistration},
    task::{LocalTaskRef, ScopeStorage, SendTaskRef, TaskHandleRef},
    utils::{FastRand, ownership::Ownership},
};

pub(crate) mod infra;

use infra::{
    AtomicBitset, GlobalInjector, IdleController, IdleStack, NUMAGroup, RuntimeProgressCoordinator,
    TaskScheduler, TopologyContext, WorkerQueue, WorkerRegistry,
};

pub struct RuntimeSharedBase {
    pub(crate) registry: WorkerRegistry,
    pub(crate) topo: TopologyContext,
    pub(crate) scheduler: TaskScheduler,
    pub(crate) idle: IdleController,
    pub(crate) shutdown: AtomicBool,
    pub(crate) worker_tick_hook: Option<WorkerTickHook>,
    /// Worker 线程核心上下文（不含用户 extra 状态）。
    pub(crate) tls: Tls<RuntimeContext>,
}

pub struct RuntimeShared<T> {
    pub base: RuntimeSharedBase,
    pub(crate) idle_hook: Option<IdleHook<T>>,
    /// Worker 线程用户自定义 extra 状态。
    pub extra_tls: Tls<T>,
}

unsafe impl<T> Send for RuntimeShared<T> {}
unsafe impl<T> Sync for RuntimeShared<T> {}
unsafe impl Send for RuntimeSharedBase {}
unsafe impl Sync for RuntimeSharedBase {}

pub(crate) struct Receivers {
    pub(crate) remote_receivers: Vec<Receiver<SendTaskRef>>,
    pub(crate) pinned_receivers: Vec<Receiver<SendTaskRef>>,
    pub(crate) local_receivers: Vec<Receiver<LocalTaskRef>>,
    pub(crate) deques: Vec<Worker<SendTaskRef>>,
}

pub(crate) fn init_runtime_components(
    worker_count: NonZeroUsize,
    _queue_capacity: NonZeroUsize,
) -> (WorkerRegistry, TopologyContext, Receivers) {
    let worker_count_val = worker_count.get();
    let mut unparkers = Vec::with_capacity(worker_count_val);
    let mut parker_inners = Vec::with_capacity(worker_count_val);
    let mut remote_receivers = Vec::with_capacity(worker_count_val);
    let mut pinned_receivers = Vec::with_capacity(worker_count_val);
    let mut local_receivers = Vec::with_capacity(worker_count_val);
    let mut deques = Vec::with_capacity(worker_count_val);
    let mut workers = Vec::with_capacity(worker_count_val);
    let mut next_idle = Vec::with_capacity(worker_count_val);

    for _ in 0..worker_count_val {
        let inner = Arc::new(ParkerInner {
            state: AtomicU32::new(0),
        });
        unparkers.push(Unparker::from_inner(inner.clone()));
        parker_inners.push(inner);

        let (rtx, rrx) = mpsc::channel();
        let (ptx, prx) = mpsc::channel();
        let (ltx, lrx) = mpsc::channel();
        remote_receivers.push(rrx);
        pinned_receivers.push(prx);
        local_receivers.push(lrx);

        let worker_deque = Worker::new_lifo();
        let stealer = worker_deque.stealer();
        deques.push(worker_deque);

        workers.push(Arc::new(WorkerQueue::new(rtx, ptx, ltx, stealer)));
        next_idle.push(AtomicUsize::new(usize::MAX));
    }

    // NUMA detection
    let topo_info = Topology::discover().ok();
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
        WorkerRegistry {
            workers: workers.into_boxed_slice(),
            unparkers: unparkers.into_boxed_slice(),
            parker_inners: parker_inners.into_boxed_slice(),
        },
        TopologyContext {
            groups,
            worker_to_group,
            next_idle,
        },
        Receivers {
            remote_receivers,
            pinned_receivers,
            local_receivers,
            deques,
        },
    )
}

impl<T> RuntimeShared<T> {
    pub fn base(&self) -> &RuntimeSharedBase {
        &self.base
    }

    pub(crate) fn new(
        registry: WorkerRegistry,
        topo: TopologyContext,
        worker_count: NonZeroUsize,
        idle_hook: Option<IdleHook<T>>,
        worker_tick_hook: Option<WorkerTickHook>,
    ) -> Self {
        Self {
            base: RuntimeSharedBase {
                registry,
                topo,
                scheduler: TaskScheduler {
                    injector: GlobalInjector::new(),
                    next_worker: AtomicUsize::new(0),
                },
                idle: IdleController {
                    idle_mask: AtomicBitset::new(worker_count.get()),
                    event_count: EventCount::new(),
                },
                shutdown: AtomicBool::new(false),
                worker_tick_hook,
                tls: Tls::new(|| panic!("RuntimeContext accessed outside of a worker thread")),
            },
            idle_hook,
            extra_tls: Tls::new(|| panic!("extra TLS accessed outside of a worker thread")),
        }
    }
}

impl RuntimeSharedBase {
    pub fn unparkers(&self) -> Box<[Unparker]> {
        self.registry.unparkers.clone()
    }

    #[inline]
    pub fn worker_count(&self) -> NonZeroUsize {
        NonZeroUsize::new(self.registry.workers.len())
            .expect("runtime must have at least one worker")
    }

    #[inline]
    pub fn validate_worker_id(&self, worker_id: usize) -> Result<()> {
        let worker_count = self.worker_count().get();
        if worker_id < worker_count {
            return Ok(());
        }

        RuntimeError::WorkerIdOutOfBounds {
            worker_id,
            worker_count,
        }
        .with_category("runtime.dispatch")
    }

    #[inline]
    fn assert_worker_id(&self, worker_id: usize) {
        let worker_count = self.registry.workers.len();
        assert!(
            worker_id < worker_count,
            "worker_id {worker_id} is out of bounds (worker_count: {worker_count})"
        );
    }

    /// 将本地任务入队当前线程的本地队列。
    pub fn enqueue_local(&self, worker_id: usize, task: LocalTaskRef) {
        if task.header().is_completed() {
            return;
        }
        if task.header().try_mark_queued() {
            let worker = &self.registry.workers[worker_id];
            worker.local_count.fetch_add(1, Ordering::Release);
            if worker.local_tx.send(task).is_ok() {
                task.header().notify_runtime_active();
            } else {
                worker.local_count.fetch_sub(1, Ordering::Release);
            }
        }
    }

    pub fn enqueue_pinned(&self, worker_id: usize, task: SendTaskRef) -> bool {
        self.assert_worker_id(worker_id);
        if task.header().is_completed() {
            return false;
        }
        if task.header().try_mark_queued() {
            self.idle.event_count.notify();
            let worker = &self.registry.workers[worker_id];
            worker.pinned_count.fetch_add(1, Ordering::Release);
            if worker.pinned_tx.send(task).is_err() {
                worker.pinned_count.fetch_sub(1, Ordering::Release);
                if task.header().clear_queued() {
                    task.header().acknowledge_completion();
                }
                return false;
            }
            self.wake_worker(worker_id);
        }
        true
    }

    #[inline]
    pub fn wake_worker(&self, worker_id: usize) {
        self.registry.unpark(worker_id);
    }

    fn fn_pop_send(&self, worker_id: usize) -> Option<SendTaskRef> {
        let worker = &self.registry.workers[worker_id];
        if let Some(header) = worker.lifo.swap(None, Ordering::AcqRel) {
            return Some(unsafe { SendTaskRef::from_header(header.as_ptr()) });
        }
        self.tls.with(|ctx| ctx.worker.pop())
    }

    fn fn_pop_pinned(&self, worker_id: usize, rx: &Receiver<SendTaskRef>) -> Option<SendTaskRef> {
        let res = rx.try_recv().ok();
        if res.is_some() {
            self.registry.workers[worker_id]
                .pinned_count
                .fetch_sub(1, Ordering::Release);
        }
        res
    }

    fn fn_pop_local(&self, worker_id: usize, rx: &Receiver<LocalTaskRef>) -> Option<LocalTaskRef> {
        let res = rx.try_recv().ok();
        if res.is_some() {
            self.registry.workers[worker_id]
                .local_count
                .fetch_sub(1, Ordering::Release);
        }
        res
    }

    fn pop_global(&self) -> Option<SendTaskRef> {
        self.scheduler.pop_global()
    }

    fn steal_send(&self, thief_id: usize, rand: &FastRand) -> Option<SendTaskRef> {
        self.tls.with(|ctx| {
            self.scheduler
                .steal_send(thief_id, &self.registry, &self.topo, rand, &ctx.worker)
        })
    }

    fn poll_local_task(&self, worker_id: usize, task: LocalTaskRef) {
        if task.header().clear_queued() {
            task.header().acknowledge_completion();
        } else {
            let _ = task.poll_task(worker_id);
        }
    }

    pub(crate) fn poll_send_task(&self, worker_id: usize, task: SendTaskRef) {
        if task.header().clear_queued() {
            task.header().acknowledge_completion();
        } else {
            let _ = task.poll_task(worker_id);
        }
    }

    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Release);
        for i in 0..self.registry.unparkers.len() {
            self.registry.unpark(i);
        }
    }

    pub fn enqueue_send(&self, worker_id: usize, task: SendTaskRef) {
        self.assert_worker_id(worker_id);
        if task.header().is_completed() {
            return;
        }
        if task.header().try_mark_queued() {
            self.idle.event_count.notify();
            let worker = &self.registry.workers[worker_id];
            if worker.remote_tx.send(task).is_err() {
                self.scheduler.injector.push(task);
            }
            self.wake_worker(worker_id);
        }
    }
}

impl<T> RuntimeShared<T> {
    pub fn worker_id(&self) -> usize {
        self.base
            .tls
            .try_with(|ctx| ctx.worker_id)
            .unwrap_or(usize::MAX)
    }

    pub fn unparkers(&self) -> Box<[Unparker]> {
        self.base.unparkers()
    }

    pub fn choose_worker(&self) -> usize {
        let current = self
            .base
            .tls
            .try_with(|ctx| ctx.worker_id)
            .unwrap_or(usize::MAX);
        self.base
            .topo
            .choose_worker_with_current(&self.base.scheduler.next_worker, current)
    }

    #[inline]
    pub fn worker_count(&self) -> NonZeroUsize {
        self.base.worker_count()
    }

    #[inline]
    pub fn validate_worker_id(&self, worker_id: usize) -> Result<()> {
        self.base.validate_worker_id(worker_id)
    }

    pub fn enqueue_local(&self, worker_id: usize, task: LocalTaskRef) {
        self.base.enqueue_local(worker_id, task);
    }

    pub(crate) fn has_work(&self, worker_id: usize) -> bool {
        let worker = &self.base.registry.workers[worker_id];
        let local_has_work = worker.local_count.load(Ordering::Acquire) > 0;
        worker.lifo.load(Ordering::Acquire).is_some()
            || !worker.stealer.is_empty()
            || local_has_work
            || worker.pinned_count.load(Ordering::Acquire) > 0
    }

    pub fn enqueue_pinned(&self, worker_id: usize, task: SendTaskRef) -> bool {
        self.base.enqueue_pinned(worker_id, task)
    }

    #[inline]
    pub fn wake_worker(&self, worker_id: usize) {
        self.base.wake_worker(worker_id)
    }

    pub fn enqueue_send(&self, worker_id: usize, task: SendTaskRef) {
        self.base.assert_worker_id(worker_id);
        if task.header().is_completed() {
            return;
        }

        let current = self
            .base
            .tls
            .try_with(|ctx| ctx.worker_id)
            .unwrap_or(usize::MAX);

        if current == worker_id && task.header().try_mark_queued() {
            self.base.idle.event_count.notify();

            let worker = &self.base.registry.workers[worker_id];
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
                self.wake_worker(worker_id);
                return;
            }

            self.base.tls.with(|ctx| {
                ctx.worker.push(task);
            });
            self.wake_worker(worker_id);
            return;
        }

        self.base.enqueue_send(worker_id, task);
    }

    pub fn shutdown(&self) {
        self.base.shutdown();
    }

    pub fn drive_worker<'a, S: ScopeStorage, O: Ownership + 'a>(
        &self,
        completion: Option<&O::Shared<GenericScopeCompletion<S, O>>>,
    ) {
        self.base.tls.with(move |ctx| {
            let worker_id = ctx.worker_id;

            let worker_tick_hook = self.base.worker_tick_hook;

            let waker = create_unpark_waker(self.base.registry.unparkers[worker_id].clone());
            let mut completion_registration =
                completion.map(|c| ScopeCompletionRegistration::new(&**c, &waker));

            let mut tick = 0u32;
            const INJECTOR_CHECK_INTERVAL: u32 = 61;
            let mut processed_tasks = 0u32;

            while !self.base.shutdown.load(Ordering::Acquire) {
                let mut progressed = false;

                if let Some(hook) = worker_tick_hook {
                    hook();
                }

                if completion.map(|c| c.is_done()).unwrap_or(false) {
                    return;
                }

                if completion.is_none() && worker_id == 0 {
                    return;
                }

                tick = tick.wrapping_add(1);

                if processed_tasks >= 64 {
                    processed_tasks = 0;
                    if let Some(task) = self.base.pop_global() {
                        self.base.poll_send_task(worker_id, task);
                        progressed = true;
                    }
                }

                if !progressed && let Some(task) = self.base.fn_pop_send(worker_id) {
                    self.base.poll_send_task(worker_id, task);
                    progressed = true;
                }

                if !progressed
                    && let Some(task) = self.base.fn_pop_pinned(worker_id, &ctx.pinned_rx)
                {
                    self.base.poll_send_task(worker_id, task);
                    progressed = true;
                }

                if !progressed && let Some(task) = self.base.fn_pop_local(worker_id, &ctx.local_rx)
                {
                    self.base.poll_local_task(worker_id, task);
                    progressed = true;
                }

                if !progressed
                    && tick.is_multiple_of(INJECTOR_CHECK_INTERVAL)
                    && let Some(task) = self.base.pop_global()
                {
                    self.base.poll_send_task(worker_id, task);
                    progressed = true;
                }

                if !progressed && let Ok(task) = ctx.remote_rx.try_recv() {
                    self.base.poll_send_task(worker_id, task);
                    progressed = true;
                }

                if progressed {
                    processed_tasks = processed_tasks.wrapping_add(1);
                    continue;
                }

                for _ in 0..4 {
                    if let Some(task) = self.base.steal_send(worker_id, &ctx.rand) {
                        self.base.poll_send_task(worker_id, task);
                        progressed = true;
                        break;
                    }
                    spin_loop();
                }

                if progressed {
                    processed_tasks = processed_tasks.wrapping_add(1);
                    continue;
                }

                if let Some(registration) = completion_registration.as_mut() {
                    registration.register(&waker);
                }
                RuntimeProgressCoordinator::new(self, worker_id).run(completion.map(|c| &**c));
            }
        })
    }
}
