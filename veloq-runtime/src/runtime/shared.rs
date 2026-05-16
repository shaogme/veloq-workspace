use std::future::Future;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::ptr::NonNull;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicUsize, Ordering};
use std::sync::mpsc::{self, Receiver};
use std::task::{Context, Poll};

use super::context::{IdleHook, RuntimeContext, WorkerTickHook};
use crate::runtime::primitives::{self, Unparker};
use crate::scope::GenericScopeCompletion;
use crate::task::{LocalTaskRef, SendTaskRef, TaskHandleRef};
use crate::utils::FastRand;
use crate::utils::ownership::Ownership;
use crate::utils::storage::Storage;

pub(crate) mod infra;

use infra::{
    GlobalInjector, IdleController, IdleStack, NUMAGroup, RuntimeProgressCoordinator,
    TaskScheduler, TopologyContext, WorkerQueue, WorkerRegistry,
};

pub struct RuntimeSharedBase {
    pub(crate) registry: WorkerRegistry,
    pub(crate) topo: TopologyContext,
    pub(crate) scheduler: TaskScheduler,
    pub(crate) idle: IdleController,
    pub(crate) shutdown: AtomicBool,
    pub(crate) worker_tick_hook: Option<WorkerTickHook>,
}

pub struct RuntimeShared<T> {
    pub base: Arc<RuntimeSharedBase>,
    pub(crate) idle_hook: Option<IdleHook<T>>,
    pub context_tls: veloq_tls::Tls<RuntimeContext<T>>,
}

pub(crate) struct Receivers {
    pub(crate) local_receivers: Vec<Receiver<LocalTaskRef>>,
    pub(crate) remote_receivers: Vec<Receiver<SendTaskRef>>,
    pub(crate) pinned_receivers: Vec<Receiver<SendTaskRef>>,
}

pub(crate) fn init_runtime_components(
    worker_count: NonZeroUsize,
    queue_capacity: NonZeroUsize,
) -> (WorkerRegistry, TopologyContext, Receivers) {
    let worker_count_val = worker_count.get();
    let mut unparkers = Vec::with_capacity(worker_count_val);
    let mut parker_inners = Vec::with_capacity(worker_count_val);
    let mut local_receivers = Vec::with_capacity(worker_count_val);
    let mut remote_receivers = Vec::with_capacity(worker_count_val);
    let mut pinned_receivers = Vec::with_capacity(worker_count_val);
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
        let (ptx, prx) = mpsc::channel();
        local_receivers.push(lrx);
        remote_receivers.push(rrx);
        pinned_receivers.push(prx);
        workers.push(Arc::new(WorkerQueue::new(ltx, rtx, ptx, queue_capacity)));
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
            local_receivers,
            remote_receivers,
            pinned_receivers,
        },
    )
}

impl<T> RuntimeShared<T> {
    pub fn base(&self) -> &RuntimeSharedBase {
        self.base.as_ref()
    }

    pub(crate) fn new(
        registry: WorkerRegistry,
        topo: TopologyContext,
        worker_count: NonZeroUsize,
        idle_hook: Option<IdleHook<T>>,
        worker_tick_hook: Option<WorkerTickHook>,
    ) -> Self {
        Self {
            base: Arc::new(RuntimeSharedBase {
                registry,
                topo,
                scheduler: TaskScheduler {
                    injector: GlobalInjector::new(),
                    next_worker: AtomicUsize::new(0),
                    searching_workers: AtomicUsize::new(0),
                },
                idle: IdleController {
                    idle_mask: infra::AtomicBitset::new(worker_count.get()),
                    event_count: primitives::EventCount::new(),
                },
                shutdown: AtomicBool::new(false),
                worker_tick_hook,
            }),
            idle_hook,
            context_tls: veloq_tls::Tls::new(),
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

    pub fn enqueue_local(&self, worker_id: usize, task: LocalTaskRef) {
        if task.header().is_completed() {
            return;
        }
        if task.header().try_mark_queued() {
            let worker = &self.registry.workers[worker_id];
            worker.local_count.fetch_add(1, Ordering::Release);
            if worker.local_tx.send(task).is_err() {
                worker.local_count.fetch_sub(1, Ordering::Release);
                if task.header().clear_queued() {
                    task.header().acknowledge_completion();
                }
            } else {
                self.idle.event_count.notify();
                self.wake_worker(worker_id);
            }
        }
    }

    pub fn enqueue_pinned(&self, worker_id: usize, task: SendTaskRef) -> bool {
        if task.header().is_completed() {
            return false;
        }
        let worker_count = self.registry.workers.len();
        let worker_id = worker_id % worker_count;
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

    fn pop_pinned(&self, worker_id: usize, rx: &Receiver<SendTaskRef>) -> Option<SendTaskRef> {
        let res = rx.try_recv().ok();
        if res.is_some() {
            self.registry.workers[worker_id]
                .pinned_count
                .fetch_sub(1, Ordering::Release);
        }
        res
    }

    fn pop_global(&self) -> Option<SendTaskRef> {
        self.scheduler.pop_global()
    }

    fn steal_send(&self, thief_id: usize, rand: &FastRand) -> Option<SendTaskRef> {
        self.scheduler
            .steal_send(thief_id, &self.registry, &self.topo, rand)
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

    pub(crate) fn has_work(&self, worker_id: usize) -> bool {
        let worker = &self.registry.workers[worker_id];
        worker.lifo.load(Ordering::Acquire).is_some()
            || !worker.deque.is_empty()
            || worker.local_count.load(Ordering::Acquire) > 0
            || worker.pinned_count.load(Ordering::Acquire) > 0
    }

    pub fn enqueue_send(&self, worker_id: usize, task: SendTaskRef) {
        if task.header().is_completed() {
            return;
        }
        let worker_count = self.registry.workers.len();
        let worker_id = worker_id % worker_count;
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

impl<T: crate::runtime::context::RuntimeContextExtra> RuntimeShared<T> {
    pub fn worker_id(&self) -> usize {
        self.context_tls
            .get()
            .map(|ptr| unsafe { ptr.as_ref().worker_id })
            .unwrap_or(usize::MAX)
    }

    pub fn unparkers(&self) -> Box<[Unparker]> {
        self.base.unparkers()
    }

    pub fn choose_worker(&self) -> usize {
        let current = self
            .context_tls
            .get()
            .map(|ptr| unsafe { ptr.as_ref().worker_id })
            .unwrap_or(usize::MAX);
        self.base
            .topo
            .choose_worker_with_current(&self.base.scheduler.next_worker, current)
    }

    #[inline]
    pub fn worker_count(&self) -> NonZeroUsize {
        self.base.worker_count()
    }

    pub fn enqueue_local(&self, worker_id: usize, task: LocalTaskRef) {
        self.base.enqueue_local(worker_id, task)
    }

    pub fn enqueue_pinned(&self, worker_id: usize, task: SendTaskRef) -> bool {
        self.base.enqueue_pinned(worker_id, task)
    }

    #[inline]
    pub fn wake_worker(&self, worker_id: usize) {
        self.base.wake_worker(worker_id)
    }

    pub fn enqueue_send(&self, worker_id: usize, task: SendTaskRef) {
        if task.header().is_completed() {
            return;
        }
        let worker_count = self.base.registry.workers.len();
        let worker_id = worker_id % worker_count;

        let current = self
            .context_tls
            .get()
            .map(|ptr| unsafe { ptr.as_ref().worker_id })
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

            if worker.deque.push(task).is_ok() {
                self.wake_worker(worker_id);
                return;
            }

            // Fallback to remote_tx if deque is full
            if worker.remote_tx.send(task).is_err() {
                self.base.scheduler.injector.push(task);
            }
            self.wake_worker(worker_id);
            return;
        }

        self.base.enqueue_send(worker_id, task);
    }

    pub fn shutdown(&self) {
        self.base.shutdown();
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
        let ctx_ptr = self.context_tls.get().expect("runtime context missing");
        let ctx = unsafe { ctx_ptr.as_ref() };
        let worker_id = ctx.worker_id;

        let worker_tick_hook = self.base.worker_tick_hook;

        let waker =
            primitives::create_unpark_waker(self.base.registry.unparkers[worker_id].clone());
        let mut init_cx = Context::from_waker(&waker);

        let mut tick = 0u32;
        const INJECTOR_CHECK_INTERVAL: u32 = 61;

        while init_fut.is_some() || !self.base.shutdown.load(Ordering::Acquire) {
            let mut progressed = false;

            if let Some(hook) = worker_tick_hook {
                hook();
            }

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

            if let Some(task) = self.base.pop_send(worker_id) {
                self.base.poll_send_task(worker_id, task);
                progressed = true;
            }

            if !progressed && let Some(task) = self.base.pop_pinned(worker_id, &ctx.pinned_rx) {
                self.base.poll_send_task(worker_id, task);
                progressed = true;
            }

            if !progressed && let Some(task) = self.base.pop_local(worker_id, &ctx.local_rx) {
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
                continue;
            }

            self.base
                .scheduler
                .searching_workers
                .fetch_add(1, Ordering::Relaxed);
            for _ in 0..4 {
                if let Some(task) = self.base.steal_send(worker_id, &ctx.rand) {
                    self.base.poll_send_task(worker_id, task);
                    progressed = true;
                    break;
                }
                std::hint::spin_loop();
            }
            self.base
                .scheduler
                .searching_workers
                .fetch_sub(1, Ordering::Relaxed);

            if progressed {
                continue;
            }

            if let Some(c) = completion {
                c.register(&waker);
            }
            RuntimeProgressCoordinator::new(self, worker_id).run(completion);
        }
    }
}
