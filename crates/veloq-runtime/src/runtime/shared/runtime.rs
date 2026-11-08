use crossbeam_deque::Worker;
use crossbeam_queue::ArrayQueue;
use numaperf_topo::Topology;
use veloq_std::{
    num::NonZeroUsize,
    ptr::NonNull,
    sync::{NativeArc as Arc, atomic::Ordering},
    vec,
    vec::Vec,
};
use veloq_storage::StateOptionPtr;
use veloq_tls::Tls;

use super::{
    EnqueuePinnedOutcome, RuntimeBaseConfig, RuntimeSharedBase,
    infra::{IdleStack, NUMAGroup, TopologyContext, WorkerQueue, WorkerRegistry},
    worker_loop::{ScopeJoinController, ShutdownController, WorkerLoop},
};
use crate::{
    error::Result,
    runtime::{
        Unparker,
        context::{IdleHook, ParkHook, WorkerTickHook},
        primitives::{ShutdownCoordinator, WakeFailureState},
    },
    scope::{GenericScopeCompletion, ScopeBlockingWaiter},
    task::{LocalTaskRef, ScopeStorage, SendTaskRef, TaskHandleRef},
    utils::Ownership,
};

/// Worker 进入 idle 等待阶段时调用的 hook。
///
/// Hook 接收与 worker factory 使用同一个 `T` 的 [`RuntimeShared`]，并获得运行时选定的
/// [`IdleWaitStrategy`]。它必须实现实际的等待/驱动 park 逻辑，并通过同一个 runtime
/// 的 `Unparker` 支持唤醒。
pub struct RuntimeShared<T> {
    base: RuntimeSharedBase,
    idle_hook: Option<IdleHook<T>>,
    park_hook: Option<ParkHook<T>>,
    extra_tls: Tls<T>,
}

pub(crate) struct Receivers {
    pub(crate) deques: Vec<Worker<SendTaskRef>>,
}

pub(crate) struct RuntimeComponents {
    pub(crate) registry: WorkerRegistry,
    pub(crate) topology: TopologyContext,
    pub(crate) receivers: Receivers,
}

pub(crate) struct RuntimeSharedConfig<T> {
    pub(crate) registry: WorkerRegistry,
    pub(crate) topology: TopologyContext,
    pub(crate) worker_count: NonZeroUsize,
    pub(crate) idle_hook: Option<IdleHook<T>>,
    pub(crate) park_hook: Option<ParkHook<T>>,
    pub(crate) worker_tick_hook: Option<WorkerTickHook>,
}

fn init_runtime_components_with_topology(
    worker_count: NonZeroUsize,
    queue_capacity: NonZeroUsize,
    topology_override: Option<&[usize]>,
) -> RuntimeComponents {
    let worker_count_val = worker_count.get();
    let shutdown = Arc::new(ShutdownCoordinator::new(worker_count_val));
    let wake_failure = Arc::new(WakeFailureState::new(Arc::downgrade(&shutdown)));
    let mut unparkers = Vec::with_capacity(worker_count_val);
    let mut deques = Vec::with_capacity(worker_count_val);
    let mut workers = Vec::with_capacity(worker_count_val);

    for _ in 0..worker_count_val {
        unparkers.push(Unparker::with_failure_state(wake_failure.clone()));

        let remote_queue = ArrayQueue::new(queue_capacity.get());
        let pinned_queue = ArrayQueue::new(queue_capacity.get());
        let local_queue = ArrayQueue::new(queue_capacity.get());

        let worker_deque = Worker::new_lifo();
        let stealer = worker_deque.stealer();
        deques.push(worker_deque);

        workers.push(WorkerQueue::new(
            remote_queue,
            pinned_queue,
            local_queue,
            queue_capacity.get(),
            stealer,
        ));
    }
    shutdown.install_shutdown_targets(unparkers.clone().into());

    let topology = if let Some(worker_to_group) = topology_override {
        TopologyContext::from_worker_to_group(worker_to_group)
    } else {
        let topo_info = Topology::discover().ok();
        let mut groups = Vec::new();
        let mut worker_to_group = vec![0; worker_count_val];

        match topo_info {
            Some(t) if t.node_count() > 0 => {
                let node_count = t.node_count();
                let mut node_to_workers: Vec<Vec<usize>> = vec![Vec::new(); node_count];

                for i in 0..worker_count_val {
                    let node_idx = i % node_count;
                    node_to_workers[node_idx].push(i);
                }

                for worker_ids in node_to_workers.into_iter() {
                    if !worker_ids.is_empty() {
                        let group_idx = groups.len();
                        for &worker_id in &worker_ids {
                            worker_to_group[worker_id] = group_idx;
                        }
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
        TopologyContext::from_groups(groups, worker_to_group, worker_count_val)
    };

    RuntimeComponents {
        registry: WorkerRegistry::new(
            workers.into_boxed_slice(),
            unparkers.into_boxed_slice(),
            wake_failure,
            shutdown,
        ),
        topology,
        receivers: Receivers { deques },
    }
}

impl<T> RuntimeShared<T> {
    pub fn base(&self) -> &RuntimeSharedBase {
        &self.base
    }

    pub fn extra_tls(&self) -> &Tls<T> {
        &self.extra_tls
    }

    pub(crate) fn idle_hook(&self) -> Option<IdleHook<T>> {
        self.idle_hook
    }

    pub(crate) fn park_hook(&self) -> Option<ParkHook<T>> {
        self.park_hook
    }

    pub(crate) fn initialize_components(
        worker_count: NonZeroUsize,
        queue_capacity: NonZeroUsize,
        topology_override: Option<&[usize]>,
    ) -> RuntimeComponents {
        init_runtime_components_with_topology(worker_count, queue_capacity, topology_override)
    }

    pub(crate) fn new(config: RuntimeSharedConfig<T>) -> Self {
        let RuntimeSharedConfig {
            registry,
            topology,
            worker_count,
            idle_hook,
            park_hook,
            worker_tick_hook,
        } = config;
        let base = RuntimeSharedBase::new(RuntimeBaseConfig {
            registry,
            topology,
            worker_count,
            worker_tick_hook,
        });
        Self {
            base,
            idle_hook,
            park_hook,
            extra_tls: Tls::new(),
        }
    }
}

impl<T> RuntimeShared<T> {
    pub fn worker_id(&self) -> usize {
        self.base
            .tls()
            .try_with(|ctx| ctx.worker_id)
            .unwrap_or(usize::MAX)
    }

    pub fn unparkers(&self) -> &[Unparker] {
        self.base.unparkers()
    }

    pub(crate) fn choose_worker(&self) -> usize {
        let current = self
            .base
            .tls()
            .try_with(|ctx| ctx.worker_id)
            .unwrap_or(usize::MAX);
        self.base
            .topology()
            .choose_worker_with_current(self.base.scheduler().next_worker(), current)
    }

    #[inline]
    pub fn worker_count(&self) -> NonZeroUsize {
        self.base.worker_count()
    }

    #[inline]
    pub fn validate_worker_id(&self, worker_id: usize) -> Result<()> {
        self.base.validate_worker_id(worker_id)
    }

    pub(crate) fn enqueue_local(&self, worker_id: usize, task: LocalTaskRef) -> Result<()> {
        self.base.enqueue_local(worker_id, task)
    }

    pub(crate) fn has_work(&self, worker_id: usize) -> bool {
        let worker = self.base.registry().worker(worker_id);
        let local_wake_has_work = !self.base.local_wake_target(worker_id).is_empty();
        let local_has_work = worker.local_count().load(Ordering::Acquire) > 0;
        worker.lifo().load(Ordering::Acquire).is_some()
            || worker.remote_count().load(Ordering::Acquire) > 0
            || !worker.stealer().is_empty()
            || local_has_work
            || local_wake_has_work
            || worker.pinned_count().load(Ordering::Acquire) > 0
            || self.base.scheduler().has_global_work()
    }

    pub fn global_queue_backlog(&self) -> usize {
        self.base.global_queue_backlog()
    }

    pub fn enqueue_pinned(&self, worker_id: usize, task: SendTaskRef) -> EnqueuePinnedOutcome {
        self.base.enqueue_pinned(worker_id, task)
    }

    #[inline]
    pub(crate) fn wake_worker(&self, worker_id: usize) -> Result<()> {
        self.base.wake_worker(worker_id)
    }

    pub(crate) fn enqueue_send(&self, worker_id: usize, task: SendTaskRef) {
        let current = self
            .base
            .tls()
            .try_with(|ctx| ctx.worker_id)
            .unwrap_or(usize::MAX);

        if current == worker_id {
            let mut rejected = false;
            let mut handled = false;
            let enqueued = {
                let shutdown = self.base.shutdown_coordinator();
                let _gate = shutdown.lock_publication();
                if self.base.validate_worker_id(worker_id).is_err() {
                    false
                } else if task.header().is_result_ready() {
                    handled = true;
                    false
                } else if !shutdown.is_running() {
                    rejected = true;
                    handled = true;
                    false
                } else if task.header().try_mark_queued() {
                    handled = true;
                    let worker = self.base.registry().worker(worker_id);
                    let header_ptr = task.header() as *const _ as *mut _;
                    if worker
                        .lifo()
                        .compare_exchange(
                            None,
                            NonNull::new(header_ptr),
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        )
                        .is_err()
                    {
                        self.base.tls().with(|ctx| {
                            ctx.worker.push(task);
                        });
                    }
                    true
                } else {
                    false
                }
            };
            if rejected {
                task.header().abandon_before_enqueue();
            }
            if enqueued {
                self.base.notify_event_count();
                if self.wake_worker(worker_id).is_err() {
                    // 任务已可见；唤醒失败由共享 fatal 通道终止 runtime。
                }
                return;
            }
            if handled {
                return;
            }
        }

        self.base.enqueue_send(worker_id, task);
    }

    pub(crate) fn shutdown(&self) {
        self.base.shutdown();
    }

    pub(crate) fn join_scope<S: ScopeStorage, O: Ownership + 'static>(
        &self,
        completion: &O::Shared<GenericScopeCompletion<S, O>>,
    ) -> Result<()> {
        if self.base.tls().try_with(|ctx| ctx.worker_id).is_err() {
            let mut waiter = ScopeBlockingWaiter::new(&**completion, Unparker::new());
            while !completion.is_done() {
                waiter.arm();
                if completion.is_done() {
                    break;
                }
                waiter.park(usize::MAX)?;
            }
            return self.base.fatal_error().map_or(Ok(()), Err);
        }

        let worker_id = self.base.tls().with(|ctx| ctx.worker_id);
        let mut waiter =
            ScopeBlockingWaiter::new(&**completion, self.base.unparker(worker_id).clone());
        let mut first_error = None;
        while !completion.is_done() {
            if !self.base.is_shutdown() {
                let mut controller = ScopeJoinController::new(&**completion);
                if let Err(err) = WorkerLoop::run(self, &mut controller) {
                    if first_error.is_none() {
                        first_error = Some(err);
                    }
                    self.base.shutdown();
                }
            } else {
                self.base.complete_shutdown(worker_id);
                while !completion.is_done() {
                    waiter.arm();
                    if completion.is_done() {
                        break;
                    }
                    waiter.park(worker_id)?;
                }
            }
        }

        first_error
            .or_else(|| self.base.fatal_error())
            .map_or(Ok(()), Err)
    }

    pub(crate) fn run_worker(&self) -> Result<()> {
        WorkerLoop::run(self, &mut ShutdownController)
    }
}
