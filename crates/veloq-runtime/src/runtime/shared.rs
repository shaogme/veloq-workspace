use std::{
    hint::spin_loop,
    num::NonZeroUsize,
    panic::{AssertUnwindSafe, catch_unwind},
    ptr::NonNull,
    sync::{Arc, atomic::Ordering},
};

use crossbeam_deque::Worker;
use crossbeam_queue::ArrayQueue;
use diagweave::prelude::*;
use numaperf_topo::Topology;
use veloq_std::sync::UnpoisonedMutexGuard;
use veloq_storage::StateOptionPtr;
use veloq_tls::Tls;

use super::context::{IdleHook, IdleWaitStrategy, RuntimeTlsInner, WorkerTickHook};
use crate::{
    error::{EnqueueError, Result, RuntimeError},
    runtime::primitives::{
        EventCount, ShutdownCoordinator, ShutdownPhase, Unparker, WakeFailureState,
    },
    scope::{GenericScopeCompletion, ScopeBlockingWaiter},
    task::{
        GenericTaskHeader, LocalTaskRef, LocalWakeTarget, ScopeStorage, SendTaskRef, TaskHandleRef,
    },
    utils::{FastRand, ownership::Ownership},
};

pub(crate) mod infra;
pub(crate) mod worker_loop;

use infra::{
    IdleController, IdleSlots, IdleStack, NUMAGroup, TaskScheduler, TopologyContext, WorkerQueue,
    WorkerRegistry,
};
pub(crate) use worker_loop::{BlockOnController, run_worker_loop};
use worker_loop::{ScopeJoinController, ShutdownController};

/// 每隔多少轮循环先看一次全局队列。
///
/// 单一公平性机制：避免同时使用多套重叠且互相干扰的计数器。
const GLOBAL_QUEUE_INTERVAL: u32 = 61;

/// 本地与全局队列都空时的偷取尝试次数。
const STEAL_ATTEMPTS: usize = 4;

/// `enqueue_pinned` 的结果：所有终态结算都由任务 header 状态机负责。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EnqueuePinnedOutcome {
    /// 成功入队到目标 worker 的 pinned channel。
    Enqueued,
    /// 任务已在队列中，无需额外 scope 操作。
    AlreadyQueued,
    /// 入队 abort，任务已由 header 终结。
    AbortedAcknowledged,
    /// 任务已进入终态，调用方不得重复结算。
    AlreadySettled,
}

pub struct RuntimeSharedBase {
    pub(crate) registry: WorkerRegistry,
    pub(crate) topo: TopologyContext,
    pub(crate) scheduler: TaskScheduler,
    pub(crate) idle: IdleController,
    pub(crate) shutdown: Arc<ShutdownCoordinator>,
    pub(crate) wake_failure: Arc<WakeFailureState>,
    pub(crate) local_wake_targets: Box<[Arc<LocalWakeTarget>]>,
    pub(crate) worker_tick_hook: Option<WorkerTickHook>,
    /// Worker 线程核心上下文（不含用户 extra 状态）。
    pub(crate) tls: Tls<RuntimeTlsInner>,
}

/// Worker 进入 idle 等待阶段时调用的 hook。
///
/// Hook 接收与 worker factory 使用同一个 `T` 的 [`RuntimeShared`]，并获得运行时选定的
/// [`IdleWaitStrategy`]。它必须实现实际的等待/驱动 park 逻辑，并通过同一个 runtime
/// 的 `Unparker` 支持唤醒。
pub type ParkHook<T> = fn(&RuntimeShared<T>, IdleWaitStrategy) -> Result<()>;

pub struct RuntimeShared<T> {
    pub base: RuntimeSharedBase,
    pub(crate) idle_hook: Option<IdleHook<T>>,
    pub(crate) park_hook: Option<ParkHook<T>>,
    /// Worker 线程用户自定义 extra 状态。
    pub extra_tls: Tls<T>,
}

pub(crate) struct Receivers {
    pub(crate) deques: Vec<Worker<SendTaskRef>>,
}

/// 运行时支持的 worker 数量上界（不含）：worker id 需要能被编码进 idle 栈的低 32 位。
pub(crate) const MAX_WORKER_COUNT: usize = IdleStack::MAX_WORKERS;

#[cfg(test)]
pub(crate) fn init_runtime_components(
    worker_count: NonZeroUsize,
    queue_capacity: NonZeroUsize,
) -> (WorkerRegistry, TopologyContext, Receivers) {
    init_runtime_components_with_topology(worker_count, queue_capacity, None)
}

pub(crate) fn init_runtime_components_with_topology(
    worker_count: NonZeroUsize,
    queue_capacity: NonZeroUsize,
    topology_override: Option<&[usize]>,
) -> (WorkerRegistry, TopologyContext, Receivers) {
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
        // NUMA detection
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
        TopologyContext {
            groups,
            worker_to_group,
            group_idle_slots: IdleSlots::new(worker_count_val),
        }
    };

    (
        WorkerRegistry {
            workers: workers.into_boxed_slice(),
            unparkers: unparkers.into_boxed_slice(),
            wake_failure,
            shutdown,
        },
        topology,
        Receivers { deques },
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
        park_hook: Option<ParkHook<T>>,
        worker_tick_hook: Option<WorkerTickHook>,
    ) -> Self {
        let shutdown = registry.shutdown.clone();
        let wake_failure = registry.wake_failure.clone();
        let event_count = Arc::new(EventCount::new());
        let local_wake_targets = (0..worker_count.get())
            .map(|worker_id| {
                Arc::new(LocalWakeTarget::new(
                    worker_id,
                    registry.unparkers[worker_id].clone(),
                    event_count.clone(),
                ))
            })
            .collect();
        Self {
            base: RuntimeSharedBase {
                registry,
                topo,
                scheduler: TaskScheduler::new(),
                idle: IdleController::new(worker_count.get(), event_count),
                shutdown,
                wake_failure,
                local_wake_targets,
                worker_tick_hook,
                tls: Tls::new(),
            },
            idle_hook,
            park_hook,
            extra_tls: Tls::new(),
        }
    }
}

impl RuntimeSharedBase {
    pub fn unparkers(&self) -> &[Unparker] {
        &self.registry.unparkers
    }

    #[inline]
    pub(crate) fn unparker(&self, worker_id: usize) -> &Unparker {
        &self.registry.unparkers[worker_id]
    }

    pub(crate) fn fatal_error(&self) -> Option<Report<RuntimeError>> {
        if self.shutdown.drain_failed() {
            return Some(
                RuntimeError::InvariantViolation {
                    site: "global-injector-drain",
                    detail: "task finalization panicked while draining the global injector".into(),
                }
                .to_report(),
            );
        }
        if !self.wake_failure.is_failed() {
            return None;
        }
        self.wake_failure
            .first_error()
            .map(|source| RuntimeError::WakeFailed { source }.to_report())
    }

    #[inline]
    pub fn worker_count(&self) -> NonZeroUsize {
        if let Some(count) = NonZeroUsize::new(self.registry.workers.len()) {
            count
        } else {
            // runtime 初始化路径保证至少 1 个 worker，回退仅用于防御式容错。
            unsafe { NonZeroUsize::new_unchecked(1) }
        }
    }

    #[inline]
    pub(crate) fn validate_worker_id(&self, worker_id: usize) -> Result<()> {
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
    pub(crate) fn local_wake_target(&self, worker_id: usize) -> &Arc<LocalWakeTarget> {
        &self.local_wake_targets[worker_id]
    }

    /// 由 owner worker 消费 local wake mailbox；mailbox 中的 token 不能在 foreign thread
    /// 解引用 local header。
    pub(crate) fn drain_local_wake_mailbox(&self, worker_id: usize) {
        debug_assert_eq!(self.local_wake_target(worker_id).worker_id(), worker_id);
        if let Ok(current_worker) = self.tls.try_with(|ctx| ctx.worker_id) {
            debug_assert_eq!(
                current_worker, worker_id,
                "local wake mailbox must be drained by its owner worker"
            );
        }
        while let Some(token) = self.local_wake_target(worker_id).pop() {
            token.dispatch_on_owner();
        }
    }

    /// Mark workers that were never started so shutdown barriers do not wait for nonexistent
    /// participants. Their queues are empty because task publication starts only after all worker
    /// setup has completed.
    pub(crate) fn mark_workers_unavailable(&self, worker_ids: impl IntoIterator<Item = usize>) {
        for worker_id in worker_ids {
            self.shutdown.mark_worker_unavailable(worker_id);
        }
    }

    /// 入队失败后放弃任务：先归还 `STATE_QUEUED` 持有的引用，再终结任务本体，
    /// 确保 scope 义务一定被结算。
    ///
    /// 这里只处理 header 状态，不释放任务对象。入队失败的调用方仍可能拥有对象本身
    /// （例如 `route_to` 持有 `Box<RouteJobTask>`），由它负责最后一次析构；worker backlog
    /// 则使用 [`Self::abandon_queued_task_and_drop`] 接管这份释放责任。
    fn abandon_queued_task<H: TaskHandleRef>(task: &H) {
        let header = task.header();
        // 清除队列引用只解决 STATE_QUEUED 的所有权；若任务尚未发布结果，
        // 仍必须进入 abandoned/finalized 状态机，才能结算 scope obligation。
        header.clear_queued();
        header.abandon_before_enqueue();
    }

    fn abandon_queued_task_from_wake<H: TaskHandleRef>(task: &H) {
        let header = task.header();
        header.clear_queued();
        header.abandon_before_enqueue_from_wake();
    }

    /// 放弃一个已脱离队列、且当前 worker 已成为其对象所有者的任务。
    ///
    /// 返回值表示任务完成通知或自定义 drop 回调发生了 panic。panic 被限制在单个任务内，
    /// 这样 global injector 仍能继续处理其余任务；调用方负责把该诊断升级为 runtime fatal。
    fn abandon_queued_task_and_drop<H: TaskHandleRef>(task: &H) -> bool {
        let header = task.header();
        Self::abandon_queued_task(task);
        let mut failed = header.waker_panicked();
        if header.drop_after_poll()
            && header.is_reclaimable()
            && catch_unwind(AssertUnwindSafe(|| unsafe {
                GenericTaskHeader::drop_task(NonNull::from(header))
            }))
            .is_err()
        {
            failed = true;
        }
        failed
    }

    /// 放弃当前 worker 拥有的全部积压任务并结算它们的 scope 义务。
    ///
    /// 只在 worker 因 shutdown 退出调度循环时调用：这些任务已经不可能再被 poll，若不在
    /// 这里终结，等待它们的作用域会永久挂起。arena-backed 节点仍由 arena 回收；拥有独立
    /// 析构责任的自定义任务则由这里按 vtable 回收。
    pub(crate) fn abandon_owned_backlog(&self, worker_id: usize) {
        self.drain_local_wake_mailbox(worker_id);
        let worker = &self.registry.workers[worker_id];
        let mut failed = false;

        if let Some(header) = worker.lifo.swap(None, Ordering::AcqRel) {
            failed |= Self::abandon_queued_task_and_drop(&unsafe {
                SendTaskRef::from_header(header.as_ptr())
            });
        }
        while let Ok(Some(task)) = self.tls.try_with(|ctx| ctx.worker.pop()) {
            failed |= Self::abandon_queued_task_and_drop(&task);
        }
        while let Some(task) = worker.pinned_queue.pop() {
            worker.pinned_count.fetch_sub(1, Ordering::Release);
            failed |= Self::abandon_queued_task_and_drop(&task);
        }
        while let Some(task) = worker.local_queue.pop() {
            worker.local_count.fetch_sub(1, Ordering::Release);
            failed |= Self::abandon_queued_task_and_drop(&task);
        }
        while let Some(task) = worker.remote_queue.pop() {
            worker.remote_count.fetch_sub(1, Ordering::Release);
            failed |= Self::abandon_queued_task_and_drop(&task);
        }
        // shutdown 期间 foreign wake 仍可能在第一次 drain 后到达；再次 drain 后，
        // mailbox 中的 stale token 也不会把任务重新发布到已关闭的队列。
        self.drain_local_wake_mailbox(worker_id);
        if failed {
            self.shutdown.record_drain_failure();
        }
    }

    /// 由 worker 0 在两个 barrier 完成后，唯一排空 global injector。
    pub(crate) fn drain_global_backlog_once(&self) {
        if !self.shutdown.begin_global_drain(0) {
            return;
        }
        let mut callback_failed = false;
        let (_, panicked) = self.scheduler.drain_global_once(|task| {
            callback_failed |= Self::abandon_queued_task_and_drop(&task);
        });
        if panicked || callback_failed {
            self.shutdown.record_drain_failure();
        }
        debug_assert!(self.scheduler.is_global_empty());
        self.shutdown.finish_global_drain();
    }

    /// 返回 global injector 中当前尚未被 worker 消费的任务数量。
    pub fn global_queue_backlog(&self) -> usize {
        self.scheduler.global_queue_backlog()
    }

    /// 统一的 worker shutdown 尾部，也用于 worker 初始化失败和主 worker
    /// 尚未建立 TLS 的异常收尾。
    pub(crate) fn complete_shutdown(&self, worker_id: usize) {
        self.shutdown.request_shutdown();
        self.shutdown.arrive_quiescent(worker_id);
        self.shutdown.wait_for_quiescent();
        self.abandon_owned_backlog(worker_id);
        self.shutdown.arrive_local_drained(worker_id);
        self.shutdown.wait_for_local_drained();

        if worker_id == 0 {
            if self.shutdown.phase() != ShutdownPhase::Drained {
                self.drain_global_backlog_once();
            }
        } else {
            self.shutdown.wait_for_drained();
        }
    }

    /// 将本地任务入队当前线程的本地队列。
    fn enqueue_local_inner(
        &self,
        worker_id: usize,
        task: LocalTaskRef,
        from_wake: bool,
    ) -> Result<()> {
        debug_assert_eq!(
            self.tls.try_with(|ctx| ctx.worker_id).ok(),
            Some(worker_id),
            "local task enqueue must run on the owner worker"
        );
        let mut rejected = false;
        let mut queue_rejected = None;
        let enqueued = {
            let _gate = self.shutdown.lock_publication();
            if task.header().is_result_ready() {
                false
            } else if !self.shutdown.is_running() {
                rejected = true;
                false
            } else if task.header().try_mark_queued() {
                let worker = &self.registry.workers[worker_id];
                worker.local_count.fetch_add(1, Ordering::Release);
                if let Err(task) = worker.local_queue.push(task) {
                    worker.local_count.fetch_sub(1, Ordering::Release);
                    queue_rejected = Some((
                        task,
                        EnqueueError::LocalQueueFull {
                            worker_id,
                            capacity: worker.local_capacity(),
                        },
                    ));
                    false
                } else {
                    true
                }
            } else {
                false
            }
        };
        if rejected {
            if from_wake {
                task.header().abandon_before_enqueue_from_wake();
            } else {
                task.header().abandon_before_enqueue();
            }
        }
        if let Some((task, reason)) = queue_rejected {
            task.header().clear_queued();
            task.header().record_enqueue_rejection(reason);
            if from_wake {
                task.header().abandon_before_enqueue_from_wake();
            } else {
                task.header().abandon_before_enqueue();
            }
            return Err(RuntimeError::QueueFull {
                worker_id,
                capacity: match reason {
                    EnqueueError::LocalQueueFull { capacity, .. } => capacity,
                },
            }
            .to_report());
        }
        if enqueued {
            // 唤醒失败会设置共享 shutdown。任务已经在队列中可见，必须保留队列引用和
            // 计数，交给 worker 的 shutdown drain 结算；此处提前清理会让 drain 对计数
            // 再次递减，并可能把仍在队列中的任务变成悬挂引用。
            task.header().notify_runtime_active()?;
        }
        Ok(())
    }

    pub(crate) fn enqueue_local(&self, worker_id: usize, task: LocalTaskRef) -> Result<()> {
        self.enqueue_local_inner(worker_id, task, false)
    }

    pub(crate) fn enqueue_local_from_wake(
        &self,
        worker_id: usize,
        task: LocalTaskRef,
    ) -> Result<()> {
        self.enqueue_local_inner(worker_id, task, true)
    }

    fn enqueue_pinned_inner(
        &self,
        worker_id: usize,
        task: SendTaskRef,
        from_wake: bool,
    ) -> EnqueuePinnedOutcome {
        if self.validate_worker_id(worker_id).is_err() {
            if from_wake {
                task.header().abandon_before_enqueue_from_wake();
            } else {
                task.header().abandon_before_enqueue();
            }
            return EnqueuePinnedOutcome::AbortedAcknowledged;
        }
        let mut rejected = false;
        let mut queue_rejected = None;
        let outcome = {
            let _gate = self.shutdown.lock_publication();
            let header = task.header();
            if header.is_result_ready() {
                EnqueuePinnedOutcome::AlreadySettled
            } else if !self.shutdown.is_running() {
                rejected = true;
                EnqueuePinnedOutcome::AbortedAcknowledged
            } else if header.try_mark_queued() {
                let worker = &self.registry.workers[worker_id];
                worker.pinned_count.fetch_add(1, Ordering::Release);
                if let Err(task) = worker.pinned_queue.push(task) {
                    worker.pinned_count.fetch_sub(1, Ordering::Release);
                    queue_rejected = Some(task);
                    EnqueuePinnedOutcome::AbortedAcknowledged
                } else {
                    EnqueuePinnedOutcome::Enqueued
                }
            } else {
                EnqueuePinnedOutcome::AlreadyQueued
            }
        };
        if rejected {
            if from_wake {
                task.header().abandon_before_enqueue_from_wake();
            } else {
                task.header().abandon_before_enqueue();
            }
        }
        if let Some(task) = queue_rejected {
            if from_wake {
                Self::abandon_queued_task_from_wake(&task);
            } else {
                Self::abandon_queued_task(&task);
            }
        }
        if outcome == EnqueuePinnedOutcome::Enqueued {
            // 序列号只能在任务**已经可见之后**递增，见 `EventCount::notify`。
            self.idle.event_count.notify();
            if self.wake_worker(worker_id).is_err() {
                // 任务已可见；唤醒失败由共享 fatal 通道终止 runtime。
            }
        }
        outcome
    }

    pub fn enqueue_pinned(&self, worker_id: usize, task: SendTaskRef) -> EnqueuePinnedOutcome {
        self.enqueue_pinned_inner(worker_id, task, false)
    }

    pub(crate) fn enqueue_pinned_from_wake(
        &self,
        worker_id: usize,
        task: SendTaskRef,
    ) -> EnqueuePinnedOutcome {
        self.enqueue_pinned_inner(worker_id, task, true)
    }

    #[inline]
    pub(crate) fn wake_worker(&self, worker_id: usize) -> Result<()> {
        self.registry
            .unpark(worker_id)
            .map_err(|source| RuntimeError::WakeFailed { source }.to_report())
    }

    pub(crate) fn fn_pop_send(&self, worker_id: usize) -> Option<SendTaskRef> {
        let worker = &self.registry.workers[worker_id];
        if let Some(header) = worker.lifo.swap(None, Ordering::AcqRel) {
            return Some(unsafe { SendTaskRef::from_header(header.as_ptr()) });
        }
        self.tls.with(|ctx| ctx.worker.pop())
    }

    pub(crate) fn fn_pop_pinned(&self, worker_id: usize) -> Option<SendTaskRef> {
        let res = self.registry.workers[worker_id].pinned_queue.pop();
        if res.is_some() {
            self.registry.workers[worker_id]
                .pinned_count
                .fetch_sub(1, Ordering::Release);
        }
        res
    }

    pub(crate) fn fn_pop_local(&self, worker_id: usize) -> Option<LocalTaskRef> {
        let res = self.registry.workers[worker_id].local_queue.pop();
        if res.is_some() {
            self.registry.workers[worker_id]
                .local_count
                .fetch_sub(1, Ordering::Release);
        }
        res
    }

    pub(crate) fn pop_global(&self) -> Option<SendTaskRef> {
        self.scheduler.pop_global()
    }

    fn steal_send(&self, thief_id: usize, rand: &FastRand) -> Option<SendTaskRef> {
        self.tls.with(|ctx| {
            self.scheduler
                .steal_send(thief_id, &self.registry, &self.topo, rand, &ctx.worker)
        })
    }

    pub(crate) fn poll_local_task(&self, worker_id: usize, task: LocalTaskRef) -> Result<()> {
        let header = task.header();
        let should_drop = header.drop_after_poll();
        let header_ptr = NonNull::from(header);
        if task.header().clear_queued() {
            if should_drop && header.is_reclaimable() {
                unsafe { GenericTaskHeader::drop_task(header_ptr) };
            }
            Ok(())
        } else {
            let completed = task.poll_task(worker_id)?;
            if completed && should_drop {
                unsafe { GenericTaskHeader::drop_task(header_ptr) };
            }
            Ok(())
        }
    }

    pub(crate) fn poll_send_task(&self, worker_id: usize, task: SendTaskRef) -> Result<()> {
        let header = task.header();
        let should_drop = header.drop_after_poll();
        let header_ptr = NonNull::from(header);
        let cleared = task.header().clear_queued();
        if cleared {
            if should_drop && header.is_reclaimable() {
                unsafe { GenericTaskHeader::drop_task(header_ptr) };
            }
            Ok(())
        } else {
            let completed = task.poll_task(worker_id)?;
            if completed && should_drop {
                unsafe { GenericTaskHeader::drop_task(header_ptr) };
            }
            Ok(())
        }
    }

    pub(crate) fn shutdown(&self) {
        self.shutdown.request_shutdown();
    }

    fn publish_global(
        &self,
        task: SendTaskRef,
        target_group: usize,
        publication_gate: UnpoisonedMutexGuard<'_, ()>,
    ) {
        self.scheduler.push_global(task);
        drop(publication_gate);
        self.idle.event_count.notify();
        let _ = self
            .idle
            .wake_for_global_work(target_group, &self.topo, &self.registry);
    }

    fn enqueue_send_inner(&self, worker_id: usize, task: SendTaskRef, from_wake: bool) {
        if self.validate_worker_id(worker_id).is_err() {
            // 任务不会进入任何队列，必须在此结算 scope 义务，否则 `remaining`
            // 永不归零，`wait_all` 永久挂起。
            if from_wake {
                task.header().abandon_before_enqueue_from_wake();
            } else {
                task.header().abandon_before_enqueue();
            }
            return;
        }
        let mut rejected = false;
        let destination = {
            let publication_gate = self.shutdown.lock_publication();
            if task.header().is_result_ready() {
                None
            } else if !self.shutdown.is_running() {
                rejected = true;
                None
            } else if task.header().try_mark_queued() {
                let worker = &self.registry.workers[worker_id];
                // 计数必须先于发布队列元素，避免 worker 先弹出任务再对零计数执行减法。
                worker.remote_count.fetch_add(1, Ordering::Release);
                if let Err(task) = worker.remote_queue.push(task) {
                    worker.remote_count.fetch_sub(1, Ordering::Release);
                    let target_group = self.topo.worker_to_group[worker_id];
                    self.publish_global(task, target_group, publication_gate);
                    return;
                } else {
                    Some(())
                }
            } else {
                None
            }
        };
        if rejected {
            if from_wake {
                task.header().abandon_before_enqueue_from_wake();
            } else {
                task.header().abandon_before_enqueue();
            }
            return;
        }
        if let Some(()) = destination {
            self.idle.event_count.notify();
            if self.wake_worker(worker_id).is_err() {
                // 任务已可见；唤醒失败由共享 fatal 通道终止 runtime。
            }
        }
    }

    pub(crate) fn enqueue_send(&self, worker_id: usize, task: SendTaskRef) {
        self.enqueue_send_inner(worker_id, task, false);
    }

    pub(crate) fn enqueue_send_from_wake(&self, worker_id: usize, task: SendTaskRef) {
        self.enqueue_send_inner(worker_id, task, true);
    }

    /// 从当前 worker 可见的所有来源里取出一个任务并 poll；无事可做时返回 `false`。
    ///
    /// 这是**唯一**的取任务链：worker 线程、`block_on` 主线程、作用域析构 join 全部共用
    /// 它，主 worker 因此也参与 work stealing 与公平性间隔。
    ///
    /// 调用方必须已经处于本 worker 的 TLS 上下文中（`rand` 就是从那里借来的）。
    pub(crate) fn poll_next_task(
        &self,
        worker_id: usize,
        tick: u32,
        rand: &FastRand,
    ) -> Result<bool> {
        if self.shutdown.is_shutdown() {
            return Ok(false);
        }
        self.drain_local_wake_mailbox(worker_id);

        if tick.is_multiple_of(GLOBAL_QUEUE_INTERVAL)
            && let Some(task) = self.pop_global()
        {
            self.poll_send_task(worker_id, task)?;
            return Ok(true);
        }

        if let Some(task) = self.fn_pop_send(worker_id) {
            self.poll_send_task(worker_id, task)?;
            return Ok(true);
        }

        if let Some(task) = self.fn_pop_pinned(worker_id) {
            self.poll_send_task(worker_id, task)?;
            return Ok(true);
        }

        if let Some(task) = self.fn_pop_local(worker_id) {
            self.poll_local_task(worker_id, task)?;
            return Ok(true);
        }

        if let Some(task) = self.pop_global() {
            self.poll_send_task(worker_id, task)?;
            return Ok(true);
        }

        if let Some(task) = self.registry.workers[worker_id].remote_queue.pop() {
            self.registry.workers[worker_id]
                .remote_count
                .fetch_sub(1, Ordering::Release);
            self.poll_send_task(worker_id, task)?;
            return Ok(true);
        }

        for _ in 0..STEAL_ATTEMPTS {
            if let Some(task) = self.steal_send(worker_id, rand) {
                self.poll_send_task(worker_id, task)?;
                return Ok(true);
            }
            spin_loop();
        }

        Ok(false)
    }
}

impl<T> RuntimeShared<T> {
    pub fn worker_id(&self) -> usize {
        self.base
            .tls
            .try_with(|ctx| ctx.worker_id)
            .unwrap_or(usize::MAX)
    }

    pub fn unparkers(&self) -> &[Unparker] {
        self.base.unparkers()
    }

    pub(crate) fn choose_worker(&self) -> usize {
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

    pub(crate) fn enqueue_local(&self, worker_id: usize, task: LocalTaskRef) -> Result<()> {
        self.base.enqueue_local(worker_id, task)
    }

    pub(crate) fn has_work(&self, worker_id: usize) -> bool {
        let worker = &self.base.registry.workers[worker_id];
        let local_wake_has_work = !self.base.local_wake_target(worker_id).is_empty();
        let local_has_work = worker.local_count.load(Ordering::Acquire) > 0;
        worker.lifo.load(Ordering::Acquire).is_some()
            || worker.remote_count.load(Ordering::Acquire) > 0
            || !worker.stealer.is_empty()
            || local_has_work
            || local_wake_has_work
            || worker.pinned_count.load(Ordering::Acquire) > 0
            || self.base.scheduler.has_global_work()
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
            .tls
            .try_with(|ctx| ctx.worker_id)
            .unwrap_or(usize::MAX);

        if current == worker_id {
            let mut rejected = false;
            let mut handled = false;
            let enqueued = {
                let _gate = self.base.shutdown.lock_publication();
                if self.base.validate_worker_id(worker_id).is_err() {
                    false
                } else if task.header().is_result_ready() {
                    handled = true;
                    false
                } else if !self.base.shutdown.is_running() {
                    rejected = true;
                    handled = true;
                    false
                } else if task.header().try_mark_queued() {
                    handled = true;
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
                        .is_err()
                    {
                        self.base.tls.with(|ctx| {
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
                // 任务已进入 lifo 槽或本地 deque，此刻才可以 bump 序列号。
                self.base.idle.event_count.notify();
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

    /// 阻塞直到 `completion` 的全部子任务真正结束。
    ///
    /// 这是结构化并发的最后一道保证：作用域析构时不能带着仍在运行的子任务返回，否则子
    /// 任务持有的 `'env` 借用会悬垂。因此这里**没有**提前退出的出口 —— 与
    /// `thread::scope` 在 `Drop` 里阻塞 join 同理，宁可挂住也不能放行。
    ///
    /// 正常情况下复用统一的调度循环（含 work stealing 与 idle/park 协调）；运行时正在关停
    /// 时循环会立刻返回，退化为「排空自己的队列 + 阻塞等待任务或 completion 唤醒」，而
    /// 关停路径上每个 worker 退出前都会放弃自己队列里的积压任务并结算义务，因此仍能收敛。
    pub(crate) fn join_scope<S: ScopeStorage, O: Ownership + 'static>(
        &self,
        completion: &O::Shared<GenericScopeCompletion<S, O>>,
    ) -> Result<()> {
        if self.base.tls.try_with(|ctx| ctx.worker_id).is_err() {
            // 非 worker 线程上无法驱动调度器，只能等别的 worker 把子任务跑完。
            let mut waiter = ScopeBlockingWaiter::new(&**completion, Unparker::new());
            while !completion.is_done() {
                waiter.arm();
                if completion.is_done() {
                    break;
                }
                waiter.park();
            }
            return self.base.fatal_error().map_or(Ok(()), Err);
        }

        let worker_id = self.base.tls.with(|ctx| ctx.worker_id);
        let mut waiter =
            ScopeBlockingWaiter::new(&**completion, self.base.unparker(worker_id).clone());
        let mut first_error = None;
        while !completion.is_done() {
            if !self.base.shutdown.is_shutdown() {
                let mut controller = ScopeJoinController::new(&**completion);
                if let Err(err) = run_worker_loop(self, &mut controller) {
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
                    waiter.park();
                }
            }
        }

        first_error
            .or_else(|| self.base.fatal_error())
            .map_or(Ok(()), Err)
    }

    /// worker 线程的调度循环入口：一直跑到运行时关停。
    pub(crate) fn run_worker(&self) -> Result<()> {
        run_worker_loop(self, &mut ShutdownController)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{error::RuntimeWakeError, runtime::primitives::RuntimeWaker};
    use crate::{
        scope::GenericScopeCompletion,
        task::{GenericWakerNode, ScopeRef, TaskVTable},
        utils::ownership::ArcOwnership,
    };
    use std::{
        marker::{PhantomData, PhantomPinned},
        pin::Pin,
        result::Result as StdResult,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        task::{RawWaker, Waker},
    };
    use veloq_intrusive_linklist::Link;
    use veloq_storage::AtomicStorage;

    struct RecordingWaker {
        calls: Arc<[AtomicUsize]>,
        worker_id: usize,
    }

    impl RuntimeWaker for RecordingWaker {
        fn wake(&self) -> StdResult<(), RuntimeWakeError> {
            self.calls[self.worker_id].fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
    }

    static PANIC_WAKER_VTABLE: std::task::RawWakerVTable = std::task::RawWakerVTable::new(
        |_| std::task::RawWaker::new(std::ptr::null(), &PANIC_WAKER_VTABLE),
        |_| panic!("completion waker panic"),
        |_| {},
        |_| {},
    );

    static GLOBAL_DRAIN_DROPS: AtomicUsize = AtomicUsize::new(0);

    unsafe fn noop_wake(_: NonNull<GenericTaskHeader<AtomicStorage>>) {}

    unsafe fn noop_wake_by_ref(_: &GenericTaskHeader<AtomicStorage>) {}

    unsafe fn noop_drop(_: NonNull<GenericTaskHeader<AtomicStorage>>) {
        GLOBAL_DRAIN_DROPS.fetch_add(1, Ordering::AcqRel);
    }

    static DROP_AFTER_POLL_VTABLE: TaskVTable<AtomicStorage> = TaskVTable {
        wake: noop_wake,
        wake_by_ref: noop_wake_by_ref,
        poll: |_, _| Ok(true),
        drop: noop_drop,
        drop_after_poll: true,
    };

    fn test_shared() -> RuntimeShared<()> {
        let worker_count = NonZeroUsize::new(1).expect("one worker");
        let queue_capacity = NonZeroUsize::new(1).expect("one queue slot");
        let (registry, topo, _) = init_runtime_components(worker_count, queue_capacity);
        RuntimeShared::new(registry, topo, worker_count, None, None, None)
    }

    #[test]
    fn global_drain_settles_queue_reference_and_scope_once() {
        GLOBAL_DRAIN_DROPS.store(0, Ordering::Release);
        let shared = test_shared();
        let completion = GenericScopeCompletion::<AtomicStorage, ArcOwnership>::new(None);
        completion.register_task();

        let scope = ScopeRef::from_shared::<ArcOwnership>(&completion);
        let header = GenericTaskHeader::new_placeholder(&DROP_AFTER_POLL_VTABLE);
        unsafe { header.initialize(&shared.base, 0, scope) };
        header.claim_scope_obligation();
        assert!(header.try_mark_queued());

        let task = unsafe { SendTaskRef::from_header(&header) };
        shared.base.scheduler.push_global(task);
        assert_eq!(shared.base.global_queue_backlog(), 1);
        shared.base.complete_shutdown(0);
        shared.base.complete_shutdown(0);

        assert!(header.is_result_ready());
        assert!(header.is_reclaimable());
        assert!(completion.is_done());
        assert_eq!(GLOBAL_DRAIN_DROPS.load(Ordering::Acquire), 1);
        assert_eq!(shared.base.global_queue_backlog(), 0);
    }

    #[test]
    fn global_drain_contains_waker_panic_and_finishes_task() {
        let shared = test_shared();
        let completion = GenericScopeCompletion::<AtomicStorage, ArcOwnership>::new(None);
        completion.register_task();

        let scope = ScopeRef::from_shared::<ArcOwnership>(&completion);
        let header = GenericTaskHeader::new_placeholder(&DROP_AFTER_POLL_VTABLE);
        unsafe { header.initialize(&shared.base, 0, scope) };
        header.claim_scope_obligation();
        let panic_waker =
            unsafe { Waker::from_raw(RawWaker::new(std::ptr::null(), &PANIC_WAKER_VTABLE)) };
        let mut node = GenericWakerNode {
            waker: panic_waker.clone(),
            link: Link::new(),
            marker: PhantomData,
            _pin: PhantomPinned,
        };
        let mut node = unsafe { Pin::new_unchecked(&mut node) };
        unsafe { header.register_completion(node.as_mut(), &panic_waker) };
        assert!(header.try_mark_queued());

        let task = unsafe { SendTaskRef::from_header(&header) };
        shared.base.scheduler.push_global(task);
        assert_eq!(shared.base.global_queue_backlog(), 1);
        shared.base.complete_shutdown(0);

        assert!(header.is_reclaimable());
        assert!(completion.is_done());
        assert!(header.waker_panicked());
        assert!(shared.base.fatal_error().is_some());
        assert_eq!(shared.base.global_queue_backlog(), 0);
    }

    #[test]
    fn global_publication_wakes_idle_worker_from_other_group() {
        let worker_count = NonZeroUsize::new(4).expect("workers");
        let queue_capacity = NonZeroUsize::new(1).expect("queue capacity");
        let (registry, _, _) = init_runtime_components(worker_count, queue_capacity);
        let topo = infra::TopologyContext::from_worker_to_group(&[0, 0, 1, 1]);
        let shared = RuntimeShared::<()>::new(registry, topo, worker_count, None, None, None);

        let calls: Arc<[AtomicUsize]> = (0..worker_count.get())
            .map(|_| AtomicUsize::new(0))
            .collect::<Vec<_>>()
            .into();
        shared.base.registry.unparkers[2]
            .bind(Arc::new(RecordingWaker {
                calls: calls.clone(),
                worker_id: 2,
            }))
            .expect("recording waker must bind once");
        shared.base.idle.enter_idle(2, &shared.base.topo);

        let header_in_queue = GenericTaskHeader::new_placeholder(&DROP_AFTER_POLL_VTABLE);
        unsafe {
            header_in_queue.initialize(&shared.base, 0, ScopeRef::dummy());
        }
        assert!(header_in_queue.try_mark_queued());
        let queued_task = unsafe { SendTaskRef::from_header(&header_in_queue) };
        let worker = &shared.base.registry.workers[0];
        worker.remote_count.fetch_add(1, Ordering::Release);
        assert!(worker.remote_queue.push(queued_task).is_ok());

        let global_header = GenericTaskHeader::new_placeholder(&DROP_AFTER_POLL_VTABLE);
        unsafe {
            global_header.initialize(&shared.base, 0, ScopeRef::dummy());
        }
        let global_task = unsafe { SendTaskRef::from_header(&global_header) };

        shared.base.enqueue_send(0, global_task);

        assert_eq!(shared.base.global_queue_backlog(), 1);
        assert_eq!(calls[2].load(Ordering::Relaxed), 1);
        assert_eq!(
            calls
                .iter()
                .map(|calls| calls.load(Ordering::Relaxed))
                .sum::<usize>(),
            1
        );

        let queued_task = worker.remote_queue.pop().expect("queued task");
        worker.remote_count.fetch_sub(1, Ordering::Release);
        shared
            .base
            .poll_send_task(0, queued_task)
            .expect("remote task");
        let global_task = shared.base.pop_global().expect("global task");
        shared
            .base
            .poll_send_task(2, global_task)
            .expect("global task");
        assert_eq!(shared.base.global_queue_backlog(), 0);
    }
}
