use std::{
    hint::spin_loop,
    num::NonZeroUsize,
    ptr::NonNull,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};

use crossbeam_deque::Worker;
use crossbeam_queue::ArrayQueue;
use diagweave::prelude::*;
use numaperf_topo::Topology;
use veloq_storage::StateOptionPtr;
use veloq_tls::Tls;

use super::context::{IdleHook, IdleWaitStrategy, RuntimeTlsInner, WorkerTickHook};
use crate::{
    error::{Result, RuntimeError},
    runtime::primitives::{EventCount, Unparker, WakeFailureState},
    scope::{GenericScopeCompletion, ScopeBlockingWaiter},
    task::{LocalTaskRef, LocalWakeTarget, ScopeStorage, SendTaskRef, TaskHandleRef},
    utils::{FastRand, ownership::Ownership},
};

pub(crate) mod infra;
pub(crate) mod worker_loop;

use infra::{
    AtomicBitset, GlobalInjector, IdleController, IdleSlots, IdleStack, NUMAGroup, TaskScheduler,
    TopologyContext, WorkerQueue, WorkerRegistry,
};
pub(crate) use worker_loop::{BlockOnController, run_worker_loop};
use worker_loop::{ScopeJoinController, ShutdownController};

/// 每隔多少轮循环先看一次全局队列。
///
/// 单一公平性机制：避免同时使用多套重叠且互相干扰的计数器。
const GLOBAL_QUEUE_INTERVAL: u32 = 61;

/// 本地与全局队列都空时的偷取尝试次数。
const STEAL_ATTEMPTS: usize = 4;

/// `enqueue_pinned` 的结果：区分 scope 是否已由 `acknowledge_completion` 结算。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EnqueuePinnedOutcome {
    /// 成功入队到目标 worker 的 pinned channel。
    Enqueued,
    /// 任务已在队列中，无需额外 scope 操作。
    AlreadyQueued,
    /// 入队 abort，scope 义务已由 `acknowledge_completion` 结算。
    AbortedAcknowledged,
    /// 任务已完成且 scope 已结算。
    AlreadySettled,
    /// 无法通过 header ack 结算，caller 的 `ScopeTaskGuard` 须 `settle`。
    NeedsCallerSettle,
}

pub struct RuntimeSharedBase {
    pub(crate) registry: WorkerRegistry,
    pub(crate) topo: TopologyContext,
    pub(crate) scheduler: TaskScheduler,
    pub(crate) idle: IdleController,
    pub(crate) shutdown: Arc<AtomicBool>,
    pub(crate) wake_failure: Arc<WakeFailureState>,
    pub(crate) local_wake_targets: Box<[Arc<LocalWakeTarget>]>,
    pub(crate) worker_tick_hook: Option<WorkerTickHook>,
    /// Worker 线程核心上下文（不含用户 extra 状态）。
    pub(crate) tls: Tls<RuntimeTlsInner>,
}

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

pub(crate) fn init_runtime_components(
    worker_count: NonZeroUsize,
    queue_capacity: NonZeroUsize,
) -> (WorkerRegistry, TopologyContext, Receivers) {
    let worker_count_val = worker_count.get();
    let shutdown = Arc::new(AtomicBool::new(false));
    let wake_failure = Arc::new(WakeFailureState::new(shutdown.clone()));
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
            stealer,
        ));
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
            wake_failure,
            shutdown,
        },
        TopologyContext {
            groups,
            worker_to_group,
            idle_slots: IdleSlots::new(worker_count_val),
        },
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
                scheduler: TaskScheduler {
                    injector: GlobalInjector::new(),
                    next_worker: AtomicUsize::new(0),
                },
                idle: IdleController {
                    idle_mask: AtomicBitset::new(worker_count.get()),
                    event_count,
                },
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

    /// 入队失败后放弃任务：先归还 `STATE_QUEUED` 持有的引用，再终结任务本体，
    /// 确保 scope 义务一定被结算。
    fn abandon_queued_task<H: TaskHandleRef>(task: &H) {
        let header = task.header();
        if header.clear_queued() {
            // 队列引用恰好是最后一个引用，直接结算。
            header.try_acknowledge_completion();
        } else {
            header.abandon_before_enqueue();
        }
    }

    /// 放弃当前 worker 队列里的全部积压任务并结算它们的 scope 义务。
    ///
    /// 只在 worker 因 shutdown 退出调度循环时调用：这些任务已经不可能再被 poll，若不在
    /// 这里终结，等待它们的作用域会永久挂起。任务体本身不在这里析构 —— 它随所属 arena /
    /// 调用栈一起释放，与「入队失败」路径的约定一致。
    pub(crate) fn abandon_worker_backlog(&self, worker_id: usize) {
        self.drain_local_wake_mailbox(worker_id);
        let worker = &self.registry.workers[worker_id];

        if let Some(header) = worker.lifo.swap(None, Ordering::AcqRel) {
            Self::abandon_queued_task(&unsafe { SendTaskRef::from_header(header.as_ptr()) });
        }
        while let Ok(Some(task)) = self.tls.try_with(|ctx| ctx.worker.pop()) {
            Self::abandon_queued_task(&task);
        }
        while let Some(task) = worker.pinned_queue.pop() {
            worker.pinned_count.fetch_sub(1, Ordering::Release);
            Self::abandon_queued_task(&task);
        }
        while let Some(task) = worker.local_queue.pop() {
            worker.local_count.fetch_sub(1, Ordering::Release);
            Self::abandon_queued_task(&task);
        }
        while let Some(task) = worker.remote_queue.pop() {
            worker.remote_count.fetch_sub(1, Ordering::Release);
            Self::abandon_queued_task(&task);
        }
        // shutdown 期间 foreign wake 仍可能在第一次 drain 后到达；再次 drain 后，
        // `abandon_if_shutdown` 会把它们结算，而不是把任务留在已经退出的 worker 上。
        self.drain_local_wake_mailbox(worker_id);
    }

    /// 运行时已关停时，入队等于把任务送进一个再也不会被 poll 的队列。
    ///
    /// 关停之后所有 worker 都在退出并放弃自己的积压任务，此时新入队的任务会被永远遗忘 ——
    /// 等待它的作用域（`wait_all` / 析构 join）也就永远等不到 `remaining` 归零。取消唤醒
    /// 一个挂起的任务恰好会走到这里，所以必须在这里终结它而不是入队。
    fn abandon_if_shutdown<H: TaskHandleRef>(&self, task: &H) -> bool {
        if !self.shutdown.load(Ordering::Acquire) {
            return false;
        }
        task.header().abandon_before_enqueue();
        true
    }

    /// 将本地任务入队当前线程的本地队列。
    pub(crate) fn enqueue_local(&self, worker_id: usize, task: LocalTaskRef) -> Result<()> {
        debug_assert_eq!(
            self.tls.try_with(|ctx| ctx.worker_id).ok(),
            Some(worker_id),
            "local task enqueue must run on the owner worker"
        );
        if task.header().is_completed() {
            return Ok(());
        }
        if self.abandon_if_shutdown(&task) {
            return Ok(());
        }
        if task.header().try_mark_queued() {
            let worker = &self.registry.workers[worker_id];
            worker.local_count.fetch_add(1, Ordering::Release);
            if let Err(task) = worker.local_queue.push(task) {
                worker.local_count.fetch_sub(1, Ordering::Release);
                Self::abandon_queued_task(&task);
            } else {
                // 唤醒失败会设置共享 shutdown。任务已经在队列中可见，必须保留队列引用和
                // 计数，交给 worker 的 shutdown drain 结算；此处提前清理会让 drain 对计数
                // 再次递减，并可能把仍在队列中的任务变成悬挂引用。
                task.header().notify_runtime_active()?;
            }
        }
        Ok(())
    }

    pub fn enqueue_pinned(&self, worker_id: usize, task: SendTaskRef) -> EnqueuePinnedOutcome {
        if self.validate_worker_id(worker_id).is_err() {
            task.header().abandon_before_enqueue();
            return EnqueuePinnedOutcome::AbortedAcknowledged;
        }
        let header = task.header();
        if header.is_completed() {
            if header.is_scope_acknowledged() {
                return EnqueuePinnedOutcome::AlreadySettled;
            }
            return EnqueuePinnedOutcome::NeedsCallerSettle;
        }
        if self.abandon_if_shutdown(&task) {
            return EnqueuePinnedOutcome::AbortedAcknowledged;
        }
        if header.try_mark_queued() {
            let worker = &self.registry.workers[worker_id];
            worker.pinned_count.fetch_add(1, Ordering::Release);
            if let Err(task) = worker.pinned_queue.push(task) {
                worker.pinned_count.fetch_sub(1, Ordering::Release);
                Self::abandon_queued_task(&task);
                return EnqueuePinnedOutcome::AbortedAcknowledged;
            }
            // 序列号只能在任务**已经可见之后**递增，见 `EventCount::notify`。
            self.idle.event_count.notify();
            if self.wake_worker(worker_id).is_err() {
                // 任务已可见；唤醒失败由共享 fatal 通道终止 runtime。
            }
            EnqueuePinnedOutcome::Enqueued
        } else {
            EnqueuePinnedOutcome::AlreadyQueued
        }
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
        if task.header().clear_queued() {
            task.header().acknowledge_completion();
            Ok(())
        } else {
            task.poll_task(worker_id).map(|_| ())
        }
    }

    pub(crate) fn poll_send_task(&self, worker_id: usize, task: SendTaskRef) -> Result<()> {
        if task.header().clear_queued() {
            task.header().acknowledge_completion();
            Ok(())
        } else {
            task.poll_task(worker_id).map(|_| ())
        }
    }

    pub(crate) fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Release);
        for i in 0..self.registry.unparkers.len() {
            if self.registry.unpark(i).is_err() {
                // Shutdown is already in progress; the waker records the backend failure.
            }
        }
    }

    pub(crate) fn enqueue_send(&self, worker_id: usize, task: SendTaskRef) {
        if self.validate_worker_id(worker_id).is_err() {
            // 任务不会进入任何队列，必须在此结算 scope 义务，否则 `remaining`
            // 永不归零，`wait_all` 永久挂起。
            task.header().abandon_before_enqueue();
            return;
        }
        if task.header().is_completed() {
            return;
        }
        if self.abandon_if_shutdown(&task) {
            return;
        }
        if task.header().try_mark_queued() {
            let worker = &self.registry.workers[worker_id];
            // 两条分支都先让任务可见、再 bump 序列号。
            if let Err(task) = worker.remote_queue.push(task) {
                self.scheduler.injector.push(task);
                self.idle.event_count.notify();
                let group_idx = self.topo.worker_to_group[worker_id];
                self.idle
                    .wake_idle_in_group(group_idx, &self.topo, &self.registry);
            } else {
                worker.remote_count.fetch_add(1, Ordering::Release);
                self.idle.event_count.notify();
                if self.wake_worker(worker_id).is_err() {
                    // 任务已可见；唤醒失败由共享 fatal 通道终止 runtime。
                }
            }
        }
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
    }

    pub fn enqueue_pinned(&self, worker_id: usize, task: SendTaskRef) -> EnqueuePinnedOutcome {
        self.base.enqueue_pinned(worker_id, task)
    }

    #[inline]
    pub(crate) fn wake_worker(&self, worker_id: usize) -> Result<()> {
        self.base.wake_worker(worker_id)
    }

    pub(crate) fn enqueue_send(&self, worker_id: usize, task: SendTaskRef) {
        if self.base.validate_worker_id(worker_id).is_err() {
            task.header().abandon_before_enqueue();
            return;
        }
        if task.header().is_completed() {
            return;
        }
        if self.base.abandon_if_shutdown(&task) {
            return;
        }

        let current = self
            .base
            .tls
            .try_with(|ctx| ctx.worker_id)
            .unwrap_or(usize::MAX);

        if current == worker_id && task.header().try_mark_queued() {
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
            // 任务已进入 lifo 槽或本地 deque，此刻才可以 bump 序列号。
            self.base.idle.event_count.notify();
            if self.wake_worker(worker_id).is_err() {
                // 任务已可见；唤醒失败由共享 fatal 通道终止 runtime。
            }
            return;
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
    /// `std::thread::scope` 在 `Drop` 里阻塞 join 同理，宁可挂住也不能放行。
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
            if !self.base.shutdown.load(Ordering::Acquire) {
                let mut controller = ScopeJoinController::new(&**completion);
                if let Err(err) = run_worker_loop(self, &mut controller) {
                    if first_error.is_none() {
                        first_error = Some(err);
                    }
                    self.base.shutdown();
                }
            } else if !self.drain_one_pending_task() {
                waiter.arm();
                if completion.is_done() {
                    break;
                }
                if self.drain_one_pending_task() {
                    continue;
                }
                waiter.park();
            }
        }

        first_error
            .or_else(|| self.base.fatal_error())
            .map_or(Ok(()), Err)
    }

    /// 关停期间的退化驱动：只排空自己看得见的队列，不进入 idle 协调。
    fn drain_one_pending_task(&self) -> bool {
        let base = &self.base;
        base.tls
            .with(|ctx| base.poll_next_task(ctx.worker_id, 1, &ctx.rand))
            .unwrap_or(false)
    }

    /// worker 线程的调度循环入口：一直跑到运行时关停。
    pub(crate) fn run_worker(&self) -> Result<()> {
        run_worker_loop(self, &mut ShutdownController)
    }
}
