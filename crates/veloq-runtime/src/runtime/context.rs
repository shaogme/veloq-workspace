use core::cell::UnsafeCell;
use std::{
    future::{Future, poll_fn},
    marker::PhantomData,
    mem::replace,
    num::NonZeroUsize,
    ops::AsyncFnOnce,
    pin::Pin,
    ptr::NonNull,
    sync::Arc,
    task::{Context, Poll, Waker},
    time::Duration,
};

use super::shared::{EnqueuePinnedOutcome, RuntimeShared};
use crate::{
    error::{Result, RuntimeError},
    macros::helpers::run_scope_eval,
    outcome::{IntoOutcome, Outcome},
    scope::{AsyncScope, LocalAsyncScope},
    task::{
        AnyScopeRef, GenericTaskHeader, PollStatus, RawTask, RuntimeContextExt, ScopeRef,
        SendTaskRef, TaskHandleRef, TaskHeader, TaskVTable,
    },
    utils::FastRand,
};

use crossbeam_deque::Worker;
use diagweave::prelude::*;
use veloq_std::panic::{AssertUnwindSafe, catch_unwind};
use veloq_storage::{AtomicLock, AtomicStorage, StateLock};
use veloq_waker::MwsrWaker;

/// Worker 空闲时的等待策略。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IdleWaitStrategy {
    /// 持续阻塞，直到有新的唤醒事件。
    Block,
    /// 阻塞指定时长后重新检查。
    Timeout(Duration),
}

impl IdleWaitStrategy {
    #[inline]
    pub fn timeout(duration: Duration) -> Self {
        Self::Timeout(duration)
    }

    #[inline]
    pub fn block() -> Self {
        Self::Block
    }

    #[inline]
    pub fn into_timeout(self) -> Option<Duration> {
        match self {
            Self::Block => None,
            Self::Timeout(duration) => Some(duration),
        }
    }
}

/// Worker 空闲阶段的显式决策。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IdleDecision {
    /// 继续推进，不进入阻塞等待。
    Continue,
    /// 进入等待阶段，具体方式由 `IdleWaitStrategy` 决定。
    Wait(IdleWaitStrategy),
}

impl IdleDecision {
    #[inline]
    pub fn continue_now() -> Self {
        Self::Continue
    }

    #[inline]
    pub fn wait(strategy: IdleWaitStrategy) -> Self {
        Self::Wait(strategy)
    }

    #[inline]
    pub fn is_continue(self) -> bool {
        matches!(self, Self::Continue)
    }

    #[inline]
    pub(crate) fn into_wait_strategy(self) -> Option<IdleWaitStrategy> {
        match self {
            Self::Continue => None,
            Self::Wait(strategy) => Some(strategy),
        }
    }
}

pub(crate) struct RuntimeTlsInner {
    pub(crate) worker_id: usize,
    pub(crate) rand: FastRand,
    pub(crate) worker: Worker<SendTaskRef>,
}

/// A context handle provided to the `block_on` async closure, allowing creation of scopes.
///
/// `'rt` is carried by a marker rather than by a `&'rt RuntimeShared<T>` field on purpose.
/// A reference field would imply `T: 'rt`, and a higher-ranked `for<'rt>` bound on the entry
/// closure would then collapse into `T: 'static` — which rules out the intended use of the
/// extra worker state (`veloq`'s `WorkerState<'rt>` borrows back into `RuntimeShared`).
/// With the pointer, `for<'rt> AsyncFnOnce(RuntimeCtx<'rt, T>) -> R` is expressible for any
/// `T`, and that bound is what keeps the context from escaping `block_on` inside `R`.
pub struct RuntimeCtx<'rt, T> {
    shared: NonNull<RuntimeShared<T>>,
    /// 与 `&'rt RuntimeShared<T>` 同为协变，但不引入 `T: 'rt` 的隐式约束。
    _marker: PhantomData<fn() -> &'rt ()>,
}

// 语义与被替换掉的 `&'rt RuntimeShared<T>` 完全一致：共享引用可跨线程当且仅当被指对象
// 是 `Sync`。
unsafe impl<'rt, T> Send for RuntimeCtx<'rt, T> where RuntimeShared<T>: Sync {}
unsafe impl<'rt, T> Sync for RuntimeCtx<'rt, T> where RuntimeShared<T>: Sync {}

impl<'rt, T> Copy for RuntimeCtx<'rt, T> {}

impl<'rt, T> Clone for RuntimeCtx<'rt, T> {
    fn clone(&self) -> Self {
        *self
    }
}

/// A trait to extract the runtime scope context.
pub trait IntoRuntimeCtx<'rt, T> {
    fn into_runtime_ctx(self) -> RuntimeCtx<'rt, T>;
}

impl<'rt, T> IntoRuntimeCtx<'rt, T> for RuntimeCtx<'rt, T> {
    #[inline]
    fn into_runtime_ctx(self) -> RuntimeCtx<'rt, T> {
        self
    }
}

impl<'rt, T> IntoRuntimeCtx<'rt, T> for &RuntimeCtx<'rt, T> {
    #[inline]
    fn into_runtime_ctx(self) -> RuntimeCtx<'rt, T> {
        *self
    }
}

#[repr(C)]
struct RouteJobTask<'scope_ref, F, Fut> {
    header: TaskHeader,
    job: UnsafeCell<Option<F>>,
    slot: Arc<RouteCell<Fut>>,
    marker: PhantomData<&'scope_ref ()>,
}

struct RouteTaskFinalizer<'a, Fut> {
    header: &'a TaskHeader,
    slot: &'a RouteCell<Fut>,
    published: bool,
    finalized: bool,
}

impl<'a, Fut> RouteTaskFinalizer<'a, Fut> {
    fn new(header: &'a TaskHeader, slot: &'a RouteCell<Fut>) -> Self {
        Self {
            header,
            slot,
            published: false,
            finalized: false,
        }
    }

    fn publish_ok(&mut self, future: Fut) {
        if !self.published {
            self.slot.publish(future);
            self.published = true;
        }
    }

    fn publish_err(&mut self, error: Report<RuntimeError>) {
        if !self.published {
            self.slot.publish_error_if_pending(error);
            self.published = true;
        }
    }

    fn finish(&mut self) {
        if !self.finalized {
            self.finalized = true;
            self.header.complete_external_poll();
        }
    }
}

impl<Fut> Drop for RouteTaskFinalizer<'_, Fut> {
    fn drop(&mut self) {
        if !self.published {
            self.slot.publish_error_if_pending(route_error(
                "RuntimeCtx::route_to::RouteTaskFinalizer::drop",
                "route task exited without publishing a result",
            ));
        }
        if !self.finalized {
            self.header.complete_external_poll();
        }
    }
}

fn route_error(site: &'static str, detail: impl Into<String>) -> Report<RuntimeError> {
    RuntimeError::InvariantViolation {
        site,
        detail: detail.into().into(),
    }
    .to_report()
    .with_category("runtime.route")
}

impl<'scope_ref, F, Fut> RawTask for RouteJobTask<'scope_ref, F, Fut>
where
    F: FnOnce() -> Fut + Send + 'scope_ref,
    Fut: Future + Send + 'scope_ref,
{
    type Storage = AtomicStorage;

    fn poll_raw(&self, _worker_id: usize) -> Result<bool> {
        match self.header.try_enter_poll() {
            PollStatus::Complete => return Ok(true),
            PollStatus::Yield => return Ok(false),
            PollStatus::Proceed => {}
        }

        let mut finalizer = RouteTaskFinalizer::new(&self.header, &self.slot);
        let operation = catch_unwind(AssertUnwindSafe::new(|| {
            let Some(job) = (unsafe { &mut *self.job.get() }).take() else {
                finalizer.publish_err(route_error(
                    "RuntimeCtx::route_to::RouteJobTask::poll_raw",
                    "job already taken",
                ));
                return;
            };

            match catch_unwind(AssertUnwindSafe::new(job)) {
                Ok(future) => finalizer.publish_ok(future),
                Err(_) => finalizer.publish_err(route_error(
                    "RuntimeCtx::route_to::RouteJobTask::poll_raw",
                    "route job panicked",
                )),
            }
        }));

        if operation.is_err() {
            finalizer.publish_err(route_error(
                "RuntimeCtx::route_to::RouteJobTask::poll_raw",
                "route task infrastructure panicked",
            ));
        }
        finalizer.finish();
        Ok(true)
    }

    fn header(&self) -> &GenericTaskHeader<Self::Storage> {
        &self.header
    }
}

impl<F, Fut> Drop for RouteJobTask<'_, F, Fut> {
    fn drop(&mut self) {
        self.slot.publish_error_if_pending(
            RuntimeError::ShutdownBeforeCompletion
                .to_report()
                .with_category("runtime.route"),
        );
    }
}

impl<'scope_ref, F, Fut> RouteJobTask<'scope_ref, F, Fut>
where
    F: FnOnce() -> Fut + Send + 'scope_ref,
    Fut: Future + Send + 'scope_ref,
{
    const VTABLE: &'static TaskVTable<AtomicStorage> = &TaskVTable {
        wake: |_| {},
        wake_by_ref: |_| {},
        poll: |header, worker_id| unsafe {
            let raw_ptr = header as *const GenericTaskHeader<AtomicStorage> as *const Self;
            let node = &*raw_ptr;
            RawTask::poll_raw(node, worker_id)
        },
        drop: |data| unsafe {
            let ptr = data.as_ptr() as *mut Self;
            drop(Box::from_raw(ptr));
        },
        drop_after_poll: true,
    };
}

impl<'rt, T> RuntimeCtx<'rt, T> {
    pub(crate) fn new(shared: &'rt RuntimeShared<T>) -> Self {
        Self {
            shared: NonNull::from(shared),
            _marker: PhantomData,
        }
    }

    /// Returns the total worker count in the runtime.
    pub fn worker_count(&self) -> NonZeroUsize {
        self.shared().worker_count()
    }

    /// Wakes up the specified worker.
    pub fn wake_worker(&self, worker_id: usize) -> Result<()> {
        self.shared().wake_worker(worker_id)
    }

    /// Checks if the runtime is shutting down.
    pub(crate) fn is_shutdown(&self) -> bool {
        self.shared().base.shutdown.is_shutdown()
    }

    /// Returns the shared runtime state.
    pub fn shared(&self) -> &'rt RuntimeShared<T> {
        // 由构造点保证：`RuntimeCtx` 只能从一个存活期覆盖 `'rt` 的 `&'rt RuntimeShared<T>`
        // 建立，而 `'rt` 被入口闭包的 `for<'rt>` 约束封死在 `block_on` 内部。
        unsafe { self.shared.as_ref() }
    }

    /// 为 `select!` 公平模式返回 `[0, branches)` 范围内的随机起始分支索引。
    #[doc(hidden)]
    pub fn select_poll_start(&self, branches: u32) -> u32 {
        self.shared()
            .base
            .tls
            .with(|ctx| ctx.rand.next_u32(branches))
    }

    /// Runs `f` inside a fresh thread-safe scope derived from the current task's scope.
    ///
    /// Prefer this over [`scope!`](macro@crate::scope) whenever `f`'s parameter type is already
    /// pinned down; the macro exists for `async |s| ..` literals, and explains in its own docs
    /// why it cannot just forward here. `self` is taken by value (the context is `Copy`) so the
    /// returned future owns everything it needs.
    ///
    /// The body may return `()`, a `Result`, or an explicit [`Outcome`]. A returned error cancels
    /// the child tasks before this method completes.
    pub async fn scope<'env, 'scope, F, Body>(
        self,
        f: F,
    ) -> Result<Outcome<Body::Output, Body::Error>>
    where
        'env: 'scope,
        Body: IntoOutcome,
        F: for<'scope_ref> AsyncFnOnce(&'scope_ref AsyncScope<'rt, 'scope, 'env, T>) -> Body,
    {
        let parent = poll_fn(|cx| Poll::Ready(cx.scope_completion())).await;
        let scope = AsyncScope::new(self, parent);
        let s_ref = &scope;
        run_scope_eval(s_ref, f(s_ref)).await
    }

    /// Thread-local counterpart of [`RuntimeCtx::scope`], forwarded to by
    /// [`scope_local!`](crate::scope_local).
    ///
    /// The body may return `()`, a `Result`, or an explicit [`Outcome`]. A returned error cancels
    /// the child tasks before this method completes.
    pub async fn scope_local<'env, 'scope, F, Body>(
        self,
        f: F,
    ) -> Result<Outcome<Body::Output, Body::Error>>
    where
        'env: 'scope,
        Body: IntoOutcome,
        F: for<'scope_ref> AsyncFnOnce(&'scope_ref LocalAsyncScope<'rt, 'scope, 'env, T>) -> Body,
    {
        let parent = poll_fn(|cx| Poll::Ready(cx.scope_completion())).await;
        let scope = LocalAsyncScope::new(self, parent);
        let s_ref = &scope;
        run_scope_eval(s_ref, f(s_ref)).await
    }

    pub fn route_to<'scope_ref, F, Fut>(
        &self,
        worker_id: usize,
        job: F,
    ) -> Result<RoutedFuture<Fut>>
    where
        F: FnOnce() -> Fut + Send + 'scope_ref,
        Fut: Future + Send + 'scope_ref,
    {
        self.shared().validate_worker_id(worker_id)?;

        let slot = RouteCell::new();
        let slot_for_job = slot.clone();

        let task = Box::new(RouteJobTask {
            header: TaskHeader::new(
                RouteJobTask::<'scope_ref, F, Fut>::VTABLE,
                &self.shared().base,
                worker_id,
                ScopeRef::<AtomicStorage>::dummy(),
            ),
            job: UnsafeCell::new(Some(job)),
            slot: slot_for_job,
            marker: PhantomData,
        });

        task.header.set_pinned();

        let ptr = Box::into_raw(task);
        let task_ref = unsafe { SendTaskRef::from_concrete(ptr) };
        let header_ptr = task_ref.header() as *const GenericTaskHeader<AtomicStorage>;
        let task_ctx = unsafe { SendTaskRef::from_header(header_ptr) };

        match self.shared().enqueue_pinned(worker_id, task_ctx) {
            EnqueuePinnedOutcome::Enqueued | EnqueuePinnedOutcome::AlreadyQueued => {}
            EnqueuePinnedOutcome::AbortedAcknowledged | EnqueuePinnedOutcome::AlreadySettled => {
                unsafe {
                    let _ = Box::from_raw(ptr);
                }
                let current_worker = self.try_worker_id().unwrap_or(usize::MAX);
                let is_shutdown = self.is_shutdown();
                return RuntimeError::DispatchFailed {
                    target_worker: worker_id,
                    current_worker,
                }
                .with_ctx("is_shutdown", is_shutdown)
                .with_ctx("on_worker", current_worker != usize::MAX);
            }
        }

        Ok(RoutedFuture::new(slot))
    }

    pub async fn execute_on_owner<'scope_ref, F, Fut, R>(
        &self,
        task: &impl TaskHandleRef,
        f: F,
    ) -> Result<R>
    where
        F: FnOnce() -> Fut + Send + 'scope_ref,
        Fut: Future<Output = R> + Send + 'scope_ref,
        R: Send,
    {
        let worker_id = task.header().worker_id();
        self.route_to(worker_id, f)?.await
    }

    /// Returns the current worker id.
    pub fn worker_id(&self) -> usize {
        self.try_worker_id()
            .expect("Failed to get worker id: this should be invoked from a worker thread")
    }

    pub(crate) fn try_worker_id(&self) -> Option<usize> {
        self.shared().base.tls.try_with(|ctx| ctx.worker_id).ok()
    }
}

/// 取当前任务所属的作用域，作为新建子作用域的父节点。
///
/// [`RuntimeCtx::scope`] 与 `scope!` / `scope_local!` 宏共用这一步：父作用域只能从当前
/// 任务的 waker 上取，两边不能各写一份。
#[doc(hidden)]
pub async fn current_scope() -> Option<AnyScopeRef> {
    poll_fn(|cx| Poll::Ready(cx.scope_completion())).await
}

/// Worker 空闲时调用的 hook。
///
/// Hook 接收与 worker factory 使用同一个 `T` 的 [`RuntimeShared`]，因此可以通过
/// [`RuntimeShared::extra_tls`] 访问类型安全的 worker extra 状态。
pub type IdleHook<T> = fn(&RuntimeShared<T>) -> Result<IdleDecision>;
pub(crate) type WorkerTickHook = fn();

enum RouteCellState<T> {
    Pending,
    Published(Result<T>),
    Consumed,
}

pub(crate) struct RouteCell<T> {
    state: AtomicLock<RouteCellState<T>>,
    waker: MwsrWaker,
}

impl<T> RouteCell<T> {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            state: AtomicLock::new(RouteCellState::Pending),
            waker: MwsrWaker::new(),
        })
    }

    pub(crate) fn publish(&self, value: T) -> bool {
        let published = {
            let mut state = self.state.lock();
            debug_assert!(
                matches!(&*state, RouteCellState::Pending),
                "worker route slot already has a terminal state"
            );
            if matches!(&*state, RouteCellState::Pending) {
                *state = RouteCellState::Published(Ok(value));
                true
            } else {
                false
            }
        };
        if published {
            self.waker.wake();
        }
        published
    }

    pub(crate) fn publish_error_if_pending(&self, error: Report<RuntimeError>) -> bool {
        let published = {
            let mut state = self.state.lock();
            if matches!(&*state, RouteCellState::Pending) {
                *state = RouteCellState::Published(Err(error));
                true
            } else {
                false
            }
        };
        if published {
            self.waker.wake();
        }
        published
    }

    pub(crate) fn is_populated(&self) -> bool {
        matches!(&*self.state.lock(), RouteCellState::Published(_))
    }

    pub(crate) fn take(&self) -> Option<Result<T>> {
        let mut state = self.state.lock();
        if !matches!(&*state, RouteCellState::Published(_)) {
            return None;
        }
        match replace(&mut *state, RouteCellState::Consumed) {
            RouteCellState::Published(value) => Some(value),
            RouteCellState::Pending | RouteCellState::Consumed => unreachable!(),
        }
    }

    pub(crate) fn register(&self, waker: &Waker) {
        unsafe {
            self.waker.register(waker);
        }
    }
}

pub struct RoutedFuture<F> {
    slot: Arc<RouteCell<F>>,
    inner: Option<F>,
}

impl<F> RoutedFuture<F> {
    pub(crate) fn new(slot: Arc<RouteCell<F>>) -> Self {
        Self { slot, inner: None }
    }

    /// 查询内层 Future 是否已被远程 Worker 产出并已移入当前句柄（即阶段一已就绪）。
    pub fn is_ready(&self) -> bool {
        self.inner.is_some() || self.slot.is_populated()
    }

    /// 轮询直到目标 Worker 完成闭包执行并将内层 Future 放入当前槽位。
    ///
    /// 一旦返回 `Poll::Ready(Ok(()))`，说明远程闭包（如 `submit_detached`）
    /// 已经在目标 Worker 的线程和驱动上完全执行并落地，底层内核资源已 Armed。
    pub fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<()>> {
        if self.inner.is_some() {
            return Poll::Ready(Ok(()));
        }

        if let Some(op) = self.slot.take() {
            match op {
                Ok(op) => {
                    self.inner = Some(op);
                    Poll::Ready(Ok(()))
                }
                Err(err) => Poll::Ready(Err(err)),
            }
        } else {
            self.slot.register(cx.waker());
            if let Some(op) = self.slot.take() {
                match op {
                    Ok(op) => {
                        self.inner = Some(op);
                        Poll::Ready(Ok(()))
                    }
                    Err(err) => Poll::Ready(Err(err)),
                }
            } else {
                Poll::Pending
            }
        }
    }

    /// 异步等待目标 Worker 完成闭包执行并把内层 Future 传回。
    pub async fn wait_ready(&mut self) -> Result<()> {
        poll_fn(|cx| self.poll_ready(cx)).await
    }
}

impl<F> Future for RoutedFuture<F>
where
    F: Future,
{
    type Output = Result<F::Output>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };

        // 复用 poll_ready 确保 inner 已就绪
        if this.inner.is_none() {
            match this.poll_ready(cx) {
                Poll::Ready(Ok(())) => {}
                Poll::Ready(Err(err)) => return Poll::Ready(Err(err)),
                Poll::Pending => return Poll::Pending,
            }
        }

        let Some(inner) = this.inner.as_mut() else {
            let err = RuntimeError::InvariantViolation {
                site: "RoutedFuture::poll",
                detail: "route future missing inner op after poll_ready".into(),
            }
            .to_report()
            .with_category("runtime.route");
            return Poll::Ready(Err(err));
        };

        // 阶段二：轮询内层 Future（例如 DetachedOp 的 I/O 完成事件）
        let inner_pin = unsafe { Pin::new_unchecked(inner) };
        inner_pin.poll(cx).map(Ok)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        mem::ManuallyDrop,
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
        task::{RawWaker, RawWakerVTable, Waker},
        time::Duration,
    };

    struct ReentrantWake {
        slot: Arc<RouteCell<usize>>,
        took_value: AtomicBool,
    }

    unsafe fn clone_reentrant(data: *const ()) -> RawWaker {
        unsafe {
            Arc::increment_strong_count(data as *const ReentrantWake);
        }
        RawWaker::new(data, &REENTRANT_WAKER_VTABLE)
    }

    unsafe fn wake_reentrant(data: *const ()) {
        let state = unsafe { Arc::from_raw(data as *const ReentrantWake) };
        state
            .took_value
            .store(state.slot.take().is_some(), Ordering::Release);
    }

    unsafe fn wake_reentrant_by_ref(data: *const ()) {
        let state = ManuallyDrop::new(unsafe { Arc::from_raw(data as *const ReentrantWake) });
        state
            .took_value
            .store(state.slot.take().is_some(), Ordering::Release);
    }

    unsafe fn drop_reentrant(data: *const ()) {
        unsafe {
            drop(Arc::from_raw(data as *const ReentrantWake));
        }
    }

    static REENTRANT_WAKER_VTABLE: RawWakerVTable = RawWakerVTable::new(
        clone_reentrant,
        wake_reentrant,
        wake_reentrant_by_ref,
        drop_reentrant,
    );

    #[test]
    fn idle_decision_continue_marks_continue() {
        assert!(IdleDecision::continue_now().is_continue());
    }

    #[test]
    fn idle_decision_wait_wraps_strategy() {
        let decision = IdleDecision::wait(IdleWaitStrategy::timeout(Duration::from_millis(5)));
        assert_eq!(
            decision.into_wait_strategy(),
            Some(IdleWaitStrategy::Timeout(Duration::from_millis(5)))
        );
    }

    #[test]
    fn route_cell_consumed_state_is_not_pending() {
        let slot = RouteCell::new();

        assert!(!slot.is_populated());
        assert!(slot.publish(7));
        assert!(slot.is_populated());
        assert_eq!(slot.take().and_then(|result| result.ok()), Some(7));
        assert!(!slot.is_populated());
        assert!(!slot.publish_error_if_pending(route_error(
            "context::tests",
            "result was already consumed",
        )));
        assert!(slot.take().is_none());
    }

    #[test]
    fn route_cell_wakes_after_releasing_state_lock() {
        let slot = RouteCell::new();
        let state = Arc::new(ReentrantWake {
            slot: slot.clone(),
            took_value: AtomicBool::new(false),
        });
        let raw = Arc::into_raw(state.clone()) as *const ();
        let waker = unsafe { Waker::from_raw(RawWaker::new(raw, &REENTRANT_WAKER_VTABLE)) };

        slot.register(&waker);
        assert!(slot.publish(11));
        assert!(state.took_value.load(Ordering::Acquire));
        drop(waker);
    }

    #[test]
    fn route_job_drop_publishes_shutdown_error() {
        type Job = fn() -> std::future::Ready<()>;

        let slot = RouteCell::<std::future::Ready<()>>::new();
        let task = RouteJobTask::<Job, std::future::Ready<()>> {
            header: GenericTaskHeader::new_placeholder(
                RouteJobTask::<Job, std::future::Ready<()>>::VTABLE,
            ),
            job: UnsafeCell::new(Some(|| std::future::ready(()))),
            slot: slot.clone(),
            marker: PhantomData,
        };

        drop(task);

        let error = slot
            .take()
            .expect("route drop must publish a result")
            .expect_err("route drop must publish a shutdown error");
        assert!(matches!(
            error.inner(),
            RuntimeError::ShutdownBeforeCompletion
        ));
    }

    #[test]
    fn route_finalizer_drop_finishes_header_and_slot() {
        type Job = fn() -> std::future::Ready<()>;

        let slot = RouteCell::<std::future::Ready<()>>::new();
        let header =
            GenericTaskHeader::new_placeholder(RouteJobTask::<Job, std::future::Ready<()>>::VTABLE);
        let mut finalizer = RouteTaskFinalizer::new(&header, &slot);
        finalizer.publish_err(route_error(
            "context::tests",
            "synthetic route finalizer failure",
        ));
        drop(finalizer);

        assert!(header.is_reclaimable());
        assert!(slot.take().is_some());
    }

    #[cfg(feature = "loom")]
    #[test]
    fn loom_route_cell_publish_and_take_have_one_terminal_value() {
        loom::model(|| {
            let slot = Arc::new(RouteCell::new());
            let published = Arc::new(AtomicBool::new(false));
            let consumed = Arc::new(AtomicBool::new(false));

            let publisher_slot = slot.clone();
            let publisher_published = published.clone();
            let publisher = loom::thread::spawn(move || {
                if publisher_slot.publish(17) {
                    publisher_published.store(true, Ordering::Release);
                }
            });

            let consumer_slot = slot.clone();
            let consumer_consumed = consumed.clone();
            let consumer = loom::thread::spawn(move || {
                if consumer_slot.take().is_some() {
                    consumer_consumed.store(true, Ordering::Release);
                }
            });

            publisher.join().unwrap();
            consumer.join().unwrap();
            if !consumed.load(Ordering::Acquire) {
                assert!(slot.take().is_some());
            }
            assert!(published.load(Ordering::Acquire));
        });
    }
}
