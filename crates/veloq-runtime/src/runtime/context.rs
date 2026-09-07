use core::cell::UnsafeCell;
use std::{
    future::{Future, poll_fn},
    marker::PhantomData,
    num::NonZeroUsize,
    ops::AsyncFnOnce,
    pin::Pin,
    ptr::NonNull,
    sync::{Arc, Mutex, atomic::Ordering},
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
        AnyScopeRef, GenericTaskHeader, RawTask, RuntimeContextExt, ScopeRef, SendTaskRef,
        TaskHandleRef, TaskHeader, TaskVTable,
    },
    utils::FastRand,
};

use crossbeam_deque::Worker;
use diagweave::prelude::*;
use veloq_storage::AtomicStorage;
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
    pub fn wake_worker(&self, worker_id: usize) {
        self.shared().wake_worker(worker_id);
    }

    /// Checks if the runtime is shutting down.
    pub(crate) fn is_shutdown(&self) -> bool {
        self.shared().base.shutdown.load(Ordering::Acquire)
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

        #[repr(C)]
        struct RouteJobTask<'scope_ref, F, Fut> {
            header: TaskHeader,
            job: UnsafeCell<Option<F>>,
            slot: Arc<RouteCell<Fut>>,
            marker: PhantomData<&'scope_ref ()>,
        }

        impl<'scope_ref, F, Fut> RawTask for RouteJobTask<'scope_ref, F, Fut>
        where
            F: FnOnce() -> Fut + Send + 'scope_ref,
            Fut: Future + Send + 'scope_ref,
        {
            type Storage = AtomicStorage;

            fn poll_raw(&self, _worker_id: usize) -> Result<bool> {
                let Some(job) = (unsafe { &mut *self.job.get() }).take() else {
                    self.slot.fail(
                        RuntimeError::InvariantViolation {
                            site: "RuntimeCtx::route_to::RouteJobTask::poll_raw",
                            detail: "job already taken".into(),
                        }
                        .to_report()
                        .with_category("runtime.route"),
                    )?;
                    self.header.mark_completed_and_notify();
                    unsafe {
                        let header_ptr = NonNull::from(&self.header);
                        GenericTaskHeader::drop_task(header_ptr);
                    }
                    return Ok(true);
                };
                let fut = job();
                self.slot.set(fut)?;
                // Mark as completed before self-destruct
                self.header.mark_completed_and_notify();
                unsafe {
                    let header_ptr = NonNull::from(&self.header);
                    GenericTaskHeader::drop_task(header_ptr);
                }
                Ok(true)
            }

            fn header(&self) -> &GenericTaskHeader<Self::Storage> {
                &self.header
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
                    let raw_ptr = header as *const GenericTaskHeader<AtomicStorage> as *const ();
                    let node = &*(raw_ptr as *const Self);
                    RawTask::poll_raw(node, worker_id)
                },
                drop: |data| unsafe {
                    let ptr = data.as_ptr() as *mut Self;
                    let _ = Box::from_raw(ptr);
                },
            };
        }

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
            EnqueuePinnedOutcome::AbortedAcknowledged
            | EnqueuePinnedOutcome::AlreadySettled
            | EnqueuePinnedOutcome::NeedsCallerSettle => {
                unsafe {
                    let _ = Box::from_raw(ptr);
                }
                let current_worker = self.worker_id();
                let is_shutdown = self.is_shutdown();
                return RuntimeError::DispatchFailed {
                    target_worker: worker_id,
                    current_worker,
                }
                .with_ctx("is_shutdown", is_shutdown);
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
        self.shared()
            .base
            .tls
            .try_with(|ctx| ctx.worker_id)
            .expect("Failed to get worker id: this should be invoked from a worker thread")
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

pub(crate) type IdleHook<T> = fn(&RuntimeShared<T>) -> Result<IdleDecision>;
pub(crate) type WorkerTickHook = fn();

pub(crate) struct RouteCell<T> {
    value: Mutex<Option<Result<T>>>,
    waker: MwsrWaker,
}

impl<T> RouteCell<T> {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            value: Mutex::new(None),
            waker: MwsrWaker::new(),
        })
    }

    pub(crate) fn set(&self, value: T) -> Result<()> {
        let mut slot = self.value.lock().map_err(|_| RuntimeError::PoisonedLock {
            component: "runtime.route_slot",
        })?;
        debug_assert!(slot.is_none(), "worker route slot already populated");
        *slot = Some(Ok(value));
        self.waker.wake();
        Ok(())
    }

    pub(crate) fn fail(&self, err: Report<RuntimeError>) -> Result<()> {
        let mut slot = self.value.lock().map_err(|_| RuntimeError::PoisonedLock {
            component: "runtime.route_slot",
        })?;
        debug_assert!(slot.is_none(), "worker route slot already populated");
        *slot = Some(Err(err));
        self.waker.wake();
        Ok(())
    }

    pub(crate) fn is_populated(&self) -> bool {
        self.value
            .lock()
            .map(|slot| slot.is_some())
            .unwrap_or(false)
    }

    pub(crate) fn take(&self) -> Result<Option<Result<T>>> {
        Ok(self
            .value
            .lock()
            .map_err(|_| RuntimeError::PoisonedLock {
                component: "runtime.route_slot",
            })?
            .take())
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

        if let Some(op) = self.slot.take()? {
            match op {
                Ok(op) => {
                    self.inner = Some(op);
                    Poll::Ready(Ok(()))
                }
                Err(err) => Poll::Ready(Err(err)),
            }
        } else {
            self.slot.register(cx.waker());
            if let Some(op) = self.slot.take()? {
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
    use std::time::Duration;

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
}
