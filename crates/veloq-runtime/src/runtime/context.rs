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
    scope::{AsyncScope, LocalAsyncScope},
    task::{
        GenericTaskHeader, RawTask, RuntimeContextExt, ScopeRef, SendTaskRef, TaskHandleRef,
        TaskHeader, TaskVTable,
    },
    utils::FastRand,
};

use crossbeam_deque::Worker;
use diagweave::prelude::*;
use veloq_waker::MwsrWaker;
use veloq_storage::AtomicStorage;

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
pub struct RuntimeCtx<'rt, T> {
    shared: &'rt RuntimeShared<T>,
}

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
        Self { shared }
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
        self.shared
    }

    /// 为 `select!` 公平模式返回 `[0, branches)` 范围内的随机起始分支索引。
    #[doc(hidden)]
    pub fn select_poll_start(&self, branches: u32) -> u32 {
        self.shared()
            .base
            .tls
            .with(|ctx| ctx.rand.next_u32(branches))
    }

    pub async fn scope<'env, 'scope, F, R>(&self, f: F) -> Result<R>
    where
        'env: 'scope,
        F: for<'scope_ref> AsyncFnOnce(&'scope_ref AsyncScope<'rt, 'scope, 'env, T>) -> R,
    {
        let parent = poll_fn(|cx| Poll::Ready(cx.scope_completion())).await;
        let scope = AsyncScope::new(*self, parent);
        let s_ref = &scope;
        let res = f(s_ref).await;
        scope.wait_all().await?;
        Ok(res)
    }

    pub async fn scope_local<'env, 'scope, F, R>(&self, f: F) -> Result<R>
    where
        'env: 'scope,
        F: for<'scope_ref> AsyncFnOnce(&'scope_ref LocalAsyncScope<'rt, 'scope, 'env, T>) -> R,
    {
        let parent = poll_fn(|cx| Poll::Ready(cx.scope_completion())).await;
        let scope = LocalAsyncScope::new(*self, parent);
        let s_ref = &scope;
        let res = f(s_ref).await;
        scope.wait_all().await?;
        Ok(res)
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
}

impl<F> Future for RoutedFuture<F>
where
    F: Future,
{
    type Output = Result<F::Output>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };

        if this.inner.is_none() {
            if let Some(op) = this.slot.take()? {
                match op {
                    Ok(op) => this.inner = Some(op),
                    Err(err) => return Poll::Ready(Err(err)),
                }
            } else {
                this.slot.register(cx.waker());
                if let Some(op) = this.slot.take()? {
                    match op {
                        Ok(op) => this.inner = Some(op),
                        Err(err) => return Poll::Ready(Err(err)),
                    }
                } else {
                    return Poll::Pending;
                }
            }
        }
        let Some(inner) = this.inner.as_mut() else {
            let err = RuntimeError::InvariantViolation {
                site: "RoutedFuture::poll",
                detail: "route future missing inner op".into(),
            }
            .to_report()
            .with_category("runtime.route");
            return Poll::Ready(Err(err));
        };

        match unsafe { Pin::new_unchecked(inner) }.poll(cx) {
            Poll::Ready(output) => Poll::Ready(Ok(output)),
            Poll::Pending => Poll::Pending,
        }
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
