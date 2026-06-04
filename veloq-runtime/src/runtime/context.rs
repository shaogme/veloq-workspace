use core::cell::UnsafeCell;
use std::future::{Future, poll_fn};
use std::marker::PhantomData;
use std::num::NonZeroUsize;
use std::ops::AsyncFnOnce;
use std::pin::Pin;
use std::ptr::NonNull;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex, mpsc::Receiver};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use super::shared::RuntimeShared;
use crate::error::{Result, RuntimeError};
use crate::scope::{AsyncScope, LocalAsyncScope};
use crate::task::{
    GenericTaskHeader, LocalTaskRef, RawTask, RuntimeContextExt, ScopeRef, SendTaskRef,
    TaskHandleRef, TaskHeader, TaskVTable,
};
use crate::utils::FastRand;
use crate::utils::storage::AtomicStorage;

use veloq_atomic_waker::AtomicWaker;

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
}

pub struct RuntimeContext {
    pub(crate) worker_id: usize,
    pub(crate) remote_rx: Receiver<SendTaskRef>,
    pub(crate) pinned_rx: Receiver<SendTaskRef>,
    pub(crate) local_rx: Receiver<LocalTaskRef>,
    pub(crate) rand: FastRand,
}

unsafe impl Send for RuntimeContext {}
unsafe impl Sync for RuntimeContext {}

/// A context handle provided to the `block_on` async closure, allowing creation of scopes.
pub struct RuntimeScopeContext<T> {
    shared: *const RuntimeShared<T>,
}

unsafe impl<T> Send for RuntimeScopeContext<T> {}
unsafe impl<T> Sync for RuntimeScopeContext<T> {}

impl<T> Copy for RuntimeScopeContext<T> {}

impl<T> Clone for RuntimeScopeContext<T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T> RuntimeScopeContext<T> {
    pub fn new(shared: *const RuntimeShared<T>) -> Self {
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
    pub fn is_shutdown(&self) -> bool {
        self.shared().base.shutdown.load(Ordering::Acquire)
    }

    /// Returns the shared runtime state.
    pub fn shared<'a>(&self) -> &'a RuntimeShared<T> {
        unsafe { &*self.shared }
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

            fn poll_raw(&self, _worker_id: usize) -> bool {
                let job = unsafe { &mut *self.job.get() }
                    .take()
                    .expect("job already taken");
                let fut = job();
                self.slot.set(fut);
                // Mark as completed before self-destruct
                self.header.mark_completed_and_notify();
                unsafe {
                    let header_ptr = NonNull::from(&self.header);
                    GenericTaskHeader::drop_task(header_ptr);
                }
                true
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

        if !self.shared().enqueue_pinned(worker_id, task_ctx) {
            unsafe {
                let _ = Box::from_raw(ptr);
            }
            let current_worker = self.worker_id();
            let is_shutdown = self.is_shutdown();
            return Err(
                RuntimeError::dispatch_failed_report(worker_id, current_worker)
                    .with_ctx("is_shutdown", is_shutdown),
            );
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
        Ok(self.route_to(worker_id, f)?.await)
    }

    /// Creates a new thread-safe (Send) asynchronous scope.
    pub async fn scope<'scope, R, F>(&self, f: F) -> R
    where
        F: for<'scope_ref> AsyncFnOnce(&'scope_ref AsyncScope<'scope, T>) -> R,
    {
        let parent = poll_fn(|cx| Poll::Ready(cx.scope_completion())).await;
        let s = AsyncScope::new(
            RuntimeScopeContext {
                shared: self.shared,
            },
            parent,
        );
        let res = f(&s).await;
        s.wait_all().await;
        res
    }

    /// Creates a new thread-local asynchronous scope.
    pub async fn scope_local<'scope, R, F>(&self, f: F) -> R
    where
        F: for<'scope_ref> AsyncFnOnce(&'scope_ref LocalAsyncScope<'scope, T>) -> R,
    {
        let parent = poll_fn(|cx| Poll::Ready(cx.scope_completion())).await;
        let s = LocalAsyncScope::new(
            RuntimeScopeContext {
                shared: self.shared,
            },
            parent,
        );
        let res = f(&s).await;
        s.wait_all().await;
        res
    }

    /// Returns the current worker id.
    pub fn worker_id(&self) -> usize {
        self.shared()
            .base
            .tls
            .try_with(|ctx| ctx.worker_id)
            .unwrap_or(usize::MAX)
    }
}

pub type IdleHook<T> = fn(&RuntimeShared<T>) -> IdleDecision;
pub type WorkerTickHook = fn();

/// Worker initialization context passed to the injected worker init step.
pub struct WorkerInitContext<'scope, T> {
    shared: &'scope RuntimeShared<T>,
    worker_id: usize,
    worker_count: NonZeroUsize,
}

impl<'scope, T> Clone for WorkerInitContext<'scope, T> {
    fn clone(&self) -> Self {
        Self {
            shared: self.shared,
            worker_id: self.worker_id,
            worker_count: self.worker_count,
        }
    }
}

impl<'scope, T> WorkerInitContext<'scope, T> {
    pub(crate) fn new(
        shared: &'scope RuntimeShared<T>,
        worker_id: usize,
        worker_count: NonZeroUsize,
    ) -> Self {
        Self {
            shared,
            worker_id,
            worker_count,
        }
    }

    pub fn shared(&self) -> &'scope RuntimeShared<T> {
        self.shared
    }

    /// Returns the current worker id.
    #[inline]
    pub fn worker_id(&self) -> usize {
        self.worker_id
    }

    /// Returns the total worker count in the runtime.
    #[inline]
    pub fn worker_count(&self) -> NonZeroUsize {
        self.worker_count
    }

    /// Returns the runtime scope context.
    #[inline]
    pub fn scope(&self) -> RuntimeScopeContext<T> {
        RuntimeScopeContext {
            shared: self.shared,
        }
    }

    /// Returns the custom worker extra state.
    #[inline]
    pub fn extra<R>(&self, f: impl FnOnce(&T) -> R) -> R {
        self.shared
            .extra_tls
            .try_with(|extra| f(extra))
            .expect("extra TLS accessed outside of a worker thread")
    }
}

pub struct RouteCell<T> {
    value: Mutex<Option<T>>,
    waker: AtomicWaker,
}

impl<T> RouteCell<T> {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            value: Mutex::new(None),
            waker: AtomicWaker::new(),
        })
    }

    pub(crate) fn set(&self, value: T) {
        let mut slot = self.value.lock().expect("worker route slot poisoned");
        debug_assert!(slot.is_none(), "worker route slot already populated");
        *slot = Some(value);
        self.waker.wake();
    }

    pub(crate) fn take(&self) -> Option<T> {
        self.value
            .lock()
            .expect("worker route slot poisoned")
            .take()
    }

    pub(crate) fn register(&self, waker: &Waker) {
        self.waker.register(waker);
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
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };

        if this.inner.is_none() {
            if let Some(op) = this.slot.take() {
                this.inner = Some(op);
            } else {
                this.slot.register(cx.waker());
                if let Some(op) = this.slot.take() {
                    this.inner = Some(op);
                } else {
                    return Poll::Pending;
                }
            }
        }
        let inner = this.inner.as_mut().expect("route future missing inner op");
        unsafe { Pin::new_unchecked(inner) }.poll(cx)
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
        match decision {
            IdleDecision::Wait(IdleWaitStrategy::Timeout(duration)) => {
                assert_eq!(duration, Duration::from_millis(5));
            }
            _ => panic!("unexpected idle decision"),
        }
    }
}
