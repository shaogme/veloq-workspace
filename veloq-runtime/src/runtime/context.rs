use std::future::poll_fn;
use std::num::NonZeroUsize;
use std::ops::AsyncFnOnce;
use std::sync::{Arc, mpsc::Receiver};
use std::task::Poll;
use std::time::Duration;

use super::shared::RuntimeShared;
use crate::scope::{AsyncScope, GenericAsyncScope, LocalAsyncScope};
use crate::task::{LocalTaskRef, RuntimeContextExt, SendTaskRef};
use crate::utils::FastRand;
use crate::utils::ownership::{ArcOwnership, RcOwnership};
use crate::utils::storage::{AtomicStorage, LocalStorage};

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
    pub(crate) local_rx: Receiver<LocalTaskRef>,
    pub(crate) remote_rx: Receiver<SendTaskRef>,
    pub(crate) pinned_rx: Receiver<SendTaskRef>,
    pub(crate) rand: FastRand,
}

/// A context handle provided to the `block_on` async closure, allowing creation of scopes.
#[derive(Clone)]
pub struct RuntimeScopeContext {
    pub(crate) shared: Arc<RuntimeShared>,
}

impl RuntimeScopeContext {
    /// Returns the total worker count in the runtime.
    pub fn worker_count(&self) -> NonZeroUsize {
        self.shared.worker_count()
    }

    /// Wakes up the specified worker.
    pub fn wake_worker(&self, worker_id: usize) {
        self.shared.wake_worker(worker_id);
    }

    /// Checks if the runtime is shutting down.
    pub fn is_shutdown(&self) -> bool {
        self.shared
            .shutdown
            .load(std::sync::atomic::Ordering::Acquire)
    }

    /// Returns the shared runtime state.
    pub fn shared(&self) -> &RuntimeShared {
        self.shared.as_ref()
    }

    pub fn route_to<F, Fut>(
        &self,
        worker_id: usize,
        job: F,
    ) -> std::io::Result<crate::runtime::route::RoutedFuture<'_, Fut>>
    where
        F: FnOnce() -> Fut + Send,
        Fut: std::future::Future + Send,
    {
        let slot = crate::runtime::route::RouteCell::new();
        let slot_for_job = slot.clone();

        struct RouteJobTask<F, Fut> {
            header: crate::task::TaskHeader,
            job: core::cell::UnsafeCell<Option<F>>,
            slot: std::sync::Arc<crate::runtime::route::RouteCell<Fut>>,
        }

        impl<F, Fut> crate::task::RawTask for RouteJobTask<F, Fut>
        where
            F: FnOnce() -> Fut + Send,
            Fut: std::future::Future + Send,
        {
            type Storage = crate::utils::storage::AtomicStorage;

            fn poll_raw(&self, _worker_id: usize) -> bool {
                let job = unsafe { &mut *self.job.get() }
                    .take()
                    .expect("job already taken");
                let fut = job();
                self.slot.set(fut);
                // Mark as completed before self-destruct
                self.header.mark_completed_and_notify();
                unsafe {
                    let ptr = self as *const Self as *mut Self;
                    let _ = Box::from_raw(ptr);
                }
                true
            }

            fn header(&self) -> &crate::task::GenericTaskHeader<Self::Storage> {
                &self.header
            }
        }

        impl<F, Fut> RouteJobTask<F, Fut>
        where
            F: FnOnce() -> Fut + Send,
            Fut: std::future::Future + Send,
        {
            const VTABLE: &'static crate::task::TaskVTable<crate::utils::storage::AtomicStorage> =
                &crate::task::TaskVTable {
                    wake: |_| {},
                    wake_by_ref: |_| {},
                    poll: |data, worker_id| unsafe {
                        let node = &*(data.as_ptr() as *const Self);
                        crate::task::RawTask::poll_raw(node, worker_id)
                    },
                };
        }

        let task = Box::new(RouteJobTask {
            header: crate::task::TaskHeader::new(RouteJobTask::<F, Fut>::VTABLE),
            job: core::cell::UnsafeCell::new(Some(job)),
            slot: slot_for_job,
        });

        task.header.set_pinned();
        unsafe {
            task.header
                .set_runtime_info(Arc::as_ptr(&self.shared), worker_id);
        }

        let ptr = Box::into_raw(task);
        let task_ref = unsafe { crate::task::SendTaskRef::from_concrete(ptr) };

        if !self.shared.enqueue_pinned(worker_id, task_ref) {
            unsafe {
                let _ = Box::from_raw(ptr);
            }
            return Err(std::io::Error::other("failed to dispatch job to worker"));
        }

        Ok(crate::runtime::route::RoutedFuture::new(slot))
    }

    pub async fn execute_on_owner<F, Fut, R>(
        &self,
        task: &impl crate::task::TaskHandleRef,
        f: F,
    ) -> std::io::Result<R>
    where
        F: FnOnce() -> Fut + Send,
        Fut: std::future::Future<Output = R> + Send,
        R: Send,
    {
        use std::sync::atomic::Ordering;
        let worker_id =
            crate::utils::storage::StateInt::load(&task.header().worker_id, Ordering::Acquire);
        Ok(self.route_to(worker_id, f)?.await)
    }

    /// Creates a new thread-safe (Send) asynchronous scope.
    pub async fn scope<T, F>(&self, f: F) -> T
    where
        F: for<'b, 's, 'm> AsyncFnOnce(
            &'b GenericAsyncScope<'s, AtomicStorage, ArcOwnership, &'m ()>,
        ) -> T,
    {
        let parent = poll_fn(|cx| Poll::Ready(cx.scope_completion())).await;
        let s = AsyncScope::__private_new(
            RuntimeScopeContext {
                shared: self.shared.clone(),
            },
            parent,
        );
        let res = f(&s).await;
        s.wait_all().await;
        res
    }

    /// Creates a new thread-local asynchronous scope.
    pub async fn scope_local<T, F>(&self, f: F) -> T
    where
        F: for<'b, 's, 'm> AsyncFnOnce(
            &'b GenericAsyncScope<'s, LocalStorage, RcOwnership, *const &'m ()>,
        ) -> T,
    {
        let parent = poll_fn(|cx| Poll::Ready(cx.scope_completion())).await;
        let s = LocalAsyncScope::__private_new(
            RuntimeScopeContext {
                shared: self.shared.clone(),
            },
            parent,
        );
        let res = f(&s).await;
        s.wait_all().await;
        res
    }

    /// Returns the current worker id.
    pub fn worker_id(&self) -> usize {
        self.shared
            .context_tls
            .get()
            .map(|ptr| unsafe { ptr.as_ref().worker_id })
            .unwrap_or(usize::MAX)
    }
}

pub type IdleHook = fn() -> IdleDecision;
pub type WorkerTickHook = fn();

/// Worker initialization context passed to the injected worker init step.
#[derive(Clone)]
pub struct WorkerInitContext {
    pub shared: Arc<RuntimeShared>,
    worker_id: usize,
    worker_count: NonZeroUsize,
}

impl WorkerInitContext {
    pub(crate) fn new(
        shared: Arc<RuntimeShared>,
        worker_id: usize,
        worker_count: NonZeroUsize,
    ) -> Self {
        Self {
            shared,
            worker_id,
            worker_count,
        }
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
    pub fn runtime_context(&self) -> RuntimeScopeContext {
        RuntimeScopeContext {
            shared: self.shared.clone(),
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
        match decision {
            IdleDecision::Wait(IdleWaitStrategy::Timeout(duration)) => {
                assert_eq!(duration, Duration::from_millis(5));
            }
            _ => panic!("unexpected idle decision"),
        }
    }
}
