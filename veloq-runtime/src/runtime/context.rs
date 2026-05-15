use std::future::poll_fn;
use std::num::NonZeroUsize;
use std::ops::AsyncFnOnce;
use std::sync::mpsc::Receiver;
use std::task::Poll;
use std::time::Duration;

use super::shared::RuntimeShared;
use crate::scope::{AsyncScope, GenericAsyncScope, LocalAsyncScope};
use crate::task::{LocalTaskRef, RuntimeContextExt, SendTaskRef};
use crate::utils::FastRand;
use crate::utils::ownership::{ArcOwnership, RcOwnership};
use crate::utils::storage::{AtomicStorage, LocalStorage};

use veloq_tls::Tls;

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
    pub(crate) idle_hook: Option<IdleHook>,
    pub(crate) worker_tick_hook: Option<WorkerTickHook>,
}

/// A context handle provided to the `block_on` async closure, allowing creation of scopes.
#[derive(Clone, Copy)]
pub struct RuntimeScopeContext<'a> {
    pub(crate) shared: &'a RuntimeShared,
}

impl<'a> RuntimeScopeContext<'a> {
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

    /// Creates a new thread-safe (Send) asynchronous scope.
    pub async fn scope<T, F>(&self, f: F) -> T
    where
        F: for<'b, 's, 'm> AsyncFnOnce(
            &'b GenericAsyncScope<'s, AtomicStorage, ArcOwnership, &'m ()>,
        ) -> T,
    {
        let parent = poll_fn(|cx| Poll::Ready(cx.scope_completion())).await;
        let s = AsyncScope::__private_new(self.shared, parent);
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
        let s = LocalAsyncScope::__private_new(self.shared, parent);
        let res = f(&s).await;
        s.wait_all().await;
        res
    }
}

pub type IdleHook = fn() -> IdleDecision;
pub type WorkerTickHook = fn();

pub static CONTEXT: Tls<RuntimeContext> = Tls::new();

/// Worker initialization context passed to the injected worker init step.
#[derive(Clone, Copy)]
pub struct WorkerInitContext<'a> {
    pub shared: &'a RuntimeShared,
    worker_id: usize,
    worker_count: NonZeroUsize,
}

impl<'a> WorkerInitContext<'a> {
    pub(crate) fn new(
        shared: &'a RuntimeShared,
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
    pub fn runtime_context(&self) -> RuntimeScopeContext<'_> {
        RuntimeScopeContext {
            shared: self.shared,
        }
    }
}

#[inline(always)]
pub fn current_worker_id() -> usize {
    CONTEXT
        .get()
        .map(|ptr| unsafe { ptr.as_ref().worker_id })
        .unwrap_or(usize::MAX)
}

pub fn run_worker_idle_hook() -> IdleDecision {
    CONTEXT
        .get()
        .map(|ptr| unsafe { ptr.as_ref() })
        .and_then(|c| c.idle_hook.map(|h| h()))
        .unwrap_or(IdleDecision::wait(IdleWaitStrategy::Block))
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

    #[test]
    fn idle_hook_defaults_to_block_without_context() {
        assert!(matches!(
            run_worker_idle_hook(),
            IdleDecision::Wait(IdleWaitStrategy::Block)
        ));
    }
}
