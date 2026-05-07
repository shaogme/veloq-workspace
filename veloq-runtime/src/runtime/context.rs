use super::shared::RuntimeShared;
use crate::task::{LocalTaskRef, SendTaskRef};
use crate::utils::FastRand;
use std::cell::RefCell;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::mpsc::Receiver;
use std::time::Duration;

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
    pub(crate) shared: Arc<RuntimeShared>,
    pub(crate) worker_id: usize,
    pub(crate) local_rx: Receiver<LocalTaskRef>,
    pub(crate) remote_rx: Receiver<SendTaskRef>,
    pub(crate) rand: RefCell<FastRand>,
    pub(crate) idle_hook: Option<IdleHook>,
    pub(crate) worker_tick_hook: Option<WorkerTickHook>,
}

pub type IdleHook = fn() -> IdleDecision;
pub type WorkerTickHook = fn();

thread_local! {
    pub(crate) static CONTEXT: RefCell<Option<RuntimeContext>> = const { RefCell::new(None) };
}

/// Worker initialization context passed to the injected worker init step.
#[derive(Debug, Clone, Copy)]
pub struct WorkerInitContext {
    worker_id: usize,
    worker_count: NonZeroUsize,
}

impl WorkerInitContext {
    pub(crate) fn new(worker_id: usize, worker_count: NonZeroUsize) -> Self {
        Self {
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
}

pub fn current_worker_id() -> usize {
    CONTEXT.with(|ctx| {
        ctx.borrow()
            .as_ref()
            .map(|c| c.worker_id)
            .unwrap_or(usize::MAX)
    })
}

pub fn wake_worker(worker_id: usize) {
    CONTEXT.with(|ctx| {
        if let Some(runtime) = ctx.borrow().as_ref() {
            runtime.shared.registry.unpark(worker_id);
        }
    });
}

pub fn set_current_runtime_context(context: RuntimeContext) {
    CONTEXT.with(|ctx| {
        *ctx.borrow_mut() = Some(context);
    });
}

pub fn clear_current_runtime_context() {
    CONTEXT.with(|ctx| {
        *ctx.borrow_mut() = None;
    });
}

pub(crate) fn with_current_runtime<R>(f: impl FnOnce(&Arc<RuntimeShared>) -> R) -> Option<R> {
    CONTEXT.with(|ctx| ctx.borrow().as_ref().map(|c| f(&c.shared)))
}

pub(crate) fn run_worker_idle_hook() -> IdleDecision {
    CONTEXT.with(|ctx| {
        ctx.borrow()
            .as_ref()
            .map_or(IdleDecision::wait(IdleWaitStrategy::Block), |c| {
                c.idle_hook
                    .map_or(IdleDecision::wait(IdleWaitStrategy::Block), |h| h())
            })
    })
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
