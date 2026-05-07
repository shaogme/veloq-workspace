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

pub struct RuntimeContext {
    pub(crate) shared: Arc<RuntimeShared>,
    pub(crate) worker_id: usize,
    pub(crate) local_rx: Receiver<LocalTaskRef>,
    pub(crate) remote_rx: Receiver<SendTaskRef>,
    pub(crate) rand: RefCell<FastRand>,
    pub(crate) idle_hook: Option<IdleHook>,
}

pub type IdleHook = fn() -> IdleWaitStrategy;

thread_local! {
    #[cfg_attr(all(target_arch = "x86_64", target_os = "windows", target_env = "gnu"), expect(clippy::missing_const_for_thread_local))]
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

pub(crate) fn run_worker_idle_hook() -> IdleWaitStrategy {
    CONTEXT.with(|ctx| {
        ctx.borrow().as_ref().map_or(IdleWaitStrategy::Block, |c| {
            c.idle_hook.map_or(IdleWaitStrategy::Block, |h| h())
        })
    })
}
