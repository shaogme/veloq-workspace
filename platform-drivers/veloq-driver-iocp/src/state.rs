use std::time::{Duration, Instant};
use veloq_wheel::TaskId;

/// Represents the lifecycle stage of an IOCP operation.
#[derive(Debug)]
pub enum OpLifecycle {
    /// Created, resources attached, waiting to be submitted
    Pending,
    /// Submitted to true OS operations (IOCP/RIO)
    InFlight,
    /// Completion received or Timer fired
    Completed,
    /// Cancelled by user
    Cancelled,
}

/// State associated with an IOCP operation.
pub struct IocpOpState {
    pub(crate) generation: u32,
    pub(crate) lifecycle: OpLifecycle,
    pub(crate) timer_id: Option<TaskId>,
    pub(crate) timer_deadline: Option<Instant>,
    pub(crate) is_background: bool,
    // For RIO cancel path: the slot can be recycled only after both:
    // 1) user has consumed completion; 2) late RIO CQE has been drained.
    pub(crate) rio_needs_drain: bool,
    pub(crate) rio_drained: bool,
    // recv_from served by internal RIO UDP pre-post pool; no per-op kernel I/O in flight.
    pub(crate) rio_pool_waiting: bool,
}

impl Default for IocpOpState {
    fn default() -> Self {
        Self {
            generation: 0,
            lifecycle: OpLifecycle::Pending,
            timer_id: None,
            timer_deadline: None,
            is_background: false,
            rio_needs_drain: false,
            rio_drained: false,
            rio_pool_waiting: false,
        }
    }
}

impl IocpOpState {
    pub(crate) fn new() -> Self {
        Self::default()
    }
}

/// Closing mode for the driver or operations.
#[derive(Clone, Copy, Debug)]
pub enum CloseMode {
    /// Closes quickly without waiting for pending operations.
    Fast,
    /// Closes after a specified timeout, allowing pending operations to finish.
    Strict { timeout: Duration },
}
