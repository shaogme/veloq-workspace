//! Cooperative multitasking budget.
//!
//! This module provides a thread-local budget that is decremented by cooperative
//! operations (like I/O submission). When the budget reaches zero, the operation
//! returns `Poll::Pending` and wakes the current task, forcing it to yield to the
//! executor.

use std::cell::Cell;
use std::task::{Context, Poll};

/// The default budget for a task.
///
/// This value is chosen to balance throughput and latency.
/// A higher value improves throughput (fewer context switches),
/// while a lower value improves latency (more frequent preemption).
pub const DEFAULT_BUDGET: u32 = 128;

thread_local! {
    /// The current budget for the executing task.
    static BUDGET: Cell<u32> = const { Cell::new(DEFAULT_BUDGET) };
}

/// Run a closure with a fresh budget.
///
/// This should be called by the executor before polling a task.
#[inline]
pub fn budget<F, R>(f: F) -> R
where
    F: FnOnce() -> R,
{
    // Reset budget to default
    BUDGET.with(|cell| cell.set(DEFAULT_BUDGET));
    f()
}

/// Check if the current task has remaining budget.
#[inline]
pub fn has_remaining() -> bool {
    BUDGET.with(|cell| cell.get() > 0)
}

/// Check if the current task has budget to proceed.
///
/// If the budget is sufficient, it is decremented and `Poll::Ready` is returned.
/// If the budget is exhausted, the current task is notified (woken) and
/// `Poll::Pending` is returned, forcing a yield.
#[inline]
pub fn poll_proceed(cx: &mut Context<'_>) -> Poll<()> {
    BUDGET.with(|cell| {
        let budget = cell.get();

        if budget > 0 {
            cell.set(budget - 1);
            Poll::Ready(())
        } else {
            // Budget exhausted.
            // We do NOT reset the budget here. The executor will reset it when the task is re-scheduled.

            // Wake the current task to ensure it gets scheduled again.
            cx.waker().wake_by_ref();

            Poll::Pending
        }
    })
}
