//! 唯一的 operation 生命周期账本。
//!
//! registry 仍然负责 slot storage 和 completion mailbox，但 operation 的 phase、timer
//! 与 generation 校验只能从这个 owner 进入。其它组件拿到的是短生命周期的 slot view，
//! 不再拥有生命周期状态的公开写入口。
//! This owner centralizes operation phases, timers, and generation checks; other components receive only short-lived slot views.

use core::ops::{Deref, DerefMut};

use crate::{
    driver::{
        control::ControlPlaneObserver,
        lifecycle::{CancellationPhase, SubmissionPhase, UringOpState},
    },
    error::{UringError, UringResult},
    op::{CheckedSlotView, SlotView, UringOpRegistry, UringOpRegistryExt, UringSlotSpec},
};
use veloq_driver_core::driver::{OpToken, SharedCompletionTable};
use veloq_wheel::TimerId;

/// operation registry 的唯一 owner。
/// The sole owner of the operation registry.
pub(crate) struct OperationLedger {
    registry: UringOpRegistry,
}

impl OperationLedger {
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            registry: UringOpRegistry::new(capacity),
        }
    }

    /// 在 generation 校验后的 operation state 上执行一个生命周期命令。
    /// Applies one lifecycle command after validating the operation generation.
    fn with_state<R>(
        &mut self,
        token: OpToken,
        operation: impl FnOnce(&mut UringOpState) -> R,
    ) -> UringResult<R> {
        match self.registry.checked_slot_view(token)? {
            CheckedSlotView::Valid(SlotView::Reserved(mut slot)) => {
                Ok(operation(slot.platform_mut()))
            }
            CheckedSlotView::Valid(SlotView::InFlightWaiting(mut slot)) => {
                Ok(operation(slot.platform_mut()))
            }
            CheckedSlotView::Valid(SlotView::InFlightOrphaned(mut slot)) => {
                Ok(operation(slot.platform_mut()))
            }
            CheckedSlotView::Empty(_) => Err(UringError::InvalidState.report(
                "uring.operation.state",
                "operation command targeted an idle slot",
            )),
            CheckedSlotView::Missing { .. } | CheckedSlotView::Stale(_) => {
                Err(UringError::InvalidState.report(
                    "uring.operation.state",
                    "operation command targeted a missing or stale generation",
                ))
            }
        }
    }

    pub(crate) fn transition_submission(
        &mut self,
        token: OpToken,
        next: SubmissionPhase,
        reason: &'static str,
        observer: &mut ControlPlaneObserver,
    ) -> UringResult<()> {
        self.with_state(token, |state| {
            state.transition_submission_phase(token, next, reason, observer);
        })
    }

    pub(crate) fn set_cancellation(
        &mut self,
        token: OpToken,
        phase: CancellationPhase,
    ) -> UringResult<()> {
        self.with_state(token, |state| state.set_cancellation_phase(phase))
    }

    pub(crate) fn arm_timer(&mut self, token: OpToken, timer: TimerId) -> UringResult<()> {
        self.with_state(token, |state| state.arm_timer(timer))
    }

    pub(crate) fn take_timer(&mut self, token: OpToken) -> UringResult<Option<TimerId>> {
        self.with_state(token, UringOpState::take_timer)
    }

    pub(crate) fn clear_timer(&mut self, token: OpToken) -> UringResult<()> {
        self.with_state(token, UringOpState::clear_timer)
    }

    pub(crate) fn mark_terminal(
        &mut self,
        token: OpToken,
        observer: &mut ControlPlaneObserver,
        reason: &'static str,
    ) -> UringResult<()> {
        self.clear_timer(token)?;
        self.transition_submission(token, SubmissionPhase::Terminal, reason, observer)
    }
}

impl Deref for OperationLedger {
    type Target = UringOpRegistry;

    fn deref(&self) -> &Self::Target {
        &self.registry
    }
}

impl DerefMut for OperationLedger {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.registry
    }
}

impl OperationLedger {
    pub(crate) fn shared_table(&self) -> SharedCompletionTable<UringSlotSpec> {
        self.registry.shared_table()
    }
}
