use veloq_std::format;

use diagweave::prelude::*;
use veloq_io_uring::cqueue;

use crate::{
    diagnostics::UringCompletionDiagnostics,
    driver::registration::{
        ProvidedBufLease, ProvidedBufLeaseAction, ProvidedBufLeaseError, ProvidedBufLeasePhase,
        ProvidedBufPort,
    },
    error::{UringError, UringResult},
};

/// The driver state a completion is allowed to reach.
///
/// Mirrors [`SqeEnv`] on the other end of an operation. The completion path holds a slot
/// borrowed out of the operation registry while it builds the record, so it cannot be handed the
/// whole driver either — but unlike submission it does need to *mutate* something: a CQE that
/// carries a buffer id has consumed a ring entry, and that entry has to be settled before the
/// record leaves this function.
/// 完成路径只能访问此投影视图。CQE 选中的 buffer 已从 ring 消耗，必须在记录离开此函数前结算。
pub(crate) struct CqeEnv<'provided, 'diagnostics> {
    provided: Option<ProvidedBufPort<'provided>>,
    diagnostics: &'diagnostics UringCompletionDiagnostics,
    selected_buffer_state: SelectedBufferState,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SelectedBufferState {
    Unclaimed,
    Claimed { bid: u16, publish_seq: u64 },
    HandedOff { bid: u16, publish_seq: u64 },
    Settled,
    Quarantined,
}

impl<'provided, 'diagnostics> CqeEnv<'provided, 'diagnostics> {
    #[inline]
    pub(crate) fn new(
        provided: Option<ProvidedBufPort<'provided>>,
        diagnostics: &'diagnostics UringCompletionDiagnostics,
    ) -> Self {
        Self {
            provided,
            diagnostics,
            selected_buffer_state: SelectedBufferState::Unclaimed,
        }
    }

    /// Claims the buffer this CQE selected without refilling its slot in the ring.
    ///
    /// `Ok(None)` means the completion selected no buffer at all — `-ENOBUFS`, or an operation
    /// that never asked for one. A CQE that *does* carry a buffer id while this driver has no
    /// ring registered is an error rather than a `None`: the kernel cannot have picked from a
    /// group that does not exist, so silently dropping it would hide a real bug.
    /// `Ok(None)` 表示 CQE 未选择 buffer；若 CQE 带 BID 但本驱动没有注册 ring，则返回错误以暴露内核状态异常。
    pub(crate) fn take_selected_lease(
        &mut self,
        flags: u32,
        res: i32,
    ) -> UringResult<Option<ProvidedBufLease>> {
        let selected = cqueue::buffer_select(flags).is_some();
        match self.provided.as_mut() {
            Some(group) => {
                let lease = group
                    .claim_selected_lease(flags, res)
                    .map_err(|error| lease_error_report(error, flags))?;
                if let Some(lease) = &lease {
                    self.selected_buffer_state = SelectedBufferState::Claimed {
                        bid: lease.bid(),
                        publish_seq: lease.publish_seq(),
                    };
                } else if selected {
                    self.diagnostics.inc_provided_unknown_bid();
                }
                Ok(lease)
            }
            None if selected => UringError::InvalidState
                .push_ctx("scope", "uring.driver.cqe_env.take_selected_lease")
                .with_ctx("cqe_flags", flags)
                .attach_note("completion selected a provided buffer but no ring is registered"),
            None => Ok(None),
        }
    }

    pub(crate) fn handoff_selected_lease(
        &mut self,
        lease: &mut ProvidedBufLease,
    ) -> UringResult<()> {
        let Some(group) = self.provided.as_mut() else {
            return UringError::InvalidState
                .push_ctx("scope", "uring.driver.cqe_env.handoff_selected_lease")
                .attach_note("selected lease has no provided buffer owner");
        };
        group
            .handoff_selected_lease(lease)
            .map_err(|error| lease_error_report(error, 0))?;
        self.selected_buffer_state = SelectedBufferState::HandedOff {
            bid: lease.bid(),
            publish_seq: lease.publish_seq(),
        };
        Ok(())
    }

    pub(crate) fn begin_selected_delivery(
        &mut self,
        lease: &mut ProvidedBufLease,
    ) -> UringResult<()> {
        let Some(group) = self.provided.as_mut() else {
            return UringError::InvalidState
                .push_ctx("scope", "uring.driver.cqe_env.begin_selected_delivery")
                .attach_note("selected lease has no provided buffer owner");
        };
        group
            .begin_delivery(lease)
            .map_err(|error| lease_error_report(error, 0))?;
        Ok(())
    }

    pub(crate) fn settle_selected_lease(
        &mut self,
        lease: &mut ProvidedBufLease,
        action: ProvidedBufLeaseAction,
    ) -> UringResult<()> {
        let Some(group) = self.provided.as_mut() else {
            return UringError::InvalidState
                .push_ctx("scope", "uring.driver.cqe_env.settle_selected_lease")
                .attach_note("selected lease has no provided buffer owner");
        };
        let result = group
            .settle_selected_lease(lease, action)
            .map_err(|error| lease_error_report(error, 0));
        self.selected_buffer_state = match lease.phase() {
            ProvidedBufLeasePhase::Quarantined => SelectedBufferState::Quarantined,
            ProvidedBufLeasePhase::Settled => SelectedBufferState::Settled,
            _ => self.selected_buffer_state,
        };
        result
    }

    /// Hands the buffer this CQE selected straight back to the ring.
    ///
    /// For completions that are being discarded (cancelled, orphaned, stale token). Skipping it
    /// leaks one buffer id per discarded completion — "cancellation is not termination" in its
    /// provided-buffer form.
    /// 被丢弃的完成（取消、孤儿或过期 token）需归还所选 buffer，否则每次都会泄漏一个 BID。
    pub(crate) fn return_provided_buf(&mut self, flags: u32) {
        if !matches!(
            self.selected_buffer_state,
            SelectedBufferState::Unclaimed | SelectedBufferState::Claimed { .. }
        ) || cqueue::buffer_select(flags).is_none()
        {
            return;
        }
        if let Some(group) = self.provided.as_mut() {
            let quarantined = match self.selected_buffer_state {
                SelectedBufferState::Unclaimed => group.discard_selected(flags),
                SelectedBufferState::Claimed { bid, publish_seq } => {
                    group.quarantine_claimed(bid, publish_seq)
                }
                _ => false,
            };
            if quarantined {
                self.diagnostics.inc_provided_unknown_bid();
            }
            self.selected_buffer_state = if quarantined {
                SelectedBufferState::Quarantined
            } else {
                SelectedBufferState::Settled
            };
        }
    }

    /// Records that the kernel found the ring empty.
    /// 记录内核发现 buffer ring 已耗尽。
    pub(crate) fn note_exhausted(&mut self) {
        if let Some(group) = self.provided.as_mut() {
            group.note_exhausted();
        }
    }

    pub(crate) fn take_provided(&mut self) -> Option<ProvidedBufPort<'provided>> {
        self.provided.take()
    }
}

fn lease_error_report(error: ProvidedBufLeaseError, flags: u32) -> Report<UringError> {
    UringError::InvalidState
        .report(
            "uring.driver.cqe_env.provided_lease",
            "provided lease settlement failed",
        )
        .with_ctx("lease_error", format!("{error:?}"))
        .with_ctx("cqe_flags", flags)
}
