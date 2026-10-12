use veloq_std::boxed::Box;

use diagweave::prelude::*;
use veloq_buf::FixedBuf;

use crate::{driver::registration::ProvidedBufGroup, error::UringError};

use super::publication::ProvidedBufBidState;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ProvidedBufLeasePhase {
    Selected,
    Pending,
    Delivery,
    Settled,
    Quarantined,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ProvidedBufLeaseError {
    GroupMismatch,
    BidMismatch,
    PublishSequenceMismatch,
    InvalidState,
    AlreadySettled,
    NoReplacementBuffer,
    PublicationFailed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ProvidedBufLeaseAction {
    Republish,
    Quarantine,
}

/// Completion-side command port for the provided-buffer owner.
/// 完成处理侧的 provided-buffer 命令端口。
pub(crate) struct ProvidedBufPort<'a> {
    group: &'a mut ProvidedBufGroup,
}

impl<'a> ProvidedBufPort<'a> {
    pub(crate) fn new(group: &'a mut ProvidedBufGroup) -> Self {
        Self { group }
    }

    #[inline]
    pub(crate) fn claim_selected_lease(
        &mut self,
        flags: u32,
        res: i32,
    ) -> Result<Option<ProvidedBufLease>, ProvidedBufLeaseError> {
        self.group.claim_selected_lease(flags, res)
    }

    #[inline]
    pub(crate) fn handoff_selected_lease(
        &mut self,
        lease: &mut ProvidedBufLease,
    ) -> Result<(), ProvidedBufLeaseError> {
        self.group.handoff_selected_lease(lease)
    }

    #[inline]
    pub(crate) fn begin_delivery(
        &mut self,
        lease: &mut ProvidedBufLease,
    ) -> Result<(), ProvidedBufLeaseError> {
        self.group.begin_delivery(lease)
    }

    #[inline]
    pub(crate) fn settle_selected_lease(
        &mut self,
        lease: &mut ProvidedBufLease,
        action: ProvidedBufLeaseAction,
    ) -> Result<(), ProvidedBufLeaseError> {
        self.group.settle_selected_lease(lease, action)
    }

    #[inline]
    pub(crate) fn discard_selected(&mut self, flags: u32) -> bool {
        self.group.discard_selected(flags)
    }

    #[inline]
    pub(crate) fn quarantine_claimed(&mut self, bid: u16, publish_seq: u64) -> bool {
        self.group.quarantine_claimed(bid, publish_seq)
    }

    #[inline]
    pub(crate) fn note_exhausted(&mut self) {
        self.group.note_exhausted();
    }
}

/// The exclusive owner of a CQE-selected provided buffer.
///
/// The lease deliberately owns the [`FixedBuf`] instead of retaining only a bid. A bid is not
/// enough to reconstruct a datagram after the kernel has removed it from the ring.
/// lease 独占持有 CQE 选中的 `FixedBuf`；仅记录 bid 无法在内核将其移出 ring 后重建数据报。
pub(crate) struct ProvidedBufLease {
    group_id: u16,
    bid: u16,
    publish_seq: u64,
    buf: Option<FixedBuf>,
    len: usize,
    phase: ProvidedBufLeasePhase,
    user_buffer_taken: bool,
}

/// Ownership carrier for a failed unregister attempt.
/// 反注册失败时同时承载错误和 group 所有权。
pub(crate) struct ProvidedBufUnregisterFailure {
    report: Report<UringError>,
    group: ProvidedBufGroup,
}

impl ProvidedBufUnregisterFailure {
    pub(super) fn new(report: Report<UringError>, group: ProvidedBufGroup) -> Self {
        Self { report, group }
    }

    pub(crate) fn into_parts(self) -> (Report<UringError>, ProvidedBufGroup) {
        (self.report, self.group)
    }
}

pub(crate) type ProvidedBufUnregisterResult = Result<(), Box<ProvidedBufUnregisterFailure>>;

/// Carries optional group ownership when initialization fails after registration.
/// 初始化失败时承载错误，并在必要时保留 group 所有权。
pub(crate) struct ProvidedBufInitializationFailure {
    report: Report<UringError>,
    group: Option<ProvidedBufGroup>,
}

impl ProvidedBufInitializationFailure {
    pub(super) fn new(report: Report<UringError>, group: Option<ProvidedBufGroup>) -> Self {
        Self { report, group }
    }

    pub(crate) fn into_parts(self) -> (Report<UringError>, Option<ProvidedBufGroup>) {
        (self.report, self.group)
    }
}

pub(crate) type ProvidedBufInitializationResult =
    Result<ProvidedBufGroup, Box<ProvidedBufInitializationFailure>>;

impl ProvidedBufLease {
    pub(super) fn new(
        group_id: u16,
        bid: u16,
        publish_seq: u64,
        buf: FixedBuf,
        len: usize,
    ) -> Self {
        Self {
            group_id,
            bid,
            publish_seq,
            buf: Some(buf),
            len,
            phase: ProvidedBufLeasePhase::Selected,
            user_buffer_taken: false,
        }
    }

    pub(super) fn take_owned_buffer(&mut self) -> Option<FixedBuf> {
        self.buf.take()
    }

    pub(super) const fn user_buffer_taken(&self) -> bool {
        self.user_buffer_taken
    }

    pub(super) fn set_phase(&mut self, phase: ProvidedBufLeasePhase) {
        self.phase = phase;
    }

    pub(super) fn validate_with(
        &self,
        group: &ProvidedBufGroup,
    ) -> Result<(), ProvidedBufLeaseError> {
        if self.group_id != group.group_id() {
            return Err(ProvidedBufLeaseError::GroupMismatch);
        }
        let Some(state) = group.bid_state(self.bid) else {
            return Err(ProvidedBufLeaseError::BidMismatch);
        };
        let expected = match self.phase {
            ProvidedBufLeasePhase::Selected => ProvidedBufBidState::SelectedLease {
                publish_seq: self.publish_seq,
            },
            ProvidedBufLeasePhase::Pending => ProvidedBufBidState::PendingLease {
                publish_seq: self.publish_seq,
            },
            ProvidedBufLeasePhase::Delivery => ProvidedBufBidState::DeliveryLease {
                publish_seq: self.publish_seq,
            },
            ProvidedBufLeasePhase::Settled | ProvidedBufLeasePhase::Quarantined => {
                return Err(ProvidedBufLeaseError::AlreadySettled);
            }
        };
        if state != expected {
            return Err(ProvidedBufLeaseError::PublishSequenceMismatch);
        }
        Ok(())
    }

    pub(crate) fn bid(&self) -> u16 {
        self.bid
    }

    pub(crate) fn publish_seq(&self) -> u64 {
        self.publish_seq
    }

    pub(crate) fn len(&self) -> usize {
        self.len
    }

    pub(crate) fn phase(&self) -> ProvidedBufLeasePhase {
        self.phase
    }

    /// Borrow the bytes selected by the CQE without transferring buffer ownership. 借用 CQE 选中的字节，但不转移 buffer 所有权。
    pub(crate) fn as_slice(&self) -> Option<&[u8]> {
        self.buf.as_ref().map(FixedBuf::as_slice)
    }

    /// Move the selected buffer to a TCP record. UDP copy mode must not call this method. 将选中 buffer 移交给 TCP record；UDP 复制模式不得调用此方法。
    pub(crate) fn take_buffer_for_delivery(&mut self) -> Option<FixedBuf> {
        let buf = self.buf.take()?;
        self.user_buffer_taken = true;
        Some(buf)
    }
}
