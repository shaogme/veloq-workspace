//! Field-level borrow splits for the uring submission path.
//!
//! A submission has to hold two things at once: the slot that owns the op/payload (a `&mut`
//! borrow into the operation registry) and the driver state needed to turn that op into an SQE
//! and hand it to the kernel. Handing the whole driver to the second half while
//! the first is live is an aliasing violation, so the non-`ops` fields are projected into the
//! views below and the borrow checker verifies the split.

use crate::{
    config::IoFd,
    diagnostics::UringCompletionDiagnostics,
    driver::lifecycle::{CancellationPhase, SubmissionPhase, UringOpState},
    driver::{
        control::{
            ControlPlaneEvent, ControlPlaneObserver, PendingCancel, StagedEntry, StagedLedger,
            UringControlEffectKind, UringControlPlane, UringPostCompletionEffects, UringTimerWheel,
        },
        registration::{
            buffer::{ChunkRegistrationRecord, ChunkRegistrationState, FixedBufferSubmitPort},
            file_table::{FileTable, OwnedFdOwnershipTicket},
            provided_buf::{
                ProvidedBufLease, ProvidedBufLeaseAction, ProvidedBufLeaseError,
                ProvidedBufLeasePhase, ProvidedBufPort,
            },
        },
    },
    error::{UringError, UringResult},
    op::CompletionCleanupHintFn,
};
use diagweave::prelude::*;
use tracing::trace;

#[cfg(feature = "test-hooks")]
use tracing::debug;
use veloq_buf::{BufferRegistrar, heap::ChunkId};
use veloq_driver_core::driver::{CancelTicket, CompletionToken, OpToken, RawCompletion};
use veloq_driver_core::slot::Generation;
use veloq_io_uring::{SubmissionQueue, Submitter, cqueue, squeue};
use veloq_std::{collections::HashMap, format};
use veloq_wheel::{TimerError, TimerId};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ChunkRegistrationDecision {
    Fixed,
    RawFallback,
}

/// What a `IOSQE_BUFFER_SELECT` submission needs to know about the provided-buffer ring.
///
/// A `Copy` snapshot rather than a borrow of the group: `make_sqe` only reads these two
/// numbers, and copying them leaves the group itself free for the completion path to mutate.
#[derive(Debug, Clone, Copy)]
pub(crate) struct ProvidedBufSqeInfo {
    bgid: u16,
    buf_size: u32,
}

impl ProvidedBufSqeInfo {
    #[inline]
    pub(crate) fn new(bgid: u16, buf_size: u32) -> Self {
        Self { bgid, buf_size }
    }
}

/// The driver state a `make_sqe` implementation is allowed to consult.
///
/// `make_sqe` runs with the slot's op and payload borrowed mutably out of
/// the operation registry, so it must not be able to reach that registry itself. These fields are
/// disjoint from it and are handed out by value or immutably: lazy chunk registration happens
/// *after* `make_sqe` returns, so nothing here needs to be mutated during SQE construction.
pub(crate) struct SqeEnv<'d> {
    file_table: &'d FileTable,
    chunk_records: &'d [Option<ChunkRegistrationRecord>],
    registrar: &'d (dyn BufferRegistrar + 'd),
    provided: Option<ProvidedBufSqeInfo>,
}

pub(crate) struct SubmissionLedgerParts<'d> {
    staged_entries: &'d mut StagedLedger,
    timers: &'d mut UringTimerWheel,
    control_observer: &'d mut ControlPlaneObserver,
    completion_cleanup_hints: &'d mut HashMap<CompletionToken, Option<CompletionCleanupHintFn>>,
}

impl<'d> SubmissionLedgerParts<'d> {
    pub(crate) fn new(
        staged_entries: &'d mut StagedLedger,
        timers: &'d mut UringTimerWheel,
        control_observer: &'d mut ControlPlaneObserver,
        completion_cleanup_hints: &'d mut HashMap<CompletionToken, Option<CompletionCleanupHintFn>>,
    ) -> Self {
        Self {
            staged_entries,
            timers,
            control_observer,
            completion_cleanup_hints,
        }
    }
}

#[cfg(not(feature = "test-hooks"))]
pub(crate) struct SubmissionSidecarParts<'d> {
    completion_cleanup_capacity: usize,
    pending_cancel_cqes: &'d mut HashMap<CancelTicket, PendingCancel>,
    cancel_capacity: usize,
}

#[cfg(not(feature = "test-hooks"))]
impl<'d> SubmissionSidecarParts<'d> {
    pub(crate) fn new(
        completion_cleanup_capacity: usize,
        pending_cancel_cqes: &'d mut HashMap<CancelTicket, PendingCancel>,
        cancel_capacity: usize,
    ) -> Self {
        Self {
            completion_cleanup_capacity,
            pending_cancel_cqes,
            cancel_capacity,
        }
    }
}

#[cfg(feature = "test-hooks")]
pub(crate) struct SubmissionSidecarParts<'d> {
    completion_cleanup_capacity: usize,
    pending_cancel_cqes: &'d mut HashMap<CancelTicket, PendingCancel>,
    cancel_capacity: usize,
    push_entry_failure: &'d mut bool,
}

#[cfg(feature = "test-hooks")]
impl<'d> SubmissionSidecarParts<'d> {
    pub(crate) fn new(
        completion_cleanup_capacity: usize,
        pending_cancel_cqes: &'d mut HashMap<CancelTicket, PendingCancel>,
        cancel_capacity: usize,
        push_entry_failure: &'d mut bool,
    ) -> Self {
        Self {
            completion_cleanup_capacity,
            pending_cancel_cqes,
            cancel_capacity,
            push_entry_failure,
        }
    }
}

pub(crate) struct SubmissionControlParts<'d> {
    ledger: SubmissionLedgerParts<'d>,
    sidecar: SubmissionSidecarParts<'d>,
}

impl<'d> SubmissionControlParts<'d> {
    pub(crate) fn new(
        ledger: SubmissionLedgerParts<'d>,
        sidecar: SubmissionSidecarParts<'d>,
    ) -> Self {
        Self { ledger, sidecar }
    }

    pub(crate) fn into_view(self, submission_queue: SubmissionQueue<'d>) -> SubmitControlView<'d> {
        let Self { ledger, sidecar } = self;
        let SubmissionLedgerParts {
            staged_entries,
            timers,
            control_observer,
            completion_cleanup_hints,
        } = ledger;
        #[cfg(not(feature = "test-hooks"))]
        let SubmissionSidecarParts {
            completion_cleanup_capacity,
            pending_cancel_cqes,
            cancel_capacity,
        } = sidecar;
        #[cfg(feature = "test-hooks")]
        let SubmissionSidecarParts {
            completion_cleanup_capacity,
            pending_cancel_cqes,
            cancel_capacity,
            push_entry_failure,
        } = sidecar;
        SubmitControlView {
            submission_queue,
            staged_entries,
            timers,
            control_observer,
            completion_cleanup_hints,
            completion_cleanup_capacity,
            pending_cancel_cqes,
            cancel_capacity,
            #[cfg(feature = "test-hooks")]
            push_entry_failure,
        }
    }
}
pub(crate) struct CompletionLedgerParts<'d> {
    pending_cancel_cqes: &'d mut HashMap<CancelTicket, PendingCancel>,
    completion_cleanup_hints: &'d mut HashMap<CompletionToken, Option<CompletionCleanupHintFn>>,
    observer: &'d mut ControlPlaneObserver,
    post: &'d mut UringPostCompletionEffects,
}

impl<'d> CompletionLedgerParts<'d> {
    pub(crate) fn new(
        pending_cancel_cqes: &'d mut HashMap<CancelTicket, PendingCancel>,
        completion_cleanup_hints: &'d mut HashMap<CompletionToken, Option<CompletionCleanupHintFn>>,
        observer: &'d mut ControlPlaneObserver,
        post: &'d mut UringPostCompletionEffects,
    ) -> Self {
        Self {
            pending_cancel_cqes,
            completion_cleanup_hints,
            observer,
            post,
        }
    }
}

pub(crate) struct CompletionMetadata {
    waker_buf_len: usize,
    waker_generation: u64,
}

impl CompletionMetadata {
    pub(crate) const fn new(waker_buf_len: usize, waker_generation: u64) -> Self {
        Self {
            waker_buf_len,
            waker_generation,
        }
    }
}

pub(crate) struct CompletionControlParts<'d> {
    ledger: CompletionLedgerParts<'d>,
    metadata: CompletionMetadata,
}

impl<'d> CompletionControlParts<'d> {
    pub(crate) fn new(ledger: CompletionLedgerParts<'d>, metadata: CompletionMetadata) -> Self {
        Self { ledger, metadata }
    }

    pub(crate) fn into_view(self) -> CompletionControlView<'d> {
        let Self { ledger, metadata } = self;
        let CompletionLedgerParts {
            pending_cancel_cqes,
            completion_cleanup_hints,
            observer,
            post,
        } = ledger;
        CompletionControlView::new(
            pending_cancel_cqes,
            completion_cleanup_hints,
            metadata.waker_buf_len,
            metadata.waker_generation,
            observer,
            post,
        )
    }
}

impl SqeEnv<'_> {
    pub(crate) fn from_parts<'d>(
        file_table: &'d FileTable,
        chunk_records: &'d [Option<ChunkRegistrationRecord>],
        registrar: &'d (dyn BufferRegistrar + 'd),
        provided: Option<ProvidedBufSqeInfo>,
    ) -> SqeEnv<'d> {
        SqeEnv {
            file_table,
            chunk_records,
            registrar,
            provided,
        }
    }

    #[inline]
    pub(crate) fn file_table(&self) -> &FileTable {
        self.file_table
    }

    /// The buffer group an `IOSQE_BUFFER_SELECT` submission should draw from, together with the
    /// size every buffer in it has.
    #[inline]
    pub(crate) fn provided_buf_info(&self, scope: &'static str) -> UringResult<(u16, u32)> {
        let info = self.provided.ok_or_else(|| {
            UringError::Unsupported
                .to_report()
                .push_ctx("scope", scope)
                .attach_note("driver has no provided buffer ring registered")
        })?;
        Ok((info.bgid, info.buf_size))
    }

    /// Whether `chunk` is already in the kernel's fixed-buffer table.
    ///
    /// An out-of-range id answers `false`, which simply selects the non-fixed opcode — the
    /// same outcome as a chunk that has not been registered yet.
    #[inline]
    pub(crate) fn is_chunk_registered(&self, chunk: ChunkId) -> bool {
        let Some(record) = self
            .chunk_records
            .get(chunk.as_usize())
            .and_then(Option::as_ref)
        else {
            return false;
        };
        let Some(info) = self.registrar.resolve_chunk_info(chunk) else {
            return false;
        };
        record.state() == ChunkRegistrationState::Registered
            && info.id == chunk
            && record.matches(chunk, info.ptr.as_ptr(), info.len.get())
    }
}

#[cfg(test)]
impl<'d> SqeEnv<'d> {
    /// Creates the smallest submission environment needed by projection tests.
    pub(crate) fn for_test(
        file_table: &'d FileTable,
        registrar: &'d (dyn BufferRegistrar + 'd),
    ) -> Self {
        Self {
            file_table,
            chunk_records: &[],
            registrar,
            provided: None,
        }
    }

    pub(crate) fn for_test_with_provided(
        file_table: &'d FileTable,
        registrar: &'d (dyn BufferRegistrar + 'd),
        bgid: u16,
        buf_size: u32,
    ) -> Self {
        Self {
            file_table,
            chunk_records: &[],
            registrar,
            provided: Some(ProvidedBufSqeInfo::new(bgid, buf_size)),
        }
    }
}

/// The driver state a completion is allowed to reach.
///
/// Mirrors [`SqeEnv`] on the other end of an operation. The completion path holds a slot
/// borrowed out of the operation registry while it builds the record, so it cannot be handed the
/// whole driver either — but unlike submission it does need to *mutate* something: a CQE that
/// carries a buffer id has consumed a ring entry, and that entry has to be settled before the
/// record leaves this function.
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

/// The completion-side control projection.
///
/// It contains only bookkeeping needed to acknowledge control completions, settle cleanup
/// hints, identify the current waker generation, and append deferred control effects. Provided
/// buffers remain in the separate [`CqeEnv`] projection so completion hooks cannot reach
/// registration state.
pub(crate) struct CompletionControlView<'d> {
    pending_cancel_cqes: &'d mut HashMap<CancelTicket, PendingCancel>,
    completion_cleanup_hints: &'d mut HashMap<CompletionToken, Option<CompletionCleanupHintFn>>,
    waker_buf_len: usize,
    waker_generation: u64,
    observer: &'d mut ControlPlaneObserver,
    post: &'d mut UringPostCompletionEffects,
}

impl<'d> CompletionControlView<'d> {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        pending_cancel_cqes: &'d mut HashMap<CancelTicket, PendingCancel>,
        completion_cleanup_hints: &'d mut HashMap<CompletionToken, Option<CompletionCleanupHintFn>>,
        waker_buf_len: usize,
        waker_generation: u64,
        observer: &'d mut ControlPlaneObserver,
        post: &'d mut UringPostCompletionEffects,
    ) -> Self {
        Self {
            pending_cancel_cqes,
            completion_cleanup_hints,
            waker_buf_len,
            waker_generation,
            observer,
            post,
        }
    }

    #[inline]
    pub(crate) fn peek_completion_cleanup_hint(
        &mut self,
        token: CompletionToken,
    ) -> Option<Option<CompletionCleanupHintFn>> {
        self.completion_cleanup_hints.get(&token).copied()
    }

    #[inline]
    pub(crate) fn remove_completion_cleanup_hint(
        &mut self,
        token: CompletionToken,
        flags: u32,
    ) -> Option<Option<CompletionCleanupHintFn>> {
        let entry = if cqueue::more(flags) {
            self.completion_cleanup_hints.get(&token).copied()
        } else {
            self.completion_cleanup_hints.remove(&token)
        };
        if entry.is_some() {
            self.observer
                .record(ControlPlaneEvent::CleanupHintRemove(token));
        }
        entry
    }

    #[inline]
    pub(crate) fn take_pending_cancel(&mut self, ticket: CancelTicket) -> Option<PendingCancel> {
        let request = self.pending_cancel_cqes.remove(&ticket)?;
        self.observer
            .record(ControlPlaneEvent::CancelInFlightRemove {
                ticket,
                target: request.target(),
            });
        Some(request)
    }

    #[inline]
    pub(crate) fn waker_buf_len(&self) -> usize {
        self.waker_buf_len
    }

    #[inline]
    pub(crate) fn waker_generation(&self) -> u64 {
        self.waker_generation
    }

    #[inline]
    pub(crate) fn append_waker_effect(&mut self, generation: u64, rebuild: bool) {
        if rebuild {
            self.append_effect(
                None,
                None,
                UringControlEffectKind::WakerRebuild { generation },
            );
        }
        self.append_effect(
            None,
            None,
            UringControlEffectKind::WakerRearm { generation },
        );
        self.append_effect(None, None, UringControlEffectKind::BacklogKick);
    }

    #[inline]
    pub(crate) fn append_cancel_enoent(
        &mut self,
        cancel_ticket: CancelTicket,
        request: PendingCancel,
        raw: RawCompletion,
    ) {
        self.append_effect(
            Some(request.target()),
            Some(request.target().generation()),
            UringControlEffectKind::CancelReconcile {
                cancel_ticket,
                request,
                raw,
            },
        );
    }

    #[inline]
    pub(crate) fn append_cancel_phase_update(
        &mut self,
        cancel_ticket: CancelTicket,
        target: OpToken,
        phase: CancellationPhase,
    ) {
        self.append_effect(
            Some(target),
            Some(target.generation()),
            UringControlEffectKind::CancelAck {
                cancel_ticket,
                phase,
            },
        );
    }

    #[inline]
    pub(crate) fn append_close_unregister(&mut self, token: OpToken, fd: IoFd) {
        self.append_effect(
            Some(token),
            Some(token.generation()),
            UringControlEffectKind::CloseUnregister {
                ticket: OwnedFdOwnershipTicket::new(token, fd),
            },
        );
    }

    #[inline]
    pub(crate) fn append_udp_rearm(&mut self, token: OpToken, generation: u32) {
        self.append_effect(
            Some(token),
            Some(token.generation()),
            UringControlEffectKind::UdpRearm {
                logical_receiver_generation: generation,
            },
        );
    }

    #[inline]
    fn append_effect(
        &mut self,
        token: Option<OpToken>,
        generation: Option<Generation>,
        kind: UringControlEffectKind,
    ) {
        self.post.append(token, generation, kind);
    }
}

/// The control-plane fields a submission may mutate while an op slot is borrowed.
pub(crate) struct SubmitControlView<'d> {
    submission_queue: SubmissionQueue<'d>,
    staged_entries: &'d mut StagedLedger,
    timers: &'d mut UringTimerWheel,
    control_observer: &'d mut ControlPlaneObserver,
    completion_cleanup_hints: &'d mut HashMap<CompletionToken, Option<CompletionCleanupHintFn>>,
    completion_cleanup_capacity: usize,
    pending_cancel_cqes: &'d mut HashMap<CancelTicket, PendingCancel>,
    cancel_capacity: usize,
    #[cfg(feature = "test-hooks")]
    push_entry_failure: &'d mut bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StageResult {
    Staged,
    Full,
}

/// A rollback-safe transaction for an SQE and every control-plane record that describes it.
///
/// Metadata is reserved before the SQ push so a successful push cannot be left without a
/// sidecar.  If the SQ is full, the reserved records are removed before returning `Full`; the
/// caller may then put the operation into the fixed-capacity backlog.
pub(crate) struct StageTxn<'view, 'd> {
    control: &'view mut SubmitControlView<'d>,
    entry: StagedEntry,
    cleanup_hint: Option<(CompletionToken, Option<CompletionCleanupHintFn>)>,
    cancel_request: Option<PendingCancel>,
    metadata_committed: bool,
    finished: bool,
}

impl<'view, 'd> StageTxn<'view, 'd> {
    pub(crate) fn new(
        control: &'view mut SubmitControlView<'d>,
        entry: StagedEntry,
        cleanup_hint: Option<(CompletionToken, Option<CompletionCleanupHintFn>)>,
        cancel_request: Option<PendingCancel>,
    ) -> UringResult<Self> {
        control.validate_stage(entry, cleanup_hint, cancel_request)?;
        Ok(Self {
            control,
            entry,
            cleanup_hint,
            cancel_request,
            metadata_committed: false,
            finished: false,
        })
    }

    pub(crate) fn commit(mut self, sqe: squeue::Entry) -> StageResult {
        self.commit_metadata();
        let result = self.push_entry(sqe);
        if result == StageResult::Full {
            self.rollback_metadata();
        } else {
            self.publish_metadata_events();
            self.metadata_committed = false;
        }
        self.finished = true;
        result
    }

    fn commit_metadata(&mut self) {
        self.control.staged_entries.push_validated(self.entry);
        self.metadata_committed = true;
        match self.entry {
            StagedEntry::User(_) => {
                if let Some((token, hint)) = self.cleanup_hint {
                    let previous = self.control.completion_cleanup_hints.insert(token, hint);
                    debug_assert!(
                        previous.is_none(),
                        "completion cleanup hint was registered twice for one token"
                    );
                }
            }
            StagedEntry::Cancel { ticket, target } => {
                let request = self.cancel_request.expect("cancel stage requires request");
                debug_assert_eq!(request.target(), target);
                let previous = self.control.pending_cancel_cqes.insert(ticket, request);
                debug_assert!(
                    previous.is_none(),
                    "cancel sidecar was registered twice for one ticket"
                );
            }
            StagedEntry::Waker => {}
        }
    }

    fn publish_metadata_events(&mut self) {
        match self.entry {
            StagedEntry::User(_) => {
                if let Some((token, _)) = self.cleanup_hint {
                    self.control
                        .control_observer
                        .record(ControlPlaneEvent::CleanupHintInsert(token));
                }
            }
            StagedEntry::Cancel { ticket, target } => {
                self.control
                    .control_observer
                    .record(ControlPlaneEvent::CancelInFlightInsert { ticket, target });
            }
            StagedEntry::Waker => {}
        }
    }

    fn rollback_metadata(&mut self) {
        let removed = self.control.staged_entries.pop_back();
        debug_assert_eq!(removed, Some(self.entry));
        match self.entry {
            StagedEntry::User(_) => {
                if let Some((token, _)) = self.cleanup_hint {
                    let _ = self.control.completion_cleanup_hints.remove(&token);
                }
            }
            StagedEntry::Cancel { ticket, .. } => {
                let _ = self.control.pending_cancel_cqes.remove(&ticket);
            }
            StagedEntry::Waker => {}
        }
        self.metadata_committed = false;
    }

    fn push_entry(&mut self, entry: squeue::Entry) -> StageResult {
        trace!("Pushing SQE user_data={}", entry.get_user_data());
        #[cfg(feature = "test-hooks")]
        if *self.control.push_entry_failure {
            *self.control.push_entry_failure = false;
            debug!("injected SQ push failure");
            return StageResult::Full;
        }

        if unsafe { self.control.submission_queue.push(&entry) }.is_ok() {
            StageResult::Staged
        } else {
            StageResult::Full
        }
    }
}

impl Drop for StageTxn<'_, '_> {
    fn drop(&mut self) {
        let metadata_was_committed = self.metadata_committed;
        if metadata_was_committed {
            self.rollback_metadata();
        }
        debug_assert!(
            self.finished || !metadata_was_committed,
            "stage transaction dropped before its metadata was settled"
        );
    }
}

/// The resource-registration fields a submission may consult or update.
pub(crate) struct SubmitResourceView<'d, 'r> {
    submitter: Submitter<'d>,
    file_table: &'d FileTable,
    registration_diagnostics: &'d UringCompletionDiagnostics,
    buffers: FixedBufferSubmitPort<'d, 'r>,
}

/// The two narrow submission projections kept together for the transaction API.
pub(crate) struct SubmitEnv<'d, 'r> {
    control: SubmitControlView<'d>,
    resources: SubmitResourceView<'d, 'r>,
}

impl<'d, 'r> SubmitResourceView<'d, 'r> {
    pub(crate) fn from_registration_parts(
        submitter: Submitter<'d>,
        file_table: &'d FileTable,
        registration_diagnostics: &'d UringCompletionDiagnostics,
        buffers: FixedBufferSubmitPort<'d, 'r>,
    ) -> Self {
        Self {
            submitter,
            file_table,
            registration_diagnostics,
            buffers,
        }
    }

    #[inline]
    pub(crate) fn sqe_env(&self) -> SqeEnv<'_> {
        self.buffers.sqe_env(self.file_table)
    }

    #[inline]
    pub(crate) fn ensure_chunk_registered(
        &mut self,
        chunk_id: ChunkId,
        user_data: usize,
        scope: &'static str,
    ) -> UringResult<ChunkRegistrationDecision> {
        self.buffers.ensure_chunk_registered(
            &self.submitter,
            self.registration_diagnostics,
            chunk_id,
            user_data,
            scope,
        )
    }
}

pub(crate) struct SubmitParts<'d, 'r> {
    control: SubmitControlView<'d>,
    resources: SubmitResourceView<'d, 'r>,
}

impl<'d, 'r> SubmitParts<'d, 'r> {
    pub(crate) fn new(
        control: SubmitControlView<'d>,
        resources: SubmitResourceView<'d, 'r>,
    ) -> Self {
        Self { control, resources }
    }
}

pub(crate) struct SubmitEnvironmentFactory;

impl SubmitEnvironmentFactory {
    pub(crate) fn resource_view<'d, 'r>(
        submitter: Submitter<'d>,
        file_table: &'d FileTable,
        diagnostics: &'d UringCompletionDiagnostics,
        buffers: FixedBufferSubmitPort<'d, 'r>,
    ) -> SubmitResourceView<'d, 'r> {
        SubmitResourceView::from_registration_parts(submitter, file_table, diagnostics, buffers)
    }

    pub(crate) fn build<'d, 'r>(parts: SubmitParts<'d, 'r>) -> SubmitEnv<'d, 'r> {
        SubmitEnv {
            control: parts.control,
            resources: parts.resources,
        }
    }
}

impl SubmitControlView<'_> {
    #[inline]
    fn validate_stage(
        &mut self,
        entry: StagedEntry,
        cleanup_hint: Option<(CompletionToken, Option<CompletionCleanupHintFn>)>,
        cancel_request: Option<PendingCancel>,
    ) -> UringResult<()> {
        self.staged_entries.validate(entry).map_err(|error| {
            UringError::InvalidState
                .report("uring.stage_txn.validate", format!("{error:?}"))
                .attach_note("staged ledger rejected an SQE before it reached the SQ")
        })?;

        if let Some((token, _)) = cleanup_hint
            && self.completion_cleanup_hints.contains_key(&token)
        {
            return Err(UringError::InvalidState
                .report(
                    "uring.stage_txn.cleanup_hint",
                    "completion cleanup hint already exists for the token",
                )
                .with_ctx("completion_token", token.raw())
                .attach_note("staging would overwrite cleanup ownership"));
        }

        if cleanup_hint.is_some()
            && self.completion_cleanup_hints.len() >= self.completion_cleanup_capacity
        {
            return Err(UringError::InvalidState
                .report(
                    "uring.stage_txn.cleanup_hint",
                    "completion cleanup hint ledger is full",
                )
                .with_ctx("capacity", self.completion_cleanup_capacity)
                .attach_note("staging would require an unbounded stale-CQE sidecar"));
        }

        if let StagedEntry::Cancel { ticket, target } = entry {
            let request = cancel_request.ok_or_else(|| {
                UringError::InvalidState.report(
                    "uring.stage_txn.cancel",
                    "cancel SQE is missing its target request",
                )
            })?;
            if request.target() != target {
                return Err(UringError::InvalidState
                    .report("uring.stage_txn.cancel", "cancel request target mismatch")
                    .with_ctx("entry_target", target.index())
                    .with_ctx("request_target", request.target().index()));
            }
            if self.pending_cancel_cqes.len() >= self.cancel_capacity
                && !self.pending_cancel_cqes.contains_key(&ticket)
            {
                return Err(UringError::InvalidState
                    .report(
                        "uring.stage_txn.cancel",
                        "cancel sidecar capacity exhausted",
                    )
                    .with_ctx("capacity", self.cancel_capacity));
            }
            if self.pending_cancel_cqes.contains_key(&ticket) {
                return Err(UringError::InvalidState
                    .report(
                        "uring.stage_txn.cancel",
                        "cancel ticket already has a sidecar",
                    )
                    .with_ctx("cancel_ticket", ticket.raw()));
            }
        }
        Ok(())
    }

    #[inline]
    pub(crate) fn transition_submission_state(
        &mut self,
        token: OpToken,
        state: &mut UringOpState,
        next: SubmissionPhase,
        reason: &'static str,
    ) {
        state.transition_submission_phase(token, next, reason, self.control_observer);
    }

    #[inline]
    pub(crate) fn cancel_timer(&mut self, token: OpToken, task_id: TimerId) {
        self.timers.cancel(task_id);
        self.control_observer
            .record(ControlPlaneEvent::TimerCancel { task_id, token });
    }

    #[inline]
    pub(crate) fn record_timer_insert(&mut self, token: OpToken, task_id: TimerId) {
        self.control_observer
            .record(ControlPlaneEvent::TimerInsert { task_id, token });
    }

    #[inline]
    pub(crate) fn insert_timer(
        &mut self,
        token: OpToken,
        duration: veloq_std::time::Duration,
    ) -> UringResult<TimerId> {
        self.timers
            .insert(token, duration)
            .map_err(|error: TimerError| {
                UringError::InvalidInput
                    .report(
                        "uring.timer.insert",
                        "timer duration is outside the wheel range",
                    )
                    .with_ctx("timer_error", format!("{error:?}"))
            })
    }

    pub(crate) fn stage_user_entry(
        &mut self,
        token: OpToken,
        entry: squeue::Entry,
        cleanup_hint: Option<CompletionCleanupHintFn>,
    ) -> UringResult<StageResult> {
        StageTxn::new(
            self,
            StagedEntry::User(token),
            Some((CompletionToken::user(token), cleanup_hint)),
            None,
        )
        .map(|txn| txn.commit(entry))
    }

    pub(crate) fn stage_cancel_entry(
        &mut self,
        ticket: CancelTicket,
        request: PendingCancel,
        entry: squeue::Entry,
    ) -> UringResult<StageResult> {
        StageTxn::new(
            self,
            StagedEntry::Cancel {
                ticket,
                target: request.target(),
            },
            None,
            Some(request),
        )
        .map(|txn| txn.commit(entry))
    }

    pub(crate) fn stage_waker_entry(&mut self, entry: squeue::Entry) -> UringResult<StageResult> {
        StageTxn::new(self, StagedEntry::Waker, None, None).map(|txn| txn.commit(entry))
    }
}

impl SubmitEnv<'_, '_> {
    #[inline]
    pub(crate) fn transition_submission_state(
        &mut self,
        token: OpToken,
        state: &mut UringOpState,
        next: SubmissionPhase,
        reason: &'static str,
    ) {
        self.control
            .transition_submission_state(token, state, next, reason);
    }

    #[inline]
    pub(crate) fn cancel_timer(&mut self, token: OpToken, task_id: TimerId) {
        self.control.cancel_timer(token, task_id);
    }

    #[inline]
    pub(crate) fn record_timer_insert(&mut self, token: OpToken, task_id: TimerId) {
        self.control.record_timer_insert(token, task_id);
    }

    #[inline]
    pub(crate) fn insert_timer(
        &mut self,
        token: OpToken,
        duration: veloq_std::time::Duration,
    ) -> UringResult<TimerId> {
        self.control.insert_timer(token, duration)
    }

    #[inline]
    pub(crate) fn sqe_env(&self) -> SqeEnv<'_> {
        self.resources.sqe_env()
    }

    #[inline]
    pub(crate) fn stage_user_entry(
        &mut self,
        token: OpToken,
        entry: squeue::Entry,
        cleanup_hint: Option<CompletionCleanupHintFn>,
    ) -> UringResult<StageResult> {
        self.control.stage_user_entry(token, entry, cleanup_hint)
    }

    #[inline]
    pub(crate) fn stage_cancel_entry(
        &mut self,
        ticket: CancelTicket,
        request: PendingCancel,
        entry: squeue::Entry,
    ) -> UringResult<StageResult> {
        self.control.stage_cancel_entry(ticket, request, entry)
    }

    #[inline]
    pub(crate) fn stage_waker_entry(&mut self, entry: squeue::Entry) -> UringResult<StageResult> {
        self.control.stage_waker_entry(entry)
    }

    #[inline]
    pub(crate) fn ensure_chunk_registered(
        &mut self,
        chunk_id: ChunkId,
        user_data: usize,
        scope: &'static str,
    ) -> UringResult<ChunkRegistrationDecision> {
        self.resources
            .ensure_chunk_registered(chunk_id, user_data, scope)
    }
}

impl UringControlPlane {
    /// Projects only submission-owned control state.
    pub(crate) fn with_submit_view<'d>(
        &'d mut self,
        submission_queue: SubmissionQueue<'d>,
    ) -> SubmitControlView<'d> {
        self.submission_parts().into_view(submission_queue)
    }

    /// Projects only completion-owned control state.
    pub(crate) fn with_completion_view(&mut self) -> CompletionControlView<'_> {
        self.completion_parts().into_view()
    }
}
