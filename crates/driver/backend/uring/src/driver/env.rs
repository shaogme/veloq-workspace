//! Field-level borrow splits for the uring submission path.
//!
//! A submission has to hold two things at once: the slot that owns the op/payload (a `&mut`
//! borrow into [`UringDriver::ops`]) and the driver state needed to turn that op into an SQE
//! and hand it to the kernel. Handing the whole `&mut UringDriver` to the second half while
//! the first is live is an aliasing violation, so the non-`ops` fields are projected into the
//! views below and the borrow checker verifies the split.

use crate::{
    config::{BufferRegistrationMode, IoFd},
    diagnostics::UringCompletionDiagnostics,
    driver::lifecycle::{CancellationPhase, SubmissionPhase},
    driver::{
        FileTable, MAX_CHUNKS, PendingCancel, ProvidedBufGroup, UringDriver,
        UringRegistrationStats,
        control::{
            ControlPlaneEvent, ControlPlaneObserver, StagedEntry, StagedLedger,
            UringControlEffectKind, UringControlPlane, UringPostCompletionEffects, UringTimerWheel,
            transition_submission_phase,
        },
        registration::{
            BufferRegistrationQuarantine, ChunkRegistrationRecord, ChunkRegistrationState,
            KernelUpdateOutcome, REGISTER_FAILURE_RETRY_COOLDOWN, UpdateEvidence,
        },
    },
    error::{UringError, UringResult},
    op::{CompletionCleanupHintFn, UringOpRegistry},
};
use diagweave::prelude::*;
use tracing::{debug, trace};
use veloq_buf::{BufferRegistrar, FixedBuf, heap::ChunkId};
use veloq_driver_core::driver::{
    BufferRegistrationStatus, CancelTicket, CompletionToken, OpToken, RawCompletion,
};
use veloq_driver_core::slot::Generation;
use veloq_io_uring::{SubmissionQueue, Submitter, cqueue, squeue};
use veloq_std::{
    collections::{BitSet, HashMap},
    format, ptr,
};
use veloq_wheel::{TimerError, TimerId};

#[cfg(feature = "test-hooks")]
use crate::driver::registration::buffer::BufferUpdateInjection;

#[cfg(feature = "test-hooks")]
use veloq_std::collections::VecDeque;

#[cfg(feature = "test-hooks")]
use veloq_std::time::Instant;

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
/// [`UringDriver::ops`], so it must not be able to reach `ops` itself. These fields are
/// disjoint from it and are handed out by value or immutably: lazy chunk registration happens
/// *after* `make_sqe` returns, so nothing here needs to be mutated during SQE construction.
pub(crate) struct SqeEnv<'d> {
    pub(crate) file_table: &'d FileTable,
    chunk_records: &'d [Option<ChunkRegistrationRecord>],
    registrar: &'d (dyn BufferRegistrar + 'd),
    provided: Option<ProvidedBufSqeInfo>,
}

impl SqeEnv<'_> {
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
        record.state == ChunkRegistrationState::Registered
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
/// borrowed out of [`UringDriver::ops`] while it builds the record, so it cannot be handed the
/// whole driver either — but unlike submission it does need to *mutate* something: a CQE that
/// carries a buffer id has consumed a ring entry, and that entry has to be settled before the
/// record leaves this function.
pub(crate) struct CqeEnv<'d> {
    provided: Option<&'d mut ProvidedBufGroup>,
    diagnostics: &'d UringCompletionDiagnostics,
    selected_buffer_settled: bool,
}

impl<'d> CqeEnv<'d> {
    #[inline]
    pub(crate) fn new(
        provided: Option<&'d mut ProvidedBufGroup>,
        diagnostics: &'d UringCompletionDiagnostics,
    ) -> Self {
        Self {
            provided,
            diagnostics,
            selected_buffer_settled: false,
        }
    }

    /// Takes the buffer this CQE selected and refills its slot in the ring.
    ///
    /// `Ok(None)` means the completion selected no buffer at all — `-ENOBUFS`, or an operation
    /// that never asked for one. A CQE that *does* carry a buffer id while this driver has no
    /// ring registered is an error rather than a `None`: the kernel cannot have picked from a
    /// group that does not exist, so silently dropping it would hide a real bug.
    pub(crate) fn take_provided_buf(
        &mut self,
        flags: u32,
        res: i32,
    ) -> UringResult<Option<FixedBuf>> {
        let selected = cqueue::buffer_select(flags).is_some();
        match self.provided.as_deref_mut() {
            Some(group) => {
                let buffer = group.take_selected(flags, res);
                if selected {
                    if buffer.is_some() {
                        self.selected_buffer_settled = true;
                    } else {
                        self.diagnostics.inc_provided_unknown_bid();
                    }
                }
                Ok(buffer)
            }
            None if selected => UringError::InvalidState
                .push_ctx("scope", "uring.driver.cqe_env.take_provided_buf")
                .with_ctx("cqe_flags", flags)
                .attach_note("completion selected a provided buffer but no ring is registered"),
            None => Ok(None),
        }
    }

    /// Hands the buffer this CQE selected straight back to the ring.
    ///
    /// For completions that are being discarded (cancelled, orphaned, stale token). Skipping it
    /// leaks one buffer id per discarded completion — "cancellation is not termination" in its
    /// provided-buffer form.
    pub(crate) fn return_provided_buf(&mut self, flags: u32) {
        if self.selected_buffer_settled || cqueue::buffer_select(flags).is_none() {
            return;
        }
        if let Some(group) = self.provided.as_deref_mut() {
            if group.return_selected(flags) {
                self.diagnostics.inc_provided_unknown_bid();
            }
            self.selected_buffer_settled = true;
        }
    }

    /// Records that the kernel found the ring empty.
    pub(crate) fn note_exhausted(&mut self) {
        if let Some(group) = self.provided.as_deref_mut() {
            group.note_exhausted();
        }
    }
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
                target: request.target,
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
            Some(request.target),
            Some(request.target.generation()),
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
            UringControlEffectKind::CloseUnregister { fd },
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
                debug_assert_eq!(request.target, target);
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
    registered_chunks_cache: &'d mut BitSet,
    chunk_records: &'d mut [Option<ChunkRegistrationRecord>],
    registration_diagnostics: &'d UringCompletionDiagnostics,
    registrar: &'r (dyn BufferRegistrar + 'r),
    registration_stats: &'d mut UringRegistrationStats,
    registration_mode: BufferRegistrationMode,
    fixed_buffers_available: bool,
    fixed_buffers_failure_errno: Option<i32>,
    registration_quarantine: &'d mut Option<BufferRegistrationQuarantine>,
    #[cfg(feature = "test-hooks")]
    register_buffers_update_outcomes: &'d mut VecDeque<BufferUpdateInjection>,
    #[cfg(feature = "test-hooks")]
    bitset_set_failure: &'d mut bool,
    provided: Option<ProvidedBufSqeInfo>,
}

/// The two narrow submission projections kept together for the transaction API.
pub(crate) struct SubmitEnv<'d, 'r> {
    control: SubmitControlView<'d>,
    resources: SubmitResourceView<'d, 'r>,
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
            if request.target != target {
                return Err(UringError::InvalidState
                    .report("uring.stage_txn.cancel", "cancel request target mismatch")
                    .with_ctx("entry_target", target.index())
                    .with_ctx("request_target", request.target.index()));
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
        phase: &mut SubmissionPhase,
        next: SubmissionPhase,
        reason: &'static str,
    ) {
        transition_submission_phase(phase, token, next, reason, self.control_observer);
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
                target: request.target,
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
        phase: &mut SubmissionPhase,
        next: SubmissionPhase,
        reason: &'static str,
    ) {
        self.control
            .transition_submission_state(token, phase, next, reason);
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

    #[inline]
    pub(crate) fn register_buffer_backend(
        &mut self,
        id: ChunkId,
        ptr: *const u8,
        len: usize,
    ) -> UringResult<BufferRegistrationStatus> {
        self.resources.register_buffer_backend(id, ptr, len)
    }
}

impl SubmitResourceView<'_, '_> {
    /// Narrows this view down to what a `make_sqe` implementation may see.
    #[inline]
    pub(crate) fn sqe_env(&self) -> SqeEnv<'_> {
        SqeEnv {
            file_table: self.file_table,
            chunk_records: self.chunk_records,
            registrar: self.registrar,
            provided: self.provided,
        }
    }

    /// Registers `[ptr, ptr + len)` as the kernel's fixed buffer number `id`.
    pub(crate) fn register_buffer_backend(
        &mut self,
        id: ChunkId,
        ptr: *const u8,
        len: usize,
    ) -> UringResult<BufferRegistrationStatus> {
        let scope = "driver.register_buffer_internal";
        let index = self.prepare_chunk_registration(id, ptr, len, scope)?;
        self.ensure_registration_healthy(scope, id, None)?;

        if let Some(record) = self.chunk_records[index]
            && record.state == ChunkRegistrationState::Registered
        {
            if record.matches(id, ptr, len) {
                debug_assert!(self.cache_is_registered(index));
                return Ok(BufferRegistrationStatus::Registered);
            }
            return self.identity_mismatch_status(id, ptr, len, scope, None, record);
        }

        if !self.fixed_buffers_available {
            self.set_chunk_record(id, ptr, len, ChunkRegistrationState::Fallback);
            if self.registration_mode.is_strict() {
                return Err(self.fixed_buffers_unavailable_report(scope, id, None));
            }
            return self.registration_unavailable(
                id,
                self.fixed_buffers_failure_errno,
                "sparse fixed-buffer registration unavailable",
            );
        }

        if let Some(last_fail) = self.chunk_records[index].and_then(|record| record.last_failure) {
            if last_fail.elapsed() < REGISTER_FAILURE_RETRY_COOLDOWN {
                self.registration_stats
                    .chunk_register_skipped_recent_failure = self
                    .registration_stats
                    .chunk_register_skipped_recent_failure
                    .saturating_add(1);
                self.set_chunk_record(id, ptr, len, ChunkRegistrationState::Fallback);
                if self.registration_mode.is_strict() {
                    return UringError::Registration
                        .push_ctx("scope", scope)
                        .with_ctx("chunk_id", id.raw())
                        .attach_note("recent chunk registration failure cooldown");
                }
                return self.registration_unavailable(
                    id,
                    None,
                    "recent chunk registration failure cooldown",
                );
            }
            self.clear_chunk_failure(index);
        }

        let iovecs = [libc::iovec {
            iov_base: ptr as *mut _,
            iov_len: len,
        }];
        self.set_chunk_record(id, ptr, len, ChunkRegistrationState::Registering);
        self.registration_stats.chunk_register_attempts = self
            .registration_stats
            .chunk_register_attempts
            .saturating_add(1);
        let register = self.issue_chunk_update(index as u32, &iovecs);

        match register.outcome {
            KernelUpdateOutcome::Applied(updated) if updated == register.requested => {
                if let Err(set_report) = self.commit_chunk_registration(index, id, ptr, len) {
                    return self.recover_failed_commit(id, ptr, len, set_report);
                }
                self.registration_stats.chunk_register_success = self
                    .registration_stats
                    .chunk_register_success
                    .saturating_add(1);
                debug_assert!(self.chunk_ledger_is_consistent());
                Ok(BufferRegistrationStatus::Registered)
            }
            #[cfg(feature = "test-hooks")]
            KernelUpdateOutcome::Rejected(errno) => {
                self.note_chunk_registration_failure(id, ptr, len);
                let report = self.chunk_update_report(
                    "driver.register_buffer_internal.register_buffers_update",
                    register,
                    "fixed-buffer registration was rejected before reaching the kernel",
                );
                if self.registration_mode.is_strict() {
                    Err(report)
                } else {
                    self.registration_unavailable(
                        id,
                        Some(errno),
                        "fixed-buffer registration was explicitly rejected",
                    )
                }
            }
            KernelUpdateOutcome::Unknown(_, _) => {
                self.registration_stats.chunk_register_failures = self
                    .registration_stats
                    .chunk_register_failures
                    .saturating_add(1);
                self.quarantine_chunk(id, ptr, len, register, None);
                Err(self.chunk_update_report(
                    "driver.register_buffer_internal.register_buffers_update",
                    register,
                    "fixed-buffer registration result is unknown; ring is quarantined",
                ))
            }
            KernelUpdateOutcome::Applied(_) => {
                self.registration_stats.chunk_register_failures = self
                    .registration_stats
                    .chunk_register_failures
                    .saturating_add(1);
                self.quarantine_chunk(id, ptr, len, register, None);
                Err(self.chunk_update_report(
                    "driver.register_buffer_internal.register_buffers_update",
                    register,
                    "fixed-buffer registration applied an unexpected update count",
                ))
            }
        }
    }

    fn prepare_chunk_registration(
        &self,
        id: ChunkId,
        ptr: *const u8,
        len: usize,
        scope: &'static str,
    ) -> UringResult<usize> {
        let index = id.as_usize();
        if index >= MAX_CHUNKS {
            return UringError::InvalidInput
                .push_ctx("scope", scope)
                .with_ctx("chunk_id", index)
                .with_ctx("max_chunks", MAX_CHUNKS)
                .attach_note("chunk id exceeds maximum registered chunk count");
        }

        if ptr.is_null() || len == 0 {
            return UringError::InvalidInput
                .push_ctx("scope", scope)
                .with_ctx("chunk_id", index)
                .with_ctx("buffer_len", len)
                .attach_note("fixed-buffer registration requires a non-null pointer and length");
        }
        Ok(index)
    }

    fn set_chunk_record(
        &mut self,
        id: ChunkId,
        ptr: *const u8,
        len: usize,
        state: ChunkRegistrationState,
    ) {
        let last_failure = (state == ChunkRegistrationState::Fallback)
            .then(|| self.chunk_records[id.as_usize()].and_then(|record| record.last_failure))
            .flatten();
        self.chunk_records[id.as_usize()] =
            Some(ChunkRegistrationRecord::new(id, ptr, len, state).with_last_failure(last_failure));
    }

    fn clear_chunk_failure(&mut self, index: usize) {
        if let Some(record) = self.chunk_records[index].as_mut() {
            record.last_failure = None;
        }
    }

    #[cfg(feature = "test-hooks")]
    fn note_chunk_registration_failure(&mut self, id: ChunkId, ptr: *const u8, len: usize) {
        self.registration_stats.chunk_register_failures = self
            .registration_stats
            .chunk_register_failures
            .saturating_add(1);
        self.set_chunk_record(id, ptr, len, ChunkRegistrationState::Fallback);
        if let Some(record) = self.chunk_records[id.as_usize()].as_mut() {
            record.last_failure = Some(Instant::now());
        }
    }

    fn cache_is_registered(&self, index: usize) -> bool {
        self.registered_chunks_cache.get(index).unwrap_or(false)
    }

    #[inline]
    fn is_record_registered(&self, id: ChunkId) -> bool {
        let index = id.as_usize();
        self.chunk_records
            .get(index)
            .and_then(Option::as_ref)
            .is_some_and(|record| record.state == ChunkRegistrationState::Registered)
    }

    fn commit_chunk_registration(
        &mut self,
        index: usize,
        id: ChunkId,
        ptr: *const u8,
        len: usize,
    ) -> UringResult<()> {
        // The record is authoritative. The bitset is updated afterwards and is only a derived
        // cache used to diagnose an incomplete commit.
        self.set_chunk_record(id, ptr, len, ChunkRegistrationState::Registered);
        self.mark_registered_cache(index)
    }

    fn recover_failed_commit(
        &mut self,
        id: ChunkId,
        ptr: *const u8,
        len: usize,
        set_report: Report<UringError>,
    ) -> UringResult<BufferRegistrationStatus> {
        let index = id.as_usize();
        let zero_iovec = [libc::iovec {
            iov_base: ptr::null_mut(),
            iov_len: 0,
        }];
        let cleanup = self.issue_chunk_update(index as u32, &zero_iovec);
        match cleanup.outcome {
            KernelUpdateOutcome::Applied(updated) if updated == cleanup.requested => {
                self.set_chunk_record(id, ptr, len, ChunkRegistrationState::Fallback);
                self.clear_registered_cache(index)?;
                if self.registration_mode.is_strict() {
                    Err(set_report.attach_note(
                        "fixed-buffer registration was cleared after ledger cache update failed",
                    ))
                } else {
                    self.registration_unavailable(
                        id,
                        None,
                        "fixed-buffer ledger commit failed; using raw buffer I/O",
                    )
                }
            }
            _ => {
                self.quarantine_chunk(
                    id,
                    ptr,
                    len,
                    UpdateEvidence {
                        requested: 1,
                        outcome: KernelUpdateOutcome::Applied(1),
                    },
                    Some(cleanup),
                );
                let report = set_report
                    .with_ctx("cleanup_evidence", format!("{:?}", cleanup.outcome))
                    .attach_note(
                        "fixed-buffer cleanup result is not fully applied; registry is quarantined",
                    );
                Err(report)
            }
        }
    }

    fn fixed_identity_mismatch_report(
        &self,
        id: ChunkId,
        ptr: *const u8,
        len: usize,
        scope: &'static str,
        user_data: Option<usize>,
        record: ChunkRegistrationRecord,
    ) -> Report<UringError> {
        let report = UringError::InvalidState
            .to_report()
            .push_ctx("scope", scope)
            .with_ctx("chunk_id", id.raw())
            .with_ctx("buffer_ptr", ptr as usize)
            .with_ctx("buffer_len", len)
            .with_ctx("registered_ptr", record.ptr)
            .with_ctx("registered_len", record.len)
            .attach_note("fixed-buffer ledger identity does not match the registered kernel entry");
        if let Some(user_data) = user_data {
            report.with_ctx("user_data", user_data)
        } else {
            report
        }
    }

    fn identity_mismatch_status(
        &mut self,
        id: ChunkId,
        ptr: *const u8,
        len: usize,
        scope: &'static str,
        user_data: Option<usize>,
        record: ChunkRegistrationRecord,
    ) -> UringResult<BufferRegistrationStatus> {
        self.registration_diagnostics
            .inc_fixed_chunk_identity_mismatch();
        let report = self.fixed_identity_mismatch_report(id, ptr, len, scope, user_data, record);
        if self.registration_mode.is_strict() {
            Err(report)
        } else {
            self.registration_unavailable(
                id,
                None,
                "fixed-buffer identity mismatch; using raw buffer I/O",
            )
        }
    }

    fn chunk_ledger_is_consistent(&self) -> bool {
        (0..MAX_CHUNKS).all(|index| {
            let registered = self.registered_chunks_cache.get(index).unwrap_or(false);
            let ledger_registered = self.chunk_records[index]
                .is_some_and(|record| record.state == ChunkRegistrationState::Registered);
            registered == ledger_registered
        })
    }

    /// Registers `chunk_id` on demand so the kernel can reach the buffer this SQE points at.
    ///
    /// Runs after `make_sqe`, which means the very first submission touching a chunk uses the
    /// non-fixed opcode and only later ones get `ReadFixed`/`WriteFixed`.
    pub(crate) fn ensure_chunk_registered(
        &mut self,
        chunk_id: ChunkId,
        user_data: usize,
        scope: &'static str,
    ) -> UringResult<ChunkRegistrationDecision> {
        let index = chunk_id.as_usize();
        if index >= MAX_CHUNKS {
            return UringError::InvalidInput
                .push_ctx("scope", scope)
                .with_ctx("chunk_id", index)
                .with_ctx("max_chunks", MAX_CHUNKS)
                .with_ctx("user_data", user_data)
                .attach_note("chunk id exceeds maximum registered chunk count");
        }
        self.ensure_registration_healthy(scope, chunk_id, Some(user_data))?;
        let record = self.chunk_records[index];
        let Some(info) = self.registrar.resolve_chunk_info(chunk_id) else {
            self.registration_stats.submission_missing_chunk_info = self
                .registration_stats
                .submission_missing_chunk_info
                .saturating_add(1);
            if record.is_some_and(|record| record.state == ChunkRegistrationState::Registered)
                || self.registration_mode.is_strict()
            {
                return UringError::InvalidState
                    .push_ctx("scope", scope)
                    .with_ctx("chunk_id", chunk_id.raw())
                    .with_ctx("user_data", user_data)
                    .attach_note("fixed-buffer identity cannot be verified without chunk info");
            }
            return Ok(self.raw_fallback(
                chunk_id,
                user_data,
                None,
                "missing chunk info for lazy registration",
            ));
        };

        if info.id != chunk_id {
            return UringError::InvalidState
                .push_ctx("scope", scope)
                .with_ctx("chunk_id", chunk_id.raw())
                .with_ctx("resolved_chunk_id", info.id.raw())
                .with_ctx("user_data", user_data)
                .attach_note("chunk registrar returned mismatched chunk info");
        }

        if let Some(record) = record
            && record.state == ChunkRegistrationState::Registered
        {
            if record.matches(info.id, info.ptr.as_ptr(), info.len.get()) {
                return Ok(ChunkRegistrationDecision::Fixed);
            }
            return match self.identity_mismatch_status(
                chunk_id,
                info.ptr.as_ptr(),
                info.len.get(),
                scope,
                Some(user_data),
                record,
            )? {
                BufferRegistrationStatus::Registered => Ok(ChunkRegistrationDecision::Fixed),
                BufferRegistrationStatus::Unavailable => Ok(self.raw_fallback(
                    chunk_id,
                    user_data,
                    None,
                    "fixed-buffer identity mismatch",
                )),
            };
        }

        if !self.fixed_buffers_available {
            if self.registration_mode.is_strict() {
                return Err(self.fixed_buffers_unavailable_report(
                    scope,
                    chunk_id,
                    Some(user_data),
                ));
            }
            return Ok(self.raw_fallback(
                chunk_id,
                user_data,
                self.fixed_buffers_failure_errno,
                "sparse fixed-buffer registration unavailable",
            ));
        }

        // Resolving a missing local snapshot may have drained the worker's chunk message queue;
        // the backend may have registered the chunk through another path in the meantime.
        if self.is_record_registered(info.id) {
            return Ok(ChunkRegistrationDecision::Fixed);
        }

        match self.register_buffer_backend(info.id, info.ptr.as_ptr(), info.len.get())? {
            BufferRegistrationStatus::Registered => Ok(ChunkRegistrationDecision::Fixed),
            BufferRegistrationStatus::Unavailable => Ok(self.raw_fallback(
                chunk_id,
                user_data,
                self.fixed_buffers_failure_errno,
                "chunk fixed-buffer registration unavailable",
            )),
        }
    }

    fn registration_unavailable(
        &mut self,
        chunk_id: ChunkId,
        errno: Option<i32>,
        reason: &'static str,
    ) -> UringResult<BufferRegistrationStatus> {
        debug!(
            chunk_id = chunk_id.raw(),
            errno = ?errno,
            registration_mode = self.registration_mode.as_str(),
            fallback = true,
            reason,
            "fixed-buffer registration unavailable; using raw buffer I/O"
        );
        Ok(BufferRegistrationStatus::Unavailable)
    }

    fn fixed_buffers_unavailable_report(
        &self,
        scope: &'static str,
        chunk_id: ChunkId,
        user_data: Option<usize>,
    ) -> Report<UringError> {
        let report = UringError::Registration
            .to_report()
            .push_ctx("scope", scope)
            .with_ctx("chunk_id", chunk_id.raw())
            .with_ctx("registration_mode", self.registration_mode.as_str())
            .with_ctx("fallback", false)
            .attach_note("sparse fixed-buffer registration is unavailable");
        let report = if let Some(user_data) = user_data {
            report.with_ctx("user_data", user_data)
        } else {
            report
        };
        if let Some(errno) = self.fixed_buffers_failure_errno {
            report.with_ctx("errno", errno)
        } else {
            report
        }
    }

    fn raw_fallback(
        &mut self,
        chunk_id: ChunkId,
        user_data: usize,
        errno: Option<i32>,
        reason: &'static str,
    ) -> ChunkRegistrationDecision {
        self.registration_stats.raw_buffer_fallbacks = self
            .registration_stats
            .raw_buffer_fallbacks
            .saturating_add(1);
        debug!(
            chunk_id = chunk_id.raw(),
            user_data,
            errno = ?errno,
            registration_mode = self.registration_mode.as_str(),
            fallback = true,
            reason,
            "using raw buffer I/O"
        );
        ChunkRegistrationDecision::RawFallback
    }

    fn issue_chunk_update(&mut self, index: u32, iovecs: &[libc::iovec]) -> UpdateEvidence {
        #[cfg(feature = "test-hooks")]
        if let Some(injection) = self.register_buffers_update_outcomes.pop_front() {
            let outcome = match injection {
                BufferUpdateInjection::Applied => KernelUpdateOutcome::Applied(iovecs.len()),
                BufferUpdateInjection::Rejected(errno) => KernelUpdateOutcome::Rejected(errno),
                BufferUpdateInjection::Unknown(errno) => {
                    KernelUpdateOutcome::Unknown(Some(errno), "test-injected unknown buffer update")
                }
            };
            return UpdateEvidence {
                requested: iovecs.len(),
                outcome,
            };
        }

        // SAFETY: `iovecs` points at live chunk memory for the duration of this syscall, and the
        // caller retains ownership of that memory until every in-flight operation completes.
        let outcome = match unsafe { self.submitter.register_buffers_update(index, iovecs, None) } {
            Ok(()) => KernelUpdateOutcome::Applied(iovecs.len()),
            Err(error) => KernelUpdateOutcome::Unknown(
                error.raw_os_error(),
                "kernel fixed-buffer update returned an error",
            ),
        };
        UpdateEvidence {
            requested: iovecs.len(),
            outcome,
        }
    }

    fn chunk_update_report(
        &self,
        scope: &'static str,
        evidence: UpdateEvidence,
        note: &'static str,
    ) -> Report<UringError> {
        let report = UringError::Registration
            .to_report()
            .push_ctx("scope", scope)
            .with_ctx("requested_buffers", evidence.requested)
            .with_ctx("update_evidence", format!("{:?}", evidence.outcome))
            .attach_note(note);
        match evidence.outcome {
            KernelUpdateOutcome::Applied(_) => report,
            #[cfg(feature = "test-hooks")]
            KernelUpdateOutcome::Rejected(errno) => report.with_ctx("errno", errno),
            KernelUpdateOutcome::Unknown(errno, _) => {
                if let Some(errno) = errno {
                    report.with_ctx("errno", errno)
                } else {
                    report
                }
            }
        }
    }

    fn quarantine_chunk(
        &mut self,
        id: ChunkId,
        ptr: *const u8,
        len: usize,
        register: UpdateEvidence,
        cleanup: Option<UpdateEvidence>,
    ) {
        if let Some(quarantine) = self.registration_quarantine.as_mut() {
            quarantine.note_anomaly();
        } else {
            *self.registration_quarantine = Some(BufferRegistrationQuarantine::new(
                id,
                register,
                cleanup,
                "driver.register_buffer_internal",
            ));
        }
        self.set_chunk_record(id, ptr, len, ChunkRegistrationState::Quarantined);
    }

    fn clear_registered_cache(&mut self, index: usize) -> UringResult<()> {
        self.registered_chunks_cache.clear(index).map_err(|error| {
            UringError::InvalidState
                .to_report()
                .push_ctx("scope", "driver.register_buffer_internal.bitset_clear")
                .with_ctx("chunk_index", index)
                .with_ctx("bitset_error", format!("{error:?}"))
                .attach_note("failed to clear derived fixed-buffer registration cache")
        })
    }

    fn mark_registered_cache(&mut self, index: usize) -> UringResult<()> {
        #[cfg(feature = "test-hooks")]
        if *self.bitset_set_failure {
            *self.bitset_set_failure = false;
            return UringError::InvalidState
                .push_ctx("scope", "driver.register_buffer_internal.bitset_set")
                .with_ctx("chunk_index", index)
                .attach_note("injected registered chunk bitset failure");
        }

        self.registered_chunks_cache.set(index).map_err(|e| {
            UringError::InvalidState
                .to_report()
                .push_ctx("scope", "driver.register_buffer_internal.bitset_set")
                .with_ctx("chunk_index", index)
                .with_ctx("bitset_error", format!("{e:?}"))
                .attach_note("BitSet set failed after kernel registration")
        })
    }

    fn ensure_registration_healthy(
        &self,
        scope: &'static str,
        chunk_id: ChunkId,
        user_data: Option<usize>,
    ) -> UringResult<()> {
        let Some(quarantine) = self.registration_quarantine.as_ref() else {
            return Ok(());
        };

        let mut report = UringError::InvalidState
            .to_report()
            .push_ctx("scope", scope)
            .with_ctx("chunk_id", chunk_id.raw())
            .with_ctx("quarantine_chunk_id", quarantine.chunk_id.raw())
            .with_ctx("quarantine_scope", quarantine.scope)
            .with_ctx("quarantine_anomalies", quarantine.anomaly_count)
            .with_ctx(
                "registration_evidence",
                format!("{:?}", quarantine.register_outcome),
            )
            .with_ctx(
                "cleanup_evidence",
                format!("{:?}", quarantine.cleanup_outcome),
            )
            .attach_note("fixed-buffer registry is quarantined and requires ring rebuild");
        if let Some(errno) = quarantine.register_errno {
            report = report.with_ctx("registration_errno", errno);
        }
        if let Some(errno) = quarantine.cleanup_errno {
            report = report.with_ctx("cleanup_errno", errno);
        }
        if let Some(user_data) = user_data {
            Err(report.with_ctx("user_data", user_data))
        } else {
            Err(report)
        }
    }
}

impl UringControlPlane {
    /// Projects only submission-owned control state.
    pub(crate) fn with_submit_view<'d>(
        &'d mut self,
        submission_queue: SubmissionQueue<'d>,
    ) -> SubmitControlView<'d> {
        let cancel_capacity = self.cancellations.capacity();
        let pending_cancel_cqes = self.cancellations.in_flight_mut();
        SubmitControlView {
            submission_queue,
            staged_entries: &mut self.staged_entries,
            timers: &mut self.timers,
            control_observer: &mut self.observer,
            completion_cleanup_hints: &mut self.completion_cleanup_hints,
            completion_cleanup_capacity: self.completion_cleanup_capacity,
            pending_cancel_cqes,
            cancel_capacity,
            #[cfg(feature = "test-hooks")]
            push_entry_failure: &mut self.push_entry_failure,
        }
    }

    /// Projects only completion-owned control state.
    pub(crate) fn with_completion_view(&mut self) -> CompletionControlView<'_> {
        let waker_view = self.waker.hooks_view();
        CompletionControlView::new(
            self.cancellations.in_flight_mut(),
            &mut self.completion_cleanup_hints,
            waker_view.buf_len,
            waker_view.generation,
            &mut self.observer,
            &mut self.post,
        )
    }
}

impl<'a> UringDriver<'a> {
    /// Splits off the op registry from the rest of the driver so a slot borrow and the ring
    /// can be held at the same time. Both halves are plain field projections, so the compiler
    /// — not a raw pointer — is what guarantees they do not alias.
    pub(crate) fn split_for_submit(&mut self) -> (&mut UringOpRegistry, SubmitEnv<'_, 'a>) {
        let view = self.buffer_registry.split_for_submit();
        let (submitter, submission_queue, completion_queue) = self.ring.split();
        drop(completion_queue);
        let control = self.control.with_submit_view(submission_queue);
        let resources = SubmitResourceView {
            submitter,
            file_table: &self.file_table,
            registered_chunks_cache: view.registered_chunks_cache,
            chunk_records: view.chunk_records,
            registration_diagnostics: self.completion_diagnostics.backend(),
            registrar: view.registrar,
            registration_stats: view.registration_stats,
            registration_mode: view.registration_mode,
            fixed_buffers_available: view.fixed_buffers_available,
            fixed_buffers_failure_errno: view.fixed_buffers_failure_errno,
            registration_quarantine: view.registration_quarantine,
            #[cfg(feature = "test-hooks")]
            register_buffers_update_outcomes: view.register_buffers_update_outcomes,
            #[cfg(feature = "test-hooks")]
            bitset_set_failure: view.bitset_set_failure,
            provided: view.provided,
        };

        (&mut self.ops, SubmitEnv { control, resources })
    }

    /// The submission half of [`Self::split_for_submit`], for callers that hold no slot.
    #[inline]
    pub(crate) fn submit_env(&mut self) -> SubmitEnv<'_, 'a> {
        self.split_for_submit().1
    }
}
