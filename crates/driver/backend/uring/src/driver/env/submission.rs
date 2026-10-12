use crate::{
    driver::{
        control::{
            ControlPlaneEvent, ControlPlaneObserver, PendingCancel, StagedEntry, StagedLedger,
            UringTimerWheel,
        },
        lifecycle::{SubmissionPhase, UringOpState},
    },
    error::{UringError, UringResult},
    op::CompletionCleanupHintFn,
};
use veloq_driver_core::driver::{CancelTicket, CompletionToken, OpToken};
use veloq_io_uring::{SubmissionQueue, squeue};
use veloq_std::{collections::HashMap, format, time::Duration};
use veloq_wheel::{TimerError, TimerId};
use tracing::trace;

#[cfg(feature = "test-hooks")]
use tracing::debug;

/// The control-plane fields a submission may mutate while an op slot is borrowed.
/// 在借用操作 slot 期间，提交路径可修改的控制面字段集合。
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
/// 先预留元数据再推送 SQE，确保成功推送必有 sidecar；队列满时回滚预留项，供调用方放入有界 backlog。
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
        duration: Duration,
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
