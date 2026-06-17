use std::{
    collections::HashMap,
    num::NonZeroU8,
    sync::atomic::{AtomicBool, Ordering},
    time::{Duration, Instant},
};

use diagweave::prelude::*;
use tracing::{debug, error, trace, warn};

use crate::{
    diagnostics::UringCompletionDiagnostics,
    driver::{PendingCancel, UringDriver},
    error::{UringError, UringResult, uring_report_to_event_res},
    op::{Slot, UringSlotSpec, UringUserPayload},
};
use veloq_driver_core::{
    IoFd,
    driver::{
        AnomalyAttach, CancelCompletionId, CancelMode, CompletionAnomalyKind, CompletionBackend,
        CompletionBackendHooks, CompletionCleanupGuard, CompletionControl, CompletionEnvelope,
        CompletionFlowExt, CompletionFlowOutcome, CompletionHookOutcome, CompletionIngress,
        CompletionSource, CompletionToken, DriverCompletionDiagnostics, OpToken, PlatformOp,
        RawCompletion, SyntheticCompletionSource, UserCompletionEvent, drain_cancel_requests,
    },
    slot::{CheckedSlotView, InFlightOrphaned, InFlightWaiting, SlotRegistryExt, SlotView},
};

pub(crate) type CompletionProgress = CompletionFlowOutcome;
pub(crate) const COMP_BACKEND_URING: CompletionBackend =
    CompletionBackend::Backend(match NonZeroU8::new(2) {
        Some(val) => val,
        None => unreachable!(),
    });

pub(crate) enum UringSyntheticCompletion {
    None,
    Cancel { mode: CancelMode },
    SubmissionFailure { report: Option<Report<UringError>> },
}

impl UringSyntheticCompletion {
    #[inline]
    fn cancel_mode(&self) -> CancelMode {
        match self {
            Self::Cancel { mode } => *mode,
            Self::None | Self::SubmissionFailure { .. } => CancelMode::UserVisible,
        }
    }

    #[inline]
    fn take_submission_failure(&mut self) -> Option<Report<UringError>> {
        match self {
            Self::SubmissionFailure { report } => report.take(),
            Self::None | Self::Cancel { .. } => None,
        }
    }
}

#[derive(Default)]
struct UringPostCompletionEffects {
    rebuild_waker: bool,
    resubmit_waker: bool,
    flush_backlog: bool,
    cancel_enoent: Vec<(CancelCompletionId, PendingCancel, RawCompletion)>,
    close_unregister: Vec<IoFd>,
}

enum UringBackendEffect {
    None,
    Waker {
        should_rebuild: bool,
    },
    CancelEnoent {
        cancel_id: CancelCompletionId,
        request: PendingCancel,
        raw: RawCompletion,
    },
    CloseCompleted {
        fd: IoFd,
    },
}

impl Default for UringBackendEffect {
    #[inline]
    fn default() -> Self {
        Self::None
    }
}

struct UringCompletionHooks<'a> {
    diagnostics: &'a DriverCompletionDiagnostics<UringCompletionDiagnostics>,
    pending_cancel_cqes: &'a mut HashMap<CancelCompletionId, PendingCancel>,
    waker_buf_len: usize,
    waker_armed: &'a mut bool,
    is_waked: &'a AtomicBool,
    synthetic: UringSyntheticCompletion,
    post: UringPostCompletionEffects,
}

impl<'a> UringCompletionHooks<'a> {
    fn new(
        diagnostics: &'a DriverCompletionDiagnostics<UringCompletionDiagnostics>,
        pending_cancel_cqes: &'a mut HashMap<CancelCompletionId, PendingCancel>,
        waker_buf_len: usize,
        waker_armed: &'a mut bool,
        is_waked: &'a AtomicBool,
        synthetic: UringSyntheticCompletion,
    ) -> Self {
        Self {
            diagnostics,
            pending_cancel_cqes,
            waker_buf_len,
            waker_armed,
            is_waked,
            synthetic,
            post: UringPostCompletionEffects::default(),
        }
    }

    fn into_post_effects(self) -> UringPostCompletionEffects {
        self.post
    }

    fn handle_waker_control(&mut self, raw: RawCompletion) -> UringBackendEffect {
        let mut should_rebuild = false;
        if raw.res == self.waker_buf_len as i32 {
            self.diagnostics.backend().inc_waker_ok();
        } else if raw.res >= 0 {
            self.diagnostics.backend().inc_waker_error();
            warn!(
                res = raw.res,
                expected = self.waker_buf_len,
                "eventfd waker read returned unexpected byte count"
            );
            should_rebuild = true;
        } else {
            self.diagnostics.backend().inc_waker_error();
            match -raw.res {
                libc::EAGAIN | libc::EINTR => {
                    debug!(res = raw.res, "recoverable eventfd waker read completion");
                }
                errno => {
                    warn!(res = raw.res, errno, "eventfd waker read failed");
                    should_rebuild = true;
                }
            }
        }

        UringBackendEffect::Waker { should_rebuild }
    }

    fn handle_cancel_control(
        &mut self,
        cancel_id: CancelCompletionId,
        raw: RawCompletion,
    ) -> UringResult<CompletionHookOutcome<UringSlotSpec, UringBackendEffect>> {
        let request = self.pending_cancel_cqes.remove(&cancel_id);
        let Some(request) = request else {
            return Err(UringError::InvalidState.report(
                "uring.completion.handle_cancel_control",
                format!(
                    "async cancel completion had no pending request for cancel_id: {}",
                    cancel_id.raw()
                ),
            ));
        };

        match raw.res {
            value if value >= 0 => {
                self.diagnostics.backend().inc_cancel_ack_ok();
                trace!(
                    cancel_id = cancel_id.raw(),
                    request = ?request,
                    result = value,
                    "async cancel completed"
                );
                Ok(CompletionHookOutcome::ControlHandled {
                    effect: UringBackendEffect::None,
                })
            }
            value if value == -libc::ENOENT => {
                self.diagnostics.backend().inc_cancel_ack_not_found();
                debug!(
                    cancel_id = cancel_id.raw(),
                    request = ?request,
                    "async cancel target was already complete or absent"
                );
                Ok(CompletionHookOutcome::ControlHandled {
                    effect: UringBackendEffect::CancelEnoent {
                        cancel_id,
                        request,
                        raw,
                    },
                })
            }
            value => {
                self.diagnostics.backend().inc_cancel_ack_error();
                warn!(
                    cancel_id = cancel_id.raw(),
                    request = ?request,
                    result = value,
                    errno = -value,
                    "async cancel request failed"
                );
                Ok(CompletionHookOutcome::ControlHandled {
                    effect: UringBackendEffect::None,
                })
            }
        }
    }
}

impl CompletionBackendHooks<UringSlotSpec> for UringCompletionHooks<'_> {
    type BackendIngress = ();
    type BackendEffect = UringBackendEffect;

    fn handle_control(
        &mut self,
        control: CompletionControl,
    ) -> UringResult<CompletionHookOutcome<UringSlotSpec, Self::BackendEffect>> {
        match control {
            CompletionControl::Waker { raw, .. } => Ok(CompletionHookOutcome::ControlHandled {
                effect: self.handle_waker_control(raw),
            }),
            CompletionControl::Cancel { id, raw } => self.handle_cancel_control(id, raw),
        }
    }

    fn complete_waiting(
        &mut self,
        event: UserCompletionEvent,
        slot: Slot<'_, InFlightWaiting>,
        source: CompletionSource<'_, Self::BackendIngress>,
    ) -> UringResult<CompletionHookOutcome<UringSlotSpec, Self::BackendEffect>> {
        match source {
            CompletionSource::Synthetic(SyntheticCompletionSource::Timer) => {
                complete_timer_waiting_slot(slot, event)
            }
            CompletionSource::Synthetic(SyntheticCompletionSource::Cancel) => {
                complete_cancel_waiting_slot(slot, event, self.synthetic.cancel_mode())
            }
            CompletionSource::Synthetic(SyntheticCompletionSource::SubmissionFailure) => {
                complete_submission_failure_slot(
                    slot,
                    event,
                    self.synthetic.take_submission_failure(),
                )
            }
            CompletionSource::Kernel | CompletionSource::User | CompletionSource::Backend(_) => {
                complete_kernel_waiting_slot(slot, event.token(), event.raw())
            }
        }
    }

    fn complete_orphaned(
        &mut self,
        event: UserCompletionEvent,
        slot: Slot<'_, InFlightOrphaned>,
        source: CompletionSource<'_, Self::BackendIngress>,
    ) -> UringResult<CompletionHookOutcome<UringSlotSpec, Self::BackendEffect>> {
        let res = match source {
            CompletionSource::Synthetic(SyntheticCompletionSource::Timer) => 0,
            CompletionSource::Synthetic(SyntheticCompletionSource::Cancel) => event.res(),
            CompletionSource::Synthetic(SyntheticCompletionSource::SubmissionFailure)
            | CompletionSource::Kernel
            | CompletionSource::User
            | CompletionSource::Backend(_) => event.raw().res,
        };
        let cleanup = cleanup_orphaned_slot(slot, res);
        Ok(CompletionHookOutcome::Cleanup {
            cleanup,
            effect: UringBackendEffect::None,
        })
    }

    fn finish_backend_effect(&mut self, effect: Self::BackendEffect) -> UringResult<()> {
        match effect {
            UringBackendEffect::None => Ok(()),
            UringBackendEffect::Waker { should_rebuild } => {
                *self.waker_armed = false;
                self.is_waked.store(false, Ordering::Release);
                self.post.rebuild_waker |= should_rebuild;
                self.post.resubmit_waker = true;
                self.post.flush_backlog = true;
                Ok(())
            }
            UringBackendEffect::CancelEnoent {
                cancel_id,
                request,
                raw,
            } => {
                self.post.cancel_enoent.push((cancel_id, request, raw));
                Ok(())
            }
            UringBackendEffect::CloseCompleted { fd } => {
                self.post.close_unregister.push(fd);
                Ok(())
            }
        }
    }
}

impl<'a> UringDriver<'a> {
    pub(crate) fn wait_internal(&mut self) -> UringResult<()> {
        let _ = drain_cancel_requests(self)?;
        self.flush_cancellations()?;
        self.flush_backlog()?;

        if !self.has_active_ops_internal() {
            return Ok(());
        }

        if self.ring.completion().is_empty() {
            let next_timeout = self.wheel.next_timeout();

            if let Some(duration) = next_timeout {
                let ts = io_uring::types::Timespec::new()
                    .sec(duration.as_secs())
                    .nsec(duration.subsec_nanos());

                let args = io_uring::types::SubmitArgs::new().timespec(&ts);
                match self.ring.submitter().submit_with_args(1, &args) {
                    Ok(_) => {}
                    Err(ref e) if e.raw_os_error() == Some(libc::ETIME) => {}
                    Err(e) => {
                        return Err(UringError::CompletionWait
                            .io_report("driver.wait_internal.submit_with_args", e));
                    }
                }
            } else {
                self.ring.submit_and_wait(1).map_err(|e| {
                    UringError::CompletionWait.io_report("driver.wait_internal.submit_and_wait", e)
                })?;
            }
        }

        let now = Instant::now();
        let elapsed = now.saturating_duration_since(self.last_timer_poll);
        self.advance_timers(elapsed)?;
        self.last_timer_poll = now;

        let progress = self.process_completions_internal()?;
        let _ = progress.semantic_count();
        self.flush_cancellations()?;
        self.flush_backlog()?;
        Ok(())
    }

    pub(crate) fn advance_timers(&mut self, elapsed: Duration) -> UringResult<()> {
        self.wheel.advance(elapsed, &mut self.timer_buffer);

        let timer_buffer = std::mem::take(&mut self.timer_buffer);
        for token in timer_buffer {
            let event = UserCompletionEvent::from_parts(COMP_BACKEND_URING, token, 0, 0);
            let _ = self.accept_synthetic_completion(
                event,
                SyntheticCompletionSource::Timer,
                UringSyntheticCompletion::None,
            )?;
        }
        Ok(())
    }

    pub(crate) fn poll_nonblocking_internal(&mut self) -> UringResult<()> {
        let _ = drain_cancel_requests(self)?;
        self.flush_cancellations()?;
        self.flush_backlog()?;
        self.submit_to_kernel()?;
        let progress = self.process_completions_internal()?;
        let _ = progress.semantic_count();

        let now = Instant::now();
        let elapsed = now.saturating_duration_since(self.last_timer_poll);
        self.advance_timers(elapsed)?;
        self.last_timer_poll = now;

        self.flush_cancellations()?;
        self.flush_backlog()?;
        Ok(())
    }

    pub(crate) fn process_completions_internal(&mut self) -> UringResult<CompletionProgress> {
        let _ = unsafe {
            self.ring
                .submitter()
                .enter::<()>(0, 0, 1 /* IORING_ENTER_GETEVENTS */, None)
        };

        let mut cqes = Vec::new();
        {
            let mut cqe_kicker = self.ring.completion();
            cqe_kicker.sync();

            trace!("Processing completions, count={}", cqe_kicker.len());
            for cqe in cqe_kicker {
                cqes.push((cqe.user_data(), cqe.result(), cqe.flags()));
            }
        }

        let mut progress = CompletionProgress::default();
        for (raw_token, cqe_res, cqe_flags) in cqes {
            let outcome = self.accept_completion_ingress(
                CompletionIngress::Kernel(CompletionEnvelope::from_raw_parts(
                    COMP_BACKEND_URING,
                    raw_token,
                    cqe_res,
                    cqe_flags,
                )),
                UringSyntheticCompletion::None,
            )?;
            progress.merge(outcome);
        }

        Ok(progress)
    }

    pub(crate) fn accept_synthetic_completion(
        &mut self,
        event: UserCompletionEvent,
        source: SyntheticCompletionSource,
        synthetic: UringSyntheticCompletion,
    ) -> UringResult<CompletionFlowOutcome> {
        self.accept_completion_ingress(CompletionIngress::Synthetic { event, source }, synthetic)
    }

    pub(crate) fn accept_completion_anomaly_kind(
        &mut self,
        kind: CompletionAnomalyKind,
        attach: AnomalyAttach,
    ) -> UringResult<CompletionFlowOutcome> {
        self.accept_completion_ingress(
            CompletionIngress::Anomaly { kind, attach },
            UringSyntheticCompletion::None,
        )
    }

    fn accept_completion_ingress(
        &mut self,
        ingress: CompletionIngress<()>,
        synthetic: UringSyntheticCompletion,
    ) -> UringResult<CompletionFlowOutcome> {
        let mut hooks = UringCompletionHooks::new(
            &self.completion_diagnostics,
            &mut self.pending_cancel_cqes,
            self.waker_buf.len(),
            &mut self.waker_armed,
            &self.is_waked,
            synthetic,
        );
        let outcome = self.ops.accept_completion(
            &self.completion_table,
            &self.completion_diagnostics,
            &mut hooks,
            ingress,
        )?;
        let post = hooks.into_post_effects();
        self.apply_post_completion_effects(post)?;
        Ok(outcome)
    }

    fn apply_post_completion_effects(
        &mut self,
        post: UringPostCompletionEffects,
    ) -> UringResult<()> {
        for (cancel_id, request, raw) in post.cancel_enoent {
            self.record_cancel_enoent_if_target_active(cancel_id, request, raw)?;
        }

        for fd in post.close_unregister {
            self.unregister_close_owned_fd(fd)?;
        }

        if post.rebuild_waker {
            self.completion_diagnostics.backend().inc_waker_rebuild();
            self.rebuild_waker_fd()
                .attach_note("failed to rebuild eventfd waker")?;
        }
        if post.resubmit_waker
            && let Err(e) = self.submit_waker()
        {
            self.completion_diagnostics.backend().inc_waker_rebuild();
            error!(report = ?e, "failed to resubmit waker");
            return Err(e);
        }
        if post.flush_backlog {
            self.flush_backlog()?;
        }
        Ok(())
    }

    fn record_cancel_enoent_if_target_active(
        &mut self,
        cancel_id: CancelCompletionId,
        request: PendingCancel,
        raw: RawCompletion,
    ) -> UringResult<()> {
        let active_target = match self.ops.checked_slot_view(request.target)? {
            CheckedSlotView::Valid(SlotView::InFlightWaiting(slot)) => Some((
                slot.snapshot(),
                "async cancel returned ENOENT while target is still waiting",
            )),
            CheckedSlotView::Valid(SlotView::InFlightOrphaned(slot)) => Some((
                slot.snapshot(),
                "async cancel returned ENOENT while target is still orphaned",
            )),
            _ => None,
        };

        let Some((snapshot, _message)) = active_target else {
            return Ok(());
        };

        self.completion_diagnostics
            .backend()
            .inc_cancel_ack_enoent_active();
        Err(UringError::InvalidState
            .report(
                "record_cancel_enoent_if_target_active",
                "io_uring cancel returned ENOENT but target slot is still active",
            )
            .with_ctx("cancel_id", cancel_id.raw())
            .with_ctx("expected_index", request.target.index())
            .with_ctx("expected_generation", request.target.generation())
            .with_ctx("actual_index", snapshot.index)
            .with_ctx("actual_generation", snapshot.generation)
            .with_ctx("slot_state", format!("{:?}", snapshot.state))
            .with_ctx("raw_cqe_res", raw.res)
            .with_ctx("raw_cqe_flags", raw.flags)
            .attach_note(
                "The io_uring asynchronous cancel operation completed with -ENOENT (indicating \
                 the operation was not found in kernel's pending queue), but the corresponding \
                 user-space I/O slot remains active (InFlightWaiting or InFlightOrphaned). \
                 This state mismatch indicates a potential memory leak or race condition.",
            ))
    }
}

fn complete_kernel_waiting_slot(
    mut slot: Slot<'_, InFlightWaiting>,
    token: OpToken,
    raw: RawCompletion,
) -> UringResult<CompletionHookOutcome<UringSlotSpec, UringBackendEffect>> {
    let (final_res, cleanup) = match slot.with_op_and_payload_mut(|op, payload| {
        let final_res = unsafe { (op.vtable.on_complete)(op, payload, raw.res) };
        let cleanup = op.completion_cleanup(raw.res);
        (final_res, cleanup)
    }) {
        Ok(result) => result,
        Err(err) => {
            return Err(UringError::InvalidState.report(
                "uring.complete_kernel_waiting_slot",
                format!("slot corruption detected on completion: {:?}", err),
            ));
        }
    };

    let mut completed = slot.complete();
    let res_code = driver_result_to_event_res(&final_res);

    let (payload, mut detail) = completed.take_completion_data();
    let Some(payload) = payload else {
        drop(detail);
        return Err(UringError::InvalidState.report(
            "uring.complete_kernel_waiting_slot",
            "slot payload missing on completion",
        ));
    };

    let effect = if final_res.is_ok() {
        match &payload {
            UringUserPayload::Close(close) => UringBackendEffect::CloseCompleted { fd: close.fd },
            _ => UringBackendEffect::None,
        }
    } else {
        UringBackendEffect::None
    };

    if detail.is_none()
        && let Err(err) = final_res
    {
        detail = Some(Err(err));
    }
    let _ = completed.take_op();

    Ok(CompletionHookOutcome::User {
        event: UserCompletionEvent::from_parts(COMP_BACKEND_URING, token, res_code, raw.flags),
        payload,
        detail,
        cleanup,
        effect,
    })
}

fn complete_timer_waiting_slot(
    mut slot: Slot<'_, InFlightWaiting>,
    event: UserCompletionEvent,
) -> UringResult<CompletionHookOutcome<UringSlotSpec, UringBackendEffect>> {
    slot.platform_mut().timer_id = None;
    let mut completed = slot.complete();
    let _ = completed.take_op();
    let (payload, detail) = completed.take_completion_data();
    let Some(payload) = payload else {
        drop(detail);
        return Err(UringError::InvalidState.report(
            "uring.complete_timer_waiting_slot",
            "slot payload missing on timer completion",
        ));
    };

    Ok(CompletionHookOutcome::User {
        event,
        payload,
        detail,
        cleanup: CompletionCleanupGuard::default(),
        effect: UringBackendEffect::None,
    })
}

fn complete_cancel_waiting_slot(
    slot: Slot<'_, InFlightWaiting>,
    event: UserCompletionEvent,
    mode: CancelMode,
) -> UringResult<CompletionHookOutcome<UringSlotSpec, UringBackendEffect>> {
    complete_local_cancel_slot(slot, event, mode, false)
}

fn complete_submission_failure_slot(
    slot: Slot<'_, InFlightWaiting>,
    event: UserCompletionEvent,
    report: Option<Report<UringError>>,
) -> UringResult<CompletionHookOutcome<UringSlotSpec, UringBackendEffect>> {
    let event_res = event.res();
    let mut completed = slot.complete();
    let cleanup = completed
        .with_op_mut(|op| op.completion_cleanup(event_res))
        .unwrap_or_default();
    let _ = completed.take_op();
    let (payload, detail) = completed.take_completion_data();
    let Some(payload) = payload else {
        drop(detail);
        return Err(UringError::InvalidState.report(
            "uring.complete_submission_failure_slot",
            "slot payload missing on submission failure",
        ));
    };

    Ok(CompletionHookOutcome::User {
        event,
        payload,
        detail: detail.or(report.map(Err)),
        cleanup,
        effect: UringBackendEffect::None,
    })
}

fn complete_local_cancel_slot(
    slot: Slot<'_, InFlightWaiting>,
    event: UserCompletionEvent,
    mode: CancelMode,
    orphaned: bool,
) -> UringResult<CompletionHookOutcome<UringSlotSpec, UringBackendEffect>> {
    let mut completed = slot.complete();
    let cleanup = completed
        .with_op_mut(|op| {
            if mode == CancelMode::Abandon || orphaned {
                op.orphan_cleanup(event.res())
            } else {
                op.completion_cleanup(event.res())
            }
        })
        .unwrap_or_default();
    let (payload, detail) = completed.take_completion_data();
    let _ = completed.take_op();

    match (mode, payload) {
        (CancelMode::UserVisible, Some(payload)) => Ok(CompletionHookOutcome::User {
            event,
            payload,
            detail,
            cleanup,
            effect: UringBackendEffect::None,
        }),
        (CancelMode::UserVisible, None) => {
            drop(detail);
            Err(UringError::InvalidState.report(
                "uring.complete_local_cancel_slot",
                "slot payload missing on cancel",
            ))
        }
        (CancelMode::Abandon, payload) => {
            drop(payload);
            drop(detail);
            Ok(CompletionHookOutcome::Cleanup {
                cleanup,
                effect: UringBackendEffect::None,
            })
        }
    }
}

fn cleanup_orphaned_slot(slot: Slot<'_, InFlightOrphaned>, cqe_res: i32) -> CompletionCleanupGuard {
    let mut completed = slot.complete();
    let cleanup = completed
        .with_op_mut(|op| op.orphan_cleanup(cqe_res))
        .unwrap_or_default();
    let (payload, detail) = completed.take_completion_data();
    let _ = completed.take_op();
    drop(payload);
    drop(detail);
    cleanup
}

#[inline]
pub(crate) fn driver_result_to_event_res(res: &UringResult<usize>) -> i32 {
    match res {
        Ok(v) => (*v).min(i32::MAX as usize) as i32,
        Err(e) => uring_report_to_event_res(e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use veloq_driver_core::driver::CompletionAnomalyReason;

    fn test_hooks<'a>(
        diagnostics: &'a DriverCompletionDiagnostics<UringCompletionDiagnostics>,
        pending_cancel_cqes: &'a mut HashMap<CancelCompletionId, PendingCancel>,
        waker_armed: &'a mut bool,
        is_waked: &'a AtomicBool,
    ) -> UringCompletionHooks<'a> {
        UringCompletionHooks::new(
            diagnostics,
            pending_cancel_cqes,
            8,
            waker_armed,
            is_waked,
            UringSyntheticCompletion::None,
        )
    }

    #[test]
    fn waker_control_records_unexpected_byte_count_as_error() {
        let diagnostics = DriverCompletionDiagnostics::<UringCompletionDiagnostics>::default();
        let mut pending_cancel_cqes = HashMap::new();
        let mut waker_armed = true;
        let is_waked = AtomicBool::new(true);
        let mut hooks = test_hooks(
            &diagnostics,
            &mut pending_cancel_cqes,
            &mut waker_armed,
            &is_waked,
        );
        let raw = RawCompletion::new(COMP_BACKEND_URING, CompletionToken::waker(0), 4, 0);

        let effect = hooks.handle_waker_control(raw);

        assert!(matches!(
            effect,
            UringBackendEffect::Waker {
                should_rebuild: true
            }
        ));
        assert_eq!(diagnostics.snapshot().backend.waker_error, 1);
    }

    #[test]
    fn untracked_cancel_cqe_is_anomaly_not_user_completion() {
        let diagnostics = DriverCompletionDiagnostics::<UringCompletionDiagnostics>::default();
        let mut pending_cancel_cqes = HashMap::new();
        let mut waker_armed = true;
        let is_waked = AtomicBool::new(true);
        let mut hooks = test_hooks(
            &diagnostics,
            &mut pending_cancel_cqes,
            &mut waker_armed,
            &is_waked,
        );
        let cancel_id = CancelCompletionId::new(7);
        let raw = RawCompletion::new(COMP_BACKEND_URING, CompletionToken::cancel(cancel_id), 0, 0);

        let outcome = hooks.handle_cancel_control(cancel_id, raw);

        assert!(outcome.is_err());
        assert_eq!(*outcome.unwrap_err().inner(), UringError::InvalidState);
    }
}
