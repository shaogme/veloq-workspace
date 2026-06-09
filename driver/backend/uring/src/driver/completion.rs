use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use diagweave::prelude::*;
use tracing::{debug, error, trace, warn};

use crate::diagnostics::UringCompletionDiagnostics;
use crate::driver::{PendingCancel, UringDriver};
use crate::error::{UringDriverResult, UringError, UringResult, uring_report_to_event_res};
use crate::op::Slot;
use veloq_driver_core::driver::{
    CancelCompletionId, CancelMode, CompletionAnomaly, CompletionBackend, CompletionBackendHooks,
    CompletionCleanupGuard, CompletionControl, CompletionEnvelope, CompletionFlowExt,
    CompletionFlowOutcome, CompletionHookOutcome, CompletionIngress, CompletionSource,
    DriverCompletionDiagnostics, OpToken, PlatformOp, RawCompletion, SyntheticCompletionSource,
    UserCompletionEvent, drain_cancel_requests,
};
use veloq_driver_core::slot::{InFlightOrphaned, InFlightWaiting, SlotRegistryExt};

pub(crate) type CompletionProgress = CompletionFlowOutcome;

use std::num::NonZeroU8;
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
    is_waked: &'a std::sync::atomic::AtomicBool,
    synthetic: UringSyntheticCompletion,
    post: UringPostCompletionEffects,
}

impl<'a> UringCompletionHooks<'a> {
    fn new(
        diagnostics: &'a DriverCompletionDiagnostics<UringCompletionDiagnostics>,
        pending_cancel_cqes: &'a mut HashMap<CancelCompletionId, PendingCancel>,
        waker_buf_len: usize,
        waker_armed: &'a mut bool,
        is_waked: &'a std::sync::atomic::AtomicBool,
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
    ) -> CompletionHookOutcome<crate::op::UringSlotSpec, UringBackendEffect> {
        let request = self.pending_cancel_cqes.remove(&cancel_id);
        let Some(request) = request else {
            let anomaly =
                CompletionAnomaly::control_completion_untracked(raw.token).with_raw_completion(raw);
            debug!(
                cancel_id = cancel_id.raw(),
                result = raw.res,
                flags = raw.flags,
                token = raw.token.raw(),
                "async cancel completion had no pending request"
            );
            return CompletionHookOutcome::Anomaly {
                anomaly,
                effect: UringBackendEffect::None,
            };
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
                CompletionHookOutcome::ControlHandled {
                    effect: UringBackendEffect::None,
                }
            }
            value if value == -libc::ENOENT => {
                self.diagnostics.backend().inc_cancel_ack_not_found();
                debug!(
                    cancel_id = cancel_id.raw(),
                    request = ?request,
                    "async cancel target was already complete or absent"
                );
                CompletionHookOutcome::ControlHandled {
                    effect: UringBackendEffect::CancelEnoent {
                        cancel_id,
                        request,
                        raw,
                    },
                }
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
                CompletionHookOutcome::ControlHandled {
                    effect: UringBackendEffect::None,
                }
            }
        }
    }
}

impl CompletionBackendHooks<crate::op::UringSlotSpec> for UringCompletionHooks<'_> {
    type BackendIngress = ();
    type BackendEffect = UringBackendEffect;

    fn handle_control(
        &mut self,
        control: CompletionControl,
    ) -> CompletionHookOutcome<crate::op::UringSlotSpec, Self::BackendEffect> {
        match control {
            CompletionControl::Waker { raw, .. } => CompletionHookOutcome::ControlHandled {
                effect: self.handle_waker_control(raw),
            },
            CompletionControl::Cancel { id, raw } => self.handle_cancel_control(id, raw),
        }
    }

    fn complete_waiting(
        &mut self,
        event: UserCompletionEvent,
        slot: Slot<'_, InFlightWaiting>,
        source: CompletionSource<'_, Self::BackendIngress>,
    ) -> CompletionHookOutcome<crate::op::UringSlotSpec, Self::BackendEffect> {
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
    ) -> CompletionHookOutcome<crate::op::UringSlotSpec, Self::BackendEffect> {
        let res = match source {
            CompletionSource::Synthetic(SyntheticCompletionSource::Timer) => 0,
            CompletionSource::Synthetic(SyntheticCompletionSource::Cancel) => event.res(),
            CompletionSource::Synthetic(SyntheticCompletionSource::SubmissionFailure)
            | CompletionSource::Kernel
            | CompletionSource::User
            | CompletionSource::Backend(_) => event.raw().res,
        };
        let cleanup = cleanup_orphaned_slot(slot, res);
        CompletionHookOutcome::Cleanup {
            cleanup,
            effect: UringBackendEffect::None,
        }
    }

    fn finish_backend_effect(&mut self, effect: Self::BackendEffect) {
        match effect {
            UringBackendEffect::None => {}
            UringBackendEffect::Waker { should_rebuild } => {
                *self.waker_armed = false;
                self.is_waked.store(false, Ordering::Release);
                self.post.rebuild_waker |= should_rebuild;
                self.post.resubmit_waker = true;
                self.post.flush_backlog = true;
            }
            UringBackendEffect::CancelEnoent {
                cancel_id,
                request,
                raw,
            } => {
                self.post.cancel_enoent.push((cancel_id, request, raw));
            }
        }
    }
}

impl<'a> UringDriver<'a> {
    pub(crate) fn wait_internal(&mut self) -> UringResult<()> {
        let _ = drain_cancel_requests(self)?;
        self.flush_cancellations();
        self.flush_backlog();

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
        self.advance_timers(elapsed);
        self.last_timer_poll = now;

        let progress = self.process_completions_internal()?;
        let _ = progress.semantic_count();
        self.flush_cancellations();
        self.flush_backlog();
        Ok(())
    }

    pub(crate) fn advance_timers(&mut self, elapsed: Duration) {
        self.wheel.advance(elapsed, &mut self.timer_buffer);

        let timer_buffer = std::mem::take(&mut self.timer_buffer);
        for token in timer_buffer {
            let event = UserCompletionEvent::from_parts(COMP_BACKEND_URING, token, 0, 0);
            let _ = self.accept_synthetic_completion(
                event,
                SyntheticCompletionSource::Timer,
                UringSyntheticCompletion::None,
            );
        }
    }

    pub(crate) fn poll_nonblocking_internal(&mut self) -> UringResult<()> {
        let _ = drain_cancel_requests(self)?;
        self.flush_cancellations();
        self.flush_backlog();
        self.submit_to_kernel()?;
        let progress = self.process_completions_internal()?;
        let _ = progress.semantic_count();

        let now = Instant::now();
        let elapsed = now.saturating_duration_since(self.last_timer_poll);
        self.advance_timers(elapsed);
        self.last_timer_poll = now;

        self.flush_cancellations();
        self.flush_backlog();
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

    pub(crate) fn accept_completion_anomaly(
        &mut self,
        anomaly: CompletionAnomaly,
    ) -> UringResult<CompletionFlowOutcome> {
        self.accept_completion_ingress(
            CompletionIngress::Anomaly(anomaly),
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
        );
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
            self.flush_backlog();
        }
        Ok(())
    }

    fn record_cancel_enoent_if_target_active(
        &mut self,
        cancel_id: CancelCompletionId,
        request: PendingCancel,
        raw: RawCompletion,
    ) -> UringResult<()> {
        let active_target = match self.ops.checked_slot_view(request.target) {
            veloq_driver_core::slot::CheckedSlotView::Valid(
                veloq_driver_core::slot::SlotView::InFlightWaiting(slot),
            ) => Some((
                slot.snapshot(),
                "async cancel returned ENOENT while target is still waiting",
            )),
            veloq_driver_core::slot::CheckedSlotView::Valid(
                veloq_driver_core::slot::SlotView::InFlightOrphaned(slot),
            ) => Some((
                slot.snapshot(),
                "async cancel returned ENOENT while target is still orphaned",
            )),
            _ => None,
        };

        let Some((snapshot, message)) = active_target else {
            return Ok(());
        };

        self.completion_diagnostics
            .backend()
            .inc_cancel_ack_enoent_active();
        let target_raw = RawCompletion::new(
            COMP_BACKEND_URING,
            veloq_driver_core::driver::CompletionToken::user(request.target),
            raw.res,
            raw.flags,
        );
        let anomaly = CompletionAnomaly::cancel_ack_target_still_active(
            target_raw.token,
            snapshot.index,
            snapshot.generation,
            snapshot.state,
        )
        .with_slot_snapshot(snapshot)
        .with_raw_completion(target_raw);
        let _ = self.accept_completion_anomaly(anomaly)?;
        debug!(
            cancel_id = cancel_id.raw(),
            request = ?request,
            user_data = snapshot.index,
            generation = snapshot.generation,
            state = ?snapshot.state,
            note = message,
            "async cancel returned ENOENT while target is still active"
        );
        Ok(())
    }
}

fn complete_kernel_waiting_slot(
    mut slot: Slot<'_, InFlightWaiting>,
    token: OpToken,
    raw: RawCompletion,
) -> CompletionHookOutcome<crate::op::UringSlotSpec, UringBackendEffect> {
    let (final_res, cleanup) = match slot.with_op_and_payload_mut(|op, payload| {
        let final_res = unsafe { (op.vtable.on_complete)(op, payload, raw.res) };
        let cleanup = op.completion_cleanup(raw.res);
        (final_res, cleanup)
    }) {
        Ok(result) => result,
        Err(_) => return lost_waiting_slot_completion(slot, raw),
    };

    let mut completed = slot.complete();
    let res_code = driver_result_to_event_res(&final_res);

    let (payload, mut detail) = completed.take_completion_data();
    let Some(payload) = payload else {
        drop(detail);
        return lost_completed_slot_completion(completed, raw, cleanup);
    };
    if detail.is_none()
        && let Err(err) = final_res
    {
        detail = Some(Err(err));
    }
    let _ = completed.take_op();

    CompletionHookOutcome::User {
        event: UserCompletionEvent::from_parts(COMP_BACKEND_URING, token, res_code, raw.flags),
        payload,
        detail,
        cleanup,
        effect: UringBackendEffect::None,
    }
}

fn complete_timer_waiting_slot(
    mut slot: Slot<'_, InFlightWaiting>,
    event: UserCompletionEvent,
) -> CompletionHookOutcome<crate::op::UringSlotSpec, UringBackendEffect> {
    slot.platform_mut().timer_id = None;
    let snapshot = slot.snapshot();
    let mut completed = slot.complete();
    let _ = completed.take_op();
    let (payload, detail) = completed.take_completion_data();
    let Some(payload) = payload else {
        drop(detail);
        return CompletionHookOutcome::Lost {
            event,
            loss_reason: CompletionAnomaly::corrupt_slot_snapshot(
                event.completion_token(),
                snapshot,
            )
            .with_raw_completion(event.raw()),
            snapshot,
            cleanup: CompletionCleanupGuard::default(),
            effect: UringBackendEffect::None,
        };
    };

    CompletionHookOutcome::User {
        event,
        payload,
        detail,
        cleanup: CompletionCleanupGuard::default(),
        effect: UringBackendEffect::None,
    }
}

fn complete_cancel_waiting_slot(
    slot: Slot<'_, InFlightWaiting>,
    event: UserCompletionEvent,
    mode: CancelMode,
) -> CompletionHookOutcome<crate::op::UringSlotSpec, UringBackendEffect> {
    complete_local_cancel_slot(slot, event, mode, false)
}

fn complete_submission_failure_slot(
    slot: Slot<'_, InFlightWaiting>,
    event: UserCompletionEvent,
    report: Option<Report<UringError>>,
) -> CompletionHookOutcome<crate::op::UringSlotSpec, UringBackendEffect> {
    let event_res = event.res();
    let snapshot = slot.snapshot();
    let mut completed = slot.complete();
    let cleanup = completed
        .with_op_mut(|op| op.completion_cleanup(event_res))
        .unwrap_or_default();
    let _ = completed.take_op();
    let (payload, detail) = completed.take_completion_data();
    let Some(payload) = payload else {
        drop(detail);
        return CompletionHookOutcome::Lost {
            event,
            loss_reason: CompletionAnomaly::corrupt_slot_snapshot(
                event.completion_token(),
                snapshot,
            )
            .with_raw_completion(event.raw()),
            snapshot,
            cleanup,
            effect: UringBackendEffect::None,
        };
    };

    CompletionHookOutcome::User {
        event,
        payload,
        detail: detail.or(report.map(Err)),
        cleanup,
        effect: UringBackendEffect::None,
    }
}

fn complete_local_cancel_slot(
    slot: Slot<'_, InFlightWaiting>,
    event: UserCompletionEvent,
    mode: CancelMode,
    orphaned: bool,
) -> CompletionHookOutcome<crate::op::UringSlotSpec, UringBackendEffect> {
    let snapshot = slot.snapshot();
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
        (CancelMode::UserVisible, Some(payload)) => CompletionHookOutcome::User {
            event,
            payload,
            detail,
            cleanup,
            effect: UringBackendEffect::None,
        },
        (CancelMode::UserVisible, None) => {
            drop(detail);
            CompletionHookOutcome::Lost {
                event,
                loss_reason: CompletionAnomaly::corrupt_slot_snapshot(
                    event.completion_token(),
                    snapshot,
                )
                .with_raw_completion(event.raw()),
                snapshot,
                cleanup,
                effect: UringBackendEffect::None,
            }
        }
        (CancelMode::Abandon, payload) => {
            drop(payload);
            drop(detail);
            CompletionHookOutcome::Cleanup {
                cleanup,
                effect: UringBackendEffect::None,
            }
        }
    }
}

fn lost_waiting_slot_completion(
    slot: impl LostWaitingSlot,
    raw: RawCompletion,
) -> CompletionHookOutcome<crate::op::UringSlotSpec, UringBackendEffect> {
    let (snapshot, cleanup) = slot.finish_lost(raw.res);
    let Some(token) = lost_completion_event_token(snapshot, raw) else {
        let anomaly =
            CompletionAnomaly::corrupt_slot_snapshot(raw.token, snapshot).with_raw_completion(raw);
        drop(cleanup);
        return CompletionHookOutcome::Anomaly {
            anomaly,
            effect: UringBackendEffect::None,
        };
    };
    let event = UserCompletionEvent::from_parts(COMP_BACKEND_URING, token, raw.res, raw.flags);
    CompletionHookOutcome::Lost {
        event,
        loss_reason: CompletionAnomaly::corrupt_slot_snapshot(raw.token, snapshot)
            .with_raw_completion(raw),
        snapshot,
        cleanup,
        effect: UringBackendEffect::None,
    }
}

fn lost_completed_slot_completion(
    mut slot: Slot<'_, veloq_driver_core::slot::Completed>,
    raw: RawCompletion,
    cleanup: CompletionCleanupGuard,
) -> CompletionHookOutcome<crate::op::UringSlotSpec, UringBackendEffect> {
    let snapshot = slot.snapshot();
    let Some(token) = lost_completion_event_token(snapshot, raw) else {
        let anomaly =
            CompletionAnomaly::corrupt_slot_snapshot(raw.token, snapshot).with_raw_completion(raw);
        drop(cleanup);
        return CompletionHookOutcome::Anomaly {
            anomaly,
            effect: UringBackendEffect::None,
        };
    };
    let event = UserCompletionEvent::from_parts(COMP_BACKEND_URING, token, raw.res, raw.flags);
    let (payload, detail) = slot.take_completion_data();
    let _ = slot.take_op();
    drop(payload);
    drop(detail);
    CompletionHookOutcome::Lost {
        event,
        loss_reason: CompletionAnomaly::corrupt_slot_snapshot(raw.token, snapshot)
            .with_raw_completion(raw),
        snapshot,
        cleanup,
        effect: UringBackendEffect::None,
    }
}

#[inline]
fn lost_completion_event_token(
    snapshot: veloq_driver_core::slot::SlotSnapshot,
    raw: RawCompletion,
) -> Option<OpToken> {
    snapshot.try_token().ok().or_else(|| raw.token.op_token())
}

trait LostWaitingSlot {
    fn finish_lost(
        self,
        res: i32,
    ) -> (
        veloq_driver_core::slot::SlotSnapshot,
        CompletionCleanupGuard,
    );
}

impl<'a> LostWaitingSlot for Slot<'a, InFlightWaiting> {
    fn finish_lost(
        mut self,
        res: i32,
    ) -> (
        veloq_driver_core::slot::SlotSnapshot,
        CompletionCleanupGuard,
    ) {
        let snapshot = self.snapshot();
        let cleanup = self
            .with_op_mut(|op| op.completion_cleanup(res))
            .unwrap_or_default();
        let mut completed = self.complete();
        let (payload, detail) = completed.take_completion_data();
        let _ = completed.take_op();
        drop(payload);
        drop(detail);
        (snapshot, cleanup)
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
pub(crate) fn driver_result_to_event_res(res: &UringDriverResult<usize>) -> i32 {
    match res {
        Ok(v) => (*v).min(i32::MAX as usize) as i32,
        Err(e) => uring_report_to_event_res(e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicBool;
    use veloq_driver_core::driver::CompletionToken;

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

        assert!(matches!(
            outcome,
            CompletionHookOutcome::Anomaly {
                anomaly: CompletionAnomaly {
                    reason: veloq_driver_core::driver::CompletionAnomalyReason::ControlCompletionUntracked,
                    ..
                },
                ..
            }
        ));
    }
}
