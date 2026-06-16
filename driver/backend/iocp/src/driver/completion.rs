use std::{io, num::NonZeroU8, time::Instant};

use diagweave::prelude::*;
use veloq_driver_core::{
    driver::{
        CancelMode, CompletionAnomaly, CompletionBackend, CompletionBackendHooks,
        CompletionCleanupGuard, CompletionControl, CompletionEnvelope, CompletionFlowExt,
        CompletionFlowOutcome, CompletionHookOutcome, CompletionIngress, CompletionSource,
        SyntheticCompletionSource, UserCompletionEvent,
    },
    slot::{CheckedSlotView, InFlightOrphaned, InFlightWaiting, SlotRegistryExt, SlotView},
};

use crate::{
    driver::{IocpDriver, IocpDriverCompletionDiagnostics, polling::CompletionPump},
    error::{IocpDriverResult, IocpError, IocpResult, iocp_report_to_event_res},
    ext::Extensions,
    op::{IocpOp, IocpOpPayload, IocpSlotSpec, Slot},
    rio::{RioState, SocketInflightToken},
};

pub(crate) const COMP_BACKEND_IOCP: CompletionBackend =
    CompletionBackend::Backend(match NonZeroU8::new(1) {
        Some(val) => val,
        None => unreachable!(),
    });

pub(crate) const COMP_BACKEND_RIO: CompletionBackend =
    CompletionBackend::Backend(match NonZeroU8::new(3) {
        Some(val) => val,
        None => unreachable!(),
    });

pub(crate) enum IocpSyntheticCompletion {
    None,
    Cancel { mode: CancelMode },
}

impl IocpSyntheticCompletion {
    #[inline]
    fn cancel_mode(&self) -> CancelMode {
        match self {
            Self::Cancel { mode } => *mode,
            Self::None => CancelMode::UserVisible,
        }
    }

    #[inline]
    fn take_submission_failure(&mut self) -> Option<Report<IocpError>> {
        None
    }
}

#[derive(Default)]
struct IocpPostCompletionEffects {
    drain_socket_cleanup: bool,
}

enum IocpBackendEffect {
    None,
    SocketInflight(SocketInflightToken),
}

impl Default for IocpBackendEffect {
    #[inline]
    fn default() -> Self {
        Self::None
    }
}

struct IocpCompletionHooks<'a> {
    ext: &'a Extensions,
    diagnostics: &'a IocpDriverCompletionDiagnostics,
    rio: &'a mut RioState,
    completion: &'a CompletionPump,
    synthetic: IocpSyntheticCompletion,
    post: IocpPostCompletionEffects,
}

impl<'a> IocpCompletionHooks<'a> {
    fn new(
        ext: &'a Extensions,
        diagnostics: &'a IocpDriverCompletionDiagnostics,
        rio: &'a mut RioState,
        completion: &'a CompletionPump,
        synthetic: IocpSyntheticCompletion,
    ) -> Self {
        Self {
            ext,
            diagnostics,
            rio,
            completion,
            synthetic,
            post: IocpPostCompletionEffects::default(),
        }
    }

    fn into_post_effects(self) -> IocpPostCompletionEffects {
        self.post
    }
}

impl CompletionBackendHooks<IocpSlotSpec> for IocpCompletionHooks<'_> {
    type BackendIngress = ();
    type BackendEffect = IocpBackendEffect;

    fn handle_control(
        &mut self,
        control: CompletionControl,
    ) -> IocpDriverResult<CompletionHookOutcome<IocpSlotSpec, Self::BackendEffect>> {
        Ok(match control {
            CompletionControl::Waker { raw, .. } => {
                if raw.res >= 0 {
                    self.diagnostics.backend().inc_waker_ok();
                    self.completion.clear_notification();
                } else {
                    self.diagnostics.backend().inc_waker_error();
                    self.completion.clear_notification();
                    return Err(IocpError::Internal
                        .to_report()
                        .push_ctx("scope", "iocp.driver.completion.waker")
                        .set_error_code(-raw.res)
                        .attach_note("IOCP waker completion reported an error"));
                }
                CompletionHookOutcome::ControlHandled {
                    effect: IocpBackendEffect::None,
                }
            }
            CompletionControl::Cancel { raw, .. } => {
                let anomaly = CompletionAnomaly::control_completion_untracked(raw.token)
                    .with_raw_completion(raw);
                CompletionHookOutcome::Anomaly {
                    anomaly,
                    effect: IocpBackendEffect::None,
                }
            }
        })
    }

    fn complete_waiting(
        &mut self,
        event: UserCompletionEvent,
        mut slot: Slot<'_, InFlightWaiting>,
        source: CompletionSource<'_, Self::BackendIngress>,
    ) -> IocpDriverResult<CompletionHookOutcome<IocpSlotSpec, Self::BackendEffect>> {
        Ok(match source {
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
                let io_result = calculate_io_result_from_slot(self.ext, &mut slot, event.res());
                let socket_inflight = take_socket_inflight_from_slot(&mut slot);
                complete_iocp_waiting_slot(slot, event, io_result, socket_inflight)
            }
        })
    }

    fn complete_orphaned(
        &mut self,
        event: UserCompletionEvent,
        slot: Slot<'_, InFlightOrphaned>,
        _source: CompletionSource<'_, Self::BackendIngress>,
    ) -> IocpDriverResult<CompletionHookOutcome<IocpSlotSpec, Self::BackendEffect>> {
        let (cleanup, socket_inflight) = complete_iocp_orphaned_slot(slot, event.res());
        Ok(CompletionHookOutcome::Cleanup {
            cleanup,
            effect: socket_inflight
                .map(IocpBackendEffect::SocketInflight)
                .unwrap_or_default(),
        })
    }

    fn finish_backend_effect(&mut self, effect: Self::BackendEffect) -> IocpResult<()> {
        match effect {
            IocpBackendEffect::None => Ok(()),
            IocpBackendEffect::SocketInflight(token) => {
                self.rio.release_socket_inflight_token(token).trans()?;
                self.post.drain_socket_cleanup = true;
                Ok(())
            }
        }
    }
}

impl<'a> IocpDriver<'a> {
    pub(super) fn process_timers(&mut self) {
        let timer_buffer = self.timer.take_buffer();
        let now = Instant::now();

        let mut expired = Vec::new();
        for &token in &timer_buffer {
            match self.ops.checked_slot_view(token) {
                CheckedSlotView::Valid(SlotView::InFlightWaiting(mut slot)) => {
                    if let Some(deadline) = slot.platform().timer_deadline
                        && now < deadline
                    {
                        let remain = deadline.saturating_duration_since(now);
                        let timer_id = self.timer.insert(token, remain);
                        slot.platform_mut().timer_id = Some(timer_id);
                        continue;
                    }
                    expired.push(token);
                }
                CheckedSlotView::Valid(SlotView::InFlightOrphaned(mut slot)) => {
                    if let Some(deadline) = slot.platform().timer_deadline
                        && now < deadline
                    {
                        let remain = deadline.saturating_duration_since(now);
                        let timer_id = self.timer.insert(token, remain);
                        slot.platform_mut().timer_id = Some(timer_id);
                        continue;
                    }
                    expired.push(token);
                }
                _ => expired.push(token),
            }
        }

        for token in expired {
            let event = UserCompletionEvent::from_parts(COMP_BACKEND_IOCP, token, 0, 0);
            let _ = self.accept_synthetic_completion(
                event,
                SyntheticCompletionSource::Timer,
                IocpSyntheticCompletion::None,
            );
        }
        self.timer.restore_cleared_buffer(timer_buffer);
    }

    pub(super) fn process_completion_envelope(
        &mut self,
        envelope: CompletionEnvelope,
    ) -> IocpResult<usize> {
        self.accept_completion_ingress(
            CompletionIngress::Kernel(envelope),
            IocpSyntheticCompletion::None,
        )?;
        Ok(1)
    }

    pub(crate) fn accept_synthetic_completion(
        &mut self,
        event: UserCompletionEvent,
        source: SyntheticCompletionSource,
        synthetic: IocpSyntheticCompletion,
    ) -> IocpResult<CompletionFlowOutcome> {
        self.accept_completion_ingress(CompletionIngress::Synthetic { event, source }, synthetic)
    }

    pub(crate) fn accept_completion_anomaly(
        &mut self,
        anomaly: CompletionAnomaly,
    ) -> IocpResult<CompletionFlowOutcome> {
        self.accept_completion_ingress(
            CompletionIngress::Anomaly(anomaly),
            IocpSyntheticCompletion::None,
        )
    }

    pub(crate) fn accept_raw_completion(
        &mut self,
        raw_token: u64,
        res: i32,
        flags: u32,
    ) -> IocpResult<CompletionFlowOutcome> {
        self.accept_completion_ingress(
            CompletionIngress::Kernel(CompletionEnvelope::from_raw_parts(
                COMP_BACKEND_IOCP,
                raw_token,
                res,
                flags,
            )),
            IocpSyntheticCompletion::None,
        )
    }

    fn accept_completion_ingress(
        &mut self,
        ingress: CompletionIngress<()>,
        synthetic: IocpSyntheticCompletion,
    ) -> IocpResult<CompletionFlowOutcome> {
        let mut hooks = IocpCompletionHooks::new(
            &self.extensions,
            &self.completion_diagnostics,
            self.rio.state_mut(),
            &self.completion,
            synthetic,
        );
        let outcome = self.ops.accept_completion(
            self.completion.table(),
            &self.completion_diagnostics,
            &mut hooks,
            ingress,
        )?;
        let post = hooks.into_post_effects();
        if post.drain_socket_cleanup {
            self.drain_deferred_socket_cleanup();
        }
        Ok(outcome)
    }
}

fn calculate_io_result_from_slot(
    ext: &Extensions,
    guard: &mut Slot<'_, InFlightWaiting>,
    event_res: i32,
) -> IocpResult<usize> {
    let user_data = guard.snapshot().index;
    let mut io_result = if event_res < 0 {
        Err(IocpError::CompletionWait.io_report(
            "iocp.driver.calculate_io_result_from_slot",
            io::Error::from_raw_os_error(-event_res),
        ))
    } else {
        Ok(event_res as usize)
    };

    let _ = guard.with_op_mut(|iocp_op: &mut IocpOp| {
        let blocking_res = iocp_op
            .header
            .blocking_completion
            .take()
            .and_then(|completion| completion.take_result());
        if let Some(res) = blocking_res {
            io_result = res;
        } else if matches!(
            &iocp_op.payload,
            IocpOpPayload::Open(_)
                | IocpOpPayload::Close(_)
                | IocpOpPayload::Fsync(_)
                | IocpOpPayload::FsyncRaw(_)
                | IocpOpPayload::SyncRange(_)
                | IocpOpPayload::SyncRangeRaw(_)
                | IocpOpPayload::Fallocate(_)
                | IocpOpPayload::FallocateRaw(_)
        ) {
            io_result = Err(IocpError::CompletionWait
                .to_report()
                .push_ctx("scope", "iocp/driver")
                .with_ctx("user_data", user_data)
                .attach_note("missing blocking result for offloaded file completion"));
        } else if let Ok(val) = io_result {
            io_result = iocp_op
                .on_complete(val, ext)
                .attach_note("IOCP completion hook failed");
        }
    });

    io_result
}

fn complete_iocp_waiting_slot(
    guard: Slot<'_, InFlightWaiting>,
    event: UserCompletionEvent,
    io_result: IocpResult<usize>,
    socket_inflight: Option<SocketInflightToken>,
) -> CompletionHookOutcome<IocpSlotSpec, IocpBackendEffect> {
    let mut io_detail = Some(io_result);
    let snapshot = guard.snapshot();
    let effect = socket_inflight
        .map(IocpBackendEffect::SocketInflight)
        .unwrap_or_default();
    let mut guard = guard.complete();

    if guard.platform_mut().is_background {
        let _ = guard.take_op();
        let _ = guard.take_completion_data();
        let _data = std::mem::take(guard.platform_mut());
        return CompletionHookOutcome::Cleanup {
            cleanup: CompletionCleanupGuard::default(),
            effect,
        };
    }

    let completion_res = io_detail
        .as_ref()
        .map(io_result_to_event_res)
        .unwrap_or(event.res());
    let cleanup = if let Some(io_result) = io_detail.as_ref() {
        guard
            .with_op_mut(|op| {
                let cleanup = op.completion_cleanup(io_result);
                op.unbind_user_payload();
                cleanup
            })
            .unwrap_or_default()
    } else {
        let _ = guard.with_op_mut(|op| op.unbind_user_payload());
        CompletionCleanupGuard::default()
    };
    let (payload, detail) = guard.take_completion_data();
    let event =
        UserCompletionEvent::from_parts(COMP_BACKEND_IOCP, event.token(), completion_res, 0);
    if let Some(payload) = payload {
        let _ = guard.take_op();
        let _data = std::mem::take(guard.platform_mut());
        CompletionHookOutcome::User {
            event,
            payload,
            detail: detail.or_else(|| io_detail.take()),
            cleanup,
            effect,
        }
    } else {
        drop(detail);
        let _ = guard.take_op();
        let _data = std::mem::take(guard.platform_mut());
        CompletionHookOutcome::Lost {
            event,
            loss_reason: CompletionAnomaly::corrupt_slot_snapshot(
                event.completion_token(),
                snapshot,
            )
            .with_raw_completion(event.raw()),
            cleanup,
            effect,
        }
    }
}

fn complete_timer_waiting_slot(
    slot: Slot<'_, InFlightWaiting>,
    event: UserCompletionEvent,
) -> CompletionHookOutcome<IocpSlotSpec, IocpBackendEffect> {
    let io_result: IocpResult<usize> = Ok(0);
    complete_iocp_waiting_slot(slot, event, io_result, None)
}

fn complete_submission_failure_slot(
    slot: Slot<'_, InFlightWaiting>,
    event: UserCompletionEvent,
    report: Option<Report<IocpError>>,
) -> CompletionHookOutcome<IocpSlotSpec, IocpBackendEffect> {
    let io_result = report.unwrap_or_else(|| {
        IocpError::Submission
            .to_report()
            .push_ctx("scope", "iocp.driver.submission_failure")
            .set_error_code((-event.res()).max(1))
            .attach_note("IOCP submission failed")
    });
    complete_iocp_waiting_slot(slot, event, Err(io_result), None)
}

fn complete_cancel_waiting_slot(
    slot: Slot<'_, InFlightWaiting>,
    event: UserCompletionEvent,
    mode: CancelMode,
) -> CompletionHookOutcome<IocpSlotSpec, IocpBackendEffect> {
    let abort_result: IocpResult<usize> = Err(IocpError::CompletionWait
        .to_report()
        .push_ctx("scope", "iocp.driver.cancel")
        .set_error_code((-event.res()).max(1))
        .attach_note("operation aborted locally"));
    if mode == CancelMode::UserVisible {
        complete_iocp_waiting_slot(slot, event, abort_result, None)
    } else {
        let mut completed = slot.complete();
        let cleanup = completed
            .with_op_mut(|op| op.orphan_cleanup(&abort_result))
            .unwrap_or_default();
        let _ = completed.take_op();
        let (payload, detail) = completed.take_completion_data();
        drop(payload);
        drop(detail);
        CompletionHookOutcome::Cleanup {
            cleanup,
            effect: IocpBackendEffect::None,
        }
    }
}

fn complete_iocp_orphaned_slot(
    slot: Slot<'_, InFlightOrphaned>,
    event_res: i32,
) -> (CompletionCleanupGuard, Option<SocketInflightToken>) {
    let mut completed = slot.complete();
    let io_result = if event_res >= 0 {
        Ok(event_res as usize)
    } else {
        Err(IocpError::CompletionWait.io_report(
            "iocp.driver.process_completion.orphaned",
            io::Error::from_raw_os_error(-event_res),
        ))
    };
    let (cleanup, socket_inflight) = completed
        .with_op_mut(|op| {
            let cleanup = op.orphan_cleanup(&io_result);
            let socket_inflight = take_socket_inflight_from_op(op);
            (cleanup, socket_inflight)
        })
        .unwrap_or_default();
    let _ = completed.take_op();
    let _ = completed.take_completion_data();
    (cleanup, socket_inflight)
}

#[inline]
fn take_socket_inflight_from_slot(
    slot: &mut Slot<'_, InFlightWaiting>,
) -> Option<SocketInflightToken> {
    slot.with_op_mut(take_socket_inflight_from_op)
        .ok()
        .flatten()
}

#[inline]
fn take_socket_inflight_from_op(op: &mut IocpOp) -> Option<SocketInflightToken> {
    if op.header.in_flight {
        op.header.in_flight = false;
    }
    op.header.socket_inflight.take()
}

#[inline]
fn io_result_to_event_res(res: &IocpResult<usize>) -> i32 {
    match res {
        Ok(v) => (*v).min(i32::MAX as usize) as i32,
        Err(e) => iocp_report_to_event_res(e),
    }
}
