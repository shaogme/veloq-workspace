use std::sync::Arc;
use std::task::Poll;
use std::time::{Duration, Instant};

use diagweave::prelude::*;
use veloq_blocking::BlockingTask;
use veloq_driver_core::driver::{
    CompletionAnomaly, CompletionBackendHooks, CompletionControl, CompletionFlowExt,
    CompletionHookOutcome, CompletionIngress, CompletionSource, CompletionToken,
    DriverSubmitResult, OpToken, SharedCompletionTable, SubmitStatus, SyntheticCompletionSource,
    UserCompletionEvent,
};
use veloq_driver_core::slot::{
    CheckedSlotView, InFlightOrphaned, InFlightWaiting, Reserved, SlotAccessError, SlotRegistryExt,
    SlotView,
};

use crate::{
    config::IoFd,
    driver::{
        IocpDriver, IocpDriverCompletionDiagnostics, IocpDriverResult, IocpOpRegistry,
        registration::close_registered_owned_fd,
    },
    error::{IocpError, IocpResult, iocp_fallback_event_res},
    op::{
        BlockingCompletion, IocpOp, IocpOpPayload, IocpSlotSpec, Slot, SubmissionResult,
        SubmitContext,
    },
    win32::{IoCompletionPort, Overlapped},
};

pub(crate) struct SubmitContextInternal<'a> {
    port: Arc<IoCompletionPort>,
    wheel: &'a mut veloq_wheel::Wheel<OpToken>,
    completion_table: &'a SharedCompletionTable<IocpSlotSpec>,
    diagnostics: &'a mut IocpDriverCompletionDiagnostics,
}

impl<'a> SubmitContextInternal<'a> {
    pub(crate) fn new(
        port: Arc<IoCompletionPort>,
        wheel: &'a mut veloq_wheel::Wheel<OpToken>,
        completion_table: &'a SharedCompletionTable<IocpSlotSpec>,
        diagnostics: &'a mut IocpDriverCompletionDiagnostics,
    ) -> Self {
        Self {
            port,
            wheel,
            completion_table,
            diagnostics,
        }
    }
}

struct BlockingBridge;

impl BlockingBridge {
    fn submit(task: BlockingTask) -> bool {
        veloq_blocking::get_blocking_pool().execute(task).is_ok()
    }
}

struct SubmissionFailureHooks {
    report: Option<Report<IocpError>>,
}

impl CompletionBackendHooks<IocpSlotSpec> for SubmissionFailureHooks {
    type BackendIngress = ();
    type BackendEffect = ();

    fn handle_control(
        &mut self,
        _control: CompletionControl,
    ) -> crate::error::IocpDriverResult<CompletionHookOutcome<IocpSlotSpec, Self::BackendEffect>>
    {
        Ok(CompletionHookOutcome::Ignore { effect: () })
    }

    fn complete_waiting(
        &mut self,
        event: UserCompletionEvent,
        slot: Slot<'_, InFlightWaiting>,
        _source: CompletionSource<'_, Self::BackendIngress>,
    ) -> crate::error::IocpDriverResult<CompletionHookOutcome<IocpSlotSpec, Self::BackendEffect>>
    {
        let event_res = event.res();
        let snapshot = slot.snapshot();
        let mut guard = slot.complete();
        let cleanup = guard
            .with_op_mut(|op| {
                op.completion_cleanup(&Err(self.report.take().unwrap_or_else(|| {
                    IocpError::Submission
                        .to_report()
                        .push_ctx("scope", "iocp.driver.handle_offload")
                        .set_error_code((-event_res).max(1))
                        .attach_note("offload task submission failed")
                })))
            })
            .unwrap_or_default();
        let _ = guard.take_op();
        let (payload, detail) = guard.take_completion_data();
        Ok(if let Some(payload) = payload {
            CompletionHookOutcome::User {
                event,
                payload,
                detail,
                cleanup,
                effect: (),
            }
        } else {
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
                effect: (),
            }
        })
    }

    fn complete_orphaned(
        &mut self,
        _event: UserCompletionEvent,
        slot: Slot<'_, InFlightOrphaned>,
        _source: CompletionSource<'_, Self::BackendIngress>,
    ) -> crate::error::IocpDriverResult<CompletionHookOutcome<IocpSlotSpec, Self::BackendEffect>>
    {
        let mut guard = slot.complete();
        let cleanup = guard
            .with_op_mut(|op| op.orphan_cleanup(&Err(IocpError::Submission.to_report())))
            .unwrap_or_default();
        let _ = guard.take_op();
        let _ = guard.take_completion_data();
        Ok(CompletionHookOutcome::Cleanup {
            cleanup,
            effect: (),
        })
    }

    fn finish_backend_effect(&mut self, _effect: Self::BackendEffect) -> IocpResult<()> {
        Ok(())
    }
}

fn close_fd_from_op(op: &mut IocpOp) -> IocpResult<Option<IoFd>> {
    match &op.payload {
        IocpOpPayload::Close(payload) => {
            // SAFETY: the slot payload is bound before submission starts.
            Ok(Some(unsafe { payload.user.as_ref()? }.fd))
        }
        _ => Ok(None),
    }
}

fn slot_access_report(scope: &'static str, err: SlotAccessError) -> Report<IocpError> {
    IocpError::InvalidState
        .to_report()
        .push_ctx("scope", scope)
        .with_ctx("slot_index", err.snapshot.index)
        .with_ctx("slot_generation", err.snapshot.generation)
        .with_ctx("slot_state", format!("{:?}", err.snapshot.state))
        .with_ctx("slot_has_op", err.snapshot.has_op)
        .with_ctx("slot_has_payload", err.snapshot.has_payload)
        .with_ctx("slot_access_action", format!("{:?}", err.action))
        .with_ctx("slot_access_reason", format!("{:?}", err.reason))
        .attach_note("slot access failed during IOCP submission")
}

impl<'a> IocpDriver<'a> {
    #[inline]
    pub(crate) fn prep_op_slot(
        ops: &mut IocpOpRegistry,
        token: OpToken,
        op: IocpOp,
    ) -> IocpResult<Slot<'_, Reserved>> {
        let user_data = token.index();
        let generation = token.generation();
        let mut guard = match ops.checked_slot_view(token) {
            CheckedSlotView::Valid(SlotView::Reserved(slot)) => slot,
            _ => {
                return IocpError::InvalidState
                    .with_ctx("user_data", user_data)
                    .with_ctx("generation", generation)
                    .attach_note("reserved IOCP slot missing during preparation");
            }
        };
        guard.platform_mut().generation = generation;
        guard.platform_mut().rio_cancel_requested = false;
        let mut guard = guard
            .init_op_with(op, |sidecar| {
                sidecar.reset_for_token(token);
            })
            .map_err(|err| slot_access_report("iocp.driver.prep_op_slot.init_op", err))?;

        guard
            .with_op_and_payload_mut(|op_ref, user_payload| {
                op_ref.header.reset_for_token(token);
                op_ref.bind_user_payload(user_payload)
            })
            .map_err(|err| slot_access_report("iocp.driver.prep_op_slot.op_payload", err))??;

        Ok(guard)
    }

    pub(crate) fn handle_offload(
        ops: &mut IocpOpRegistry,
        ctx: SubmitContextInternal<'_>,
        token: OpToken,
        task: BlockingTask,
    ) -> IocpDriverResult<Poll<()>> {
        if !BlockingBridge::submit(task) {
            let report = IocpError::Submission.report("iocp/driver", "thread pool overloaded");
            let event_res = iocp_fallback_event_res(IocpError::Submission);
            let event = UserCompletionEvent::from_parts(
                super::completion::COMP_BACKEND_IOCP,
                token,
                event_res,
                0,
            );
            let mut hooks = SubmissionFailureHooks {
                report: Some(report),
            };
            let _ = ops.accept_completion(
                ctx.completion_table,
                ctx.diagnostics,
                &mut hooks,
                CompletionIngress::Synthetic {
                    event,
                    source: SyntheticCompletionSource::SubmissionFailure,
                },
            )?;
            return Err(IocpError::Submission.report("iocp/driver", "thread pool overloaded"));
        }
        Ok(Poll::Pending)
    }

    pub(crate) fn on_submit_res(
        ops: &mut IocpOpRegistry,
        ctx: SubmitContextInternal<'_>,
        result: IocpDriverResult<SubmissionResult>,
        token: OpToken,
        op_in: &mut Option<IocpOp>,
    ) -> DriverSubmitResult<IocpError> {
        match result {
            Ok(SubmissionResult::Pending) => DriverSubmitResult::submitted(Poll::Pending),
            Ok(SubmissionResult::PostToQueue) => Self::handle_post_to_queue(ops, ctx, token, op_in),
            Ok(SubmissionResult::Offload(task)) => {
                match Self::handle_offload(ops, ctx, token, task) {
                    Ok(poll) => DriverSubmitResult::submitted(poll),
                    Err(_) => DriverSubmitResult::failed(
                        IocpError::Submission
                            .report("iocp/driver", "offload task submission failed"),
                        SubmitStatus::InFlight,
                    ),
                }
            }
            Ok(SubmissionResult::Timer(duration)) => {
                Self::handle_timer_sub(ops, ctx, token, duration)
            }
            Err(e) => {
                if let CheckedSlotView::Valid(SlotView::InFlightWaiting(slot)) =
                    ops.checked_slot_view(token)
                {
                    let mut guard = slot.complete();
                    *op_in = guard.take_op().ok();
                }
                DriverSubmitResult::failed(
                    e.attach_note("operation submission failed"),
                    SubmitStatus::Void,
                )
            }
        }
    }

    pub(crate) fn handle_post_to_queue(
        ops: &mut IocpOpRegistry,
        ctx: SubmitContextInternal<'_>,
        token: OpToken,
        op_in: &mut Option<IocpOp>,
    ) -> DriverSubmitResult<IocpError> {
        let completion_token = CompletionToken::user(token);
        if let Err(err) = ctx.port.notify(completion_token) {
            if let CheckedSlotView::Valid(SlotView::InFlightWaiting(slot)) =
                ops.checked_slot_view(token)
            {
                let mut guard = slot.complete();
                *op_in = guard.take_op().ok();
            }
            DriverSubmitResult::failed(
                err.set_accumulate_src_chain(true)
                    .push_ctx("scope", "iocp/driver")
                    .attach_note("failed to post completion queue notification"),
                SubmitStatus::Void,
            )
        } else {
            DriverSubmitResult::submitted(Poll::Pending)
        }
    }

    pub(crate) fn handle_timer_sub(
        ops: &mut IocpOpRegistry,
        ctx: SubmitContextInternal<'_>,
        token: OpToken,
        duration: Duration,
    ) -> DriverSubmitResult<IocpError> {
        let timeout = ctx.wheel.insert(token, duration);
        if let Some(platform) = ops.platform_mut(token) {
            platform.timer_id = Some(timeout);
            platform.timer_deadline = Some(Instant::now() + duration);
        } else {
            ctx.wheel.cancel(timeout);
            return DriverSubmitResult::failed(
                IocpError::InvalidState
                    .to_report()
                    .push_ctx("scope", "iocp.driver.handle_timer_sub")
                    .with_ctx("user_data", token.index())
                    .with_ctx("generation", token.generation())
                    .attach_note("timer submission target slot is not active"),
                SubmitStatus::Void,
            );
        }
        DriverSubmitResult::submitted(Poll::Pending)
    }

    pub(crate) fn call_op_submit(
        &mut self,
        token: OpToken,
        op: IocpOp,
    ) -> IocpDriverResult<IocpDriverResult<SubmissionResult>> {
        let mut guard = Self::prep_op_slot(&mut self.ops, token, op)
            .push_ctx("scope", "iocp/driver")
            .attach_note("failed to prepare op slot")?;

        let overlapped = guard.with_sidecar_mut(|sidecar| &mut sidecar.inner as *mut Overlapped);

        let mut sub_guard = guard
            .start_submission_with(Some(|slot| {
                slot.with_sidecar_mut(|sidecar| sidecar.in_flight = false);
            }))
            .map_err(|err| slot_access_report("iocp.driver.call_op_submit.start", err))?;
        let close_fd = if let Some(slot) = sub_guard.slot.as_mut() {
            slot.with_op_mut(close_fd_from_op)
                .map_err(|err| slot_access_report("iocp.driver.call_op_submit.close_fd", err))??
        } else {
            None
        };

        let result = if let Some(fd) = close_fd {
            let close_result =
                close_registered_owned_fd(&mut self.handles, self.rio.state_mut(), fd);

            close_result.and_then(|(raw_handle, io_result)| {
                let completion = BlockingCompletion::new(
                    self.completion.port_arc(),
                    CompletionToken::user(token),
                    None,
                );
                completion.store_result(io_result);

                let slot = sub_guard.slot.as_mut().ok_or_else(|| {
                    IocpError::InvalidState
                        .report("iocp/driver", "submission guard slot missing during Close")
                })?;
                slot.with_op_mut(|op| {
                    op.header.resolved_handle = Some(raw_handle);
                    op.header.blocking_completion = Some(completion);
                })
                .map_err(|err| slot_access_report("iocp.driver.call_op_submit.close_op", err))?;

                Ok(SubmissionResult::PostToQueue)
            })
        } else {
            let (rio, registrar) = self.rio.state_and_registrar_mut();
            let registered_slots = self.handles.submission_slots();
            let mut ctx = SubmitContext {
                port: self.completion.port_arc(),
                overlapped,
                op_token: token,
                completion_token: CompletionToken::user(token),
                ext: &self.extensions,
                registered_slots,
                registrar,
                rio,
            };

            let slot = sub_guard.slot.as_mut().ok_or_else(|| {
                IocpError::InvalidState.report(
                    "iocp/driver",
                    "submission guard slot missing during submission",
                )
            })?;
            slot.with_op_mut(|op| op.submit(&mut ctx))
                .map_err(|err| slot_access_report("iocp.driver.call_op_submit.submit_op", err))?
        }
        .push_ctx("scope", "iocp/driver")
        .attach_note("op submit failed");

        let socket_pending_without_inflight_token = match &result {
            Ok(SubmissionResult::Pending) => sub_guard
                .slot
                .as_mut()
                .and_then(|slot| {
                    slot.with_op_mut(|op| {
                        !Self::is_rio_op(op)
                            && op.header.in_flight
                            && op.header.resolved_handle.is_some_and(|h| h.is_socket())
                            && op.header.socket_inflight.is_none()
                    })
                    .ok()
                })
                .unwrap_or(false),
            _ => false,
        };

        debug_assert!(
            !socket_pending_without_inflight_token,
            "kernel-pending socket op missing pre-acquired socket inflight token"
        );

        let mut sub_guard_opt = Some(sub_guard);
        if result.is_ok() {
            let guard = sub_guard_opt.take().ok_or_else(|| {
                IocpError::InvalidState.report("iocp/driver", "submission guard missing")
            })?;
            let _ = guard.persist();
        }
        drop(sub_guard_opt);

        Ok(result)
    }
}
