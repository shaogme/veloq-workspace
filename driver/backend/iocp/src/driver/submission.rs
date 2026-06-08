use std::sync::Arc;
use std::task::Poll;
use std::time::{Duration, Instant};

use diagweave::prelude::*;
use veloq_blocking::BlockingTask;
use veloq_driver_core::driver::{
    CompletionCleanupGuard, CompletionToken, DriverCompletionDiagnostics, DriverSubmitResult,
    OpToken, SharedCompletionQueue, SharedCompletionTable, SubmitStatus,
};
use veloq_driver_core::slot::{
    CheckedSlotView, Reserved, SlotAccessError, SlotRegistryExt, SlotView,
};

use crate::common::{completion_record, push_completion_shared};
use crate::config::IoFd;
use crate::driver::{CompletionSidecar, IocpDriver, IocpDriverResult, IocpOpRegistry};
use crate::error::{IocpError, IocpResult, iocp_fallback_event_res};
use crate::op::overlapped::BlockingCompletion;
use crate::op::slot::Slot;
use crate::op::{IocpOp, IocpOpPayload, IocpUserPayload, SubmitContext, submit};

pub(crate) struct SubmitContextInternal<'a> {
    port: Arc<crate::win32::IoCompletionPort>,
    wheel: &'a mut veloq_wheel::Wheel<OpToken>,
    completion_events: &'a SharedCompletionQueue,
    completion_table: &'a SharedCompletionTable<IocpUserPayload, IocpError>,
    diagnostics: &'a mut DriverCompletionDiagnostics,
}

impl<'a> SubmitContextInternal<'a> {
    pub(crate) fn new(
        port: Arc<crate::win32::IoCompletionPort>,
        wheel: &'a mut veloq_wheel::Wheel<OpToken>,
        completion_events: &'a SharedCompletionQueue,
        completion_table: &'a SharedCompletionTable<IocpUserPayload, IocpError>,
        diagnostics: &'a mut DriverCompletionDiagnostics,
    ) -> Self {
        Self {
            port,
            wheel,
            completion_events,
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

fn close_fd_from_op(op: &IocpOp) -> Option<IoFd> {
    match &op.payload {
        IocpOpPayload::Close(payload) => {
            // SAFETY: the slot payload is bound before submission starts.
            Some(unsafe { payload.user.as_ref() }.fd)
        }
        _ => None,
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
            .with_op_mut(|op_ref| {
                op_ref.header.reset_for_token(token);
            })
            .map_err(|err| slot_access_report("iocp.driver.prep_op_slot.op_mut", err))?;

        let user_payload = guard
            .storage
            .payload
            .as_mut()
            .ok_or(IocpError::InvalidState)
            .attach_note("User payload missing in prep_op_slot")?;
        let op_ref = guard
            .op
            .as_mut()
            .ok_or(IocpError::InvalidState)
            .attach_note("Op missing while binding user payload in prep_op_slot")?;
        op_ref.bind_user_payload(user_payload)?;

        Ok(guard)
    }

    pub(crate) fn handle_offload(
        ops: &mut IocpOpRegistry,
        ctx: SubmitContextInternal<'_>,
        token: OpToken,
        task: BlockingTask,
    ) -> IocpDriverResult<Poll<()>> {
        if !BlockingBridge::submit(task) {
            if let CheckedSlotView::Valid(SlotView::InFlightWaiting(slot)) =
                ops.checked_slot_view(token)
            {
                let mut guard = slot.complete();
                let _ = guard.take_op();
                let (payload, detail) = guard.take_completion_data();
                let sidecar = CompletionSidecar {
                    token,
                    res: iocp_fallback_event_res(IocpError::Submission),
                    flags: 0,
                    payload,
                    detail,
                    cleanup: CompletionCleanupGuard::default(),
                };
                push_completion_shared(
                    ctx.completion_events,
                    ctx.completion_table,
                    ctx.diagnostics,
                    completion_record(sidecar),
                );
            }
            let _ = ops.recycle_token(token, token.generation().wrapping_add(1));
            return Err(IocpError::Submission.report("iocp/driver", "thread pool overloaded"));
        }
        Ok(Poll::Pending)
    }

    pub(crate) fn on_submit_res(
        ops: &mut IocpOpRegistry,
        ctx: SubmitContextInternal<'_>,
        result: IocpDriverResult<submit::SubmissionResult>,
        token: OpToken,
        op_in: &mut Option<IocpOp>,
    ) -> DriverSubmitResult<IocpError> {
        match result {
            Ok(submit::SubmissionResult::Pending) => DriverSubmitResult::submitted(Poll::Pending),
            Ok(submit::SubmissionResult::PostToQueue) => {
                Self::handle_post_to_queue(ops, ctx, token, op_in)
            }
            Ok(submit::SubmissionResult::Offload(task)) => {
                match Self::handle_offload(ops, ctx, token, task) {
                    Ok(poll) => DriverSubmitResult::submitted(poll),
                    Err(_) => DriverSubmitResult::failed(
                        IocpError::Submission
                            .report("iocp/driver", "offload task submission failed"),
                        SubmitStatus::InFlight,
                    ),
                }
            }
            Ok(submit::SubmissionResult::Timer(duration)) => {
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
        let completion_key = CompletionToken::user(token).raw() as usize;
        if let Err(err) = ctx.port.notify(completion_key) {
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
        let user_data = token.index();
        let timeout = ctx.wheel.insert(token, duration);
        if let Some((_, op_entry)) = ops.get_slot_and_entry_mut(user_data) {
            op_entry.platform_data.timer_id = Some(timeout);
            op_entry.platform_data.timer_deadline = Some(Instant::now() + duration);
        }
        DriverSubmitResult::submitted(Poll::Pending)
    }

    pub(crate) fn call_op_submit(
        &mut self,
        token: OpToken,
        op: IocpOp,
    ) -> IocpDriverResult<IocpDriverResult<submit::SubmissionResult>> {
        let guard = Self::prep_op_slot(&mut self.ops, token, op)
            .push_ctx("scope", "iocp/driver")
            .attach_note("failed to prepare op slot")?;

        let overlapped = guard.storage.with_mut(|_result, _payload, sidecar| {
            &mut sidecar.inner as *mut crate::win32::Overlapped
        });

        let mut sub_guard = guard
            .start_submission_with(Some(|slot| {
                slot.storage
                    .with_mut(|_result, _payload, sidecar| sidecar.in_flight = false);
            }))
            .map_err(|err| slot_access_report("iocp.driver.call_op_submit.start", err))?;
        let close_fd = sub_guard
            .slot
            .as_mut()
            .and_then(|slot| slot.with_op_mut(|op| close_fd_from_op(op)).ok())
            .flatten();

        let result = if let Some(fd) = close_fd {
            let close_result = super::registration::close_registered_owned_fd(
                &mut self.handles,
                self.rio.state_mut(),
                fd,
            );

            close_result.and_then(|(raw_handle, io_result)| {
                let completion_key = CompletionToken::user(token).raw() as usize;
                let completion =
                    BlockingCompletion::new(self.completion.port_arc(), completion_key, None);
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

                Ok(submit::SubmissionResult::PostToQueue)
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
            Ok(submit::SubmissionResult::Pending) => sub_guard
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
