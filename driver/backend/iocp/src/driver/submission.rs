use std::sync::atomic::Ordering;
use std::task::Poll;
use std::time::{Duration, Instant};

use diagweave::DiagnosticResult;
use veloq_blocking::{BlockingTask, get_blocking_pool};
use veloq_driver_core::driver::registry::OpRegistry;
use veloq_driver_core::driver::{
    SharedCompletionQueue, SharedCompletionTable, SubmitBinder, SubmitStatus,
};
use veloq_driver_core::slot::{Reserved, SlotRegistryExt, SlotView};
use veloq_driver_core::{DriverErrorKind, DriverResult, driver_error};

use crate::common::{completion_record, iocp_fallback_event_res, push_completion_shared};
use crate::driver::{CompletionSidecar, IocpDriver, IocpOpState};
use crate::error::{IocpError, IocpResult, IocpResultExt};
use crate::op::slot::Slot;
use crate::op::{IocpOp, IocpUserPayload, OverlappedEntry, SubmitContext, submit};

pub(crate) struct SubmitContextInternal<'a> {
    pub(crate) port: &'a crate::win32::IoCompletionPort,
    pub(crate) wheel: &'a mut veloq_wheel::Wheel<usize>,
    pub(crate) completion_events: &'a SharedCompletionQueue,
    pub(crate) completion_table: &'a SharedCompletionTable<IocpUserPayload>,
}

impl<'a> IocpDriver<'a> {
    #[inline]
    pub(crate) fn prep_op_slot(
        ops: &mut OpRegistry<IocpOp, IocpUserPayload, IocpOpState, OverlappedEntry>,
        user_data: usize,
        op: IocpOp,
    ) -> IocpResult<Slot<'_, Reserved>> {
        let mut guard = ops.slot_reserve(user_data);
        let generation = guard.entry.generation(Ordering::Acquire);
        guard.platform_mut().generation = generation;
        let mut guard = guard.init_op_with(op, |sidecar| {
            sidecar.user_data = user_data;
            sidecar.generation = generation;
            sidecar.blocking_result = None;
            sidecar.in_flight = false;
            sidecar.resolved_handle = None;
        });

        guard
            .with_op_mut(|op_ref| {
                op_ref.header.user_data = user_data;
                op_ref.header.generation = generation;
                op_ref.header.resolved_handle = None;
            })
            .ok_or(IocpError::InvalidState)
            .attach_note("Op missing in prep_op_slot")?;

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
        ops: &mut OpRegistry<IocpOp, IocpUserPayload, IocpOpState, OverlappedEntry>,
        ctx: SubmitContextInternal<'_>,
        user_data: usize,
        task: BlockingTask,
    ) -> DriverResult<Poll<()>> {
        if get_blocking_pool().execute(task).is_err() {
            if let Some(SlotView::InFlightWaiting(slot)) = ops.slot_view(user_data) {
                let mut guard = slot.complete();
                let _ = guard.take_op();
                let (payload, detail) = guard.take_completion_data();
                let sidecar = CompletionSidecar {
                    user_data,
                    generation: guard.entry.generation(Ordering::Acquire),
                    res: iocp_fallback_event_res(IocpError::Submission),
                    flags: 0,
                    payload,
                    detail,
                };
                push_completion_shared(
                    ctx.completion_events,
                    ctx.completion_table,
                    completion_record(sidecar),
                );
            }
            let generation = ops.shared.slots[user_data].generation(Ordering::Acquire);
            ops.recycle(user_data, generation.wrapping_add(1));
            return Err(driver_error(
                DriverErrorKind::Submission,
                "iocp/driver",
                "thread pool overloaded",
            ));
        }
        Ok(Poll::Pending)
    }

    pub(crate) fn on_submit_res(
        ops: &mut OpRegistry<IocpOp, IocpUserPayload, IocpOpState, OverlappedEntry>,
        ctx: SubmitContextInternal<'_>,
        result: DriverResult<submit::SubmissionResult>,
        user_data: usize,
        op_in: &mut Option<IocpOp>,
        binder: SubmitBinder,
    ) -> veloq_driver_core::driver::Outcome<
        Result<Poll<()>, (veloq_driver_core::DriverErrorReport, SubmitStatus)>,
    > {
        match result {
            Ok(submit::SubmissionResult::Pending) => binder.ok(Poll::Pending),
            Ok(submit::SubmissionResult::PostToQueue) => {
                Self::handle_post_to_queue(ops, ctx, user_data, op_in, binder)
            }
            Ok(submit::SubmissionResult::Offload(task)) => {
                match Self::handle_offload(ops, ctx, user_data, task) {
                    Ok(poll) => binder.ok(poll),
                    Err(_) => binder.err(
                        driver_error(
                            DriverErrorKind::Submission,
                            "iocp/driver",
                            "offload task submission failed",
                        ),
                        SubmitStatus::InFlight,
                    ),
                }
            }
            Ok(submit::SubmissionResult::Timer(duration)) => {
                Self::handle_timer_sub(ops, ctx, user_data, duration, binder)
            }
            Err(_e) => {
                if let Some(SlotView::InFlightWaiting(slot)) = ops.slot_view(user_data) {
                    let mut guard = slot.complete();
                    *op_in = guard.take_op();
                }
                binder.err(
                    driver_error(
                        DriverErrorKind::Submission,
                        "iocp/driver",
                        "operation submission failed",
                    ),
                    SubmitStatus::Void,
                )
            }
        }
    }

    pub(crate) fn handle_post_to_queue(
        ops: &mut OpRegistry<IocpOp, IocpUserPayload, IocpOpState, OverlappedEntry>,
        ctx: SubmitContextInternal<'_>,
        user_data: usize,
        op_in: &mut Option<IocpOp>,
        binder: SubmitBinder,
    ) -> veloq_driver_core::driver::Outcome<
        Result<Poll<()>, (veloq_driver_core::DriverErrorReport, SubmitStatus)>,
    > {
        if let Err(err) = ctx.port.notify(user_data) {
            if let Some(SlotView::InFlightWaiting(slot)) = ops.slot_view(user_data) {
                let mut guard = slot.complete();
                *op_in = guard.take_op();
            }
            let generation = ops.shared.slots[user_data].generation(Ordering::Acquire);
            ops.recycle(user_data, generation.wrapping_add(1));
            binder.err(
                err.set_accumulate_src_chain(true)
                    .map_err(|_| DriverErrorKind::Submission)
                    .with_ctx("scope", "iocp/driver")
                    .attach_note("failed to post completion queue notification"),
                SubmitStatus::Void,
            )
        } else {
            binder.ok(Poll::Pending)
        }
    }

    pub(crate) fn handle_timer_sub(
        ops: &mut OpRegistry<IocpOp, IocpUserPayload, IocpOpState, OverlappedEntry>,
        ctx: SubmitContextInternal<'_>,
        user_data: usize,
        duration: Duration,
        binder: SubmitBinder,
    ) -> veloq_driver_core::driver::Outcome<
        Result<Poll<()>, (veloq_driver_core::DriverErrorReport, SubmitStatus)>,
    > {
        let timeout = ctx.wheel.insert(user_data, duration);
        if let Some((_, op_entry)) = ops.get_slot_and_entry_mut(user_data) {
            op_entry.platform_data.timer_id = Some(timeout);
            op_entry.platform_data.timer_deadline = Some(Instant::now() + duration);
        }
        binder.ok(Poll::Pending)
    }

    pub(crate) fn call_op_submit(
        &mut self,
        user_data: usize,
        op: IocpOp,
    ) -> DriverResult<DriverResult<submit::SubmissionResult>> {
        let slots_per_page = self.ops.local.len();
        let (slab_ptr, slab_len) = self.ops.get_page_slice(0).ok_or_else(|| {
            driver_error(
                DriverErrorKind::InvalidState,
                "iocp/driver",
                "failed to get page slice",
            )
        })?;

        let guard = Self::prep_op_slot(&mut self.ops, user_data, op).to_driver_result(
            DriverErrorKind::InvalidState,
            "iocp/driver",
            "failed to prepare op slot",
        )?;

        let overlapped = guard.storage.with_mut(|_op, _result, _payload, sidecar| {
            &mut sidecar.inner as *mut crate::win32::Overlapped
        });

        let mut ctx = SubmitContext {
            port: self.port.as_ref(),
            overlapped,
            ext: &self.extensions,
            registered_files: &self.registered_files,
            registrar: self.registrar.as_ref(),
            rio: &mut self.rio_state,
            slots_per_page,
            slab_resolver: &|idx| (idx == 0).then_some((slab_ptr, slab_len)),
        };

        let mut sub_guard = guard.start_submission_with(Some(|slot| {
            slot.storage
                .with_mut(|_op, _result, _payload, sidecar| sidecar.in_flight = false);
        }));
        let result = sub_guard
            .slot
            .as_mut()
            .and_then(|slot| slot.with_op_mut(|op| op.submit(&mut ctx)))
            .ok_or_else(|| {
                driver_error(
                    DriverErrorKind::InvalidState,
                    "iocp/driver",
                    "op missing during submission",
                )
            })?
            .to_driver_result(
                DriverErrorKind::Submission,
                "iocp/driver",
                "op submit failed",
            );

        let pending_socket_key = if matches!(result, Ok(submit::SubmissionResult::Pending)) {
            sub_guard
                .slot
                .as_mut()
                .and_then(|slot| {
                    slot.with_op_mut(|op| {
                        op.header
                            .in_flight
                            .then_some(op.header.resolved_handle)
                            .flatten()
                    })
                })
                .flatten()
                .filter(|h| h.is_socket())
                .map(|h| h.actor_key())
        } else {
            None
        };

        let mut sub_guard_opt = Some(sub_guard);
        if result.is_ok() {
            let guard = sub_guard_opt.take().ok_or_else(|| {
                driver_error(
                    DriverErrorKind::InvalidState,
                    "iocp/driver",
                    "submission guard missing",
                )
            })?;
            let _ = guard.persist();
        }
        drop(sub_guard_opt);

        if let Some(socket_key) = pending_socket_key {
            self.track_socket_submit_pending(socket_key);
        }

        Ok(result)
    }
}
