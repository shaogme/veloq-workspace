use std::sync::atomic::Ordering;
use std::task::Poll;
use std::time::{Duration, Instant};

use diagweave::prelude::{DiagnosticResult, ResultReportExt};
use veloq_blocking::{BlockingTask, get_blocking_pool};
use veloq_driver_core::driver::{
    DriverSubmitResult, SharedCompletionQueue, SharedCompletionTable, SubmitStatus,
};
use veloq_driver_core::slot::{Reserved, SlotRegistryExt, SlotView};

use crate::common::{completion_record, push_completion_shared};
use crate::driver::{CompletionSidecar, IocpDriver, IocpDriverResult, IocpOpRegistry};
use crate::error::{IocpError, IocpResult, iocp_fallback_event_res};
use crate::op::slot::Slot;
use crate::op::{IocpOp, IocpUserPayload, SubmitContext, submit};

pub(crate) struct SubmitContextInternal<'a> {
    pub(crate) port: &'a crate::win32::IoCompletionPort,
    pub(crate) wheel: &'a mut veloq_wheel::Wheel<usize>,
    pub(crate) completion_events: &'a SharedCompletionQueue,
    pub(crate) completion_table: &'a SharedCompletionTable<IocpUserPayload, IocpError>,
}

impl<'a> IocpDriver<'a> {
    #[inline]
    pub(crate) fn prep_op_slot(
        ops: &mut IocpOpRegistry,
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
        ops: &mut IocpOpRegistry,
        ctx: SubmitContextInternal<'_>,
        user_data: usize,
        task: BlockingTask,
    ) -> IocpDriverResult<Poll<()>> {
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
            return Err(IocpError::Submission.report("iocp/driver", "thread pool overloaded"));
        }
        Ok(Poll::Pending)
    }

    pub(crate) fn on_submit_res(
        ops: &mut IocpOpRegistry,
        ctx: SubmitContextInternal<'_>,
        result: IocpDriverResult<submit::SubmissionResult>,
        user_data: usize,
        op_in: &mut Option<IocpOp>,
    ) -> DriverSubmitResult<IocpError> {
        match result {
            Ok(submit::SubmissionResult::Pending) => DriverSubmitResult::submitted(Poll::Pending),
            Ok(submit::SubmissionResult::PostToQueue) => {
                Self::handle_post_to_queue(ops, ctx, user_data, op_in)
            }
            Ok(submit::SubmissionResult::Offload(task)) => {
                match Self::handle_offload(ops, ctx, user_data, task) {
                    Ok(poll) => DriverSubmitResult::submitted(poll),
                    Err(_) => DriverSubmitResult::failed(
                        IocpError::Submission
                            .report("iocp/driver", "offload task submission failed"),
                        SubmitStatus::InFlight,
                    ),
                }
            }
            Ok(submit::SubmissionResult::Timer(duration)) => {
                Self::handle_timer_sub(ops, ctx, user_data, duration)
            }
            Err(e) => {
                if let Some(SlotView::InFlightWaiting(slot)) = ops.slot_view(user_data) {
                    let mut guard = slot.complete();
                    *op_in = guard.take_op();
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
        user_data: usize,
        op_in: &mut Option<IocpOp>,
    ) -> DriverSubmitResult<IocpError> {
        if let Err(err) = ctx.port.notify(user_data) {
            if let Some(SlotView::InFlightWaiting(slot)) = ops.slot_view(user_data) {
                let mut guard = slot.complete();
                *op_in = guard.take_op();
            }
            let generation = ops.shared.slots[user_data].generation(Ordering::Acquire);
            ops.recycle(user_data, generation.wrapping_add(1));
            DriverSubmitResult::failed(
                err.set_accumulate_src_chain(true)
                    .with_ctx("scope", "iocp/driver")
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
        user_data: usize,
        duration: Duration,
    ) -> DriverSubmitResult<IocpError> {
        let timeout = ctx.wheel.insert(user_data, duration);
        if let Some((_, op_entry)) = ops.get_slot_and_entry_mut(user_data) {
            op_entry.platform_data.timer_id = Some(timeout);
            op_entry.platform_data.timer_deadline = Some(Instant::now() + duration);
        }
        DriverSubmitResult::submitted(Poll::Pending)
    }

    pub(crate) fn call_op_submit(
        &mut self,
        user_data: usize,
        op: IocpOp,
    ) -> IocpDriverResult<IocpDriverResult<submit::SubmissionResult>> {
        let slots_per_page = self.ops.local.len();
        let (slab_ptr, slab_len) = self.ops.get_page_slice(0).ok_or_else(|| {
            IocpError::InvalidState.report("iocp/driver", "failed to get page slice")
        })?;

        let guard = Self::prep_op_slot(&mut self.ops, user_data, op)
            .with_ctx("scope", "iocp/driver")
            .attach_note("failed to prepare op slot")?;

        let overlapped = guard.storage.with_mut(|_result, _payload, sidecar| {
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
                .with_mut(|_result, _payload, sidecar| sidecar.in_flight = false);
        }));
        let result = sub_guard
            .slot
            .as_mut()
            .and_then(|slot| slot.with_op_mut(|op| op.submit(&mut ctx)))
            .ok_or_else(|| {
                IocpError::InvalidState.report("iocp/driver", "op missing during submission")
            })?
            .with_ctx("scope", "iocp/driver")
            .attach_note("op submit failed");

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
                IocpError::InvalidState.report("iocp/driver", "submission guard missing")
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
