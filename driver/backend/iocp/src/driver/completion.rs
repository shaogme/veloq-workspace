use std::sync::atomic::Ordering;
use std::time::Instant;

use diagweave::prelude::*;
use tracing::debug;
use veloq_driver_core::slot::{InFlightWaiting, SlotRegistryExt, SlotView};

use crate::common::{completion_record, io_result_to_event_res, push_completion_shared};
use crate::driver::{CompletionSidecar, IocpDriver, IocpOpRegistry};
use crate::error::{IocpError, IocpResult};
use crate::op::slot::Slot;
use crate::op::{IocpOp, IocpUserPayload};

pub(super) struct EmitContext<'a> {
    pub(super) completion_events: &'a veloq_driver_core::driver::SharedCompletionQueue,
    pub(super) completion_table:
        &'a veloq_driver_core::driver::SharedCompletionTable<IocpUserPayload, IocpError>,
}

impl<'a> IocpDriver<'a> {
    pub(super) fn process_timers(&mut self) {
        let timer_buffer = self.timer.take_buffer();
        let mut pending_events: Vec<CompletionSidecar> = Vec::new();
        let now = Instant::now();

        let mut expired = Vec::new();
        for &user_data in &timer_buffer {
            let in_flight = matches!(
                self.ops.slot_view(user_data),
                Some(SlotView::InFlightWaiting(_)) | Some(SlotView::InFlightOrphaned(_))
            );
            if let Some(op) = self.ops.local.get_mut(user_data) {
                if in_flight {
                    if let Some(deadline) = op.entry.platform_data.timer_deadline
                        && now < deadline
                    {
                        let remain = deadline.saturating_duration_since(now);
                        op.entry.platform_data.timer_id =
                            Some(self.timer.insert(user_data, remain));
                        continue;
                    }
                    expired.push(user_data);
                } else {
                    op.entry.platform_data.timer_id = None;
                    op.entry.platform_data.timer_deadline = None;
                }
            }
        }
        for user_data in expired {
            Self::finish_timer_op(&mut self.ops, user_data, &mut pending_events);
        }

        for completion in pending_events {
            push_completion_shared(
                self.completion.events(),
                self.completion.table(),
                completion_record(completion),
            );
        }
        self.timer.restore_cleared_buffer(timer_buffer);
    }

    fn finish_timer_op(
        ops: &mut IocpOpRegistry,
        user_data: usize,
        pending_events: &mut Vec<CompletionSidecar>,
    ) {
        let mut guard = match ops.slot_view(user_data) {
            Some(SlotView::InFlightWaiting(slot)) => slot.complete(),
            _ => return,
        };

        let generation = guard.entry.generation(Ordering::Acquire);
        let _ = guard.take_op();
        let (payload_erased, detail) = guard.take_completion_data();
        pending_events.push(CompletionSidecar {
            user_data,
            generation,
            res: 0,
            flags: 0,
            payload: payload_erased,
            detail,
        });
        ops.remove(user_data);
    }

    pub(super) fn process_completion(
        &mut self,
        user_data: usize,
        success: bool,
        error_code: Option<u32>,
        bytes_transferred: u32,
    ) {
        if !self.ops.contains(user_data) {
            return;
        }

        let io_result = self.calculate_io_result(user_data, success, error_code, bytes_transferred);
        match self.ops.slot_view(user_data) {
            Some(SlotView::InFlightWaiting(_)) => {
                let slot_generation;
                {
                    let slot = &self.ops.shared.slots[user_data];
                    let op = &mut self.ops.local[user_data];
                    slot_generation = slot.generation(Ordering::Acquire);

                    if op.entry.platform_data.generation != slot_generation {
                        debug!(user_data, "Ignoring stale completion");
                        return;
                    }
                }

                self.release_socket_inflight_for_op(user_data);
                let ctx = EmitContext {
                    completion_events: self.completion.events(),
                    completion_table: self.completion.table(),
                };
                Self::emit_event_inner(ctx, &mut self.ops, user_data, slot_generation, io_result);
            }
            Some(SlotView::InFlightOrphaned(_)) => {
                let slot_generation =
                    self.ops.shared.slots[user_data].generation(Ordering::Acquire);
                self.release_socket_inflight_for_op(user_data);
                let Some(SlotView::InFlightOrphaned(slot)) = self.ops.slot_view(user_data) else {
                    return;
                };
                let mut completed = slot.complete();
                let _ = completed.take_op();
                let _ = completed.take_completion_data();
                self.ops.recycle(user_data, slot_generation.wrapping_add(1));
            }
            _ => {
                debug!(user_data, "Ignoring completion for non in-flight slot");
            }
        }
    }

    #[inline]
    pub(super) fn with_inflight_slot<R>(
        ops: &mut IocpOpRegistry,
        index: usize,
        f: impl FnOnce(Slot<'_, InFlightWaiting>) -> R,
    ) -> Option<R> {
        match ops.slot_view(index)? {
            SlotView::InFlightWaiting(slot) => Some(f(slot)),
            _ => None,
        }
    }

    fn calculate_io_result(
        &mut self,
        user_data: usize,
        success: bool,
        error_code: Option<u32>,
        bytes_transferred: u32,
    ) -> IocpResult<usize> {
        let mut io_result = if !success {
            Err(IocpError::CompletionWait.io_report(
                "iocp.driver.calculate_io_result",
                std::io::Error::from_raw_os_error(error_code.unwrap_or(0) as i32),
            ))
        } else {
            Ok(bytes_transferred as usize)
        };

        let processed = Self::with_inflight_slot(&mut self.ops, user_data, |mut guard| {
            // SAFETY: InFlight state grants sidecar mutable access.
            let blocking_res = unsafe { guard.sidecar_unchecked(|s| s.blocking_result.take()) };

            let _ = guard.with_op_mut(|iocp_op: &mut IocpOp| {
                if let Some(res) = blocking_res {
                    io_result = res
                        .with_ctx("outer_scope", "iocp.driver.blocking_completion")
                        .attach_note("blocking completion returned stored error");
                } else if matches!(
                    &iocp_op.payload,
                    crate::op::IocpOpPayload::Open(_)
                        | crate::op::IocpOpPayload::Close(_)
                        | crate::op::IocpOpPayload::Fsync(_)
                        | crate::op::IocpOpPayload::FsyncRaw(_)
                        | crate::op::IocpOpPayload::SyncRange(_)
                        | crate::op::IocpOpPayload::SyncRangeRaw(_)
                        | crate::op::IocpOpPayload::Fallocate(_)
                        | crate::op::IocpOpPayload::FallocateRaw(_)
                ) {
                    io_result = Err(IocpError::CompletionWait
                        .to_report()
                        .push_ctx("scope", "iocp/driver")
                        .attach_note("missing blocking result for offloaded file completion"));
                } else if let Ok(val) = io_result {
                    io_result = iocp_op
                        .on_complete(val, &self.extensions)
                        .attach_note("IOCP completion hook failed");
                }
            });
        });

        if processed.is_none() {
            debug!(
                user_data,
                "Skipping IO result calculation for non in-flight slot"
            );
            return io_result;
        }

        io_result
    }

    pub(super) fn emit_event_inner(
        ctx: EmitContext<'_>,
        ops: &mut IocpOpRegistry,
        user_data: usize,
        slot_generation: u32,
        io_result: IocpResult<usize>,
    ) {
        let mut should_free = false;
        let mut sidecar_to_push = None;
        let handled = Self::with_inflight_slot(ops, user_data, |guard| {
            let completion_res = io_result_to_event_res(&io_result);
            let mut io_detail = io_result.err().map(Err);
            let mut guard = guard.complete();

            if guard.platform_mut().is_background {
                let _ = guard.take_op();
                let _ = guard.take_completion_data();
                let _data = std::mem::take(guard.platform_mut());
                should_free = true;
            } else {
                if let Some(op) = guard.op.as_mut() {
                    op.unbind_user_payload();
                }
                let (payload, detail) = guard.take_completion_data();
                sidecar_to_push = Some(CompletionSidecar {
                    user_data,
                    generation: slot_generation,
                    res: completion_res,
                    flags: 0,
                    payload,
                    detail: detail.or_else(|| io_detail.take()),
                });
                let _ = guard.take_op();
                let _data = std::mem::take(guard.platform_mut());
                should_free = true;
            }
        });

        if handled.is_none() {
            debug!(user_data, "Received completion for non-active slot");
        } else if should_free {
            ops.remove(user_data);
        }

        if let Some(sidecar) = sidecar_to_push {
            push_completion_shared(
                ctx.completion_events,
                ctx.completion_table,
                completion_record(sidecar),
            );
        }
    }
}
