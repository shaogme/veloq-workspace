use std::sync::atomic::Ordering;

use tracing::debug;
use veloq_driver_core::slot::{SlotRegistryExt, SlotView};

use crate::common::{completion_record, push_completion_shared};
use crate::driver::completion::EmitContext;
use crate::driver::{CompletionSidecar, IocpDriver, IocpOpRegistry};
use crate::op::submit;

struct CancelContext<'a> {
    registered_slots: &'a [crate::config::RegisteredSlot],
}

impl<'a> IocpDriver<'a> {
    pub(super) fn cancel_op_internal(&mut self, user_data: usize) {
        if !self.ops.contains(user_data) {
            return;
        }

        let emit_ctx = EmitContext {
            completion_events: self.completion.events(),
            completion_table: self.completion.table(),
        };

        let timer_id = self
            .ops
            .get_mut(user_data)
            .and_then(|op| op.platform_data.timer_id);
        if let Some(tid) = timer_id {
            self.timer.cancel(tid);
            Self::emit_aborted_inner(emit_ctx, user_data, &mut self.ops);
            return;
        }

        let state = self.ops.slot_view(user_data);
        match state {
            Some(SlotView::InFlightWaiting(_)) | Some(SlotView::InFlightOrphaned(_)) => {
                let ctx = CancelContext {
                    registered_slots: self.handles.registered_slots(),
                };
                Self::perform_cancel(ctx, user_data, &mut self.ops);
            }
            _ => {
                Self::emit_aborted_inner(emit_ctx, user_data, &mut self.ops);
            }
        }
    }

    fn perform_cancel(ctx: CancelContext<'_>, user_data: usize, ops: &mut IocpOpRegistry) {
        let handled = match ops.slot_view(user_data) {
            Some(SlotView::InFlightWaiting(mut guard)) => {
                let is_rio = guard
                    .with_op_mut(|iocp_op| Self::is_rio_op(iocp_op))
                    .unwrap_or(false);

                if is_rio {
                    guard.platform_mut().rio_cancel_requested = true;
                } else {
                    let raw_handle = guard
                        .with_op_mut(|iocp_op| iocp_op.header.resolved_handle)
                        .flatten()
                        .or_else(|| {
                            let fd = guard.with_op_mut(|iocp_op| iocp_op.get_fd()).flatten()?;
                            submit::resolve_fd_handle(&fd, ctx.registered_slots).ok()
                        });

                    if let Some(raw_handle) = raw_handle {
                        let handle = raw_handle.as_handle();
                        // SAFETY: `guard.storage` exposes the overlapped entry for this cancelled slot.
                        let overlapped_ptr =
                            guard.storage.with_mut(|_result, _payload, sidecar| {
                                &mut sidecar.inner as *mut crate::win32::Overlapped
                            });
                        // SAFETY: handle and overlapped_ptr are valid for this operation.
                        let _ = unsafe {
                            crate::win32::IoCompletionPort::cancel_request(handle, overlapped_ptr)
                        };
                    }
                }
                Some(())
            }
            Some(SlotView::InFlightOrphaned(mut guard)) => {
                let is_rio = guard.op.as_ref().map(Self::is_rio_op).unwrap_or(false);

                if is_rio {
                    guard.platform_mut().rio_cancel_requested = true;
                } else {
                    let raw_handle = guard
                        .op
                        .as_mut()
                        .and_then(|iocp_op| iocp_op.header.resolved_handle)
                        .or_else(|| {
                            let fd = guard.op.as_mut().and_then(|iocp_op| iocp_op.get_fd())?;
                            submit::resolve_fd_handle(&fd, ctx.registered_slots).ok()
                        });

                    if let Some(raw_handle) = raw_handle {
                        let handle = raw_handle.as_handle();
                        let overlapped_ptr =
                            guard.storage.with_mut(|_result, _payload, sidecar| {
                                &mut sidecar.inner as *mut crate::win32::Overlapped
                            });
                        let _ = unsafe {
                            crate::win32::IoCompletionPort::cancel_request(handle, overlapped_ptr)
                        };
                    }
                }
                Some(())
            }
            _ => None,
        };

        if handled.is_none() {
            debug!(user_data, "Skipping cancel for non in-flight slot");
        }
    }

    fn emit_aborted_inner(ctx: EmitContext<'_>, user_data: usize, ops: &mut IocpOpRegistry) {
        let generation = ops.shared.slots[user_data].generation(Ordering::Acquire);
        let inflight = Self::with_inflight_slot(ops, user_data, |guard| {
            let mut guard = guard.complete();
            let _ = guard.take_op();
            let data = guard.take_completion_data();
            let _ = guard.reset();
            data
        });

        let (payload, detail) = if let Some(data) = inflight {
            data
        } else {
            ops.with_slot_storage_mut(user_data, |result, payload, _sidecar| {
                (payload.take(), result.take())
            })
            .unwrap_or((None, None))
        };

        push_completion_shared(
            ctx.completion_events,
            ctx.completion_table,
            completion_record(CompletionSidecar {
                user_data,
                generation,
                res: -(windows_sys::Win32::Foundation::ERROR_OPERATION_ABORTED as i32),
                flags: 0,
                payload,
                detail,
            }),
        );

        ops.remove(user_data);
    }
}
