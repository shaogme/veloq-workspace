use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};
use tracing::debug;
use windows_sys::Win32::Foundation::WAIT_TIMEOUT;

use veloq_driver_core::driver::{
    SharedCompletionQueue, SharedCompletionTable, drain_cancel_requests,
};
use veloq_driver_core::slot::{InFlightWaiting, SlotRegistryExt, SlotView};

use diagweave::prelude::*;

use crate::common::{
    IocpErrorContext, WAKEUP_USER_DATA, completion_record, io_result_to_event_res, iocp_msg,
    push_completion_shared,
};
use crate::config::SocketKey;
use crate::driver::{
    CloseMode, CompletionSidecar, IocpDriver, IocpDriverResult, IocpOpRegistry, RIO_EVENT_KEY,
};
use crate::error::{IocpError, IocpResult};
use crate::op::slot::Slot;
use crate::op::{IocpOp, IocpUserPayload, submit};

pub(crate) struct EmitContext<'a> {
    pub(crate) completion_events: &'a SharedCompletionQueue,
    pub(crate) completion_table: &'a SharedCompletionTable<IocpUserPayload, IocpError>,
}

pub(crate) struct CancelContext<'a> {
    pub(crate) registered_files: &'a [Option<crate::config::RegisteredHandle>],
    pub(crate) completion_events: &'a SharedCompletionQueue,
    pub(crate) completion_table: &'a SharedCompletionTable<IocpUserPayload, IocpError>,
}

impl<'a> IocpDriver<'a> {
    pub(crate) fn shutdown_ops(&mut self) -> usize {
        if self.shutting_down {
            return 0;
        }
        self.shutting_down = true;
        self.rio_state.begin_shutdown();

        let mut in_flight = Vec::new();
        for user_data in 0..self.ops.local.len() {
            if matches!(
                self.ops.slot_view(user_data),
                Some(SlotView::InFlightWaiting(_)) | Some(SlotView::InFlightOrphaned(_))
            ) {
                in_flight.push(user_data);
            }
        }
        let count = in_flight.len();
        for user_data in in_flight {
            self.cancel_op_internal(user_data);
        }
        count
    }

    pub(crate) fn drain_pending_iocp(
        &mut self,
        pending_count: usize,
        timeout: Duration,
    ) -> IocpDriverResult<()> {
        if pending_count == 0 {
            return Ok(());
        }
        let mut drained = 0usize;
        let deadline = Instant::now() + timeout;

        while drained < pending_count {
            if Instant::now() >= deadline {
                return Err(IocpError::CompletionWait.report("iocp/driver", "drain timed out"));
            }
            drained += self.poll_completion()?;
        }
        Ok(())
    }

    pub(crate) fn poll_completion(&mut self) -> IocpDriverResult<usize> {
        let status = self
            .port
            .get_status(10)
            .with_ctx("scope", "iocp/driver")
            .attach_note("failed to poll IOCP status")?;

        match status {
            crate::win32::CompletionStatus::Completed {
                bytes,
                key,
                overlapped,
                success,
                error_code,
            } => {
                if key == RIO_EVENT_KEY {
                    let processed = self
                        .rio_state
                        .process_completions(
                            &mut self.ops,
                            &self.extensions,
                            &*self.registrar,
                            &self.completion_events,
                            &self.completion_table,
                        )
                        .inspect(|_| {
                            self.drain_deferred_socket_cleanup();
                        })
                        .with_ctx("scope", "iocp/driver")
                        .attach_note("failed to process rio completions")
                        .trans()?;
                    return Ok(processed);
                }

                if !overlapped.is_null() {
                    // SAFETY: overlapped pointer is guaranteed to be valid during IOCP completion.
                    let id = unsafe { crate::win32::OverlappedId::from_ptr(overlapped) };
                    self.process_completion(id.as_usize(), success, error_code, bytes);
                    return Ok(1);
                }
            }
            crate::win32::CompletionStatus::Timeout => {}
        }
        Ok(0)
    }

    pub(crate) fn close_impl(&mut self, mode: CloseMode) -> IocpDriverResult<()> {
        if self.closed {
            return Ok(());
        }
        let pending = self.shutdown_ops();
        if let CloseMode::Strict { timeout } = mode {
            self.drain_pending_iocp(pending, timeout).map_err(|e| {
                e.set_accumulate_src_chain(true)
                    .with_ctx("scope", "iocp/driver")
                    .attach_note("drain pending iocp timed out")
            })?;
            self.rio_state
                .drain_outstanding(timeout)
                .with_ctx("scope", "iocp/driver")
                .attach_note("failed to drain RIO outstanding requests")
                .trans()?;
        }
        self.rio_state.kernel.close();
        self.closed = true;
        Ok(())
    }

    /// Retrieves completion events from the I/O completion port.
    pub(crate) fn get_completion(&mut self, timeout_ms: u32) -> IocpResult<()> {
        drain_cancel_requests(self);
        let wait_ms = self.calculate_wait_ms(timeout_ms);

        let status = self.port.get_status(wait_ms);
        let now = Instant::now();
        let elapsed = now.saturating_duration_since(self.last_timer_poll);
        self.wheel.advance(elapsed, &mut self.timer_buffer);
        self.process_timers();
        self.last_timer_poll = now;

        let status = status
            .attach_note("failed to get IOCP completion status")
            .trans()?;

        match status {
            crate::win32::CompletionStatus::Completed {
                bytes,
                key,
                overlapped,
                success,
                error_code,
            } => {
                if key == RIO_EVENT_KEY {
                    self.rio_state
                        .process_completions(
                            &mut self.ops,
                            &self.extensions,
                            &*self.registrar,
                            &self.completion_events,
                            &self.completion_table,
                        )
                        .inspect(|_| {
                            self.drain_deferred_socket_cleanup();
                        })
                        .attach_note("failed to process RIO completions")
                        .trans()?;
                    return Ok(());
                }

                let user_data = self.resolve_user_data(overlapped, success, key, error_code)?;

                if user_data == WAKEUP_USER_DATA {
                    self.is_notified.store(false, Ordering::Release);
                    return Ok(());
                }
                self.process_completion(user_data, success, error_code, bytes);
            }
            crate::win32::CompletionStatus::Timeout => {}
        }
        Ok(())
    }

    pub(crate) fn calculate_wait_ms(&self, timeout_ms: u32) -> u32 {
        if let Some(delay) = self.wheel.next_timeout() {
            let millis = delay.as_millis().min(u32::MAX as u128) as u32;
            std::cmp::min(timeout_ms, millis)
        } else {
            timeout_ms
        }
    }

    pub(crate) fn resolve_user_data(
        &self,
        overlapped: *mut crate::win32::Overlapped,
        success: bool,
        completion_key: usize,
        error_code: Option<u32>,
    ) -> IocpResult<usize> {
        if !overlapped.is_null() {
            // SAFETY: overlapped is non-null and corresponds to a valid OverlappedEntry.
            let id = unsafe { crate::win32::OverlappedId::from_ptr(overlapped) };
            let idx = id.as_usize();
            if idx >= self.ops.local.len() {
                debug!(idx, "Completed index out of bounds");
                return Ok(usize::MAX - 2);
            }
            Ok(idx)
        } else {
            if !success {
                let err = error_code.unwrap_or(0);
                if err == WAIT_TIMEOUT {
                    return Ok(WAKEUP_USER_DATA);
                }
                if completion_key == 0 {
                    return Err(iocp_msg(
                        IocpErrorContext::CompletionWait,
                        format!(
                            "GetQueuedCompletionStatus failed: err={}, key={}, overlapped=null",
                            err, completion_key
                        ),
                    ));
                }
            }
            Ok(completion_key)
        }
    }

    pub(crate) fn process_timers(&mut self) {
        let timer_buffer = std::mem::take(&mut self.timer_buffer);
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
                            Some(self.wheel.insert(user_data, remain));
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
                &self.completion_events,
                &self.completion_table,
                completion_record(completion),
            );
        }
        self.timer_buffer = timer_buffer;
        self.timer_buffer.clear();
    }

    pub(crate) fn finish_timer_op(
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

    pub(crate) fn process_completion(
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
                    completion_events: &self.completion_events,
                    completion_table: &self.completion_table,
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
                let (payload, detail) = completed.take_completion_data();
                push_completion_shared(
                    &self.completion_events,
                    &self.completion_table,
                    completion_record(CompletionSidecar {
                        user_data,
                        generation: slot_generation,
                        res: io_result_to_event_res(&io_result),
                        flags: 0,
                        payload,
                        detail,
                    }),
                );
                self.ops.recycle(user_data, slot_generation.wrapping_add(1));
            }
            _ => {
                debug!(user_data, "Ignoring completion for non in-flight slot");
            }
        }
    }

    #[inline]
    pub(crate) fn with_inflight_slot<R>(
        ops: &mut IocpOpRegistry,
        index: usize,
        f: impl FnOnce(Slot<'_, InFlightWaiting>) -> R,
    ) -> Option<R> {
        match ops.slot_view(index)? {
            SlotView::InFlightWaiting(slot) => Some(f(slot)),
            _ => None,
        }
    }

    pub(crate) fn calculate_io_result(
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
                    io_result = res.map_err(|e| {
                        IocpError::Win32.io_report("iocp.driver.blocking_completion", e)
                    });
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
                    io_result = IocpError::CompletionWait
                        .attach_note("missing blocking result for offloaded file completion");
                } else if let Ok(val) = io_result {
                    io_result = iocp_op
                        .on_complete(val, &self.extensions)
                        .attach_note("IOCP completion hook failed")
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

        if io_result.is_err() {
            let _ = self
                .ops
                .with_slot_storage_mut(user_data, |result, _payload, _sidecar| {
                    *result = Some(Err(IocpError::CompletionWait
                        .report("iocp/driver", "completion without os error")));
                });
        }
        io_result
    }

    pub(crate) fn emit_event_inner(
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
                    detail,
                });
                if !guard.platform_mut().rio_needs_drain || guard.platform_mut().rio_drained {
                    let _ = guard.take_op();
                    let _data = std::mem::take(guard.platform_mut());
                    should_free = true;
                }
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

    pub(crate) fn cancel_op_internal(&mut self, user_data: usize) {
        if !self.ops.contains(user_data) {
            return;
        }

        let emit_ctx = EmitContext {
            completion_events: &self.completion_events,
            completion_table: &self.completion_table,
        };

        let timer_id = self
            .ops
            .get_mut(user_data)
            .and_then(|op| op.platform_data.timer_id);
        if let Some(tid) = timer_id {
            self.wheel.cancel(tid);
            Self::emit_aborted_inner(emit_ctx, user_data, &mut self.ops);
            return;
        }

        let state = self.ops.slot_view(user_data);
        match state {
            Some(SlotView::InFlightOrphaned(_)) => {
                if self.shutting_down {
                    Self::emit_aborted_inner(emit_ctx, user_data, &mut self.ops);
                }
            }
            Some(SlotView::InFlightWaiting(_)) => {
                let ctx = CancelContext {
                    registered_files: &self.registered_files,
                    completion_events: &self.completion_events,
                    completion_table: &self.completion_table,
                };
                if let Some(key) = Self::perform_cancel(ctx, user_data, &mut self.ops) {
                    self.rio_state.release_socket_inflight(key);
                    self.drain_deferred_socket_cleanup();
                }
            }
            _ => {
                Self::emit_aborted_inner(emit_ctx, user_data, &mut self.ops);
            }
        }
    }

    pub(crate) fn perform_cancel(
        ctx: CancelContext<'_>,
        user_data: usize,
        ops: &mut IocpOpRegistry,
    ) -> Option<SocketKey> {
        let mut should_emit_aborted = false;
        let mut aborted_socket_key = None;
        let handled = match ops.slot_view(user_data) {
            Some(SlotView::InFlightWaiting(mut guard)) => {
                let raw_handle = guard
                    .with_op_mut(|iocp_op| iocp_op.header.resolved_handle)
                    .flatten()
                    .or_else(|| {
                        let fd = guard.with_op_mut(|iocp_op| iocp_op.get_fd()).flatten()?;
                        submit::resolve_fd_handle(&fd, ctx.registered_files).ok()
                    });

                if let Some(raw_handle) = raw_handle {
                    let handle = raw_handle.as_handle();
                    let is_rio = guard
                        .with_op_mut(|iocp_op| Self::is_rio_op(iocp_op))
                        .unwrap_or(false);

                    if is_rio {
                        let _ = guard.with_op_mut(|iocp_op| {
                            iocp_op.header.in_flight = false;
                        });
                        should_emit_aborted = true;
                        aborted_socket_key =
                            raw_handle.is_socket().then_some(raw_handle.actor_key());
                    } else {
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
            _ => None,
        };

        if handled.is_none() {
            debug!(user_data, "Skipping cancel for non in-flight slot");
        } else if should_emit_aborted {
            let emit_ctx = EmitContext {
                completion_events: ctx.completion_events,
                completion_table: ctx.completion_table,
            };
            Self::emit_aborted_inner(emit_ctx, user_data, ops);
            return aborted_socket_key;
        }
        None
    }

    pub(crate) fn emit_aborted_inner(
        ctx: EmitContext<'_>,
        user_data: usize,
        ops: &mut IocpOpRegistry,
    ) {
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
