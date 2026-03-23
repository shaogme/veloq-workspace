use std::collections::VecDeque;
use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

use crossbeam_queue::SegQueue;
use tracing::{debug, trace};
use windows_sys::Win32::Foundation::WAIT_TIMEOUT;

use veloq_buf::{BufferRegistrar, NoopRegistrar};
use veloq_driver_core::driver::{
    Driver, RemoteWaker, SharedCompletionQueue, SharedCompletionTable,
};
use veloq_driver_core::op_registry::OpRegistry;
use veloq_wheel::{Wheel, WheelConfig};

use crate::common::{
    IocpErrorContext, IocpWaker, WAKEUP_USER_DATA, completion_record, io_error, io_msg,
    io_result_to_event_res, push_completion_shared,
};
use crate::config::{BufferRegistrationMode, IocpConfig, RawHandle};
use crate::driver::{CompletionSidecar, IocpOpState};
use crate::ops::slot::Slot;
use crate::ops::{IocpOp, IocpOpPayload, OverlappedEntry, submit};
use crate::rio::{RioState, SocketActorKey};
use crate::win32::Overlapped;
use veloq_driver_core::slot::{DetachedCancelTable, InFlightWaiting, SlotRegistryExt, SlotView};

pub(crate) const RIO_EVENT_KEY: usize = usize::MAX - 1;
pub(crate) const CONTROL_EVENT_KEY: usize = usize::MAX - 2;

pub(crate) type PreInit = crate::win32::IoCompletionPort;

struct EmitContext<'a> {
    completion_events: &'a SharedCompletionQueue,
    completion_table: &'a SharedCompletionTable,
}

pub(crate) struct DeferredSocketCleanup {
    pub(crate) handle: RawHandle,
    pub(crate) registered_fd: Option<crate::config::IoFd>,
}

/// The IOCP driver implementation that manages I/O completion ports and operations.
pub struct IocpDriver {
    pub(crate) port: Arc<crate::win32::IoCompletionPort>,
    pub(crate) ops: OpRegistry<IocpOp, IocpOpState, OverlappedEntry>,
    pub(crate) extensions: crate::ext::Extensions,
    pub(crate) wheel: Wheel<usize>,
    pub(crate) timer_buffer: Vec<usize>,
    pub(crate) registered_files: Vec<Option<RawHandle>>,
    pub(crate) free_slots: Vec<usize>,
    pub(crate) is_notified: Arc<AtomicBool>,
    pub(crate) completion_events: SharedCompletionQueue,
    pub(crate) completion_table: SharedCompletionTable,
    pub(crate) detached_cancel_table: Arc<DetachedCancelTable>,

    // RIO Support (required)
    pub(crate) rio_state: RioState,
    pub(crate) registrar: Box<dyn BufferRegistrar>,
    pub(crate) shutting_down: bool,
    pub(crate) closed: bool,
    pub(crate) deferred_socket_cleanup: VecDeque<DeferredSocketCleanup>,
}

impl IocpDriver {
    /// Checks if the provided operation is a RIO-based operation.
    fn is_rio_op(op: &IocpOp) -> bool {
        matches!(
            op.payload,
            IocpOpPayload::Recv(_)
                | IocpOpPayload::Send(_)
                | IocpOpPayload::UdpRecv(_)
                | IocpOpPayload::UdpSend(_)
                | IocpOpPayload::SendTo(_)
                | IocpOpPayload::UdpRecvStream(_)
        )
    }

    /// Creates a pre-initialization completion port handle.
    pub(crate) fn create_pre_init() -> io::Result<PreInit> {
        crate::win32::IoCompletionPort::new(0)
    }

    /// Creates a new IOCP driver instance.
    pub fn new(config: impl AsRef<IocpConfig>) -> io::Result<Self> {
        let cfg = config.as_ref();
        let pre = Self::create_pre_init()?;
        Self::new_from_pre_init(cfg.entries.get(), pre, cfg.registration_mode)
    }

    /// Creates a new IOCP driver from a pre-initialized handle.
    pub(crate) fn new_from_pre_init(
        entries: u32,
        port_val: PreInit,
        registration_mode: BufferRegistrationMode,
    ) -> io::Result<Self> {
        let port_handle = port_val.as_raw();
        debug!(port = ?port_handle, "Initializing IocpDriver");
        let extensions = crate::ext::Extensions::new().map_err(|e| {
            io_error(
                IocpErrorContext::DriverInit,
                e,
                format!("failed to load IOCP extensions, port={port_handle:?}"),
            )
        })?;
        let rio_state = RioState::new(
            crate::config::RawHandle::for_file(port_handle).borrow(),
            entries,
            &extensions,
            registration_mode,
        )
        .map_err(|e| {
            use crate::rio::error::RioReportExt;
            e.to_io_error(format!(
                "failed to initialize RIO state, entries={entries}, port={port_handle:?}"
            ))
        })?;
        let ops = OpRegistry::new(entries as usize);
        let completion_table: SharedCompletionTable = ops.shared.clone();
        Ok(Self {
            port: Arc::new(port_val),
            ops,
            extensions,
            wheel: Wheel::new(WheelConfig::default()),
            timer_buffer: Vec::new(),
            registered_files: Vec::new(),
            free_slots: Vec::new(),
            is_notified: Arc::new(AtomicBool::new(false)),
            completion_events: Arc::new(SegQueue::new()),
            completion_table,
            detached_cancel_table: Arc::new(DetachedCancelTable::new(entries as usize)),
            rio_state,
            registrar: Box::new(NoopRegistrar),
            shutting_down: false,
            closed: false,
            deferred_socket_cleanup: VecDeque::new(),
        })
    }

    /// Retrieves completion events from the I/O completion port.
    pub(crate) fn get_completion(&mut self, timeout_ms: u32) -> io::Result<()> {
        self.drain_cancel_requests();
        let wait_ms = self.calculate_wait_ms(timeout_ms);

        trace!(wait_ms, "Entering GetQueuedCompletionStatus");
        let start = Instant::now();
        let status = self.port.get_status(wait_ms);
        let elapsed = start.elapsed();
        self.wheel.advance(elapsed, &mut self.timer_buffer);
        self.process_timers();

        let status = status?;

        match status {
            crate::win32::CompletionStatus::Completed {
                bytes,
                key,
                overlapped,
                success,
                error_code,
            } => {
                if key == CONTROL_EVENT_KEY {
                    self.handle_control_completion(overlapped);
                    return Ok(());
                }
                if key == RIO_EVENT_KEY {
                    self.rio_state.process_completions(
                        &mut self.ops,
                        &*self.registrar,
                        &self.completion_events,
                        &self.completion_table,
                    )?;
                    return Ok(());
                }

                let user_data = self.resolve_user_data(overlapped, success, key, error_code)?;

                if user_data == WAKEUP_USER_DATA {
                    self.is_notified.store(false, Ordering::Release);
                    trace!("Wakeup received");
                    return Ok(());
                }
                trace!(user_data, bytes, "CQE received");
                self.process_completion(user_data, success, error_code, bytes);
            }
            crate::win32::CompletionStatus::Timeout => {}
        }
        Ok(())
    }

    fn calculate_wait_ms(&self, timeout_ms: u32) -> u32 {
        if let Some(delay) = self.wheel.next_timeout() {
            let millis = delay.as_millis().min(u32::MAX as u128) as u32;
            std::cmp::min(timeout_ms, millis)
        } else {
            timeout_ms
        }
    }

    fn resolve_user_data(
        &self,
        overlapped: *mut Overlapped,
        success: bool,
        completion_key: usize,
        error_code: Option<u32>,
    ) -> io::Result<usize> {
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
                    return Err(io_msg(
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

    fn process_timers(&mut self) {
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

    fn finish_timer_op(
        ops: &mut OpRegistry<IocpOp, IocpOpState, OverlappedEntry>,
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
        let mut pending = guard.reset();
        let _ = std::mem::take(pending.platform_mut());
        ops.shared.push_free(user_data);
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
                let (payload, detail) = completed.take_completion_data();
                let _ = completed.take_op();
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
                self.ops.remove(user_data);
            }
            _ => {
                debug!(user_data, "Ignoring completion for non in-flight slot");
            }
        }
    }

    #[inline]
    fn with_inflight_slot<R>(
        ops: &mut OpRegistry<IocpOp, IocpOpState, OverlappedEntry>,
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
    ) -> io::Result<usize> {
        let mut io_result = if !success {
            Err(io::Error::from_raw_os_error(error_code.unwrap_or(0) as i32))
        } else {
            Ok(bytes_transferred as usize)
        };

        let processed = Self::with_inflight_slot(&mut self.ops, user_data, |mut guard| {
            // SAFETY: InFlight state grants sidecar mutable access.
            let blocking_res = unsafe { guard.sidecar_unchecked(|s| s.blocking_result.take()) };

            let _ = guard.with_op_mut(|iocp_op: &mut IocpOp| {
                if let Some(res) = blocking_res {
                    io_result = res;
                } else if let Ok(val) = io_result {
                    io_result = iocp_op.on_complete(val, &self.extensions);
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

        if let Err(e) = &io_result
            && e.raw_os_error().is_none()
        {
            let _ = self
                .ops
                .with_slot_storage_mut(user_data, |_op, result, _payload, _sidecar| {
                    *result = Some(Err(io::Error::new(e.kind(), e.to_string())));
                });
        }
        io_result
    }

    fn emit_event_inner(
        ctx: EmitContext<'_>,
        ops: &mut OpRegistry<IocpOp, IocpOpState, OverlappedEntry>,
        user_data: usize,
        slot_generation: u32,
        io_result: io::Result<usize>,
    ) {
        let mut should_free = false;
        let handled = Self::with_inflight_slot(ops, user_data, |mut guard| {
            let completion_res = io_result_to_event_res(&io_result);
            guard.platform_mut().rio_pool_waiting = false;
            let mut guard = guard.complete();

            if guard.platform_mut().is_background {
                let _ = guard.take_op();
                let _ = guard.take_completion_data();
                let _data = std::mem::take(guard.platform_mut());
                should_free = true;
            } else {
                let (payload, detail) = guard.take_completion_data();
                push_completion_shared(
                    ctx.completion_events,
                    ctx.completion_table,
                    completion_record(CompletionSidecar {
                        user_data,
                        generation: slot_generation,
                        res: completion_res,
                        flags: 0,
                        payload,
                        detail,
                    }),
                );
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
            ops.shared.push_free(user_data);
        }
    }

    pub(crate) fn cancel_op_internal(&mut self, user_data: usize) {
        if !self.ops.contains(user_data) {
            return;
        }

        trace!(user_data, "Cancelling op");
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
            Some(SlotView::InFlightOrphaned(_)) => {}
            Some(SlotView::InFlightWaiting(_)) => {
                let ctx = CancelContext {
                    registered_files: &self.registered_files,
                    rio_state: &mut self.rio_state,
                    registrar: &*self.registrar,
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

    fn perform_cancel(
        ctx: CancelContext<'_>,
        user_data: usize,
        ops: &mut OpRegistry<IocpOp, IocpOpState, OverlappedEntry>,
    ) -> Option<SocketActorKey> {
        let mut should_emit_aborted = false;
        let mut aborted_socket_key = None;
        let handled = match ops.slot_view(user_data) {
            Some(SlotView::InFlightWaiting(mut guard)) => {
                let raw_handle = guard
                    .with_op_mut(|iocp_op| iocp_op.header.resolved_handle)
                    .flatten()
                    .or_else(|| {
                        let fd = guard.with_op_mut(|iocp_op| iocp_op.get_fd()).flatten()?;
                        submit::resolve_fd(fd, ctx.registered_files).ok()
                    });

                if let Some(raw_handle) = raw_handle {
                    let handle = raw_handle.handle;
                    let is_rio = guard
                        .with_op_mut(|iocp_op| Self::is_rio_op(iocp_op))
                        .unwrap_or(false);

                    if guard.platform_mut().rio_pool_waiting || is_rio {
                        if guard.platform_mut().rio_pool_waiting {
                            ctx.rio_state.cancel_udp_waiter(
                                raw_handle.actor_key(),
                                (user_data, guard.platform_mut().generation),
                                ctx.registrar,
                            );
                        }
                        should_emit_aborted = true;
                        aborted_socket_key =
                            (raw_handle.generation != 0).then_some(raw_handle.actor_key());
                    } else {
                        // SAFETY: `guard.storage` exposes the overlapped entry for this cancelled slot.
                        let overlapped_ptr =
                            guard.storage.with_mut(|_op, _result, _payload, sidecar| {
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

    fn emit_aborted_inner(
        ctx: EmitContext<'_>,
        user_data: usize,
        ops: &mut OpRegistry<IocpOp, IocpOpState, OverlappedEntry>,
    ) {
        let generation = ops.shared.slots[user_data].generation(Ordering::Acquire);
        let inflight = Self::with_inflight_slot(ops, user_data, |guard| {
            let mut guard = guard.complete();
            let _ = guard.take_op();
            let data = guard.take_completion_data();
            let _ = guard.reset();
            data
        });
        let was_inflight = inflight.is_some();

        let (payload, detail) = if let Some(data) = inflight {
            data
        } else {
            ops.with_slot_storage_mut(user_data, |_op, result, payload, _sidecar| {
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

        if was_inflight {
            ops.shared.push_free(user_data);
        } else {
            ops.recycle(user_data, generation.wrapping_add(1));
        }
    }

    pub(crate) fn wake(&self) -> io::Result<()> {
        // SAFETY: we are posting a null overlapped pointer for wakeup.
        unsafe { self.port.post(0, WAKEUP_USER_DATA, std::ptr::null_mut()) }
    }

    pub(crate) fn create_waker(&self) -> Arc<dyn RemoteWaker> {
        Arc::new(IocpWaker {
            port: self.port.clone(),
            is_notified: self.is_notified.clone(),
        })
    }
}

struct CancelContext<'a> {
    registered_files: &'a [Option<RawHandle>],
    rio_state: &'a mut RioState,
    registrar: &'a dyn BufferRegistrar,
    completion_events: &'a SharedCompletionQueue,
    completion_table: &'a SharedCompletionTable,
}
