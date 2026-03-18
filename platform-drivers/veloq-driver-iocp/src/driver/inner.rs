use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

use crossbeam_queue::SegQueue;
use tracing::{debug, trace};
use windows_sys::Win32::Foundation::{HANDLE, WAIT_TIMEOUT};
use windows_sys::Win32::System::IO::OVERLAPPED;

use veloq_buf::{BufferRegistrar, NoopRegistrar};
use veloq_driver_core::driver::{
    CompletionTable, RemoteWaker, SharedCompletionQueue, SharedCompletionTable,
};
use veloq_driver_core::op_registry::{OpEntry, OpRegistry};
use veloq_driver_core::slot::{SlotEntry, SlotTable};
use veloq_wheel::{Wheel, WheelConfig};

use crate::common::{
    IocpErrorContext, IocpWaker, WAKEUP_USER_DATA, completion_record, io_error, io_msg,
    io_result_to_event_res, push_completion_shared,
};
use crate::config::{BufferRegistrationMode, IocpConfig};
use crate::driver::{CompletionSidecar, IocpOpState, OpLifecycle};
use crate::ops::slot::{InFlight, Slot};
use crate::ops::{IocpOp, IocpOpPayload, OverlappedEntry, submit};
use crate::rio::RioState;

pub(crate) const RIO_EVENT_KEY: usize = usize::MAX - 1;

pub(crate) type PreInit = crate::common::IoCompletionPort;

struct EmitContext<'a> {
    ops_shared: &'a Arc<SlotTable<IocpOp, OverlappedEntry>>,
    completion_events: &'a SharedCompletionQueue,
    completion_table: &'a SharedCompletionTable,
}

/// The IOCP driver implementation that manages I/O completion ports and operations.
pub struct IocpDriver {
    pub(crate) port: Arc<crate::common::IoCompletionPort>,
    pub(crate) ops: OpRegistry<IocpOp, IocpOpState, OverlappedEntry>,
    pub(crate) extensions: crate::ext::Extensions,
    pub(crate) wheel: Wheel<usize>,
    pub(crate) timer_buffer: Vec<usize>,
    pub(crate) registered_files: Vec<Option<HANDLE>>,
    pub(crate) free_slots: Vec<usize>,
    pub(crate) is_notified: Arc<AtomicBool>,
    pub(crate) completion_events: SharedCompletionQueue,
    pub(crate) completion_table: SharedCompletionTable,

    // RIO Support (required)
    pub(crate) rio_state: RioState,
    pub(crate) registrar: Box<dyn BufferRegistrar>,
    pub(crate) shutting_down: bool,
    pub(crate) closed: bool,
}

impl IocpDriver {
    /// Checks if the provided operation is a RIO-based operation.
    fn is_rio_op(op: &IocpOp) -> bool {
        matches!(
            op.payload,
            IocpOpPayload::Recv(_)
                | IocpOpPayload::Send(_)
                | IocpOpPayload::SendTo(_)
                | IocpOpPayload::UdpRecvStream(_)
                | IocpOpPayload::UdpRefill(_)
        )
    }

    /// Creates a pre-initialization completion port handle.
    pub(crate) fn create_pre_init() -> io::Result<PreInit> {
        crate::common::IoCompletionPort::new(0)
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
        let rio_state = RioState::new(port_handle, entries, &extensions, registration_mode)
            .map_err(|e| {
                io_error(
                    IocpErrorContext::DriverInit,
                    e,
                    format!(
                        "failed to initialize RIO state, entries={entries}, port={port_handle:?}"
                    ),
                )
            })?;
        Ok(Self {
            port: Arc::new(port_val),
            ops: OpRegistry::new(entries as usize),
            extensions,
            wheel: Wheel::new(WheelConfig::default()),
            timer_buffer: Vec::new(),
            registered_files: Vec::new(),
            free_slots: Vec::new(),
            is_notified: Arc::new(AtomicBool::new(false)),
            completion_events: Arc::new(SegQueue::new()),
            completion_table: Arc::new(CompletionTable::new(entries as usize)),
            rio_state,
            registrar: Box::new(NoopRegistrar),
            shutting_down: false,
            closed: false,
        })
    }

    /// Retrieves completion events from the I/O completion port.
    pub(crate) fn get_completion(&mut self, timeout_ms: u32) -> io::Result<()> {
        let wait_ms = self.calculate_wait_ms(timeout_ms);

        trace!(wait_ms, "Entering GetQueuedCompletionStatus");
        let start = Instant::now();
        let status = self.port.get_status(wait_ms);
        let elapsed = start.elapsed();
        self.wheel.advance(elapsed, &mut self.timer_buffer);
        self.process_timers();

        let status = status?;

        match status {
            crate::common::CompletionStatus::Completed {
                bytes,
                key,
                overlapped,
                success,
                error_code,
            } => {
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
            crate::common::CompletionStatus::Timeout => {}
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
        overlapped: *mut OVERLAPPED,
        success: bool,
        completion_key: usize,
        error_code: Option<u32>,
    ) -> io::Result<usize> {
        if !overlapped.is_null() {
            // SAFETY: overlapped is non-null and corresponds to a valid OverlappedEntry.
            let idx = unsafe { OverlappedEntry::user_data_from_ptr(overlapped) };
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
            let in_flight = Slot::<InFlight>::is_in_flight(&self.ops.shared, user_data);
            if let Some(op) = self.ops.local.get_mut(user_data) {
                if in_flight {
                    if let Some(deadline) = op.platform_data.timer_deadline
                        && now < deadline
                    {
                        let remain = deadline.saturating_duration_since(now);
                        op.platform_data.timer_id = Some(self.wheel.insert(user_data, remain));
                        continue;
                    }
                    expired.push(user_data);
                } else {
                    op.platform_data.timer_id = None;
                    op.platform_data.timer_deadline = None;
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
        let op = &mut ops.local[user_data];

        let slot = &ops.shared.slots[user_data];
        let mut guard =
            unsafe { Slot::<InFlight>::assume_in_flight_entry(slot, user_data) }.complete();

        let generation = slot.generation.load(Ordering::Acquire);
        // SAFETY: Slot is being processed for timer expiration; exclusively owned.
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
        let _ = guard.reset();
        let _ = std::mem::take(&mut op.platform_data);
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

        let slot_generation;
        {
            let slot = &self.ops.shared.slots[user_data];
            let op = &mut self.ops.local[user_data];
            slot_generation = slot.generation.load(Ordering::Acquire);

            if op.platform_data.generation != slot_generation {
                debug!(user_data, "Ignoring stale completion");
                return;
            }
            if !Slot::<InFlight>::is_in_flight_entry(slot) {
                debug!(user_data, "Ignoring completion for non in-flight slot");
                return;
            }
        }

        let io_result = {
            let slot = &self.ops.shared.slots[user_data];
            self.calculate_io_result(user_data, slot, success, error_code, bytes_transferred)
        };

        let op = &mut self.ops.local[user_data];
        let slot = &self.ops.shared.slots[user_data];
        let ctx = EmitContext {
            ops_shared: &self.ops.shared,
            completion_events: &self.completion_events,
            completion_table: &self.completion_table,
        };
        Self::emit_event_inner(ctx, user_data, op, slot, slot_generation, io_result);
    }

    fn calculate_io_result(
        &self,
        user_data: usize,
        slot: &SlotEntry<IocpOp, OverlappedEntry>,
        success: bool,
        error_code: Option<u32>,
        bytes_transferred: u32,
    ) -> io::Result<usize> {
        let mut io_result = if !success {
            Err(io::Error::from_raw_os_error(error_code.unwrap_or(0) as i32))
        } else {
            Ok(bytes_transferred as usize)
        };

        let mut guard = unsafe { Slot::<InFlight>::assume_in_flight_entry(slot, user_data) };

        // SAFETY: InFlight state grants sidecar mutable access.
        let blocking_res =
            unsafe { guard.with_sidecar_mut_unchecked(|s| s.blocking_result.take()) };

        // SAFETY: op is checked for presence.
        unsafe {
            guard.with_op_mut_unchecked(|iocp_op: &mut IocpOp| {
                if let Some(res) = blocking_res {
                    io_result = res;
                } else if let Ok(val) = io_result {
                    io_result = iocp_op.on_complete(val, &self.extensions);
                }
            })
        };

        if let Err(e) = &io_result
            && e.raw_os_error().is_none()
        {
            // SAFETY: slot.result.get() is a valid pointer.
            unsafe {
                *slot.result.get() = Some(Err(io::Error::new(e.kind(), e.to_string())));
            }
        }
        io_result
    }

    fn emit_event_inner(
        ctx: EmitContext<'_>,
        user_data: usize,
        op: &mut OpEntry<IocpOpState>,
        slot: &SlotEntry<IocpOp, OverlappedEntry>,
        slot_generation: u32,
        io_result: io::Result<usize>,
    ) {
        if !Slot::<InFlight>::is_in_flight_entry(slot) {
            debug!(user_data, "Received completion for non-InFlight slot");
            return;
        }

        let was_cancelled = matches!(op.platform_data.lifecycle, OpLifecycle::Cancelled);
        let completion_res = io_result_to_event_res(&io_result);
        op.platform_data.rio_pool_waiting = false;

        let mut guard =
            unsafe { Slot::<InFlight>::assume_in_flight_entry(slot, user_data) }.complete();

        if op.platform_data.is_background {
            let _ = guard.take_op();
            let _ = guard.take_completion_data();
            let _data = std::mem::take(&mut op.platform_data);
            ctx.ops_shared.push_free(user_data);
        } else {
            if !(was_cancelled && op.platform_data.rio_needs_drain) {
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
            }
            if !op.platform_data.rio_needs_drain || op.platform_data.rio_drained {
                let _ = guard.take_op();
                let _data = std::mem::take(&mut op.platform_data);
                ctx.ops_shared.push_free(user_data);
            }
        }
    }

    pub(crate) fn cancel_op_internal(&mut self, user_data: usize) {
        if let Some(op) = self.ops.local.get_mut(user_data) {
            trace!(user_data, "Cancelling op");
            let slot = &self.ops.shared.slots[user_data];

            let emit_ctx = EmitContext {
                ops_shared: &self.ops.shared,
                completion_events: &self.completion_events,
                completion_table: &self.completion_table,
            };

            if let Some(tid) = op.platform_data.timer_id {
                self.wheel.cancel(tid);
                Self::emit_aborted_inner(emit_ctx, user_data, op, slot);
                return;
            }

            if matches!(op.platform_data.lifecycle, OpLifecycle::Cancelled) {
                return;
            }
            if Slot::<InFlight>::is_in_flight_entry(slot) {
                op.platform_data.lifecycle = OpLifecycle::Cancelled;
                let ctx = CancelContext {
                    registered_files: &self.registered_files,
                    rio_state: &mut self.rio_state,
                    registrar: &*self.registrar,
                    ops_shared: &self.ops.shared,
                    completion_events: &self.completion_events,
                    completion_table: &self.completion_table,
                };
                Self::perform_cancel(ctx, user_data, op, slot);
            } else {
                Self::emit_aborted_inner(emit_ctx, user_data, op, slot);
            }
        }
    }

    fn perform_cancel(
        ctx: CancelContext<'_>,
        user_data: usize,
        op: &mut OpEntry<IocpOpState>,
        slot: &SlotEntry<IocpOp, OverlappedEntry>,
    ) {
        let guard = unsafe { Slot::<InFlight>::assume_in_flight_entry(slot, user_data) };

        // SAFETY: slot.op.get() is a valid pointer.
        if let Some(res) = unsafe { &mut *slot.op.get() }
            && let Some(fd) = res.get_fd()
            && let Ok(handle) = submit::resolve_fd(fd, ctx.registered_files)
        {
            if op.platform_data.rio_pool_waiting || Self::is_rio_op(res) {
                if op.platform_data.rio_pool_waiting {
                    ctx.rio_state.cancel_udp_recv_waiter(
                        handle,
                        (user_data, op.platform_data.generation),
                        ctx.registrar,
                    );
                }
                let emit_ctx = EmitContext {
                    ops_shared: ctx.ops_shared,
                    completion_events: ctx.completion_events,
                    completion_table: ctx.completion_table,
                };
                Self::emit_aborted_inner(emit_ctx, user_data, op, slot);
                return;
            }

            // SAFETY: `overlapped_ptr()` provides a valid overlapped pointer for `cancel_request`.
            let overlapped_ptr = guard.overlapped_ptr();
            let _ =
                unsafe { crate::common::IoCompletionPort::cancel_request(handle, overlapped_ptr) };
        }
    }

    fn emit_aborted_inner(
        ctx: EmitContext<'_>,
        user_data: usize,
        op: &mut OpEntry<IocpOpState>,
        slot: &SlotEntry<IocpOp, OverlappedEntry>,
    ) {
        let generation = slot.generation.load(Ordering::Acquire);
        let (payload, detail) = if Slot::<InFlight>::is_in_flight_entry(slot) {
            let mut guard =
                unsafe { Slot::<InFlight>::assume_in_flight_entry(slot, user_data) }.complete();
            let _ = guard.take_op();
            let data = guard.take_completion_data();
            let _ = guard.reset();
            data
        } else {
            unsafe {
                let _ = (*slot.op.get()).take();
                let payload = (*slot.payload.get()).take();
                let detail = (*slot.result.get()).take();
                slot.reset(generation.wrapping_add(1));
                (payload, detail)
            }
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
        let _data = std::mem::take(&mut op.platform_data);
        ctx.ops_shared.push_free(user_data);
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
    registered_files: &'a [Option<HANDLE>],
    rio_state: &'a mut RioState,
    registrar: &'a dyn BufferRegistrar,
    ops_shared: &'a Arc<SlotTable<IocpOp, OverlappedEntry>>,
    completion_events: &'a SharedCompletionQueue,
    completion_table: &'a SharedCompletionTable,
}
