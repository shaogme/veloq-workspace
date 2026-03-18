use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

use crossbeam_queue::SegQueue;
use tracing::{debug, trace};
use windows_sys::Win32::Foundation::{
    GetLastError, HANDLE, INVALID_HANDLE_VALUE, WAIT_TIMEOUT,
};
use windows_sys::Win32::System::IO::{
    CreateIoCompletionPort, GetQueuedCompletionStatus, OVERLAPPED,
    PostQueuedCompletionStatus,
};

use veloq_buf::{BufferRegistrar, NoopRegistrar};
use veloq_driver_core::driver::{
    CompletionTable, RemoteWaker, SharedCompletionQueue, SharedCompletionTable,
};
use veloq_driver_core::op_registry::{OpEntry, OpRegistry};
use veloq_driver_core::slot::SlotTable;
use veloq_wheel::{Wheel, WheelConfig};

use crate::config::{BufferRegistrationMode, IocpConfig};
use crate::common::{
    CompletionPort, IocpErrorContext, WAKEUP_USER_DATA, completion_record, io_error, io_msg,
    io_result_to_event_res, push_completion_event_shared, IocpWaker,
};
use crate::ops::{IocpOp, OverlappedEntry, submit};
use crate::rio::RioState;
use crate::driver::{IocpOpState, OpLifecycle, CompletionSidecar};

pub(crate) const RIO_EVENT_KEY: usize = usize::MAX - 1;

pub(crate) type PreInit = usize;

struct EmitContext<'a> {
    ops_shared: &'a Arc<SlotTable<IocpOp, OverlappedEntry>>,
    completion_events: &'a SharedCompletionQueue,
    completion_table: &'a SharedCompletionTable,
}

/// The IOCP driver implementation that manages I/O completion ports and operations.
pub struct IocpDriver {
    pub(crate) port: Arc<CompletionPort>,
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
        // SAFETY: The vtable pointer is guaranteed to be valid and point to a valid OpVTable.
        let s = unsafe { op.vtable.as_ref().submit as *const () as usize };
        s == submit::submit_recv as *const () as usize
            || s == submit::submit_send as *const () as usize
            || s == submit::submit_send_to as *const () as usize
            || s == submit::submit_udp_recv_stream as *const () as usize
            || s == submit::submit_udp_refill as *const () as usize
    }

    /// Creates a pre-initialization completion port handle.
    pub(crate) fn create_pre_init() -> io::Result<PreInit> {
        // SAFETY: `INVALID_HANDLE_VALUE` is a valid argument for creating a new completion port.
        let port =
            unsafe { CreateIoCompletionPort(INVALID_HANDLE_VALUE, std::ptr::null_mut(), 0, 0) };
        if port.is_null() {
            Err(io::Error::last_os_error())
        } else {
            Ok(port as usize)
        }
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
        let port = port_val as HANDLE;
        debug!(port = ?port, "Initializing IocpDriver");
        let extensions = crate::ext::Extensions::new().map_err(|e| {
            io_error(
                IocpErrorContext::DriverInit,
                e,
                format!("failed to load IOCP extensions, port={port:?}"),
            )
        })?;
        let rio_state =
            RioState::new(port, entries, &extensions, registration_mode).map_err(|e| {
                io_error(
                    IocpErrorContext::DriverInit,
                    e,
                    format!("failed to initialize RIO state, entries={entries}, port={port:?}"),
                )
            })?;
        Ok(Self {
            port: Arc::new(CompletionPort { handle: port }),
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
        let mut bytes_transferred = 0;
        let mut completion_key = 0;
        let mut overlapped = std::ptr::null_mut();

        let wait_ms = self.calculate_wait_ms(timeout_ms);

        trace!(wait_ms, "Entering GetQueuedCompletionStatus");
        let start = Instant::now();
        // SAFETY: The `self.port.handle` is a valid, open I/O completion port handle.
        // Pointers for `bytes_transferred`, `completion_key`, and `overlapped` are valid for mutation by the OS.
        let res = unsafe {
            GetQueuedCompletionStatus(
                self.port.handle,
                &mut bytes_transferred,
                &mut completion_key,
                &mut overlapped,
                wait_ms,
            )
        };
        let elapsed = start.elapsed();

        self.wheel.advance(elapsed, &mut self.timer_buffer);
        self.process_timers();

        if completion_key == RIO_EVENT_KEY {
            self.rio_state.process_completions(
                &mut self.ops,
                &*self.registrar,
                &self.completion_events,
                &self.completion_table,
            )?;
            return Ok(());
        }

        let user_data = self.resolve_user_data(overlapped, res, completion_key, wait_ms)?;

        if user_data == WAKEUP_USER_DATA {
            self.is_notified.store(false, Ordering::Release);
            trace!("Wakeup received");
            return Ok(());
        }

        trace!(user_data, bytes_transferred, "CQE received");
        self.process_completion(user_data, res, bytes_transferred);
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
        res: i32,
        completion_key: usize,
        wait_ms: u32,
    ) -> io::Result<usize> {
        if !overlapped.is_null() {
            // SAFETY: overlapped is non-null and corresponds to a valid OverlappedEntry.
            let idx = unsafe { (*(overlapped as *const OverlappedEntry)).user_data };
            if idx >= self.ops.local.len() {
                debug!(idx, "Completed index out of bounds");
                return Ok(usize::MAX - 2);
            }
            Ok(idx)
        } else {
            if res == 0 {
                // SAFETY: GetLastError is safe to call after failure.
                let err = unsafe { GetLastError() };
                if err == WAIT_TIMEOUT {
                    return Ok(WAKEUP_USER_DATA);
                }
                if completion_key == 0 {
                    return Err(io_msg(
                        IocpErrorContext::CompletionWait,
                        format!(
                            "GetQueuedCompletionStatus failed: err={}, wait_ms={}, key={}, overlapped=null",
                            err, wait_ms, completion_key
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
            if let Some(op) = self.ops.local.get_mut(user_data) {
                if matches!(op.platform_data.lifecycle, OpLifecycle::InFlight) {
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
            push_completion_event_shared(
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
        op.platform_data.lifecycle = OpLifecycle::Completed;

        let slot = &ops.shared.slots[user_data];
        let generation = slot.generation.load(Ordering::Acquire);
        // SAFETY: slot.op.get() is a valid pointer for the lifetime of the SlotEntry.
        let _ = unsafe { (*slot.op.get()).take() };
        // SAFETY: slot.payload.get() is a valid pointer and contains the payload.
        let payload = unsafe { (*slot.payload.get()).take() };
        // SAFETY: slot.result.get() is a valid pointer and contains the result.
        let detail = unsafe { (*slot.result.get()).take() };
        pending_events.push(CompletionSidecar {
            user_data,
            generation,
            res: 0,
            flags: 0,
            payload,
            detail,
        });
        let _ = std::mem::take(&mut op.platform_data);
        ops.shared.push_free(user_data);
    }

    pub(crate) fn process_completion(
        &mut self,
        user_data: usize,
        res: i32,
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
        }

        let io_result = {
            let slot = &self.ops.shared.slots[user_data];
            self.calculate_io_result(slot, res, bytes_transferred)
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
        slot: &SlotTableEntry<IocpOp, OverlappedEntry>,
        res: i32,
        bytes_transferred: u32,
    ) -> io::Result<usize> {
        let mut io_result = if res == 0 {
            // SAFETY: Calling GetLastError to get the reason for failure.
            Err(io::Error::from_raw_os_error(
                unsafe { GetLastError() } as i32
            ))
        } else {
            Ok(bytes_transferred as usize)
        };
        // SAFETY: slot.op.get() is a valid pointer.
        if let Some(iocp_op) = unsafe { &mut *slot.op.get() } {
            // SAFETY: slot.sidecar.get() is a valid pointer.
            let slot_overlapped = unsafe { &mut *slot.sidecar.get() };
            if let Some(blocking_res) = slot_overlapped.blocking_result.take() {
                io_result = blocking_res;
            } else if let Ok(val) = io_result {
                // SAFETY: vtable pointer is valid and on_complete is a valid function pointer if present.
                if let Some(on_comp) = unsafe { iocp_op.vtable.as_ref().on_complete } {
                    // SAFETY: on_comp is a valid function pointer.
                    io_result = unsafe { (on_comp)(iocp_op, val, &self.extensions) };
                }
            }
        }
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
        slot: &SlotTableEntry<IocpOp, OverlappedEntry>,
        slot_generation: u32,
        io_result: io::Result<usize>,
    ) {
        match op.platform_data.lifecycle {
            OpLifecycle::Cancelled | OpLifecycle::InFlight => {
                let was_cancelled = matches!(op.platform_data.lifecycle, OpLifecycle::Cancelled);
                let completion_res = io_result_to_event_res(&io_result);
                op.platform_data.rio_pool_waiting = false;

                if op.platform_data.is_background {
                    // SAFETY: Cleaning up background op slots.
                    unsafe {
                        (*slot.op.get()).take();
                        (*slot.payload.get()).take();
                        (*slot.result.get()).take();
                    }
                    let _data = std::mem::take(&mut op.platform_data);
                    ctx.ops_shared.push_free(user_data);
                } else {
                    op.platform_data.lifecycle = OpLifecycle::Completed;
                    if !(was_cancelled && op.platform_data.rio_needs_drain) {
                        // SAFETY: Taking payload and result for event emission.
                        let (payload, detail) =
                            unsafe { ((*slot.payload.get()).take(), (*slot.result.get()).take()) };
                        push_completion_event_shared(
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
                        // SAFETY: Final cleanup of op slot.
                        unsafe { (*slot.op.get()).take() };
                        let _data = std::mem::take(&mut op.platform_data);
                        ctx.ops_shared.push_free(user_data);
                    }
                }
            }
            _ => debug!(user_data, "Received completion for non-InFlight op"),
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

            match op.platform_data.lifecycle {
                OpLifecycle::Pending => Self::emit_aborted_inner(emit_ctx, user_data, op, slot),
                OpLifecycle::InFlight => {
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
                }
                _ => {}
            }
        }
    }

    fn perform_cancel(
        ctx: CancelContext<'_>,
        user_data: usize,
        op: &mut OpEntry<IocpOpState>,
        slot: &SlotTableEntry<IocpOp, OverlappedEntry>,
    ) {
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

            // SAFETY: `slot.sidecar.get()` provides a valid overlapped pointer for `CancelIoEx`.
            let overlapped_ptr = unsafe { &mut (*slot.sidecar.get()).inner as *mut _ };
            // SAFETY: Calling Win32 API to cancel asynchronous I/O.
            unsafe {
                windows_sys::Win32::System::IO::CancelIoEx(handle, overlapped_ptr);
            }
        }
    }

    fn emit_aborted_inner(
        ctx: EmitContext<'_>,
        user_data: usize,
        op: &mut OpEntry<IocpOpState>,
        slot: &SlotTableEntry<IocpOp, OverlappedEntry>,
    ) {
        op.platform_data.lifecycle = OpLifecycle::Completed;
        let generation = slot.generation.load(Ordering::Acquire);
        // SAFETY: slot.payload.get() and slot.result.get() are valid pointers.
        let (payload, detail) =
            unsafe { ((*slot.payload.get()).take(), (*slot.result.get()).take()) };
        push_completion_event_shared(
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
        // SAFETY: slot.op.get() is a valid pointer.
        unsafe { (*slot.op.get()).take() };
        let _data = std::mem::take(&mut op.platform_data);
        ctx.ops_shared.push_free(user_data);
    }

    pub(crate) fn wake(&self) -> io::Result<()> {
        // SAFETY: `self.port.handle` is valid and used to post a wakeup status.
        let res = unsafe {
            PostQueuedCompletionStatus(self.port.handle, 0, WAKEUP_USER_DATA, std::ptr::null_mut())
        };
        if res == 0 {
            // SAFETY: Calling GetLastError to get the reason for failure.
            return Err(io::Error::from_raw_os_error(
                unsafe { GetLastError() } as i32
            ));
        }
        Ok(())
    }

    pub(crate) fn create_waker(&self) -> Arc<dyn RemoteWaker> {
        Arc::new(IocpWaker {
            port: self.port.clone(),
            is_notified: self.is_notified.clone(),
        })
    }
}

type SlotTableEntry<Op, Sidecar> = veloq_driver_core::slot::SlotEntry<Op, Sidecar>;

struct CancelContext<'a> {
    registered_files: &'a [Option<HANDLE>],
    rio_state: &'a mut RioState,
    registrar: &'a dyn BufferRegistrar,
    ops_shared: &'a Arc<SlotTable<IocpOp, OverlappedEntry>>,
    completion_events: &'a SharedCompletionQueue,
    completion_table: &'a SharedCompletionTable,
}
