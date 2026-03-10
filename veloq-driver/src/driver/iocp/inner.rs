use super::CompletionSidecar;
use super::error::{IocpErrorContext, io_error, io_msg};
use super::ext::Extensions;
use super::rio::RioState;
use super::submit;
use crate::config::IocpConfig;
use crate::driver::iocp::op::IocpOp;
use crate::driver::op_registry::OpRegistry;
use crate::driver::slot::OverlappedEntry;
use crate::driver::{
    CompletionEvent, CompletionRecord, RemoteWaker, SharedCompletionQueue, SharedCompletionTable,
    encode_completion_token,
};

use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};
use tracing::{debug, trace};

use windows_sys::Win32::Foundation::{GetLastError, HANDLE, INVALID_HANDLE_VALUE, WAIT_TIMEOUT};
use windows_sys::Win32::System::IO::{
    CreateIoCompletionPort, GetQueuedCompletionStatus, PostQueuedCompletionStatus,
};

use veloq_wheel::{TaskId, Wheel, WheelConfig};

pub(crate) const WAKEUP_USER_DATA: usize = usize::MAX;
pub(crate) const RIO_EVENT_KEY: usize = usize::MAX - 1;

#[derive(Debug)]
pub enum OpLifecycle {
    /// Created, resources attached, waiting to be submitted
    Pending,
    /// Submitted to true OS operations (IOCP/RIO)
    InFlight,
    /// Completion received or Timer fired
    Completed,
    /// Cancelled by user
    Cancelled,
}

pub struct IocpOpState {
    pub(crate) generation: u32,
    pub(crate) lifecycle: OpLifecycle,
    pub(crate) timer_id: Option<TaskId>,
    pub(crate) timer_deadline: Option<Instant>,
    pub(crate) is_background: bool,
    // For RIO cancel path: the slot can be recycled only after both:
    // 1) user has consumed completion; 2) late RIO CQE has been drained.
    pub(crate) rio_needs_drain: bool,
    pub(crate) rio_drained: bool,
    // recv_from served by internal RIO UDP pre-post pool; no per-op kernel I/O in flight.
    pub(crate) rio_pool_waiting: bool,
}

impl Default for IocpOpState {
    fn default() -> Self {
        Self {
            generation: 0,
            lifecycle: OpLifecycle::Pending,
            timer_id: None,
            timer_deadline: None,
            is_background: false,
            rio_needs_drain: false,
            rio_drained: false,
            rio_pool_waiting: false,
        }
    }
}

impl IocpOpState {
    pub(crate) fn new() -> Self {
        Self::default()
    }
}

pub(crate) type PreInit = usize;

#[derive(Clone, Copy, Debug)]
pub enum CloseMode {
    Fast,
    Strict { timeout: Duration },
}

pub struct IocpDriver {
    pub(crate) port: Arc<CompletionPort>,
    pub(crate) ops: OpRegistry<IocpOp, IocpOpState>,
    pub(crate) extensions: Extensions,
    pub(crate) wheel: Wheel<usize>,
    pub(crate) timer_buffer: Vec<usize>,
    pub(crate) registered_files: Vec<Option<HANDLE>>,
    pub(crate) free_slots: Vec<usize>,
    pub(crate) is_waked: Arc<AtomicBool>,
    pub(crate) completion_events: SharedCompletionQueue,
    pub(crate) completion_table: SharedCompletionTable,

    // RIO Support (required)
    pub(crate) rio_state: RioState,
    pub(crate) registrar: Box<dyn veloq_buf::BufferRegistrar>,
    pub(crate) shutting_down: bool,
    pub(crate) closed: bool,
}

pub struct CompletionPort {
    pub(crate) handle: HANDLE,
}

impl Drop for CompletionPort {
    fn drop(&mut self) {
        unsafe {
            windows_sys::Win32::Foundation::CloseHandle(self.handle);
        }
    }
}

unsafe impl Send for CompletionPort {}
unsafe impl Sync for CompletionPort {}

pub(crate) struct IocpWaker {
    pub(crate) port: Arc<CompletionPort>,
    pub(crate) is_waked: Arc<AtomicBool>,
}

impl RemoteWaker for IocpWaker {
    fn wake(&self) -> io::Result<()> {
        if self.is_waked.load(Ordering::Relaxed) {
            return Ok(());
        }
        if !self.is_waked.swap(true, Ordering::AcqRel) {
            let res = unsafe {
                PostQueuedCompletionStatus(
                    self.port.handle,
                    0,
                    WAKEUP_USER_DATA,
                    std::ptr::null_mut(),
                )
            };
            if res == 0 {
                return Err(io::Error::last_os_error());
            }
        }
        Ok(())
    }
}

impl IocpDriver {
    fn is_rio_op(op: &IocpOp) -> bool {
        let submit_fn = unsafe { op.vtable.as_ref().submit as *const () as usize };
        submit_fn == crate::driver::iocp::submit::submit_recv as *const () as usize
            || submit_fn == crate::driver::iocp::submit::submit_send as *const () as usize
            || submit_fn == crate::driver::iocp::submit::submit_send_to as *const () as usize
            || submit_fn
                == crate::driver::iocp::submit::submit_udp_recv_stream as *const () as usize
            || submit_fn == crate::driver::iocp::submit::submit_udp_refill as *const () as usize
    }

    pub(crate) fn create_pre_init() -> io::Result<PreInit> {
        // Create a new completion port.
        let port =
            unsafe { CreateIoCompletionPort(INVALID_HANDLE_VALUE, std::ptr::null_mut(), 0, 0) };

        if port.is_null() {
            Err(io::Error::last_os_error())
        } else {
            Ok(port as usize)
        }
    }

    pub fn new(config: impl AsRef<IocpConfig>) -> io::Result<Self> {
        let pre = Self::create_pre_init()?;
        Self::new_from_pre_init(config.as_ref().entries.get(), pre)
    }

    pub(crate) fn new_from_pre_init(entries: u32, port_val: PreInit) -> io::Result<Self> {
        let port = port_val as HANDLE;
        debug!(port = ?port, "Initializing IocpDriver");
        // Load extensions
        let extensions = Extensions::new().map_err(|e| {
            io_error(
                IocpErrorContext::DriverInit,
                e,
                format!("failed to load IOCP extensions, port={port:?}"),
            )
        })?;

        // Initialize RIO State
        let rio_state = RioState::new(port, entries, &extensions).map_err(|e| {
            io_error(
                IocpErrorContext::DriverInit,
                e,
                format!("failed to initialize RIO state, entries={entries}, port={port:?}"),
            )
        })?;

        // Changed from with_capacity to new
        let ops = OpRegistry::new(entries as usize);

        let is_waked = Arc::new(AtomicBool::new(false));

        Ok(Self {
            port: Arc::new(CompletionPort { handle: port }),
            ops,
            extensions,
            wheel: Wheel::new(WheelConfig::default()),
            timer_buffer: Vec::new(),
            registered_files: Vec::new(),
            free_slots: Vec::new(),
            is_waked,
            completion_events: std::sync::Arc::new(crossbeam_queue::SegQueue::new()),
            completion_table: std::sync::Arc::new(crate::driver::CompletionTable::new(
                entries as usize,
            )),
            rio_state,
            registrar: Box::new(veloq_buf::NoopRegistrar),
            shutting_down: false,
            closed: false,
        })
    }

    /// Retrieve completion events from the port.
    /// timeout_ms: 0 for poll, u32::MAX for wait.
    pub(crate) fn get_completion(&mut self, timeout_ms: u32) -> io::Result<()> {
        let mut bytes_transferred = 0;
        let mut completion_key = 0;
        let mut overlapped = std::ptr::null_mut();

        // Calculate timeout based on wheel
        let mut wait_ms = timeout_ms;
        if let Some(delay) = self.wheel.next_timeout() {
            let millis = delay.as_millis().min(u32::MAX as u128) as u32;
            wait_ms = std::cmp::min(wait_ms, millis);
        }

        trace!(wait_ms, "Entering GetQueuedCompletionStatus");
        let start = Instant::now();
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

        // Process expired timers
        self.wheel.advance(elapsed, &mut self.timer_buffer);
        self.process_timer_completions();

        if completion_key == RIO_EVENT_KEY {
            self.rio_state.process_completions(
                &mut self.ops,
                &*self.registrar,
                &self.completion_events,
                &self.completion_table,
            )?;
            return Ok(());
        }

        let user_data = if !overlapped.is_null() {
            // Since OverlappedEntry is #[repr(C)], and inner is the first field,
            // we can safely cast *mut OVERLAPPED to *const OverlappedEntry to access user_data.
            // This relies on Slot::reset preserving the user_data (index).
            let entry = overlapped as *const OverlappedEntry;
            let idx = unsafe { (*entry).user_data };

            if idx >= self.ops.local.len() {
                debug!(idx, "Completed index out of bounds");
                return Ok(());
            }
            idx
        } else {
            if res == 0 {
                let err = unsafe { GetLastError() };
                if err == WAIT_TIMEOUT {
                    return Ok(());
                }
                if completion_key == 0 && overlapped.is_null() {
                    return Err(io_msg(
                        IocpErrorContext::CompletionWait,
                        format!(
                            "GetQueuedCompletionStatus failed: err={}, wait_ms={}, completion_key={}, overlapped=null",
                            err, wait_ms, completion_key
                        ),
                    ));
                }
            }
            completion_key
        };

        if user_data == WAKEUP_USER_DATA {
            self.is_waked.store(false, Ordering::Release);
            trace!("Wakeup received");
            return Ok(());
        }

        trace!(user_data, bytes_transferred, "CQE received");

        self.process_iocp_completion(user_data, res, bytes_transferred);

        Ok(())
    }

    fn process_timer_completions(&mut self) {
        let mut timer_buffer = std::mem::take(&mut self.timer_buffer);
        let ops_local = &mut self.ops.local;
        let ops_shared = &self.ops.shared;
        let mut pending_events: Vec<CompletionSidecar> = Vec::new();
        let now = Instant::now();

        for &user_data in &timer_buffer {
            if let Some(op) = ops_local.get_mut(user_data) {
                if matches!(op.platform_data.lifecycle, OpLifecycle::InFlight) {
                    if let Some(deadline) = op.platform_data.timer_deadline
                        && now < deadline
                    {
                        let remain = deadline.saturating_duration_since(now);
                        op.platform_data.timer_id = Some(self.wheel.insert(user_data, remain));
                        continue;
                    }

                    op.platform_data.lifecycle = OpLifecycle::Completed;

                    let slot = &ops_shared.slots[user_data];
                    let generation = slot.generation.load(Ordering::Acquire);
                    let _ = unsafe { (*slot.op.get()).take() };
                    let payload = unsafe { (*slot.payload.get()).take() };
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
                    ops_shared.push_free(user_data);
                }
                op.platform_data.timer_id = None;
                op.platform_data.timer_deadline = None;
            }
        }
        for completion in pending_events {
            push_completion_event_shared(
                &self.completion_events,
                &self.completion_table,
                completion_record(completion),
            );
        }
        timer_buffer.clear();
        self.timer_buffer = timer_buffer;
    }

    fn process_iocp_completion(&mut self, user_data: usize, res: i32, bytes_transferred: u32) {
        if !self.ops.contains(user_data) {
            return;
        }

        let completion_events = self.completion_events.clone();
        let completion_table = self.completion_table.clone();
        let ops_local = &mut self.ops.local;
        let ops_shared = &self.ops.shared;

        let slot = &ops_shared.slots[user_data];
        let op = &mut ops_local[user_data];
        let slot_generation = slot.generation.load(Ordering::Acquire);
        if op.platform_data.generation != slot_generation {
            debug!(
                user_data,
                op_generation = op.platform_data.generation,
                slot_generation,
                "Ignoring stale completion due to generation mismatch"
            );
            return;
        }

        let mut io_result = if res == 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(bytes_transferred as usize)
        };

        if let Some(iocp_op) = unsafe { &mut *slot.op.get() } {
            let slot_overlapped = unsafe { &mut *slot.overlapped.get() };
            if let Some(blocking_res) = slot_overlapped.blocking_result.take() {
                io_result = blocking_res;
            } else if io_result.is_ok()
                && let Some(on_comp) = unsafe { iocp_op.vtable.as_ref().on_complete }
            {
                let val = io_result.unwrap();
                io_result = unsafe { (on_comp)(iocp_op, val, &self.extensions) };
            }
        }
        if let Err(e) = &io_result
            && e.raw_os_error().is_none()
        {
            unsafe {
                *slot.result.get() = Some(Err(io::Error::new(e.kind(), e.to_string())));
            }
        }

        match op.platform_data.lifecycle {
            OpLifecycle::Cancelled | OpLifecycle::InFlight => {
                let was_cancelled = matches!(op.platform_data.lifecycle, OpLifecycle::Cancelled);
                let completion_res = io_result_to_event_res(&io_result);
                op.platform_data.rio_pool_waiting = false;

                if op.platform_data.is_background {
                    let _ = unsafe { (*slot.op.get()).take() };
                    let _ = unsafe { (*slot.payload.get()).take() };
                    let _ = unsafe { (*slot.result.get()).take() };
                    let _data = std::mem::take(&mut op.platform_data);
                    self.ops.shared.push_free(user_data);
                } else {
                    op.platform_data.lifecycle = OpLifecycle::Completed;

                    let should_emit = !(was_cancelled && op.platform_data.rio_needs_drain);
                    if should_emit {
                        let payload = unsafe { (*slot.payload.get()).take() };
                        let detail = unsafe { (*slot.result.get()).take() };
                        push_completion_event_shared(
                            &completion_events,
                            &completion_table,
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
                        let _ = unsafe { (*slot.op.get()).take() };
                        let _data = std::mem::take(&mut op.platform_data);
                        self.ops.shared.push_free(user_data);
                    }
                }
            }
            _ => {
                debug!(user_data, "Received completion for non-InFlight op");
            }
        }
    }

    pub(crate) fn register_chunk(&mut self, id: u16, ptr: *const u8, len: usize) -> io::Result<()> {
        self.rio_state.register_chunk(id, ptr, len)?;
        Ok(())
    }

    pub fn shutdown_udp_pool_for_handle(&mut self, handle: crate::RawHandle) {
        self.rio_state
            .begin_udp_pool_shutdown_for_handle(handle.handle);
    }

    pub(crate) fn cancel_op_internal(&mut self, user_data: usize) {
        let completion_events = self.completion_events.clone();
        let completion_table = self.completion_table.clone();
        let ops_local = &mut self.ops.local;
        let ops_shared = &self.ops.shared;

        if let Some(op) = ops_local.get_mut(user_data) {
            trace!(user_data, "Cancelling op");
            let slot = &ops_shared.slots[user_data];

            if let Some(tid) = op.platform_data.timer_id {
                self.wheel.cancel(tid);
                op.platform_data.timer_id = None;
                op.platform_data.timer_deadline = None;
                op.platform_data.lifecycle = OpLifecycle::Completed;
                let generation = slot.generation.load(Ordering::Acquire);

                push_completion_event_shared(
                    &completion_events,
                    &completion_table,
                    completion_record(CompletionSidecar {
                        user_data,
                        generation,
                        res: -(windows_sys::Win32::Foundation::ERROR_OPERATION_ABORTED as i32),
                        flags: 0,
                        payload: unsafe { (*slot.payload.get()).take() },
                        detail: unsafe { (*slot.result.get()).take() },
                    }),
                );
                let _ = unsafe { (*slot.op.get()).take() };
                let _data = std::mem::take(&mut op.platform_data);
                self.ops.shared.push_free(user_data);
                return;
            }

            match op.platform_data.lifecycle {
                OpLifecycle::Pending => {
                    op.platform_data.lifecycle = OpLifecycle::Cancelled;
                    let generation = slot.generation.load(Ordering::Acquire);
                    push_completion_event_shared(
                        &completion_events,
                        &completion_table,
                        completion_record(CompletionSidecar {
                            user_data,
                            generation,
                            res: -(windows_sys::Win32::Foundation::ERROR_OPERATION_ABORTED as i32),
                            flags: 0,
                            payload: unsafe { (*slot.payload.get()).take() },
                            detail: unsafe { (*slot.result.get()).take() },
                        }),
                    );
                    let _ = unsafe { (*slot.op.get()).take() };
                    let _data = std::mem::take(&mut op.platform_data);
                    self.ops.shared.push_free(user_data);
                }
                OpLifecycle::InFlight => {
                    op.platform_data.lifecycle = OpLifecycle::Cancelled;

                    if let Some(res) = unsafe { &mut *slot.op.get() }
                        && let Some(fd) = res.get_fd()
                        && let Ok(handle) = submit::resolve_fd(fd, &self.registered_files)
                    {
                        if op.platform_data.rio_pool_waiting {
                            self.rio_state.cancel_udp_recv_waiter(
                                handle,
                                (user_data, op.platform_data.generation),
                                &*self.registrar,
                            );
                            op.platform_data.rio_pool_waiting = false;
                            let generation = slot.generation.load(Ordering::Acquire);
                            push_completion_event_shared(
                                &completion_events,
                                &completion_table,
                                completion_record(CompletionSidecar {
                                    user_data,
                                    generation,
                                    res: -(windows_sys::Win32::Foundation::ERROR_OPERATION_ABORTED
                                        as i32),
                                    flags: 0,
                                    payload: unsafe { (*slot.payload.get()).take() },
                                    detail: unsafe { (*slot.result.get()).take() },
                                }),
                            );
                            let _ = unsafe { (*slot.op.get()).take() };
                            let _data = std::mem::take(&mut op.platform_data);
                            self.ops.shared.push_free(user_data);
                            return;
                        }

                        if Self::is_rio_op(res) {
                            let generation = slot.generation.load(Ordering::Acquire);
                            push_completion_event_shared(
                                &completion_events,
                                &completion_table,
                                completion_record(CompletionSidecar {
                                    user_data,
                                    generation,
                                    res: -(windows_sys::Win32::Foundation::ERROR_OPERATION_ABORTED
                                        as i32),
                                    flags: 0,
                                    payload: unsafe { (*slot.payload.get()).take() },
                                    detail: unsafe { (*slot.result.get()).take() },
                                }),
                            );
                            let _ = unsafe { (*slot.op.get()).take() };
                            let _data = std::mem::take(&mut op.platform_data);
                            self.ops.shared.push_free(user_data);
                            return;
                        }

                        // Safe usage of overlapped ptr
                        let overlapped_ptr = slot.overlapped_ptr();
                        unsafe {
                            use windows_sys::Win32::System::IO::CancelIoEx;
                            let _ = CancelIoEx(handle, overlapped_ptr);
                        }
                    }
                }
                _ => {}
            }
        }
    }

    pub(crate) fn register_files(
        &mut self,
        files: &[crate::RawHandle],
    ) -> io::Result<Vec<crate::op::IoFd>> {
        let mut registered = Vec::with_capacity(files.len());
        for &handle in files {
            let idx = if let Some(idx) = self.free_slots.pop() {
                self.registered_files[idx] = Some(handle.handle);
                self.rio_state.clear_registered_rq(idx);
                idx
            } else {
                self.registered_files.push(Some(handle.handle));
                self.rio_state
                    .resize_registered_rqs(self.registered_files.len());
                self.registered_files.len() - 1
            };
            registered.push(crate::op::IoFd::Fixed(idx as u32));
        }
        Ok(registered)
    }

    pub(crate) fn unregister_files(&mut self, files: Vec<crate::op::IoFd>) -> io::Result<()> {
        for fd in files {
            if let crate::op::IoFd::Fixed(idx) = fd {
                let idx = idx as usize;
                if idx < self.registered_files.len() && self.registered_files[idx].is_some() {
                    self.registered_files[idx] = None;
                    self.rio_state.clear_registered_rq(idx);
                    self.free_slots.push(idx);
                }
            }
        }
        Ok(())
    }

    pub(crate) fn wake(&self) -> io::Result<()> {
        let res = unsafe {
            PostQueuedCompletionStatus(self.port.handle, 0, WAKEUP_USER_DATA, std::ptr::null_mut())
        };
        trace!("Wake up posted");
        if res == 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    pub(crate) fn create_waker(&self) -> std::sync::Arc<dyn RemoteWaker> {
        std::sync::Arc::new(IocpWaker {
            port: self.port.clone(),
            is_waked: self.is_waked.clone(),
        })
    }

    #[inline]
    pub(crate) fn push_completion_event(&self, sidecar: CompletionSidecar) {
        push_completion_event_shared(
            &self.completion_events,
            &self.completion_table,
            completion_record(sidecar),
        );
    }

    fn shutdown_inflight_ops(&mut self) -> usize {
        if self.shutting_down {
            return 0;
        }
        self.shutting_down = true;

        self.rio_state.begin_shutdown();

        let mut pending_count = 0;
        for user_data in 0..self.ops.local.len() {
            let Some(op) = self.ops.local.get(user_data) else {
                continue;
            };
            if !matches!(op.platform_data.lifecycle, OpLifecycle::InFlight) {
                continue;
            }
            if op.platform_data.timer_id.is_some() {
                self.cancel_op_internal(user_data);
                continue;
            }
            if op.platform_data.rio_pool_waiting {
                self.cancel_op_internal(user_data);
                continue;
            }
            pending_count += 1;
            self.cancel_op_internal(user_data);
        }

        pending_count
    }

    fn drain_pending_iocp(&mut self, pending_count: usize, timeout: Duration) -> io::Result<()> {
        if pending_count == 0 {
            return Ok(());
        }

        let mut ops_drained = 0usize;
        let deadline = Instant::now() + timeout;

        while ops_drained < pending_count {
            if Instant::now() >= deadline {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    format!(
                        "strict close timed out while draining IOCP: drained={}, pending={}",
                        ops_drained, pending_count
                    ),
                ));
            }

            let mut bytes = 0;
            let mut key = 0;
            let mut overlapped = std::ptr::null_mut();

            let res = unsafe {
                GetQueuedCompletionStatus(
                    self.port.handle,
                    &mut bytes,
                    &mut key,
                    &mut overlapped,
                    10,
                )
            };

            if key == RIO_EVENT_KEY {
                if let Ok(count) = self.rio_state.process_completions(
                    &mut self.ops,
                    &*self.registrar,
                    &self.completion_events,
                    &self.completion_table,
                ) {
                    ops_drained += count;
                }
                continue;
            }

            if !overlapped.is_null() {
                let entry = overlapped as *const OverlappedEntry;
                let user_data = unsafe { (*entry).user_data };
                self.process_iocp_completion(user_data, res, bytes);
                ops_drained += 1;
                continue;
            }

            if res == 0 {
                let err = unsafe { GetLastError() };
                if err != WAIT_TIMEOUT {
                    return Err(io_error(
                        IocpErrorContext::CompletionWait,
                        io::Error::from_raw_os_error(err as i32),
                        "strict close failed while draining IOCP",
                    ));
                }
            }
        }

        Ok(())
    }

    fn close_impl(&mut self, mode: CloseMode) -> io::Result<()> {
        if self.closed {
            return Ok(());
        }

        let pending = self.shutdown_inflight_ops();

        if let CloseMode::Strict { timeout } = mode {
            self.drain_pending_iocp(pending, timeout)?;
            self.rio_state.drain_outstanding_for(timeout)?;
        }

        self.closed = true;
        Ok(())
    }
}

#[inline]
fn io_result_to_event_res(res: &io::Result<usize>) -> i32 {
    match res {
        Ok(v) => (*v).min(i32::MAX as usize) as i32,
        Err(e) => -e.raw_os_error().unwrap_or(1).abs(),
    }
}

#[inline]
fn completion_record(sidecar: CompletionSidecar) -> CompletionRecord {
    CompletionRecord {
        event: CompletionEvent {
            user_data: encode_completion_token(sidecar.user_data, sidecar.generation),
            res: sidecar.res,
            flags: sidecar.flags,
        },
        payload: sidecar.payload,
        detail: sidecar.detail,
    }
}

#[inline]
fn push_completion_event_shared(
    queue: &SharedCompletionQueue,
    table: &SharedCompletionTable,
    record: CompletionRecord,
) {
    table.record_completion_with_data(record.event, record.payload, record.detail);
    queue.push(record.event);
}

impl Drop for IocpDriver {
    fn drop(&mut self) {
        debug!("Dropping IocpDriver");
        let _ = self.close_impl(CloseMode::Fast);
    }
}
