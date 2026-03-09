use super::ext::Extensions;
use super::error::{IocpErrorContext, io_error, io_msg};
use super::rio::RioState;
use super::submit;
use crate::config::IocpConfig;
use crate::driver::RemoteWaker;
use crate::driver::op_registry::OpRegistry;
use crate::driver::slot::{OverlappedEntry, STATE_COMPLETED}; // Removed DetachedCompleter if unused, or keep if used in public API
// Removed STATE_SUBMITTED if unused here.
use crate::driver::iocp::op::IocpOp;

use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;
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
    Completed(io::Result<usize>), // Stores the result here!
    /// Cancelled by user
    Cancelled,
    /// Detached completer running or done (Legacy, keeping for compatibility if generic DetachedCompleter logic is used)
    Detached,
}

pub struct IocpOpState {
    pub generation: u32,
    pub lifecycle: OpLifecycle,
    pub detached_completer:
        Option<Box<dyn crate::driver::DetachedCompleter<crate::driver::iocp::op::IocpOp>>>,
    pub timer_id: Option<TaskId>,
    pub is_background: bool,
    // For RIO cancel path: the slot can be recycled only after both:
    // 1) user has consumed completion; 2) late RIO CQE has been drained.
    pub rio_needs_drain: bool,
    pub rio_drained: bool,
}

impl Default for IocpOpState {
    fn default() -> Self {
        Self {
            generation: 0,
            lifecycle: OpLifecycle::Pending,
            detached_completer: None,
            timer_id: None,
            is_background: false,
            rio_needs_drain: false,
            rio_drained: false,
        }
    }
}

impl IocpOpState {
    pub fn new() -> Self {
        Self::default()
    }
}

pub type PreInit = usize;

pub struct IocpDriver {
    pub(crate) port: Arc<CompletionPort>,
    pub(crate) ops: OpRegistry<IocpOp, IocpOpState>,
    pub(crate) extensions: Extensions,
    pub(crate) wheel: Wheel<usize>,
    pub(crate) timer_buffer: Vec<usize>,
    pub(crate) registered_files: Vec<Option<HANDLE>>,
    pub(crate) free_slots: Vec<usize>,
    pub(crate) is_waked: Arc<AtomicBool>,

    // RIO Support (required)
    pub(crate) rio_state: RioState,
    pub(crate) registrar: Box<dyn veloq_buf::BufferRegistrar>,
}

pub(crate) struct CompletionPort {
    pub handle: HANDLE,
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

unsafe impl Send for IocpWaker {}
unsafe impl Sync for IocpWaker {}

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
            || submit_fn == crate::driver::iocp::submit::submit_recv_from as *const () as usize
    }

    pub fn create_pre_init() -> io::Result<PreInit> {
        // Create a new completion port.
        let port =
            unsafe { CreateIoCompletionPort(INVALID_HANDLE_VALUE, std::ptr::null_mut(), 0, 0) };

        if port.is_null() {
            Err(io::Error::last_os_error())
        } else {
            Ok(port as usize)
        }
    }

    pub fn pre_init_handle(pre: &PreInit) -> crate::RawHandle {
        crate::RawHandle { handle: *pre as _ }
    }

    pub fn new(config: impl AsRef<IocpConfig>) -> io::Result<Self> {
        let pre = Self::create_pre_init()?;
        Self::new_from_pre_init(config.as_ref().entries.get(), pre)
    }

    pub fn new_from_pre_init(entries: u32, port_val: PreInit) -> io::Result<Self> {
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
            rio_state,
            registrar: Box::new(veloq_buf::NoopRegistrar),
        })
    }

    /// Retrieve completion events from the port.
    /// timeout_ms: 0 for poll, u32::MAX for wait.
    pub fn get_completion(&mut self, timeout_ms: u32) -> io::Result<()> {
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

        let user_data = if completion_key == RIO_EVENT_KEY {
            return self.rio_state.process_completions(&mut self.ops);
        } else if !overlapped.is_null() {
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

        for &user_data in &timer_buffer {
            let is_detached = if let Some(op) = ops_local.get_mut(user_data) {
                op.platform_data.detached_completer.is_some()
            } else {
                continue;
            };

            if is_detached {
                let slot = &ops_shared.slots[user_data];

                if let Some(op) = ops_local.get_mut(user_data)
                    && matches!(op.platform_data.lifecycle, OpLifecycle::InFlight)
                    && let Some(completer) = op.platform_data.detached_completer.take()
                {
                    op.platform_data.lifecycle = OpLifecycle::Detached;
                    op.platform_data.timer_id = None;

                    let resources = unsafe { (*slot.op.get()).take() };

                    if let Some(res) = resources {
                        completer.complete(Ok(0), res);
                    }
                    unsafe { *slot.result.get() = Some(Ok(0)) };
                    slot.state.store(STATE_COMPLETED, Ordering::Release);
                    slot.waker.wake();
                }
            } else if let Some(op) = ops_local.get_mut(user_data) {
                if matches!(op.platform_data.lifecycle, OpLifecycle::InFlight) {
                    op.platform_data.lifecycle = OpLifecycle::Completed(Ok(0));

                    let slot = &ops_shared.slots[user_data];
                    unsafe { *slot.result.get() = Some(Ok(0)) };
                    slot.state.store(STATE_COMPLETED, Ordering::Release);
                    slot.waker.wake();
                }
                op.platform_data.timer_id = None;
            }
        }
        timer_buffer.clear();
        self.timer_buffer = timer_buffer;
    }

    fn process_iocp_completion(&mut self, user_data: usize, res: i32, bytes_transferred: u32) {
        if !self.ops.contains(user_data) {
            return;
        }

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

        match op.platform_data.lifecycle {
            OpLifecycle::Cancelled | OpLifecycle::InFlight => {
                unsafe { *slot.result.get() = Some(io_result) };

                if op.platform_data.is_background {
                    // Drop resource
                    let _op = unsafe { (*slot.op.get()).take() };
                    // We need to mark slot as free in registry.
                    // Can't use self.ops.remove here because borrowing split.
                    // Manually implement remove logic on local
                    let _data = std::mem::take(&mut op.platform_data);
                    self.ops.free_indices.push(user_data);
                } else if let Some(completer) = op.platform_data.detached_completer.take() {
                    op.platform_data.lifecycle = OpLifecycle::Detached;
                    let resource = unsafe { (*slot.op.get()).take() };
                    if let Some(iocp_op) = resource {
                        let res = unsafe { (*slot.result.get()).take().unwrap() };
                        completer.complete(res, iocp_op);
                    }
                    // Remove from registry
                    // self.ops.remove(user_data);
                    let _data = std::mem::take(&mut op.platform_data);
                    self.ops.free_indices.push(user_data);
                } else {
                    // Normal completion
                    let res_clone = unsafe {
                        (*slot.result.get())
                            .as_ref()
                            .unwrap()
                            .as_ref()
                            .map(|x| *x)
                            .map_err(|e| {
                                if let Some(code) = e.raw_os_error() {
                                    io::Error::from_raw_os_error(code)
                                } else {
                                    io::Error::new(e.kind(), e.to_string())
                                }
                            })
                    };
                    op.platform_data.lifecycle = OpLifecycle::Completed(res_clone);

                    slot.state.store(STATE_COMPLETED, Ordering::Release);
                    slot.waker.wake();
                }
            }
            _ => {
                debug!(user_data, "Received completion for non-InFlight op");
            }
        }
    }

    pub fn register_chunk(&mut self, id: u16, ptr: *const u8, len: usize) -> io::Result<()> {
        self.rio_state.register_chunk(id, ptr, len)?;
        Ok(())
    }

    pub fn cancel_op_internal(&mut self, user_data: usize) {
        let ops_local = &mut self.ops.local;
        let ops_shared = &self.ops.shared;

        if let Some(op) = ops_local.get_mut(user_data) {
            trace!(user_data, "Cancelling op");
            let slot = &ops_shared.slots[user_data];

            if let Some(tid) = op.platform_data.timer_id {
                self.wheel.cancel(tid);
                op.platform_data.timer_id = None;
                let err = io::Error::from_raw_os_error(
                    windows_sys::Win32::Foundation::ERROR_OPERATION_ABORTED as i32,
                );
                op.platform_data.lifecycle = OpLifecycle::Completed(Err(err));

                unsafe {
                    *slot.result.get() = Some(Err(io::Error::from_raw_os_error(
                        windows_sys::Win32::Foundation::ERROR_OPERATION_ABORTED as i32,
                    )))
                };
                slot.state.store(STATE_COMPLETED, Ordering::Release);
                slot.waker.wake();
                return;
            }

            match op.platform_data.lifecycle {
                OpLifecycle::Pending => {
                    op.platform_data.lifecycle = OpLifecycle::Cancelled;
                    slot.state.store(STATE_COMPLETED, Ordering::Release);
                    unsafe {
                        *slot.result.get() = Some(Err(io::Error::from_raw_os_error(
                            windows_sys::Win32::Foundation::ERROR_OPERATION_ABORTED as i32,
                        )))
                    };
                    slot.waker.wake();
                }
                OpLifecycle::InFlight => {
                    op.platform_data.lifecycle = OpLifecycle::Cancelled;

                    if let Some(res) = unsafe { &mut *slot.op.get() }
                        && let Some(fd) = res.get_fd()
                        && let Ok(handle) = submit::resolve_fd(fd, &self.registered_files)
                    {
                        if Self::is_rio_op(res) {
                            op.platform_data.rio_needs_drain = true;
                            op.platform_data.rio_drained = false;
                            unsafe {
                                *slot.result.get() = Some(Err(io::Error::from_raw_os_error(
                                    windows_sys::Win32::Foundation::ERROR_OPERATION_ABORTED as i32,
                                )))
                            };
                            slot.state.store(STATE_COMPLETED, Ordering::Release);
                            slot.waker.wake();
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

    pub fn register_files(
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

    pub fn unregister_files(&mut self, files: Vec<crate::op::IoFd>) -> io::Result<()> {
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

    pub fn wake(&self) -> io::Result<()> {
        let res = unsafe {
            PostQueuedCompletionStatus(self.port.handle, 0, WAKEUP_USER_DATA, std::ptr::null_mut())
        };
        trace!("Wake up posted");
        if res == 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    pub fn create_waker(&self) -> std::sync::Arc<dyn RemoteWaker> {
        std::sync::Arc::new(IocpWaker {
            port: self.port.clone(),
            is_waked: self.is_waked.clone(),
        })
    }
}

impl Drop for IocpDriver {
    fn drop(&mut self) {
        debug!("Dropping IocpDriver");
        let mut pending_count = 0;

        for (user_data, op) in self.ops.local.iter_mut().enumerate() {
            if matches!(op.platform_data.lifecycle, OpLifecycle::InFlight) {
                let slot = &self.ops.shared.slots[user_data];

                if let Some(tid) = op.platform_data.timer_id {
                    self.wheel.cancel(tid);
                    op.platform_data.timer_id = None;
                    op.platform_data.lifecycle = OpLifecycle::Cancelled;
                    continue;
                }

                if let Some(res) = unsafe { &mut *slot.op.get() }
                    && let Some(fd) = res.get_fd()
                    && let Ok(handle) = submit::resolve_fd(fd, &self.registered_files)
                {
                    let overlapped_ptr = slot.overlapped_ptr();
                    unsafe {
                        use windows_sys::Win32::System::IO::CancelIoEx;
                        let _ = CancelIoEx(handle, overlapped_ptr);
                    }
                }
                pending_count += 1;
            }
        }

        let mut ops_drained = 0;

        while ops_drained < pending_count {
            let mut bytes = 0;
            let mut key = 0;
            let mut overlapped = std::ptr::null_mut();

            let res = unsafe {
                GetQueuedCompletionStatus(
                    self.port.handle,
                    &mut bytes,
                    &mut key,
                    &mut overlapped,
                    100,
                )
            };

            if !overlapped.is_null() {
                ops_drained += 1;
            } else if res == 0 {
                let err = unsafe { GetLastError() };
                if err == WAIT_TIMEOUT {
                    continue;
                }
            }
        }

        // Safety cleanup for remote ops
        for (user_data, op_entry) in self.ops.local.iter_mut().enumerate() {
            let slot = &self.ops.shared.slots[user_data];
            if let Some(completer) = op_entry.platform_data.detached_completer.take()
                && let Some(op) = unsafe { (*slot.op.get()).take() }
            {
                completer.complete(Err(io::Error::from(io::ErrorKind::Interrupted)), op);
            }
        }

        // Port is closed when Arc<CompletionPort> drops
    }
}
