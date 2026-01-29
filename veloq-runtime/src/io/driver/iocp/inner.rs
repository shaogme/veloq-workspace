use super::ext::Extensions;
use super::op::{IocpOp, OverlappedEntry};
use super::rio::RioState;
use super::submit;
use crate::io::driver::op_registry::OpRegistry;
use crate::io::driver::{DetachedCompleter, RemoteWaker};

use std::io;
use std::time::Instant;
use tracing::{debug, trace};

use windows_sys::Win32::Foundation::{
    DUPLICATE_SAME_ACCESS, DuplicateHandle, GetLastError, HANDLE, INVALID_HANDLE_VALUE,
    WAIT_TIMEOUT,
};
use windows_sys::Win32::System::IO::{
    CreateIoCompletionPort, GetQueuedCompletionStatus, PostQueuedCompletionStatus,
};
use windows_sys::Win32::System::Threading::GetCurrentProcess;

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
    /// Detached completer running or done
    Detached,
}

pub struct IocpOpState {
    pub lifecycle: OpLifecycle,
    pub detached_completer: Option<Box<dyn DetachedCompleter<IocpOp>>>,
    pub timer_id: Option<TaskId>,
    pub is_background: bool,
}

impl Default for IocpOpState {
    fn default() -> Self {
        Self {
            lifecycle: OpLifecycle::Pending,
            detached_completer: None,
            timer_id: None,
            is_background: false,
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
    pub(crate) port: HANDLE,
    pub(crate) ops: OpRegistry<IocpOp, IocpOpState>,
    pub(crate) extensions: Extensions,
    pub(crate) wheel: Wheel<usize>,
    pub(crate) timer_buffer: Vec<usize>,
    pub(crate) registered_files: Vec<Option<HANDLE>>,
    pub(crate) free_slots: Vec<usize>,

    // RIO Support (Decoupled)
    pub(crate) rio_state: Option<RioState>,
}

pub(crate) struct IocpWaker(pub(crate) HANDLE);

unsafe impl Send for IocpWaker {}
unsafe impl Sync for IocpWaker {}

impl RemoteWaker for IocpWaker {
    fn wake(&self) -> io::Result<()> {
        let res = unsafe {
            PostQueuedCompletionStatus(self.0, 0, WAKEUP_USER_DATA, std::ptr::null_mut())
        };
        if res == 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }
}

impl Drop for IocpWaker {
    fn drop(&mut self) {
        unsafe {
            windows_sys::Win32::Foundation::CloseHandle(self.0);
        }
    }
}

impl IocpDriver {
    pub fn create_pre_init(_config: &crate::config::Config) -> io::Result<PreInit> {
        // Create a new completion port.
        let port =
            unsafe { CreateIoCompletionPort(INVALID_HANDLE_VALUE, std::ptr::null_mut(), 0, 0) };

        if port.is_null() {
            Err(io::Error::last_os_error())
        } else {
            Ok(port as usize)
        }
    }

    pub fn pre_init_handle(pre: &PreInit) -> crate::io::RawHandle {
        crate::io::RawHandle { handle: *pre as _ }
    }

    pub fn new(config: &crate::config::Config) -> io::Result<Self> {
        let pre = Self::create_pre_init(config)?;
        Self::new_from_pre_init(config, pre)
    }

    pub fn new_from_pre_init(
        config: &crate::config::Config,
        port_val: PreInit,
    ) -> io::Result<Self> {
        let port = port_val as HANDLE;
        debug!(port = ?port, "Initializing IocpDriver");
        // Load extensions
        let extensions = Extensions::new()?;
        let entries = config.iocp.entries;

        // Initialize RIO State
        let mut rio_state = RioState::new(port, entries, &extensions)?;

        let ops = OpRegistry::with_capacity(entries as usize);

        // Pre-register existing pages (created by with_capacity)
        if let Some(rio) = &mut rio_state {
            for i in 0..ops.page_count() {
                rio.ensure_slab_page_registration(i, &ops);
            }
        }

        Ok(Self {
            port,
            ops,
            extensions,
            wheel: Wheel::new(WheelConfig::default()),
            timer_buffer: Vec::new(),
            registered_files: Vec::new(),
            free_slots: Vec::new(),
            rio_state,
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
                self.port,
                &mut bytes_transferred,
                &mut completion_key,
                &mut overlapped,
                wait_ms,
            )
        };
        let elapsed = start.elapsed();

        // Process expired timers
        self.wheel.advance(elapsed, &mut self.timer_buffer);
        for &user_data in &self.timer_buffer {
            let is_detached = if let Some(op) = self.ops.get_mut(user_data) {
                op.platform_data.detached_completer.is_some()
            } else {
                continue;
            };

            if is_detached {
                // Handle detached timer
                if let Some(op) = self.ops.get_mut(user_data) {
                    if matches!(op.platform_data.lifecycle, OpLifecycle::InFlight) {
                        if let Some(completer) = op.platform_data.detached_completer.take() {
                            op.platform_data.lifecycle = OpLifecycle::Detached;
                            op.platform_data.timer_id = None; // clear timer id
                            if let Some(resources) = op.resources.take() {
                                completer.complete(Ok(0), resources);
                            }
                        }
                    }
                }
                // Remove from registry
                self.ops.remove(user_data);
            } else {
                if let Some(op) = self.ops.get_mut(user_data) {
                    // Mark timer as completed if it was in flight
                    if matches!(op.platform_data.lifecycle, OpLifecycle::InFlight) {
                        op.platform_data.lifecycle = OpLifecycle::Completed(Ok(0));
                        if let Some(waker) = op.waker.take() {
                            waker.wake();
                        }
                    }
                    op.platform_data.timer_id = None;
                }
            }
        }
        self.timer_buffer.clear();

        // Determine user_data from overlapped or completion_key
        let user_data = if completion_key == RIO_EVENT_KEY {
            // RIO event is triggered. Process RIO CQ.
            if let Some(rio) = &mut self.rio_state {
                return rio.process_completions(&mut self.ops);
            } else {
                return Ok(());
            }
        } else if !overlapped.is_null() {
            let entry = unsafe { &*(overlapped as *const OverlappedEntry) };
            entry.user_data
        } else {
            if res == 0 {
                let err = unsafe { GetLastError() };
                if err == WAIT_TIMEOUT {
                    return Ok(());
                }
                if completion_key == 0 && overlapped.is_null() {
                    // Spurious wake or error without op context
                    return Err(io::Error::from_raw_os_error(err as i32));
                }
            }
            completion_key
        };

        if user_data == WAKEUP_USER_DATA {
            trace!("Wakeup received");
            return Ok(());
        }

        trace!(user_data, bytes_transferred, "CQE received");

        if self.ops.contains(user_data) {
            let op = self.ops.get_mut(user_data).unwrap();

            // Determine IO result
            let mut io_result = if res == 0 {
                Err(io::Error::last_os_error())
            } else {
                Ok(bytes_transferred as usize)
            };

            // Post-processing hooks (e.g. for buffer updates)
            if let Some(iocp_op) = op.resources.as_mut() {
                if let Some(blocking_res) = iocp_op.header.blocking_result.take() {
                    io_result = blocking_res;
                } else if io_result.is_ok() {
                    if let Some(on_comp) = iocp_op.vtable.on_complete {
                        let val = io_result.unwrap();
                        io_result = unsafe { (on_comp)(iocp_op, val, &self.extensions) };
                    }
                }
            }

            match op.platform_data.lifecycle {
                OpLifecycle::Cancelled | OpLifecycle::InFlight => {
                    if op.platform_data.is_background {
                        // Background op completed, just remove.
                        self.ops.remove(user_data);
                    } else if let Some(completer) = op.platform_data.detached_completer.take() {
                        // Detached op completed
                        op.platform_data.lifecycle = OpLifecycle::Detached;
                        let mut entry = self.ops.remove(user_data);
                        if let Some(iocp_op) = entry.resources.take() {
                            completer.complete(io_result, iocp_op);
                        }
                    } else {
                        // Normal completion
                        op.platform_data.lifecycle = OpLifecycle::Completed(io_result);
                        if let Some(waker) = op.waker.take() {
                            waker.wake();
                        }
                    }
                }
                _ => {
                    // Pending or already Completed/Detached - unexpected new completion?
                    // Could be valid for partial completions if supported (not for now), or bug.
                    debug!(user_data, "Received completion for non-InFlight op");
                }
            }
        }

        Ok(())
    }

    pub fn register_buffer_regions(
        &mut self,
        regions: &[crate::io::buffer::BufferRegion],
    ) -> io::Result<Vec<usize>> {
        if let Some(rio) = &mut self.rio_state {
            rio.register_buffers(regions)?;
            // RIO state stores IDs sequentially in registered_bufs matching the regions input
            return Ok((0..regions.len()).collect());
        }
        // If not RIO, we might just return dummy indices if we supported other mechanisms,
        // but currently IOCP driver purely relies on RIO for registration.
        // If no RIO, we effectively "do nothing" but return tokens that won't be used (or will fail later).
        Ok((0..regions.len()).collect())
    }

    pub fn cancel_op_internal(&mut self, user_data: usize) {
        if let Some(op) = self.ops.get_mut(user_data) {
            trace!(user_data, "Cancelling op");
            // Transition to Cancelled state

            // If it's a timer
            if let Some(tid) = op.platform_data.timer_id {
                self.wheel.cancel(tid);
                op.platform_data.timer_id = None;
                // For a timer, cancellation is immediate completion with error
                op.platform_data.lifecycle =
                    OpLifecycle::Completed(Err(io::Error::from_raw_os_error(
                        windows_sys::Win32::Foundation::ERROR_OPERATION_ABORTED as i32,
                    )));
                if let Some(waker) = op.waker.take() {
                    waker.wake();
                }
                return;
            }

            match op.platform_data.lifecycle {
                OpLifecycle::Pending => {
                    op.platform_data.lifecycle = OpLifecycle::Cancelled;
                    // Pending ops haven't been submitted, so we can just wake and let poll see Cancelled
                    // or just remove?
                    // Usually if it's Pending, it means it's in the registry but not submitted?
                    // But our reserve_op puts it there.
                    // If we just wake, poll_op needs to handle Cancelled.
                    if let Some(waker) = op.waker.take() {
                        waker.wake();
                    }
                }
                OpLifecycle::InFlight => {
                    op.platform_data.lifecycle = OpLifecycle::Cancelled;

                    // Try to CancelIoEx
                    if let Some(res) = &mut op.resources
                        && let Some(fd) = res.get_fd()
                        && let Ok(handle) = submit::resolve_fd(fd, &self.registered_files)
                    {
                        // Direct access to header
                        let entry = &mut res.header;
                        unsafe {
                            use windows_sys::Win32::System::IO::CancelIoEx;
                            let _ = CancelIoEx(handle, &entry.inner as *const _ as *mut _);
                        }
                    }
                    // We do NOT remove the op here. usage of CancelIoEx implies we expect a completion packet
                    // with ERROR_OPERATION_ABORTED. We handle cleanup in `get_completion`.
                }
                _ => {}
            }
        }
    }

    pub fn register_files(
        &mut self,
        files: &[crate::io::RawHandle],
    ) -> io::Result<Vec<crate::io::op::IoFd>> {
        let mut registered = Vec::with_capacity(files.len());
        for &handle in files {
            let idx = if let Some(idx) = self.free_slots.pop() {
                self.registered_files[idx] = Some(handle.handle);
                if let Some(rio) = &mut self.rio_state {
                    rio.clear_registered_rq(idx);
                }
                idx
            } else {
                self.registered_files.push(Some(handle.handle));
                if let Some(rio) = &mut self.rio_state {
                    rio.resize_registered_rqs(self.registered_files.len());
                }
                self.registered_files.len() - 1
            };
            registered.push(crate::io::op::IoFd::Fixed(idx as u32));
        }
        Ok(registered)
    }

    pub fn unregister_files(&mut self, files: Vec<crate::io::op::IoFd>) -> io::Result<()> {
        for fd in files {
            if let crate::io::op::IoFd::Fixed(idx) = fd {
                let idx = idx as usize;
                if idx < self.registered_files.len() && self.registered_files[idx].is_some() {
                    self.registered_files[idx] = None;
                    if let Some(rio) = &mut self.rio_state {
                        rio.clear_registered_rq(idx);
                    }
                    self.free_slots.push(idx);
                }
            }
        }
        Ok(())
    }

    pub fn wake(&self) -> io::Result<()> {
        let res = unsafe {
            PostQueuedCompletionStatus(self.port, 0, WAKEUP_USER_DATA, std::ptr::null_mut())
        };
        trace!("Wake up posted");
        if res == 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    pub fn create_waker(&self) -> std::sync::Arc<dyn RemoteWaker> {
        let process = unsafe { GetCurrentProcess() };
        let mut new_handle = INVALID_HANDLE_VALUE;
        let res = unsafe {
            DuplicateHandle(
                process,
                self.port,
                process,
                &mut new_handle,
                0,
                0,
                DUPLICATE_SAME_ACCESS,
            )
        };
        if res == 0 {
            panic!("Failed to dup handle");
        }
        std::sync::Arc::new(IocpWaker(new_handle))
    }
}

impl Drop for IocpDriver {
    fn drop(&mut self) {
        debug!("Dropping IocpDriver");
        let mut pending_count = 0;
        for (_user_data, op) in self.ops.iter_mut() {
            if matches!(op.platform_data.lifecycle, OpLifecycle::InFlight) {
                // Attempt to cancel timer
                if let Some(tid) = op.platform_data.timer_id {
                    self.wheel.cancel(tid);
                    op.platform_data.timer_id = None;
                    // Mark as cancelled so we don't consider it anymore
                    op.platform_data.lifecycle = OpLifecycle::Cancelled;
                    // Software timers don't generate IOCP completion packets when cancelled here,
                    // so we don't need to increment pending_count.
                    continue;
                }

                if let Some(res) = op.resources.as_mut()
                    && let Some(fd) = res.get_fd()
                    && let Ok(handle) = submit::resolve_fd(fd, &self.registered_files)
                {
                    // Direct access to header
                    let entry = &mut res.header;
                    unsafe {
                        use windows_sys::Win32::System::IO::CancelIoEx;
                        let _ = CancelIoEx(handle, &entry.inner as *const _ as *mut _);
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
                GetQueuedCompletionStatus(self.port, &mut bytes, &mut key, &mut overlapped, 100)
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

        // Complete any remaining remote ops to avoid panics in their waiters.
        // During shutdown, we must ensure all RemoteOps return their resources.
        for (_user_data, op_entry) in self.ops.iter_mut() {
            if let Some(completer) = op_entry.platform_data.detached_completer.take() {
                if let Some(op) = op_entry.resources.take() {
                    completer.complete(Err(io::Error::from(io::ErrorKind::Interrupted)), op);
                }
            }
        }

        unsafe { windows_sys::Win32::Foundation::CloseHandle(self.port) };
    }
}
