pub(crate) mod inner;

use std::io;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::task::Poll;
use std::time::{Duration, Instant};

use tracing::{debug, trace};
use windows_sys::Win32::Foundation::ERROR_OPERATION_ABORTED;
use windows_sys::Win32::System::IO::{OVERLAPPED, PostQueuedCompletionStatus};

use veloq_blocking::{BlockingTask, get_blocking_pool};
#[cfg(feature = "test-hooks")]
pub use veloq_driver_core::driver::test_hooks::DriverTestHooks;
use veloq_driver_core::driver::{
    CompletionEvent, CompletionSidecar as CoreCompletionSidecar, Driver, Outcome, RemoteWaker,
    SharedCompletionQueue, SharedCompletionTable, SubmitBinder,
};
use veloq_driver_core::op_registry::{OpEntry, OpRegistry};
use veloq_driver_core::slot::SlotTable;
use veloq_wheel::TaskId;

use crate::common::{completion_record, push_completion_shared};
use crate::config::{IoFd, RawHandle};
use crate::ops::slot_ext::IocpSlotExt;
use crate::ops::{IocpOp, OverlappedEntry, SubmitContext, submit};
pub use inner::IocpDriver;
use inner::RIO_EVENT_KEY;

// ============================================================================
// State & Lifecycle Types
// ============================================================================

/// Represents the lifecycle stage of an IOCP operation.
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

/// State associated with an IOCP operation.
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

impl IocpOpState {}

/// Closing mode for the driver or operations.
#[derive(Clone, Copy, Debug)]
pub enum CloseMode {
    /// Closes quickly without waiting for pending operations.
    Fast,
    /// Closes after a specified timeout, allowing pending operations to finish.
    Strict { timeout: Duration },
}

pub(crate) type CompletionSidecar = CoreCompletionSidecar;

// ============================================================================
// Driver Implementation
// ============================================================================

struct SubmitContextInternal<'a> {
    port: &'a crate::common::IoCompletionPort,
    wheel: &'a mut veloq_wheel::Wheel<usize>,
    completion_events: &'a SharedCompletionQueue,
    completion_table: &'a SharedCompletionTable,
}

impl IocpDriver {
    #[inline]
    fn prep_op_slot(
        ops: &mut OpRegistry<IocpOp, IocpOpState, OverlappedEntry>,
        user_data: usize,
        op: IocpOp,
    ) -> io::Result<(&mut IocpOp, &mut OpEntry<IocpOpState>, *mut OVERLAPPED)> {
        let (slot, op_entry) = ops
            .get_slot_and_entry_mut(user_data)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "Op not found"))?;

        let generation = slot.generation.load(Ordering::Acquire);
        // SAFETY: Initializing slot with op and sidecar data.
        unsafe { slot.init_op(op, user_data, generation) };

        // SAFETY: `init_op` ensures `self.op` is `Some`.
        let op_ref = unsafe {
            (*slot.op.get())
                .as_mut()
                .ok_or_else(|| io::Error::other("Failed to get op ref"))?
        };
        op_ref.header.user_data = user_data;
        op_ref.header.generation = generation;
        op_entry.platform_data.generation = generation;

        let overlapped_ptr = unsafe { slot.overlapped_ptr() };
        Ok((op_ref, op_entry, overlapped_ptr))
    }

    fn handle_offload(
        ops: &mut OpRegistry<IocpOp, IocpOpState, OverlappedEntry>,
        ctx: SubmitContextInternal<'_>,
        user_data: usize,
        task: BlockingTask,
    ) -> io::Result<Poll<()>> {
        if let Some((_, op_entry)) = ops.get_slot_and_entry_mut(user_data) {
            op_entry.platform_data.lifecycle = OpLifecycle::InFlight;
            op_entry.platform_data.rio_pool_waiting = false;
        }
        if get_blocking_pool().execute(task).is_err() {
            let err = io::Error::other("Thread pool overloaded");
            if let Some((slot, _)) = ops.get_slot_and_entry_mut(user_data) {
                let generation = slot.generation.load(Ordering::Acquire);
                // SAFETY: Cleaning up slot and extracting completion data.
                let (payload, detail) = unsafe {
                    slot.take_op();
                    slot.take_completion_data()
                };
                let sidecar = CompletionSidecar {
                    user_data,
                    generation,
                    res: -err.raw_os_error().unwrap_or(1).abs(),
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
            ops.remove(user_data);
            return Err(err);
        }
        Ok(Poll::Pending)
    }

    fn on_submit_res(
        ops: &mut OpRegistry<IocpOp, IocpOpState, OverlappedEntry>,
        ctx: SubmitContextInternal<'_>,
        result: io::Result<submit::SubmissionResult>,
        user_data: usize,
        op_in: &mut Option<IocpOp>,
        binder: SubmitBinder,
        is_rio_pool_waiting: bool,
    ) -> Outcome<io::Result<Poll<()>>> {
        match result {
            Ok(submit::SubmissionResult::Pending) => {
                if let Some((_, op_entry)) = ops.get_slot_and_entry_mut(user_data) {
                    op_entry.platform_data.lifecycle = OpLifecycle::InFlight;
                    op_entry.platform_data.rio_pool_waiting = is_rio_pool_waiting;
                }
                binder.ok(Poll::Pending)
            }
            Ok(submit::SubmissionResult::PostToQueue) => {
                // SAFETY: port_handle is a valid IOCP handle.
                let posted = unsafe {
                    PostQueuedCompletionStatus(
                        ctx.port.as_raw(),
                        0,
                        user_data,
                        std::ptr::null_mut(),
                    )
                };
                if posted == 0 {
                    let err = io::Error::last_os_error();
                    if let Some((slot, _)) = ops.get_slot_and_entry_mut(user_data) {
                        // SAFETY: Taking the op from slot.
                        *op_in = unsafe { slot.take_op() };
                    }
                    ops.remove(user_data);
                    binder.err(err)
                } else {
                    if let Some((_, op_entry)) = ops.get_slot_and_entry_mut(user_data) {
                        op_entry.platform_data.lifecycle = OpLifecycle::InFlight;
                        op_entry.platform_data.rio_pool_waiting = false;
                    }
                    binder.ok(Poll::Pending)
                }
            }
            Ok(submit::SubmissionResult::Offload(task)) => {
                match Self::handle_offload(ops, ctx, user_data, task) {
                    Ok(poll) => binder.ok(poll),
                    Err(e) => binder.err(e),
                }
            }
            Ok(submit::SubmissionResult::Timer(duration)) => {
                let timeout = ctx.wheel.insert(user_data, duration);
                if let Some((_, op_entry)) = ops.get_slot_and_entry_mut(user_data) {
                    op_entry.platform_data.timer_id = Some(timeout);
                    op_entry.platform_data.timer_deadline = Some(Instant::now() + duration);
                    op_entry.platform_data.lifecycle = OpLifecycle::InFlight;
                    op_entry.platform_data.rio_pool_waiting = false;
                }
                binder.ok(Poll::Pending)
            }
            Err(e) => {
                if let Some((slot, _)) = ops.get_slot_and_entry_mut(user_data) {
                    // SAFETY: Taking the op from slot.
                    *op_in = unsafe { slot.take_op() };
                }
                ops.remove(user_data);
                binder.err(e)
            }
        }
    }

    fn call_op_submit(
        &mut self,
        user_data: usize,
        op: IocpOp,
    ) -> io::Result<(bool, io::Result<submit::SubmissionResult>)> {
        let slots_per_page = self.ops.local.len();
        let (slab_ptr, slab_len) = self
            .ops
            .get_page_slice(0)
            .ok_or_else(|| io::Error::other("Failed to get page slice"))?;

        let (op_ref, _, overlapped_ptr) = Self::prep_op_slot(&mut self.ops, user_data, op)?;

        // SAFETY: compare function pointer with known submit function.
        let is_rio_pool_waiting = unsafe {
            std::ptr::eq(
                op_ref.vtable.as_ref().submit as *const (),
                submit::submit_udp_recv_stream as *const (),
            )
        };

        let mut ctx = SubmitContext {
            port: self.port.as_ref(),
            overlapped: overlapped_ptr,
            ext: &self.extensions,
            registered_files: &self.registered_files,
            registrar: self.registrar.as_ref(),
            rio: &mut self.rio_state,
            slots_per_page,
            slab_resolver: &|idx| (idx == 0).then_some((slab_ptr, slab_len)),
        };

        // SAFETY: submit function pointer is valid.
        let result = unsafe { (op_ref.vtable.as_ref().submit)(op_ref, &mut ctx) };
        Ok((is_rio_pool_waiting, result))
    }

    /// Registers a chunk of memory for RIO operations.
    pub(crate) fn register_chunk(&mut self, id: u16, ptr: *const u8, len: usize) -> io::Result<()> {
        self.rio_state.register_chunk(id, ptr, len)?;
        Ok(())
    }

    /// Shuts down the UDP buffer pool associated with the specified handle.
    pub fn shutdown_udp_pool(&mut self, handle: RawHandle) {
        self.rio_state
            .begin_udp_pool_shutdown_for_handle(handle.handle);
    }

    /// Registers a set of file/socket handles for use with the driver.
    pub(crate) fn register_files(&mut self, files: &[RawHandle]) -> io::Result<Vec<IoFd>> {
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
            registered.push(IoFd::Fixed(idx as u32));
        }
        Ok(registered)
    }

    /// Unregisters a set of previously registered files.
    pub(crate) fn unregister_files(&mut self, files: Vec<IoFd>) -> io::Result<()> {
        for fd in files {
            if let IoFd::Fixed(idx) = fd {
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

    pub(crate) fn shutdown_ops(&mut self) -> usize {
        if self.shutting_down {
            return 0;
        }
        self.shutting_down = true;
        self.rio_state.begin_shutdown();

        let mut in_flight = Vec::new();
        for user_data in 0..self.ops.local.len() {
            if let Some(op) = self.ops.local.get(user_data)
                && matches!(op.platform_data.lifecycle, OpLifecycle::InFlight)
            {
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
    ) -> io::Result<()> {
        if pending_count == 0 {
            return Ok(());
        }
        let mut drained = 0usize;
        let deadline = Instant::now() + timeout;

        while drained < pending_count {
            if Instant::now() >= deadline {
                return Err(io::Error::new(io::ErrorKind::TimedOut, "drain timed out"));
            }
            drained += self.poll_completion()?;
        }
        Ok(())
    }

    pub(crate) fn poll_completion(&mut self) -> io::Result<usize> {
        let status = self.port.get_status(10)?;

        match status {
            crate::common::CompletionStatus::Completed {
                bytes,
                key,
                overlapped,
                success,
                error_code,
            } => {
                if key == RIO_EVENT_KEY {
                    return self.rio_state.process_completions(
                        &mut self.ops,
                        &*self.registrar,
                        &self.completion_events,
                        &self.completion_table,
                    );
                }

                if !overlapped.is_null() {
                    let user_data = unsafe { (*(overlapped as *const OverlappedEntry)).user_data };
                    self.process_completion(user_data, success, error_code, bytes);
                    return Ok(1);
                }
            }
            crate::common::CompletionStatus::Timeout => {}
        }
        Ok(0)
    }

    pub(crate) fn close_impl(&mut self, mode: CloseMode) -> io::Result<()> {
        if self.closed {
            return Ok(());
        }
        let pending = self.shutdown_ops();
        if let CloseMode::Strict { timeout } = mode {
            self.drain_pending_iocp(pending, timeout)?;
            self.rio_state.drain_outstanding_for(timeout)?;
        }
        self.closed = true;
        Ok(())
    }
}

impl Driver for IocpDriver {
    type Op = IocpOp;
    type Handle = RawHandle;
    type Sidecar = OverlappedEntry;

    fn reserve_op(&mut self) -> io::Result<(usize, u32)> {
        let (user_data, generation) = match self.ops.insert(OpEntry::new(IocpOpState::default())) {
            Ok(handle) => (handle.index, handle.generation),
            Err(_) => {
                return Err(io::Error::new(
                    io::ErrorKind::OutOfMemory,
                    "OpRegistry is full",
                ));
            }
        };
        trace!(user_data, generation, "Reserved op slot");
        Ok((user_data, generation))
    }

    fn slot_table(&self) -> Arc<SlotTable<Self::Op, Self::Sidecar>> {
        self.ops.shared.clone()
    }

    fn submit(
        &mut self,
        user_data: usize,
        op_in: &mut Option<Self::Op>,
        binder: SubmitBinder,
    ) -> Outcome<io::Result<Poll<()>>> {
        if self.shutting_down {
            return binder.err(io::Error::from_raw_os_error(ERROR_OPERATION_ABORTED as i32));
        }
        let op = match op_in.take() {
            Some(op) => op,
            None => return binder.err(io::Error::new(io::ErrorKind::InvalidInput, "Empty Option")),
        };

        let (is_rio_pool_waiting, result) = match self.call_op_submit(user_data, op) {
            Ok(res) => res,
            Err(e) => return binder.err(e),
        };

        let ctx = SubmitContextInternal {
            port: self.port.as_ref(),
            wheel: &mut self.wheel,
            completion_events: &self.completion_events,
            completion_table: &self.completion_table,
        };

        Self::on_submit_res(
            &mut self.ops,
            ctx,
            result,
            user_data,
            op_in,
            binder,
            is_rio_pool_waiting,
        )
    }

    fn submit_background(&mut self, op: Self::Op) -> io::Result<()> {
        if self.shutting_down {
            return Err(io::Error::from_raw_os_error(ERROR_OPERATION_ABORTED as i32));
        }
        let (user_data, _) = self.reserve_op()?;
        let (_, result) = self.call_op_submit(user_data, op)?;

        match result {
            Ok(submit::SubmissionResult::Offload(task)) => {
                let (_, op_entry) = self
                    .ops
                    .get_slot_and_entry_mut(user_data)
                    .ok_or_else(|| io::Error::other("Op not found"))?;
                op_entry.platform_data.is_background = true;
                op_entry.platform_data.lifecycle = OpLifecycle::InFlight;
                if get_blocking_pool().execute(task).is_err() {
                    let _ = std::mem::take(&mut op_entry.platform_data);
                    self.ops.shared.push_free(user_data);
                    return Err(io::Error::other("Thread pool overloaded"));
                }
            }
            Ok(_) => {
                let (_, op_entry) = self
                    .ops
                    .get_slot_and_entry_mut(user_data)
                    .ok_or_else(|| io::Error::other("Op not found"))?;
                op_entry.platform_data.is_background = true;
                op_entry.platform_data.lifecycle = OpLifecycle::InFlight;
            }
            Err(e) => {
                if let Some((slot, _)) = self.ops.get_slot_and_entry_mut(user_data) {
                    // SAFETY: Taking the op from slot.
                    let _ = unsafe { slot.take_op() };
                }
                self.ops.remove(user_data);
                return Err(e);
            }
        }
        Ok(())
    }

    fn submit_queue(&mut self) -> io::Result<()> {
        Ok(())
    }

    fn wait(&mut self) -> io::Result<()> {
        self.get_completion(u32::MAX)
    }

    fn process_completions(&mut self) {
        let _ = self.get_completion(0);
    }

    fn completion_queue(&self) -> SharedCompletionQueue {
        self.completion_events.clone()
    }

    fn completion_table(&self) -> SharedCompletionTable {
        self.completion_table.clone()
    }

    fn wait_and_drain_completions(&mut self, out: &mut Vec<CompletionEvent>) -> io::Result<usize> {
        self.get_completion(u32::MAX)?;
        Ok(self.drain_completions(out))
    }

    fn cancel_op(&mut self, user_data: usize) {
        self.cancel_op_internal(user_data);
    }

    fn register_chunk(&mut self, id: u16, ptr: *const u8, len: usize) -> io::Result<()> {
        IocpDriver::register_chunk(self, id, ptr, len)
    }

    fn register_files(&mut self, files: &[RawHandle]) -> io::Result<Vec<IoFd>> {
        IocpDriver::register_files(self, files)
    }

    fn unregister_files(&mut self, files: Vec<IoFd>) -> io::Result<()> {
        IocpDriver::unregister_files(self, files)
    }

    fn wake(&mut self) -> io::Result<()> {
        IocpDriver::wake(self)
    }

    fn inner_handle(&self) -> RawHandle {
        RawHandle {
            handle: self.port.as_raw() as _,
        }
    }

    fn create_waker(&self) -> Arc<dyn RemoteWaker> {
        IocpDriver::create_waker(self)
    }

    fn driver_id(&self) -> usize {
        self.port.as_raw() as usize
    }

    fn set_registrar(&mut self, registrar: Box<dyn veloq_buf::BufferRegistrar>) {
        self.registrar = registrar;
    }
}

impl Drop for IocpDriver {
    fn drop(&mut self) {
        debug!("Dropping IocpDriver");
        let _ = self.close_impl(CloseMode::Fast);
    }
}

#[cfg(feature = "test-hooks")]
impl DriverTestHooks for IocpDriver {
    fn debug_chunk_register_attempts(&self) -> u64 {
        self.rio_state
            .registry
            .registration_stats
            .chunk_register_attempts
    }
}
