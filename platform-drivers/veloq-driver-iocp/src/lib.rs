mod addr;
mod error;
mod ext;
mod inner;
mod lifecycle;
mod op;
mod port;
mod registration;
mod rio;
mod socket;
mod state;
mod submit;
mod utils;
mod waker;

#[cfg(test)]
mod tests;

use std::io;
use std::num::NonZeroU32;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::task::Poll;
use std::time::Instant;

use tracing::trace;
use veloq_blocking::{get_blocking_pool, BlockingTask};
use veloq_driver_core::IoFd as CoreIoFd;
use veloq_driver_core::driver::{
    CompletionSidecar, Driver, Outcome, RemoteWaker, SharedCompletionQueue, SharedCompletionTable,
    SubmitBinder,
};
use veloq_driver_core::op_registry::{OpEntry, OpRegistry};
use veloq_driver_core::slot::{SlotEntry, SlotTable};
use veloq_wheel::Wheel;
use windows_sys::Win32::Foundation::{ERROR_OPERATION_ABORTED, HANDLE};
use windows_sys::Win32::Networking::WinSock::{WSADATA, WSAStartup};
use windows_sys::Win32::System::IO::{OVERLAPPED, PostQueuedCompletionStatus};

use crate::op::{IocpOp, OverlappedEntry, SubmitContext};
use crate::submit::SubmissionResult;
use crate::utils::{completion_record, push_completion_event_shared};

pub use addr::*;
pub use inner::IocpDriver;
pub use socket::Socket;
pub use state::*;

/// Specifies how buffers are registered and validated.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum BufferRegistrationMode {
    /// Strict registration with validation.
    #[default]
    Strict,
    /// Compatible registration for fallback.
    Compatible,
}

impl BufferRegistrationMode {
    /// Returns true if the mode is strict.
    #[inline]
    pub const fn is_strict(self) -> bool {
        matches!(self, Self::Strict)
    }
}

/// Configuration for the IOCP driver.
#[derive(Debug, Clone)]
pub struct IocpConfig {
    /// Number of entries in the completion port.
    pub entries: NonZeroU32,
    /// Mode for buffer registration.
    pub registration_mode: BufferRegistrationMode,
}

impl AsRef<IocpConfig> for IocpConfig {
    fn as_ref(&self) -> &IocpConfig {
        self
    }
}

impl Default for IocpConfig {
    fn default() -> Self {
        Self {
            entries: NonZeroU32::new(1024).unwrap_or(unsafe { NonZeroU32::new_unchecked(1024) }),
            registration_mode: BufferRegistrationMode::Strict,
        }
    }
}

impl IocpConfig {
    /// Sets the registration mode.
    pub fn registration_mode(mut self, mode: BufferRegistrationMode) -> Self {
        self.registration_mode = mode;
        self
    }
}

/// A raw Windows handle wrapper.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(transparent)]
pub struct RawHandle {
    /// The underlying Windows HANDLE.
    pub handle: HANDLE,
}

// SAFETY: Windows HANDLEs are thread-safe and can be sent across threads.
unsafe impl Send for RawHandle {}
// SAFETY: Windows HANDLEs can be accessed from multiple threads simultaneously.
unsafe impl Sync for RawHandle {}

impl From<usize> for RawHandle {
    fn from(handle: usize) -> Self {
        Self {
            handle: handle as HANDLE,
        }
    }
}

impl From<RawHandle> for usize {
    fn from(handle: RawHandle) -> Self {
        handle.handle as usize
    }
}

/// Type alias for I/O descriptors using RawHandle.
pub type IoFd = CoreIoFd<RawHandle>;

#[used]
#[unsafe(link_section = ".CRT$XCU")]
static INIT_WINSOCK: unsafe extern "C" fn() = {
    unsafe extern "C" fn init() {
        // SAFETY: WSAStartup is required for networking on Windows.
        unsafe {
            let mut data: WSADATA = std::mem::zeroed();
            let _ = WSAStartup(0x0202, &mut data);
        }
    }
    init
};

#[inline]
fn slot_overlapped_ptr(slot: &SlotEntry<IocpOp, OverlappedEntry>) -> *mut OVERLAPPED {
    // SAFETY: slot is guaranteed to be valid during the operation.
    unsafe { &mut (*slot.sidecar.get()).inner as *mut _ }
}

#[cfg(feature = "test-hooks")]
impl veloq_driver_core::driver::test_hooks::DriverTestHooks for IocpDriver {
    fn debug_chunk_register_attempts(&self) -> u64 {
        self.rio_state
            .registry
            .registration_stats
            .chunk_register_attempts
    }
}

struct SubmitContextInternal<'a> {
    port_handle: HANDLE,
    wheel: &'a mut Wheel<usize>,
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

        // SAFETY: slot.op.get() is a valid pointer for the lifetime of the SlotEntry.
        unsafe { *slot.op.get() = Some(op) };

        // SAFETY: We just inserted the op, so it's guaranteed to be Some.
        let op_ref = unsafe {
            (*slot.op.get())
                .as_mut()
                .ok_or_else(|| io::Error::other("Failed to get op ref"))?
        };
        op_ref.header.user_data = user_data;
        let generation = slot.generation.load(Ordering::Acquire);
        op_ref.header.generation = generation;
        op_entry.platform_data.generation = generation;

        // SAFETY: slot.sidecar.get() is valid for the lifetime of the SlotEntry.
        unsafe {
            let sidecar = &mut *slot.sidecar.get();
            sidecar.user_data = user_data;
            sidecar.generation = generation;
            sidecar.blocking_result = None;
        }
        let overlapped_ptr = slot_overlapped_ptr(slot);
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
            if let Some((slot, op_entry)) = ops.get_slot_and_entry_mut(user_data) {
                // SAFETY: slot.result.get() is a valid pointer.
                unsafe {
                    *slot.result.get() = Some(Err(io::Error::new(err.kind(), err.to_string())));
                }
                op_entry.platform_data.lifecycle = OpLifecycle::Completed;
                let generation = slot.generation.load(Ordering::Acquire);
                // SAFETY: slot.op.get(), slot.payload.get(), slot.result.get() are valid pointers.
                let _ = unsafe { (*slot.op.get()).take() };
                let payload = unsafe { (*slot.payload.get()).take() };
                let detail = unsafe { (*slot.result.get()).take() };
                let sidecar = CompletionSidecar {
                    user_data,
                    generation,
                    res: -err.raw_os_error().unwrap_or(1).abs(),
                    flags: 0,
                    payload,
                    detail,
                };
                push_completion_event_shared(
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
        result: io::Result<SubmissionResult>,
        user_data: usize,
        op_in: &mut Option<IocpOp>,
        binder: SubmitBinder,
        is_rio_pool_waiting: bool,
    ) -> Outcome<io::Result<Poll<()>>> {
        match result {
            Ok(SubmissionResult::Pending) => {
                if let Some((_, op_entry)) = ops.get_slot_and_entry_mut(user_data) {
                    op_entry.platform_data.lifecycle = OpLifecycle::InFlight;
                    op_entry.platform_data.rio_pool_waiting = is_rio_pool_waiting;
                }
                binder.ok(Poll::Pending)
            }
            Ok(SubmissionResult::PostToQueue) => {
                // SAFETY: port_handle is a valid IOCP handle.
                let posted = unsafe {
                    PostQueuedCompletionStatus(ctx.port_handle, 0, user_data, std::ptr::null_mut())
                };
                if posted == 0 {
                    let err = io::Error::last_os_error();
                    if let Some((slot, _)) = ops.get_slot_and_entry_mut(user_data) {
                        // SAFETY: slot.op.get() is a valid pointer.
                        let op = unsafe { (*slot.op.get()).take() };
                        *op_in = op;
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
            Ok(SubmissionResult::Offload(task)) => {
                match Self::handle_offload(ops, ctx, user_data, task) {
                    Ok(poll) => binder.ok(poll),
                    Err(e) => binder.err(e),
                }
            }
            Ok(SubmissionResult::Timer(duration)) => {
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
                    // SAFETY: slot.op.get() is a valid pointer.
                    let op = unsafe { (*slot.op.get()).take() };
                    *op_in = op;
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
    ) -> io::Result<(bool, io::Result<SubmissionResult>)> {
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
                crate::submit::submit_udp_recv_stream as *const (),
            )
        };

        let mut ctx = SubmitContext {
            port: self.port.handle,
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
}

impl Driver for IocpDriver {
    type Op = IocpOp;
    type Handle = RawHandle;
    type Sidecar = OverlappedEntry;

    fn reserve_op(&mut self) -> io::Result<(usize, u32)> {
        let (user_data, generation) = match self.ops.insert(OpEntry::new(IocpOpState::new())) {
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
            port_handle: self.port.handle,
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
            Ok(SubmissionResult::Offload(task)) => {
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
                    // SAFETY: slot.op.get() is a valid pointer.
                    let _ = unsafe { (*slot.op.get()).take() };
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

    fn wait_and_drain_completions(
        &mut self,
        out: &mut Vec<veloq_driver_core::driver::CompletionEvent>,
    ) -> io::Result<usize> {
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
            handle: self.port.handle as _,
        }
    }

    fn create_waker(&self) -> std::sync::Arc<dyn RemoteWaker> {
        IocpDriver::create_waker(self)
    }

    fn driver_id(&self) -> usize {
        self.port.handle as usize
    }

    fn set_registrar(&mut self, registrar: Box<dyn veloq_buf::BufferRegistrar>) {
        self.registrar = registrar;
    }
}
