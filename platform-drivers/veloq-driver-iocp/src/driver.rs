pub(crate) mod inner;

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::task::Poll;
use std::time::{Duration, Instant};

use tracing::{debug, trace};
use windows_sys::Win32::Foundation::ERROR_OPERATION_ABORTED;
use windows_sys::Win32::Networking::WinSock::{
    SO_TYPE, SOCKET, SOL_SOCKET, WSAENOTSOCK, WSAGetLastError, getsockopt,
};

use veloq_blocking::{BlockingTask, get_blocking_pool};
#[cfg(feature = "test-hooks")]
pub use veloq_driver_core::driver::test_hooks::DriverTestHooks;
use veloq_driver_core::driver::{
    CompletionEvent, CompletionSidecar as CoreCompletionSidecar, Driver, Outcome, RegisterFd,
    RemoteWaker, SharedCompletionQueue, SharedCompletionTable, SubmitBinder, SubmitStatus,
};
use veloq_driver_core::error::{
    DriverErrorKind, DriverErrorReport, DriverResult, driver_error, driver_os_error,
};
use veloq_driver_core::op_registry::{OpEntry, OpRegistry};
use veloq_driver_core::slot::{
    DetachedCancelTable, ErasedPayload, Reserved, SlotRegistryExt, SlotTable, SlotView,
};
use veloq_wheel::TaskId;

use crate::common::{completion_record, iocp_fallback_event_res, push_completion_shared};
use crate::config::{IoFd, IocpHandle, RawHandle, RawHandleKind, RegisteredHandle, SocketKey};
use crate::error::{IocpError, IocpResult, IocpResultExt, from_io_error};
use crate::ops::slot::Slot;
use crate::ops::{IocpOp, OverlappedEntry, SubmitContext, submit};
pub use inner::IocpDriver;
use inner::{CONTROL_EVENT_KEY, RIO_EVENT_KEY};

// ============================================================================
// State & Lifecycle Types
// ============================================================================

/// State associated with an IOCP operation.
#[derive(Default)]
pub struct IocpOpState {
    pub(crate) generation: u32,
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

enum DriverControlCommand {
    SocketCleanup {
        handle: SocketKey,
        registered_fd: Option<IoFd>,
    },
}

#[derive(Clone)]
pub struct SocketLifecycleHandle {
    port: Arc<crate::win32::IoCompletionPort>,
}

impl SocketLifecycleHandle {
    pub(crate) fn new(port: Arc<crate::win32::IoCompletionPort>) -> Self {
        Self { port }
    }

    pub fn schedule_socket_cleanup(
        &self,
        handle: RawHandle,
        registered_fd: Option<IoFd>,
    ) -> DriverResult<()> {
        let cmd = DriverControlCommand::SocketCleanup {
            handle: handle.raw().actor_key(),
            registered_fd,
        };
        let ptr = Box::into_raw(Box::new(cmd)) as *mut crate::win32::Overlapped;
        // SAFETY: `ptr` is an opaque pointer passed through IOCP and recovered in the
        // control-event branch before any overlapped-id decoding.
        let post_res = unsafe { self.port.post(0, CONTROL_EVENT_KEY, ptr) };
        if let Err(_err) = post_res {
            // SAFETY: post failed, ownership of `ptr` never left this thread.
            unsafe { drop(Box::from_raw(ptr as *mut DriverControlCommand)) };
            return Err(driver_error(
                DriverErrorKind::Submission,
                "iocp/driver",
                "failed to post socket cleanup control command",
            ));
        }
        Ok(())
    }

    #[inline]
    pub const fn supports_registration(&self) -> bool {
        true
    }
}

struct SubmitContextInternal<'a> {
    port: &'a crate::win32::IoCompletionPort,
    wheel: &'a mut veloq_wheel::Wheel<usize>,
    completion_events: &'a SharedCompletionQueue,
    completion_table: &'a SharedCompletionTable,
}

impl IocpDriver {
    #[inline]
    fn with_report_detail(
        kind: DriverErrorKind,
        scope: &'static str,
        detail: &'static str,
        report: impl std::fmt::Display,
    ) -> DriverErrorReport {
        driver_error(kind, scope, format!("{detail}: {report}"))
    }

    /// Fallback probe for potentially untrusted external raw handles.
    ///
    /// We trust `RawHandle` enum semantics by default. Probe is only used when
    /// callers provide a `File`-tagged handle that may actually be a socket
    /// (for example, legacy/raw trait boundaries that still pass plain HANDLE).
    fn detect_socket_from_file_handle(handle: RawHandle) -> IocpResult<bool> {
        let socket = handle.raw().as_socket();
        let mut ty = 0i32;
        let mut len = std::mem::size_of::<i32>() as i32;
        // SAFETY: buffer pointers are valid for getsockopt call.
        let ret = unsafe {
            getsockopt(
                socket as SOCKET,
                SOL_SOCKET,
                SO_TYPE,
                &mut ty as *mut i32 as *mut u8,
                &mut len,
            )
        };
        if ret == 0 {
            return Ok(true);
        }
        // SAFETY: reads last winsock error after getsockopt failure.
        let err = unsafe { WSAGetLastError() };
        if err == WSAENOTSOCK {
            Ok(false)
        } else {
            Err(from_io_error(
                IocpError::ResolveFd,
                "iocp/driver.detect_socket_from_file_handle",
                std::io::Error::from_raw_os_error(err),
            ))
        }
    }

    pub fn socket_lifecycle_handle(&self) -> SocketLifecycleHandle {
        SocketLifecycleHandle::new(self.port.clone())
    }

    fn track_socket_submit_pending(&mut self, key: SocketKey) {
        let _ = self.rio_state.try_acquire_socket_inflight(key);
    }

    pub(crate) fn release_socket_inflight_for_op(&mut self, user_data: usize) {
        let socket_key = self
            .ops
            .get_slot_entry_op_storage_and_entry_mut(user_data)
            .and_then(|(_, _, op_opt, _)| {
                let op = op_opt.as_mut()?;
                if !op.header.in_flight {
                    return None;
                }
                op.header.in_flight = false;
                op.header
                    .resolved_handle
                    .filter(|h| h.is_socket())
                    .map(|h| h.actor_key())
            });

        if let Some(key) = socket_key {
            self.rio_state.release_socket_inflight(key);
            self.drain_deferred_socket_cleanup();
        }
    }

    fn schedule_deferred_socket_cleanup(&mut self, handle: SocketKey, registered_fd: Option<IoFd>) {
        let key = handle;
        self.rio_state.mark_socket_closing(key);
        self.deferred_socket_cleanup
            .push_back(inner::DeferredSocketCleanup {
                handle,
                registered_fd,
            });
        self.drain_deferred_socket_cleanup();
    }

    fn drain_deferred_socket_cleanup(&mut self) {
        let mut rounds = self.deferred_socket_cleanup.len();
        while rounds > 0 {
            rounds -= 1;
            let Some(pending) = self.deferred_socket_cleanup.pop_front() else {
                break;
            };

            let key = pending.handle;
            let ready = self.rio_state.socket_ready_for_cleanup(key);

            if ready {
                self.rio_state.shutdown_actor(key);
                if let Some(fd) = pending.registered_fd {
                    let _ = self.unregister_files(vec![fd]);
                }
                self.rio_state.forget_socket_runtime(key);
            } else {
                self.deferred_socket_cleanup.push_back(pending);
            }
        }
    }

    pub(crate) fn handle_control_completion(&mut self, overlapped: *mut crate::win32::Overlapped) {
        if overlapped.is_null() {
            return;
        }
        // SAFETY: control events are posted with a pointer from Box<DriverControlCommand>.
        let cmd = unsafe { Box::from_raw(overlapped as *mut DriverControlCommand) };
        match *cmd {
            DriverControlCommand::SocketCleanup {
                handle,
                registered_fd,
            } => {
                self.schedule_deferred_socket_cleanup(handle, registered_fd);
            }
        }
    }

    #[inline]
    fn prep_op_slot(
        ops: &mut OpRegistry<IocpOp, IocpOpState, OverlappedEntry>,
        user_data: usize,
        op: IocpOp,
    ) -> IocpResult<Slot<'_, Reserved>> {
        let mut guard = ops.slot_reserve(user_data);
        let generation = guard.entry.generation(Ordering::Acquire);
        guard.platform_mut().generation = generation;
        let mut guard = guard.init_op_with(op, |sidecar| {
            sidecar.user_data = user_data;
            sidecar.generation = generation;
            sidecar.blocking_result = None;
            sidecar.in_flight = false;
            sidecar.resolved_handle = None;
        });

        guard
            .with_op_mut(|op_ref| {
                op_ref.header.user_data = user_data;
                op_ref.header.generation = generation;
                op_ref.header.resolved_handle = None;
            })
            .ok_or_else(|| {
                error_stack::Report::new(IocpError::InvalidState)
                    .attach("Op missing in prep_op_slot")
            })?;

        Ok(guard)
    }

    fn handle_offload(
        ops: &mut OpRegistry<IocpOp, IocpOpState, OverlappedEntry>,
        ctx: SubmitContextInternal<'_>,
        user_data: usize,
        task: BlockingTask,
    ) -> DriverResult<Poll<()>> {
        if let Some((_, op_entry)) = ops.get_slot_and_entry_mut(user_data) {
            op_entry.platform_data.rio_pool_waiting = false;
        }
        if get_blocking_pool().execute(task).is_err() {
            if let Some(SlotView::InFlightWaiting(slot)) = ops.slot_view(user_data) {
                let mut guard = slot.complete();
                let (payload, detail) = guard.take_completion_data();
                let _ = guard.take_op();
                let sidecar = CompletionSidecar {
                    user_data,
                    generation: guard.entry.generation(Ordering::Acquire),
                    res: iocp_fallback_event_res(IocpError::Submission),
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
            return Err(driver_error(
                DriverErrorKind::Submission,
                "iocp/driver",
                "thread pool overloaded",
            ));
        }
        Ok(Poll::Pending)
    }

    fn on_submit_res(
        ops: &mut OpRegistry<IocpOp, IocpOpState, OverlappedEntry>,
        ctx: SubmitContextInternal<'_>,
        result: DriverResult<submit::SubmissionResult>,
        user_data: usize,
        op_in: &mut Option<IocpOp>,
        binder: SubmitBinder,
        is_rio_pool_waiting: bool,
    ) -> Outcome<Result<Poll<()>, (DriverErrorReport, SubmitStatus)>> {
        match result {
            Ok(submit::SubmissionResult::Pending) => {
                if let Some((_, op_entry)) = ops.get_slot_and_entry_mut(user_data) {
                    op_entry.platform_data.rio_pool_waiting = is_rio_pool_waiting;
                }
                binder.ok(Poll::Pending)
            }
            Ok(submit::SubmissionResult::PostToQueue) => {
                Self::handle_post_to_queue(ops, ctx, user_data, op_in, binder)
            }
            Ok(submit::SubmissionResult::Offload(task)) => {
                match Self::handle_offload(ops, ctx, user_data, task) {
                    Ok(poll) => binder.ok(poll),
                    Err(_) => binder.err(
                        driver_error(
                            DriverErrorKind::Submission,
                            "iocp/driver",
                            "offload task submission failed",
                        ),
                        SubmitStatus::InFlight,
                    ),
                }
            }
            Ok(submit::SubmissionResult::Timer(duration)) => {
                Self::handle_timer_sub(ops, ctx, user_data, duration, binder)
            }
            Err(_e) => {
                if let Some(SlotView::InFlightWaiting(slot)) = ops.slot_view(user_data) {
                    let mut guard = slot.complete();
                    *op_in = guard.take_op();
                }
                binder.err(
                    driver_error(
                        DriverErrorKind::Submission,
                        "iocp/driver",
                        "operation submission failed",
                    ),
                    SubmitStatus::Void,
                )
            }
        }
    }

    fn handle_post_to_queue(
        ops: &mut OpRegistry<IocpOp, IocpOpState, OverlappedEntry>,
        ctx: SubmitContextInternal<'_>,
        user_data: usize,
        op_in: &mut Option<IocpOp>,
        binder: SubmitBinder,
    ) -> Outcome<Result<Poll<()>, (DriverErrorReport, SubmitStatus)>> {
        if let Err(err) = ctx.port.notify(user_data) {
            if let Some(SlotView::InFlightWaiting(slot)) = ops.slot_view(user_data) {
                let mut guard = slot.complete();
                *op_in = guard.take_op();
            }
            ops.remove(user_data);
            binder.err(
                driver_error(
                    DriverErrorKind::Submission,
                    "iocp/driver",
                    format!("failed to post completion queue notification: {err:#}"),
                ),
                SubmitStatus::Void,
            )
        } else {
            if let Some((_, op_entry)) = ops.get_slot_and_entry_mut(user_data) {
                op_entry.platform_data.rio_pool_waiting = false;
            }
            binder.ok(Poll::Pending)
        }
    }

    fn handle_timer_sub(
        ops: &mut OpRegistry<IocpOp, IocpOpState, OverlappedEntry>,
        ctx: SubmitContextInternal<'_>,
        user_data: usize,
        duration: Duration,
        binder: SubmitBinder,
    ) -> Outcome<Result<Poll<()>, (DriverErrorReport, SubmitStatus)>> {
        let timeout = ctx.wheel.insert(user_data, duration);
        if let Some((_, op_entry)) = ops.get_slot_and_entry_mut(user_data) {
            op_entry.platform_data.timer_id = Some(timeout);
            op_entry.platform_data.timer_deadline = Some(Instant::now() + duration);
            op_entry.platform_data.rio_pool_waiting = false;
        }
        binder.ok(Poll::Pending)
    }

    fn call_op_submit(
        &mut self,
        user_data: usize,
        op: IocpOp,
    ) -> DriverResult<(bool, DriverResult<submit::SubmissionResult>)> {
        let slots_per_page = self.ops.local.len();
        let (slab_ptr, slab_len) = self
            .ops
            .get_page_slice(0)
            .ok_or_else(|| {
                driver_error(
                    DriverErrorKind::InvalidState,
                    "iocp/driver",
                    "failed to get page slice",
                )
            })?;

        let mut guard = Self::prep_op_slot(&mut self.ops, user_data, op).map_err(|e| {
            driver_error(
                DriverErrorKind::InvalidState,
                "iocp/driver",
                format!("failed to prepare op slot: {e:#}"),
            )
        })?;

        let is_rio_pool_waiting = guard
            .with_op_mut(|op| {
                matches!(
                    op.payload,
                    crate::ops::IocpOpPayload::UdpRecvStream(_)
                        | crate::ops::IocpOpPayload::UdpRecv(_)
                )
            })
            .unwrap_or(false);
        let overlapped = guard.storage.with_mut(|_op, _result, _payload, sidecar| {
            &mut sidecar.inner as *mut crate::win32::Overlapped
        });

        let mut ctx = SubmitContext {
            port: self.port.as_ref(),
            overlapped,
            ext: &self.extensions,
            registered_files: &self.registered_files,
            registrar: self.registrar.as_ref(),
            rio: &mut self.rio_state,
            slots_per_page,
            slab_resolver: &|idx| (idx == 0).then_some((slab_ptr, slab_len)),
        };

        let mut sub_guard = guard.start_submission_with(Some(|slot| {
            slot.storage
                .with_mut(|_op, _result, _payload, sidecar| sidecar.in_flight = false);
        }));
        let result = sub_guard
            .slot
            .as_mut()
            .and_then(|slot| slot.with_op_mut(|op| op.submit(&mut ctx)))
            .ok_or_else(|| {
                driver_error(
                    DriverErrorKind::InvalidState,
                    "iocp/driver",
                    "op missing during submission",
                )
            })?
            .to_driver_result(DriverErrorKind::Submission, "iocp/driver", "op submit failed");

        let pending_socket_key = if matches!(result, Ok(submit::SubmissionResult::Pending)) {
            sub_guard
                .slot
                .as_mut()
                .and_then(|slot| {
                    slot.with_op_mut(|op| {
                        op.header
                            .in_flight
                            .then_some(op.header.resolved_handle)
                            .flatten()
                    })
                })
                .flatten()
                .filter(|h| h.is_socket())
                .map(|h| h.actor_key())
        } else {
            None
        };

        let mut sub_guard_opt = Some(sub_guard);
        if result.is_ok() {
            let guard = sub_guard_opt
                .take()
                .ok_or_else(|| {
                    driver_error(
                        DriverErrorKind::InvalidState,
                        "iocp/driver",
                        "submission guard missing",
                    )
                })?;
            let _ = guard.persist();
        }
        drop(sub_guard_opt);

        if let Some(socket_key) = pending_socket_key {
            self.track_socket_submit_pending(socket_key);
        }

        Ok((is_rio_pool_waiting, result))
    }

    /// Registers a chunk of memory for RIO operations.
    pub(crate) fn register_chunk(&mut self, id: u16, ptr: *const u8, len: usize) -> DriverResult<()> {
        use crate::rio::error::RioResultExt;
        self.rio_state
            .register_chunk(id, ptr, len)
            .to_driver_result(
                DriverErrorKind::Registration,
                "iocp/driver",
                "failed to register RIO chunk",
            )?;
        Ok(())
    }

    /// Shuts down the RIO actor associated with the specified socket handle.
    /// This is used by both TCP and UDP teardown paths.
    pub fn shutdown_actor(&mut self, handle: RawHandle) {
        self.rio_state.shutdown_actor(handle.raw().actor_key());
    }

    /// Registers a set of file/socket handles for use with the driver.
    pub(crate) fn register_files<'a>(
        &mut self,
        files: Vec<RegisterFd<'a, IocpHandle>>,
    ) -> DriverResult<Vec<IoFd>> {
        let mut registered = Vec::with_capacity(files.len());
        for file in files {
            let (handle, is_owned_input) = match file {
                RegisterFd::Borrowed(h) => (RawHandle::new(h.raw()), false),
                RegisterFd::Owned(h) => (h.into_raw(), true),
            };
            // Trust enum semantics first; only probe file-tagged handles as fallback.
            let canonical = match handle.kind() {
                RawHandleKind::Socket => handle,
                RawHandleKind::File => {
                    if Self::detect_socket_from_file_handle(handle).map_err(|e| {
                        driver_error(
                            DriverErrorKind::InvalidInput,
                            "iocp/driver",
                            format!("detect socket from file handle failed: {e:#}"),
                        )
                    })? {
                        RawHandle::new(IocpHandle::for_socket(handle.raw().as_handle()))
                    } else {
                        handle
                    }
                }
            };
            let kind = canonical.kind();
            if kind == RawHandleKind::Socket {
                self.rio_state
                    .mark_socket_registered(canonical.raw().actor_key());
            }
            let entry = if is_owned_input {
                // SAFETY: ownership comes from RegisterFd::Owned and is transferred
                // into the registered slot for deterministic lifecycle management.
                RegisteredHandle::Owned(unsafe { crate::OwnedRawHandle::from_raw_owned(canonical) })
            } else {
                // Borrowed handles must remain non-owning to avoid accidental close/double-close.
                RegisteredHandle::Weak(canonical)
            };
            let idx = if let Some(idx) = self.free_slots.pop() {
                self.registered_files[idx] = Some(entry);
                self.rio_state.clear_registered_rq(idx);
                idx
            } else {
                self.registered_files.push(Some(entry));
                self.rio_state.resize_rqs(self.registered_files.len());
                self.registered_files.len() - 1
            };
            registered.push(IoFd::fixed(idx as u32));
        }
        Ok(registered)
    }

    /// Unregisters a set of previously registered files.
    pub(crate) fn unregister_files(&mut self, files: Vec<IoFd>) -> DriverResult<()> {
        for fd in files {
            let idx = fd.fixed_index() as usize;
            if idx < self.registered_files.len() {
                let Some(entry) = self.registered_files[idx].take() else {
                    continue;
                };
                if entry.as_raw().kind() == RawHandleKind::Socket {
                    self.rio_state
                        .shutdown_actor(entry.as_raw().raw().actor_key());
                }
                self.rio_state.clear_registered_rq(idx);
                self.free_slots.push(idx);
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
    ) -> DriverResult<()> {
        if pending_count == 0 {
            return Ok(());
        }
        let mut drained = 0usize;
        let deadline = Instant::now() + timeout;

        while drained < pending_count {
            if Instant::now() >= deadline {
                return Err(driver_error(
                    DriverErrorKind::Timeout,
                    "iocp/driver",
                    "drain timed out",
                ));
            }
            drained += self.poll_completion()?;
        }
        Ok(())
    }

    pub(crate) fn poll_completion(&mut self) -> DriverResult<usize> {
        let status = self
            .port
            .get_status(10)
            .to_driver_result(
                DriverErrorKind::Completion,
                "iocp/driver",
                "failed to poll IOCP status",
            )?;

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
                    return Ok(1);
                }
                if key == RIO_EVENT_KEY {
                    return self.rio_state.process_completions(
                        &mut self.ops,
                        &*self.registrar,
                        &self.completion_events,
                        &self.completion_table,
                    )
                    .map_err(|e| {
                        Self::with_report_detail(
                            DriverErrorKind::Completion,
                            "iocp/driver",
                            "failed to process rio completions",
                            format!("{e:#}"),
                        )
                    });
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

    pub(crate) fn close_impl(&mut self, mode: CloseMode) -> DriverResult<()> {
        if self.closed {
            return Ok(());
        }
        let pending = self.shutdown_ops();
        if let CloseMode::Strict { timeout } = mode {
            self.drain_pending_iocp(pending, timeout).map_err(|e| {
                Self::with_report_detail(
                    DriverErrorKind::Timeout,
                    "iocp/driver",
                    "drain pending iocp timed out",
                    format!("{e:#}"),
                )
            })?;
            self.rio_state.drain_outstanding(timeout).map_err(|e| {
                Self::with_report_detail(
                    DriverErrorKind::Completion,
                    "iocp/driver",
                    "failed to drain RIO outstanding requests",
                    format!("{e:#}"),
                )
            })?;
        }
        self.rio_state.kernel.close();
        self.closed = true;
        Ok(())
    }
}

impl Driver for IocpDriver {
    type Op = IocpOp;
    type Raw = IocpHandle;
    type Sidecar = OverlappedEntry;
    type Completion = usize;

    fn reserve_op(&mut self) -> DriverResult<(usize, u32)> {
        let (user_data, generation) = match self.ops.insert(OpEntry::new(IocpOpState::default())) {
            Ok(handle) => (handle.index, handle.generation),
            Err(_) => {
                return Err(driver_error(
                    DriverErrorKind::Registration,
                    "iocp/driver",
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

    fn detached_cancel_table(&self) -> Arc<DetachedCancelTable> {
        self.detached_cancel_table.clone()
    }

    fn slot_set_payload(&mut self, user_data: usize, payload: ErasedPayload) {
        let _ =
            self.ops
                .with_slot_storage_mut(user_data, |_op, _result, payload_cell, _sidecar| {
                    *payload_cell = Some(payload);
                });
    }

    fn slot_take_payload(&mut self, user_data: usize) -> Option<ErasedPayload> {
        let payload = self
            .ops
            .with_slot_storage_mut(user_data, |_op, _result, payload_cell, _sidecar| {
                payload_cell.take()
            })
            .flatten();
        self.ops.remove(user_data);
        payload
    }

    fn submit(
        &mut self,
        user_data: usize,
        op_in: &mut Option<Self::Op>,
        binder: SubmitBinder,
    ) -> Outcome<Result<Poll<()>, (DriverErrorReport, SubmitStatus)>> {
        if self.shutting_down {
            return binder.err(
                driver_os_error(
                    DriverErrorKind::System,
                    "iocp/driver",
                    windows_sys::Win32::Foundation::ERROR_OPERATION_ABORTED as i32,
                    "driver is shutting down",
                ),
                SubmitStatus::Void,
            );
        }
        let op = match op_in.take() {
            Some(op) => op,
            None => {
                return binder.err(
                    driver_error(
                        DriverErrorKind::InvalidInput,
                        "iocp/driver",
                        "submit called with empty option",
                    ),
                    SubmitStatus::Void,
                );
            }
        };

        let (is_rio_pool_waiting, result) = match self.call_op_submit(user_data, op) {
            Ok(res) => res,
            Err(e) => {
                return binder.err(
                    Self::with_report_detail(
                        DriverErrorKind::Submission,
                        "iocp/driver",
                        "call_op_submit failed",
                        format!("{e:#}"),
                    ),
                    SubmitStatus::Void,
                );
            }
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

    fn submit_background(&mut self, op: Self::Op) -> DriverResult<()> {
        if self.shutting_down {
            return Err(driver_os_error(
                DriverErrorKind::System,
                "iocp/driver",
                ERROR_OPERATION_ABORTED as i32,
                "driver is shutting down",
            ));
        }
        let (user_data, _) = self.reserve_op()?;
        let (_, result) = self.call_op_submit(user_data, op)?;

        match result {
            Ok(submit::SubmissionResult::Offload(task)) => {
                let (_, op_entry) = self
                    .ops
                    .get_slot_and_entry_mut(user_data)
                    .ok_or_else(|| {
                        driver_error(DriverErrorKind::Internal, "iocp/driver", "op not found")
                    })?;
                op_entry.platform_data.is_background = true;
                if get_blocking_pool().execute(task).is_err() {
                    let _ = std::mem::take(&mut op_entry.platform_data);
                    self.ops.shared.push_free(user_data);
                    return Err(driver_error(
                        DriverErrorKind::Submission,
                        "iocp/driver",
                        "thread pool overloaded",
                    ));
                }
            }
            Ok(_) => {
                let (_, op_entry) = self
                    .ops
                    .get_slot_and_entry_mut(user_data)
                    .ok_or_else(|| {
                        driver_error(DriverErrorKind::Internal, "iocp/driver", "op not found")
                    })?;
                op_entry.platform_data.is_background = true;
            }
            Err(e) => {
                let _ = self
                    .ops
                    .get_slot_entry_op_storage_and_entry_mut(user_data)
                    .and_then(|(_, _, op, _)| op.take());
                self.ops.remove(user_data);
                return Err(e);
            }
        }
        Ok(())
    }

    fn submit_queue(&mut self) -> DriverResult<()> {
        self.drain_cancel_requests();
        Ok(())
    }

    fn wait(&mut self) -> DriverResult<()> {
        self.get_completion(u32::MAX).map_err(|e| {
            Self::with_report_detail(
                DriverErrorKind::Completion,
                "iocp/driver",
                "wait for completion failed",
                format!("{e:#}"),
            )
        })
    }

    fn process_completions(&mut self) {
        if let Err(e) = self.get_completion(0) {
            tracing::error!(report = ?e, "iocp process_completions failed");
        }
    }

    fn completion_queue(&self) -> SharedCompletionQueue {
        self.completion_events.clone()
    }

    fn completion_table(&self) -> SharedCompletionTable {
        self.completion_table.clone()
    }

    fn wait_and_drain_completions(&mut self, out: &mut Vec<CompletionEvent>) -> DriverResult<usize> {
        self.get_completion(u32::MAX).map_err(|e| {
            Self::with_report_detail(
                DriverErrorKind::Completion,
                "iocp/driver",
                "wait for completion failed",
                format!("{e:#}"),
            )
        })?;
        Ok(self.drain_completions(out))
    }

    fn cancel_op(&mut self, user_data: usize) {
        self.cancel_op_internal(user_data);
    }

    fn register_chunk(&mut self, id: u16, ptr: *const u8, len: usize) -> DriverResult<()> {
        IocpDriver::register_chunk(self, id, ptr, len).map_err(|e| {
            Self::with_report_detail(
                DriverErrorKind::Registration,
                "iocp/driver",
                "register chunk failed",
                format!("{e:#}"),
            )
        })
    }

    fn register_files<'a>(
        &mut self,
        files: Vec<RegisterFd<'a, IocpHandle>>,
    ) -> DriverResult<Vec<IoFd>> {
        IocpDriver::register_files(self, files)
    }

    fn unregister_files(&mut self, files: Vec<IoFd>) -> DriverResult<()> {
        IocpDriver::unregister_files(self, files)
    }

    fn wake(&mut self) -> DriverResult<()> {
        IocpDriver::wake(self)
            .map_err(|e| driver_error(DriverErrorKind::Submission, "iocp/driver", format!("wakeup failed: {e:#}")))
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
        if let Err(e) = self.close_impl(CloseMode::Fast) {
            tracing::error!(report = ?e, "iocp close_impl fast failed during drop");
        }
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
