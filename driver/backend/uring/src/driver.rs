use diagweave::prelude::*;
use io_uring::IoUring;
use std::collections::{HashMap, VecDeque};
use std::io;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, mpsc};
use std::time::Instant;

use tracing::{debug, trace};

use crate::config::{
    BufferRegistrationMode, IoFd, IoMode, OwnedRawHandle, RawHandle, UringConfig, UringRawHandle,
};
use crate::diagnostics::UringCompletionDiagnostics;
use crate::error::{UringError, UringResult};
use crate::op::{UringOp, UringUserPayload};
use veloq_driver_core::DriverResult as CoreDriverResult;
use veloq_driver_core::driver::registry::{OpEntry, OpHandle};
use veloq_driver_core::driver::{
    CancelCompletionId, CancelMode, CancelRequest, CancelSubmitOutcome, DriveMode, DriveOutcome,
    Driver, DriverCompletionDiagnostics, DriverSubmitResult, OpToken, RegisterFd,
    RemoteCancelSender, RemoteWaker, SharedCompletionTable, SharedDriverSlotTable, SubmitStatus,
};

mod completion;
mod lifecycle;
mod registration;
mod submission;

pub use lifecycle::UringOpState;
pub(crate) use registration::{
    FileSlot, MAX_CHUNKS, RegisteredFileEntry, UringRegistrationStats, resolve_registered_fixed_fd,
};

use crate::op::{UringOpRegistry, UringSlotSpec};

type DriverResult<T> = CoreDriverResult<T, UringError>;
pub(crate) struct EventFd {
    pub(crate) fd: OwnedRawHandle,
}

pub(crate) struct WakerFdState {
    fd: Mutex<Arc<EventFd>>,
}

impl WakerFdState {
    #[inline]
    pub(crate) fn new(fd: Arc<EventFd>) -> Self {
        Self { fd: Mutex::new(fd) }
    }

    #[inline]
    fn lock_fd(&self) -> MutexGuard<'_, Arc<EventFd>> {
        self.fd
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    #[inline]
    pub(crate) fn current(&self) -> Arc<EventFd> {
        self.lock_fd().clone()
    }

    #[inline]
    pub(crate) fn replace(&self, fd: Arc<EventFd>) -> Arc<EventFd> {
        std::mem::replace(&mut *self.lock_fd(), fd)
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct PendingCancel {
    pub(crate) target: OpToken,
    pub(crate) mode: CancelMode,
}

impl PendingCancel {
    #[inline]
    pub(crate) const fn new(request: CancelRequest) -> Self {
        Self {
            target: request.target,
            mode: request.mode,
        }
    }

    #[inline]
    pub(crate) const fn user_parts(self) -> (usize, u32) {
        self.target.parts()
    }
}

pub(crate) struct UringWaker {
    pub(crate) state: Arc<WakerFdState>,
    pub(crate) is_waked: Arc<AtomicBool>,
}

impl RemoteWaker<UringError> for UringWaker {
    fn wake(&self) -> DriverResult<()> {
        if self.is_waked.load(Ordering::Relaxed) {
            return Ok(());
        }
        if !self.is_waked.swap(true, Ordering::AcqRel) {
            let buf = 1u64.to_ne_bytes();
            let fd = self.state.current();
            let ret = unsafe { libc::write(fd.fd.raw().as_fd(), buf.as_ptr() as *const _, 8) };
            if ret < 0 {
                let err = io::Error::last_os_error();
                if err.raw_os_error() == Some(libc::EAGAIN) {
                    return Ok(());
                }
                return Err(UringError::Internal
                    .to_report()
                    .push_ctx("scope", "uring.driver.waker.wake")
                    .set_error_code(err.raw_os_error().unwrap_or(libc::EIO))
                    .attach_note(err.to_string()));
            }
        }
        Ok(())
    }
}

pub struct UringDriver<'a> {
    pub(crate) ring: IoUring,
    pub(crate) ops: UringOpRegistry,
    pub(crate) backlog: VecDeque<OpToken>,
    pub(crate) pending_cancellations: VecDeque<PendingCancel>,
    pub(crate) pending_cancel_cqes: HashMap<CancelCompletionId, PendingCancel>,
    pub(crate) next_cancel_id: u16,
    pub(crate) completion_diagnostics: DriverCompletionDiagnostics<UringCompletionDiagnostics>,
    pub(crate) completion_table: SharedCompletionTable<UringSlotSpec>,
    pub(crate) remote_cancel_sender: RemoteCancelSender,
    pub(crate) remote_cancel_receiver: mpsc::Receiver<CancelRequest>,

    pub(crate) waker_state: Arc<WakerFdState>,
    pub(crate) waker_registered_fd: Option<IoFd>,
    pub(crate) waker_armed: bool,
    pub(crate) waker_buf: Box<[u8; 8]>,
    pub(crate) registered_chunks: veloq_bitset::BitSet,
    pub(crate) is_waked: Arc<AtomicBool>,

    pub(crate) wheel: veloq_wheel::Wheel<OpToken>,
    pub(crate) timer_buffer: Vec<OpToken>,
    pub(crate) last_timer_poll: Instant,
    pub(crate) registrar: Box<dyn veloq_buf::BufferRegistrar + 'a>,
    pub(crate) registration_stats: UringRegistrationStats,
    pub(crate) registration_mode: BufferRegistrationMode,
    pub(crate) chunk_register_failures_recent: HashMap<veloq_buf::heap::ChunkId, Instant>,
    pub(crate) file_slots: Vec<FileSlot>,
    pub(crate) free_file_slots: Vec<u32>,
    pub(crate) file_table_initialized: bool,
}

impl<'a> UringDriver<'a> {
    fn new_internal(
        config: impl AsRef<UringConfig>,
        registrar: Box<dyn veloq_buf::BufferRegistrar + 'a>,
    ) -> UringResult<Self> {
        let config = config.as_ref();
        let mut builder = IoUring::builder();

        builder
            .setup_coop_taskrun()
            .setup_single_issuer()
            .setup_defer_taskrun();

        if let IoMode::Polling(idle_ms) = config.mode {
            builder.setup_sqpoll(idle_ms.get());
        }

        let entries = config.entries.get();
        let ring = builder
            .build(entries)
            .or_else(|e| {
                if e.raw_os_error() == Some(libc::EINVAL) {
                    IoUring::new(entries)
                } else {
                    Err(e)
                }
            })
            .map_err(|e| UringError::DriverInit.io_report("driver.new.build_ring", e))?;

        let ops = UringOpRegistry::new(entries as usize);
        let completion_table: SharedCompletionTable<UringSlotSpec> = ops.shared.clone();
        let completion_diagnostics = ops.shared.completion_diagnostics();

        let waker_fd = Self::create_event_fd("driver.new.eventfd")?;

        debug!("Initalized UringDriver with {} entries", entries);

        let is_waked = Arc::new(AtomicBool::new(false));
        let (remote_cancel_sender, remote_cancel_receiver) = mpsc::channel();

        let mut driver = Self {
            ring,
            ops,
            backlog: VecDeque::new(),
            pending_cancellations: VecDeque::new(),
            pending_cancel_cqes: HashMap::new(),
            next_cancel_id: 1,
            completion_diagnostics,
            completion_table,
            remote_cancel_sender,
            remote_cancel_receiver,
            waker_state: Arc::new(WakerFdState::new(waker_fd)),
            waker_registered_fd: None,
            waker_armed: false,
            waker_buf: Box::new([0; 8]),
            registered_chunks: veloq_bitset::BitSet::new(MAX_CHUNKS),
            is_waked,

            wheel: veloq_wheel::Wheel::new(veloq_wheel::WheelConfig::default()),
            timer_buffer: Vec::new(),
            last_timer_poll: Instant::now(),
            registrar,
            registration_stats: UringRegistrationStats::default(),
            registration_mode: config.registration_mode,
            chunk_register_failures_recent: HashMap::new(),
            file_slots: Vec::new(),
            free_file_slots: Vec::new(),
            file_table_initialized: false,
        };

        driver.submit_waker()?;

        // Sparse registration
        let iovecs = vec![
            libc::iovec {
                iov_base: std::ptr::null_mut(),
                iov_len: 0
            };
            MAX_CHUNKS
        ];

        if let Err(e) = unsafe { driver.ring.submitter().register_buffers(&iovecs) } {
            tracing::warn!("Failed to register sparse buffers: {}", e);
        }

        Ok(driver)
    }

    pub fn new(
        config: impl AsRef<UringConfig>,
        registrar: Box<dyn veloq_buf::BufferRegistrar + 'a>,
    ) -> UringResult<Self> {
        Self::new_internal(config, registrar).attach_note("create uring driver")
    }

    fn has_active_ops_internal(&mut self) -> bool {
        self.ops.has_active_ops()
    }

    pub(crate) fn create_event_fd(scope: &'static str) -> UringResult<Arc<EventFd>> {
        let fd = unsafe { libc::eventfd(0, libc::EFD_CLOEXEC | libc::EFD_NONBLOCK) };
        if fd < 0 {
            return Err(UringError::DriverInit.io_report(scope, io::Error::last_os_error()));
        }
        Ok(Arc::new(EventFd {
            // SAFETY: `eventfd` returns a freshly created fd owned by this driver.
            fd: unsafe {
                OwnedRawHandle::from_raw_owned(RawHandle::new(UringRawHandle::for_file(fd)))
            },
        }))
    }

    pub(crate) fn rebuild_waker_fd(&mut self) -> UringResult<()> {
        let new_fd = Self::create_event_fd("driver.rebuild_waker_fd.eventfd")?;
        if let Some(fixed_fd) = self.waker_registered_fd {
            let raw = RawHandle::new(UringRawHandle::for_file(new_fd.fd.raw().as_fd()));
            self.replace_registered_fixed_fd(fixed_fd, raw)?;
        }
        let _old_fd = self.waker_state.replace(new_fd);
        Ok(())
    }
}

impl<'a> Drop for UringDriver<'a> {
    fn drop(&mut self) {}
}

impl<'a> Driver for UringDriver<'a> {
    type Op = UringOp;
    type UP = UringUserPayload;
    type Raw = UringRawHandle;
    type Sidecar = ();
    type Completion = usize;
    type Error = UringError;
    type SlotSpec = UringSlotSpec;

    fn reserve_op_raw(&mut self) -> DriverResult<OpToken> {
        match self.ops.insert(OpEntry::new(UringOpState::new())) {
            Ok(OpHandle {
                index: id,
                generation,
            }) => {
                trace!(id, generation, "Reserved op slot");
                OpToken::from_registry_parts(id, generation).map_err(|err| {
                    UringError::InvalidState
                        .to_report()
                        .push_ctx("scope", "uring.driver.reserve_op")
                        .with_ctx("slot_index", id)
                        .with_ctx("generation", generation)
                        .with_ctx("op_token_error", format!("{err:?}"))
                        .attach_note("reserved op slot cannot be encoded as completion token")
                })
            }
            Err(_) => {
                Err(UringError::InvalidState.report("uring.driver.reserve_op", "OpRegistry full"))
            }
        }
    }

    fn slot_table(&self) -> SharedDriverSlotTable<Self> {
        self.ops.shared.clone()
    }

    fn remote_cancel_sender(&self) -> RemoteCancelSender {
        self.remote_cancel_sender.clone()
    }

    fn try_recv_remote_cancel_request(&mut self) -> Option<CancelRequest> {
        self.remote_cancel_receiver.try_recv().ok()
    }

    fn slot_set_payload_raw(&mut self, token: OpToken, payload: UringUserPayload) {
        let _ = self
            .ops
            .with_slot_storage_mut(token, |_result, payload_cell, _sidecar| {
                *payload_cell = Some(payload);
            });
    }

    fn slot_take_payload_raw(&mut self, token: OpToken) -> Option<UringUserPayload> {
        self.ops
            .with_slot_storage_mut(token, |_result, payload_cell, _sidecar| payload_cell.take())
            .flatten()
    }

    fn release_op_slot_raw(&mut self, token: OpToken) {
        let _ = self.ops.remove(token);
    }

    fn submit_op_raw(
        &mut self,
        token: OpToken,
        op_in: &mut Option<Self::Op>,
    ) -> DriverSubmitResult<Self::Error> {
        let Some(op) = op_in.take() else {
            return DriverSubmitResult::failed(
                UringError::InvalidState
                    .report("driver.submit", "submit called with empty Option")
                    .push_ctx("scope", "uring.driver.submit")
                    .attach_note("submit called with empty Option"),
                SubmitStatus::Void,
            );
        };
        let op: UringOp = op;
        let strategy = op.vtable.strategy;

        match strategy {
            crate::op::SubmissionStrategy::SubmitSqe => self.submit_sqe_internal(token, op, op_in),
            crate::op::SubmissionStrategy::SoftwareTimer => {
                self.submit_timer_internal(token, op, op_in)
            }
        }
    }

    fn drive(&mut self, mode: DriveMode) -> DriverResult<DriveOutcome> {
        match mode {
            DriveMode::Poll => {
                self.poll_nonblocking_internal()
                    .push_ctx("scope", "uring.driver.drive.poll")
                    .attach_note("poll completions")?;
            }
            DriveMode::Wait => {
                let pending_progress =
                    self.has_active_ops_internal() || self.ops.shared.has_ready_completion();
                if !pending_progress {
                    return Ok(DriveOutcome {
                        next_timeout_hint: self.wheel.next_timeout(),
                        pending_progress,
                    });
                }
                self.wait_internal()
                    .push_ctx("scope", "uring.driver.drive.wait")
                    .attach_note("wait for completions")?;
            }
        }

        let pending_progress =
            self.has_active_ops_internal() || self.ops.shared.has_ready_completion();
        Ok(DriveOutcome {
            next_timeout_hint: self.wheel.next_timeout(),
            pending_progress,
        })
    }

    fn completion_table(&self) -> SharedCompletionTable<Self::SlotSpec> {
        self.completion_table.clone()
    }

    fn cancel_op(&mut self, request: CancelRequest) -> DriverResult<CancelSubmitOutcome> {
        Ok(self.cancel_op_internal(request))
    }

    fn register_chunk(
        &mut self,
        id: veloq_buf::heap::ChunkId,
        ptr: *const u8,
        len: usize,
    ) -> DriverResult<()> {
        self.register_chunk_internal(id, ptr, len)
            .push_ctx("scope", "uring.driver.register_chunk")
            .with_ctx("driver_error_kind", UringError::Registration.to_string())
            .attach_note("register chunk")
    }

    fn register_files<'f>(
        &mut self,
        files: Vec<RegisterFd<'f, UringRawHandle>>,
    ) -> DriverResult<Vec<IoFd>> {
        self.register_files_internal(files)
            .push_ctx("scope", "uring.driver.register_files")
            .attach_note("register files")
    }

    fn unregister_files(&mut self, files: Vec<IoFd>) -> DriverResult<()> {
        for fd in files {
            self.unregister_fixed_fd(fd)
                .push_ctx("scope", "uring.driver.unregister_files")
                .attach_note("unregister fixed fd")?;
        }
        Ok(())
    }

    fn create_waker(&self) -> Arc<dyn RemoteWaker<UringError>> {
        Arc::new(UringWaker {
            state: self.waker_state.clone(),
            is_waked: self.is_waked.clone(),
        })
    }
}

#[cfg(feature = "test-hooks")]
impl veloq_driver_core::driver::test_hooks::DriverTestHooks for UringDriver<'_> {
    fn debug_chunk_register_attempts(&self) -> u64 {
        self.registration_stats.chunk_register_attempts
    }
}
