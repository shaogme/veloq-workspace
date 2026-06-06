pub(crate) mod completion;
pub(crate) mod registration;
pub(crate) mod submission;

pub(crate) const RIO_EVENT_KEY: usize = usize::MAX - 1;
pub(crate) type PreInit = crate::win32::IoCompletionPort;

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::{Duration, Instant};

use crossbeam_queue::SegQueue;
use tracing::{debug, trace};

use veloq_buf::BufferRegistrar;
use veloq_driver_core::DriverResult as CoreDriverResult;
use veloq_driver_core::driver::registry::OpEntry;
use veloq_driver_core::driver::{
    CompletionSidecar as CoreCompletionSidecar, DriveMode, DriveOutcome, Driver,
    DriverSubmitResult, RegisterFd, RemoteWaker, SharedCompletionQueue, SharedCompletionTable,
    SharedDriverSlotTable, SubmitStatus,
};
use veloq_driver_core::slot::DetachedCancelTable;
use veloq_wheel::{Wheel, WheelConfig};

use diagweave::prelude::*;

use crate::common::IocpWaker;
use crate::config::{BufferRegistrationMode, IoFd, IocpConfig, IocpHandle, RegisteredHandle};
use crate::error::{IocpError, IocpResult};
use crate::op::{IocpOp, IocpOpPayload, IocpUserPayload};
use crate::rio::RioState;

pub(crate) type IocpDriverResult<T> = CoreDriverResult<T, IocpError>;
pub(crate) use crate::op::slot::{IocpOpRegistry, IocpSlotSpec};

// ============================================================================
// State & Lifecycle Types
// ============================================================================

/// The IOCP driver implementation that manages I/O completion ports and operations.
pub struct IocpDriver<'a> {
    pub(crate) port: Arc<crate::win32::IoCompletionPort>,
    pub(crate) ops: IocpOpRegistry,
    pub(crate) extensions: crate::ext::Extensions,
    pub(crate) wheel: Wheel<usize>,
    pub(crate) timer_buffer: Vec<usize>,
    pub(crate) last_timer_poll: Instant,
    pub(crate) registered_files: Vec<Option<RegisteredHandle>>,
    pub(crate) file_generations: Vec<u64>,
    pub(crate) free_slots: Vec<usize>,
    pub(crate) is_notified: Arc<AtomicBool>,
    pub(crate) completion_events: SharedCompletionQueue,
    pub(crate) completion_table: SharedCompletionTable<IocpUserPayload, IocpError>,
    pub(crate) detached_cancel_table: Arc<DetachedCancelTable>,

    // RIO Support (required)
    pub(crate) rio_state: RioState,
    pub(crate) registrar: Box<dyn BufferRegistrar + 'a>,
    pub(crate) shutting_down: bool,
    pub(crate) closed: bool,
    pub(crate) deferred_socket_cleanup: VecDeque<registration::DeferredSocketCleanup>,
    pub(crate) socket_generation_counter: u64,
}

/// State associated with an IOCP operation.
#[derive(Default)]
pub struct IocpOpState {
    pub(crate) generation: u32,
    pub(crate) timer_id: Option<veloq_wheel::TaskId>,
    pub(crate) timer_deadline: Option<Instant>,
    pub(crate) is_background: bool,
    // For RIO cancel path: the slot can be recycled only after both:
    // 1) user has consumed completion; 2) late RIO CQE has been drained.
    pub(crate) rio_needs_drain: bool,
    pub(crate) rio_drained: bool,
}

/// Closing mode for the driver or operations.
#[derive(Clone, Copy, Debug)]
pub enum CloseMode {
    /// Closes quickly without waiting for pending operations.
    Fast,
    /// Closes after a specified timeout, allowing pending operations to finish.
    Strict { timeout: Duration },
}

pub(crate) type CompletionSidecar = CoreCompletionSidecar<IocpUserPayload, IocpError>;

impl<'a> IocpDriver<'a> {
    /// Checks if the provided operation is a RIO-based operation.
    pub(crate) fn is_rio_op(op: &IocpOp) -> bool {
        matches!(
            op.payload,
            IocpOpPayload::Recv(_)
                | IocpOpPayload::Send(_)
                | IocpOpPayload::UdpRecv(_)
                | IocpOpPayload::UdpSend(_)
                | IocpOpPayload::SendTo(_)
                | IocpOpPayload::UdpRecvFrom(_)
        )
    }

    /// Creates a pre-initialization completion port handle.
    pub(crate) fn create_pre_init() -> IocpResult<PreInit> {
        crate::win32::IoCompletionPort::new(0).attach_note("failed to create pre-init IOCP")
    }

    /// Creates a new IOCP driver instance.
    pub fn new(
        config: impl AsRef<IocpConfig>,
        registrar: Box<dyn BufferRegistrar + 'a>,
    ) -> IocpResult<Self> {
        let cfg = config.as_ref();
        let pre = Self::create_pre_init()?;
        Self::new_from_pre_init(cfg.entries.get(), pre, cfg.registration_mode, registrar)
    }

    /// Creates a new IOCP driver from a pre-initialized handle.
    pub(crate) fn new_from_pre_init(
        entries: u32,
        port_val: PreInit,
        registration_mode: BufferRegistrationMode,
        registrar: Box<dyn BufferRegistrar + 'a>,
    ) -> IocpResult<Self> {
        use windows_sys::Win32::Networking::WinSock::{WSADATA, WSAStartup};
        // SAFETY: WSAStartup is required for networking on Windows.
        // It is called here to avoid global static initialization.
        unsafe {
            let mut data: WSADATA = std::mem::zeroed();
            let _ = WSAStartup(0x0202, &mut data);
        }

        let port_handle = port_val.as_raw();
        debug!(port = ?port_handle, "Initializing IocpDriver");
        let extensions = crate::ext::Extensions::new().attach_note(format!(
            "failed to load IOCP extensions, port={port_handle:?}"
        ))?;
        let rio_state = RioState::new(
            crate::config::RawHandle::new(IocpHandle::for_file(port_handle)).borrow(),
            entries,
            &extensions,
            registration_mode,
        )
        .attach_note(format!(
            "failed to initialize RIO state, entries={entries}, port={port_handle:?}"
        ))
        .trans()?;
        let ops = IocpOpRegistry::new(entries as usize);
        let completion_table: SharedCompletionTable<IocpUserPayload, IocpError> =
            ops.shared.clone();
        Ok(Self {
            port: Arc::new(port_val),
            ops,
            extensions,
            wheel: Wheel::new(WheelConfig::default()),
            timer_buffer: Vec::new(),
            last_timer_poll: Instant::now(),
            registered_files: Vec::new(),
            file_generations: Vec::new(),
            free_slots: Vec::new(),
            is_notified: Arc::new(AtomicBool::new(false)),
            completion_events: Arc::new(SegQueue::new()),
            completion_table,
            detached_cancel_table: Arc::new(DetachedCancelTable::new(entries as usize)),
            rio_state,
            registrar,
            shutting_down: false,
            closed: false,
            deferred_socket_cleanup: VecDeque::new(),
            socket_generation_counter: 1,
        })
    }

    pub(crate) fn has_active_ops_internal(&mut self) -> bool {
        self.ops.has_active_ops()
    }

    pub(crate) fn create_waker(&self) -> Arc<dyn RemoteWaker<IocpError>> {
        Arc::new(IocpWaker {
            port: self.port.clone(),
            is_notified: self.is_notified.clone(),
        })
    }
}

impl<'a> Driver for IocpDriver<'a> {
    type Op = crate::op::IocpOp;
    type UP = IocpUserPayload;
    type Raw = IocpHandle;
    type Sidecar = crate::op::OverlappedEntry;
    type Completion = usize;
    type Error = IocpError;
    type SlotSpec = IocpSlotSpec;

    fn reserve_op(&mut self) -> IocpDriverResult<(usize, u32)> {
        let (user_data, generation) = match self.ops.insert(OpEntry::new(IocpOpState::default())) {
            Ok(handle) => (handle.index, handle.generation),
            Err(_) => {
                return Err(IocpError::Registration.report("iocp/driver", "OpRegistry is full"));
            }
        };
        trace!(user_data, generation, "Reserved op slot");
        Ok((user_data, generation))
    }

    fn slot_table(&self) -> SharedDriverSlotTable<Self> {
        self.ops.shared.clone()
    }

    fn detached_cancel_table(&self) -> Arc<DetachedCancelTable> {
        self.detached_cancel_table.clone()
    }

    fn slot_set_payload(&mut self, user_data: usize, payload: Self::UP) {
        let _ = self
            .ops
            .with_slot_storage_mut(user_data, |_result, payload_cell, _sidecar| {
                *payload_cell = Some(payload);
            });
    }

    fn slot_take_payload(&mut self, user_data: usize) -> Option<Self::UP> {
        use std::sync::atomic::Ordering;
        let payload = self
            .ops
            .with_slot_storage_mut(user_data, |_result, payload_cell, _sidecar| {
                payload_cell.take()
            })
            .flatten();
        let generation = self.ops.shared.slots[user_data].generation(Ordering::Acquire);
        self.ops.recycle(user_data, generation.wrapping_add(1));
        payload
    }

    fn submit(
        &mut self,
        user_data: usize,
        op_in: &mut Option<Self::Op>,
    ) -> DriverSubmitResult<Self::Error> {
        if self.shutting_down {
            return DriverSubmitResult::failed(
                IocpError::Internal
                    .to_report()
                    .with_ctx("scope", "iocp/driver")
                    .set_error_code(windows_sys::Win32::Foundation::ERROR_OPERATION_ABORTED as i32)
                    .attach_note("driver is shutting down"),
                SubmitStatus::Void,
            );
        }
        let op = match op_in.take() {
            Some(op) => op,
            None => {
                return DriverSubmitResult::failed(
                    IocpError::InvalidInput
                        .report("iocp/driver", "submit called with empty option"),
                    SubmitStatus::Void,
                );
            }
        };

        let result = match self.call_op_submit(user_data, op) {
            Ok(res) => res,
            Err(e) => {
                return DriverSubmitResult::failed(
                    e.with_ctx("scope", "iocp/driver")
                        .attach_note("call_op_submit failed"),
                    SubmitStatus::Void,
                );
            }
        };

        let ctx = submission::SubmitContextInternal {
            port: self.port.as_ref(),
            wheel: &mut self.wheel,
            completion_events: &self.completion_events,
            completion_table: &self.completion_table,
        };

        Self::on_submit_res(&mut self.ops, ctx, result, user_data, op_in)
    }

    fn drive(&mut self, mode: DriveMode) -> IocpDriverResult<DriveOutcome> {
        match mode {
            DriveMode::Poll => {
                self.get_completion(0)
                    .with_ctx("scope", "iocp/driver.drive.poll")
                    .attach_note("drive(Poll) failed")?;
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
                self.get_completion(u32::MAX)
                    .with_ctx("scope", "iocp/driver.drive.wait")
                    .attach_note("wait for completion failed")?;
            }
        }

        let pending_progress =
            self.has_active_ops_internal() || self.ops.shared.has_ready_completion();
        Ok(DriveOutcome {
            next_timeout_hint: self.wheel.next_timeout(),
            pending_progress,
        })
    }

    fn completion_queue(&self) -> SharedCompletionQueue {
        self.completion_events.clone()
    }

    fn completion_table(&self) -> SharedCompletionTable<Self::UP, Self::Error, Self::Completion> {
        self.completion_table.clone()
    }

    fn cancel_op(&mut self, user_data: usize) {
        self.cancel_op_internal(user_data);
    }

    fn register_chunk(&mut self, id: u16, ptr: *const u8, len: usize) -> IocpDriverResult<()> {
        IocpDriver::register_chunk(self, id, ptr, len).map_err(|e| {
            e.with_ctx("scope", "iocp/driver")
                .attach_note("register chunk failed")
        })
    }

    fn register_files<'f>(
        &mut self,
        files: Vec<RegisterFd<'f, IocpHandle>>,
    ) -> IocpDriverResult<Vec<IoFd>> {
        IocpDriver::register_files(self, files)
    }

    fn unregister_files(&mut self, files: Vec<IoFd>) -> IocpDriverResult<()> {
        IocpDriver::unregister_files(self, files)
    }

    fn create_waker(&self) -> Arc<dyn RemoteWaker<IocpError>> {
        IocpDriver::create_waker(self)
    }
}

impl Drop for IocpDriver<'_> {
    fn drop(&mut self) {
        debug!("Dropping IocpDriver");
        if let Err(e) = self.close_impl(CloseMode::Fast) {
            tracing::error!(report = ?e, "iocp close_impl fast failed during drop");
        }
    }
}

#[cfg(feature = "test-hooks")]
impl veloq_driver_core::driver::test_hooks::DriverTestHooks for IocpDriver<'_> {
    fn debug_chunk_register_attempts(&self) -> u64 {
        self.rio_state
            .registry
            .registration_stats
            .chunk_register_attempts
    }
}
