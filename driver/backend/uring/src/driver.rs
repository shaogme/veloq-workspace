use diagweave::prelude::*;
use io_uring::IoUring;
use std::collections::{HashMap, VecDeque};
use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

use tracing::{debug, trace};

use crate::config::{
    BufferRegistrationMode, IoFd, IoMode, OwnedRawHandle, RawHandle, UringConfig, UringRawHandle,
};
use crate::error::{UringError, UringResult};
use crate::op::{UringOp, UringUserPayload};
use veloq_driver_core::DriverResult as CoreDriverResult;
use veloq_driver_core::driver::registry::{OpEntry, OpHandle};
use veloq_driver_core::driver::{
    DriveMode, DriveOutcome, Driver, DriverSubmitResult, RegisterFd, RemoteWaker,
    SharedCompletionQueue, SharedCompletionTable, SharedDriverSlotTable, SubmitStatus,
};
use veloq_driver_core::slot::DetachedCancelTable;

mod completion;
mod lifecycle;
mod registration;
mod submission;

pub use lifecycle::UringOpState;
pub(crate) use registration::{MAX_CHUNKS, RegisteredFileEntry, UringRegistrationStats};

use crate::op::slot::{UringOpRegistry, UringOpRegistryExt, UringSlotSpec};

type DriverResult<T> = CoreDriverResult<T, UringError>;
pub(crate) struct EventFd {
    pub(crate) fd: OwnedRawHandle,
}

pub(crate) struct UringWaker {
    pub(crate) fd: Arc<EventFd>,
    pub(crate) is_waked: Arc<AtomicBool>,
}

impl RemoteWaker<UringError> for UringWaker {
    fn wake(&self) -> DriverResult<()> {
        if self.is_waked.load(Ordering::Relaxed) {
            return Ok(());
        }
        if !self.is_waked.swap(true, Ordering::AcqRel) {
            let buf = 1u64.to_ne_bytes();
            let ret = unsafe { libc::write(self.fd.fd.raw().as_fd(), buf.as_ptr() as *const _, 8) };
            if ret < 0 {
                let err = io::Error::last_os_error();
                if err.raw_os_error() == Some(libc::EAGAIN) {
                    return Ok(());
                }
                return Err(UringError::Internal
                    .to_report()
                    .with_ctx("scope", "uring.driver.waker.wake")
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
    pub(crate) backlog: VecDeque<usize>,
    pub(crate) pending_cancellations: VecDeque<usize>,
    pub(crate) completion_events: SharedCompletionQueue,
    pub(crate) completion_table: SharedCompletionTable<UringUserPayload, UringError>,
    pub(crate) detached_cancel_table: Arc<DetachedCancelTable>,

    pub(crate) waker_fd: Arc<EventFd>,
    pub(crate) waker_registered_fd: Option<IoFd>,
    pub(crate) waker_token: Option<usize>,
    pub(crate) registered_chunks: veloq_bitset::BitSet,
    pub(crate) is_waked: Arc<AtomicBool>,

    pub(crate) wheel: veloq_wheel::Wheel<usize>,
    pub(crate) timer_buffer: Vec<usize>,
    pub(crate) last_timer_poll: Instant,
    pub(crate) registrar: Box<dyn veloq_buf::BufferRegistrar + 'a>,
    pub(crate) registration_stats: UringRegistrationStats,
    pub(crate) registration_mode: BufferRegistrationMode,
    pub(crate) chunk_register_failures_recent: HashMap<u16, Instant>,
    pub(crate) registered_files: Vec<Option<RegisteredFileEntry>>,
    pub(crate) file_generations: Vec<u64>,
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
        let completion_table: SharedCompletionTable<UringUserPayload, UringError> =
            ops.shared.clone();

        let waker_fd = unsafe { libc::eventfd(0, libc::EFD_CLOEXEC | libc::EFD_NONBLOCK) };
        if waker_fd < 0 {
            return Err(
                UringError::DriverInit.io_report("driver.new.eventfd", io::Error::last_os_error())
            );
        }

        debug!("Initalized UringDriver with {} entries", entries);

        let is_waked = Arc::new(AtomicBool::new(false));

        let mut driver = Self {
            ring,
            ops,
            backlog: VecDeque::new(),
            pending_cancellations: VecDeque::new(),
            completion_events: std::sync::Arc::new(crossbeam_queue::SegQueue::new()),
            completion_table,
            detached_cancel_table: Arc::new(DetachedCancelTable::new(entries as usize)),
            waker_fd: Arc::new(EventFd {
                // SAFETY: `eventfd` returns a freshly created fd owned by this driver.
                fd: unsafe {
                    OwnedRawHandle::from_raw_owned(RawHandle::new(UringRawHandle::for_file(
                        waker_fd,
                    )))
                },
            }),
            waker_registered_fd: None,
            waker_token: None,
            registered_chunks: veloq_bitset::BitSet::new(MAX_CHUNKS),
            is_waked,

            wheel: veloq_wheel::Wheel::new(veloq_wheel::WheelConfig::default()),
            timer_buffer: Vec::new(),
            last_timer_poll: Instant::now(),
            registrar,
            registration_stats: UringRegistrationStats::default(),
            registration_mode: config.registration_mode,
            chunk_register_failures_recent: HashMap::new(),
            registered_files: Vec::new(),
            file_generations: Vec::new(),
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
        Self::new_internal(config, registrar).map_err(|e| e.attach_note("create uring driver"))
    }

    fn has_active_ops_internal(&mut self) -> bool {
        self.ops.has_active_ops()
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

    fn reserve_op(&mut self) -> DriverResult<(usize, u32)> {
        match self.ops.insert(OpEntry::new(UringOpState::new())) {
            Ok(OpHandle {
                index: id,
                generation,
            }) => {
                trace!(id, generation, "Reserved op slot");
                self.ops.slot_reserve(id);
                Ok((id, generation))
            }
            Err(_) => {
                Err(UringError::InvalidState.report("uring.driver.reserve_op", "OpRegistry full"))
            }
        }
    }

    fn slot_table(&self) -> SharedDriverSlotTable<Self> {
        self.ops.shared.clone()
    }

    fn detached_cancel_table(&self) -> std::sync::Arc<DetachedCancelTable> {
        self.detached_cancel_table.clone()
    }

    fn slot_set_payload(&mut self, user_data: usize, payload: UringUserPayload) {
        let _ = self
            .ops
            .with_slot_storage_mut(user_data, |_result, payload_cell, _sidecar| {
                *payload_cell = Some(payload);
            });
    }

    fn slot_take_payload(&mut self, user_data: usize) -> Option<UringUserPayload> {
        self.ops
            .with_slot_storage_mut(user_data, |_result, payload_cell, _sidecar| {
                payload_cell.take()
            })
            .flatten()
    }

    fn submit(
        &mut self,
        user_data: usize,
        op_in: &mut Option<Self::Op>,
    ) -> DriverSubmitResult<Self::Error> {
        let Some(op) = op_in.take() else {
            return DriverSubmitResult::failed(
                UringError::InvalidState
                    .report("driver.submit", "submit called with empty Option")
                    .with_ctx("scope", "uring.driver.submit")
                    .attach_note("submit called with empty Option"),
                SubmitStatus::Void,
            );
        };
        let op: UringOp = op;
        let strategy = op.vtable.strategy;
        if strategy == crate::op::SubmissionStrategy::BackgroundOnly {
            *op_in = Some(op);
            return DriverSubmitResult::failed(
                UringError::Unsupported.report(
                    "uring.driver.submit",
                    "background op cannot be submitted normally",
                ),
                SubmitStatus::Void,
            );
        }

        match strategy {
            crate::op::SubmissionStrategy::BackgroundOnly => DriverSubmitResult::failed(
                UringError::InvalidState
                    .report(
                        "driver.submit",
                        "background strategy reached normal submit path",
                    )
                    .with_ctx("scope", "uring.driver.submit")
                    .attach_note("background strategy reached normal submit path"),
                SubmitStatus::Void,
            ),
            crate::op::SubmissionStrategy::SubmitSqe => {
                self.submit_sqe_internal(user_data, op, op_in)
            }
            crate::op::SubmissionStrategy::SoftwareTimer => {
                self.submit_timer_internal(user_data, op, op_in)
            }
        }
    }

    fn drive(&mut self, mode: DriveMode) -> DriverResult<DriveOutcome> {
        match mode {
            DriveMode::Poll => {
                self.poll_nonblocking_internal()
                    .with_ctx("scope", "uring.driver.drive.poll")
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
                    .with_ctx("scope", "uring.driver.drive.wait")
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

    fn completion_queue(&self) -> SharedCompletionQueue {
        self.completion_events.clone()
    }

    fn completion_table(&self) -> SharedCompletionTable<UringUserPayload, UringError> {
        self.completion_table.clone()
    }

    fn cancel_op(&mut self, user_data: usize) {
        self.cancel_op_internal(user_data);
    }

    fn register_chunk(&mut self, id: u16, ptr: *const u8, len: usize) -> DriverResult<()> {
        self.register_chunk_internal(id, ptr, len)
            .with_ctx("scope", "uring.driver.register_chunk")
            .with_ctx("driver_error_kind", UringError::Registration.to_string())
            .attach_note("register chunk")
    }

    fn register_files<'f>(
        &mut self,
        files: Vec<RegisterFd<'f, UringRawHandle>>,
    ) -> DriverResult<Vec<IoFd>> {
        self.register_files_internal(files)
            .with_ctx("scope", "uring.driver.register_files")
            .attach_note("register files")
    }

    fn unregister_files(&mut self, files: Vec<IoFd>) -> DriverResult<()> {
        for fd in files {
            self.unregister_fixed_fd(fd)
                .with_ctx("scope", "uring.driver.unregister_files")
                .attach_note("unregister fixed fd")?;
        }
        Ok(())
    }

    fn create_waker(&self) -> Arc<dyn RemoteWaker<UringError>> {
        Arc::new(UringWaker {
            fd: self.waker_fd.clone(),
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
