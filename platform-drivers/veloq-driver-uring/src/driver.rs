use error_stack::Report;
use io_uring::{IoUring, squeue};
use std::collections::{HashMap, VecDeque};
use std::io;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::Poll;
use std::time::Instant;

use tracing::{debug, trace};

use crate::config::{
    BufferRegistrationMode, IoFd, IoMode, OwnedRawHandle, RawHandle, UringConfig, UringRawHandle,
};
use crate::error::{UringError, UringResult, UringResultExt, from_io_error};
use crate::op::{SubmissionStrategy, UringOp};
use veloq_driver_core::driver::{
    CompletionEvent, CompletionSidecar, Driver, Outcome, RegisterFd, RemoteWaker,
    SharedCompletionQueue, SharedCompletionTable, SubmitBinder, SubmitStatus,
    encode_completion_token,
};
use veloq_driver_core::error::{
    DriverErrorKind, DriverErrorReport, DriverResult, driver_error,
    driver_error_report_to_event_res, driver_os_error,
};
use veloq_driver_core::op::{IntoPlatformOp, Wakeup};
use veloq_driver_core::op_registry::{AllocResult, OpEntry, OpHandle, OpRegistry};
use veloq_driver_core::slot::DetachedCancelTable;

mod lifecycle;
mod registration;

pub use lifecycle::UringOpState;
pub(crate) use registration::{MAX_CHUNKS, UringRegistrationStats};

use crate::op::slot::{Slot, SlotView, UringOpRegistryExt};

pub(crate) struct EventFd {
    pub(crate) fd: OwnedRawHandle,
}

pub(crate) struct UringWaker {
    pub(crate) fd: Arc<EventFd>,
    pub(crate) is_waked: Arc<AtomicBool>,
}

impl RemoteWaker for UringWaker {
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
                return Err(driver_os_error(
                    DriverErrorKind::System,
                    "uring.driver.waker.wake",
                    err.raw_os_error().unwrap_or(libc::EIO),
                    err.to_string(),
                ));
            }
        }
        Ok(())
    }
}

pub(crate) const CANCEL_USER_DATA: u64 = u64::MAX - 1;
pub(crate) const BACKGROUND_USER_DATA: u64 = u64::MAX - 2;
const MIN_FILE_TABLE_CAPACITY: usize = 1;

#[inline]
fn invalid_state(scope: &'static str, msg: impl Into<String>) -> Report<UringError> {
    Report::new(UringError::InvalidState).attach(format!("{scope}: {}", msg.into()))
}

#[inline]
fn invalid_input(scope: &'static str, msg: impl Into<String>) -> Report<UringError> {
    Report::new(UringError::InvalidInput).attach(format!("{scope}: {}", msg.into()))
}

#[inline]
fn unsupported(scope: &'static str, msg: impl Into<String>) -> Report<UringError> {
    Report::new(UringError::Unsupported).attach(format!("{scope}: {}", msg.into()))
}

#[inline]
fn map_uring_error(
    report: Report<UringError>,
    kind: DriverErrorKind,
    scope: &'static str,
    detail: impl ToString,
) -> DriverErrorReport {
    let detail_text = detail.to_string();
    match Err::<(), _>(report).to_driver_result(kind, scope, detail_text.clone()) {
        Ok(()) => driver_error(
            DriverErrorKind::Internal,
            scope,
            format!("unexpected ok while mapping uring report: {detail_text}"),
        ),
        Err(e) => e,
    }
}

#[derive(Debug)]
pub(crate) enum RegisteredFileEntry {
    BorrowedFd(i32),
    OwnedHandle(OwnedRawHandle),
}

pub struct UringDriver {
    pub(crate) ring: IoUring,
    pub(crate) ops: OpRegistry<UringOp, UringOpState, ()>,
    pub(crate) backlog: VecDeque<usize>,
    pub(crate) pending_cancellations: VecDeque<usize>,
    pub(crate) completion_events: SharedCompletionQueue,
    pub(crate) completion_table: SharedCompletionTable,
    pub(crate) detached_cancel_table: Arc<DetachedCancelTable>,

    pub(crate) waker_fd: Arc<EventFd>,
    pub(crate) waker_registered_fd: Option<IoFd>,
    pub(crate) waker_token: Option<usize>,
    pub(crate) waker_payload: Option<Box<Wakeup>>,
    pub(crate) registered_chunks: veloq_bitset::BitSet,
    pub(crate) is_waked: Arc<AtomicBool>,

    pub(crate) wheel: veloq_wheel::Wheel<usize>,
    pub(crate) timer_buffer: Vec<usize>,
    pub(crate) registrar: Box<dyn veloq_buf::BufferRegistrar>,
    pub(crate) registration_stats: UringRegistrationStats,
    pub(crate) registration_mode: BufferRegistrationMode,
    pub(crate) chunk_register_failures_recent: HashMap<u16, Instant>,
    pub(crate) registered_files: Vec<Option<RegisteredFileEntry>>,
    pub(crate) free_file_slots: Vec<u32>,
    pub(crate) file_table_initialized: bool,
}

impl UringDriver {
    fn unregister_fixed_fd(&mut self, fd: IoFd) -> UringResult<()> {
        if !self.file_table_initialized {
            return Ok(());
        }
        let idx = fd.fixed_index();
        let index = idx as usize;
        if index < self.registered_files.len() {
            let Some(_entry) = self.registered_files[index].take() else {
                return Ok(());
            };
            self.ring
                .submitter()
                .register_files_update(idx, &[-1])
                .map_err(|e| {
                    from_io_error(UringError::Registration, "driver.unregister_fixed_fd", e)
                })?;
            self.free_file_slots.push(idx);
        }
        Ok(())
    }

    fn ensure_file_table_initialized(&mut self) -> UringResult<()> {
        if self.file_table_initialized {
            return Ok(());
        }

        let capacity = self.ops.local.len().max(MIN_FILE_TABLE_CAPACITY);
        let sparse = vec![-1; capacity];
        self.ring.submitter().register_files(&sparse).map_err(|e| {
            from_io_error(
                UringError::Registration,
                "driver.ensure_file_table_initialized",
                e,
            )
        })?;

        self.registered_files = (0..capacity).map(|_| None).collect();
        self.free_file_slots = (0..capacity as u32).rev().collect();
        self.file_table_initialized = true;
        Ok(())
    }

    fn new_internal(config: impl AsRef<UringConfig>) -> UringResult<Self> {
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
            .map_err(|e| from_io_error(UringError::DriverInit, "driver.new.build_ring", e))?;

        let ops = OpRegistry::new(entries as usize);
        let completion_table: SharedCompletionTable = ops.shared.clone();

        let waker_fd = unsafe { libc::eventfd(0, libc::EFD_CLOEXEC | libc::EFD_NONBLOCK) };
        if waker_fd < 0 {
            return Err(from_io_error(
                UringError::DriverInit,
                "driver.new.eventfd",
                io::Error::last_os_error(),
            ));
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
            waker_payload: None,
            registered_chunks: veloq_bitset::BitSet::new(MAX_CHUNKS),
            is_waked,

            wheel: veloq_wheel::Wheel::new(veloq_wheel::WheelConfig::default()),
            timer_buffer: Vec::new(),
            registrar: Box::new(veloq_buf::NoopRegistrar),
            registration_stats: UringRegistrationStats::default(),
            registration_mode: config.registration_mode,
            chunk_register_failures_recent: HashMap::new(),
            registered_files: Vec::new(),
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

    pub fn new(config: impl AsRef<UringConfig>) -> UringResult<Self> {
        Self::new_internal(config).map_err(|e| e.attach("create uring driver"))
    }

    pub(crate) unsafe fn submit_from_slot_raw(
        driver: *mut UringDriver,
        user_data: usize,
        slot: Slot<'_, crate::op::slot::Reserved>,
    ) -> UringResult<bool> {
        let driver = unsafe { &mut *driver };
        let mut sub_guard = slot.start_submission_with(None);
        let strategy = sub_guard
            .slot
            .as_mut()
            .ok_or_else(|| {
                invalid_state(
                    "driver.submit_from_slot_raw",
                    "submission guard slot missing",
                )
            })?
            .op_mut()
            .vtable
            .strategy;

        match strategy {
            SubmissionStrategy::SubmitSqe => {
                let mut chunks = [0u16; 4];
                let (count, sqe) = {
                    let driver_ptr = driver as *mut UringDriver;
                    let op = sub_guard
                        .slot
                        .as_mut()
                        .ok_or_else(|| {
                            invalid_state(
                                "driver.submit_from_slot_raw",
                                "submission guard slot missing",
                            )
                        })?
                        .op_mut();
                    let vtable = op.vtable;
                    let count = unsafe { (vtable.resolve_chunks)(op, &mut chunks) };
                    let sqe = unsafe {
                        (vtable.make_sqe)(op, &mut *driver_ptr)
                            .map_err(|e| {
                                Report::new(UringError::Submission)
                                    .attach(format!("driver.submit_from_slot_raw.make_sqe: {e:#}"))
                            })?
                            .user_data(user_data as u64)
                    };
                    (count, sqe)
                };

                for &chunk_id in chunks.iter().take(count) {
                    let index = chunk_id as usize;
                    let is_registered = driver.registered_chunks.get(index).map_err(|e| {
                        invalid_state(
                            "driver.submit_from_slot_raw.bitset_get",
                            format!("BitSet get failed index={index}: {e:?}"),
                        )
                    })?;

                    if !is_registered
                        && let Some(info) = driver.registrar.resolve_chunk_info(chunk_id)
                    {
                        if let Err(e) = driver.register_chunk_internal(
                            info.id,
                            info.ptr.as_ptr(),
                            info.len.get(),
                        ) {
                            if driver.registration_mode.is_strict() {
                                return Err(e.attach(format!(
                                    "strict mode lazy register failed: chunk_id={chunk_id}, user_data={user_data}"
                                )));
                            }
                            return Err(e);
                        }
                    } else if !is_registered {
                        driver.registration_stats.submission_missing_chunk_info = driver
                            .registration_stats
                            .submission_missing_chunk_info
                            .saturating_add(1);
                        if driver.registration_mode.is_strict() {
                            return Err(invalid_state(
                                "driver.submit_from_slot_raw.missing_chunk_info",
                                format!(
                                    "strict mode missing chunk info for lazy registration: chunk_id={chunk_id}, user_data={user_data}"
                                ),
                            ));
                        }
                        return Err(invalid_input(
                            "driver.submit_from_slot_raw.missing_chunk_info",
                            format!(
                                "Missing chunk info for lazy registration: chunk_id={chunk_id}, user_data={user_data}"
                            ),
                        ));
                    }
                }

                if driver.push_entry(sqe) {
                    let _ = sub_guard.persist();
                    trace!(user_data, "Submitted to SQ");
                    Ok(true)
                } else {
                    debug!(user_data, "SQ full");
                    Ok(false)
                }
            }
            SubmissionStrategy::SoftwareTimer => {
                let duration_opt = {
                    let slot = sub_guard.slot.as_mut().ok_or_else(|| {
                        invalid_state(
                            "driver.submit_from_slot_raw.timer",
                            "submission guard slot missing",
                        )
                    })?;
                    let vtable = slot.op_mut().vtable;
                    unsafe { (vtable.get_timeout)(slot.op_mut()) }
                };
                if let Some(duration) = duration_opt {
                    let task_id = driver.wheel.insert(user_data, duration);
                    if let Some(entry) = driver.ops.get_mut(user_data) {
                        entry.platform_data.timer_id = Some(task_id);
                    }
                    let _ = sub_guard.persist();
                    trace!(user_data, ?duration, "Registered software timer");
                    Ok(true)
                } else {
                    Err(invalid_input(
                        "driver.submit_from_slot_raw.timer_duration",
                        "Timer duration missing",
                    ))
                }
            }
            _ => Err(unsupported(
                "driver.submit_from_slot_raw.strategy",
                "Unsupported strategy for slot submission",
            )),
        }
    }

    pub(crate) fn submit_from_slot_index(&mut self, user_data: usize) -> UringResult<bool> {
        let driver_ptr = self as *mut UringDriver;
        let slot = match self.ops.slot_view(user_data) {
            Some(SlotView::Reserved(slot)) => slot,
            _ => {
                return Err(invalid_state(
                    "driver.submit_from_slot_index",
                    "op missing in slot",
                ));
            }
        };
        unsafe { Self::submit_from_slot_raw(driver_ptr, user_data, slot) }
    }

    fn submit_waker(&mut self) -> UringResult<()> {
        if self.waker_token.is_some() {
            return Ok(());
        }

        let fixed_fd = match self.waker_registered_fd {
            Some(fd) => fd,
            None => {
                let fd = self.waker_fd.fd.raw().as_fd();
                let raw = RawHandle::new(UringRawHandle::for_file(fd));
                let mut fds =
                    self.register_files_internal(vec![RegisterFd::Borrowed(raw.borrow())])?;
                let fixed_fd = fds.pop().ok_or_else(|| {
                    invalid_state("driver.submit_waker", "register_files returned empty")
                })?;
                self.waker_registered_fd = Some(fixed_fd);
                fixed_fd
            }
        };
        let op = Wakeup { fd: fixed_fd };
        let (uring_op, payload) = <Wakeup as IntoPlatformOp<UringOp>>::into_kernel_and_payload(op);

        let result = self.ops.alloc(UringOpState::new());

        if let Ok(AllocResult {
            handle: OpHandle {
                index: user_data, ..
            },
        }) = result
        {
            self.waker_token = Some(user_data);
            self.waker_payload = Some(payload);

            let driver_ptr = self as *mut UringDriver;
            let slot = self
                .ops
                .slot_reserve(user_data)
                .init_op_with(uring_op, |_| {});
            match unsafe { Self::submit_from_slot_raw(driver_ptr, user_data, slot) } {
                Ok(true) => {}
                Ok(false) => self.push_backlog(user_data),
                Err(e) => return Err(e),
            }
            Ok(())
        } else {
            Err(invalid_state(
                "driver.submit_waker",
                "failed to reserve waker slot",
            ))
        }
    }

    pub(crate) fn submit_to_kernel(&mut self) -> UringResult<()> {
        trace!("submit_to_kernel entered");
        if self.ring.params().is_setup_sqpoll() {
            if self.ring.submission().need_wakeup() {
                self.ring.submit().map_err(|e| {
                    from_io_error(
                        UringError::Submission,
                        "driver.submit_to_kernel.submit.sqpoll",
                        e,
                    )
                })?;
            }
        } else {
            self.ring.submit().map_err(|e| {
                from_io_error(UringError::Submission, "driver.submit_to_kernel.submit", e)
            })?;
        }
        self.flush_backlog();
        Ok(())
    }

    pub(crate) fn wait_internal(&mut self) -> UringResult<()> {
        self.drain_cancel_requests();
        self.flush_cancellations();
        self.flush_backlog();

        if !self.has_active_ops() {
            return Ok(());
        }

        if !self.ring.completion().is_empty() {
            self.process_completions_internal();
        } else {
            let next_timeout = self.wheel.next_timeout();
            let start = std::time::Instant::now();

            if let Some(duration) = next_timeout {
                let ts = io_uring::types::Timespec::new()
                    .sec(duration.as_secs())
                    .nsec(duration.subsec_nanos());

                let args = io_uring::types::SubmitArgs::new().timespec(&ts);
                match self.ring.submitter().submit_with_args(1, &args) {
                    Ok(_) => {}
                    Err(ref e) if e.raw_os_error() == Some(libc::ETIME) => {}
                    Err(e) => {
                        return Err(from_io_error(
                            UringError::CompletionWait,
                            "driver.wait_internal.submit_with_args",
                            e,
                        ));
                    }
                }
            } else {
                self.ring.submit_and_wait(1).map_err(|e| {
                    from_io_error(
                        UringError::CompletionWait,
                        "driver.wait_internal.submit_and_wait",
                        e,
                    )
                })?;
            }

            let elapsed = start.elapsed();
            self.wheel.advance(elapsed, &mut self.timer_buffer);

            let timer_buffer = std::mem::take(&mut self.timer_buffer);
            for user_data in timer_buffer {
                let sidecar = self.ops.slot_view(user_data).and_then(|slot| match slot {
                    SlotView::InFlightWaiting(mut slot) => {
                        slot.platform_mut().timer_id = None;
                        let mut completed = slot.complete();

                        let generation = completed.entry.generation(Ordering::Acquire);
                        let _ = completed.take_op();
                        let (payload, detail) = completed.take_completion_data();

                        Some(CompletionSidecar {
                            user_data,
                            generation,
                            res: 0,
                            flags: 0,
                            payload,
                            detail,
                        })
                    }
                    _ => None,
                });

                if let Some(sidecar) = sidecar {
                    self.push_completion_event(sidecar);
                    self.ops.remove(user_data);
                }
            }
        }

        self.process_completions_internal();
        self.flush_cancellations();
        self.flush_backlog();
        Ok(())
    }

    fn has_active_ops(&mut self) -> bool {
        let len = self.ops.local.len();
        for idx in 0..len {
            if self.ops.slot_view(idx).is_some() {
                return true;
            }
        }
        false
    }

    pub(crate) fn process_completions_internal(&mut self) {
        let mut needs_waker_resubmit = false;
        let mut pending_events: Vec<CompletionSidecar> = Vec::new();

        let mut cqes = Vec::new();
        {
            let mut cqe_kicker = self.ring.completion();
            cqe_kicker.sync();

            trace!("Processing completions, count={}", cqe_kicker.len());
            for cqe in cqe_kicker {
                cqes.push((cqe.user_data(), cqe.result(), cqe.flags()));
            }
        }

        for (cqe_user_data, cqe_res, cqe_flags) in cqes {
            let user_data = cqe_user_data as usize;

            if user_data == u64::MAX as usize
                || user_data == CANCEL_USER_DATA as usize
                || user_data == BACKGROUND_USER_DATA as usize
            {
                continue;
            }

            if Some(user_data) == self.waker_token {
                needs_waker_resubmit = true;
                continue;
            }

            let sidecar = self.ops.slot_view(user_data).and_then(|slot| match slot {
                SlotView::InFlightWaiting(mut slot) => {
                    let res_val = cqe_res;
                    let final_res = slot
                        .with_op_mut(|op| unsafe { (op.vtable.on_complete)(op, res_val) })
                        .unwrap_or_else(|| {
                            if res_val >= 0 {
                                Ok(res_val as usize)
                            } else {
                                Err(driver_os_error(
                                    DriverErrorKind::Completion,
                                    "uring.driver.process_completions_internal.on_complete",
                                    -res_val,
                                    "kernel completion returned error",
                                ))
                            }
                        });

                    let mut completed = slot.complete();
                    let generation = completed.entry.generation(Ordering::Acquire);
                    let res_code = driver_result_to_event_res(&final_res);

                    let (payload, mut detail) = completed.take_completion_data();
                    if detail.is_none()
                        && let Err(err) = final_res
                    {
                        detail = Some(Err(err));
                    }
                    let _ = completed.take_op();

                    Some(CompletionSidecar {
                        user_data,
                        generation,
                        res: res_code,
                        flags: cqe_flags,
                        payload,
                        detail,
                    })
                }
                SlotView::InFlightOrphaned(slot) => {
                    let generation = slot.entry.generation(Ordering::Acquire);
                    let mut completed = slot.complete();
                    let (payload, detail) = completed.take_completion_data();
                    let _ = completed.take_op();

                    Some(CompletionSidecar {
                        user_data,
                        generation,
                        res: cqe_res,
                        flags: cqe_flags,
                        payload,
                        detail,
                    })
                }
                _ => None,
            });

            if let Some(sidecar) = sidecar {
                pending_events.push(sidecar);
                self.ops.remove(user_data);
            }
        }

        for sidecar in pending_events {
            self.push_completion_event(sidecar);
        }

        if needs_waker_resubmit {
            self.is_waked.store(false, Ordering::Release);
            if let Some(token) = self.waker_token.take() {
                self.ops.remove(token);
            }
            self.waker_payload = None;
            if let Err(e) = self.submit_waker() {
                tracing::error!(report = ?e, "failed to resubmit waker");
            }
            self.flush_backlog();
        }
    }

    pub(crate) fn push_entry(&mut self, entry: squeue::Entry) -> bool {
        trace!("Pushing SQE user_data={}", entry.get_user_data());
        let mut sq = self.ring.submission();

        if unsafe { sq.push(&entry) }.is_ok() {
            return true;
        }

        drop(sq);
        let _ = self.ring.submit();

        let mut sq = self.ring.submission();
        if unsafe { sq.push(&entry) }.is_ok() {
            return true;
        }

        debug!("SQ full even after flush");
        false
    }
}

#[inline]
fn driver_result_to_event_res(res: &DriverResult<usize>) -> i32 {
    match res {
        Ok(v) => (*v).min(i32::MAX as usize) as i32,
        Err(e) => driver_error_report_to_event_res(e),
    }
}

impl UringDriver {
    fn register_files_internal<'a>(
        &mut self,
        files: Vec<RegisterFd<'a, UringRawHandle>>,
    ) -> UringResult<Vec<IoFd>> {
        self.ensure_file_table_initialized()?;

        let mut fixed_fds = Vec::with_capacity(files.len());
        for file in files {
            let entry = match file {
                RegisterFd::Borrowed(b) => RegisteredFileEntry::BorrowedFd(b.raw().as_fd()),
                RegisterFd::Owned(o) => RegisteredFileEntry::OwnedHandle(o),
            };
            let fd = match &entry {
                RegisteredFileEntry::BorrowedFd(fd) => *fd,
                RegisteredFileEntry::OwnedHandle(o) => o.raw().as_fd(),
            };
            let idx = self.free_file_slots.pop().ok_or_else(|| {
                invalid_state(
                    "driver.register_files_internal",
                    "io_uring registered file table exhausted",
                )
            })?;
            self.ring
                .submitter()
                .register_files_update(idx, &[fd])
                .map_err(|e| {
                    from_io_error(
                        UringError::Registration,
                        "driver.register_files_internal.register_files_update",
                        e,
                    )
                })?;
            self.registered_files[idx as usize] = Some(entry);
            fixed_fds.push(IoFd::fixed(idx));
        }
        Ok(fixed_fds)
    }
}

impl UringDriver {
    #[inline]
    pub(crate) fn push_completion_event(&mut self, sidecar: CompletionSidecar) {
        let token = encode_completion_token(sidecar.user_data, sidecar.generation);
        let event = CompletionEvent {
            user_data: token,
            res: sidecar.res,
            flags: sidecar.flags,
        };
        self.completion_table
            .record_completion_with_data(event, sidecar.payload, sidecar.detail);
        self.completion_events.push(event);
    }
}

impl Drop for UringDriver {
    fn drop(&mut self) {}
}

impl Driver for UringDriver {
    type Op = UringOp;
    type Raw = UringRawHandle;
    type Sidecar = ();
    type Completion = usize;

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
            Err(_) => Err(driver_error(
                DriverErrorKind::InvalidState,
                "uring.driver.reserve_op",
                "OpRegistry full",
            )),
        }
    }

    fn slot_table(
        &self,
    ) -> std::sync::Arc<veloq_driver_core::slot::SlotTable<Self::Op, Self::Sidecar>> {
        self.ops.shared.clone()
    }

    fn detached_cancel_table(&self) -> std::sync::Arc<DetachedCancelTable> {
        self.detached_cancel_table.clone()
    }

    fn slot_set_payload(
        &mut self,
        user_data: usize,
        payload: veloq_driver_core::slot::ErasedPayload,
    ) {
        let _ =
            self.ops
                .with_slot_storage_mut(user_data, |_op, _result, payload_cell, _sidecar| {
                    *payload_cell = Some(payload);
                });
    }

    fn slot_take_payload(
        &mut self,
        user_data: usize,
    ) -> Option<veloq_driver_core::slot::ErasedPayload> {
        self.ops
            .with_slot_storage_mut(user_data, |_op, _result, payload_cell, _sidecar| {
                payload_cell.take()
            })
            .flatten()
    }

    fn submit(
        &mut self,
        user_data: usize,
        op_in: &mut Option<Self::Op>,
        binder: SubmitBinder,
    ) -> Outcome<Result<Poll<()>, (DriverErrorReport, SubmitStatus)>> {
        let Some(op) = op_in.take() else {
            return binder.err(
                map_uring_error(
                    invalid_state("driver.submit", "submit called with empty Option"),
                    DriverErrorKind::InvalidState,
                    "uring.driver.submit",
                    "submit called with empty Option",
                ),
                SubmitStatus::Void,
            );
        };
        let op: UringOp = op;
        let strategy = op.vtable.strategy;
        if strategy == crate::op::SubmissionStrategy::BackgroundOnly {
            *op_in = Some(op);
            return binder.err(
                driver_error(
                    DriverErrorKind::Unsupported,
                    "uring.driver.submit",
                    "background op cannot be submitted normally",
                ),
                SubmitStatus::Void,
            );
        }

        match strategy {
            crate::op::SubmissionStrategy::BackgroundOnly => binder.err(
                map_uring_error(
                    invalid_state(
                        "driver.submit",
                        "background strategy reached normal submit path",
                    ),
                    DriverErrorKind::InvalidState,
                    "uring.driver.submit",
                    "background strategy reached normal submit path",
                ),
                SubmitStatus::Void,
            ),
            crate::op::SubmissionStrategy::SubmitSqe => {
                self.submit_sqe_internal(user_data, op, op_in, binder)
            }
            crate::op::SubmissionStrategy::SoftwareTimer => {
                self.submit_timer_internal(user_data, op, op_in, binder)
            }
        }
    }

    fn submit_background(&mut self, mut op: Self::Op) -> DriverResult<()> {
        let strategy = op.vtable.strategy;
        if strategy == crate::op::SubmissionStrategy::BackgroundOnly {
            let sqe =
                unsafe { (op.vtable.make_sqe)(&mut op, self)?.user_data(BACKGROUND_USER_DATA) };

            if !self.push_entry(sqe) {
                return Err(driver_error(
                    DriverErrorKind::Submission,
                    "uring.driver.submit_background",
                    "sq full",
                ));
            }
            Ok(())
        } else {
            Err(driver_error(
                DriverErrorKind::Unsupported,
                "uring.driver.submit_background",
                "background op only supports BackgroundOnly strategy",
            ))
        }
    }

    fn submit_queue(&mut self) -> DriverResult<()> {
        self.drain_cancel_requests();
        self.flush_cancellations();
        self.flush_backlog();
        self.submit_to_kernel().to_driver_result(
            DriverErrorKind::Submission,
            "uring.driver.submit_queue",
            "submit queue to kernel",
        )
    }

    fn wait(&mut self) -> DriverResult<()> {
        self.wait_internal().to_driver_result(
            DriverErrorKind::Completion,
            "uring.driver.wait",
            "wait for completions",
        )
    }

    fn process_completions(&mut self) {
        self.drain_cancel_requests();
        self.process_completions_internal();
        self.flush_cancellations();
        self.flush_backlog();
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
    ) -> DriverResult<usize> {
        self.wait_internal().to_driver_result(
            DriverErrorKind::Completion,
            "uring.driver.wait_and_drain_completions",
            "wait and drain completions",
        )?;
        Ok(self.drain_completions(out))
    }

    fn cancel_op(&mut self, user_data: usize) {
        self.cancel_op_internal(user_data);
    }

    fn register_chunk(&mut self, id: u16, ptr: *const u8, len: usize) -> DriverResult<()> {
        self.register_chunk_internal(id, ptr, len).to_driver_result(
            DriverErrorKind::Registration,
            "uring.driver.register_chunk",
            "register chunk",
        )
    }

    fn register_files<'a>(
        &mut self,
        files: Vec<RegisterFd<'a, UringRawHandle>>,
    ) -> DriverResult<Vec<IoFd>> {
        self.register_files_internal(files).to_driver_result(
            DriverErrorKind::Registration,
            "uring.driver.register_files",
            "register files",
        )
    }

    fn unregister_files(&mut self, files: Vec<IoFd>) -> DriverResult<()> {
        for fd in files {
            self.unregister_fixed_fd(fd).to_driver_result(
                DriverErrorKind::Registration,
                "uring.driver.unregister_files",
                "unregister fixed fd",
            )?;
        }
        Ok(())
    }

    fn warmup_udp_socket(
        &mut self,
        _fd: IoFd,
        _buf_capacity: NonZeroUsize,
        _credits: usize,
    ) -> DriverResult<()> {
        Ok(())
    }

    fn wake(&mut self) -> DriverResult<()> {
        let buf = 1u64.to_ne_bytes();
        let ret =
            unsafe { libc::write(self.waker_fd.fd.raw().as_fd(), buf.as_ptr() as *const _, 8) };
        if ret < 0 {
            let err = io::Error::last_os_error();
            if err.raw_os_error() == Some(libc::EAGAIN) {
                return Ok(());
            }
            return Err(driver_os_error(
                DriverErrorKind::System,
                "uring.driver.wake",
                err.raw_os_error().unwrap_or(libc::EIO),
                err.to_string(),
            ));
        }
        Ok(())
    }

    fn create_waker(&self) -> Arc<dyn RemoteWaker> {
        Arc::new(UringWaker {
            fd: self.waker_fd.clone(),
            is_waked: self.is_waked.clone(),
        })
    }

    fn driver_id(&self) -> usize {
        self.waker_fd.fd.raw().as_fd() as usize
    }

    fn set_registrar(&mut self, registrar: Box<dyn veloq_buf::BufferRegistrar>) {
        self.registrar = registrar;
    }
}

impl UringDriver {
    fn submit_sqe_internal(
        &mut self,
        user_data: usize,
        op: <Self as Driver>::Op,
        op_in: &mut Option<<Self as Driver>::Op>,
        binder: SubmitBinder,
    ) -> Outcome<Result<Poll<()>, (DriverErrorReport, SubmitStatus)>> {
        let driver_ptr = self as *mut UringDriver;
        let slot = match self.ops.slot_view(user_data) {
            Some(SlotView::Reserved(slot)) => {
                if slot.has_op() {
                    let mut slot = slot;
                    *slot.op_mut() = op;
                    slot
                } else {
                    slot.init_op_with(op, |_| {})
                }
            }
            Some(SlotView::InFlightWaiting(_)) | Some(SlotView::InFlightOrphaned(_)) | None => {
                return binder.err(
                    driver_error(
                        DriverErrorKind::InvalidState,
                        "uring.driver.submit_sqe_internal",
                        "Op slot missing in registry",
                    ),
                    SubmitStatus::Void,
                );
            }
        };

        match unsafe { Self::submit_from_slot_raw(driver_ptr, user_data, slot) } {
            Ok(true) => binder.ok(Poll::Ready(())),
            Ok(false) => {
                debug!(user_data, "SQ full, pushing to backlog");
                self.push_backlog(user_data);
                binder.ok(Poll::Pending)
            }
            Err(e) => {
                if let Some(op) = self
                    .ops
                    .get_slot_entry_op_storage_and_entry_mut(user_data)
                    .and_then(|(_, _, op, _)| op.take())
                {
                    *op_in = Some(op);
                }
                binder.err(
                    map_uring_error(
                        e,
                        DriverErrorKind::Submission,
                        "uring.driver.submit_sqe_internal",
                        "submit sqe",
                    ),
                    SubmitStatus::Void,
                )
            }
        }
    }

    fn submit_timer_internal(
        &mut self,
        user_data: usize,
        op: <Self as Driver>::Op,
        op_in: &mut Option<<Self as Driver>::Op>,
        binder: SubmitBinder,
    ) -> Outcome<Result<Poll<()>, (DriverErrorReport, SubmitStatus)>> {
        let driver_ptr = self as *mut UringDriver;
        let slot = match self.ops.slot_view(user_data) {
            Some(SlotView::Reserved(slot)) => {
                if slot.has_op() {
                    let mut slot = slot;
                    *slot.op_mut() = op;
                    slot
                } else {
                    slot.init_op_with(op, |_| {})
                }
            }
            Some(SlotView::InFlightWaiting(_)) | Some(SlotView::InFlightOrphaned(_)) | None => {
                return binder.err(
                    driver_error(
                        DriverErrorKind::InvalidState,
                        "uring.driver.submit_timer_internal",
                        "Op slot missing in registry",
                    ),
                    SubmitStatus::Void,
                );
            }
        };

        match unsafe { Self::submit_from_slot_raw(driver_ptr, user_data, slot) } {
            Ok(true) => binder.ok(Poll::Ready(())),
            Ok(false) => {
                debug!(
                    user_data,
                    "SQ full (unexpected for timer), pushing to backlog"
                );
                self.push_backlog(user_data);
                binder.ok(Poll::Pending)
            }
            Err(e) => {
                if let Some(op) = self
                    .ops
                    .get_slot_entry_op_storage_and_entry_mut(user_data)
                    .and_then(|(_, _, op, _)| op.take())
                {
                    *op_in = Some(op);
                }
                binder.err(
                    map_uring_error(
                        e,
                        DriverErrorKind::Submission,
                        "uring.driver.submit_timer_internal",
                        "submit timer",
                    ),
                    SubmitStatus::Void,
                )
            }
        }
    }
}

#[cfg(feature = "test-hooks")]
impl veloq_driver_core::driver::test_hooks::DriverTestHooks for UringDriver {
    fn debug_chunk_register_attempts(&self) -> u64 {
        self.registration_stats.chunk_register_attempts
    }
}
