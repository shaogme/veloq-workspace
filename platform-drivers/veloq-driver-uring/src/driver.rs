use io_uring::{IoUring, squeue};
use std::collections::{HashMap, VecDeque};
use std::io;
use std::os::unix::io::{AsRawFd, RawFd};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::Poll;
use std::time::Instant;

use tracing::{debug, trace};

use crate::config::{BufferRegistrationMode, IoFd, IoMode, RawHandle, UringConfig};
use crate::op::{OpVTable, SubmissionStrategy, UringOp};
use veloq_driver_core::driver::{
    CompletionEvent, CompletionSidecar, CompletionTable, Driver, Outcome, RemoteWaker,
    SharedCompletionQueue, SharedCompletionTable, SubmitBinder, encode_completion_token,
};
use veloq_driver_core::slot::ErasedPayload;
use veloq_driver_core::op::{IntoPlatformOp, Wakeup};
use veloq_driver_core::op_registry::{AllocResult, OpEntry, OpHandle, OpRegistry};

mod lifecycle;
mod registration;

pub(crate) use lifecycle::OpLifecycle;
pub use lifecycle::UringOpState;
pub(crate) use registration::{MAX_CHUNKS, UringRegistrationStats};

use crate::op::slot::{InFlight, Initialized, Pending, Slot};

pub(crate) struct EventFd {
    pub(crate) fd: RawFd,
}

impl Drop for EventFd {
    fn drop(&mut self) {
        unsafe {
            libc::close(self.fd);
        }
    }
}

pub(crate) struct UringWaker {
    pub(crate) fd: Arc<EventFd>,
    pub(crate) is_waked: Arc<AtomicBool>,
}

impl RemoteWaker for UringWaker {
    fn wake(&self) -> io::Result<()> {
        if self.is_waked.load(Ordering::Relaxed) {
            return Ok(());
        }
        if !self.is_waked.swap(true, Ordering::AcqRel) {
            let buf = 1u64.to_ne_bytes();
            let ret = unsafe { libc::write(self.fd.fd, buf.as_ptr() as *const _, 8) };
            if ret < 0 {
                let err = io::Error::last_os_error();
                if err.raw_os_error() == Some(libc::EAGAIN) {
                    return Ok(());
                }
                return Err(err);
            }
        }
        Ok(())
    }
}

pub(crate) const CANCEL_USER_DATA: u64 = u64::MAX - 1;
pub(crate) const BACKGROUND_USER_DATA: u64 = u64::MAX - 2;

pub struct UringDriver {
    pub(crate) ring: IoUring,
    pub(crate) ops: OpRegistry<UringOp, UringOpState, ()>,
    pub(crate) backlog_head: Option<usize>,
    pub(crate) backlog_tail: Option<usize>,
    pub(crate) pending_cancellations: VecDeque<usize>,
    pub(crate) completion_events: SharedCompletionQueue,
    pub(crate) completion_table: SharedCompletionTable,

    pub(crate) waker_fd: Arc<EventFd>,
    pub(crate) waker_token: Option<usize>,
    pub(crate) waker_payload: Option<Box<Wakeup<RawHandle>>>,
    pub(crate) registered_chunks: veloq_bitset::BitSet,
    pub(crate) is_waked: Arc<AtomicBool>,

    pub(crate) wheel: veloq_wheel::Wheel<usize>,
    pub(crate) timer_buffer: Vec<usize>,
    pub(crate) registrar: Box<dyn veloq_buf::BufferRegistrar>,
    pub(crate) registration_stats: UringRegistrationStats,
    pub(crate) registration_mode: BufferRegistrationMode,
    pub(crate) chunk_register_failures_recent: HashMap<u16, Instant>,
}

impl UringDriver {
    pub fn new(config: impl AsRef<UringConfig>) -> io::Result<Self> {
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
        let ring = builder.build(entries).or_else(|e| {
            if e.raw_os_error() == Some(libc::EINVAL) {
                IoUring::new(entries)
            } else {
                Err(e)
            }
        })?;

        let ops = OpRegistry::new(entries as usize);

        let waker_fd = unsafe { libc::eventfd(0, libc::EFD_CLOEXEC | libc::EFD_NONBLOCK) };
        if waker_fd < 0 {
            return Err(io::Error::last_os_error());
        }

        debug!("Initalized UringDriver with {} entries", entries);

        let is_waked = Arc::new(AtomicBool::new(false));

        let mut driver = Self {
            ring,
            ops,
            backlog_head: None,
            backlog_tail: None,
            pending_cancellations: VecDeque::new(),
            completion_events: std::sync::Arc::new(crossbeam_queue::SegQueue::new()),
            completion_table: std::sync::Arc::new(CompletionTable::new(entries as usize)),
            waker_fd: Arc::new(EventFd { fd: waker_fd }),
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
        };

        driver.submit_waker();

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

    pub(crate) fn submit_from_slot(&mut self, user_data: usize) -> io::Result<bool> {
        let (vtable, strategy) = {
            let (_, _, storage) = self
                .ops
                .get_slot_entry_storage_and_entry_mut(user_data)
                .ok_or_else(|| io::Error::other("Op missing in slot"))?;

            storage.with_mut(|op, _, _, _| {
                let op = op.as_ref().ok_or_else(|| io::Error::other("Op missing"))?;
                Ok::<(&'static OpVTable, SubmissionStrategy), io::Error>((op.vtable, op.vtable.strategy))
            })?
        };

        match strategy {
            SubmissionStrategy::SubmitSqe => {
                let mut chunks = [0u16; 4];
                let count = {
                    let (_, _, storage) = self.ops.get_slot_entry_storage_and_entry_mut(user_data).unwrap();
                    storage.with_mut(|op, _, _, _| {
                        let op = op.as_ref().unwrap();
                        unsafe { (vtable.resolve_chunks)(op, &mut chunks) }
                    })
                };

                for &chunk_id in chunks.iter().take(count) {
                    let index = chunk_id as usize;
                    let is_registered = self.registered_chunks.get(index).map_err(|e| {
                        io::Error::other(format!("BitSet error getting {}: {:?}", index, e))
                    })?;

                    if !is_registered
                        && let Some(info) = self.registrar.resolve_chunk_info(chunk_id)
                    {
                        if let Err(e) =
                            self.register_chunk_internal(info.id, info.ptr.as_ptr(), info.len.get())
                        {
                            if self.registration_mode.is_strict() {
                                panic!(
                                    "strict registration mode: io_uring lazy register failed: chunk_id={}, user_data={}, error={}",
                                    chunk_id, user_data, e
                                );
                            }
                            return Err(e);
                        }
                    } else if !is_registered {
                        self.registration_stats.submission_missing_chunk_info = self
                            .registration_stats
                            .submission_missing_chunk_info
                            .saturating_add(1);
                        if self.registration_mode.is_strict() {
                            panic!(
                                "strict registration mode: io_uring missing chunk info for lazy registration: chunk_id={}, user_data={}",
                                chunk_id, user_data
                            );
                        }
                        return Err(io::Error::other(format!(
                            "Missing chunk info for lazy registration: chunk_id={chunk_id}, user_data={user_data}"
                        )));
                    }
                }
                
                let driver_ptr = self as *mut UringDriver;
                let sqe = {
                    let (slot_entry, op_entry, storage) = self.ops.get_slot_entry_storage_and_entry_mut(user_data).unwrap();
                    let mut slot = Slot::<Initialized>::as_initialized(slot_entry, storage, &mut op_entry.platform_data, user_data);
                    slot.with_op_mut(|op| {
                        unsafe { (vtable.make_sqe)(op, &mut *driver_ptr).user_data(user_data as u64) }
                    }).expect("op missing in slot")
                };
                
                if self.push_entry(sqe) {
                    if let Some((slot_entry, op_entry, storage)) = self.ops.get_slot_entry_storage_and_entry_mut(user_data) {
                        let slot = Slot::<Initialized>::as_initialized(slot_entry, storage, &mut op_entry.platform_data, user_data);
                        let _in_flight = slot.start_submission().persist();
                    }
                    trace!(user_data, "Submitted to SQ");
                    Ok(true)
                } else {
                    debug!(user_data, "SQ full");
                    Ok(false)
                }
            }
            SubmissionStrategy::SoftwareTimer => {
                let duration_opt = {
                    let (_, _, storage) = self.ops.get_slot_entry_storage_and_entry_mut(user_data).unwrap();
                    storage.with_mut(|op, _, _, _| {
                        let op = op.as_ref().unwrap();
                        unsafe { (vtable.get_timeout)(op) }
                    })
                };
                if let Some(duration) = duration_opt {
                    let task_id = self.wheel.insert(user_data, duration);
                    if let Some((slot_entry, op_entry, storage)) = self.ops.get_slot_entry_storage_and_entry_mut(user_data) {
                        let slot = Slot::<Initialized>::as_initialized(slot_entry, storage, &mut op_entry.platform_data, user_data);
                        let in_flight = slot.start_submission().persist();
                        in_flight.platform.timer_id = Some(task_id);
                    }
                    trace!(user_data, ?duration, "Registered software timer");
                    Ok(true)
                } else {
                    Err(io::Error::other("Timer duration missing"))
                }
            }
            _ => {
                Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    "Unsupported strategy for slot submission",
                ))
            }
        }
    }

    fn submit_waker(&mut self) {
        if self.waker_token.is_some() {
            return;
        }

        let fd = self.waker_fd.fd;
        let op = Wakeup {
            fd: IoFd::Raw(RawHandle { fd }),
        };
        let (uring_op, payload) =
            <Wakeup<RawHandle> as IntoPlatformOp<UringOp>>::into_kernel_and_payload(op);

        let result = self.ops.alloc(UringOpState::new());

        if let Ok(AllocResult {
            handle: OpHandle {
                index: user_data, ..
            },
        }) = result
        {
            self.waker_token = Some(user_data);
            self.waker_payload = Some(payload);

            let vtable = {
                let (slot_entry, op_entry, storage) = self
                    .ops
                    .get_slot_entry_storage_and_entry_mut(user_data)
                    .expect("Failed to get waker slot");

                let mut slot = Slot::<Pending>::new(slot_entry, storage, &mut op_entry.platform_data, user_data)
                    .init_op(uring_op);
                
                let vtable = slot.with_op_mut(|op| op.vtable).expect("waker op missing in slot");
                vtable
            };

            let driver_ptr = self as *mut UringDriver;
            let sqe = {
                let (slot_entry, op_entry, storage) = self.ops.get_slot_entry_storage_and_entry_mut(user_data).unwrap();
                let mut slot = Slot::<Initialized>::as_initialized(slot_entry, storage, &mut op_entry.platform_data, user_data);
                slot.with_op_mut(|op| {
                    unsafe { (vtable.make_sqe)(op, &mut *driver_ptr).user_data(user_data as u64) }
                }).expect("waker op missing in slot during SQE build")
            };

            if self.push_entry(sqe) {
                if let Some((slot_entry, op_entry, storage)) = self.ops.get_slot_entry_storage_and_entry_mut(user_data) {
                    let slot = Slot::<Initialized>::as_initialized(slot_entry, storage, &mut op_entry.platform_data, user_data);
                    let in_flight = slot.start_submission().persist();
                    in_flight.platform.lifecycle = OpLifecycle::InFlight;
                }
            } else {
                self.push_backlog(user_data);
            }
        } else {
            panic!("Failed to reserve waker slot");
        }
    }

    pub(crate) fn submit_to_kernel(&mut self) -> io::Result<()> {
        trace!("submit_to_kernel entered");
        if self.ring.params().is_setup_sqpoll() {
            if self.ring.submission().need_wakeup() {
                self.ring.submit()?;
            }
        } else {
            self.ring.submit()?;
        }
        self.flush_backlog();
        Ok(())
    }

    pub(crate) fn wait_internal(&mut self) -> io::Result<()> {
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
                    Err(e) => return Err(e),
                }
            } else {
                self.ring.submit_and_wait(1)?;
            }

            let elapsed = start.elapsed();
            self.wheel.advance(elapsed, &mut self.timer_buffer);

            let timer_buffer = std::mem::take(&mut self.timer_buffer);
            for user_data in timer_buffer {
                if let Some((slot_entry, op_entry, storage)) = self.ops.get_slot_entry_storage_and_entry_mut(user_data) 
                    && matches!(op_entry.platform_data.lifecycle, OpLifecycle::InFlight)
                {
                    let slot = Slot::<InFlight>::as_in_flight(slot_entry, storage, &mut op_entry.platform_data, user_data);
                    
                    slot.platform.timer_id = None;
                    let mut completed = slot.complete();
                    
                    let generation = completed.entry.generation.load(Ordering::Acquire);
                    let _ = completed.take_op();
                    let (payload, detail) = completed.take_completion_data();

                    self.push_completion_event(CompletionSidecar {
                        user_data,
                        generation,
                        res: 0,
                        flags: 0,
                        payload,
                        detail,
                    });
                    self.ops.remove(user_data);
                }
            }
        }

        self.process_completions_internal();
        self.flush_cancellations();
        self.flush_backlog();
        Ok(())
    }

    fn has_active_ops(&self) -> bool {
        self.ops.local.iter().any(|entry| {
            matches!(
                entry.entry.platform_data.lifecycle,
                OpLifecycle::Pending | OpLifecycle::InFlight | OpLifecycle::Cancelled
            )
        })
    }

    pub(crate) fn process_completions_internal(&mut self) {
        let mut needs_waker_resubmit = false;

        let mut pending_events: Vec<CompletionSidecar> = Vec::new();
        {
            let mut cqe_kicker = self.ring.completion();
            cqe_kicker.sync();

            trace!("Processing completions, count={}", cqe_kicker.len());

            for cqe in cqe_kicker {
                let user_data = cqe.user_data() as usize;

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

                if let Some((slot_entry, op_entry, storage)) = self.ops.get_slot_entry_storage_and_entry_mut(user_data) {
                    let mut slot = Slot::<InFlight>::as_in_flight(slot_entry, storage, &mut op_entry.platform_data, user_data);
                    let is_cancelled = matches!(slot.platform.lifecycle, OpLifecycle::Cancelled);

                    if is_cancelled {
                        let generation = slot.entry.generation.load(Ordering::Acquire);
                        let mut completed = slot.complete();
                        let (payload, detail) = completed.take_completion_data();
                        
                        pending_events.push(CompletionSidecar {
                            user_data,
                            generation,
                            res: cqe.result(),
                            flags: cqe.flags(),
                            payload,
                            detail,
                        });
                        let _ = completed.take_op();
                        self.ops.remove(user_data);
                    } else {
                        let res_val = cqe.result();
                        let final_res = slot.with_op_mut(|op| {
                            unsafe { (op.vtable.on_complete)(op, res_val) }
                        }).unwrap_or_else(|| {
                            if res_val >= 0 {
                                Ok(res_val as usize)
                            } else {
                                Err(io::Error::from_raw_os_error(-res_val))
                            }
                        });

                        let mut completed = slot.complete();
                        let generation = completed.entry.generation.load(Ordering::Acquire);
                        let res_code = io_result_to_event_res(&final_res);
                        
                        let (payload, mut detail): (Option<ErasedPayload>, Option<io::Result<usize>>) = completed.take_completion_data();
                        if detail.is_none() {
                            detail = clone_result_if_non_os_error(&final_res);
                        }
                        
                        pending_events.push(CompletionSidecar {
                            user_data,
                            generation,
                            res: res_code,
                            flags: cqe.flags(),
                            payload,
                            detail,
                        });
                        let _ = completed.take_op();
                        self.ops.remove(user_data);
                    }
                }
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
            self.submit_waker();
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
fn io_result_to_event_res(res: &io::Result<usize>) -> i32 {
    match res {
        Ok(v) => (*v).min(i32::MAX as usize) as i32,
        Err(e) => -e.raw_os_error().unwrap_or(1).abs(),
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

#[inline]
fn clone_result_if_non_os_error(res: &io::Result<usize>) -> Option<io::Result<usize>> {
    match res {
        Ok(_) => None,
        Err(e) => {
            if e.raw_os_error().is_some() {
                None
            } else {
                Some(Err(io::Error::new(e.kind(), e.to_string())))
            }
        }
    }
}

impl Drop for UringDriver {
    fn drop(&mut self) {}
}

impl Driver for UringDriver {
    type Op = UringOp;
    type Handle = RawHandle;
    type Sidecar = ();

    fn reserve_op(&mut self) -> io::Result<(usize, u32)> {
        match self.ops.insert(OpEntry::new(UringOpState::new())) {
            Ok(OpHandle {
                index: id,
                generation,
            }) => {
                trace!(id, generation, "Reserved op slot");
                Ok((id, generation))
            }
            Err(_) => Err(io::Error::new(
                io::ErrorKind::OutOfMemory,
                "OpRegistry full",
            )),
        }
    }

    fn slot_table(
        &self,
    ) -> std::sync::Arc<veloq_driver_core::slot::SlotTable<Self::Op, Self::Sidecar>> {
        self.ops.shared.clone()
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
    ) -> Outcome<io::Result<Poll<()>>> {
        let op: UringOp = op_in.take().expect("submit called with empty Option");
        let strategy = op.vtable.strategy;
        if strategy == crate::op::SubmissionStrategy::BackgroundOnly {
            *op_in = Some(op);
            return binder.err(io::Error::new(
                io::ErrorKind::Unsupported,
                "background op cannot be submitted normally",
            ));
        }

        match strategy {
            crate::op::SubmissionStrategy::BackgroundOnly => unreachable!(),
            crate::op::SubmissionStrategy::SubmitSqe => {
                self.submit_sqe_internal(user_data, op, op_in, binder)
            }
            crate::op::SubmissionStrategy::SoftwareTimer => {
                self.submit_timer_internal(user_data, op, op_in, binder)
            }
        }
    }

    fn submit_background(&mut self, mut op: Self::Op) -> io::Result<()> {
        let strategy = op.vtable.strategy;
        if strategy == crate::op::SubmissionStrategy::BackgroundOnly {
            let sqe =
                unsafe { (op.vtable.make_sqe)(&mut op, self).user_data(BACKGROUND_USER_DATA) };

            if !self.push_entry(sqe) {
                return Err(io::Error::other("sq full"));
            }
            Ok(())
        } else {
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "background op only supports BackgroundOnly strategy",
            ))
        }
    }

    fn submit_queue(&mut self) -> io::Result<()> {
        self.flush_cancellations();
        self.flush_backlog();
        self.submit_to_kernel()
    }

    fn wait(&mut self) -> io::Result<()> {
        self.wait_internal()
    }

    fn process_completions(&mut self) {
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
    ) -> io::Result<usize> {
        self.wait_internal()?;
        Ok(self.drain_completions(out))
    }

    fn cancel_op(&mut self, user_data: usize) {
        self.cancel_op_internal(user_data);
    }

    fn register_chunk(&mut self, id: u16, ptr: *const u8, len: usize) -> io::Result<()> {
        self.register_chunk_internal(id, ptr, len)
    }

    fn register_files(&mut self, files: &[RawHandle]) -> io::Result<Vec<IoFd>> {
        let fds: Vec<i32> = files.iter().map(|h| h.fd).collect();
        self.ring.submitter().register_files(&fds)?;

        let mut fixed_fds = Vec::with_capacity(files.len());
        for i in 0..files.len() {
            fixed_fds.push(IoFd::Fixed(i as u32));
        }
        Ok(fixed_fds)
    }

    fn unregister_files(&mut self, _files: Vec<IoFd>) -> io::Result<()> {
        self.ring.submitter().unregister_files()
    }

    fn wake(&mut self) -> io::Result<()> {
        let buf = 1u64.to_ne_bytes();
        let ret = unsafe { libc::write(self.waker_fd.fd, buf.as_ptr() as *const _, 8) };
        if ret < 0 {
            let err = io::Error::last_os_error();
            if err.raw_os_error() == Some(libc::EAGAIN) {
                return Ok(());
            }
            return Err(err);
        }
        Ok(())
    }

    fn inner_handle(&self) -> RawHandle {
        RawHandle {
            fd: self.ring.as_raw_fd(),
        }
    }

    fn create_waker(&self) -> Arc<dyn RemoteWaker> {
        Arc::new(UringWaker {
            fd: self.waker_fd.clone(),
            is_waked: self.is_waked.clone(),
        })
    }

    fn driver_id(&self) -> usize {
        self.waker_fd.fd as usize
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
    ) -> Outcome<io::Result<Poll<()>>> {
        let _ =
            self.ops
                .with_slot_storage_mut(user_data, |slot_op, _result, _payload, _sidecar| {
                    *slot_op = Some(op);
                });

        match self.submit_from_slot(user_data) {
            Ok(true) => binder.ok(Poll::Ready(())),
            Ok(false) => {
                debug!(user_data, "SQ full, pushing to backlog");
                if let Some(entry) = self.ops.get_mut(user_data) {
                    entry.platform_data.lifecycle = OpLifecycle::Pending;
                }
                self.push_backlog(user_data);
                binder.ok(Poll::Pending)
            }
            Err(e) => {
                let op = self
                    .ops
                    .with_slot_storage_mut(user_data, |slot_op, _result, _payload, _sidecar| {
                        slot_op.take().unwrap()
                    })
                    .expect("slot storage missing in submit_sqe recovery");
                *op_in = Some(op);
                binder.err(e)
            }
        }
    }

    fn submit_timer_internal(
        &mut self,
        user_data: usize,
        op: <Self as Driver>::Op,
        op_in: &mut Option<<Self as Driver>::Op>,
        binder: SubmitBinder,
    ) -> Outcome<io::Result<Poll<()>>> {
        let _ =
            self.ops
                .with_slot_storage_mut(user_data, |slot_op, _result, _payload, _sidecar| {
                    *slot_op = Some(op);
                });

        match self.submit_from_slot(user_data) {
            Ok(true) => binder.ok(Poll::Ready(())),
            Ok(false) => {
                debug!(
                    user_data,
                    "SQ full (unexpected for timer), pushing to backlog"
                );
                if let Some(entry) = self.ops.get_mut(user_data) {
                    entry.platform_data.lifecycle = OpLifecycle::Pending;
                }
                self.push_backlog(user_data);
                binder.ok(Poll::Pending)
            }
            Err(e) => {
                let op = self
                    .ops
                    .with_slot_storage_mut(user_data, |slot_op, _result, _payload, _sidecar| {
                        slot_op.take().unwrap()
                    })
                    .expect("slot storage missing in submit_timer recovery");
                *op_in = Some(op);
                binder.err(e)
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
