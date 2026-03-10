use io_uring::{IoUring, opcode, squeue};
use std::collections::HashMap;
use std::collections::VecDeque;
use std::io;
use std::os::unix::io::RawFd;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use tracing::{debug, trace};

use crate::config::{IoMode, UringConfig};
use crate::driver::op_registry::OpRegistry;
use crate::driver::uring::op::UringOp;
use crate::driver::{
    CompletionEvent, CompletionSidecar, RemoteWaker, SharedCompletionQueue, SharedCompletionTable,
    encode_completion_token,
};
use crate::op::IntoPlatformOp;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum OpLifecycle {
    /// Created, waiting to be submitted
    Pending,
    /// Submitted to ring or timer wheel
    InFlight,
    /// Completion arrived (result is in Slot)
    #[default]
    Completed,
    /// Aborted by user
    Cancelled,
}

#[derive(Clone)]
pub struct UringOpState {
    pub(crate) lifecycle: OpLifecycle,
    pub(crate) next: Option<usize>,
    pub(crate) timer_id: Option<veloq_wheel::TaskId>,
}

impl Default for UringOpState {
    fn default() -> Self {
        Self {
            lifecycle: OpLifecycle::Completed,
            next: None,
            timer_id: None,
        }
    }
}

impl UringOpState {
    pub(crate) fn new() -> Self {
        Self::default()
    }
}

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
pub(crate) const MAX_CHUNKS: usize = 1024;
const REGISTER_FAILURE_RETRY_COOLDOWN: Duration = Duration::from_millis(250);

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct UringRegistrationStats {
    pub(crate) chunk_register_attempts: u64,
    pub(crate) chunk_register_success: u64,
    pub(crate) chunk_register_failures: u64,
    pub(crate) chunk_register_skipped_recent_failure: u64,
    pub(crate) submission_missing_chunk_info: u64,
}

pub struct UringDriver {
    pub(crate) ring: IoUring,
    pub(crate) ops: OpRegistry<UringOp, UringOpState>,
    pub(crate) backlog_head: Option<usize>,
    pub(crate) backlog_tail: Option<usize>,
    pub(crate) pending_cancellations: VecDeque<usize>,
    pub(crate) completion_events: SharedCompletionQueue,
    pub(crate) completion_table: SharedCompletionTable,

    pub(crate) waker_fd: Arc<EventFd>,
    pub(crate) waker_token: Option<usize>,
    pub(crate) waker_payload: Option<Box<crate::op::Wakeup>>,
    pub(crate) registered_chunks: veloq_bitset::BitSet,
    pub(crate) is_waked: Arc<AtomicBool>,

    pub(crate) wheel: veloq_wheel::Wheel<usize>,
    pub(crate) timer_buffer: Vec<usize>,
    pub(crate) registrar: Box<dyn veloq_buf::BufferRegistrar>,
    pub(crate) registration_stats: UringRegistrationStats,
    chunk_register_failures_recent: HashMap<u16, Instant>,
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
            completion_table: std::sync::Arc::new(crate::driver::CompletionTable::new(
                entries as usize,
            )),
            waker_fd: Arc::new(EventFd { fd: waker_fd }),
            waker_token: None,
            waker_payload: None,
            registered_chunks: veloq_bitset::BitSet::new(MAX_CHUNKS),
            is_waked,

            wheel: veloq_wheel::Wheel::new(veloq_wheel::WheelConfig::default()),
            timer_buffer: Vec::new(),
            registrar: Box::new(veloq_buf::NoopRegistrar),
            registration_stats: UringRegistrationStats::default(),
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

        // We ignore errors here? Or warn?
        // If this fails, then subsequent register_chunk will likely fail or revert to slow path.
        if let Err(e) = unsafe { driver.ring.submitter().register_buffers(&iovecs) } {
            tracing::warn!("Failed to register sparse buffers: {}", e);
        }

        Ok(driver)
    }

    /// Tries to submit the op at `user_data` to the ring or timer wheel.
    /// Returns:
    /// - Ok(true): Submitted (SQE pushed or Timer started)
    /// - Ok(false): SQ Full (SQE not pushed)
    /// - Err(e): Fatal error (e.g. missing timer duration)
    pub(crate) fn submit_from_slot(&mut self, user_data: usize) -> io::Result<bool> {
        let (sqe_opt, strategy, duration_opt) = {
            let slot = &self.ops.shared.slots[user_data];
            unsafe {
                if let Some(res) = (*slot.op.get()).as_mut() {
                    let vtable = res.vtable.as_ref();
                    let strategy = vtable.strategy;

                    match strategy {
                        crate::driver::uring::op::SubmissionStrategy::SubmitSqe => {
                            // --- Lazy Registration ---
                            let mut chunks = [0u16; 4];
                            let count = (vtable.resolve_chunks)(res, &mut chunks);
                            for &chunk_id in chunks.iter().take(count) {
                                let index = chunk_id as usize;
                                // Explicitly handle bitset error
                                let is_registered =
                                    self.registered_chunks.get(index).map_err(|e| {
                                        io::Error::other(format!(
                                            "BitSet error getting {}: {:?}",
                                            index, e
                                        ))
                                    })?;

                                if !is_registered
                                    && let Some(info) = self.registrar.resolve_chunk_info(chunk_id)
                                {
                                    self.register_chunk(
                                        info.id,
                                        info.ptr.as_ptr(),
                                        info.len.get(),
                                    )?;
                                } else if !is_registered {
                                    self.registration_stats.submission_missing_chunk_info = self
                                        .registration_stats
                                        .submission_missing_chunk_info
                                        .saturating_add(1);
                                    return Err(io::Error::other(format!(
                                        "Missing chunk info for lazy registration: chunk_id={chunk_id}, user_data={user_data}"
                                    )));
                                }
                            }
                            // -------------------------

                            let s = (vtable.make_sqe)(res, self).user_data(user_data as u64);
                            (Some(s), strategy, None)
                        }
                        crate::driver::uring::op::SubmissionStrategy::SoftwareTimer => {
                            let d = (vtable.get_timeout)(res);
                            (None, strategy, d)
                        }
                        _ => (None, strategy, None),
                    }
                } else {
                    return Err(io::Error::other("Op missing in slot"));
                }
            }
        };

        match strategy {
            crate::driver::uring::op::SubmissionStrategy::SubmitSqe => {
                if let Some(sqe) = sqe_opt {
                    if self.push_entry(sqe) {
                        if let Some(entry) = self.ops.get_mut(user_data) {
                            entry.platform_data.lifecycle = OpLifecycle::InFlight;
                        }
                        trace!(user_data, "Submitted to SQ");
                        Ok(true)
                    } else {
                        debug!(user_data, "SQ full");
                        Ok(false)
                    }
                } else {
                    Err(io::Error::other("SQE generation failed"))
                }
            }
            crate::driver::uring::op::SubmissionStrategy::SoftwareTimer => {
                if let Some(duration) = duration_opt {
                    let task_id = self.wheel.insert(user_data, duration);
                    if let Some(entry) = self.ops.get_mut(user_data) {
                        entry.platform_data.lifecycle = OpLifecycle::InFlight;
                        entry.platform_data.timer_id = Some(task_id);
                    }
                    trace!(user_data, ?duration, "Registered software timer");
                    Ok(true)
                } else {
                    Err(io::Error::other("Timer duration missing"))
                }
            }
            _ => Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "Unsupported strategy for slot submission",
            )),
        }
    }

    fn submit_waker(&mut self) {
        if self.waker_token.is_some() {
            return;
        }

        let fd = self.waker_fd.fd;
        let op = crate::op::Wakeup {
            fd: crate::op::IoFd::Raw(crate::RawHandle { fd }),
        };
        let (uring_op, payload) = <crate::op::Wakeup as IntoPlatformOp<
            crate::driver::uring::op::UringOp,
        >>::into_kernel_and_payload(op);

        let state = UringOpState {
            lifecycle: OpLifecycle::Pending, // Will change to InFlight below
            next: None,
            timer_id: None,
        };

        let result = self.ops.alloc(state);

        if let Ok(crate::driver::op_registry::AllocResult {
            handle:
                crate::driver::op_registry::OpHandle {
                    index: user_data, ..
                },
        }) = result
        {
            self.waker_token = Some(user_data);
            self.waker_payload = Some(payload);
            let slot = &self.ops.shared.slots[user_data];

            // Put op into slot
            unsafe {
                *slot.op.get() = Some(uring_op);
            }

            // Generate SQE
            let sqe = {
                let slot = &self.ops.shared.slots[user_data];
                unsafe {
                    let op_ref = (*slot.op.get()).as_mut().unwrap();
                    (op_ref.vtable.as_ref().make_sqe)(op_ref, self).user_data(user_data as u64)
                }
            };

            if self.push_entry(sqe) {
                if let Some(entry) = self.ops.get_mut(user_data) {
                    entry.platform_data.lifecycle = OpLifecycle::InFlight;
                }
            } else {
                self.push_backlog(user_data);
            }
        } else {
            // Should not happen during init unless 0 entries
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

    pub(crate) fn wait(&mut self) -> io::Result<()> {
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
                if let Some(entry) = self.ops.get_mut(user_data)
                    && matches!(entry.platform_data.lifecycle, OpLifecycle::InFlight)
                {
                    entry.platform_data.lifecycle = OpLifecycle::Completed;
                    entry.platform_data.timer_id = None;

                    let generation = {
                        let slot = &self.ops.shared.slots[user_data];
                        let generation = slot.generation.load(Ordering::Acquire);
                        let _ = unsafe { (*slot.op.get()).take() };
                        generation
                    };
                    let slot = &self.ops.shared.slots[user_data];
                    let payload = unsafe { (*slot.payload.get()).take() };
                    let detail = unsafe { (*slot.result.get()).take() };
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
                entry.platform_data.lifecycle,
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

                if user_data < self.ops.local.len() {
                    let op_state = &mut self.ops.local[user_data].platform_data;
                    let slot = &self.ops.shared.slots[user_data];

                    // Don't touch op if Cancelled
                    if matches!(op_state.lifecycle, OpLifecycle::Cancelled) {
                        let generation = slot.generation.load(Ordering::Acquire);
                        let payload = unsafe { (*slot.payload.get()).take() };
                        let detail = unsafe { (*slot.result.get()).take() };
                        pending_events.push(CompletionSidecar {
                            user_data,
                            generation,
                            res: cqe.result(),
                            flags: cqe.flags(),
                            payload,
                            detail,
                        });
                        unsafe {
                            *slot.op.get() = None;
                        }
                        self.ops.remove(user_data);
                    } else {
                        // Standard completion
                        let res_val = cqe.result();
                        // Call on_complete
                        let final_res = unsafe {
                            if let Some(op) = (*slot.op.get()).as_mut() {
                                (op.vtable.as_ref().on_complete)(op, res_val)
                            } else {
                                // Op missing? unexpected
                                if res_val >= 0 {
                                    Ok(res_val as usize)
                                } else {
                                    Err(io::Error::from_raw_os_error(-res_val))
                                }
                            }
                        };

                        op_state.lifecycle = OpLifecycle::Completed;
                        let generation = slot.generation.load(Ordering::Acquire);
                        let res_code = io_result_to_event_res(&final_res);
                        let mut detail = unsafe { (*slot.result.get()).take() };
                        if detail.is_none() {
                            detail = clone_result_if_non_os_error(&final_res);
                        }
                        let payload = unsafe { (*slot.payload.get()).take() };
                        pending_events.push(CompletionSidecar {
                            user_data,
                            generation,
                            res: res_code,
                            flags: cqe.flags(),
                            payload,
                            detail,
                        });
                        let _ = unsafe { (*slot.op.get()).take() };
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
                // Remove existing waker op/slot
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

    pub(crate) fn flush_cancellations(&mut self) {
        let mut submitted_count = 0;
        let limit = self.pending_cancellations.len();

        while submitted_count < limit {
            if let Some(user_data) = self.pending_cancellations.front().cloned() {
                if !self.ops.contains(user_data) {
                    self.pending_cancellations.pop_front();
                    continue;
                }

                let cancel_sqe = opcode::AsyncCancel::new(user_data as u64)
                    .build()
                    .user_data(CANCEL_USER_DATA);

                if self.push_entry(cancel_sqe) {
                    self.pending_cancellations.pop_front();
                    submitted_count += 1;
                } else {
                    break;
                }
            } else {
                break;
            }
        }
    }

    pub(crate) fn flush_backlog(&mut self) {
        enum BacklogAction {
            Submit,
            Cancel,
            Drop,
        }

        while let Some(user_data) = self.backlog_head {
            // Inspect state to decide action before taking mutable borrow for processing.
            // We need to check if the Op is still valid/pending cancellation.

            let mut action = BacklogAction::Drop;

            if let Some(entry) = self.ops.get(user_data) {
                action = match entry.platform_data.lifecycle {
                    OpLifecycle::Cancelled => BacklogAction::Cancel,
                    OpLifecycle::Pending => {
                        // Check if op exists in slot
                        let slot = &self.ops.shared.slots[user_data];
                        // SAFETY: Pending state implies Driver owns Op
                        if unsafe { (*slot.op.get()).is_some() } {
                            BacklogAction::Submit
                        } else {
                            BacklogAction::Drop
                        }
                    }
                    _ => BacklogAction::Drop,
                };
            }

            match action {
                BacklogAction::Cancel => {
                    self.pop_backlog();
                    self.cancel_op_internal(user_data);
                }
                BacklogAction::Drop => {
                    self.pop_backlog();
                }
                BacklogAction::Submit => {
                    match self.submit_from_slot(user_data) {
                        Ok(true) => {
                            self.pop_backlog();
                        }
                        Ok(false) => {
                            // SQ Full, stop processing backlog
                            break;
                        }
                        Err(_) => {
                            // Error during submission (e.g. invalid op state)
                            // We should probably drop it from backlog to avoid infinite loop
                            self.pop_backlog();
                        }
                    }
                }
            }
        }
    }

    pub(crate) fn push_backlog(&mut self, user_data: usize) {
        if let Some(tail) = self.backlog_tail {
            if let Some(entry) = self.ops.get_mut(tail) {
                entry.platform_data.next = Some(user_data);
            }
            self.backlog_tail = Some(user_data);
        } else {
            self.backlog_head = Some(user_data);
            self.backlog_tail = Some(user_data);
        }
        if let Some(entry) = self.ops.get_mut(user_data) {
            entry.platform_data.next = None;
        }
    }

    pub(crate) fn pop_backlog(&mut self) -> Option<usize> {
        let head = self.backlog_head?;
        let next = if let Some(entry) = self.ops.get_mut(head) {
            entry.platform_data.next
        } else {
            None
        };

        self.backlog_head = next;
        if next.is_none() {
            self.backlog_tail = None;
        }

        if let Some(entry) = self.ops.get_mut(head) {
            entry.platform_data.next = None;
        }

        Some(head)
    }

    pub(crate) fn register_chunk(&mut self, id: u16, ptr: *const u8, len: usize) -> io::Result<()> {
        if let Some(last_fail) = self.chunk_register_failures_recent.get(&id)
            && last_fail.elapsed() < REGISTER_FAILURE_RETRY_COOLDOWN
        {
            self.registration_stats
                .chunk_register_skipped_recent_failure = self
                .registration_stats
                .chunk_register_skipped_recent_failure
                .saturating_add(1);
            return Err(io::Error::other(format!(
                "io_uring register_chunk skipped due to recent failure: chunk_id={id}"
            )));
        }

        let index = id as usize;
        if index >= MAX_CHUNKS {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "ChunkID exceeds MAX_CHUNKS",
            ));
        }

        let iovecs = [libc::iovec {
            iov_base: ptr as *mut _,
            iov_len: len,
        }];

        // Use register_buffers_update
        // offset = index
        self.registration_stats.chunk_register_attempts = self
            .registration_stats
            .chunk_register_attempts
            .saturating_add(1);
        let register_result = unsafe {
            self.ring
                .submitter()
                .register_buffers_update(index as u32, &iovecs, None)
        };
        if let Err(e) = register_result {
            self.registration_stats.chunk_register_failures = self
                .registration_stats
                .chunk_register_failures
                .saturating_add(1);
            self.chunk_register_failures_recent
                .insert(id, Instant::now());
            return Err(e);
        }

        // Mark as registered in local bitset
        let _ = self.registered_chunks.set(index); // ignore out of bounds error as we checked
        self.chunk_register_failures_recent.remove(&id);
        self.registration_stats.chunk_register_success = self
            .registration_stats
            .chunk_register_success
            .saturating_add(1);

        Ok(())
    }

    pub(crate) fn cancel_op_internal(&mut self, user_data: usize) {
        let (action, timer_id) = if let Some(op) = self.ops.get_mut(user_data) {
            match &op.platform_data.lifecycle {
                OpLifecycle::Completed | OpLifecycle::Cancelled => (None, None),
                OpLifecycle::Pending => (Some(OpLifecycle::Pending), None),
                OpLifecycle::InFlight => (Some(OpLifecycle::InFlight), op.platform_data.timer_id),
            }
        } else {
            (None, None)
        };

        match action {
            None => {}
            Some(OpLifecycle::Completed) | Some(OpLifecycle::Cancelled) => {} // already done
            Some(OpLifecycle::Pending) => {
                if let Some(op) = self.ops.get_mut(user_data) {
                    op.platform_data.lifecycle = OpLifecycle::Cancelled;
                    let generation = {
                        let slot = &self.ops.shared.slots[user_data];
                        let generation = slot.generation.load(Ordering::Acquire);
                        let _ = unsafe { (*slot.op.get()).take() };
                        generation
                    };
                    let slot = &self.ops.shared.slots[user_data];
                    let payload = unsafe { (*slot.payload.get()).take() };
                    let detail = unsafe { (*slot.result.get()).take() };
                    self.push_completion_event(CompletionSidecar {
                        user_data,
                        generation,
                        res: -libc::ECANCELED,
                        flags: 0,
                        payload,
                        detail,
                    });
                    self.ops.remove(user_data);
                }
            }
            Some(OpLifecycle::InFlight) => {
                if let Some(op) = self.ops.get_mut(user_data) {
                    op.platform_data.lifecycle = OpLifecycle::Cancelled;
                }

                if let Some(tid) = timer_id {
                    self.wheel.cancel(tid);
                    if self.ops.get_mut(user_data).is_some() {
                        let generation = {
                            let slot = &self.ops.shared.slots[user_data];
                            let generation = slot.generation.load(Ordering::Acquire);
                            let _ = unsafe { (*slot.op.get()).take() };
                            generation
                        };
                        let slot = &self.ops.shared.slots[user_data];
                        let payload = unsafe { (*slot.payload.get()).take() };
                        let detail = unsafe { (*slot.result.get()).take() };
                        self.push_completion_event(CompletionSidecar {
                            user_data,
                            generation,
                            res: -libc::ECANCELED,
                            flags: 0,
                            payload,
                            detail,
                        });
                        self.ops.remove(user_data);
                    }
                    return;
                }

                let cancel_sqe = opcode::AsyncCancel::new(user_data as u64)
                    .build()
                    .user_data(CANCEL_USER_DATA);

                if !self.push_entry(cancel_sqe) {
                    self.pending_cancellations.push_back(user_data);
                }

                // Cancellation is async, we wait for CQE to clean up.
            }
        }
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
    fn drop(&mut self) {
        // waker_fd is closed when Arc<EventFd> drops
    }
}
