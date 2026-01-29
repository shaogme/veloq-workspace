use crate::io::driver::op_registry::{OpEntry, OpRegistry};
use crate::io::driver::{DetachedCompleter, RemoteWaker};
use io_uring::{IoUring, opcode, squeue};
use std::collections::VecDeque;
use std::io;
use std::os::unix::io::RawFd;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use tracing::{debug, trace};

use crate::io::driver::uring::op::UringOp;
use crate::io::op::IntoPlatformOp;

#[derive(Default)]
pub struct UringOpState {
    pub submitted: bool,
    pub next: Option<usize>,
    pub detached_completer: Option<Box<dyn DetachedCompleter<UringOp>>>,
    pub timer_id: Option<veloq_wheel::TaskId>,
}

impl UringOpState {
    pub fn new() -> Self {
        Self::default()
    }
}

pub(crate) struct UringWaker {
    pub(crate) fd: RawFd,
    pub(crate) is_waked: Arc<AtomicBool>,
}

impl RemoteWaker for UringWaker {
    fn wake(&self) -> io::Result<()> {
        if !self.is_waked.swap(true, Ordering::SeqCst) {
            let buf = 1u64.to_ne_bytes();
            let ret = unsafe { libc::write(self.fd, buf.as_ptr() as *const _, 8) };
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

impl Drop for UringWaker {
    fn drop(&mut self) {
        unsafe {
            libc::close(self.fd);
        }
    }
}

/// Special user_data value for cancel operations.
/// We use u64::MAX - 1 because u64::MAX is already reserved.
/// CQEs with this user_data are ignored (they're just confirmations that cancel was submitted).
pub(crate) const CANCEL_USER_DATA: u64 = u64::MAX - 1;
pub(crate) const BACKGROUND_USER_DATA: u64 = u64::MAX - 2;

pub struct UringDriver {
    /// The actual io_uring instance
    pub(crate) ring: IoUring,
    /// Store for in-flight operations.
    /// The key (usize) is used as the io_uring user_data.
    /// Payload (UringOpState) tracks submission state and backlog list.
    pub(crate) ops: OpRegistry<UringOp, UringOpState>,
    /// Head of the intrusive backlog list.
    pub(crate) backlog_head: Option<usize>,
    /// Tail of the intrusive backlog list.
    pub(crate) backlog_tail: Option<usize>,
    /// Queue for cancellation requests that failed to submit.
    pub(crate) pending_cancellations: VecDeque<usize>,

    pub(crate) waker_fd: RawFd,
    pub(crate) waker_token: Option<usize>,
    pub(crate) buffers_registered: bool,
    pub(crate) is_waked: Arc<AtomicBool>,

    pub(crate) wheel: veloq_wheel::Wheel<usize>,
    pub(crate) timer_buffer: Vec<usize>,
}

impl UringDriver {
    pub fn new(config: &crate::config::Config) -> io::Result<Self> {
        let entries = config.uring.entries;
        let mut builder = IoUring::builder();

        builder
            .setup_coop_taskrun() // Reduce IPIs (Kernel 5.19+)
            .setup_single_issuer() // Optimized for single-threaded submission (Kernel 6.0+)
            .setup_defer_taskrun(); // Defer work until enter (Kernel 6.1+)

        if config.uring.mode == crate::config::IoMode::Polling {
            builder.setup_sqpoll(config.uring.sqpoll_idle_ms); // Kernel 5.1+
        }

        let ring = builder.build(entries).or_else(|e| {
            // Fallback for older kernels if flags are unsupported (EINVAL)
            if e.raw_os_error() == Some(libc::EINVAL) {
                // If the optimized build failed, try a basic one.
                IoUring::new(entries)
            } else {
                Err(e)
            }
        })?;

        let ops = OpRegistry::with_capacity(entries as usize);

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
            waker_fd,
            waker_token: None,
            buffers_registered: false,
            is_waked,

            wheel: veloq_wheel::Wheel::new(veloq_wheel::WheelConfig::default()),
            timer_buffer: Vec::new(),
        };

        driver.submit_waker();

        Ok(driver)
    }

    fn submit_waker(&mut self) {
        if self.waker_token.is_some() {
            return;
        }

        let fd = self.waker_fd;
        let op = crate::io::op::Wakeup {
            fd: crate::io::op::IoFd::Raw(crate::io::RawHandle { fd }),
        };
        // Use into_platform_op to convert to UringOp
        let uring_op = <crate::io::op::Wakeup as IntoPlatformOp<UringDriver>>::into_platform_op(op);

        let user_data = self
            .ops
            .insert(OpEntry::new(Some(uring_op), UringOpState::new()));
        self.waker_token = Some(user_data);

        // Generate SQE
        let sqe = if let Some(entry) = self.ops.get_mut(user_data) {
            if let Some(ref mut resources) = entry.resources {
                Some(unsafe {
                    (resources.vtable.make_sqe)(resources, self.waker_fd as usize)
                        .user_data(user_data as u64)
                })
            } else {
                None
            }
        } else {
            None
        };

        if let Some(sqe) = sqe {
            if self.push_entry(sqe) {
                if let Some(entry) = self.ops.get_mut(user_data) {
                    entry.platform_data.submitted = true;
                }
            } else {
                // Waker failed to submit. This is bad but handled by backlog logic.
                self.push_backlog(user_data);
            }
        }
    }

    pub fn submit_to_kernel(&mut self) -> io::Result<()> {
        trace!("submit_to_kernel entered");
        if self.ring.params().is_setup_sqpoll() {
            if self.ring.submission().need_wakeup() {
                self.ring.submit()?;
            }
        } else {
            self.ring.submit()?;
        }
        // Always try to flush backlog after submit, as submit likely freed up SQ space
        self.flush_backlog();
        Ok(())
    }

    /// Wait for completions.
    pub fn wait(&mut self) -> io::Result<()> {
        // Try to flush backlog first before waiting
        self.flush_cancellations();
        self.flush_backlog();

        if self.ops.is_empty() {
            return Ok(());
        }

        if !self.ring.completion().is_empty() {
            self.process_completions_internal();
            // Also process timers even if we have IO completions to be fair?
            // Fall through to timer processing below
        } else {
            // Need to wait. Calculate timeout.
            let next_timeout = self.wheel.next_timeout();
            let start = std::time::Instant::now();

            if let Some(duration) = next_timeout {
                let ts = io_uring::types::Timespec::new()
                    .sec(duration.as_secs())
                    .nsec(duration.subsec_nanos());

                // Use submit_with_args to pass timeout
                let args = io_uring::types::SubmitArgs::new().timespec(&ts);
                match self.ring.submitter().submit_with_args(1, &args) {
                    Ok(_) => {}
                    Err(ref e) if e.raw_os_error() == Some(libc::ETIME) => {
                        // Timeout expired without IO
                    }
                    Err(e) => return Err(e),
                }
            } else {
                self.ring.submit_and_wait(1)?;
            }

            // Advance wheel
            let elapsed = start.elapsed();
            self.wheel.advance(elapsed, &mut self.timer_buffer);

            for &user_data in &self.timer_buffer {
                if let Some(entry) = self.ops.get_mut(user_data) {
                    // Mark timer task as completed
                    if !entry.cancelled && entry.result.is_none() {
                        entry.result = Some(Ok(0));
                        if let Some(waker) = entry.waker.take() {
                            waker.wake();
                        }
                    }
                    entry.platform_data.timer_id = None;
                    // Note: We don't remove it here. The task will poll_op, see Ready, and remove it.
                }
            }
            self.timer_buffer.clear();
        }

        self.process_completions_internal();

        // After wait (which implies submit), we might have space
        self.flush_cancellations();
        self.flush_backlog();
        Ok(())
    }

    /// Process the completion queue.
    pub(crate) fn process_completions_internal(&mut self) {
        let mut needs_waker_resubmit = false;

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

                if self.ops.contains(user_data) {
                    let op = &mut self.ops[user_data];

                    let res = if let Some(ref mut resources) = op.resources {
                        unsafe { (resources.vtable.on_complete)(resources, cqe.result()) }
                    } else if cqe.result() >= 0 {
                        Ok(cqe.result() as usize)
                    } else {
                        Err(io::Error::from_raw_os_error(-cqe.result()))
                    };

                    if op.cancelled {
                        self.ops.remove(user_data);
                    } else {
                        // Check if it has a detached completer
                        if let Some(completer) = op.platform_data.detached_completer.take() {
                            // Must take resources
                            if let Some(res_op) = op.resources.take() {
                                completer.complete(res, res_op);
                            }
                            // Remove op
                            self.ops.remove(user_data);
                        } else {
                            op.result = Some(res);
                            if let Some(waker) = op.waker.take() {
                                waker.wake();
                            }
                        }
                    }
                }
            }
        }

        if needs_waker_resubmit {
            self.is_waked.store(false, Ordering::SeqCst);
            if let Some(token) = self.waker_token.take() {
                self.ops.remove(token);
            }
            self.submit_waker();
            // Ensure waker is in the ring immediately to avoid lost wakeups
            self.flush_backlog();
        }
    }

    /// Try to push an entry to the submission queue.
    /// Returns true if successful, false if SQ is full.
    pub(crate) fn push_entry(&mut self, entry: squeue::Entry) -> bool {
        trace!("Pushing SQE user_data={}", entry.get_user_data());
        let mut sq = self.ring.submission();

        if unsafe { sq.push(&entry) }.is_ok() {
            return true;
        }

        // SQ full, try to submit (flush)
        drop(sq);
        let _ = self.ring.submit(); // Ignore error here, we retry push anyway

        let mut sq = self.ring.submission();
        if unsafe { sq.push(&entry) }.is_ok() {
            return true;
        }

        debug!("SQ full even after flush");
        false
    }

    /// Try to submit pending cancellations
    pub(crate) fn flush_cancellations(&mut self) {
        let mut submitted_count = 0;
        let limit = self.pending_cancellations.len();

        while submitted_count < limit {
            if let Some(user_data) = self.pending_cancellations.front().cloned() {
                // If the operation is gone or completed, we don't need to cancel anymore
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

    /// Attempt to submit operations from the backlog.
    pub(crate) fn flush_backlog(&mut self) {
        while let Some(user_data) = self.backlog_head {
            // 1. Check state
            let (is_cancelled, is_submitted, has_resources) =
                if let Some(entry) = self.ops.get_mut(user_data) {
                    (
                        entry.cancelled,
                        entry.platform_data.submitted,
                        entry.resources.is_some(),
                    )
                } else {
                    // Op missing? Pop and continue
                    self.pop_backlog();
                    continue;
                };

            // Optimize: If cancelled, do NOT submit.
            if is_cancelled {
                self.pop_backlog();
                // Complete with ECANCELED
                if let Some(entry) = self.ops.get_mut(user_data) {
                    entry.result = Some(Err(io::Error::from_raw_os_error(libc::ECANCELED)));
                    if let Some(waker) = entry.waker.take() {
                        waker.wake();
                    }
                }
                continue;
            }

            if is_submitted {
                self.pop_backlog();
                continue;
            }

            if !has_resources {
                self.pop_backlog();
                continue;
            }

            // 2. Generate SQE
            let sqe = {
                let entry = self.ops.get_mut(user_data).unwrap();
                let res = entry.resources.as_mut().unwrap();
                unsafe {
                    (res.vtable.make_sqe)(res, self.waker_fd as usize).user_data(user_data as u64)
                }
            };

            // 3. Push
            if self.push_entry(sqe) {
                self.pop_backlog();
                if let Some(entry) = self.ops.get_mut(user_data) {
                    entry.platform_data.submitted = true;
                    if let Some(waker) = entry.waker.take() {
                        waker.wake();
                    }
                }
            } else {
                // Full
                break;
            }
        }
    }

    pub(crate) fn push_backlog(&mut self, user_data: usize) {
        if let Some(tail) = self.backlog_tail {
            // Update old tail
            if let Some(entry) = self.ops.get_mut(tail) {
                entry.platform_data.next = Some(user_data);
            }
            self.backlog_tail = Some(user_data);
        } else {
            // Empty
            self.backlog_head = Some(user_data);
            self.backlog_tail = Some(user_data);
        }
        // Ensure new node terminates
        if let Some(entry) = self.ops.get_mut(user_data) {
            entry.platform_data.next = None;
        }
    }

    pub(crate) fn pop_backlog(&mut self) -> Option<usize> {
        let head = self.backlog_head?;
        // get next
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

    pub(crate) fn register_buffer_regions(
        &mut self,
        regions: &[crate::io::buffer::BufferRegion],
    ) -> io::Result<Vec<usize>> {
        if self.buffers_registered {
            // Assume existing registration matches?
            // Since we return indices, and they are usually 0..N, we return based on input length.
            // Ideally we shouldn't registering twice unless regions are different?
            // For now, simple behavior.
            return Ok((0..regions.len()).collect());
        }

        let iovecs: Vec<libc::iovec> = regions
            .iter()
            .map(|region| libc::iovec {
                iov_base: region.as_mut_ptr() as *mut _,
                iov_len: region.len(),
            })
            .collect();

        match unsafe { self.ring.submitter().register_buffers(&iovecs) } {
            Ok(_) => {
                self.buffers_registered = true;
                Ok((0..regions.len()).collect())
            }
            Err(e) => {
                if e.raw_os_error() == Some(libc::EBUSY) {
                    self.buffers_registered = true;
                    Ok((0..regions.len()).collect())
                } else {
                    Err(e)
                }
            }
        }
    }

    pub(crate) fn cancel_op_internal(&mut self, user_data: usize) {
        if let Some(op) = self.ops.get_mut(user_data) {
            op.cancelled = true;
            op.waker = None;

            if let Some(tid) = op.platform_data.timer_id.take() {
                self.wheel.cancel(tid);
                // If it was a pure timer (no SQE), we are done.
                // We should probably check if we need to remove it from ops immediately?
                // IOCP behavior: yes, but here we can just leave it cancelled.
                // flush_backlog handles cancelled ops by completing them with ECANCELED.
                // But this op is marked "submitted" (fake submission).
                // So it won't be in backlog.
                // We must ensure subsequent poll returns error or we remove it now.
                // If we remove it now, subsequent poll helper panics if not found?
                // OpRegistry::poll_op panics if not found.
                // So we should leave it, but set result to ECANCELED?
                if op.result.is_none() {
                    op.result = Some(Err(io::Error::from_raw_os_error(libc::ECANCELED)));
                }
                return;
            }

            let cancel_sqe = opcode::AsyncCancel::new(user_data as u64)
                .build()
                .user_data(CANCEL_USER_DATA);

            if !self.push_entry(cancel_sqe) {
                // Store for later retry
                self.pending_cancellations.push_back(user_data);
            }
        }
        // Remove from backlog if present? O(N).
        // We let flush_backlog handle it (it checks cancelled).
    }
}

impl Drop for UringDriver {
    fn drop(&mut self) {
        unsafe {
            libc::close(self.waker_fd);
        }
    }
}
