use crate::io::driver::op_registry::OpEntry;
use std::io;
use std::sync::Arc;
use std::task::{Context, Poll};
use tracing::{debug, trace};

mod inner;
pub mod op;
pub mod submit;

pub use inner::{UringDriver, UringOpState};

use crate::io::driver::{DetachedCompleter, Driver, RemoteWaker};
use inner::UringWaker;
use op::UringOp;

impl Driver for UringDriver {
    type Op = UringOp;

    fn reserve_op(&mut self) -> usize {
        let id = self.ops.insert(OpEntry::new(None, UringOpState::new()));
        trace!(id, "Reserved op slot");
        id
    }

    fn attach_detached_completer(
        &mut self,
        user_data: usize,
        completer: Box<dyn DetachedCompleter<Self::Op>>,
    ) {
        if let Some(entry) = self.ops.get_mut(user_data) {
            entry.platform_data.detached_completer = Some(completer);
        }
    }

    fn submit(
        &mut self,
        user_data: usize,
        op: Self::Op,
    ) -> Result<Poll<()>, (io::Error, Self::Op)> {
        match op.vtable.strategy {
            op::SubmissionStrategy::BackgroundOnly => Err((
                io::Error::new(
                    io::ErrorKind::Unsupported,
                    "background op cannot be submitted normally",
                ),
                op,
            )),
            op::SubmissionStrategy::SubmitSqe => {
                // 1. Store resources FIRST to ensure stable address
                if let Some(entry) = self.ops.get_mut(user_data) {
                    entry.resources = Some(op);
                } else {
                    return Err((io::Error::other("op slot not found"), op));
                }

                // 2. Generate SQE from STABLE location
                let sqe_res = if let Some(entry) = self.ops.get_mut(user_data) {
                    if let Some(res) = entry.resources.as_mut() {
                        unsafe {
                            // We construct the SQE referencing the stable 'res'
                            Some(
                                (res.vtable.make_sqe)(res, self.waker_fd as usize)
                                    .user_data(user_data as u64),
                            )
                        }
                    } else {
                        None
                    }
                } else {
                    None
                };

                // 3. Push
                if let Some(sqe) = sqe_res {
                    if self.push_entry(sqe) {
                        trace!(user_data, "Submitted to SQ");
                        if let Some(entry) = self.ops.get_mut(user_data) {
                            entry.platform_data.submitted = true;
                        }
                        Ok(Poll::Ready(()))
                    } else {
                        debug!(user_data, "SQ full, pushing to backlog");
                        if let Some(entry) = self.ops.get_mut(user_data) {
                            entry.platform_data.submitted = false;
                        }
                        self.push_backlog(user_data);
                        Ok(Poll::Pending)
                    }
                } else {
                    // Logic error: resource missing immediately after insertion or slot gone
                    // Try to recover op to return error, but it's tricky since we put it in.
                    // If we can take it back, good.
                    if let Some(entry) = self.ops.get_mut(user_data) {
                        if let Some(op) = entry.resources.take() {
                            return Err((io::Error::other("failed to access stored op"), op));
                        }
                    }
                    panic!("Critical driver error: Ops resource lost during submission!");
                }
            }
            op::SubmissionStrategy::SoftwareTimer => {
                // 1. Store resources FIRST
                if let Some(entry) = self.ops.get_mut(user_data) {
                    entry.resources = Some(op);
                } else {
                    return Err((io::Error::other("op slot not found"), op));
                }

                // 2. Extract duration from STABLE location (via helper or direct access)
                let duration_opt = if let Some(entry) = self.ops.get_mut(user_data) {
                    if let Some(res) = entry.resources.as_ref() {
                        unsafe { (res.vtable.get_timeout)(res) }
                    } else {
                        None
                    }
                } else {
                    None
                };

                if let Some(duration) = duration_opt {
                    let task_id = self.wheel.insert(user_data, duration);
                    if let Some(entry) = self.ops.get_mut(user_data) {
                        entry.platform_data.submitted = true;
                        entry.platform_data.timer_id = Some(task_id);
                    }
                    trace!(user_data, ?duration, "Registered software timer");
                    Ok(Poll::Pending)
                } else {
                    // Should not happen for SoftwareTimer strategy
                    // Recover op
                    if let Some(entry) = self.ops.get_mut(user_data) {
                        if let Some(op) = entry.resources.take() {
                            return Err((
                                io::Error::other("failed to get duration from timer op"),
                                op,
                            ));
                        }
                    }
                    panic!("Critical driver error: Ops resource lost during timer submission!");
                }
            }
        }
    }

    fn submit_background(&mut self, mut op: Self::Op) -> io::Result<()> {
        if op.vtable.strategy == op::SubmissionStrategy::BackgroundOnly {
            let sqe = unsafe {
                (op.vtable.make_sqe)(&mut op, self.waker_fd as usize)
                    .user_data(inner::BACKGROUND_USER_DATA)
            };

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

    fn poll_op(
        &mut self,
        user_data: usize,
        cx: &mut Context<'_>,
    ) -> Poll<(io::Result<usize>, Self::Op)> {
        // First check if stored in backlog (not submitted)
        if let Some(entry) = self.ops.get_mut(user_data)
            && !entry.platform_data.submitted
        {
            // Not in ring yet. Try to flush backlog.
            self.flush_backlog();
            self.flush_cancellations();

            // Check again
            let entry = self.ops.get_mut(user_data).unwrap();
            if !entry.platform_data.submitted {
                // Still not in ring. Register waker.
                if entry
                    .waker
                    .as_ref()
                    .map_or(true, |w| !w.will_wake(cx.waker()))
                {
                    entry.waker = Some(cx.waker().clone());
                }
                return Poll::Pending;
            }
        }

        // Delegate to ops registry for result check
        self.ops.poll_op(user_data, cx)
    }

    fn submit_queue(&mut self) -> io::Result<()> {
        self.flush_cancellations();
        self.flush_backlog();
        self.submit_to_kernel()
    }

    fn wait(&mut self) -> io::Result<()> {
        // self.wait() calls inherent method defined in inner.rs (and imported via Deref? No re-exported struct impl)
        // Rust structs have inherent methods. inner.rs defines them.
        UringDriver::wait(self)?;
        Ok(())
    }

    fn process_completions(&mut self) {
        self.process_completions_internal();
        self.flush_cancellations();
        self.flush_backlog();
    }

    fn cancel_op(&mut self, user_data: usize) {
        self.cancel_op_internal(user_data);
    }

    fn register_buffer_regions(
        &mut self,
        regions: &[crate::io::buffer::BufferRegion],
    ) -> io::Result<Vec<usize>> {
        UringDriver::register_buffer_regions(self, regions)
    }

    fn register_files(
        &mut self,
        files: &[crate::io::RawHandle],
    ) -> io::Result<Vec<crate::io::op::IoFd>> {
        let fds: Vec<i32> = files.iter().map(|h| h.fd).collect();
        self.ring.submitter().register_files(&fds)?;

        let mut fixed_fds = Vec::with_capacity(files.len());
        for i in 0..files.len() {
            fixed_fds.push(crate::io::op::IoFd::Fixed(i as u32));
        }
        Ok(fixed_fds)
    }

    fn unregister_files(&mut self, _files: Vec<crate::io::op::IoFd>) -> io::Result<()> {
        self.ring.submitter().unregister_files()
    }

    fn wake(&mut self) -> io::Result<()> {
        let buf = 1u64.to_ne_bytes();
        let ret = unsafe { libc::write(self.waker_fd, buf.as_ptr() as *const _, 8) };
        if ret < 0 {
            let err = io::Error::last_os_error();
            if err.raw_os_error() == Some(libc::EAGAIN) {
                return Ok(());
            }
            return Err(err);
        }
        Ok(())
    }

    fn inner_handle(&self) -> crate::io::RawHandle {
        use std::os::unix::io::AsRawFd;
        crate::io::RawHandle {
            fd: self.ring.as_raw_fd(),
        }
    }

    fn create_waker(&self) -> Arc<dyn RemoteWaker> {
        let new_fd = unsafe { libc::dup(self.waker_fd) };
        if new_fd < 0 {
            panic!("Failed to dup waker fd");
        }
        Arc::new(UringWaker {
            fd: new_fd,
            is_waked: self.is_waked.clone(),
        })
    }

    fn driver_id(&self) -> usize {
        self.waker_fd as usize
    }
}
