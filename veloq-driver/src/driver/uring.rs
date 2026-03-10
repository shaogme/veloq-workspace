use crate::driver::op_registry::OpEntry;
use std::io;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::task::{Context, Poll};
use tracing::{debug, trace};

mod inner;
pub mod op;
pub mod submit;

pub use inner::{UringDriver, UringOpState};

use crate::driver::slot::STATE_COMPLETED;
use crate::driver::{Driver, Outcome, PollBinder, RemoteWaker, SubmitBinder};
use inner::UringWaker;
use op::UringOp;

impl UringDriver {
    fn submit_sqe(
        &mut self,
        user_data: usize,
        op: <Self as Driver>::Op,
        op_in: &mut Option<<Self as Driver>::Op>,
        binder: SubmitBinder,
    ) -> Outcome<io::Result<Poll<()>>> {
        // 1. Store resources FIRST in the Slot
        {
            let slot = &self.ops.shared.slots[user_data];
            unsafe {
                *slot.op.get() = Some(op);
            }
        }

        // 2. Try submit using centralized logic
        match self.submit_from_slot(user_data) {
            Ok(true) => binder.ok(Poll::Ready(())),
            Ok(false) => {
                debug!(user_data, "SQ full, pushing to backlog");
                if let Some(entry) = self.ops.get_mut(user_data) {
                    entry.platform_data.lifecycle = inner::OpLifecycle::Pending;
                }
                // Op remains in Slot, waiting for flush_backlog to pick it up
                self.push_backlog(user_data);
                binder.ok(Poll::Pending)
            }
            Err(e) => {
                // Recover op
                let slot = &self.ops.shared.slots[user_data];
                let op = unsafe { (*slot.op.get()).take().unwrap() };
                *op_in = Some(op);
                binder.err(e)
            }
        }
    }

    fn submit_timer(
        &mut self,
        user_data: usize,
        op: <Self as Driver>::Op,
        op_in: &mut Option<<Self as Driver>::Op>,
        binder: SubmitBinder,
    ) -> Outcome<io::Result<Poll<()>>> {
        // 1. Store resources FIRST
        {
            let slot = &self.ops.shared.slots[user_data];
            unsafe {
                *slot.op.get() = Some(op);
            }
        }

        // 2. Try submit
        match self.submit_from_slot(user_data) {
            Ok(true) => binder.ok(Poll::Ready(())),
            Ok(false) => {
                // Should technically not happen for SoftwareTimer unless we change logic later
                debug!(
                    user_data,
                    "SQ full (unexpected for timer), pushing to backlog"
                );
                if let Some(entry) = self.ops.get_mut(user_data) {
                    entry.platform_data.lifecycle = inner::OpLifecycle::Pending;
                }
                self.push_backlog(user_data);
                binder.ok(Poll::Pending)
            }
            Err(e) => {
                // Recover op
                let slot = &self.ops.shared.slots[user_data];
                let op = unsafe { (*slot.op.get()).take().unwrap() };
                *op_in = Some(op);
                binder.err(e)
            }
        }
    }
}

impl Driver for UringDriver {
    type Op = UringOp;

    fn reserve_op(&mut self) -> io::Result<(usize, u32)> {
        // Only one arg needed now
        match self.ops.insert(OpEntry::new(UringOpState::new())) {
            Ok(crate::driver::op_registry::OpHandle {
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

    fn slot_table(&self) -> std::sync::Arc<crate::driver::slot::SlotTable<Self::Op>> {
        self.ops.shared.clone()
    }

    fn submit(
        &mut self,
        user_data: usize,
        op_in: &mut Option<Self::Op>,
        binder: SubmitBinder,
    ) -> Outcome<io::Result<Poll<()>>> {
        let op = op_in.take().expect("submit called with empty Option");
        let strategy = unsafe { op.vtable.as_ref().strategy };
        if strategy == op::SubmissionStrategy::BackgroundOnly {
            *op_in = Some(op);
            return binder.err(io::Error::new(
                io::ErrorKind::Unsupported,
                "background op cannot be submitted normally",
            ));
        }

        match strategy {
            op::SubmissionStrategy::BackgroundOnly => unreachable!(),
            op::SubmissionStrategy::SubmitSqe => self.submit_sqe(user_data, op, op_in, binder),
            op::SubmissionStrategy::SoftwareTimer => {
                self.submit_timer(user_data, op, op_in, binder)
            }
        }
    }

    fn submit_background(&mut self, mut op: Self::Op) -> io::Result<()> {
        let strategy = unsafe { op.vtable.as_ref().strategy };
        if strategy == op::SubmissionStrategy::BackgroundOnly {
            let sqe = unsafe {
                (op.vtable.as_ref().make_sqe)(&mut op, self).user_data(inner::BACKGROUND_USER_DATA)
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
        binder: PollBinder,
    ) -> Outcome<Poll<io::Result<usize>>> {
        // 1. Check if we need to flush pending op
        let is_pending = if let Some(entry) = self.ops.get(user_data) {
            matches!(entry.platform_data.lifecycle, inner::OpLifecycle::Pending)
        } else {
            // If op missing, it might be already removed? Or invalid.
            panic!("Op not found in registry during poll");
        };

        if is_pending {
            self.flush_backlog();
            self.flush_cancellations();
        }

        // Block to limit slot borrow
        let state = {
            let slot = &self.ops.shared.slots[user_data];
            // 2. Register Waker
            slot.waker.register(cx.waker());
            // 3. Check for completion state
            slot.state.load(Ordering::Acquire)
        };

        if state == STATE_COMPLETED {
            // Completed. Extract result and drop kernel op.
            let res = {
                let slot = &self.ops.shared.slots[user_data];
                let res = unsafe {
                    (*slot.result.get())
                        .take()
                        .expect("Result missing in COMPLETED slot")
                };
                let _ = unsafe { (*slot.op.get()).take() };
                // Mark slot as consumed?
                use crate::driver::slot::STATE_CONSUMED;
                slot.state.store(STATE_CONSUMED, Ordering::Release);
                res
            };

            // Cleanup registry
            self.ops.remove(user_data);

            binder.ready(res)
        } else {
            binder.pending()
        }
    }

    fn submit_queue(&mut self) -> io::Result<()> {
        self.flush_cancellations();
        self.flush_backlog();
        self.submit_to_kernel()
    }

    fn wait(&mut self) -> io::Result<()> {
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

    fn register_chunk(&mut self, id: u16, ptr: *const u8, len: usize) -> io::Result<()> {
        UringDriver::register_chunk(self, id, ptr, len)
    }

    fn register_files(&mut self, files: &[crate::RawHandle]) -> io::Result<Vec<crate::op::IoFd>> {
        let fds: Vec<i32> = files.iter().map(|h| h.fd).collect();
        self.ring.submitter().register_files(&fds)?;

        let mut fixed_fds = Vec::with_capacity(files.len());
        for i in 0..files.len() {
            fixed_fds.push(crate::op::IoFd::Fixed(i as u32));
        }
        Ok(fixed_fds)
    }

    fn unregister_files(&mut self, _files: Vec<crate::op::IoFd>) -> io::Result<()> {
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

    fn inner_handle(&self) -> crate::RawHandle {
        use std::os::unix::io::AsRawFd;
        crate::RawHandle {
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
