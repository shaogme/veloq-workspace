mod ext;
mod inner;
pub mod op;
pub mod rio;
mod submit;
#[cfg(test)]
mod tests;

use crate::driver::op_registry::OpEntry;
use crate::driver::{DetachedCompleter, Driver, RemoteWaker};
use std::io;
use std::task::{Context, Poll};
use tracing::{debug, trace};
use windows_sys::Win32::System::IO::{OVERLAPPED, PostQueuedCompletionStatus};

pub use inner::{IocpDriver, IocpOpState, OpLifecycle};
use op::IocpOp;
use submit::SubmissionResult;

impl Driver for IocpDriver {
    type Op = IocpOp;

    fn reserve_op(&mut self) -> usize {
        let old_pages = self.ops.page_count();
        let user_data = self.ops.insert(OpEntry::new(None, IocpOpState::new()));
        trace!(user_data, "Reserved op slot");

        if self.ops.page_count() > old_pages {
            // New page allocated, register it immediately
            if let Some(rio) = &mut self.rio_state {
                let new_page_idx = self.ops.page_count() - 1;
                rio.ensure_slab_page_registration(new_page_idx, &self.ops);
            }
        }
        user_data
    }

    fn attach_detached_completer(
        &mut self,
        user_data: usize,
        completer: Box<dyn DetachedCompleter<Self::Op>>,
    ) {
        if let Some(op) = self.ops.get_mut(user_data) {
            op.platform_data.detached_completer = Some(completer);
        }
    }

    fn submit(
        &mut self,
        user_data: usize,
        op: Self::Op,
    ) -> Result<Poll<()>, (io::Error, Self::Op)> {
        trace!(user_data, "Submitting op");
        // Since RIO slab registration is handled eagerly in reserve_op (and new),
        // we no longer need to check/register it here.
        // This resolves the borrow checker conflict.

        if let Some(op_entry) = self.ops.get_mut(user_data) {
            // Important: we must pin the op in resources first, then get pointers
            op_entry.resources = Some(op);
            let op_ref = op_entry.resources.as_mut().unwrap();

            op_ref.header.user_data = user_data;

            // Construct SubmitContext utilizing Split Borrow
            let mut ctx = crate::driver::iocp::op::SubmitContext {
                port: self.port,
                overlapped: &mut op_ref.header.inner as *mut OVERLAPPED,
                ext: &self.extensions,
                registered_files: &self.registered_files,
                rio: self.rio_state.as_mut(),
                // ops removed from context
            };

            let result = unsafe { (op_ref.vtable.submit)(op_ref, &mut ctx) };

            match result {
                Ok(SubmissionResult::Pending) => {
                    op_entry.platform_data.lifecycle = OpLifecycle::InFlight;
                }
                Ok(SubmissionResult::PostToQueue) => {
                    // E.g. Wakeup. Post immediately.
                    let _ = unsafe {
                        PostQueuedCompletionStatus(self.port, 0, user_data, std::ptr::null_mut())
                    };
                    // Treat as in-flight; completion comes immediately via CQ
                    op_entry.platform_data.lifecycle = OpLifecycle::InFlight;
                }
                Ok(SubmissionResult::Offload(task)) => {
                    use veloq_blocking::get_blocking_pool;
                    op_entry.platform_data.lifecycle = OpLifecycle::InFlight;
                    if get_blocking_pool().execute(task).is_err() {
                        op_entry.platform_data.lifecycle =
                            OpLifecycle::Completed(Err(io::Error::other("Thread pool overloaded")));
                        if let Some(waker) = op_entry.waker.take() {
                            waker.wake();
                        }
                    }
                }
                Ok(SubmissionResult::Timer(duration)) => {
                    let timeout = self.wheel.insert(user_data, duration);
                    op_entry.platform_data.timer_id = Some(timeout);
                    op_entry.platform_data.lifecycle = OpLifecycle::InFlight;
                }
                Err(e) => {
                    op_entry.platform_data.lifecycle = OpLifecycle::Completed(Err(e));
                    if let Some(waker) = op_entry.waker.take() {
                        waker.wake();
                    }
                }
            }
        }

        let should_complete_detached = if let Some(op) = self.ops.get_mut(user_data) {
            matches!(op.platform_data.lifecycle, OpLifecycle::Completed(_))
                && op.platform_data.detached_completer.is_some()
        } else {
            false
        };

        if should_complete_detached {
            let entry = self.ops.remove(user_data);
            if let OpLifecycle::Completed(result) = entry.platform_data.lifecycle {
                if let Some(completer) = entry.platform_data.detached_completer {
                    if let Some(iocp_op) = entry.resources {
                        completer.complete(result, iocp_op);
                    }
                }
            }
        }

        Ok(Poll::Ready(()))
    }

    fn submit_background(&mut self, op: Self::Op) -> io::Result<()> {
        let user_data = self.reserve_op();

        if let Some(op_entry) = self.ops.get_mut(user_data) {
            op_entry.platform_data.is_background = true;
            op_entry.resources = Some(op);
            let op_ref = op_entry.resources.as_mut().unwrap();
            op_ref.header.user_data = user_data;

            let mut ctx = crate::driver::iocp::op::SubmitContext {
                port: self.port,
                overlapped: &mut op_ref.header.inner as *mut OVERLAPPED,
                ext: &self.extensions,
                registered_files: &self.registered_files,
                rio: self.rio_state.as_mut(),
            };

            let result = unsafe { (op_ref.vtable.submit)(op_ref, &mut ctx) };

            match result {
                Ok(SubmissionResult::Offload(task)) => {
                    op_entry.platform_data.lifecycle = OpLifecycle::InFlight;
                    use veloq_blocking::get_blocking_pool;
                    if get_blocking_pool().execute(task).is_err() {
                        self.ops.remove(user_data);
                        return Err(io::Error::other("Thread pool overloaded"));
                    }
                }
                Ok(_) => {
                    op_entry.platform_data.lifecycle = OpLifecycle::InFlight;
                }
                Err(e) => {
                    debug!(error = ?e, user_data, "Background submit failed");
                    self.ops.remove(user_data);
                    return Err(e);
                }
            }
        }
        Ok(())
    }

    fn poll_op(
        &mut self,
        user_data: usize,
        cx: &mut Context<'_>,
    ) -> Poll<(io::Result<usize>, Self::Op)> {
        trace!(user_data, "IocpDriver::poll_op");
        if let Some(op) = self.ops.get_mut(user_data) {
            match op.platform_data.lifecycle {
                OpLifecycle::Completed(_) => {
                    // We can't move out of match arm if we match on &mut op.platform_data.lifecycle
                    // So we check, then take.
                }
                OpLifecycle::Cancelled => {
                    // If cancelled, usually it implies we are waiting for cancellation to finish or it already finished
                    // with error. If we see Cancelled here, maybe we should return Error?
                    // Similar to Completed(Err(Cancelled))
                    let mut entry = self.ops.remove(user_data);
                    if let Some(res) = entry.resources.take() {
                        return Poll::Ready((
                            Err(io::Error::from_raw_os_error(
                                windows_sys::Win32::Foundation::ERROR_OPERATION_ABORTED as i32,
                            )),
                            res,
                        ));
                    }
                    panic!("Op cancelled but resources missing");
                }
                _ => {
                    op.waker = Some(cx.waker().clone());
                    return Poll::Pending;
                }
            }
        } else {
            panic!("Op not found in registry");
        }

        // If we refer to op above, we can't remove it. separate scope.
        let mut entry = self.ops.remove(user_data);
        if let OpLifecycle::Completed(res) = entry.platform_data.lifecycle {
            let resources = entry
                .resources
                .take()
                .expect("Op completed but resources missing");
            Poll::Ready((res, resources))
        } else {
            // Should be unreachable due to check above
            panic!("Inconsistent state in poll_op");
        }
    }

    fn submit_queue(&mut self) -> io::Result<()> {
        Ok(())
    }

    fn wait(&mut self) -> io::Result<()> {
        self.get_completion(u32::MAX)
    }

    fn process_completions(&mut self) {
        let _ = self.get_completion(0);
    }

    fn cancel_op(&mut self, user_data: usize) {
        self.cancel_op_internal(user_data);
    }

    fn register_buffer_regions(
        &mut self,
        regions: &[veloq_buf::buffer::BufferRegion],
    ) -> io::Result<Vec<usize>> {
        IocpDriver::register_buffer_regions(self, regions)
    }

    fn register_files(&mut self, files: &[crate::RawHandle]) -> io::Result<Vec<crate::op::IoFd>> {
        IocpDriver::register_files(self, files)
    }

    fn unregister_files(&mut self, files: Vec<crate::op::IoFd>) -> io::Result<()> {
        IocpDriver::unregister_files(self, files)
    }

    fn wake(&mut self) -> io::Result<()> {
        IocpDriver::wake(self)
    }

    fn inner_handle(&self) -> crate::RawHandle {
        self.port.into()
    }

    fn create_waker(&self) -> std::sync::Arc<dyn RemoteWaker> {
        IocpDriver::create_waker(self)
    }

    fn driver_id(&self) -> usize {
        0
    }
}
