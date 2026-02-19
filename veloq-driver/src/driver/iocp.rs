mod ext;
mod inner;
pub mod op;
pub mod rio;
mod submit;
#[cfg(test)]
mod tests;

use crate::driver::op_registry::OpEntry;
use crate::driver::slot::{STATE_COMPLETED, STATE_CONSUMED, STATE_SUBMITTED};
use crate::driver::{Driver, RemoteWaker};
use std::io;
use std::sync::atomic::Ordering;
use std::task::{Context, Poll};
use tracing::{debug, trace};
use windows_sys::Win32::System::IO::PostQueuedCompletionStatus;

pub use inner::{IocpDriver, IocpOpState, OpLifecycle};
use op::IocpOp;
use submit::SubmissionResult;

impl Driver for IocpDriver {
    type Op = IocpOp;

    fn reserve_op(&mut self) -> io::Result<(usize, u32)> {
        // OpRegistry::alloc handles internal vectors and free list management autonomously.

        let old_pages = self.ops.page_count();
        let (user_data, generation) = match self.ops.insert(OpEntry::new(IocpOpState::new())) {
            Ok(handle) => (handle.index, handle.generation),
            Err(_) => {
                return Err(io::Error::new(
                    io::ErrorKind::OutOfMemory,
                    "OpRegistry is full",
                ));
            }
        };
        trace!(user_data, generation, "Reserved op slot");

        if self.ops.page_count() > old_pages {
            // New page allocated, register it immediately
            if let Some(rio) = &mut self.rio_state {
                let new_page_idx = self.ops.page_count() - 1;
                rio.ensure_slab_page_registration(new_page_idx, &self.ops);
            }
        }
        Ok((user_data, generation))
    }

    fn slot_table(&self) -> std::sync::Arc<crate::driver::slot::SlotTable<Self::Op>> {
        self.ops.shared.clone()
    }

    fn submit(
        &mut self,
        user_data: usize,
        op: Self::Op,
    ) -> Result<Poll<()>, (io::Error, Self::Op)> {
        trace!(user_data, "Submitting op");

        let slots_per_page = self.ops.local.len();

        // Scope for initial submission
        {
            let (slot, op_entry) = match self.ops.get_slot_and_entry_mut(user_data) {
                Some(pair) => pair,
                None => panic!("Op not found"),
            };

            unsafe { *slot.op.get() = Some(op) };
            slot.state.store(STATE_SUBMITTED, Ordering::Release);

            let op_ref = unsafe { (*slot.op.get()).as_mut().unwrap() };
            op_ref.header.user_data = user_data;

            // Use the overlapped pointer from the slot.
            // This is safe because:
            // 1. The Slot is pinned in memory (part of Arc<SlotTable>).
            // 2. OverlappedEntry is #[repr(C)], so we can recover the user_data from the pointer.
            let overlapped_ptr = slot.overlapped_ptr();

            let mut ctx = crate::driver::iocp::op::SubmitContext {
                port: self.port.handle,
                overlapped: overlapped_ptr,
                ext: &self.extensions,
                registered_files: &self.registered_files,
                rio: self.rio_state.as_mut(),
                slots_per_page,
            };

            let result = unsafe { (op_ref.vtable.as_ref().submit)(op_ref, &mut ctx) };

            match result {
                Ok(SubmissionResult::Pending) => {
                    op_entry.platform_data.lifecycle = OpLifecycle::InFlight;
                }
                Ok(SubmissionResult::PostToQueue) => {
                    let _ = unsafe {
                        PostQueuedCompletionStatus(
                            self.port.handle,
                            0,
                            user_data,
                            std::ptr::null_mut(),
                        )
                    };
                    op_entry.platform_data.lifecycle = OpLifecycle::InFlight;
                }
                Ok(SubmissionResult::Offload(task)) => {
                    use veloq_blocking::get_blocking_pool;
                    op_entry.platform_data.lifecycle = OpLifecycle::InFlight;
                    if get_blocking_pool().execute(task).is_err() {
                        op_entry.platform_data.lifecycle =
                            OpLifecycle::Completed(Err(io::Error::other("Thread pool overloaded")));
                        // Update Slot State
                        unsafe {
                            *slot.result.get() =
                                Some(Err(io::Error::other("Thread pool overloaded")))
                        };
                        slot.state.store(STATE_COMPLETED, Ordering::Release);
                        slot.waker.wake();
                    }
                }
                Ok(SubmissionResult::Timer(duration)) => {
                    let timeout = self.wheel.insert(user_data, duration);
                    op_entry.platform_data.timer_id = Some(timeout);
                    op_entry.platform_data.lifecycle = OpLifecycle::InFlight;
                }
                Err(e) => {
                    op_entry.platform_data.lifecycle =
                        OpLifecycle::Completed(Err(io::Error::other("Submit Error")));
                    let op = unsafe { (*slot.op.get()).take().unwrap() };
                    slot.state
                        .store(crate::driver::slot::STATE_EMPTY, Ordering::Release);
                    return Err((e, op));
                }
            }
        } // End of submission scope

        // Logic for detached completer
        // Access self.ops directly
        let should_complete_detached = if let Some(op) = self.ops.get(user_data) {
            matches!(op.platform_data.lifecycle, OpLifecycle::Completed(_))
                && op.platform_data.detached_completer.is_some()
        } else {
            false
        };

        if should_complete_detached {
            if let Some((slot, entry)) = self.ops.get_slot_and_entry_mut(user_data) {
                if let OpLifecycle::Completed(result) = &entry.platform_data.lifecycle {
                    let res_copy = result.as_ref().map(|x| *x).map_err(|e| {
                        if let Some(code) = e.raw_os_error() {
                            io::Error::from_raw_os_error(code)
                        } else {
                            io::Error::new(e.kind(), e.to_string())
                        }
                    });

                    if let Some(completer) = entry.platform_data.detached_completer.take() {
                        if let Some(iocp_op) = unsafe { (*slot.op.get()).take() } {
                            completer.complete(res_copy, iocp_op);
                        }
                    }
                }
                let _ = std::mem::replace(&mut entry.platform_data, IocpOpState::default());
                self.ops.free_indices.push(user_data);
            }
        }

        Ok(Poll::Ready(()))
    }

    fn submit_background(&mut self, op: Self::Op) -> io::Result<()> {
        let (user_data, _) = self.reserve_op()?;

        let slots_per_page = self.ops.local.len();
        let (slot, op_entry) = match self.ops.get_slot_and_entry_mut(user_data) {
            Some(pair) => pair,
            None => panic!("Op not found after reserve"),
        };

        unsafe { *slot.op.get() = Some(op) };
        slot.state.store(STATE_SUBMITTED, Ordering::Release);

        let op_ref = unsafe { (*slot.op.get()).as_mut().unwrap() };
        op_ref.header.user_data = user_data;

        op_entry.platform_data.is_background = true;
        let overlapped_ptr = slot.overlapped_ptr();

        let mut ctx = crate::driver::iocp::op::SubmitContext {
            port: self.port.handle,
            overlapped: overlapped_ptr,
            ext: &self.extensions,
            registered_files: &self.registered_files,
            rio: self.rio_state.as_mut(),
            slots_per_page,
        };

        let result = unsafe { (op_ref.vtable.as_ref().submit)(op_ref, &mut ctx) };

        match result {
            Ok(SubmissionResult::Offload(task)) => {
                op_entry.platform_data.lifecycle = OpLifecycle::InFlight;
                use veloq_blocking::get_blocking_pool;
                if get_blocking_pool().execute(task).is_err() {
                    let _ = std::mem::replace(&mut op_entry.platform_data, IocpOpState::default());
                    self.ops.free_indices.push(user_data);
                    return Err(io::Error::other("Thread pool overloaded"));
                }
            }
            Ok(_) => {
                op_entry.platform_data.lifecycle = OpLifecycle::InFlight;
            }
            Err(e) => {
                debug!(error = ?e, user_data, "Background submit failed");
                let _ = std::mem::replace(&mut op_entry.platform_data, IocpOpState::default());
                self.ops.free_indices.push(user_data);
                return Err(e);
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
        // Check Slot Logic first
        let slot = &self.ops.shared.slots[user_data];

        // No local registry access needed for pure poll check if we trust slot state?
        // But we need to remove from registry on completion.

        let state = slot.state.load(Ordering::Acquire);

        if state == STATE_COMPLETED {
            let res = unsafe { (*slot.result.get()).take().expect("Result missing") };
            let op = unsafe { (*slot.op.get()).take().expect("Op missing") };

            slot.state.store(STATE_CONSUMED, Ordering::Release);
            // Remove from local registry
            self.ops.remove(user_data);

            return Poll::Ready((res, op));
        }

        // Register waker if Pending
        slot.waker.register(cx.waker());

        // Double check state
        let state = slot.state.load(Ordering::Acquire);
        if state == STATE_COMPLETED {
            let res = unsafe { (*slot.result.get()).take().expect("Result missing") };
            let op = unsafe { (*slot.op.get()).take().expect("Op missing") };
            slot.state.store(STATE_CONSUMED, Ordering::Release);
            self.ops.remove(user_data);
            return Poll::Ready((res, op));
        }

        Poll::Pending
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

    fn register_chunk(&mut self, id: u16, ptr: *const u8, len: usize) -> io::Result<()> {
        IocpDriver::register_chunk(self, id, ptr, len)
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
        crate::RawHandle {
            handle: self.port.handle as _,
        }
    }

    fn create_waker(&self) -> std::sync::Arc<dyn RemoteWaker> {
        IocpDriver::create_waker(self)
    }

    fn driver_id(&self) -> usize {
        0
    }
}
