mod error;
mod ext;
mod inner;
pub mod op;
pub mod rio;
mod submit;
#[cfg(test)]
mod tests;

use crate::driver::op_registry::OpEntry;
use crate::driver::slot::STATE_SUBMITTED;
use crate::driver::{
    Driver, Outcome, RemoteWaker, SharedCompletionQueue, SharedCompletionTable, SubmitBinder,
};
use std::io;
use std::sync::atomic::Ordering;
use std::task::Poll;
use tracing::{debug, trace};
use windows_sys::Win32::System::IO::PostQueuedCompletionStatus;

use inner::WAKEUP_USER_DATA;
pub use inner::{CloseMode, IocpDriver, IocpOpState, OpLifecycle};
use op::IocpOp;
use submit::SubmissionResult;

pub(super) struct CompletionSidecar {
    user_data: usize,
    generation: u32,
    res: i32,
    flags: u32,
    payload: Option<crate::driver::slot::ErasedPayload>,
    detail: Option<io::Result<usize>>,
}

impl Driver for IocpDriver {
    type Op = IocpOp;

    fn reserve_op(&mut self) -> io::Result<(usize, u32)> {
        // OpRegistry::alloc handles internal vectors and free list management autonomously.
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
        Ok((user_data, generation))
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
        if self.shutting_down {
            return binder.err(io::Error::from_raw_os_error(
                windows_sys::Win32::Foundation::ERROR_OPERATION_ABORTED as i32,
            ));
        }
        trace!(user_data, "Submitting op");

        let slots_per_page = self.ops.local.len();
        // On Windows, the slab is currently a single contiguous block (page 0).
        let (slab_ptr, slab_len) = self.ops.get_page_slice(0).unwrap();
        let slab_resolver = move |idx| {
            if idx == 0 {
                Some((slab_ptr, slab_len))
            } else {
                None
            }
        };
        let mut deferred_event: Option<CompletionSidecar> = None;

        // Scope for initial submission
        {
            let (slot, op_entry) = match self.ops.get_slot_and_entry_mut(user_data) {
                Some(pair) => pair,
                None => panic!("Op not found"),
            };

            let op = op_in.take().expect("submit called with empty Option");
            unsafe { *slot.op.get() = Some(op) };
            slot.state.store(STATE_SUBMITTED, Ordering::Release);

            let op_ref = unsafe { (*slot.op.get()).as_mut().unwrap() };
            op_ref.header.user_data = user_data;
            let generation = slot.generation.load(Ordering::Acquire);
            op_ref.header.generation = generation;
            op_entry.platform_data.generation = generation;

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
                registrar: self.registrar.as_ref(),
                rio: &mut self.rio_state,
                slots_per_page,
                slab_resolver: &slab_resolver,
            };

            let result = unsafe { (op_ref.vtable.as_ref().submit)(op_ref, &mut ctx) };
            let is_rio_pool_waiting = unsafe {
                std::ptr::eq(
                    op_ref.vtable.as_ref().submit as *const (),
                    crate::driver::iocp::submit::submit_udp_recv_stream as *const (),
                )
            };

            match result {
                Ok(SubmissionResult::Pending) => {
                    op_entry.platform_data.lifecycle = OpLifecycle::InFlight;
                    op_entry.platform_data.rio_pool_waiting = is_rio_pool_waiting;
                }
                Ok(SubmissionResult::PostToQueue) => {
                    let posted = unsafe {
                        PostQueuedCompletionStatus(
                            self.port.handle,
                            0,
                            user_data,
                            std::ptr::null_mut(),
                        )
                    };
                    if posted == 0 {
                        let op = unsafe { (*slot.op.get()).take().unwrap() };
                        *op_in = Some(op);
                        slot.state
                            .store(crate::driver::slot::STATE_EMPTY, Ordering::Release);
                        self.ops.remove(user_data);
                        return binder.err(io::Error::last_os_error());
                    } else {
                        op_entry.platform_data.lifecycle = OpLifecycle::InFlight;
                        op_entry.platform_data.rio_pool_waiting = false;
                    }
                }
                Ok(SubmissionResult::Offload(task)) => {
                    use veloq_blocking::get_blocking_pool;
                    op_entry.platform_data.lifecycle = OpLifecycle::InFlight;
                    op_entry.platform_data.rio_pool_waiting = false;
                    if get_blocking_pool().execute(task).is_err() {
                        let err = io::Error::other("Thread pool overloaded");
                        unsafe {
                            *slot.result.get() =
                                Some(Err(io::Error::new(err.kind(), err.to_string())));
                        }
                        op_entry.platform_data.lifecycle = OpLifecycle::Completed(Err(
                            io::Error::new(err.kind(), err.to_string()),
                        ));
                        let generation = slot.generation.load(Ordering::Acquire);
                        let _ = unsafe { (*slot.op.get()).take() };
                        let payload = unsafe { (*slot.payload.get()).take() };
                        let detail = unsafe { (*slot.result.get()).take() };
                        deferred_event = Some(CompletionSidecar {
                            user_data,
                            generation,
                            res: -err.raw_os_error().unwrap_or(1).abs(),
                            flags: 0,
                            payload,
                            detail,
                        });
                    }
                }
                Ok(SubmissionResult::Timer(duration)) => {
                    let timeout = self.wheel.insert(user_data, duration);
                    op_entry.platform_data.timer_id = Some(timeout);
                    op_entry.platform_data.timer_deadline =
                        Some(std::time::Instant::now() + duration);
                    op_entry.platform_data.lifecycle = OpLifecycle::InFlight;
                    op_entry.platform_data.rio_pool_waiting = false;
                }
                Err(e) => {
                    let op = unsafe { (*slot.op.get()).take().unwrap() };
                    *op_in = Some(op);
                    slot.state
                        .store(crate::driver::slot::STATE_EMPTY, Ordering::Release);
                    self.ops.remove(user_data);
                    return binder.err(e);
                }
            }
        } // End of submission scope

        if let Some(deferred) = deferred_event {
            self.push_completion_event(
                deferred.user_data,
                deferred.generation,
                deferred.res,
                deferred.flags,
                deferred.payload,
                deferred.detail,
            );
            self.ops.remove(deferred.user_data);
        }
        binder.ok(Poll::Ready(()))
    }

    fn submit_background(&mut self, op: Self::Op) -> io::Result<()> {
        if self.shutting_down {
            return Err(io::Error::from_raw_os_error(
                windows_sys::Win32::Foundation::ERROR_OPERATION_ABORTED as i32,
            ));
        }
        let (user_data, _) = self.reserve_op()?;
        let mut submit_error: Option<io::Error> = None;

        let slots_per_page = self.ops.local.len();
        // Pre-fetch slab info to avoid borrow conflicts
        let (slab_ptr, slab_len) = self.ops.get_page_slice(0).unwrap();
        let slab_resolver = move |idx| {
            if idx == 0 {
                Some((slab_ptr, slab_len))
            } else {
                None
            }
        };

        let (slot, op_entry) = match self.ops.get_slot_and_entry_mut(user_data) {
            Some(pair) => pair,
            None => panic!("Op not found after reserve"),
        };

        unsafe { *slot.op.get() = Some(op) };
        slot.state.store(STATE_SUBMITTED, Ordering::Release);

        let op_ref = unsafe { (*slot.op.get()).as_mut().unwrap() };
        op_ref.header.user_data = user_data;
        let generation = slot.generation.load(Ordering::Acquire);
        op_ref.header.generation = generation;
        op_entry.platform_data.generation = generation;

        op_entry.platform_data.is_background = true;
        let overlapped_ptr = slot.overlapped_ptr();

        let mut ctx = crate::driver::iocp::op::SubmitContext {
            port: self.port.handle,
            overlapped: overlapped_ptr,
            ext: &self.extensions,
            registered_files: &self.registered_files,
            registrar: self.registrar.as_ref(),
            rio: &mut self.rio_state,
            slots_per_page,
            slab_resolver: &slab_resolver,
        };

        let result = unsafe { (op_ref.vtable.as_ref().submit)(op_ref, &mut ctx) };

        match result {
            Ok(SubmissionResult::Offload(task)) => {
                op_entry.platform_data.lifecycle = OpLifecycle::InFlight;
                use veloq_blocking::get_blocking_pool;
                if get_blocking_pool().execute(task).is_err() {
                    let _ = std::mem::take(&mut op_entry.platform_data);
                    self.ops.shared.push_free(user_data);
                    return Err(io::Error::other("Thread pool overloaded"));
                }
            }
            Ok(_) => {
                op_entry.platform_data.lifecycle = OpLifecycle::InFlight;
            }
            Err(e) => {
                debug!(error = ?e, user_data, "Background submit failed");
                let _ = unsafe { (*slot.op.get()).take() };
                slot.state
                    .store(crate::driver::slot::STATE_EMPTY, Ordering::Release);
                submit_error = Some(e);
            }
        }

        if let Some(e) = submit_error {
            self.ops.remove(user_data);
            return Err(e);
        }
        Ok(())
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

    fn completion_queue(&self) -> SharedCompletionQueue {
        self.completion_events.clone()
    }

    fn completion_table(&self) -> SharedCompletionTable {
        self.completion_table.clone()
    }

    fn wait_and_drain_completions(
        &mut self,
        out: &mut Vec<crate::driver::CompletionEvent>,
    ) -> io::Result<usize> {
        self.get_completion(u32::MAX)?;
        Ok(self.drain_completions(out))
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
        if !self.is_waked.swap(true, Ordering::AcqRel) {
            let posted = unsafe {
                PostQueuedCompletionStatus(self.port.handle, 0, WAKEUP_USER_DATA, std::ptr::null())
            };
            if posted == 0 {
                self.is_waked.store(false, Ordering::Release);
                return Err(io::Error::last_os_error());
            }
        }
        Ok(())
    }

    fn inner_handle(&self) -> crate::RawHandle {
        crate::RawHandle {
            handle: self.port.handle as _,
        }
    }

    fn create_waker(&self) -> std::sync::Arc<dyn RemoteWaker> {
        std::sync::Arc::new(crate::driver::iocp::inner::IocpWaker {
            port: self.port.clone(),
            is_waked: self.is_waked.clone(),
        })
    }

    fn driver_id(&self) -> usize {
        self.port.handle as usize
    }

    fn set_registrar(&mut self, registrar: Box<dyn veloq_buf::BufferRegistrar>) {
        self.registrar = registrar;
    }
}
