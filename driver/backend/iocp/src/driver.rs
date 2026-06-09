mod cancellation;
mod completion;
mod lifecycle;
mod polling;
mod registration;
mod submission;

pub(crate) const RIO_EVENT_KEY: usize =
    veloq_driver_core::driver::CompletionToken::rio_wake(0).raw() as usize;
pub(crate) type PreInit = crate::win32::IoCompletionPort;

use std::sync::{Arc, mpsc};
use std::time::Duration;

use tracing::trace;

use veloq_driver_core::DriverResult as CoreDriverResult;
use veloq_driver_core::driver::registry::OpEntry;
use veloq_driver_core::driver::{
    CancelRequest, CancelSubmitOutcome, CompletionSidecar as CoreCompletionSidecar, DriveMode,
    DriveOutcome, Driver, DriverCompletionDiagnostics, DriverSubmitResult, OpToken, RegisterFd,
    RemoteCancelSender, RemoteWaker, SharedCompletionTable, SharedDriverSlotTable, SubmitStatus,
};

use diagweave::prelude::*;

use crate::config::{IoFd, IocpHandle};
use crate::error::IocpError;
use crate::op::{IocpOp, IocpOpPayload, IocpOpRegistry, IocpSlotSpec, IocpUserPayload};

pub(crate) type IocpDriverResult<T> = CoreDriverResult<T, IocpError>;
pub use crate::op::IocpOpState;

// ============================================================================
// State & Lifecycle Types
// ============================================================================

/// The IOCP driver implementation that manages I/O completion ports and operations.
pub struct IocpDriver<'a> {
    completion: polling::CompletionPump,
    ops: IocpOpRegistry,
    extensions: crate::ext::Extensions,
    timer: polling::TimerEngine,
    handles: registration::HandleRegistry,
    remote_cancel_sender: RemoteCancelSender,
    remote_cancel_receiver: mpsc::Receiver<CancelRequest>,
    completion_diagnostics: DriverCompletionDiagnostics,

    // RIO Support (required)
    rio: lifecycle::IocpRioRuntime<'a>,
    shutting_down: bool,
    closed: bool,

    // Rust drops fields in declaration order; keep this last so WSACleanup runs
    // after socket/RIO-backed state has been torn down.
    _winsock: lifecycle::WinsockGuard,
}

/// Closing mode for the driver or operations.
#[derive(Clone, Copy, Debug)]
pub enum CloseMode {
    /// Closes quickly without waiting for pending operations.
    Fast,
    /// Closes after a specified timeout, allowing pending operations to finish.
    Strict { timeout: Duration },
}

pub(crate) type CompletionSidecar = CoreCompletionSidecar<IocpUserPayload, IocpError>;

impl<'a> IocpDriver<'a> {
    /// Checks if the provided operation is a RIO-based operation.
    pub(crate) fn is_rio_op(op: &IocpOp) -> bool {
        matches!(
            op.payload,
            IocpOpPayload::Recv(_)
                | IocpOpPayload::Send(_)
                | IocpOpPayload::UdpRecv(_)
                | IocpOpPayload::UdpSend(_)
                | IocpOpPayload::SendTo(_)
                | IocpOpPayload::UdpRecvFrom(_)
        )
    }

    pub(crate) fn has_active_ops_internal(&mut self) -> bool {
        self.ops.has_active_ops()
    }

    pub(crate) fn create_waker(&self) -> Arc<dyn RemoteWaker<IocpError>> {
        self.completion.create_waker()
    }

    #[cfg(test)]
    pub(crate) fn debug_registered_file(
        &self,
        idx: usize,
    ) -> Option<&crate::config::RegisteredHandle> {
        self.handles.registered_file(idx)
    }

    #[cfg(test)]
    pub(crate) fn debug_remote_free_contains(&self, needle: usize) -> bool {
        use std::sync::atomic::Ordering;
        use veloq_driver_core::slot::SlotTable;

        let mut cur = self.ops.shared.remote_free_head.load(Ordering::Acquire);
        while cur != SlotTable::<crate::op::IocpSlotSpec>::NULL_INDEX {
            if cur == needle {
                return true;
            }
            cur = self.ops.shared.slots[cur].next_free.load(Ordering::Relaxed);
        }
        false
    }
}

impl<'a> Driver for IocpDriver<'a> {
    type Op = crate::op::IocpOp;
    type UP = IocpUserPayload;
    type Raw = IocpHandle;
    type Sidecar = crate::op::OverlappedEntry;
    type Completion = usize;
    type Error = IocpError;
    type SlotSpec = IocpSlotSpec;

    fn reserve_op_raw(&mut self) -> IocpDriverResult<OpToken> {
        let (user_data, generation) = match self.ops.insert(OpEntry::new(IocpOpState::default())) {
            Ok(handle) => (handle.index, handle.generation),
            Err(_) => {
                return Err(IocpError::Registration.report("iocp/driver", "OpRegistry is full"));
            }
        };
        trace!(user_data, generation, "Reserved op slot");
        Ok(OpToken::new(user_data, generation))
    }

    fn slot_table(&self) -> SharedDriverSlotTable<Self> {
        self.ops.shared.clone()
    }

    fn remote_cancel_sender(&self) -> RemoteCancelSender {
        self.remote_cancel_sender.clone()
    }

    fn try_recv_remote_cancel_request(&mut self) -> Option<CancelRequest> {
        self.remote_cancel_receiver.try_recv().ok()
    }

    fn slot_set_payload_raw(&mut self, token: OpToken, payload: Self::UP) {
        let _ = self
            .ops
            .with_slot_storage_mut(token, |_result, payload_cell, _sidecar| {
                *payload_cell = Some(payload);
            });
    }

    fn slot_take_payload_raw(&mut self, token: OpToken) -> Option<Self::UP> {
        self.ops
            .with_slot_storage_mut(token, |_result, payload_cell, _sidecar| payload_cell.take())
            .flatten()
    }

    fn release_op_slot_raw(&mut self, token: OpToken) {
        let _ = self.ops.recycle(token, token.generation().wrapping_add(1));
    }

    fn submit_op_raw(
        &mut self,
        token: OpToken,
        op_in: &mut Option<Self::Op>,
    ) -> DriverSubmitResult<Self::Error> {
        if self.shutting_down {
            return DriverSubmitResult::failed(
                IocpError::Internal
                    .to_report()
                    .push_ctx("scope", "iocp/driver")
                    .set_error_code(windows_sys::Win32::Foundation::ERROR_OPERATION_ABORTED as i32)
                    .attach_note("driver is shutting down"),
                SubmitStatus::Void,
            );
        }
        let op = match op_in.take() {
            Some(op) => op,
            None => {
                return DriverSubmitResult::failed(
                    IocpError::InvalidInput
                        .report("iocp/driver", "submit called with empty option"),
                    SubmitStatus::Void,
                );
            }
        };

        let result = match self.call_op_submit(token, op) {
            Ok(res) => res,
            Err(e) => {
                return DriverSubmitResult::failed(
                    e.push_ctx("scope", "iocp/driver")
                        .attach_note("call_op_submit failed"),
                    SubmitStatus::Void,
                );
            }
        };

        let completion = &self.completion;
        let timer = &mut self.timer;
        let diagnostics = &mut self.completion_diagnostics;
        let ctx = submission::SubmitContextInternal::new(
            completion.port_arc(),
            timer.wheel_mut(),
            completion.table(),
            diagnostics,
        );

        Self::on_submit_res(&mut self.ops, ctx, result, token, op_in)
    }

    fn drive(&mut self, mode: DriveMode) -> IocpDriverResult<DriveOutcome> {
        match mode {
            DriveMode::Poll => {
                self.get_completion(0)
                    .push_ctx("scope", "iocp/driver.drive.poll")
                    .attach_note("drive(Poll) failed")?;
            }
            DriveMode::Wait => {
                let pending_progress =
                    self.has_active_ops_internal() || self.ops.shared.has_ready_completion();
                if !pending_progress {
                    return Ok(DriveOutcome {
                        next_timeout_hint: self.timer.next_timeout(),
                        pending_progress,
                    });
                }
                self.get_completion(u32::MAX)
                    .push_ctx("scope", "iocp/driver.drive.wait")
                    .attach_note("wait for completion failed")?;
            }
        }

        let pending_progress =
            self.has_active_ops_internal() || self.ops.shared.has_ready_completion();
        Ok(DriveOutcome {
            next_timeout_hint: self.timer.next_timeout(),
            pending_progress,
        })
    }

    fn completion_table(&self) -> SharedCompletionTable<Self::UP, Self::Error, Self::Completion> {
        self.completion.completion_table()
    }

    fn cancel_op(&mut self, request: CancelRequest) -> IocpDriverResult<CancelSubmitOutcome> {
        self.cancel_op_internal(request)
    }

    fn register_chunk(
        &mut self,
        id: veloq_buf::heap::ChunkId,
        ptr: *const u8,
        len: usize,
    ) -> IocpDriverResult<()> {
        IocpDriver::register_chunk(self, id, ptr, len)
            .push_ctx("scope", "iocp/driver")
            .attach_note("register chunk failed")
    }

    fn register_files<'f>(
        &mut self,
        files: Vec<RegisterFd<'f, IocpHandle>>,
    ) -> IocpDriverResult<Vec<IoFd>> {
        IocpDriver::register_files(self, files)
    }

    fn unregister_files(&mut self, files: Vec<IoFd>) -> IocpDriverResult<()> {
        IocpDriver::unregister_files(self, files)
    }

    fn create_waker(&self) -> Arc<dyn RemoteWaker<IocpError>> {
        IocpDriver::create_waker(self)
    }
}

#[cfg(feature = "test-hooks")]
impl veloq_driver_core::driver::test_hooks::DriverTestHooks for IocpDriver<'_> {
    fn debug_chunk_register_attempts(&self) -> u64 {
        self.rio
            .state()
            .registry
            .registration_stats
            .chunk_register_attempts
    }

    fn debug_completion_diagnostics(
        &self,
    ) -> veloq_driver_core::driver::DriverCompletionDiagnosticsSnapshot {
        self.completion_diagnostics.snapshot()
    }
}
