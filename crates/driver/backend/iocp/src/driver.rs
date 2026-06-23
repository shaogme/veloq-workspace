mod cancellation;
pub(crate) mod completion;
mod lifecycle;
mod polling;
mod registration;
mod submission;

use std::{
    sync::{Arc, mpsc},
    time::Duration,
};
use veloq_blocking::ThreadPool;

use diagweave::prelude::*;

use lifecycle::{IocpRioRuntime, WinsockGuard};
use polling::{CompletionPump, TimerEngine};
use registration::HandleRegistry;
use submission::SubmitContextInternal;

#[cfg(test)]
use crate::RegisteredHandle;
use crate::{
    IocpResult,
    config::{IoFd, IocpHandle},
    diagnostics::IocpCompletionDiagnostics,
    error::IocpError,
    ext::Extensions,
    op::{IocpOp, IocpOpPayload, IocpOpRegistry, IocpSlotSpec, IocpUserPayload},
    win32::IoCompletionPort,
};

use veloq_buf::heap::ChunkId;
use veloq_driver_core::driver::{
    CancelRequest, CancelSubmitOutcome, CompletionToken, DriveMode, DriveOutcome, Driver,
    DriverCompletionDiagnostics, DriverSubmitResult, OpToken, RegisterFd, RemoteCancelSender,
    RemoteWaker, SharedCompletionTable, SharedDriverSlotTable, SubmitStatus, registry::OpEntry,
};

use windows_sys::Win32::Foundation::ERROR_OPERATION_ABORTED;

#[cfg(feature = "test-hooks")]
use veloq_driver_core::driver::test_hooks::DriverTestHooks;

pub(crate) const RIO_EVENT_TOKEN: CompletionToken = match CompletionToken::encode_control(3, 0) {
    Ok(t) => t,
    Err(_) => panic!("Failed to encode RIO_EVENT_TOKEN"),
};

pub(crate) const RIO_EVENT_KEY: usize = RIO_EVENT_TOKEN.raw() as usize;
pub(crate) type PreInit = IoCompletionPort;

pub(crate) type IocpDriverCompletionDiagnostics =
    DriverCompletionDiagnostics<IocpCompletionDiagnostics>;
pub use crate::op::IocpOpState;

// ============================================================================
// State & Lifecycle Types
// ============================================================================

/// The IOCP driver implementation that manages I/O completion ports and operations.
pub struct IocpDriver<'a> {
    completion: CompletionPump,
    ops: IocpOpRegistry,
    extensions: Extensions,
    timer: TimerEngine,
    handles: HandleRegistry,
    remote_cancel_sender: RemoteCancelSender,
    remote_cancel_receiver: mpsc::Receiver<CancelRequest>,
    completion_diagnostics: IocpDriverCompletionDiagnostics,

    // RIO Support (required)
    rio: IocpRioRuntime<'a>,
    shutting_down: bool,
    closed: bool,

    blocking_pool: ThreadPool,

    // Rust drops fields in declaration order; keep this last so WSACleanup runs
    // after socket/RIO-backed state has been torn down.
    _winsock: WinsockGuard,
}

/// Closing mode for the driver or operations.
#[derive(Clone, Copy, Debug)]
pub enum CloseMode {
    /// Closes quickly without waiting for pending operations.
    Fast,
    /// Closes after a specified timeout, allowing pending operations to finish.
    Strict { timeout: Duration },
}

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
    pub(crate) fn debug_registered_file(&self, idx: usize) -> Option<&RegisteredHandle> {
        self.handles.registered_file(idx)
    }

    #[cfg(test)]
    pub(crate) fn debug_remote_free_contains(&self, needle: usize) -> bool {
        use std::sync::atomic::Ordering;
        use veloq_driver_core::slot::SlotTable;

        let mut cur = self.ops.shared.remote_free_head.load(Ordering::Acquire);
        while cur != SlotTable::<IocpSlotSpec>::NULL_INDEX {
            if cur == needle {
                return true;
            }
            cur = self.ops.shared.slots[cur].next_free.load(Ordering::Relaxed);
        }
        false
    }
}

impl<'a> Driver for IocpDriver<'a> {
    type SlotSpec = IocpSlotSpec;
    type Raw = IocpHandle;

    fn reserve_op_raw(&mut self) -> IocpResult<OpToken> {
        let (user_data, generation) = match self.ops.insert(OpEntry::new(IocpOpState::default())) {
            Ok(handle) => (handle.index, handle.generation),
            Err(_) => {
                return Err(IocpError::Registration.report("iocp/driver", "OpRegistry is full"));
            }
        };
        OpToken::from_registry_parts(user_data, generation).map_err(|err| {
            IocpError::Registration
                .to_report()
                .push_ctx("scope", "iocp/driver.reserve_op")
                .with_ctx("slot_index", user_data)
                .with_ctx("generation", generation)
                .with_ctx("op_token_error", format!("{err:?}"))
                .attach_note("reserved op slot cannot be encoded as completion token")
        })
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

    fn slot_set_payload_raw(&mut self, token: OpToken, payload: IocpUserPayload) {
        let _ = self
            .ops
            .with_slot_storage_mut(token, |_result, payload_cell, _sidecar| {
                *payload_cell = Some(payload);
            });
    }

    fn slot_take_payload_raw(&mut self, token: OpToken) -> Option<IocpUserPayload> {
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
        op_in: &mut Option<IocpOp>,
    ) -> DriverSubmitResult<IocpError> {
        if self.shutting_down {
            return DriverSubmitResult::failed(
                IocpError::Internal
                    .to_report()
                    .push_ctx("scope", "iocp/driver")
                    .set_error_code(ERROR_OPERATION_ABORTED as i32)
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
        let ctx = SubmitContextInternal::new(
            completion.port_arc(),
            timer.wheel_mut(),
            completion.table(),
            diagnostics,
        );

        Self::on_submit_res(
            &mut self.ops,
            ctx,
            result,
            token,
            op_in,
            &self.blocking_pool,
        )
    }

    fn drive(&mut self, mode: DriveMode) -> IocpResult<DriveOutcome> {
        self.drain_deferred_socket_cleanup();

        match mode {
            DriveMode::Poll => {
                self.get_completion(0)
                    .push_ctx("scope", "iocp/driver.drive.poll")
                    .attach_note("drive(Poll) failed")?;
            }
            DriveMode::Wait => {
                let pending_progress = self.has_active_ops_internal()
                    || self.ops.shared.has_ready_completion()
                    || self.handles.deferred_cleanup_len() > 0;
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

        self.drain_deferred_socket_cleanup();

        let pending_progress = self.has_active_ops_internal()
            || self.ops.shared.has_ready_completion()
            || self.handles.deferred_cleanup_len() > 0;
        Ok(DriveOutcome {
            next_timeout_hint: self.timer.next_timeout(),
            pending_progress,
        })
    }

    fn completion_table(&self) -> SharedCompletionTable<Self::SlotSpec> {
        self.completion.completion_table()
    }

    fn cancel_op(&mut self, request: CancelRequest) -> IocpResult<CancelSubmitOutcome> {
        self.cancel_op_internal(request)
    }

    fn register_chunk(&mut self, id: ChunkId, ptr: *const u8, len: usize) -> IocpResult<()> {
        IocpDriver::register_chunk(self, id, ptr, len)
            .push_ctx("scope", "iocp/driver")
            .attach_note("register chunk failed")
    }

    fn register_files<'f>(
        &mut self,
        files: Vec<RegisterFd<'f, IocpHandle>>,
    ) -> IocpResult<Vec<IoFd>> {
        IocpDriver::register_files(self, files)
    }

    fn unregister_files(&mut self, files: Vec<IoFd>) -> IocpResult<()> {
        IocpDriver::unregister_files(self, files)
    }

    fn create_waker(&self) -> Arc<dyn RemoteWaker<IocpError>> {
        IocpDriver::create_waker(self)
    }
}

#[cfg(feature = "test-hooks")]
impl DriverTestHooks for IocpDriver<'_> {
    fn debug_chunk_register_attempts(&self) -> u64 {
        self.rio
            .state()
            .registry
            .registration_stats
            .chunk_register_attempts
    }
}
