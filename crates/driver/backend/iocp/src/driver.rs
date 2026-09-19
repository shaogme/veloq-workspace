mod cancellation;
pub(crate) mod completion;
mod lifecycle;
mod polling;
mod registration;
mod submission;

use veloq_blocking::ThreadPool;
use veloq_std::{
    boxed::Box,
    format,
    sync::{Arc, mpsc},
    time::Duration,
    vec::Vec,
};

use diagweave::prelude::*;
use veloq_buf::{AnyBufPool, BufPool, BufferRegistrar, FixedBuf};

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
    op::{IocpOp, IocpOpRegistry, IocpSlotSpec, IocpUserPayload},
    win32::IoCompletionPort,
};

use veloq_buf::heap::ChunkId;
use veloq_driver_core::driver::{
    BufferRegistrationStatus, CancelRequest, CancelSubmitOutcome, CompletionToken, DriveMode,
    DriveOutcome, DriverCompletionDiagnostics, DriverRaw, DriverSubmitResult, OpToken, RegisterFd,
    RemoteCancelSender, RemoteWaker, SharedCompletionTable, SharedSlotTable, SubmitStatus,
    UdpReceiveOperationBuilder, registry::OpEntry, sealed,
};
use veloq_driver_core::op::OpKind;
use veloq_driver_core::{
    op::types::UdpRecvMulti,
    platform::receive_pump::{ReceivePumpState, ReceiveSlot, UdpReceiveConfig},
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
    /// The registrar is borrowed by the live driver only. Deferred cleanup must not retain this
    /// borrow because it can outlive the caller that owns the registrar.
    registrar: &'a (dyn BufferRegistrar + 'a),
    state: Option<Box<IocpDriverState>>,
}

/// Address-stable state that owns every resource which may be referenced by a pending request.
///
/// An active `OVERLAPPED` points into an operation registry slot, so transferring this state to
/// the reaper must move only the box pointer and must never move individual operations into a
/// relocating collection.
pub(crate) struct IocpDriverState {
    completion: CompletionPump,
    ops: IocpOpRegistry,
    extensions: Extensions,
    timer: TimerEngine,
    remote_cancel_sender: RemoteCancelSender,
    remote_cancel_receiver: mpsc::Receiver<CancelRequest>,
    completion_diagnostics: IocpDriverCompletionDiagnostics,

    // RIO Support (required)
    rio: IocpRioRuntime,
    handles: HandleRegistry,
    shutting_down: bool,
    closed: bool,

    blocking_pool: ThreadPool,

    // Rust drops fields in declaration order; keep this last so WSACleanup runs
    // after socket/RIO-backed state has been torn down.
    _winsock: WinsockGuard,
}

// SAFETY: the reaper receives the complete state through one channel and is the only thread that
// polls or drops it afterwards. Active OVERLAPPED values remain in their original registry slots.
unsafe impl Send for IocpDriverState {}

impl<'a> IocpDriver<'a> {
    fn state(&self) -> &IocpDriverState {
        self.state
            .as_deref()
            .expect("IocpDriver state was transferred to the deferred reaper")
    }

    fn state_mut(&mut self) -> &mut IocpDriverState {
        self.state
            .as_deref_mut()
            .expect("IocpDriver state was transferred to the deferred reaper")
    }

    pub(super) fn take_state(&mut self) -> Box<IocpDriverState> {
        self.state
            .take()
            .expect("IocpDriver state was already transferred")
    }
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
        match op.kind() {
            OpKind::Recv | OpKind::Send | OpKind::UdpSend | OpKind::SendTo => true,
            OpKind::RecvProvided | OpKind::RecvMulti | OpKind::UdpRecvMulti => {
                op.multishot_vtable().is_some_and(|vtable| {
                    let cardinality_ok = match op.kind() {
                        OpKind::RecvProvided => !op.is_multishot(),
                        _ => op.is_multishot(),
                    };
                    cardinality_ok && vtable.operation == op.kind()
                })
            }
            OpKind::AcceptMulti => false,
            _ => false,
        }
    }

    pub(crate) fn has_active_ops_internal(&self) -> bool {
        self.state().ops.has_active_ops()
    }

    pub(crate) fn create_waker(&self) -> Arc<dyn RemoteWaker<IocpError>> {
        self.state().completion.create_waker()
    }

    #[cfg(test)]
    pub(crate) fn debug_registered_file(&self, idx: usize) -> Option<&RegisteredHandle> {
        self.state().handles.registered_file(idx)
    }

    #[cfg(test)]
    pub(crate) fn debug_remote_free_contains(&self, needle: usize) -> bool {
        use veloq_driver_core::slot::SlotTable;
        use veloq_std::sync::atomic::Ordering;

        let ops = &self.state().ops;
        let mut cur = ops.shared.remote_free_head.load(Ordering::Acquire);
        while cur != SlotTable::<IocpSlotSpec>::NULL_INDEX {
            if cur == needle {
                return true;
            }
            cur = ops.shared.slots[cur].next_free.load(Ordering::Relaxed);
        }
        false
    }
}

impl<'a> sealed::Sealed for IocpDriver<'a> {}

impl<'a> UdpReceiveOperationBuilder for IocpDriver<'a> {
    type BuildError = IocpError;

    fn build_udp_recv_multi(
        &mut self,
        fd: IoFd,
        config: UdpReceiveConfig,
        buffer_pool: AnyBufPool,
    ) -> IocpResult<UdpRecvMulti<Self::Raw>> {
        let config = config.validate().map_err(|error| {
            IocpError::InvalidInput
                .report(
                    "iocp.udp_recv_multi.build",
                    "invalid UDP receive configuration",
                )
                .with_ctx("build_error", format!("{error:?}"))
        })?;
        let pump_config = config.into_pump_config();
        let mut slots = Vec::with_capacity(pump_config.kernel_capacity.get());
        for slot_id in 0..pump_config.kernel_capacity.get() {
            let buffer = buffer_pool
                .alloc(pump_config.datagram_capacity, 0)
                .or_else(|| FixedBuf::alloc_heap(pump_config.datagram_capacity, 0).ok())
                .ok_or_else(|| {
                    IocpError::Submission
                        .report(
                            "iocp.udp_recv_multi.build",
                            "failed to allocate backend receive buffer",
                        )
                        .with_ctx("slot_id", slot_id)
                        .with_ctx("buffer_capacity", pump_config.datagram_capacity.get())
                })?;
            slots.push(ReceiveSlot::new(slot_id as u32, buffer));
        }
        let pump =
            ReceivePumpState::try_new(pump_config, slots.into_boxed_slice()).map_err(|error| {
                IocpError::InvalidInput
                    .report("iocp.udp_recv_multi.build", "failed to create receive pump")
                    .with_ctx("receive_pump_error", format!("{error:?}"))
            })?;
        Ok(UdpRecvMulti::from_backend(fd, pump))
    }
}

impl<'a> DriverRaw for IocpDriver<'a> {
    type SlotSpec = IocpSlotSpec;
    type Raw = IocpHandle;

    fn reserve_op_raw(&mut self) -> IocpResult<OpToken> {
        let state = self.state_mut();
        let (user_data, generation) = match state.ops.insert(OpEntry::new(IocpOpState::default())) {
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

    fn slot_table_raw(&self) -> SharedSlotTable<Self::SlotSpec> {
        self.state().ops.shared.clone()
    }

    fn remote_cancel_sender_raw(&self) -> RemoteCancelSender {
        self.state().remote_cancel_sender.clone()
    }

    fn try_recv_remote_cancel_request(&mut self) -> Option<CancelRequest> {
        self.state_mut().remote_cancel_receiver.try_recv().ok()
    }

    fn slot_set_payload_raw(&mut self, token: OpToken, payload: IocpUserPayload) {
        let _ =
            self.state_mut()
                .ops
                .with_slot_storage_mut(token, |_result, payload_cell, _sidecar| {
                    *payload_cell = Some(payload);
                });
    }

    fn slot_take_payload_raw(&mut self, token: OpToken) -> Option<IocpUserPayload> {
        self.state_mut()
            .ops
            .with_slot_storage_mut(token, |_result, payload_cell, _sidecar| payload_cell.take())
            .flatten()
    }

    fn release_op_slot_raw(&mut self, token: OpToken) {
        let _ = self.state_mut().ops.remove(token);
    }

    fn submit_op_raw(
        &mut self,
        token: OpToken,
        op_in: &mut Option<IocpOp>,
    ) -> DriverSubmitResult<IocpError> {
        if self.state().shutting_down {
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

        let state = self.state_mut();
        let completion = &state.completion;
        let timer = &mut state.timer;
        let diagnostics = &mut state.completion_diagnostics;
        let ctx = SubmitContextInternal::new(
            completion.port_arc(),
            timer,
            completion.table(),
            diagnostics,
        );

        Self::on_submit_res(
            &mut state.ops,
            ctx,
            result,
            token,
            op_in,
            &state.blocking_pool,
        )
    }

    fn drive_raw(&mut self, mode: DriveMode) -> IocpResult<DriveOutcome> {
        self.drain_deferred_socket_cleanup();

        match mode {
            DriveMode::Poll => {
                self.get_completion(Some(Duration::ZERO))
                    .push_ctx("scope", "iocp/driver.drive.poll")
                    .attach_note("drive(Poll) failed")?;
            }
            DriveMode::Wait { timeout } => {
                self.state()
                    .completion_diagnostics
                    .backend()
                    .inc_wait_enter();
                let wait_timeout = if self.state().ops.shared.has_ready_completion() {
                    Some(Duration::ZERO)
                } else {
                    timeout
                };
                self.get_completion(wait_timeout)
                    .push_ctx("scope", "iocp/driver.drive.wait")
                    .attach_note("wait for completion failed")?;
            }
        }

        self.drain_deferred_socket_cleanup();

        let next_timeout_hint = self.state().timer.next_deadline().map_err(|error| {
            IocpError::InvalidState
                .report(
                    "iocp.timer.deadline",
                    "timer wheel deadline could not be queried",
                )
                .with_ctx("timer_error", format!("{error:?}"))
        })?;

        Ok(DriveOutcome {
            next_timeout_hint,
            ready_completion: self.state().ops.shared.has_ready_completion(),
            in_flight: self.has_active_ops_internal(),
            pending_work: Default::default(),
            budget_exhausted: false,
            needs_next_round: false,
        })
    }

    fn completion_table_raw(&self) -> SharedCompletionTable<Self::SlotSpec> {
        self.state().completion.completion_table()
    }

    fn cancel_op_raw(&mut self, request: CancelRequest) -> IocpResult<CancelSubmitOutcome> {
        self.cancel_op_internal(request)
    }

    fn register_buffer_raw(
        &mut self,
        id: ChunkId,
        ptr: *const u8,
        len: usize,
    ) -> IocpResult<BufferRegistrationStatus> {
        IocpDriver::register_buffer_backend(self, id, ptr, len)
            .push_ctx("scope", "iocp/driver")
            .attach_note("register buffer failed")
    }

    fn register_files_raw<'f>(
        &mut self,
        files: Vec<RegisterFd<'f, IocpHandle>>,
    ) -> IocpResult<Vec<IoFd>> {
        IocpDriver::register_files(self, files)
    }

    fn unregister_files_raw(&mut self, files: Vec<IoFd>) -> IocpResult<()> {
        IocpDriver::unregister_files(self, files)
    }

    fn create_waker_raw(&self) -> Arc<dyn RemoteWaker<IocpError>> {
        IocpDriver::create_waker(self)
    }
}

#[cfg(feature = "test-hooks")]
impl DriverTestHooks for IocpDriver<'_> {
    fn debug_chunk_register_attempts(&self) -> u64 {
        self.state()
            .rio
            .state()
            .registry
            .registration_stats
            .chunk_register_attempts
    }
}
