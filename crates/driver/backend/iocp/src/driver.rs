mod cancellation;
mod completion;
mod lifecycle;
mod overlapped;
mod polling;
mod registration;
mod state;
mod submission;

use veloq_std::{
    boxed::Box, format, marker::PhantomData, mem::size_of, num::NonZeroUsize, sync::Arc,
    time::Duration, vec::Vec,
};

use diagweave::prelude::*;
use veloq_buf::{AnyBufPool, BufPool, BufferRegistrationRequest, FixedBuf};

use lifecycle::LifecycleService;
pub(crate) use overlapped::OverlappedRegistry;
use registration::RegistrationService;
use submission::{SubmissionService, SubmitContextInternal, SubmitResolution};
use tracing::debug;

pub(crate) use completion::COMP_BACKEND_RIO;

#[cfg(test)]
use crate::RegisteredHandle;
use crate::{
    IocpResult,
    config::{IoFd, IocpConfig, IocpHandle},
    error::IocpError,
    net::addr::SockAddrStorage,
    op::{IocpOp, IocpSlotSpec, IocpUserPayload},
    win32::IoCompletionPort,
};

use veloq_driver_core::{
    driver::{
        BufferRegistrationStatus, CancelRequest, CancelSubmitOutcome, CompletionToken, DriveMode,
        DriveOutcome, DriverRaw, DriverStartup, DriverStartupRequirements, DriverStartupResources,
        DriverSubmitResult, OpToken, RegisterFd, RemoteCancelSender, RemoteWaker,
        SharedCompletionTable, SharedSlotTable, SubmitStatus, UdpReceiveOperationBuilder,
        registry::OpEntry, sealed,
    },
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

pub use crate::op::IocpOpState;
pub(crate) use state::IocpDriverCompletionDiagnostics;
use state::IocpDriverState;

// ============================================================================
// 状态与生命周期类型 / State and lifecycle types
// ============================================================================

/// 管理 I/O completion port 和操作的 IOCP driver 实现。
/// Implements the IOCP driver that manages I/O completion ports and operations.
pub struct IocpDriver<'a> {
    _lifetime: PhantomData<&'a ()>,
    state: Option<Box<IocpDriverState>>,
}

impl<'a> IocpDriver<'a> {
    /// 使用给定配置创建 IOCP driver。 / Creates an IOCP driver with the given configuration.
    pub fn new(config: impl AsRef<IocpConfig>) -> IocpResult<Self> {
        let config_ref = config.as_ref();
        let requirements = <Self as DriverStartup>::startup_requirements(config_ref, &[0])?;
        let address_permit = if let Some(bytes) = requirements.locked_memory_bytes_per_worker() {
            config_ref
                .get_locked_memory_budget()
                .try_reserve_repeated_unaligned(bytes.get(), 1)
                .map_err(|error| {
                    IocpError::InvalidInput
                        .to_report()
                        .with_ctx("charge_error", format!("{error:?}"))
                        .attach_note("failed to reserve RIO address scratch budget")
                })?
                .and_then(|mut permits| permits.pop())
        } else {
            None
        };
        Self::new_with_startup_resources(config, DriverStartupResources::new(address_permit))
    }

    fn close_impl(&mut self, mode: CloseMode) -> IocpResult<()> {
        if self.state.is_none() {
            return Ok(());
        }
        let schedule_reaper = {
            let mut view = self.state_mut().lifecycle_view();
            LifecycleService::new(&mut view).close_impl(mode)?
        };
        if schedule_reaper {
            LifecycleService::schedule_deferred_cleanup(self.take_state());
        }
        Ok(())
    }

    fn drain_deferred_socket_cleanup(&mut self) {
        let mut view = self.state_mut().registration_view();
        RegistrationService::new(&mut view).drain_deferred_socket_cleanup();
    }

    fn cancel_op_internal(&mut self, request: CancelRequest) -> IocpResult<CancelSubmitOutcome> {
        let mut view = self.state_mut().cancellation_view();
        cancellation::CancellationService::new(&mut view).cancel_op_internal(request)
    }

    fn get_completion(&mut self, timeout: Option<Duration>) -> IocpResult<()> {
        let mut view = self.state_mut().polling_view();
        polling::PollingService::new(&mut view).get_completion(timeout)
    }

    fn register_buffer_backend(
        &mut self,
        request: BufferRegistrationRequest,
    ) -> IocpResult<BufferRegistrationStatus> {
        let mut view = self.state_mut().registration_view();
        RegistrationService::new(&mut view).register_buffer_backend(request)
    }

    fn register_files<'h>(
        &mut self,
        files: Vec<RegisterFd<'h, IocpHandle>>,
    ) -> IocpResult<Vec<IoFd>> {
        let mut view = self.state_mut().registration_view();
        RegistrationService::new(&mut view).register_files(files)
    }

    fn unregister_files(&mut self, files: Vec<IoFd>) -> IocpResult<()> {
        let mut view = self.state_mut().registration_view();
        RegistrationService::new(&mut view).unregister_files(files)
    }

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

    fn take_state(&mut self) -> Box<IocpDriverState> {
        self.state
            .take()
            .expect("IocpDriver state was already transferred")
    }
}

impl Drop for IocpDriver<'_> {
    fn drop(&mut self) {
        if self.state.is_none() {
            return;
        }
        debug!("Dropping IocpDriver");
        if let Err(error) = self.close_impl(CloseMode::Fast) {
            tracing::error!(report = ?error, "iocp close_impl fast failed during drop");
        }
    }
}

/// driver 或操作的关闭模式。
/// Closing mode for the driver or operations.
#[derive(Clone, Copy, Debug)]
pub enum CloseMode {
    /// 立即关闭，不等待未完成操作。
    /// Closes quickly without waiting for pending operations.
    Fast,
    /// 等待指定时长以允许未完成操作结束，然后关闭。
    /// Closes after a specified timeout, allowing pending operations to finish.
    Strict { timeout: Duration },
}

impl<'a> IocpDriver<'a> {
    pub(crate) fn has_active_ops_internal(&self) -> bool {
        self.state().has_active_ops()
    }

    pub(crate) fn create_waker(&self) -> Arc<dyn RemoteWaker<IocpError>> {
        self.state().create_waker()
    }

    #[cfg(test)]
    pub(crate) fn debug_registered_file(&self, idx: usize) -> Option<&RegisteredHandle> {
        self.state().debug_registered_file(idx)
    }

    #[cfg(test)]
    pub(crate) fn debug_remote_free_contains(&self, needle: usize) -> bool {
        self.state().debug_remote_free_contains(needle)
    }
}

impl<'a> sealed::Sealed for IocpDriver<'a> {}

impl DriverStartup for IocpDriver<'_> {
    type Config = IocpConfig;
    type StartupError = IocpError;

    fn startup_requirements(
        config: &Self::Config,
        workers: &[usize],
    ) -> IocpResult<DriverStartupRequirements> {
        if workers.is_empty() {
            return Ok(DriverStartupRequirements::none());
        }
        let bytes = (config.entries().get() as usize)
            .checked_mul(size_of::<SockAddrStorage>())
            .and_then(NonZeroUsize::new)
            .ok_or_else(|| {
                IocpError::InvalidInput
                    .to_report()
                    .attach_note("RIO address scratch size overflow")
            })?;
        Ok(DriverStartupRequirements::locked_memory_per_worker(bytes))
    }

    fn new_with_startup_resources(
        config: impl AsRef<Self::Config>,
        mut resources: DriverStartupResources,
    ) -> IocpResult<Self> {
        let state = LifecycleService::build(config, resources.take_locked_memory_permit())?;
        Ok(Self {
            _lifetime: PhantomData,
            state: Some(Box::new(state)),
        })
    }
}

impl<'a> UdpReceiveOperationBuilder for IocpDriver<'a> {
    type BuildError = IocpError;

    fn build_udp_recv_multi(
        &mut self,
        fd: IoFd,
        config: UdpReceiveConfig,
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
        let mut slots = Vec::new();
        slots
            .try_reserve_exact(pump_config.kernel_capacity.get())
            .map_err(|error| {
                IocpError::Submission
                    .report(
                        "iocp.udp_recv_multi.build",
                        "failed to reserve backend receive slots",
                    )
                    .with_ctx("slot_count", pump_config.kernel_capacity.get())
                    .attach_note(format!("receive slot allocation failed: {error}"))
            })?;
        for slot_id in 0..pump_config.kernel_capacity.get() {
            let buffer = self
                .state()
                .buffer_pool()
                .and_then(|pool| pool.alloc(pump_config.datagram_capacity, 0))
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
    const BACKEND_NAME: &'static str = "iocp";

    fn reserve_op_raw(&mut self) -> IocpResult<OpToken> {
        let state = self.state_mut();
        let (user_data, generation) =
            match state.ops_mut().insert(OpEntry::new(IocpOpState::default())) {
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
        self.state().ops().shared.clone()
    }

    fn remote_cancel_sender_raw(&self) -> RemoteCancelSender {
        self.state().remote_cancel_sender().clone()
    }

    fn try_recv_remote_cancel_request(&mut self) -> Option<CancelRequest> {
        self.state_mut()
            .remote_cancel_receiver_mut()
            .try_recv()
            .ok()
    }

    fn slot_set_payload_raw(&mut self, token: OpToken, payload: IocpUserPayload) {
        let _ = self.state_mut().ops_mut().with_slot_storage_mut(
            token,
            |_result, payload_cell, _sidecar| {
                *payload_cell = Some(payload);
            },
        );
    }

    fn slot_take_payload_raw(&mut self, token: OpToken) -> Option<IocpUserPayload> {
        self.state_mut()
            .ops_mut()
            .with_slot_storage_mut(token, |_result, payload_cell, _sidecar| payload_cell.take())
            .flatten()
    }

    fn release_op_slot_raw(&mut self, token: OpToken) {
        let _ = self.state_mut().ops_mut().remove(token);
    }

    fn submit_op_raw(
        &mut self,
        token: OpToken,
        op_in: &mut Option<IocpOp>,
    ) -> DriverSubmitResult<IocpError> {
        if self.state().is_shutting_down() {
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

        let mut view = self.state_mut().submission_view();
        let result = match SubmissionService::new(&mut view).call_op_submit(token, op) {
            Ok(res) => res,
            Err(e) => {
                return DriverSubmitResult::failed(
                    e.push_ctx("scope", "iocp/driver")
                        .attach_note("call_op_submit failed"),
                    SubmitStatus::Void,
                );
            }
        };

        let state = self.state_mut().submission_view();
        let port = state.completion.port_arc();
        let completion_table = state.completion.table();
        let ctx = SubmitContextInternal::new(
            port,
            state.timer,
            completion_table,
            state.completion_diagnostics,
        );
        SubmissionService::on_submit_res(SubmitResolution {
            ops: state.ops,
            ctx,
            result,
            token,
            op_in,
            blocking_pool: state.blocking_pool,
        })
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
                    .completion_diagnostics()
                    .backend()
                    .inc_wait_enter();
                let wait_timeout = if self.state().ops().shared.has_ready_completion() {
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

        let next_timeout_hint = self.state().timer().next_deadline().map_err(|error| {
            IocpError::InvalidState
                .report(
                    "iocp.timer.deadline",
                    "timer wheel deadline could not be queried",
                )
                .with_ctx("timer_error", format!("{error:?}"))
        })?;

        Ok(DriveOutcome {
            next_timeout_hint,
            ready_completion: self.state().ops().shared.has_ready_completion(),
            in_flight: self.has_active_ops_internal(),
            pending_work: Default::default(),
            budget_exhausted: false,
            needs_next_round: false,
        })
    }

    fn completion_table_raw(&self) -> SharedCompletionTable<Self::SlotSpec> {
        self.state().completion().completion_table()
    }

    fn cancel_op_raw(&mut self, request: CancelRequest) -> IocpResult<CancelSubmitOutcome> {
        self.cancel_op_internal(request)
    }

    fn register_buffer_raw(
        &mut self,
        request: BufferRegistrationRequest,
    ) -> IocpResult<BufferRegistrationStatus> {
        IocpDriver::register_buffer_backend(self, request)
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

    fn attach_buffer_pool_raw(&mut self, pool: AnyBufPool) -> IocpResult<()> {
        self.state_mut().retain_buffer_pool(pool)
    }
}

#[cfg(feature = "test-hooks")]
impl DriverTestHooks for IocpDriver<'_> {
    fn debug_chunk_register_attempts(&self) -> u64 {
        self.state().debug_chunk_register_attempts()
    }
}
