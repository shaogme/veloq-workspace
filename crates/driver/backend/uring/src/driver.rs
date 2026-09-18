use diagweave::prelude::*;
use tracing::{debug, trace};
use veloq_buf::{AnyBufPool, BufferRegistrar, heap::ChunkId};
use veloq_io_uring::{
    IoUring, KernelCapabilities, Probe, ResourceLayout, RingConfig, SetupFlags, SetupPolicy,
    opcode, types,
};
use veloq_std::{format, io, sync::Arc, vec::Vec};

#[cfg(feature = "test-hooks")]
use veloq_std::string::String;

use crate::{
    config::{IoFd, IoMode, UringConfig, UringRawHandle},
    diagnostics::{
        UringCapabilitySnapshot, UringCompletionDiagnostics, UringCompletionDiagnosticsSnapshot,
        UringSetupSnapshot,
    },
    driver::{
        capability::{CapabilityState, CapabilityStateSnapshot},
        completion::CompletionEngine,
        context::{DriveContext, SubmitPort},
        control::{ControlPlaneEvent, UringControlPlane, UringWakerManager},
        drive::DriveCoordinator,
        lifecycle::LifecycleEngine,
        operation::OperationLedger,
        registration::provided_buf::{PROVIDED_BUF_GROUP_ID, RingLifetimeOwner},
        registration::{MAX_CHUNKS, OperationProbe, RegistrationEngine, RegistrationPorts},
        submission::SubmissionEngine,
    },
    error::{UringError, UringResult},
    op::{UringOp, UringSlotSpec, UringUserPayload},
};
use veloq_driver_core::driver::{
    BufferRegistrationStatus, CancelRequest, CancelSubmitOutcome, CompletionToken, DriveMode,
    DriveOutcome, DriverCapabilities, DriverCapability, DriverCompletionDiagnostics,
    DriverCompletionDiagnosticsSnapshot, DriverRaw, DriverSubmitResult, OpToken, RegisterFd,
    RemoteCancelSender, RemoteWaker, SharedCompletionTable, SharedSlotTable, SubmitStatus,
    registry::{OpEntry, OpHandle},
    sealed,
};

pub(crate) mod capability;
pub(crate) mod completion;
pub(crate) mod context;
pub(crate) mod control;
pub(crate) mod drive;
pub(crate) mod env;
pub(crate) mod lifecycle;
pub(crate) mod operation;
pub(crate) mod registration;
pub(crate) mod submission;

#[cfg(test)]
mod protocol_model;

pub use lifecycle::UringOpState;
pub use registration::ProvidedBufferSnapshot;

/// 从 opcode 探测结果得出乐观的能力集合。
///
/// 「乐观」是关键：opcode 在场只说明**可能**支持 multishot 变体，真正的判定推迟到第一次
/// 提交（见 [`Driver::note_capability_rejected`]）。`provided_buffers` 不在此列——它不靠
/// 猜：`register_buf_ring` 成功与否就是答案，而那要等池到位（见
/// [`Driver::attach_buffer_pool`]），所以这里先记 `false`。
fn probe_capabilities(probe: &Probe) -> DriverCapabilities {
    DriverCapabilities {
        accept_multi: probe.is_supported(opcode::Accept::<types::Fd>::CODE),
        recv_multi: probe.is_supported(opcode::Recv::<types::Fd>::CODE),
        provided_buffers: false,
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct SetupNegotiation {
    requested_flags: u32,
    rejected_flags: u32,
    failure_errno: Option<i32>,
}

fn setup_report(
    kind: UringError,
    scope: &'static str,
    error: io::Error,
    policy: SetupPolicy,
    mode: IoMode,
) -> Report<UringError> {
    kind.io_report(scope, error)
        .with_ctx("setup_required_flags", policy.required().bits())
        .with_ctx("setup_best_effort_flags", policy.best_effort().bits())
        .with_ctx("setup_disabled_flags", policy.disabled().bits())
        .with_ctx("setup_mode", format!("{mode:?}"))
}

fn build_negotiated_ring(config: &UringConfig) -> UringResult<(IoUring, SetupNegotiation)> {
    let mut policy = config.setup_policy;
    if let IoMode::Polling(_) = config.mode {
        policy = policy.with_required(SetupFlags::SQPOLL);
    }
    policy.validate().map_err(|error| {
        setup_report(
            UringError::InvalidInput,
            "driver.new.setup_policy",
            error,
            policy,
            config.mode,
        )
    })?;

    let entries = config.entries.get();
    let base = IoUring::from_config(RingConfig::new(entries)).map_err(|error| {
        setup_report(
            UringError::DriverInit,
            "driver.new.build_base_ring",
            error,
            policy,
            config.mode,
        )
    })?;
    let baseline_capabilities = base.params().capabilities();
    let requested = policy.requested();
    if requested.is_empty() {
        debug!(
            baseline_setup_flags = baseline_capabilities.setup_flags,
            "created io_uring base setup profile"
        );
        return Ok((base, SetupNegotiation::default()));
    }

    let profile = RingConfig::new(entries)
        .with_setup_policy(policy)
        .with_sq_thread_idle(match config.mode {
            IoMode::Polling(idle_ms) => idle_ms.get(),
            IoMode::Interrupt => 0,
        });
    let full_error = match IoUring::from_config(profile) {
        Ok(ring) => {
            drop(base);
            return Ok((
                ring,
                SetupNegotiation {
                    requested_flags: requested.bits(),
                    ..SetupNegotiation::default()
                },
            ));
        }
        Err(error) => error,
    };
    let full_errno = full_error.raw_os_error();

    let negotiable = [
        SetupFlags::COOP_TASKRUN,
        SetupFlags::SINGLE_ISSUER,
        SetupFlags::DEFER_TASKRUN,
    ];
    let mut selected_ring = None;
    let mut selected_count = 0;
    for subset in 0_u32..(1_u32 << negotiable.len()) {
        let mut best_effort = SetupFlags::EMPTY;
        for (index, flag) in negotiable.iter().enumerate() {
            if subset & (1_u32 << index) != 0 && policy.best_effort().contains(*flag) {
                best_effort = best_effort.union(*flag);
            }
        }
        if best_effort == policy.best_effort() {
            continue;
        }
        let candidate_policy = SetupPolicy::new(policy.required(), best_effort, policy.disabled());
        let candidate = RingConfig::new(entries)
            .with_setup_policy(candidate_policy)
            .with_sq_thread_idle(match config.mode {
                IoMode::Polling(idle_ms) => idle_ms.get(),
                IoMode::Interrupt => 0,
            });
        if let Ok(ring) = IoUring::from_config(candidate) {
            let count = best_effort.bits().count_ones();
            if count > selected_count {
                selected_ring = Some(ring);
                selected_count = count;
            }
        }
    }

    if let Some(ring) = selected_ring {
        let accepted = SetupFlags::from_bits_retain(ring.params().setup_flags());
        let rejected = requested.difference(accepted);
        drop(base);
        return Ok((
            ring,
            SetupNegotiation {
                requested_flags: requested.bits(),
                rejected_flags: rejected.bits(),
                failure_errno: full_errno,
            },
        ));
    }

    if policy.required().is_empty() {
        let rejected = requested.difference(SetupFlags::from_bits_retain(
            baseline_capabilities.setup_flags,
        ));
        debug!(
            requested_setup_flags = requested.bits(),
            rejected_setup_flags = rejected.bits(),
            errno = ?full_errno,
            "falling back to the io_uring base setup profile"
        );
        return Ok((
            base,
            SetupNegotiation {
                requested_flags: requested.bits(),
                rejected_flags: rejected.bits(),
                failure_errno: full_errno,
            },
        ));
    }

    let kind = if matches!(config.mode, IoMode::Polling(_))
        && policy.required().contains(SetupFlags::SQPOLL)
    {
        UringError::PollingUnavailable
    } else {
        UringError::DriverInit
    };
    Err(setup_report(
        kind,
        "driver.new.build_setup_profile",
        full_error,
        policy,
        config.mode,
    ))
}

pub struct UringDriver<'a> {
    // Rust 按声明顺序从上到下析构字段。`ring`、`ring_lifetime` 必须在
    // `registration` 之前声明：Drop 函数体只尝试显式反注册；如果 syscall 失败或 driver
    // 尚未 quiescent，group 会留在 registry 中。函数体返回后，`IoUring` 先析构并关闭 ring
    // fd，随后 `ring_lifetime` 把 owner token 标记为 dead，最后 registration 才释放 mapping 和
    // `FixedBuf`。这把原来仅靠字段顺序的兜底变成了可断言的生命周期协议。
    ring: IoUring,
    ring_lifetime: RingLifetimeOwner,
    operations: OperationLedger,
    completion_diagnostics: DriverCompletionDiagnostics<UringCompletionDiagnostics>,
    completion_table: SharedCompletionTable<UringSlotSpec>,

    control: UringControlPlane,
    setup_negotiation: SetupNegotiation,
    kernel_capabilities: KernelCapabilities,
    capability_state: CapabilityState,
    // Component owners keep submission, lifecycle, completion scratch and drive budgets out of
    // the public facade while preserving the existing protocol implementation.
    registration: RegistrationEngine<'a>,
    submission: SubmissionEngine,
    lifecycle: LifecycleEngine,
    completion: CompletionEngine,
    drive: DriveCoordinator,
}

impl<'a> UringDriver<'a> {
    fn new_internal(
        config: impl AsRef<UringConfig>,
        registrar: &'a (dyn BufferRegistrar + 'a),
    ) -> UringResult<Self> {
        let config = config.as_ref();
        let entries = config.entries.get();
        config
            .drive_limits
            .validate(entries as usize)
            .map_err(|message| {
                UringError::DriverInit
                    .report("driver.new.drive_limits", message)
                    .attach_note("all io_uring drive budgets must be bounded and non-zero")
            })?;
        let (mut ring, setup_negotiation) = build_negotiated_ring(config)?;

        let operations = OperationLedger::new(entries as usize);
        let completion_table: SharedCompletionTable<UringSlotSpec> = operations.shared_table();
        let completion_diagnostics = operations.shared.completion_diagnostics();

        let waker = UringWakerManager::new()?;
        let ring_lifetime = RingLifetimeOwner::new();

        // opcode 探测只能回答「这个 opcode 存在吗」，回答不了「它的 multishot 变体存在
        // 吗」——那是同一个 opcode 上后加的标志位。所以这里只排除掉真正缺 opcode 的内核，
        // 剩下的由第一次提交去问（`note_capability_rejected`）。
        let mut ring_probe = Probe::new();
        let mut kernel_capabilities = ring.params().capabilities();
        let probe_error = match ring.submitter().register_probe(&mut ring_probe) {
            Ok(()) => {
                kernel_capabilities = kernel_capabilities.with_probe(&ring_probe);
                None
            }
            Err(error) => {
                let errno = error.raw_os_error().unwrap_or(libc::EIO);
                kernel_capabilities = kernel_capabilities.with_probe_error(errno);
                Some(errno)
            }
        };
        if probe_error.is_some() {
            debug!("IORING_REGISTER_PROBE unavailable; assuming no optional opcodes");
            ring_probe = Probe::new();
        }
        debug!("Initalized UringDriver with {} entries", entries);

        let mut driver = Self {
            ring,
            ring_lifetime,
            operations,
            completion_diagnostics,
            completion_table,
            control: UringControlPlane::with_capacity(
                waker,
                (entries as usize).saturating_mul(2).saturating_add(1),
            ),
            setup_negotiation,
            kernel_capabilities,
            capability_state: CapabilityState::new(probe_capabilities(&ring_probe)),
            registration: RegistrationEngine::new(
                config.registration_mode,
                config.provided_buffers,
                registrar,
                config.file_table_capacity,
                config.file_table_exhaustion,
            ),
            submission: SubmissionEngine::new(),
            lifecycle: LifecycleEngine::new(),
            completion: CompletionEngine::with_capacity(
                config
                    .drive_limits
                    .max_cqe_batch
                    .max(config.drive_limits.emergency_drain_limit),
                config
                    .drive_limits
                    .max_cqe_batch
                    .max(config.drive_limits.emergency_drain_limit)
                    .saturating_mul(4)
                    .saturating_add(8),
                config.drive_limits,
            ),
            drive: DriveCoordinator::new(config.drive_limits),
        };

        let submitter = driver.ring.submitter();
        driver
            .registration
            .ensure_file_table_initialized(&submitter, &mut driver.kernel_capabilities)?;

        match driver.ring.submitter().register_buffers_sparse(MAX_CHUNKS) {
            Ok(registration) => {
                driver.kernel_capabilities = driver
                    .kernel_capabilities
                    .with_buffer_registration(&registration);
                driver
                    .registration
                    .set_fixed_buffers_available(registration);
                debug!(
                    registration_mode = config.registration_mode.as_str(),
                    fixed_buffers_available = true,
                    fixed_buffer_slots = MAX_CHUNKS,
                    fallback = false,
                    "registered sparse fixed-buffer table"
                );
            }
            Err(e) => {
                let errno = e.raw_os_error();
                driver.kernel_capabilities =
                    driver.kernel_capabilities.with_buffer_registration_error(
                        MAX_CHUNKS as u32,
                        ResourceLayout::Sparse,
                        false,
                        errno,
                    );
                driver.registration.set_fixed_buffers_unavailable(errno);
                tracing::warn!(
                    registration_mode = config.registration_mode.as_str(),
                    errno = ?errno,
                    fixed_buffers_available = false,
                    fallback = !config.registration_mode.is_strict(),
                    error = %e,
                    "sparse fixed-buffer registration unavailable"
                );
                if config.registration_mode.is_strict() {
                    return Err(UringError::Registration
                        .io_report("driver.new.register_fixed_buffers", e)
                        .attach_note(
                            "strict buffer registration mode does not permit a raw-buffer fallback",
                        ));
                }
            }
        }

        {
            let Self {
                operations,
                control,
                registration,
                completion_diagnostics,
                ring,
                kernel_capabilities,
                submission,
                ..
            } = &mut driver;
            let mut context = SubmitPort::from_parts(
                operations,
                control,
                registration,
                completion_diagnostics,
                ring,
                kernel_capabilities,
            );
            submission.submit_waker(&mut context)?;
        }

        debug!(
            capabilities = ?driver.capability_state_snapshot(),
            "recorded io_uring capability snapshot"
        );

        Ok(driver)
    }

    pub fn new(
        config: impl AsRef<UringConfig>,
        registrar: &'a (dyn BufferRegistrar + 'a),
    ) -> UringResult<Self> {
        Self::new_internal(config, registrar).attach_note("create uring driver")
    }

    /// provided buffer 环的运行期统计，`None` 表示这个 driver 没有环。
    pub fn provided_buf_stats(&self) -> Option<ProvidedBufferSnapshot> {
        self.provided_buffer_snapshot()
    }

    /// Returns the point-in-time provided-buffer snapshot captured while harvesting CQEs.
    pub fn provided_buffer_snapshot(&self) -> Option<ProvidedBufferSnapshot> {
        self.registration.provided_buf_stats()
    }

    /// Returns a point-in-time snapshot of completion and backend cleanup diagnostics.
    pub fn completion_diagnostics_snapshot(
        &self,
    ) -> DriverCompletionDiagnosticsSnapshot<UringCompletionDiagnosticsSnapshot> {
        self.completion_diagnostics.snapshot()
    }

    /// Returns the effective capability snapshot currently used by new operations.
    pub fn capability_snapshot(&self) -> UringCapabilitySnapshot {
        self.capability_snapshot_for(self.capability_state.effective())
    }

    /// Returns the immutable capability baseline observed during ring setup and probing.
    pub fn capability_baseline(&self) -> UringCapabilitySnapshot {
        self.capability_snapshot_for(self.capability_state.baseline())
    }

    /// Returns the capability set accepted by setup and resource negotiation.
    pub fn negotiated_capabilities(&self) -> DriverCapabilities {
        self.capability_state.negotiated()
    }

    /// Returns the capability set that is safe for new operations now.
    pub fn effective_capabilities(&self) -> DriverCapabilities {
        self.capability_state.effective()
    }

    /// Returns baseline, negotiated, effective capabilities and disable reasons together.
    pub fn capability_state_snapshot(&self) -> CapabilityStateSnapshot {
        self.capability_state.snapshot()
    }

    fn capability_snapshot_for(&self, capabilities: DriverCapabilities) -> UringCapabilitySnapshot {
        UringCapabilitySnapshot::new(
            self.kernel_capabilities,
            capabilities,
            UringSetupSnapshot {
                requested_flags: self.setup_negotiation.requested_flags,
                rejected_flags: self.setup_negotiation.rejected_flags,
                failure_errno: self.setup_negotiation.failure_errno,
            },
            self.registration.fixed_buffers_available(),
            self.registration.fixed_buffers_failure_errno(),
            capabilities.provided_buffers,
            self.registration.provided_buffers_failure_errno(),
        )
    }
}

/// The single shutdown protocol for normal drop and every initialization failure after the
/// driver value has been constructed. The field order below then provides the final fallback:
/// ring fd, lifetime token, registration-owned resources, and remaining control resources.
struct RingShutdown;

impl RingShutdown {
    fn run<'a>(driver: &mut UringDriver<'a>) {
        let has_provided_buffers = driver.registration.has_provided_buffers();
        if driver.operations.has_active_ops() {
            tracing::warn!("UringDriver dropped with active in-flight operations");
        }
        if !has_provided_buffers {
            driver
                .completion_diagnostics
                .backend()
                .inc_provided_drop_skipped_unregister();
        } else {
            let pending_completions = if driver.completion.has_pending_cqes()
                || driver.operations.shared.has_ready_completion()
            {
                true
            } else {
                let mut completion = driver.ring.completion();
                completion.sync();
                !completion.is_empty()
            };
            let probe = OperationProbe::from_registry(&mut driver.operations, pending_completions);
            let quiesce = driver.registration.quiescence(probe);
            if quiesce.is_quiescent() {
                // 正常关闭优先显式反注册。失败时 release_provided_buffers 会恢复 group 的所有权，
                // 不在仍存活的 IoUring 前释放映射；随后依靠 owner token 和字段顺序完成最终兜底。
                if let Err(report) = driver
                    .registration
                    .release_provided_buffers(&driver.ring.submitter())
                {
                    tracing::warn!(
                        bgid = PROVIDED_BUF_GROUP_ID,
                        report = ?report,
                        "failed to unregister provided buffer ring; retaining it until io_uring drops"
                    );
                }
            } else {
                driver
                    .completion_diagnostics
                    .backend()
                    .inc_provided_drop_deferred_unregister();
                tracing::warn!(
                    active_operations = quiesce.active_operations(),
                    armed_provided_multishot = quiesce.armed_provided_multishot(),
                    pending_completions = quiesce.pending_completions(),
                    selected_bids = quiesce.selected_bids(),
                    "deferring provided buffer ring unregister until io_uring drops"
                );
            }
        }
        let outstanding_cancel_tickets = driver.control.cancel_in_flight_len();
        if outstanding_cancel_tickets != 0 {
            tracing::warn!(
                count = outstanding_cancel_tickets,
                "UringDriver dropped with outstanding cancel tickets"
            );
        }
        driver.control.clear_cancel_in_flight();
    }
}

impl<'a> Drop for UringDriver<'a> {
    fn drop(&mut self) {
        RingShutdown::run(self);
    }
}

impl<'a> sealed::Sealed for UringDriver<'a> {}

impl<'a> DriverRaw for UringDriver<'a> {
    type SlotSpec = UringSlotSpec;
    type Raw = UringRawHandle;

    fn reserve_op_raw(&mut self) -> UringResult<OpToken> {
        match self.operations.insert(OpEntry::new(UringOpState::new())) {
            Ok(OpHandle {
                index: id,
                generation,
            }) => {
                trace!(id, generation = generation.get(), "Reserved op slot");
                OpToken::from_registry_parts(id, generation).map_err(|err| {
                    UringError::InvalidState
                        .to_report()
                        .push_ctx("scope", "uring.driver.reserve_op")
                        .with_ctx("slot_index", id)
                        .with_ctx("generation", generation)
                        .with_ctx("op_token_error", format!("{err:?}"))
                        .attach_note("reserved op slot cannot be encoded as completion token")
                })
            }
            Err(_) => {
                Err(UringError::InvalidState.report("uring.driver.reserve_op", "OpRegistry full"))
            }
        }
    }

    fn slot_table_raw(&self) -> SharedSlotTable<Self::SlotSpec> {
        self.operations.shared.clone()
    }

    fn remote_cancel_sender_raw(&self) -> RemoteCancelSender {
        self.control.remote_cancel_sender()
    }

    fn try_recv_remote_cancel_request(&mut self) -> Option<CancelRequest> {
        self.control.try_recv_cancel()
    }

    fn slot_set_payload_raw(&mut self, token: OpToken, payload: UringUserPayload) {
        let _ = self
            .operations
            .with_slot_storage_mut(token, |_result, payload_cell, _sidecar| {
                *payload_cell = Some(payload);
            });
    }

    fn slot_take_payload_raw(&mut self, token: OpToken) -> Option<UringUserPayload> {
        self.operations
            .with_slot_storage_mut(token, |_result, payload_cell, _sidecar| payload_cell.take())
            .flatten()
    }

    fn release_op_slot_raw(&mut self, token: OpToken) {
        let cleanup_token = CompletionToken::user(token);
        if self
            .control
            .completion_cleanup_hints_mut()
            .remove(&cleanup_token)
            .is_some()
        {
            self.control
                .record(ControlPlaneEvent::CleanupHintRemove(cleanup_token));
        }
        let _ = self.operations.remove(token);
    }

    fn submit_op_raw(
        &mut self,
        token: OpToken,
        op_in: &mut Option<UringOp>,
    ) -> DriverSubmitResult<UringError> {
        let Some(op) = op_in.take() else {
            return DriverSubmitResult::failed(
                UringError::InvalidState
                    .report("driver.submit", "submit called with empty Option")
                    .push_ctx("scope", "uring.driver.submit")
                    .attach_note("submit called with empty Option"),
                SubmitStatus::Void,
            );
        };
        let op: UringOp = op;
        let strategy = op.descriptor().strategy;

        let Self {
            operations,
            control,
            registration,
            completion_diagnostics,
            ring,
            kernel_capabilities,
            submission,
            ..
        } = self;
        let mut context = SubmitPort::from_parts(
            operations,
            control,
            registration,
            completion_diagnostics,
            ring,
            kernel_capabilities,
        );
        submission.submit_operation_internal(&mut context, token, op, op_in, strategy)
    }

    fn drive_raw(&mut self, mode: DriveMode) -> UringResult<DriveOutcome> {
        let report = {
            let Self {
                operations,
                control,
                registration,
                completion_table,
                completion_diagnostics,
                capability_state,
                ring,
                kernel_capabilities,
                submission,
                lifecycle,
                completion,
                drive,
                ..
            } = self;
            let mut context = DriveContext::from_parts(
                operations,
                control,
                registration,
                completion_table,
                completion_diagnostics,
                capability_state,
                ring,
                kernel_capabilities,
            );
            drive
                .run(&mut context, submission, lifecycle, completion, mode)
                .push_ctx("scope", "uring.driver.drive")
                .attach_note("advance unified uring drive cycle")?
        };
        context::check_control_plane_invariants(&mut self.operations, &mut self.control)?;

        let next_timeout_hint = self.control.timers().next_deadline().map_err(|error| {
            UringError::InvalidState
                .report(
                    "uring.timer.deadline",
                    "timer wheel deadline could not be queried",
                )
                .with_ctx("timer_error", format!("{error:?}"))
        })?;

        Ok(DriveOutcome {
            next_timeout_hint,
            ready_completion: self.operations.shared.has_ready_completion(),
            in_flight: self.operations.has_active_ops(),
            pending_work: report.pending_work,
            budget_exhausted: report.budget_exhausted,
            needs_next_round: report.needs_next_round,
        })
    }

    fn completion_table_raw(&self) -> SharedCompletionTable<Self::SlotSpec> {
        self.completion_table.clone()
    }

    fn cancel_op_raw(&mut self, request: CancelRequest) -> UringResult<CancelSubmitOutcome> {
        let Self {
            operations,
            control,
            registration,
            completion_table,
            completion_diagnostics,
            capability_state,
            ring,
            kernel_capabilities,
            submission,
            lifecycle,
            completion,
            ..
        } = self;
        let mut context = DriveContext::from_parts(
            operations,
            control,
            registration,
            completion_table,
            completion_diagnostics,
            capability_state,
            ring,
            kernel_capabilities,
        );
        context.cancel_operation(lifecycle, completion, submission, request)
    }

    fn register_buffer_raw(
        &mut self,
        id: ChunkId,
        ptr: *const u8,
        len: usize,
    ) -> UringResult<BufferRegistrationStatus> {
        let submitter = self.ring.submitter();
        self.registration
            .register_buffer_internal(
                &submitter,
                self.completion_diagnostics.backend(),
                id,
                ptr,
                len,
            )
            .push_ctx("scope", "uring.driver.register_buffer")
            .attach_note("register buffer")
    }

    fn register_files_raw<'f>(
        &mut self,
        files: Vec<RegisterFd<'f, UringRawHandle>>,
    ) -> UringResult<Vec<IoFd>> {
        let submitter = self.ring.submitter();
        self.registration
            .register_files_internal(
                &submitter,
                self.completion_diagnostics.backend(),
                &mut self.kernel_capabilities,
                files,
            )
            .push_ctx("scope", "uring.driver.register_files")
            .attach_note("register files")
    }

    fn unregister_files_raw(&mut self, files: Vec<IoFd>) -> UringResult<()> {
        let submitter = self.ring.submitter();
        let ports = RegistrationPorts::new(&submitter, self.completion_diagnostics.backend());
        for fd in files {
            self.registration
                .unregister_fixed_fd(&ports, fd)
                .push_ctx("scope", "uring.driver.unregister_files")
                .attach_note("unregister fixed fd")?;
        }
        Ok(())
    }

    fn create_waker_raw(&self) -> Arc<dyn RemoteWaker<UringError>> {
        self.control.waker().create_waker()
    }

    /// 用刚建好的 worker 池注册 provided buffer 环。
    ///
    /// 注册失败**不是**驱动初始化失败：`IORING_REGISTER_PBUF_RING` 要 5.19，而仓库声明的
    /// 最低内核是 5.6。失败就把能力留在 `false`，门面层据此拒绝那些需要它的操作，其余一切
    /// 照旧。
    fn attach_buffer_pool_raw(&mut self, pool: AnyBufPool) -> UringResult<()> {
        if self.registration.attach_buffer_pool(
            &self.ring.submitter(),
            pool,
            self.ring_lifetime.token(),
        )? {
            let mut negotiated = self.capability_state.negotiated();
            negotiated.provided_buffers = self.registration.provided_buffers_enabled();
            self.capability_state.set_negotiated(negotiated);
            self.capability_state
                .enable_negotiated(DriverCapability::ProvidedBuffers);
        }
        Ok(())
    }

    fn capabilities_raw(&self) -> DriverCapabilities {
        self.capability_state.effective()
    }

    fn note_capability_rejected_raw(&mut self, capability: DriverCapability) {
        let effective = self.capability_state.effective();
        let enabled = match capability {
            DriverCapability::AcceptMulti => effective.accept_multi,
            DriverCapability::RecvMulti => effective.recv_multi,
            DriverCapability::ProvidedBuffers => effective.provided_buffers,
        };
        if enabled {
            debug!(
                ?capability,
                "kernel rejected an optional capability; disabling it"
            );
            self.capability_state
                .disable(capability, "kernel_rejected", None);
        }
    }
}

#[cfg(feature = "test-hooks")]
use veloq_driver_core::driver::test_hooks::{DriverTestHooks, RegisterFilesUpdateOutcome};

#[cfg(feature = "test-hooks")]
impl DriverTestHooks for UringDriver<'_> {
    fn debug_chunk_register_attempts(&self) -> u64 {
        self.registration
            .registration_stats()
            .chunk_register_attempts()
    }

    fn debug_chunk_register_failures(&self) -> u64 {
        self.registration
            .registration_stats()
            .chunk_register_failures()
    }

    fn debug_chunk_register_skipped_recent_failure(&self) -> u64 {
        self.registration
            .registration_stats()
            .chunk_register_skipped_recent_failure()
    }

    fn debug_submission_missing_chunk_info(&self) -> u64 {
        self.registration
            .registration_stats()
            .submission_missing_chunk_info()
    }

    fn debug_raw_buffer_fallbacks(&self) -> u64 {
        self.registration
            .registration_stats()
            .raw_buffer_fallbacks()
    }

    fn debug_fixed_buffers_available(&self) -> bool {
        self.registration.fixed_buffers_available()
    }

    fn debug_inject_register_buffers_update_failure(&mut self, errno: i32) {
        self.registration
            .inject_register_buffers_update_failure(errno);
    }

    fn debug_inject_register_buffers_update_unknown(&mut self, errno: i32) {
        self.registration
            .inject_register_buffers_update_unknown(errno);
    }

    fn debug_inject_register_buffers_update_sequence(&mut self, outcomes: &[Option<i32>]) {
        self.registration
            .inject_register_buffers_update_sequence(outcomes);
    }

    fn debug_inject_register_files_update_failure(&mut self, errno: i32) {
        self.registration
            .inject_register_files_update_failure(errno);
    }

    fn debug_inject_register_files_update_sequence(
        &mut self,
        outcomes: &[RegisterFilesUpdateOutcome],
    ) {
        self.registration
            .inject_register_files_update_sequence(outcomes);
    }

    fn debug_file_table_poisoned(&self) -> bool {
        self.registration.file_table_poisoned()
    }

    fn debug_register_files_update_outcomes_pending(&self) -> usize {
        self.registration.register_files_update_outcomes_pending()
    }

    fn debug_inject_bitset_set_failure(&mut self) {
        self.registration.inject_bitset_set_failure();
    }

    fn debug_chunk_registered(&self, chunk_id: usize) -> bool {
        let Ok(raw) = u16::try_from(chunk_id) else {
            return false;
        };
        self.registration
            .is_chunk_registered(ChunkId::from_raw(raw))
    }

    fn debug_inject_push_entry_failure(&mut self) {
        self.control.inject_push_entry_failure();
    }

    fn debug_control_plane_snapshot(&mut self) -> String {
        format!(
            "{:?}",
            context::control_plane_snapshot(&mut self.operations, &mut self.control)
        )
    }

    fn debug_control_plane_events(&mut self) -> Vec<String> {
        self.control
            .take_events()
            .into_iter()
            .map(|event| format!("{event:?}"))
            .collect()
    }
}
