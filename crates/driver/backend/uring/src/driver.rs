use diagweave::prelude::*;
use io_uring::{IoUring, opcode};
use tracing::{debug, trace};
use veloq_buf::{AnyBufPool, BufferRegistrar, heap::ChunkId};
use veloq_std::{collections::VecDeque, format, ptr, string::String, sync::Arc, vec, vec::Vec};

use crate::{
    config::{IoFd, IoMode, RawHandle, UringConfig, UringRawHandle},
    diagnostics::{UringCompletionDiagnostics, UringCompletionDiagnosticsSnapshot},
    driver::control::{
        ControlInvariantError, ControlPlaneEvent, ControlPlaneSnapshot, ControlTokenSnapshot,
    },
    driver::lifecycle::SubmissionPhase,
    error::{UringError, UringResult},
    op::{
        CheckedSlotView, SlotView, SubmissionStrategy, UringOp, UringOpRegistry,
        UringOpRegistryExt, UringSlotSpec, UringUserPayload,
    },
};
use veloq_driver_core::driver::{
    BufferRegistrationStatus, CancelCompletionId, CancelRequest, CancelSubmitOutcome,
    CompletionToken, DriveMode, DriveOutcome, DriverCapabilities, DriverCapability,
    DriverCompletionDiagnostics, DriverCompletionDiagnosticsSnapshot, DriverRaw,
    DriverSubmitResult, OpToken, RegisterFd, RemoteCancelSender, RemoteWaker,
    SharedCompletionTable, SharedSlotTable, SubmitStatus,
    registry::{OpEntry, OpHandle},
    sealed,
};

mod completion;
mod control;
mod env;
mod lifecycle;
mod registration;
mod submission;

pub(crate) use control::{
    PendingCancel, UringCancelManager, UringControlPlane, UringTimerWheel, UringWakerManager,
};
pub(crate) use env::{CompletionControlView, CqeEnv, SqeEnv};
pub use lifecycle::UringOpState;
pub use registration::ProvidedBufStats;
pub(crate) use registration::{
    FileTable, MAX_CHUNKS, PROVIDED_BUF_GROUP_ID, ProvidedBufGroup, RegisteredFileEntry, SqeFd,
    UringBufferRegistry, UringRegistrationStats,
};

/// 从 opcode 探测结果得出乐观的能力集合。
///
/// 「乐观」是关键：opcode 在场只说明**可能**支持 multishot 变体，真正的判定推迟到第一次
/// 提交（见 [`Driver::note_capability_rejected`]）。`provided_buffers` 不在此列——它不靠
/// 猜：`register_buf_ring` 成功与否就是答案，而那要等池到位（见
/// [`Driver::attach_buffer_pool`]），所以这里先记 `false`。
fn probe_capabilities(probe: &io_uring::Probe) -> DriverCapabilities {
    DriverCapabilities {
        accept_multi: probe.is_supported(opcode::Accept::CODE),
        recv_multi: probe.is_supported(opcode::Recv::CODE),
        provided_buffers: false,
    }
}

pub struct UringDriver<'a> {
    // Rust 按声明顺序从上到下析构字段。`ring` 必须在 `buffer_registry` 之前声明：
    // Drop 函数体只尝试显式反注册；如果 syscall 失败，group 会留在 registry 中。函数体
    // 返回后，`IoUring` 先析构并关闭 ring fd，内核随之不再持有 provided-buffer ring；
    // 之后 registry 才释放 `FixedBuf` 和 `RingMapping`。这是失败路径的有意生命周期兜底。
    pub(crate) ring: IoUring,
    pub(crate) ops: UringOpRegistry,
    pub(crate) completion_diagnostics: DriverCompletionDiagnostics<UringCompletionDiagnostics>,
    pub(crate) completion_table: SharedCompletionTable<UringSlotSpec>,

    pub(crate) control: UringControlPlane,
    pub(crate) buffer_registry: UringBufferRegistry<'a>,

    /// Reused across `process_completions_internal` calls so draining the CQ never allocates.
    pub(crate) cqe_buffer: Vec<(u64, i32, u32)>,
    pub(crate) file_table: FileTable,
    #[cfg(feature = "test-hooks")]
    pub(crate) register_files_update_outcomes: VecDeque<RegisterFilesUpdateOutcome>,
    pub(crate) capabilities: DriverCapabilities,
}

impl<'a> UringDriver<'a> {
    fn new_internal(
        config: impl AsRef<UringConfig>,
        registrar: &'a (dyn BufferRegistrar + 'a),
    ) -> UringResult<Self> {
        let config = config.as_ref();
        let mut builder = IoUring::builder();

        builder
            .setup_coop_taskrun()
            .setup_single_issuer()
            .setup_defer_taskrun();

        if let IoMode::Polling(idle_ms) = config.mode {
            builder.setup_sqpoll(idle_ms.get());
        }

        let entries = config.entries.get();
        let ring = builder
            .build(entries)
            .or_else(|e| {
                if e.raw_os_error() == Some(libc::EINVAL) {
                    IoUring::new(entries)
                } else {
                    Err(e)
                }
            })
            .map_err(|e| UringError::DriverInit.io_report("driver.new.build_ring", e))?;

        let ops = UringOpRegistry::new(entries as usize);
        let completion_table: SharedCompletionTable<UringSlotSpec> = ops.shared.clone();
        let completion_diagnostics = ops.shared.completion_diagnostics();

        let waker = UringWakerManager::new()?;

        // opcode 探测只能回答「这个 opcode 存在吗」，回答不了「它的 multishot 变体存在
        // 吗」——那是同一个 opcode 上后加的标志位。所以这里只排除掉真正缺 opcode 的内核，
        // 剩下的由第一次提交去问（`note_capability_rejected`）。
        let mut ring_probe = io_uring::Probe::new();
        if ring.submitter().register_probe(&mut ring_probe).is_err() {
            debug!("IORING_REGISTER_PROBE unavailable; assuming no optional opcodes");
            ring_probe = io_uring::Probe::new();
        }

        debug!("Initalized UringDriver with {} entries", entries);

        let mut driver = Self {
            ring,
            ops,
            completion_diagnostics,
            completion_table,
            control: UringControlPlane::new(waker),
            buffer_registry: UringBufferRegistry::new(
                config.registration_mode,
                config.provided_buffers,
                registrar,
            ),
            cqe_buffer: Vec::with_capacity(entries as usize),
            file_table: FileTable::new(config.file_table_capacity, config.file_table_exhaustion),
            #[cfg(feature = "test-hooks")]
            register_files_update_outcomes: VecDeque::new(),
            capabilities: probe_capabilities(&ring_probe),
        };

        driver.submit_waker()?;

        // Sparse registration
        let iovecs = vec![
            libc::iovec {
                iov_base: ptr::null_mut(),
                iov_len: 0
            };
            MAX_CHUNKS
        ];

        match unsafe { driver.ring.submitter().register_buffers(&iovecs) } {
            Ok(_) => {
                driver
                    .buffer_registry
                    .set_fixed_buffers_available(true, None);
                debug!(
                    registration_mode = config.registration_mode.as_str(),
                    fixed_buffers_available = true,
                    fallback = false,
                    "registered sparse fixed-buffer table"
                );
            }
            Err(e) => {
                let errno = e.raw_os_error();
                driver
                    .buffer_registry
                    .set_fixed_buffers_available(false, errno);
                tracing::warn!(
                    registration_mode = config.registration_mode.as_str(),
                    errno = ?errno,
                    fixed_buffers_available = false,
                    fallback = !config.registration_mode.is_strict(),
                    error = %e,
                    "sparse fixed-buffer registration unavailable"
                );
            }
        }

        Ok(driver)
    }

    pub fn new(
        config: impl AsRef<UringConfig>,
        registrar: &'a (dyn BufferRegistrar + 'a),
    ) -> UringResult<Self> {
        Self::new_internal(config, registrar).attach_note("create uring driver")
    }

    fn has_active_ops_internal(&self) -> bool {
        self.ops.has_active_ops()
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn control_plane_snapshot(&mut self) -> UringResult<ControlPlaneSnapshot> {
        let active_tokens: Vec<OpToken> = self.ops.local_active_tokens().collect();
        let mut active = Vec::with_capacity(self.ops.active_count());
        for token in active_tokens {
            let (slot, submission_phase, timer_id) = match self.ops.checked_slot_view(token)? {
                CheckedSlotView::Valid(SlotView::Reserved(slot)) => (
                    slot.snapshot(),
                    slot.platform().control.submission,
                    slot.platform().timer_id,
                ),
                CheckedSlotView::Valid(SlotView::InFlightWaiting(slot)) => (
                    slot.snapshot(),
                    slot.platform().control.submission,
                    slot.platform().timer_id,
                ),
                CheckedSlotView::Valid(SlotView::InFlightOrphaned(slot)) => (
                    slot.snapshot(),
                    slot.platform().control.submission,
                    slot.platform().timer_id,
                ),
                CheckedSlotView::Empty(_) => {
                    return Err(UringError::InvalidState.report(
                        "uring.control_plane.snapshot",
                        "local active token has an idle slot view",
                    ));
                }
                CheckedSlotView::Missing { .. } | CheckedSlotView::Stale(_) => {
                    return Err(UringError::InvalidState.report(
                        "uring.control_plane.snapshot",
                        "active token disappeared while taking a control-plane snapshot",
                    ));
                }
            };
            active.push(ControlTokenSnapshot {
                token,
                slot,
                submission_phase,
                timer_id,
                has_cleanup_hint: self
                    .control
                    .completion_cleanup_hints
                    .contains_key(&CompletionToken::user(token)),
            });
        }

        let mut backlog_tokens: Vec<OpToken> = self
            .control
            .backlog
            .entries()
            .into_iter()
            .map(|entry| entry.token)
            .collect();
        backlog_tokens.sort_by_key(|token| (token.index(), token.generation().get()));
        let mut pending_cancel_targets: Vec<OpToken> =
            self.control.cancellations.pending_targets().collect();
        pending_cancel_targets.sort_by_key(|token| (token.index(), token.generation().get()));
        let mut in_flight_cancel_targets: Vec<(CancelCompletionId, OpToken)> =
            self.control.cancellations.in_flight_targets().collect();
        in_flight_cancel_targets.sort_by_key(|(id, _)| id.raw());
        let mut timer_tokens = self.control.timer_entries();
        timer_tokens.sort_by_key(|(task_id, _)| task_id.raw());
        let mut cleanup_hint_tokens: Vec<CompletionToken> = self
            .control
            .completion_cleanup_hints
            .keys()
            .copied()
            .collect();
        cleanup_hint_tokens.sort_by_key(|token| token.raw());
        let mut quarantined_tokens = self.control.quarantined_tokens();
        quarantined_tokens.sort_by_key(|token| (token.index(), token.generation().get()));

        Ok(ControlPlaneSnapshot {
            active_tokens: active,
            backlog_tokens,
            pending_cancel_targets,
            in_flight_cancel_targets,
            timer_tokens,
            cleanup_hint_tokens,
            quarantined_tokens,
        })
    }

    #[cfg(any(test, feature = "test-hooks"))]
    fn control_invariant_failure(&mut self, error: ControlInvariantError) -> UringResult<()> {
        self.control
            .record(ControlPlaneEvent::InvariantViolation(error));
        Err(UringError::InvalidState
            .report("uring.control_plane.invariant", format!("{error:?}"))
            .attach_note("control-plane batch invariant check failed"))
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn check_control_plane_invariants(&mut self) -> UringResult<()> {
        let snapshot = self.control_plane_snapshot()?;
        if snapshot.active_tokens.len() != self.ops.active_count() {
            return self.control_invariant_failure(ControlInvariantError::ActiveCountMismatch {
                registry: self.ops.active_count(),
                observed: snapshot.active_tokens.len(),
            });
        }

        let mut seen_backlog = veloq_std::collections::HashSet::default();
        for token in &snapshot.backlog_tokens {
            if !seen_backlog.insert(*token) {
                return self.control_invariant_failure(ControlInvariantError::BacklogDuplicate {
                    token: *token,
                });
            }
            if !self.ops.is_current_active(*token) {
                return self.control_invariant_failure(ControlInvariantError::BacklogInactive {
                    token: *token,
                });
            }
        }

        for active in &snapshot.active_tokens {
            if self.control.is_timer_quarantined(active.token) {
                continue;
            }
            if let Some(task_id) = active.timer_id {
                if active.submission_phase != SubmissionPhase::TimerArmed {
                    return self.control_invariant_failure(
                        ControlInvariantError::TimerStateMismatch {
                            token: active.token,
                            state: active.submission_phase,
                        },
                    );
                }
                if self.control.timer_for(active.token) != Some(task_id) {
                    return self.control_invariant_failure(
                        ControlInvariantError::TimerSlotMismatch {
                            task_id,
                            expected: active.token,
                            actual: self.control.timer_for(active.token).and_then(|id| {
                                self.control
                                    .timer_entries()
                                    .into_iter()
                                    .find_map(|(entry_id, token)| (entry_id == id).then_some(token))
                            }),
                        },
                    );
                }
            } else if active.submission_phase == SubmissionPhase::TimerArmed {
                return self.control_invariant_failure(ControlInvariantError::TimerStateMismatch {
                    token: active.token,
                    state: active.submission_phase,
                });
            }

            if active.has_cleanup_hint {
                if !matches!(
                    active.submission_phase,
                    SubmissionPhase::SqeStaged | SubmissionPhase::KernelOutstanding
                ) {
                    return self.control_invariant_failure(
                        ControlInvariantError::CleanupHintNotKernelSubmitted {
                            token: active.token,
                            state: active.submission_phase,
                        },
                    );
                }
                if !self.control.has_staged_kernel_token(active.token) {
                    return self.control_invariant_failure(
                        ControlInvariantError::CleanupHintNotStaged {
                            token: active.token,
                        },
                    );
                }
            }
        }

        for cleanup_token in &snapshot.cleanup_hint_tokens {
            let Some(token) = cleanup_token.op_token() else {
                continue;
            };
            let Some(active) = snapshot
                .active_tokens
                .iter()
                .find(|active| active.token == token)
            else {
                return self.control_invariant_failure(
                    ControlInvariantError::CleanupHintInactive { token },
                );
            };
            if !active.has_cleanup_hint {
                return self.control_invariant_failure(
                    ControlInvariantError::CleanupHintInactive { token },
                );
            }
        }

        for (task_id, token) in &snapshot.timer_tokens {
            let Some(active) = snapshot
                .active_tokens
                .iter()
                .find(|active| active.token == *token)
            else {
                return self.control_invariant_failure(ControlInvariantError::TimerSlotMismatch {
                    task_id: *task_id,
                    expected: *token,
                    actual: None,
                });
            };
            if active.timer_id != Some(*task_id) {
                return self.control_invariant_failure(ControlInvariantError::TimerSlotMismatch {
                    task_id: *task_id,
                    expected: *token,
                    actual: active.timer_id.and_then(|id| {
                        snapshot
                            .timer_tokens
                            .iter()
                            .find_map(|(entry_id, entry_token)| {
                                (*entry_id == id).then_some(*entry_token)
                            })
                    }),
                });
            }
        }

        for target in &snapshot.pending_cancel_targets {
            if !self.ops.is_current_active(*target) {
                return self.control_invariant_failure(ControlInvariantError::BacklogInactive {
                    token: *target,
                });
            }
        }
        Ok(())
    }

    #[cfg(not(any(test, feature = "test-hooks")))]
    #[inline]
    pub(crate) fn check_control_plane_invariants(&mut self) -> UringResult<()> {
        Ok(())
    }

    /// provided buffer 环的运行期统计，`None` 表示这个 driver 没有环。
    pub fn provided_buf_stats(&self) -> Option<ProvidedBufStats> {
        self.buffer_registry.provided_buf_stats()
    }

    /// Returns a point-in-time snapshot of completion and backend cleanup diagnostics.
    pub fn completion_diagnostics_snapshot(
        &self,
    ) -> DriverCompletionDiagnosticsSnapshot<UringCompletionDiagnosticsSnapshot> {
        self.completion_diagnostics.snapshot()
    }

    pub(crate) fn rebuild_waker_fd(&mut self) -> UringResult<()> {
        if self.file_table.is_poisoned() {
            return Err(self.file_table.poisoned_report(
                "driver.rebuild_waker_fd",
                self.control.waker.registered_fd(),
            ));
        }
        let new_fd = UringWakerManager::create_event_fd("driver.rebuild_waker_fd.eventfd")?;
        let raw = RawHandle::new(UringRawHandle::for_file(new_fd.fd.raw().as_fd()));
        let registered_fd = self.control.waker.registered_fd();
        let state = self.control.waker.state();

        state.with_lock(|current_fd| {
            // Keep fd replacement and remote wake writes under the same mutex. A pending
            // notification is copied to the new eventfd before the shared fd is swapped, so a
            // failed copy leaves both the old fd and the notification state intact.
            if self.control.waker.has_pending_notification() {
                UringWakerManager::write_event_fd(&new_fd)?;
            }

            match registered_fd {
                // A registered waker keeps its slot: only the kernel table entry changes, so the
                // descriptor stays valid across the rebuild.
                Some(fd @ IoFd::Registered { .. }) => self.replace_registered_fixed_fd(fd, raw)?,
                // A direct descriptor *is* the fd, so a rebuilt eventfd needs a new one. Only the
                // driver holds this descriptor, so replacing it invalidates nothing.
                Some(IoFd::Direct(_)) => {
                    self.control
                        .waker
                        .set_registered_fd(Some(IoFd::direct(raw.raw())));
                }
                Some(IoFd::OwnedDirect { .. }) => {
                    return Err(UringError::InvalidState
                        .report(
                            "driver.rebuild_waker_fd",
                            "owned direct waker descriptors are unsupported",
                        )
                        .with_ctx("fd", format!("{:?}", registered_fd.unwrap())));
                }
                None => {}
            }

            *current_fd = new_fd.clone();
            Ok(())
        })
    }
}

impl<'a> Drop for UringDriver<'a> {
    fn drop(&mut self) {
        if self.ops.has_active_ops() {
            tracing::warn!("UringDriver dropped with active in-flight operations");
        }
        // 正常关闭优先显式反注册。失败时 release_provided_buffers 会恢复 group 的所有权，
        // 不在仍存活的 IoUring 前释放映射；随后依靠上面的字段声明顺序完成最终兜底。
        if let Err(report) = self
            .buffer_registry
            .release_provided_buffers(&self.ring.submitter())
        {
            tracing::warn!(
                bgid = PROVIDED_BUF_GROUP_ID,
                report = ?report,
                "failed to unregister provided buffer ring; retaining it until io_uring drops"
            );
        }
    }
}

impl<'a> sealed::Sealed for UringDriver<'a> {}

impl<'a> DriverRaw for UringDriver<'a> {
    type SlotSpec = UringSlotSpec;
    type Raw = UringRawHandle;

    fn reserve_op_raw(&mut self) -> UringResult<OpToken> {
        match self.ops.insert(OpEntry::new(UringOpState::new())) {
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
        self.ops.shared.clone()
    }

    fn remote_cancel_sender_raw(&self) -> RemoteCancelSender {
        self.control.cancellations.remote_sender()
    }

    fn try_recv_remote_cancel_request(&mut self) -> Option<CancelRequest> {
        self.control.cancellations.try_recv_remote()
    }

    fn slot_set_payload_raw(&mut self, token: OpToken, payload: UringUserPayload) {
        let _ = self
            .ops
            .with_slot_storage_mut(token, |_result, payload_cell, _sidecar| {
                *payload_cell = Some(payload);
            });
    }

    fn slot_take_payload_raw(&mut self, token: OpToken) -> Option<UringUserPayload> {
        self.ops
            .with_slot_storage_mut(token, |_result, payload_cell, _sidecar| payload_cell.take())
            .flatten()
    }

    fn release_op_slot_raw(&mut self, token: OpToken) {
        let cleanup_token = CompletionToken::user(token);
        if self
            .control
            .completion_cleanup_hints
            .remove(&cleanup_token)
            .is_some()
        {
            self.control
                .record(ControlPlaneEvent::CleanupHintRemove(cleanup_token));
        }
        let _ = self.ops.remove(token);
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
        let strategy = op.vtable.strategy;

        match strategy {
            SubmissionStrategy::SubmitSqe => self.submit_sqe_internal(token, op, op_in),
            SubmissionStrategy::SoftwareTimer => self.submit_timer_internal(token, op, op_in),
        }
    }

    fn drive_raw(&mut self, mode: DriveMode) -> UringResult<DriveOutcome> {
        match mode {
            DriveMode::Poll => {
                self.poll_nonblocking_internal()
                    .push_ctx("scope", "uring.driver.drive.poll")
                    .attach_note("poll completions")?;
            }
            DriveMode::Wait { timeout } => {
                self.completion_diagnostics.backend().inc_wait_enter();
                self.wait_internal(timeout)
                    .push_ctx("scope", "uring.driver.drive.wait")
                    .attach_note("wait for completions")?;
            }
        }

        Ok(DriveOutcome {
            next_timeout_hint: self.control.timers.next_timeout(),
            ready_completion: self.ops.shared.has_ready_completion(),
            in_flight: self.has_active_ops_internal(),
        })
    }

    fn completion_table_raw(&self) -> SharedCompletionTable<Self::SlotSpec> {
        self.completion_table.clone()
    }

    fn cancel_op_raw(&mut self, request: CancelRequest) -> UringResult<CancelSubmitOutcome> {
        self.cancel_op_internal(request)
    }

    fn register_buffer_raw(
        &mut self,
        id: ChunkId,
        ptr: *const u8,
        len: usize,
    ) -> UringResult<BufferRegistrationStatus> {
        self.register_buffer_internal(id, ptr, len)
            .push_ctx("scope", "uring.driver.register_buffer")
            .attach_note("register buffer")
    }

    fn register_files_raw<'f>(
        &mut self,
        files: Vec<RegisterFd<'f, UringRawHandle>>,
    ) -> UringResult<Vec<IoFd>> {
        self.register_files_internal(files)
            .push_ctx("scope", "uring.driver.register_files")
            .attach_note("register files")
    }

    fn unregister_files_raw(&mut self, files: Vec<IoFd>) -> UringResult<()> {
        for fd in files {
            self.unregister_fixed_fd(fd)
                .push_ctx("scope", "uring.driver.unregister_files")
                .attach_note("unregister fixed fd")?;
        }
        Ok(())
    }

    fn create_waker_raw(&self) -> Arc<dyn RemoteWaker<UringError>> {
        self.control.waker.create_waker()
    }

    /// 用刚建好的 worker 池注册 provided buffer 环。
    ///
    /// 注册失败**不是**驱动初始化失败：`IORING_REGISTER_PBUF_RING` 要 5.19，而仓库声明的
    /// 最低内核是 5.6。失败就把能力留在 `false`，门面层据此拒绝那些需要它的操作，其余一切
    /// 照旧。
    fn attach_buffer_pool_raw(&mut self, pool: AnyBufPool) -> UringResult<()> {
        if self
            .buffer_registry
            .attach_buffer_pool(&self.ring.submitter(), pool)?
        {
            self.capabilities.provided_buffers = true;
        }
        Ok(())
    }

    fn capabilities_raw(&self) -> DriverCapabilities {
        self.capabilities
    }

    fn note_capability_rejected_raw(&mut self, capability: DriverCapability) {
        let slot = match capability {
            DriverCapability::AcceptMulti => &mut self.capabilities.accept_multi,
            DriverCapability::RecvMulti => &mut self.capabilities.recv_multi,
            DriverCapability::ProvidedBuffers => &mut self.capabilities.provided_buffers,
        };
        if *slot {
            debug!(
                ?capability,
                "kernel rejected an optional capability; disabling it"
            );
            *slot = false;
        }
    }
}

#[cfg(feature = "test-hooks")]
use veloq_driver_core::driver::test_hooks::{DriverTestHooks, RegisterFilesUpdateOutcome};

#[cfg(feature = "test-hooks")]
impl DriverTestHooks for UringDriver<'_> {
    fn debug_chunk_register_attempts(&self) -> u64 {
        self.buffer_registry.stats().chunk_register_attempts
    }

    fn debug_chunk_register_failures(&self) -> u64 {
        self.buffer_registry.stats().chunk_register_failures
    }

    fn debug_chunk_register_skipped_recent_failure(&self) -> u64 {
        self.buffer_registry
            .stats()
            .chunk_register_skipped_recent_failure
    }

    fn debug_submission_missing_chunk_info(&self) -> u64 {
        self.buffer_registry.stats().submission_missing_chunk_info
    }

    fn debug_raw_buffer_fallbacks(&self) -> u64 {
        self.buffer_registry.stats().raw_buffer_fallbacks
    }

    fn debug_fixed_buffers_available(&self) -> bool {
        self.buffer_registry.fixed_buffers_available()
    }

    fn debug_inject_register_buffers_update_failure(&mut self, errno: i32) {
        self.buffer_registry
            .inject_register_buffers_update_failure(errno);
    }

    fn debug_inject_register_buffers_update_sequence(&mut self, outcomes: &[Option<i32>]) {
        self.buffer_registry
            .inject_register_buffers_update_sequence(outcomes);
    }

    fn debug_inject_register_files_update_failure(&mut self, errno: i32) {
        self.register_files_update_outcomes
            .push_back(RegisterFilesUpdateOutcome::Error(errno));
    }

    fn debug_inject_register_files_update_sequence(
        &mut self,
        outcomes: &[RegisterFilesUpdateOutcome],
    ) {
        self.register_files_update_outcomes.clear();
        self.register_files_update_outcomes
            .extend(outcomes.iter().copied());
    }

    fn debug_file_table_poisoned(&self) -> bool {
        self.file_table.is_poisoned()
    }

    fn debug_register_files_update_outcomes_pending(&self) -> usize {
        self.register_files_update_outcomes.len()
    }

    fn debug_inject_bitset_set_failure(&mut self) {
        self.buffer_registry.inject_bitset_set_failure();
    }

    fn debug_chunk_registered(&self, chunk_id: usize) -> bool {
        let Ok(raw) = u16::try_from(chunk_id) else {
            return false;
        };
        self.buffer_registry
            .is_chunk_registered(ChunkId::from_raw(raw))
    }

    fn debug_inject_push_entry_failure(&mut self) {
        self.control.push_entry_failure = true;
    }

    fn debug_control_plane_snapshot(&mut self) -> String {
        format!("{:?}", self.control_plane_snapshot())
    }

    fn debug_control_plane_events(&mut self) -> Vec<String> {
        self.control
            .take_events()
            .into_iter()
            .map(|event| format!("{event:?}"))
            .collect()
    }
}
