use diagweave::prelude::*;
use io_uring::{IoUring, opcode};
use tracing::{debug, trace};
use veloq_buf::{AnyBufPool, BufferRegistrar, heap::ChunkId};
use veloq_std::{collections::VecDeque, format, ptr, string::ToString, sync::Arc, vec, vec::Vec};

use crate::{
    config::{IoFd, IoMode, RawHandle, UringConfig, UringRawHandle},
    diagnostics::UringCompletionDiagnostics,
    error::{UringError, UringResult},
    op::{SubmissionStrategy, UringOp, UringOpRegistry, UringSlotSpec, UringUserPayload},
};
use veloq_driver_core::driver::{
    CancelRequest, CancelSubmitOutcome, DriveMode, DriveOutcome, DriverCapabilities,
    DriverCapability, DriverCompletionDiagnostics, DriverRaw, DriverSubmitResult, OpToken,
    RegisterFd, RemoteCancelSender, RemoteWaker, SharedCompletionTable, SharedSlotTable,
    SubmitStatus,
    registry::{OpEntry, OpHandle},
    sealed,
};

mod completion;
mod control;
mod env;
mod lifecycle;
mod registration;
mod submission;

pub(crate) use control::{PendingCancel, UringCancelManager, UringTimerWheel, UringWakerManager};
pub(crate) use env::{CqeEnv, SqeEnv};
pub use lifecycle::UringOpState;
pub use registration::ProvidedBufStats;
pub(crate) use registration::{
    FileTable, MAX_CHUNKS, ProvidedBufGroup, RegisteredFileEntry, SqeFd, UringBufferRegistry,
    UringRegistrationStats,
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
    // Rust 按声明顺序从上到下析构字段。
    // `ring` 必须在 `ops`（以及其它持有 buffer/slot 的字段）之前声明：
    // 当 `UringDriver` 被 drop 时，`ring` (IoUring) 最先被析构并关闭 ring file descriptor，
    // 内核随之取消所有在途 Operation 并释放对用户 Buffer 的引用；
    // 之后 `ops` 才会析构并释放底层 `LocalSlot` 内存，避免内存提前释放导致的 Use-After-Free。
    pub(crate) ring: IoUring,
    pub(crate) ops: UringOpRegistry,
    pub(crate) backlog: VecDeque<OpToken>,
    pub(crate) completion_diagnostics: DriverCompletionDiagnostics<UringCompletionDiagnostics>,
    pub(crate) completion_table: SharedCompletionTable<UringSlotSpec>,

    pub(crate) cancellations: UringCancelManager,
    pub(crate) waker: UringWakerManager,
    pub(crate) timers: UringTimerWheel,
    pub(crate) buffer_registry: UringBufferRegistry<'a>,

    /// Reused across `process_completions_internal` calls so draining the CQ never allocates.
    pub(crate) cqe_buffer: Vec<(u64, i32, u32)>,
    pub(crate) file_table: FileTable,
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
            backlog: VecDeque::new(),
            completion_diagnostics,
            completion_table,
            cancellations: UringCancelManager::new(),
            waker,
            timers: UringTimerWheel::new(),
            buffer_registry: UringBufferRegistry::new(
                config.registration_mode,
                config.provided_buffers,
                registrar,
            ),
            cqe_buffer: Vec::with_capacity(entries as usize),
            file_table: FileTable::new(config.file_table_capacity, config.file_table_exhaustion),
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

        if let Err(e) = unsafe { driver.ring.submitter().register_buffers(&iovecs) } {
            tracing::warn!("Failed to register sparse buffers: {}", e);
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

    /// provided buffer 环的运行期统计，`None` 表示这个 driver 没有环。
    pub fn provided_buf_stats(&self) -> Option<ProvidedBufStats> {
        self.buffer_registry.provided_buf_stats()
    }

    pub(crate) fn rebuild_waker_fd(&mut self) -> UringResult<()> {
        let new_fd = UringWakerManager::create_event_fd("driver.rebuild_waker_fd.eventfd")?;
        let raw = RawHandle::new(UringRawHandle::for_file(new_fd.fd.raw().as_fd()));
        match self.waker.registered_fd() {
            // A registered waker keeps its slot: only the kernel table entry changes, so the
            // descriptor stays valid across the rebuild.
            Some(fd @ IoFd::Registered { .. }) => self.replace_registered_fixed_fd(fd, raw)?,
            // A direct descriptor *is* the fd, so a rebuilt eventfd needs a new one. Only the
            // driver holds this descriptor, so replacing it invalidates nothing.
            Some(IoFd::Direct(_)) => self.waker.set_registered_fd(Some(IoFd::direct(raw.raw()))),
            None => {}
        }
        let _old_fd = self.waker.replace_state_fd(new_fd);
        Ok(())
    }
}

impl<'a> Drop for UringDriver<'a> {
    fn drop(&mut self) {
        if self.ops.has_active_ops() {
            tracing::warn!("UringDriver dropped with active in-flight operations");
        }
        // 顺序不能反：先反注册，内核才不会再碰那段环内存和里面的 buffer；然后 `group`
        // 落地析构，`FixedBuf` 各自回池、映射还给内核。`Drop::drop` 在任何字段析构之前
        // 跑完，所以这里不依赖字段声明顺序。
        self.buffer_registry
            .release_provided_buffers(&self.ring.submitter());
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
        self.cancellations.remote_sender()
    }

    fn try_recv_remote_cancel_request(&mut self) -> Option<CancelRequest> {
        self.cancellations.try_recv_remote()
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
            next_timeout_hint: self.timers.next_timeout(),
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

    fn register_chunk_raw(&mut self, id: ChunkId, ptr: *const u8, len: usize) -> UringResult<()> {
        self.register_chunk_internal(id, ptr, len)
            .push_ctx("scope", "uring.driver.register_chunk")
            .with_ctx("driver_error_kind", UringError::Registration.to_string())
            .attach_note("register chunk")
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
        self.waker.create_waker()
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
use veloq_driver_core::driver::test_hooks::DriverTestHooks;

#[cfg(feature = "test-hooks")]
impl DriverTestHooks for UringDriver<'_> {
    fn debug_chunk_register_attempts(&self) -> u64 {
        self.buffer_registry.stats().chunk_register_attempts
    }
}
