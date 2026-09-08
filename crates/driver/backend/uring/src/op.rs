//! io_uring Platform-Specific Operation Definitions

use crate::{
    diagnostics::UringCompletionDiagnostics,
    driver::{CqeEnv, SqeEnv, UringOpState},
    error::{UringError, UringResult},
};
use io_uring::squeue;
use veloq_buf::heap::ChunkId;
use veloq_driver_core::{
    driver::{
        CompletionCleanupGuard, PlatformOp, SubmitTokenContext,
        registry::OpRegistry as CoreOpRegistry,
    },
    slot::{Slot as CoreSlot, SlotSpec as CoreSlotSpec},
};
use veloq_std::time::Duration;

mod payload;
mod spec;
mod submit;

pub(crate) use payload::UringOpPayload;
pub use payload::UringUserPayload;
pub(crate) use payload::{
    Accept, AcceptMulti, AcceptedSocket, Close, Connect, Fallocate, FallocateRaw, Fsync, FsyncRaw,
    OpSend, Open, ProvidedBuf, ReadFixed, ReadRaw, Recv, RecvMulti, RecvProvided, SendTo,
    SyncFileRange, SyncFileRangeRaw, Timeout, UdpConnect, UdpRecv, UdpRecvFrom, UdpSend, Wakeup,
    WriteFixed, WriteRaw,
};
pub(crate) use submit::sqe_with_fd;

// ============================================================================
// VTable Definition
// ============================================================================

/// Builds the SQE for one operation.
///
/// `env` is deliberately narrower than `&mut UringDriver`: the op and payload handed in are
/// borrowed out of the driver's slot registry, so an implementation that could reach the
/// registry again would alias them.
pub(crate) type MakeSqeFn = unsafe fn(
    op: &mut UringKernelOp,
    payload: &mut UringUserPayload,
    env: &SqeEnv<'_>,
    token: SubmitTokenContext,
) -> UringResult<squeue::Entry>;
pub(crate) type OnCompleteFn = unsafe fn(
    op: &mut UringKernelOp,
    payload: &mut UringUserPayload,
    result: i32,
) -> UringResult<usize>;
pub(crate) type CompletionCleanupFn =
    unsafe fn(op: &mut UringKernelOp, result: i32) -> CompletionCleanupGuard;
pub(crate) type OrphanCleanupFn =
    unsafe fn(op: &mut UringKernelOp, result: i32) -> CompletionCleanupGuard;
pub(crate) type GetTimeoutFn =
    unsafe fn(op: &UringKernelOp, payload: &UringUserPayload) -> Option<Duration>;
pub(crate) type ResolveChunksFn =
    unsafe fn(op: &UringKernelOp, payload: &UringUserPayload, chunks: &mut [ChunkId]) -> usize;

/// 为一条完成构造它自己的记录 payload。
///
/// 返回 `None` 表示这个操作的记录 payload **就是**提交 payload——绝大多数操作如此，完成
/// 路径照旧把 slot 里那个取走。返回 `Some` 表示两者不是一回事：
///
/// - multishot（`AcceptMulti`）：提交 payload 是监听 socket，必须留在 slot 里给内核后续
///   的完成用，每条完成的产物是一个新连接；
/// - provided buffer（`RecvProvided`）：提交时根本没有 buffer，它由内核在数据到达时才从
///   环里挑一个，所以产物只能在这里构造。
///
/// `env` 就是为后一种情形存在的：从环里取 buffer 要改 driver 的状态。
pub(crate) type RecordItemFn = unsafe fn(
    op: &mut UringKernelOp,
    payload: &mut UringUserPayload,
    result: i32,
    flags: u32,
    env: &mut CqeEnv<'_>,
) -> UringResult<Option<UringUserPayload>>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SubmissionStrategy {
    /// Submit a Standard SQE to the ring
    SubmitSqe,
    /// Handled by software timer wheel (no SQE submitted)
    SoftwareTimer,
}

pub(crate) struct OpVTable {
    pub(crate) make_sqe: MakeSqeFn,
    pub(crate) on_complete: OnCompleteFn,
    pub(crate) completion_cleanup: CompletionCleanupFn,
    pub(crate) orphan_cleanup: OrphanCleanupFn,
    pub(crate) strategy: SubmissionStrategy,
    pub(crate) get_timeout: GetTimeoutFn,
    pub(crate) resolve_chunks: ResolveChunksFn,
    pub(crate) record_item: RecordItemFn,
}

// ============================================================================
// UringKernelOp Struct & Payload (Type-Erased)
// ============================================================================

#[repr(C)]
pub struct UringKernelOp {
    /// Virtual Table for dynamic dispatch
    pub(crate) vtable: &'static OpVTable,

    /// Type-erased payload (kernel-side data)
    pub(crate) payload: UringOpPayload,
}

impl PlatformOp for UringKernelOp {
    type CleanupContext<'a> = i32;

    #[inline]
    fn completion_cleanup(&mut self, result: Self::CleanupContext<'_>) -> CompletionCleanupGuard {
        unsafe { (self.vtable.completion_cleanup)(self, result) }
    }

    #[inline]
    fn orphan_cleanup(&mut self, result: Self::CleanupContext<'_>) -> CompletionCleanupGuard {
        unsafe { (self.vtable.orphan_cleanup)(self, result) }
    }
}

pub type UringOp = UringKernelOp;

// ============================================================================
// Slot Registry Binding
// ============================================================================

pub enum UringSlotSpec {}

impl CoreSlotSpec for UringSlotSpec {
    type Op = UringOp;
    type UserPayload = UringUserPayload;
    type PlatformData = UringOpState;
    type Sidecar = ();
    type Error = UringError;
    type Completion = usize;
    type CompletionDiagnostics = UringCompletionDiagnostics;
}

pub(crate) type UringOpRegistry = CoreOpRegistry<UringSlotSpec>;
pub(crate) type Slot<'a, State> = CoreSlot<'a, State, UringSlotSpec>;

pub(crate) use veloq_driver_core::slot::{
    CheckedSlotView, Reserved, SlotMarker as SlotState, SlotRegistryExt as UringOpRegistryExt,
    SlotView,
};
