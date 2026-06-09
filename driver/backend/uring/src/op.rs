//! io_uring Platform-Specific Operation Definitions

use crate::diagnostics::UringCompletionDiagnostics;
use crate::driver::{UringDriver, UringOpState};
use crate::error::{UringDriverResult as DriverResult, UringError};
use io_uring::squeue;
use std::time::Duration;
use veloq_driver_core::driver::registry::OpRegistry as CoreOpRegistry;
use veloq_driver_core::driver::{CompletionCleanupGuard, PlatformOp, SubmitTokenContext};
use veloq_driver_core::slot::{Slot as CoreSlot, SlotSpec as CoreSlotSpec};

mod payload;
mod spec;
mod submit;

pub(crate) use payload::UringOpPayload;
pub use payload::UringUserPayload;
pub(crate) use payload::{
    Accept, Close, Connect, Fallocate, FallocateRaw, Fsync, FsyncRaw, OpSend, Open, ReadFixed,
    ReadRaw, Recv, SendTo, SyncFileRange, SyncFileRangeRaw, Timeout, UdpConnect, UdpRecv,
    UdpRecvFrom, UdpSend, Wakeup, WriteFixed, WriteRaw,
};

// ============================================================================
// VTable Definition
// ============================================================================

pub(crate) type MakeSqeFn = unsafe fn(
    op: &mut UringKernelOp,
    payload: &mut UringUserPayload,
    driver: &mut UringDriver,
    token: SubmitTokenContext,
) -> DriverResult<squeue::Entry>;
pub(crate) type OnCompleteFn = unsafe fn(
    op: &mut UringKernelOp,
    payload: &mut UringUserPayload,
    result: i32,
) -> DriverResult<usize>;
pub(crate) type CompletionCleanupFn =
    unsafe fn(op: &mut UringKernelOp, result: i32) -> CompletionCleanupGuard;
pub(crate) type OrphanCleanupFn =
    unsafe fn(op: &mut UringKernelOp, result: i32) -> CompletionCleanupGuard;
pub(crate) type GetTimeoutFn =
    unsafe fn(op: &UringKernelOp, payload: &UringUserPayload) -> Option<Duration>;
pub(crate) type ResolveChunksFn = unsafe fn(
    op: &UringKernelOp,
    payload: &UringUserPayload,
    chunks: &mut [veloq_buf::heap::ChunkId],
) -> usize;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SubmissionStrategy {
    /// Submit a Standard SQE to the ring
    SubmitSqe,
    /// Handled by software timer wheel (no SQE submitted)
    SoftwareTimer,
    /// Only for background operations (e.g. Close)
    BackgroundOnly,
}

pub(crate) struct OpVTable {
    pub(crate) make_sqe: MakeSqeFn,
    pub(crate) on_complete: OnCompleteFn,
    pub(crate) completion_cleanup: CompletionCleanupFn,
    pub(crate) orphan_cleanup: OrphanCleanupFn,
    pub(crate) strategy: SubmissionStrategy,
    pub(crate) get_timeout: GetTimeoutFn,
    pub(crate) resolve_chunks: ResolveChunksFn,
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
