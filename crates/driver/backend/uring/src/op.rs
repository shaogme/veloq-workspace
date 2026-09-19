//! io_uring Platform-Specific Operation Definitions

use crate::{
    diagnostics::UringCompletionDiagnostics, driver::lifecycle::UringOpState, error::UringError,
};
use veloq_driver_core::{
    driver::{CompletionCleanupGuard, PlatformOp, registry::OpRegistry as CoreOpRegistry},
    slot::{Slot as CoreSlot, SlotSpec as CoreSlotSpec},
};
use veloq_std::pin::Pin;

mod descriptor;
mod payload;
mod spec;
mod submit;

pub(crate) use descriptor::{
    CompletionCardinality, CompletionCleanupHintFn, ErasedOperationDescriptor, OperationDescriptor,
    RecordPolicy, UringRecordItem,
};

pub use payload::UringUserPayload;
pub(crate) use payload::{
    Accept, AcceptMulti, AcceptedSocket, Close, Connect, Fallocate, FallocateRaw, Fsync, FsyncRaw,
    OpSend, Open, ProvidedBuf, ReadFixed, ReadRaw, Recv, RecvMulti, RecvProvided, SendTo,
    SyncFileRange, SyncFileRangeRaw, Timeout, UdpConnect, UdpRecvMulti, UdpRecvPacket, UdpSend,
    Wakeup, WriteFixed, WriteRaw,
};
pub(crate) use spec::{UringKernelPayloadStorage, UringOperationDescriptor};

pub(crate) use submit::{opcode_build, sqe_with_fd};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SubmissionStrategy {
    /// Submit a Standard SQE to the ring
    SubmitSqe,
    /// Handled by software timer wheel (no SQE submitted)
    SoftwareTimer,
}

// ============================================================================
// UringKernelOp Struct & Payload (Type-Erased)
// ============================================================================

pub struct UringKernelOp {
    /// Static descriptor used for all runtime dispatch.
    descriptor: &'static ErasedOperationDescriptor,

    /// Type-erased payload (kernel-side data)
    payload: UringKernelPayloadStorage,
}

impl PlatformOp for UringKernelOp {
    type CleanupContext<'a> = i32;

    #[inline]
    fn completion_cleanup(
        self: Pin<&mut Self>,
        result: Self::CleanupContext<'_>,
    ) -> CompletionCleanupGuard {
        (self.as_ref().get_ref().descriptor.completion_cleanup)(result)
    }

    #[inline]
    fn orphan_cleanup(
        self: Pin<&mut Self>,
        result: Self::CleanupContext<'_>,
    ) -> CompletionCleanupGuard {
        (self.as_ref().get_ref().descriptor.orphan_cleanup)(result)
    }
}

impl UringKernelOp {
    /// Constructs a type-erased operation with its descriptor and kernel payload paired together.
    #[inline]
    pub(crate) fn new<S>(kernel_payload: S::KernelPayload) -> Self
    where
        S: UringOperationDescriptor,
    {
        let descriptor = S::descriptor();
        Self {
            descriptor: &descriptor.erased,
            payload: (descriptor.encode_kernel)(kernel_payload),
        }
    }

    #[inline]
    pub(crate) fn descriptor(&self) -> &'static ErasedOperationDescriptor {
        self.descriptor
    }

    /// Replaces the descriptor with an intentionally mismatched one for projection tests only.
    #[cfg(test)]
    pub(crate) fn with_descriptor_for_test(
        mut self,
        descriptor: &'static ErasedOperationDescriptor,
    ) -> Self {
        self.descriptor = descriptor;
        self
    }

    #[inline]
    pub(crate) fn is_provided_multishot(&self) -> bool {
        self.descriptor.cardinality == CompletionCardinality::Multi
            && matches!(
                self.descriptor.record_policy,
                RecordPolicy::NewProvidedBuffer | RecordPolicy::UdpMultishot
            )
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
