//! IOCP Platform-Specific Operation Definitions
//!
//! This module defines:
//! - `IocpKernelOp`: The Type-Erased kernel operation struct using Unions and VTables
//! - `OpVTable`: The virtual table for dynamic dispatch without enums
//! - `IntoPlatformOp` implementations split into `(KernelOp, SubmitPayload)`

mod file;
mod net;
mod payload;
mod spec;
mod state;
mod submit;

pub use payload::IocpUserPayload;
pub(crate) use payload::{
    ACCEPT_EX_ADDR_SECTION_LEN, ACCEPT_EX_OUTPUT_BUFFER_LEN, AcceptMultiPayload, AcceptPayload,
    IocpOpPayload, KernelRef, OpenPayload, PayloadRef, RecvMultiPayload, RecvProvidedPayload,
    SendToPayload, UdpRecvMultiPayload, kernel_ref,
};
use spec::{
    IocpMultiShotErasure, IocpMultiShotSpec, IocpMultiShotVTable, IocpOpErasure, IocpOpSpec,
};
pub(crate) use state::{BlockingCompletion, BlockingSuccessCleanup, IocpOpRegistry, Slot};
pub use state::{IocpOpState, IocpSlotSpec, OverlappedEntry};
pub(crate) use submit::{SubmissionResult, locate_registered_slot, resolve_fd_handle};

use veloq_std::{pin::Pin, sync::Arc};

use diagweave::{prelude::*, report::Report};

use crate::{
    config::{IoFd, IocpHandle, OwnedRawHandle, RegisteredSlot},
    error::{IocpError, IocpResult},
    ext::Extensions,
    net::addr::SockAddrStorage,
    rio::RioState,
    win32::{IoCompletionPort, Overlapped},
};

use veloq_driver_core::{
    driver::{CompletionCleanupGuard, CompletionToken, OpToken, PlatformOp},
    op::{
        IntoPlatformOp, LostReason, OpCompletion, OpError, OpResult, SingleShotOp,
        payload_projection_mismatch_report,
        types::{
            Accept as AcceptBase, AcceptMulti as AcceptMultiBase,
            AcceptedSocket as AcceptedSocketBase, Close as CloseBase, Connect as ConnectBase,
            Fallocate as FallocateBase, FallocateRaw as FallocateRawBase, Fsync as FsyncBase,
            FsyncRaw as FsyncRawBase, OpKind, Open as OpenBase, ProvidedBuf,
            ReadFixed as ReadFixedBase, ReadRaw as ReadRawBase, Recv as RecvBase,
            RecvMulti as RecvMultiBase, RecvProvided as RecvProvidedBase, Send as OpSendBase,
            SendTo as SendToBase, SyncFileRange as SyncFileRangeBase,
            SyncFileRangeRaw as SyncFileRangeRawBase, Timeout as TimeoutBase,
            UdpConnect as UdpConnectBase, UdpRecvMulti as UdpRecvMultiBase,
            UdpRecvPacket as UdpRecvPacketBase, UdpSend as UdpSendBase, Wakeup as WakeupBase,
            WriteFixed as WriteFixedBase, WriteRaw as WriteRawBase,
        },
    },
    platform::receive_pump::ReceivePumpState,
    slot::Generation,
};

// ============================================================================
// Type Aliases for Core Ops
// ============================================================================

pub(crate) type ReadFixed = ReadFixedBase<IocpHandle>;
pub(crate) type ReadRaw = ReadRawBase<IocpHandle>;
pub(crate) type WriteFixed = WriteFixedBase<IocpHandle>;
pub(crate) type WriteRaw = WriteRawBase<IocpHandle>;
pub(crate) type Recv = RecvBase<IocpHandle>;
pub(crate) type OpSend = OpSendBase<IocpHandle>;
pub(crate) type UdpSend = UdpSendBase<IocpHandle>;
pub(crate) type Close = CloseBase<IocpHandle>;
pub(crate) type Fsync = FsyncBase<IocpHandle>;
pub(crate) type FsyncRaw = FsyncRawBase<IocpHandle>;
pub(crate) type Connect = ConnectBase<IocpHandle, SockAddrStorage>;
pub(crate) type UdpConnect = UdpConnectBase<IocpHandle, SockAddrStorage>;
pub(crate) type Accept = AcceptBase<IocpHandle, SockAddrStorage>;
pub(crate) type AcceptedSocket = AcceptedSocketBase;
pub(crate) type SendTo = SendToBase<IocpHandle>;
pub(crate) type SyncFileRange = SyncFileRangeBase<IocpHandle>;
pub(crate) type SyncFileRangeRaw = SyncFileRangeRawBase<IocpHandle>;
pub(crate) type Fallocate = FallocateBase<IocpHandle>;
pub(crate) type FallocateRaw = FallocateRawBase<IocpHandle>;
pub(crate) type UdpRecvMulti = UdpRecvMultiBase<IocpHandle>;
pub(crate) type UdpRecvPacket = UdpRecvPacketBase;
pub(crate) type Open = OpenBase;
pub(crate) type Timeout = TimeoutBase;
pub(crate) type Wakeup = WakeupBase<IocpHandle>;

pub(crate) type AcceptMulti = AcceptMultiBase<IocpHandle>;
pub(crate) type RecvProvided = RecvProvidedBase<IocpHandle>;
pub(crate) type RecvMulti = RecvMultiBase<IocpHandle>;

// ============================================================================
// SubmitContext Definition
// ============================================================================

/// Context for submitting IOCP operations.
pub(crate) struct SubmitContext<'a> {
    pub(crate) port: Arc<IoCompletionPort>,
    pub(crate) overlapped: *mut Overlapped,
    pub(crate) op_token: OpToken,
    pub(crate) completion_token: CompletionToken,
    pub(crate) ext: &'a Extensions,
    pub(crate) registered_slots: &'a mut [RegisteredSlot],
    pub(crate) registrar: &'a dyn veloq_buf::BufferRegistrar,

    // RIO Support
    pub(crate) rio: &'a mut RioState,
}

// ============================================================================
// Type-Erased VTable
// ============================================================================

pub(crate) struct OpVTable {
    pub(crate) kind: OpKind,
    pub(crate) multishot: bool,
    pub(crate) multishot_ops: Option<&'static IocpMultiShotVTable>,
    pub(crate) submit: fn(&mut IocpKernelOp, &mut SubmitContext) -> IocpResult<SubmissionResult>,
    pub(crate) on_complete:
        unsafe fn(&mut IocpKernelOp, result: usize, ext: &Extensions) -> IocpResult<usize>,
    pub(crate) completion_cleanup:
        unsafe fn(&mut IocpKernelOp, result: &IocpResult<usize>) -> CompletionCleanupGuard,
    pub(crate) orphan_cleanup:
        unsafe fn(&mut IocpKernelOp, result: &IocpResult<usize>) -> CompletionCleanupGuard,
    pub(crate) get_fd: unsafe fn(&IocpKernelOp) -> Option<IoFd>,
    pub(crate) bind_user_payload: fn(&mut IocpKernelOp, &mut IocpUserPayload) -> IocpResult<()>,
    pub(crate) unbind_user_payload: fn(&mut IocpKernelOp),
}

pub struct IocpKernelOp {
    pub(crate) vtable: &'static OpVTable,
    pub(crate) header: OverlappedEntry,
    pub(crate) payload: IocpOpPayload,
}

impl PlatformOp for IocpKernelOp {
    type CleanupContext<'a> = &'a IocpResult<usize>;

    #[inline]
    fn completion_cleanup(
        self: Pin<&mut Self>,
        result: Self::CleanupContext<'_>,
    ) -> CompletionCleanupGuard {
        let op = self.get_mut();
        unsafe { (op.vtable.completion_cleanup)(op, result) }
    }

    #[inline]
    fn orphan_cleanup(
        self: Pin<&mut Self>,
        result: Self::CleanupContext<'_>,
    ) -> CompletionCleanupGuard {
        let op = self.get_mut();
        unsafe { (op.vtable.orphan_cleanup)(op, result) }
    }
}

impl IocpKernelOp {
    pub(crate) fn bind_user_payload(&mut self, erased: &mut IocpUserPayload) -> IocpResult<()> {
        (self.vtable.bind_user_payload)(self, erased)
    }

    pub(crate) fn unbind_user_payload(&mut self) {
        (self.vtable.unbind_user_payload)(self);
    }

    pub(crate) fn get_fd(&self) -> Option<IoFd> {
        unsafe { (self.vtable.get_fd)(self) }
    }

    pub(crate) const fn kind(&self) -> OpKind {
        self.vtable.kind
    }

    pub(crate) const fn is_multishot(&self) -> bool {
        self.vtable.multishot
    }

    pub(crate) const fn multishot_vtable(&self) -> Option<&'static IocpMultiShotVTable> {
        self.vtable.multishot_ops
    }

    pub(crate) fn accepted_socket_record(&self) -> Option<IocpUserPayload> {
        let vtable = self.multishot_vtable()?;
        (vtable.record_kind == spec::IocpRecordKind::AcceptedSocket)
            .then(|| (vtable.encode_accepted_socket)())
    }

    pub(crate) fn validate_accept_multi_completion(&self, token: OpToken) -> IocpResult<()> {
        if self.kind() != OpKind::AcceptMulti {
            return IocpError::InvalidState
                .with_ctx("operation", self.kind() as u16)
                .attach_note("AcceptMulti completion validation reached a different operation");
        }
        if self.header.token != token {
            return IocpError::InvalidState
                .with_ctx("expected_index", token.index())
                .with_ctx("expected_generation", token.generation())
                .with_ctx("actual_index", self.header.token.index())
                .with_ctx("actual_generation", self.header.token.generation())
                .attach_note("AcceptMulti completion token does not match its slot");
        }
        if !self.header.in_flight {
            return IocpError::InvalidState
                .with_ctx("user_data", token.index())
                .with_ctx("generation", token.generation())
                .attach_note("AcceptMulti completion was observed after request settlement");
        }
        let IocpOpPayload::AcceptMulti(payload) = &self.payload else {
            return IocpError::InvalidState
                .attach_note("AcceptMulti completion payload variant is missing");
        };
        if payload.request_generation != self.header.request_generation {
            return IocpError::InvalidState
                .with_ctx("payload_request_generation", payload.request_generation)
                .with_ctx("header_request_generation", self.header.request_generation)
                .attach_note("AcceptMulti completion request generation does not match");
        }
        if payload.accept_socket.is_none() {
            return IocpError::InvalidState
                .attach_note("AcceptMulti completion has no pending accepted socket");
        }
        Ok(())
    }

    pub(crate) fn recv_multi_pump_mut(&mut self) -> Option<&mut ReceivePumpState> {
        let IocpOpPayload::RecvMulti(payload) = &mut self.payload else {
            return None;
        };
        payload.pump.as_mut()
    }

    pub(crate) fn submit(&mut self, ctx: &mut SubmitContext) -> IocpResult<SubmissionResult> {
        (self.vtable.submit)(self, ctx)
    }

    pub(crate) fn on_complete(&mut self, result: usize, ext: &Extensions) -> IocpResult<usize> {
        unsafe { (self.vtable.on_complete)(self, result, ext) }
    }

    pub(crate) fn take_recv_provided_record(
        &mut self,
        deliver_buffer: bool,
    ) -> IocpResult<IocpUserPayload> {
        let IocpOpPayload::RecvProvided(payload) = &mut self.payload else {
            return IocpError::InvalidState
                .with_ctx("operation", self.kind() as u16)
                .attach_note("RecvProvided record requested from a different IOCP payload");
        };
        let buffer = payload.buffer.take();
        let buffer = if deliver_buffer {
            buffer
        } else {
            drop(buffer);
            None
        };
        Ok(IocpUserPayload::ProvidedBuf(ProvidedBuf { buf: buffer }))
    }
}

macro_rules! impl_iocp_op_erasure {
    ($OpType:ty, $user_variant:ident, $kernel_variant:ident, $completion:ty) => {
        impl IocpOpErasure for $OpType {
            fn erase_kernel_payload(payload: Self::KernelPayload) -> IocpOpPayload {
                IocpOpPayload::$kernel_variant(payload)
            }

            fn kernel_payload_ref(payload: &IocpOpPayload) -> Option<&Self::KernelPayload> {
                match payload {
                    IocpOpPayload::$kernel_variant(payload) => Some(payload),
                    _ => None,
                }
            }

            fn kernel_payload_mut(payload: &mut IocpOpPayload) -> Option<&mut Self::KernelPayload> {
                match payload {
                    IocpOpPayload::$kernel_variant(payload) => Some(payload),
                    _ => None,
                }
            }

            fn erase_user_payload(payload: Self) -> IocpUserPayload {
                IocpUserPayload::$user_variant(payload)
            }

            fn try_user_payload(payload: IocpUserPayload) -> IocpResult<Self> {
                match payload {
                    IocpUserPayload::$user_variant(payload) => Ok(payload),
                    _ => Err(veloq_driver_core::op::payload_projection_mismatch_report::<
                        IocpError,
                    >(stringify!($OpType), "IocpUserPayload")),
                }
            }

            fn user_payload_mut(payload: &mut IocpUserPayload) -> Option<&mut Self> {
                match payload {
                    IocpUserPayload::$user_variant(payload) => Some(payload),
                    _ => None,
                }
            }

            fn vtable() -> &'static OpVTable {
                static TABLE: OpVTable = OpVTable {
                    kind: <$OpType as IocpOpSpec>::PAYLOAD_KIND,
                    multishot: false,
                    multishot_ops: None,
                    submit: spec::submit_shim::<$OpType>,
                    on_complete: spec::on_complete_shim::<$OpType>,
                    completion_cleanup: spec::completion_cleanup_shim::<$OpType>,
                    orphan_cleanup: spec::orphan_cleanup_shim::<$OpType>,
                    get_fd: spec::get_fd_shim::<$OpType>,
                    bind_user_payload: spec::bind_user_payload_shim::<$OpType>,
                    unbind_user_payload: spec::unbind_user_payload_shim::<$OpType>,
                };
                &TABLE
            }
        }

        /// IOCP 后端的 multishot 操作由 facade 的既有 fallback 避开；这里保留统一
        /// operation bound 所需的 payload 形状。
        impl IntoPlatformOp<IocpSlotSpec> for $OpType {
            type SubmitPayload = $OpType;
            type RecordPayload = $OpType;
            type Output = $OpType;
            type Completion = $completion;

            const PAYLOAD_KIND: OpKind = <$OpType as IocpOpSpec>::PAYLOAD_KIND;

            fn into_kernel_and_payload(self) -> (IocpKernelOp, Self::SubmitPayload) {
                let kernel_payload = <$OpType as IocpOpSpec>::new_kernel_payload(&self);
                let op = IocpKernelOp {
                    vtable: <$OpType as IocpOpErasure>::vtable(),
                    header: OverlappedEntry::new(
                        OpToken::from_registry_parts(0, Generation::ZERO)
                            .expect("zero token should be encodable"),
                    ),
                    payload: <$OpType as IocpOpErasure>::erase_kernel_payload(kernel_payload),
                };
                (op, self)
            }

            fn payload_into_erased(payload: Self::SubmitPayload) -> IocpUserPayload {
                <$OpType as IocpOpErasure>::erase_user_payload(payload)
            }

            fn try_record_from_erased(payload: IocpUserPayload) -> IocpResult<Self::RecordPayload> {
                <$OpType as IocpOpErasure>::try_user_payload(payload)
            }

            fn complete(
                payload: Self::RecordPayload,
                res: IocpResult<usize>,
            ) -> OpCompletion<Self::Output, IocpError, Self::Completion> {
                let completion = <$OpType as IocpOpSpec>::map_completion(&payload, res);
                OpCompletion::new(completion, payload)
            }
        }

        impl SingleShotOp<IocpSlotSpec> for $OpType {}
    };
}

/// Erase an IOCP operation whose submit payload must stay in the slot while each completion gets
/// its own record payload.
macro_rules! impl_iocp_multishot_op_erasure {
    (
        $OpType:ty,
        $user_variant:ident,
        $kernel_variant:ident,
        $record_variant:ident,
        $record:ty,
        $record_kind:ident,
        $completion:ty,
        $multishot:expr,
        $continuation:path
    ) => {
        impl IocpMultiShotErasure for $OpType {
            fn erase_kernel_payload(payload: Self::KernelPayload) -> IocpOpPayload {
                IocpOpPayload::$kernel_variant(payload)
            }

            fn kernel_payload_ref(payload: &IocpOpPayload) -> Option<&Self::KernelPayload> {
                match payload {
                    IocpOpPayload::$kernel_variant(payload) => Some(payload),
                    _ => None,
                }
            }

            fn kernel_payload_mut(payload: &mut IocpOpPayload) -> Option<&mut Self::KernelPayload> {
                match payload {
                    IocpOpPayload::$kernel_variant(payload) => Some(payload),
                    _ => None,
                }
            }

            fn erase_user_payload(payload: Self) -> IocpUserPayload {
                IocpUserPayload::$user_variant(payload)
            }

            fn user_payload_mut(payload: &mut IocpUserPayload) -> Option<&mut Self> {
                match payload {
                    IocpUserPayload::$user_variant(payload) => Some(payload),
                    _ => None,
                }
            }

            fn try_record_payload(payload: IocpUserPayload) -> IocpResult<Self::RecordPayload> {
                match payload {
                    IocpUserPayload::$record_variant(payload) => Ok(payload),
                    _ => Err(payload_projection_mismatch_report::<IocpError>(
                        stringify!($record),
                        "IocpUserPayload",
                    )),
                }
            }

            fn vtable() -> &'static OpVTable {
                static MULTI_SHOT: IocpMultiShotVTable = IocpMultiShotVTable {
                    operation: <$OpType as IocpMultiShotSpec>::PAYLOAD_KIND,
                    record_kind: spec::IocpRecordKind::$record_kind,
                    encode_accepted_socket: spec::encode_accepted_socket,
                    rearm: spec::submit_multishot_shim::<$OpType>,
                    continuation: $continuation,
                };
                static TABLE: OpVTable = OpVTable {
                    kind: <$OpType as IocpMultiShotSpec>::PAYLOAD_KIND,
                    multishot: $multishot,
                    multishot_ops: Some(&MULTI_SHOT),
                    submit: spec::submit_multishot_shim::<$OpType>,
                    on_complete: spec::on_complete_multishot_shim::<$OpType>,
                    completion_cleanup: spec::completion_cleanup_multishot_shim::<$OpType>,
                    orphan_cleanup: spec::orphan_cleanup_multishot_shim::<$OpType>,
                    get_fd: spec::get_fd_multishot_shim::<$OpType>,
                    bind_user_payload: spec::bind_multishot_user_payload_shim::<$OpType>,
                    unbind_user_payload: spec::unbind_multishot_user_payload_shim::<$OpType>,
                };
                &TABLE
            }
        }

        impl IntoPlatformOp<IocpSlotSpec> for $OpType {
            type SubmitPayload = $OpType;
            type RecordPayload = $record;
            type Output = $record;
            type Completion = $completion;

            const PAYLOAD_KIND: OpKind = <$OpType as IocpMultiShotSpec>::PAYLOAD_KIND;

            fn into_kernel_and_payload(self) -> (IocpKernelOp, Self::SubmitPayload) {
                let kernel_payload = <$OpType as IocpMultiShotSpec>::new_kernel_payload(&self);
                let op = IocpKernelOp {
                    vtable: <$OpType as IocpMultiShotErasure>::vtable(),
                    header: OverlappedEntry::new(
                        OpToken::from_registry_parts(0, Generation::ZERO)
                            .expect("zero token should be encodable"),
                    ),
                    payload: <$OpType as IocpMultiShotErasure>::erase_kernel_payload(
                        kernel_payload,
                    ),
                };
                (op, self)
            }

            fn payload_into_erased(payload: Self::SubmitPayload) -> IocpUserPayload {
                <$OpType as IocpMultiShotErasure>::erase_user_payload(payload)
            }

            fn try_record_from_erased(payload: IocpUserPayload) -> IocpResult<Self::RecordPayload> {
                <$OpType as IocpMultiShotErasure>::try_record_payload(payload)
            }

            fn complete(
                payload: Self::RecordPayload,
                res: IocpResult<usize>,
            ) -> OpCompletion<Self::Output, IocpError, Self::Completion> {
                let completion = <$OpType as IocpMultiShotSpec>::map_completion(&payload, res);
                OpCompletion::new(completion, payload)
            }

            fn submit_failed(
                erased: IocpUserPayload,
                report: Report<IocpError>,
            ) -> OpResult<Self::Output, IocpError, Self::Completion> {
                drop(erased);
                OpResult::ResourceLost(OpError::new(LostReason::Other, report))
            }
        }
    };
}

/// Alias for the platform-specific IOCP kernel operation.
pub type IocpOp = IocpKernelOp;

// ============================================================================
// Op Definitions
// ============================================================================

impl IocpOpSpec for Timeout {
    type KernelPayload = KernelRef<Self>;
    type Completion = usize;

    const PAYLOAD_KIND: OpKind = OpKind::Timeout;

    fn new_kernel_payload(user: &Self) -> Self::KernelPayload {
        kernel_ref(user)
    }

    fn submit(
        header: &mut OverlappedEntry,
        payload: &mut Self::KernelPayload,
        ctx: &mut SubmitContext,
    ) -> IocpResult<SubmissionResult> {
        submit::submit_timeout(header, payload, ctx)
    }

    fn map_completion(_payload: &Self, res: IocpResult<usize>) -> IocpResult<Self::Completion> {
        res
    }
}

impl IocpOpSpec for Wakeup {
    type KernelPayload = KernelRef<Self>;
    type Completion = usize;

    const PAYLOAD_KIND: OpKind = OpKind::Wakeup;

    fn new_kernel_payload(user: &Self) -> Self::KernelPayload {
        kernel_ref(user)
    }

    fn submit(
        header: &mut OverlappedEntry,
        payload: &mut Self::KernelPayload,
        ctx: &mut SubmitContext,
    ) -> IocpResult<SubmissionResult> {
        submit::submit_wakeup(header, payload, ctx)
    }

    fn map_completion(_payload: &Self, res: IocpResult<usize>) -> IocpResult<Self::Completion> {
        res
    }
}

impl_iocp_op_erasure!(ReadFixed, ReadFixed, Read, usize);
impl_iocp_op_erasure!(ReadRaw, ReadRaw, ReadRaw, usize);
impl_iocp_op_erasure!(WriteFixed, WriteFixed, Write, usize);
impl_iocp_op_erasure!(WriteRaw, WriteRaw, WriteRaw, usize);
impl_iocp_op_erasure!(Recv, Recv, Recv, usize);
impl_iocp_op_erasure!(OpSend, OpSend, Send, usize);
impl_iocp_op_erasure!(UdpSend, UdpSend, UdpSend, usize);
impl_iocp_op_erasure!(Close, Close, Close, usize);
impl_iocp_op_erasure!(Fsync, Fsync, Fsync, usize);
impl_iocp_op_erasure!(FsyncRaw, FsyncRaw, FsyncRaw, usize);
impl_iocp_op_erasure!(SyncFileRange, SyncFileRange, SyncRange, usize);
impl_iocp_op_erasure!(SyncFileRangeRaw, SyncFileRangeRaw, SyncRangeRaw, usize);
impl_iocp_op_erasure!(Fallocate, Fallocate, Fallocate, usize);
impl_iocp_op_erasure!(FallocateRaw, FallocateRaw, FallocateRaw, usize);
impl_iocp_op_erasure!(Timeout, Timeout, Timeout, usize);
impl_iocp_op_erasure!(Connect, Connect, Connect, usize);
impl_iocp_op_erasure!(UdpConnect, UdpConnect, UdpConnect, usize);
impl_iocp_op_erasure!(Accept, Accept, Accept, OwnedRawHandle);
impl_iocp_op_erasure!(SendTo, SendTo, SendTo, usize);
impl_iocp_multishot_op_erasure!(
    AcceptMulti,
    AcceptMulti,
    AcceptMulti,
    AcceptedSocket,
    AcceptedSocket,
    AcceptedSocket,
    OwnedRawHandle,
    true,
    spec::more_on_success
);
impl_iocp_multishot_op_erasure!(
    RecvProvided,
    RecvProvided,
    RecvProvided,
    ProvidedBuf,
    ProvidedBuf,
    ProvidedBuf,
    usize,
    false,
    spec::final_continuation
);
impl SingleShotOp<IocpSlotSpec> for RecvProvided {}
impl_iocp_multishot_op_erasure!(
    RecvMulti,
    RecvMulti,
    RecvMulti,
    ProvidedBuf,
    ProvidedBuf,
    ProvidedBuf,
    usize,
    true,
    spec::more_on_success
);
impl_iocp_multishot_op_erasure!(
    UdpRecvMulti,
    UdpRecvMulti,
    UdpRecvMulti,
    UdpRecvPacket,
    UdpRecvPacket,
    UdpRecvPacket,
    usize,
    true,
    spec::more_on_success
);
impl_iocp_op_erasure!(Open, Open, Open, OwnedRawHandle);
impl_iocp_op_erasure!(Wakeup, Wakeup, Wakeup, usize);

#[cfg(test)]
mod tests {
    use super::*;
    use veloq_buf::FixedBuf;
    use veloq_driver_core::op::types::UdpRecvPacketBuf;
    use veloq_driver_core::platform::receive_pump::{ReceivePumpConfig, ReceiveSlot};
    use veloq_std::{net::SocketAddr, num::NonZeroUsize, time::Duration, vec};

    fn receive_pump() -> veloq_driver_core::platform::receive_pump::ReceivePumpState {
        let config = ReceivePumpConfig {
            kernel_capacity: NonZeroUsize::new(1).expect("non-zero depth"),
            queue_capacity: NonZeroUsize::new(1).expect("non-zero queue"),
            datagram_capacity: NonZeroUsize::new(64).expect("non-zero datagram capacity"),
            close_timeout: Duration::from_secs(1),
        };
        let slots = vec![ReceiveSlot::new(
            0,
            FixedBuf::alloc_heap(NonZeroUsize::new(64).expect("non-zero buffer"), 0)
                .expect("receive slot allocation"),
        )]
        .into_boxed_slice();
        veloq_driver_core::platform::receive_pump::ReceivePumpState::try_new(config, slots)
            .expect("valid receive pump")
    }

    #[test]
    fn udp_receive_multishot_keeps_submit_and_record_payloads_separate() {
        let fd = IoFd::Direct(IocpHandle::for_socket(core::ptr::null_mut()));
        let operation = UdpRecvMulti::from_backend(fd, receive_pump());
        let (mut kernel, submit_payload) = operation.into_kernel_and_payload();
        let mut erased = IocpUserPayload::UdpRecvMulti(submit_payload);

        kernel
            .bind_user_payload(&mut erased)
            .expect("persistent submit payload should bind");
        assert_eq!(kernel.kind(), OpKind::UdpRecvMulti);
        assert!(kernel.is_multishot());
        assert!(kernel.multishot_vtable().is_some());

        let packet = UdpRecvPacket {
            buf: UdpRecvPacketBuf::from_fixed_buf(
                FixedBuf::alloc_heap(NonZeroUsize::new(8).expect("non-zero buffer"), 2)
                    .expect("record allocation"),
            ),
            addr: "127.0.0.1:9000"
                .parse::<SocketAddr>()
                .expect("valid source address"),
        };
        let record_erased = IocpUserPayload::UdpRecvPacket(packet);
        assert!(matches!(record_erased, IocpUserPayload::UdpRecvPacket(_)));
        let record = UdpRecvMulti::try_record_from_erased(record_erased)
            .expect("record payload should project independently");
        assert_eq!(record.buf.len(), 2);

        kernel.unbind_user_payload();
    }
}
