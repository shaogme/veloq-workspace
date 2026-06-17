//! IOCP Platform-Specific Operation Definitions
//!
//! This module defines:
//! - `IocpKernelOp`: The Type-Erased kernel operation struct using Unions and VTables
//! - `OpVTable`: The virtual table for dynamic dispatch without enums
//! - `IntoPlatformOp` implementations split into `(KernelOp, UserPayload)`

mod file;
mod net;
mod payload;
mod spec;
mod state;
mod submit;

pub use payload::IocpUserPayload;
pub(crate) use payload::{
    ACCEPT_EX_ADDR_SECTION_LEN, ACCEPT_EX_OUTPUT_BUFFER_LEN, AcceptPayload, IocpOpPayload,
    KernelRef, OpenPayload, PayloadRef, SendToPayload, UdpRecvFromPayload, kernel_ref,
};
use spec::{IocpOpErasure, IocpOpSpec};
pub(crate) use state::{
    BlockingCompletion, BlockingSuccessCleanup, IocpOpRegistry, IocpSlotSpec, Slot,
};
pub use state::{IocpOpState, OverlappedEntry};
pub(crate) use submit::{SubmissionResult, resolve_fd_handle};

use std::sync::Arc;

use crate::{
    config::{IoFd, IocpHandle, OwnedRawHandle, RegisteredSlot},
    error::{IocpError, IocpResult},
    ext::Extensions,
    net::addr::SockAddrStorage,
    rio::RioState,
};

use veloq_driver_core::{
    driver::{CompletionCleanupGuard, CompletionToken, OpToken, PlatformOp},
    op::{
        IntoPlatformOp, OpCompletion,
        types::{
            Accept as AcceptBase, Close as CloseBase, Connect as ConnectBase,
            Fallocate as FallocateBase, FallocateRaw as FallocateRawBase, Fsync as FsyncBase,
            FsyncRaw as FsyncRawBase, OpKind, Open as OpenBase, ReadFixed as ReadFixedBase,
            ReadRaw as ReadRawBase, Recv as RecvBase, Send as OpSendBase, SendTo as SendToBase,
            SyncFileRange as SyncFileRangeBase, SyncFileRangeRaw as SyncFileRangeRawBase,
            Timeout as TimeoutBase, UdpConnect as UdpConnectBase, UdpRecv as UdpRecvBase,
            UdpRecvFrom as UdpRecvFromBase, UdpSend as UdpSendBase, Wakeup as WakeupBase,
            WriteFixed as WriteFixedBase, WriteRaw as WriteRawBase,
        },
    },
};

// ============================================================================
// Type Aliases for Core Ops
// ============================================================================

pub(crate) type ReadFixed = ReadFixedBase;
pub(crate) type ReadRaw = ReadRawBase<IocpHandle>;
pub(crate) type WriteFixed = WriteFixedBase;
pub(crate) type WriteRaw = WriteRawBase<IocpHandle>;
pub(crate) type Recv = RecvBase;
pub(crate) type OpSend = OpSendBase;
pub(crate) type UdpRecv = UdpRecvBase;
pub(crate) type UdpSend = UdpSendBase;
pub(crate) type Close = CloseBase;
pub(crate) type Fsync = FsyncBase;
pub(crate) type FsyncRaw = FsyncRawBase<IocpHandle>;
pub(crate) type Connect = ConnectBase<SockAddrStorage>;
pub(crate) type UdpConnect = UdpConnectBase<SockAddrStorage>;
pub(crate) type Accept = AcceptBase<SockAddrStorage>;
pub(crate) type SendTo = SendToBase;
pub(crate) type SyncFileRange = SyncFileRangeBase;
pub(crate) type SyncFileRangeRaw = SyncFileRangeRawBase<IocpHandle>;
pub(crate) type Fallocate = FallocateBase;
pub(crate) type FallocateRaw = FallocateRawBase<IocpHandle>;
pub(crate) type UdpRecvFrom = UdpRecvFromBase;
pub(crate) type Open = OpenBase;
pub(crate) type Timeout = TimeoutBase;
pub(crate) type Wakeup = WakeupBase;

// ============================================================================
// SubmitContext Definition
// ============================================================================

/// Context for submitting IOCP operations.
pub(crate) struct SubmitContext<'a> {
    pub(crate) port: Arc<crate::win32::IoCompletionPort>,
    pub(crate) overlapped: *mut crate::win32::Overlapped,
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
    fn completion_cleanup(&mut self, result: Self::CleanupContext<'_>) -> CompletionCleanupGuard {
        unsafe { (self.vtable.completion_cleanup)(self, result) }
    }

    #[inline]
    fn orphan_cleanup(&mut self, result: Self::CleanupContext<'_>) -> CompletionCleanupGuard {
        unsafe { (self.vtable.orphan_cleanup)(self, result) }
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

    pub(crate) fn submit(&mut self, ctx: &mut SubmitContext) -> IocpResult<SubmissionResult> {
        (self.vtable.submit)(self, ctx)
    }

    pub(crate) fn on_complete(&mut self, result: usize, ext: &Extensions) -> IocpResult<usize> {
        unsafe { (self.vtable.on_complete)(self, result, ext) }
    }

    pub(crate) fn completion_cleanup(
        &mut self,
        result: &IocpResult<usize>,
    ) -> CompletionCleanupGuard {
        PlatformOp::completion_cleanup(self, result)
    }

    pub(crate) fn orphan_cleanup(&mut self, result: &IocpResult<usize>) -> CompletionCleanupGuard {
        PlatformOp::orphan_cleanup(self, result)
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

        impl IntoPlatformOp<IocpOp> for $OpType {
            type UserPayload = $OpType;
            type ErasedPayload = IocpUserPayload;
            type Output = $OpType;
            type Completion = $completion;
            type DriverCompletion = usize;
            type Error = IocpError;

            const PAYLOAD_KIND: OpKind = <$OpType as IocpOpSpec>::PAYLOAD_KIND;

            fn into_kernel_and_payload(self) -> (IocpKernelOp, Self::UserPayload) {
                let kernel_payload = <$OpType as IocpOpSpec>::new_kernel_payload(&self);
                let op = IocpKernelOp {
                    vtable: <$OpType as IocpOpErasure>::vtable(),
                    header: OverlappedEntry::new(
                        OpToken::from_registry_parts(0, 0).expect("zero token should be encodable"),
                    ),
                    payload: <$OpType as IocpOpErasure>::erase_kernel_payload(kernel_payload),
                };
                (op, self)
            }

            fn payload_into_erased(payload: Self::UserPayload) -> IocpUserPayload {
                <$OpType as IocpOpErasure>::erase_user_payload(payload)
            }

            fn try_payload_from_erased(payload: IocpUserPayload) -> IocpResult<Self::UserPayload> {
                <$OpType as IocpOpErasure>::try_user_payload(payload)
            }

            fn complete(
                payload: Self::UserPayload,
                res: IocpResult<usize>,
            ) -> OpCompletion<Self::Output, IocpError, Self::Completion> {
                let completion = <$OpType as IocpOpSpec>::map_completion(&payload, res);
                OpCompletion::new(completion, payload)
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
impl_iocp_op_erasure!(UdpRecv, UdpRecv, UdpRecv, usize);
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
impl_iocp_op_erasure!(UdpRecvFrom, UdpRecvFrom, UdpRecvFrom, usize);
impl_iocp_op_erasure!(Open, Open, Open, OwnedRawHandle);
impl_iocp_op_erasure!(Wakeup, Wakeup, Wakeup, usize);
