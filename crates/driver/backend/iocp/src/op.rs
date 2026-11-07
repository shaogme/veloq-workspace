//! IOCP 平台特定操作定义 / IOCP platform-specific operation definitions
//!
//! 本模块定义：/ This module defines:
//! - `IocpKernelOp`：通过 union 和 vtable 实现类型擦除的内核操作结构。 / A type-erased kernel operation using unions and vtables.
//! - `OpVTable`：无需枚举即可动态分派的虚表。 / A virtual table for dynamic dispatch without enums.
//! - `IntoPlatformOp`：将实现拆分为 `(KernelOp, SubmitPayload)`。 / Implementations split into `(KernelOp, SubmitPayload)`.

mod file;
mod net;
mod payload;
mod spec;
mod state;
mod submit;
mod vtable;

pub use payload::IocpUserPayload;
pub(crate) use payload::{
    ACCEPT_EX_ADDR_SECTION_LEN, ACCEPT_EX_OUTPUT_BUFFER_LEN, AcceptMultiPayload, AcceptPayload,
    IocpOpPayload, KernelRef, OpenPayload, RawUdpReceiveRequest, RecvMultiPayload,
    RecvProvidedPayload, SendToPayload, UdpRecvMultiPayload,
};
pub(crate) use state::{BlockingCompletion, BlockingSuccessCleanup, IocpOpRegistry, Slot};
pub use state::{IocpOpState, OverlappedEntry};
pub(crate) use submit::{SubmissionOps, SubmissionResult, UdpReceiveSubmit};
pub(crate) use vtable::{
    IocpMultiShotErasure, IocpMultiShotSpec, IocpMultiShotVTable, IocpOpErasure, IocpOpSpec,
    IocpRecordKind, OpVTable, PayloadBinding,
};

use veloq_buf::{AnyBufPool, FixedBuf, RegistrationCapability};
use veloq_std::{marker::PhantomData, pin::Pin, sync::Arc};

use diagweave::{prelude::*, report::Report};

use crate::{
    config::{IoFd, IocpHandle, OwnedRawHandle, RegisteredSlot},
    diagnostics::IocpCompletionDiagnostics,
    driver::OverlappedRegistry,
    error::{IocpError, IocpResult},
    ext::Extensions,
    net::SockAddrStorage,
    rio::RioState,
    win32::{IoCompletionPort, Overlapped},
};

use veloq_driver_core::{
    Accept as AcceptBase, AcceptMulti as AcceptMultiBase, AcceptedSocket as AcceptedSocketBase,
    Close as CloseBase, CompletionCleanupGuard, CompletionToken, Connect as ConnectBase,
    Fallocate as FallocateBase, FallocateRaw as FallocateRawBase, Fsync as FsyncBase,
    FsyncRaw as FsyncRawBase, Generation, IntoPlatformOp, LostReason, OpCompletion, OpError,
    OpKind, OpResult, OpToken, Open as OpenBase, PlatformOp, ProvidedBuf,
    ReadFixed as ReadFixedBase, ReadRaw as ReadRawBase, ReceivePumpState, Recv as RecvBase,
    RecvMulti as RecvMultiBase, RecvProvided as RecvProvidedBase, Send as OpSendBase,
    SendTo as SendToBase, SingleShotOperation, SlotSpec as CoreSlotSpec,
    SyncFileRange as SyncFileRangeBase, SyncFileRangeRaw as SyncFileRangeRawBase,
    Timeout as TimeoutBase, UdpConnect as UdpConnectBase, UdpRecvMulti as UdpRecvMultiBase,
    UdpRecvMultiBackend, UdpRecvPacket as UdpRecvPacketBase, UdpSend as UdpSendBase,
    Wakeup as WakeupBase, WriteFixed as WriteFixedBase, WriteRaw as WriteRawBase,
};

// ============================================================================
// 核心操作类型别名 / Type aliases for core operations
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

pub(crate) const RAW_UDP_RECV_MULTI_RESUME_BYTES: u32 = i32::MAX as u32;

struct OpSpec<T>(PhantomData<T>);

// ============================================================================
// SubmitContext 定义 / SubmitContext definition
// ============================================================================
/// 提交 IOCP 操作时使用的上下文。
/// Context for submitting IOCP operations.
pub(crate) struct SubmitContext<'a> {
    pub(crate) port: Arc<IoCompletionPort>,
    pub(crate) overlapped: *mut Overlapped,
    pub(crate) op_token: OpToken,
    pub(crate) completion_token: CompletionToken,
    pub(crate) ext: &'a Extensions,
    pub(crate) registered_slots: &'a mut [RegisteredSlot],
    pub(crate) overlapped_registry: Option<&'a mut OverlappedRegistry>,
    pub(crate) buffer_pool: Option<&'a AnyBufPool>,

    // RIO 支持 / RIO support
    pub(crate) rio: &'a mut RioState,
}

pub struct IocpKernelOp {
    vtable: &'static OpVTable,
    header: OverlappedEntry,
    payload: IocpOpPayload,
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
    pub(crate) fn header(&self) -> &OverlappedEntry {
        &self.header
    }

    pub(crate) fn header_mut(&mut self) -> &mut OverlappedEntry {
        &mut self.header
    }

    pub(crate) fn payload(&self) -> &IocpOpPayload {
        &self.payload
    }

    pub(crate) fn payload_mut(&mut self) -> &mut IocpOpPayload {
        &mut self.payload
    }

    pub(crate) fn header_payload_mut(&mut self) -> (&mut OverlappedEntry, &mut IocpOpPayload) {
        (&mut self.header, &mut self.payload)
    }

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

    /// 返回此操作是否使用 RIO 后端。 / Returns whether this operation uses the RIO backend.
    pub(crate) fn uses_rio_backend(&self) -> bool {
        let Some(handle) = self.header.resolved_handle() else {
            return false;
        };
        if !handle.raw().uses_rio() {
            return false;
        }

        let is_registered = |buffer: &FixedBuf| {
            matches!(
                buffer.resolve_region_info().registration,
                RegistrationCapability::RegisteredChunk(_)
            )
        };
        match self.kind() {
            OpKind::Recv => match &self.payload {
                IocpOpPayload::Recv(payload) => unsafe { payload.user().as_ref() }
                    .map(|user| is_registered(&user.buf))
                    .unwrap_or(false),
                _ => false,
            },
            OpKind::Send => match &self.payload {
                IocpOpPayload::Send(payload) => unsafe { payload.user().as_ref() }
                    .map(|user| is_registered(&user.buf))
                    .unwrap_or(false),
                _ => false,
            },
            OpKind::UdpSend => match &self.payload {
                IocpOpPayload::UdpSend(payload) => unsafe { payload.user().as_ref() }
                    .map(|user| is_registered(&user.buf))
                    .unwrap_or(false),
                _ => false,
            },
            OpKind::SendTo => match &self.payload {
                IocpOpPayload::SendTo(payload) => unsafe { payload.user().as_ref() }
                    .map(|user| is_registered(&user.buf))
                    .unwrap_or(false),
                _ => false,
            },
            OpKind::RecvProvided => false,
            OpKind::RecvMulti => match &self.payload {
                IocpOpPayload::RecvMulti(payload) => payload.pump().is_some(),
                _ => false,
            },
            OpKind::UdpRecvMulti => match &self.payload {
                IocpOpPayload::UdpRecvMulti(payload) => unsafe { payload.user().as_ref() }
                    .map(|user| {
                        let pump = user.receive_pump();
                        let target_depth = pump.snapshot().target_depth as u32;
                        target_depth > 0
                            && (0..target_depth).all(|slot_id| {
                                pump.slot(slot_id)
                                    .is_some_and(|slot| is_registered(slot.data_buf()))
                            })
                    })
                    .unwrap_or(false),
                _ => false,
            },
            OpKind::AcceptMulti => false,
            _ => false,
        }
    }

    pub(crate) const fn multishot_vtable(&self) -> Option<&'static IocpMultiShotVTable> {
        self.vtable.multishot_ops
    }

    pub(crate) fn accepted_socket_record(&self) -> Option<IocpUserPayload> {
        let vtable = self.multishot_vtable()?;
        (vtable.record_kind == IocpRecordKind::AcceptedSocket)
            .then(|| (vtable.encode_accepted_socket)())
    }

    pub(crate) fn validate_accept_multi_completion(&self, token: OpToken) -> IocpResult<()> {
        if self.kind() != OpKind::AcceptMulti {
            return IocpError::InvalidState
                .with_ctx("operation", self.kind() as u16)
                .attach_note("AcceptMulti completion validation reached a different operation");
        }
        if self.header.token() != token {
            return IocpError::InvalidState
                .with_ctx("expected_index", token.index())
                .with_ctx("expected_generation", token.generation())
                .with_ctx("actual_index", self.header.token().index())
                .with_ctx("actual_generation", self.header.token().generation())
                .attach_note("AcceptMulti completion token does not match its slot");
        }
        if !self.header.in_flight() {
            return IocpError::InvalidState
                .with_ctx("user_data", token.index())
                .with_ctx("generation", token.generation())
                .attach_note("AcceptMulti completion was observed after request settlement");
        }
        let IocpOpPayload::AcceptMulti(payload) = &self.payload else {
            return IocpError::InvalidState
                .attach_note("AcceptMulti completion payload variant is missing");
        };
        if payload.request_generation() != self.header.request_generation() {
            return IocpError::InvalidState
                .with_ctx("payload_request_generation", payload.request_generation())
                .with_ctx(
                    "header_request_generation",
                    self.header.request_generation(),
                )
                .attach_note("AcceptMulti completion request generation does not match");
        }
        if payload.accept_socket().is_none() {
            return IocpError::InvalidState
                .attach_note("AcceptMulti completion has no pending accepted socket");
        }
        Ok(())
    }

    pub(crate) fn recv_multi_pump_mut(&mut self) -> Option<&mut ReceivePumpState> {
        let IocpOpPayload::RecvMulti(payload) = &mut self.payload else {
            return None;
        };
        payload.pump_mut().as_mut()
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
        let buffer = payload.buffer_mut().take();
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
    ($SpecType:ty, $OpType:ty, $user_variant:ident, $kernel_variant:ident, $completion:ty) => {
        impl IocpOpErasure for $SpecType {
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

            fn erase_user_payload(payload: Self::User) -> IocpUserPayload {
                IocpUserPayload::$user_variant(payload)
            }

            fn try_user_payload(payload: IocpUserPayload) -> IocpResult<Self::User> {
                match payload {
                    IocpUserPayload::$user_variant(payload) => Ok(payload),
                    _ => Err(OpError::<IocpError>::payload_projection_mismatch_report(
                        stringify!($OpType),
                        "IocpUserPayload",
                    )),
                }
            }

            fn user_payload_mut(payload: &mut IocpUserPayload) -> Option<&mut Self::User> {
                match payload {
                    IocpUserPayload::$user_variant(payload) => Some(payload),
                    _ => None,
                }
            }

            fn vtable() -> &'static OpVTable {
                static TABLE: OpVTable = OpVTable {
                    kind: <$SpecType as IocpOpSpec>::PAYLOAD_KIND,
                    multishot_ops: None,
                    submit: spec::IocpSpecDispatch::submit_shim::<$SpecType>,
                    on_complete: spec::IocpSpecDispatch::on_complete_shim::<$SpecType>,
                    completion_cleanup: spec::IocpSpecDispatch::completion_cleanup_shim::<$SpecType>,
                    orphan_cleanup: spec::IocpSpecDispatch::orphan_cleanup_shim::<$SpecType>,
                    get_fd: spec::IocpSpecDispatch::get_fd_shim::<$SpecType>,
                    bind_user_payload: spec::IocpSpecDispatch::bind_user_payload_shim::<$SpecType>,
                    unbind_user_payload: spec::IocpSpecDispatch::unbind_user_payload_shim::<$SpecType>,
                };
                &TABLE
            }
        }

        /// facade 的既有回退路径会避开 IOCP multishot 操作；这里仍保留统一 operation bound 所需的 payload 形状。
        /// The facade's existing fallback avoids IOCP multishot operations; this keeps the payload shape required by the shared operation bound.
        impl IntoPlatformOp<IocpSlotSpec> for $OpType {
            type SubmitPayload = $OpType;
            type RecordPayload = $OpType;
            type Output = $OpType;
            type Completion = $completion;

            const PAYLOAD_KIND: OpKind = <$SpecType as IocpOpSpec>::PAYLOAD_KIND;

            fn into_kernel_and_payload(self) -> (IocpKernelOp, Self::SubmitPayload) {
                let kernel_payload = <$SpecType as IocpOpSpec>::new_kernel_payload(&self);
                let op = IocpKernelOp {
                    vtable: <$SpecType as IocpOpErasure>::vtable(),
                    header: OverlappedEntry::new(
                        OpToken::from_registry_parts(0, Generation::ZERO)
                            .expect("zero token should be encodable"),
                    ),
                    payload: <$SpecType as IocpOpErasure>::erase_kernel_payload(kernel_payload),
                };
                (op, self)
            }

            fn payload_into_erased(payload: Self::SubmitPayload) -> IocpUserPayload {
                <$SpecType as IocpOpErasure>::erase_user_payload(payload)
            }

            fn try_record_from_erased(payload: IocpUserPayload) -> IocpResult<Self::RecordPayload> {
                <$SpecType as IocpOpErasure>::try_user_payload(payload)
            }

            fn complete(
                payload: Self::RecordPayload,
                res: IocpResult<usize>,
            ) -> OpCompletion<Self::Output, IocpError, Self::Completion> {
                let completion = <$SpecType as IocpOpSpec>::map_completion(&payload, res);
                OpCompletion::new(completion, payload)
            }
        }

    };
}

/// 擦除 IOCP 操作类型；提交 payload 保留在 slot 中，每次完成使用独立的记录 payload。
/// Erases an IOCP operation whose submit payload stays in the slot while each completion gets its own record payload.
macro_rules! impl_iocp_multishot_op_erasure {
    (
        $SpecType:ty,
        $OpType:ty,
        $user_variant:ident,
        $kernel_variant:ident,
        $record_variant:ident,
        $record:ty,
        $record_kind:ident,
        $completion:ty,
        $continuation:path
    ) => {
        impl IocpMultiShotErasure for $SpecType {
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

            fn erase_user_payload(payload: Self::User) -> IocpUserPayload {
                IocpUserPayload::$user_variant(payload)
            }

            fn user_payload_mut(payload: &mut IocpUserPayload) -> Option<&mut Self::User> {
                match payload {
                    IocpUserPayload::$user_variant(payload) => Some(payload),
                    _ => None,
                }
            }

            fn try_record_payload(payload: IocpUserPayload) -> IocpResult<Self::RecordPayload> {
                match payload {
                    IocpUserPayload::$record_variant(payload) => Ok(payload),
                    _ => Err(OpError::<IocpError>::payload_projection_mismatch_report(
                        stringify!($record),
                        "IocpUserPayload",
                    )),
                }
            }

            fn vtable() -> &'static OpVTable {
                static MULTI_SHOT: IocpMultiShotVTable = IocpMultiShotVTable {
                    record_kind: IocpRecordKind::$record_kind,
                    encode_accepted_socket: spec::IocpSpecDispatch::encode_accepted_socket,
                    rearm: spec::IocpSpecDispatch::submit_multishot_shim::<$SpecType>,
                    continuation: $continuation,
                };
                static TABLE: OpVTable = OpVTable {
                    kind: <$SpecType as IocpMultiShotSpec>::PAYLOAD_KIND,
                    multishot_ops: Some(&MULTI_SHOT),
                    submit: spec::IocpSpecDispatch::submit_multishot_shim::<$SpecType>,
                    on_complete: spec::IocpSpecDispatch::on_complete_multishot_shim::<$SpecType>,
                    completion_cleanup: spec::IocpSpecDispatch::completion_cleanup_multishot_shim::<$SpecType>,
                    orphan_cleanup: spec::IocpSpecDispatch::orphan_cleanup_multishot_shim::<$SpecType>,
                    get_fd: spec::IocpSpecDispatch::get_fd_multishot_shim::<$SpecType>,
                    bind_user_payload: spec::IocpSpecDispatch::bind_multishot_user_payload_shim::<$SpecType>,
                    unbind_user_payload: spec::IocpSpecDispatch::unbind_multishot_user_payload_shim::<$SpecType>,
                };
                &TABLE
            }
        }

        impl IntoPlatformOp<IocpSlotSpec> for $OpType {
            type SubmitPayload = $OpType;
            type RecordPayload = $record;
            type Output = $record;
            type Completion = $completion;

            const PAYLOAD_KIND: OpKind = <$SpecType as IocpMultiShotSpec>::PAYLOAD_KIND;

            fn into_kernel_and_payload(self) -> (IocpKernelOp, Self::SubmitPayload) {
                let kernel_payload = <$SpecType as IocpMultiShotSpec>::new_kernel_payload(&self);
                let op = IocpKernelOp {
                    vtable: <$SpecType as IocpMultiShotErasure>::vtable(),
                    header: OverlappedEntry::new(
                        OpToken::from_registry_parts(0, Generation::ZERO)
                            .expect("zero token should be encodable"),
                    ),
                    payload: <$SpecType as IocpMultiShotErasure>::erase_kernel_payload(
                        kernel_payload,
                    ),
                };
                (op, self)
            }

            fn payload_into_erased(payload: Self::SubmitPayload) -> IocpUserPayload {
                <$SpecType as IocpMultiShotErasure>::erase_user_payload(payload)
            }

            fn try_record_from_erased(payload: IocpUserPayload) -> IocpResult<Self::RecordPayload> {
                <$SpecType as IocpMultiShotErasure>::try_record_payload(payload)
            }

            fn complete(
                payload: Self::RecordPayload,
                res: IocpResult<usize>,
            ) -> OpCompletion<Self::Output, IocpError, Self::Completion> {
                let completion = <$SpecType as IocpMultiShotSpec>::map_completion(&payload, res);
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

/// 平台特定 IOCP 内核操作的别名。
/// Alias for the platform-specific IOCP kernel operation.
pub type IocpOp = IocpKernelOp;

pub enum IocpSlotSpec {}

impl CoreSlotSpec for IocpSlotSpec {
    type Op = IocpOp;
    type UserPayload = IocpUserPayload;
    type PlatformData = IocpOpState;
    type Sidecar = OverlappedEntry;
    type Error = IocpError;
    type Completion = usize;
    type CompletionDiagnostics = IocpCompletionDiagnostics;
}

macro_rules! impl_iocp_single_shot_operations {
    ($($operation:ty),+ $(,)?) => {
        $(impl SingleShotOperation<$operation> for IocpSlotSpec {})+
    };
}

impl_iocp_single_shot_operations!(
    ReadFixed,
    ReadRaw,
    WriteFixed,
    WriteRaw,
    Recv,
    OpSend,
    UdpSend,
    Close,
    Fsync,
    FsyncRaw,
    SyncFileRange,
    SyncFileRangeRaw,
    Fallocate,
    FallocateRaw,
    Timeout,
    Connect,
    UdpConnect,
    Accept,
    SendTo,
    Open,
    Wakeup,
    RecvProvided,
);

// ============================================================================
// 操作定义 / Operation definitions
// ============================================================================

impl IocpOpSpec for OpSpec<Timeout> {
    type User = Timeout;
    type KernelPayload = KernelRef<Self::User>;
    type Completion = usize;

    const PAYLOAD_KIND: OpKind = OpKind::Timeout;

    fn new_kernel_payload(user: &Self::User) -> Self::KernelPayload {
        KernelRef::new(user)
    }

    fn submit(
        header: &mut OverlappedEntry,
        payload: &mut Self::KernelPayload,
        ctx: &mut SubmitContext,
    ) -> IocpResult<SubmissionResult> {
        submit::SubmissionOps::submit_timeout(header, payload, ctx)
    }

    fn map_completion(
        _payload: &Self::User,
        res: IocpResult<usize>,
    ) -> IocpResult<Self::Completion> {
        res
    }
}

impl IocpOpSpec for OpSpec<Wakeup> {
    type User = Wakeup;
    type KernelPayload = KernelRef<Self::User>;
    type Completion = usize;

    const PAYLOAD_KIND: OpKind = OpKind::Wakeup;

    fn new_kernel_payload(user: &Self::User) -> Self::KernelPayload {
        KernelRef::new(user)
    }

    fn submit(
        header: &mut OverlappedEntry,
        payload: &mut Self::KernelPayload,
        ctx: &mut SubmitContext,
    ) -> IocpResult<SubmissionResult> {
        submit::SubmissionOps::submit_wakeup(header, payload, ctx)
    }

    fn map_completion(
        _payload: &Self::User,
        res: IocpResult<usize>,
    ) -> IocpResult<Self::Completion> {
        res
    }
}

impl_iocp_op_erasure!(
    file::FileOpSpec<ReadFixed>,
    ReadFixed,
    ReadFixed,
    Read,
    usize
);
impl_iocp_op_erasure!(file::FileOpSpec<ReadRaw>, ReadRaw, ReadRaw, ReadRaw, usize);
impl_iocp_op_erasure!(
    file::FileOpSpec<WriteFixed>,
    WriteFixed,
    WriteFixed,
    Write,
    usize
);
impl_iocp_op_erasure!(
    file::FileOpSpec<WriteRaw>,
    WriteRaw,
    WriteRaw,
    WriteRaw,
    usize
);
impl_iocp_op_erasure!(net::NetworkOpSpec<Recv>, Recv, Recv, Recv, usize);
impl_iocp_op_erasure!(net::NetworkOpSpec<OpSend>, OpSend, OpSend, Send, usize);
impl_iocp_op_erasure!(
    net::NetworkOpSpec<UdpSend>,
    UdpSend,
    UdpSend,
    UdpSend,
    usize
);
impl_iocp_op_erasure!(file::FileOpSpec<Close>, Close, Close, Close, usize);
impl_iocp_op_erasure!(file::FileOpSpec<Fsync>, Fsync, Fsync, Fsync, usize);
impl_iocp_op_erasure!(
    file::FileOpSpec<FsyncRaw>,
    FsyncRaw,
    FsyncRaw,
    FsyncRaw,
    usize
);
impl_iocp_op_erasure!(
    file::FileOpSpec<SyncFileRange>,
    SyncFileRange,
    SyncFileRange,
    SyncRange,
    usize
);
impl_iocp_op_erasure!(
    file::FileOpSpec<SyncFileRangeRaw>,
    SyncFileRangeRaw,
    SyncFileRangeRaw,
    SyncRangeRaw,
    usize
);
impl_iocp_op_erasure!(
    file::FileOpSpec<Fallocate>,
    Fallocate,
    Fallocate,
    Fallocate,
    usize
);
impl_iocp_op_erasure!(
    file::FileOpSpec<FallocateRaw>,
    FallocateRaw,
    FallocateRaw,
    FallocateRaw,
    usize
);
impl_iocp_op_erasure!(OpSpec<Timeout>, Timeout, Timeout, Timeout, usize);
impl_iocp_op_erasure!(
    net::NetworkOpSpec<Connect>,
    Connect,
    Connect,
    Connect,
    usize
);
impl_iocp_op_erasure!(
    net::NetworkOpSpec<UdpConnect>,
    UdpConnect,
    UdpConnect,
    UdpConnect,
    usize
);
impl_iocp_op_erasure!(
    net::NetworkOpSpec<Accept>,
    Accept,
    Accept,
    Accept,
    OwnedRawHandle
);
impl_iocp_op_erasure!(net::NetworkOpSpec<SendTo>, SendTo, SendTo, SendTo, usize);
impl_iocp_multishot_op_erasure!(
    net::NetworkOpSpec<AcceptMulti>,
    AcceptMulti,
    AcceptMulti,
    AcceptMulti,
    AcceptedSocket,
    AcceptedSocket,
    AcceptedSocket,
    OwnedRawHandle,
    spec::IocpSpecDispatch::more_on_success
);
impl_iocp_multishot_op_erasure!(
    net::NetworkOpSpec<RecvProvided>,
    RecvProvided,
    RecvProvided,
    RecvProvided,
    ProvidedBuf,
    ProvidedBuf,
    ProvidedBuf,
    usize,
    spec::IocpSpecDispatch::final_continuation
);
impl_iocp_multishot_op_erasure!(
    net::NetworkOpSpec<RecvMulti>,
    RecvMulti,
    RecvMulti,
    RecvMulti,
    ProvidedBuf,
    ProvidedBuf,
    ProvidedBuf,
    usize,
    spec::IocpSpecDispatch::more_on_success
);
impl_iocp_multishot_op_erasure!(
    net::NetworkOpSpec<UdpRecvMulti>,
    UdpRecvMulti,
    UdpRecvMulti,
    UdpRecvMulti,
    UdpRecvPacket,
    UdpRecvPacket,
    UdpRecvPacket,
    usize,
    spec::IocpSpecDispatch::more_on_success
);
impl_iocp_op_erasure!(file::FileOpSpec<Open>, Open, Open, Open, OwnedRawHandle);
impl_iocp_op_erasure!(OpSpec<Wakeup>, Wakeup, Wakeup, Wakeup, usize);

#[cfg(test)]
mod tests {
    use crate::{
        config::{IoFd, IocpHandle},
        op::{IocpUserPayload, UdpRecvMulti},
    };
    use veloq_buf::FixedBuf;
    use veloq_driver_core::{
        IntoPlatformOp, OpKind, ReceivePumpConfig, ReceivePumpState, ReceiveSlot, UdpRecvPacket,
        UdpRecvPacketBuf,
    };
    use veloq_std::{net::SocketAddr, num::NonZeroUsize, ptr::null_mut, time::Duration, vec};

    fn receive_pump() -> ReceivePumpState {
        let config = ReceivePumpConfig::new(
            NonZeroUsize::new(1).expect("non-zero depth"),
            NonZeroUsize::new(1).expect("non-zero queue"),
            NonZeroUsize::new(64).expect("non-zero datagram capacity"),
            Duration::from_secs(1),
        );
        let slots = vec![ReceiveSlot::new(
            0,
            FixedBuf::alloc_heap(NonZeroUsize::new(64).expect("non-zero buffer"), 0)
                .expect("receive slot allocation"),
        )]
        .into_boxed_slice();
        ReceivePumpState::try_new(config, slots).expect("valid receive pump")
    }

    #[test]
    fn udp_receive_multishot_keeps_submit_and_record_payloads_separate() {
        let fd = IoFd::Direct(IocpHandle::for_socket(null_mut()));
        let operation = UdpRecvMulti::from_backend(fd, receive_pump());
        let (mut kernel, submit_payload) = operation.into_kernel_and_payload();
        let mut erased = IocpUserPayload::UdpRecvMulti(submit_payload);

        kernel
            .bind_user_payload(&mut erased)
            .expect("persistent submit payload should bind");
        assert_eq!(kernel.kind(), OpKind::UdpRecvMulti);
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
