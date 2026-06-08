//! IOCP Platform-Specific Operation Definitions
//!
//! This module defines:
//! - `IocpKernelOp`: The Type-Erased kernel operation struct using Unions and VTables
//! - `OpVTable`: The virtual table for dynamic dispatch without enums
//! - `IntoPlatformOp` implementations split into `(KernelOp, UserPayload)`

pub(crate) mod overlapped;
pub(crate) mod slot;
mod spec;
pub(crate) mod submit;

pub use overlapped::OverlappedEntry;
use spec::{IocpOpErasure, IocpOpSpec, PayloadBinding};
pub(crate) use submit::SubmissionResult;

use std::ptr::NonNull;
use std::sync::Arc;

use crate::config::{IoFd, IocpHandle, OwnedRawHandle, RawHandle, RegisteredSlot};
use crate::error::{IocpDriverResult as DriverResult, IocpError, IocpResult};
use crate::ext::Extensions;
use crate::net::addr::SockAddrStorage;
use crate::rio::RioState;

use veloq_driver_core::driver::{CompletionToken, OpToken, PlatformOp};
use veloq_driver_core::op::{
    Accept as AcceptBase, Close as CloseBase, Connect as ConnectBase, Fallocate as FallocateBase,
    FallocateRaw as FallocateRawBase, Fsync as FsyncBase, FsyncRaw as FsyncRawBase, IntoPlatformOp,
    OpCompletion, OpKind, Open, ReadFixed as ReadFixedBase, ReadRaw as ReadRawBase,
    Recv as RecvBase, Send as OpSendBase, SendTo as SendToBase, SyncFileRange as SyncFileRangeBase,
    SyncFileRangeRaw as SyncFileRangeRawBase, Timeout, UdpConnect as UdpConnectBase,
    UdpRecv as UdpRecvBase, UdpRecvFrom as UdpRecvFromBase, UdpSend as UdpSendBase,
    Wakeup as WakeupBase, WriteFixed as WriteFixedBase, WriteRaw as WriteRawBase,
};

use windows_sys::Win32::Networking::WinSock::{SOCKADDR_IN, SOCKADDR_IN6, SOCKADDR_STORAGE};

use veloq_driver_core::driver::CompletionCleanupGuard;

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
// Type-Erased Payloads & VTable
// ============================================================================

pub enum IocpUserPayload {
    ReadFixed(ReadFixed),
    ReadRaw(ReadRaw),
    WriteFixed(WriteFixed),
    WriteRaw(WriteRaw),
    Recv(Recv),
    OpSend(OpSend),
    UdpRecv(UdpRecv),
    UdpSend(UdpSend),
    Close(Close),
    Fsync(Fsync),
    FsyncRaw(FsyncRaw),
    SyncFileRange(SyncFileRange),
    SyncFileRangeRaw(SyncFileRangeRaw),
    Fallocate(Fallocate),
    FallocateRaw(FallocateRaw),
    Timeout(Timeout),
    Connect(Connect),
    UdpConnect(UdpConnect),
    Accept(Accept),
    SendTo(SendTo),
    UdpRecvFrom(UdpRecvFrom),
    Open(Open),
    Wakeup(Wakeup),
}

unsafe impl Send for IocpUserPayload {}

pub(crate) enum IocpOpPayload {
    Read(KernelRef<ReadFixed>),
    ReadRaw(KernelRef<ReadRaw>),
    Write(KernelRef<WriteFixed>),
    WriteRaw(KernelRef<WriteRaw>),
    Recv(KernelRef<Recv>),
    Send(KernelRef<OpSend>),
    UdpRecv(KernelRef<UdpRecv>),
    UdpSend(KernelRef<UdpSend>),
    Close(KernelRef<Close>),
    Fsync(KernelRef<Fsync>),
    FsyncRaw(KernelRef<FsyncRaw>),
    SyncRange(KernelRef<SyncFileRange>),
    SyncRangeRaw(KernelRef<SyncFileRangeRaw>),
    Fallocate(KernelRef<Fallocate>),
    FallocateRaw(KernelRef<FallocateRaw>),
    Timeout(KernelRef<Timeout>),
    Connect(KernelRef<Connect>),
    UdpConnect(KernelRef<UdpConnect>),
    Accept(AcceptPayload),
    SendTo(SendToPayload),
    UdpRecvFrom(UdpRecvFromPayload),
    Open(OpenPayload),
    Wakeup(KernelRef<Wakeup>),
}

pub(crate) struct OpVTable {
    pub(crate) submit: fn(&mut IocpKernelOp, &mut SubmitContext) -> IocpResult<SubmissionResult>,
    pub(crate) on_complete:
        unsafe fn(&mut IocpKernelOp, result: usize, ext: &Extensions) -> IocpResult<usize>,
    pub(crate) completion_cleanup:
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

impl PlatformOp for IocpKernelOp {}

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
        unsafe { (self.vtable.completion_cleanup)(self, result) }
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

            fn try_user_payload(payload: IocpUserPayload) -> DriverResult<Self> {
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
                    header: OverlappedEntry::new(OpToken::new(0, 0)),
                    payload: <$OpType as IocpOpErasure>::erase_kernel_payload(kernel_payload),
                };
                (op, self)
            }

            fn payload_into_erased(payload: Self::UserPayload) -> IocpUserPayload {
                <$OpType as IocpOpErasure>::erase_user_payload(payload)
            }

            fn try_payload_from_erased(
                payload: IocpUserPayload,
            ) -> DriverResult<Self::UserPayload> {
                <$OpType as IocpOpErasure>::try_user_payload(payload)
            }

            fn complete(
                payload: Self::UserPayload,
                res: DriverResult<usize>,
            ) -> OpCompletion<Self::Output, IocpError, Self::Completion> {
                let completion = <$OpType as IocpOpSpec>::map_completion(&payload, res);
                OpCompletion::new(completion, payload)
            }
        }
    };
}

// ============================================================================
// Payload Structures for Complex Ops
// ============================================================================

/// Reference to a kernel operation.
pub(crate) struct PayloadRef<T> {
    user: Option<NonNull<T>>,
}

impl<T> PayloadRef<T> {
    #[inline]
    pub(crate) const fn unbound() -> Self {
        Self { user: None }
    }

    #[inline]
    pub(crate) fn bind(&mut self, user: NonNull<T>) {
        self.user = Some(user);
    }

    #[inline]
    pub(crate) fn clear(&mut self) {
        self.user = None;
    }

    #[inline]
    pub(crate) unsafe fn as_ref(&self) -> &T {
        let user = self.user.expect("IOCP user payload used before binding");
        // SAFETY: the payload is bound to the live slot payload before submission.
        unsafe { user.as_ref() }
    }

    #[inline]
    pub(crate) unsafe fn as_mut(&mut self) -> &mut T {
        let mut user = self.user.expect("IOCP user payload used before binding");
        // SAFETY: the payload is bound to the live slot payload before submission.
        unsafe { user.as_mut() }
    }
}

pub(crate) struct KernelRef<T> {
    pub(crate) user: PayloadRef<T>,
}

/// Payload for the socket accept operation.
pub(crate) const ACCEPT_EX_ADDR_SECTION_LEN: usize = std::mem::size_of::<SOCKADDR_STORAGE>() + 16;
pub(crate) const ACCEPT_EX_OUTPUT_BUFFER_LEN: usize = ACCEPT_EX_ADDR_SECTION_LEN * 2;

pub(crate) struct AcceptPayload {
    pub(crate) user: PayloadRef<Accept>,
    pub(crate) accept_buffer: [u8; ACCEPT_EX_OUTPUT_BUFFER_LEN],
    pub(crate) accept_socket: Option<OwnedRawHandle>,
}

/// Payload for the socket send-to operation.
pub(crate) struct SendToPayload {
    pub(crate) user: PayloadRef<SendTo>,
    pub(crate) addr: SockAddrStorage,
    pub(crate) addr_len: i32,
}

/// Payload for the socket recv-from operation.
pub(crate) struct UdpRecvFromPayload {
    pub(crate) user: PayloadRef<UdpRecvFrom>,
    pub(crate) addr: SockAddrStorage,
}

/// Payload for the file open operation.
pub(crate) struct OpenPayload {
    pub(crate) user: PayloadRef<Open>,
}

fn kernel_ref<T>(_user: &T) -> KernelRef<T> {
    KernelRef {
        user: PayloadRef::unbound(),
    }
}

impl<T> PayloadBinding<T> for KernelRef<T> {
    fn bind(&mut self, user: NonNull<T>) {
        self.user.bind(user);
    }

    fn clear(&mut self) {
        self.user.clear();
    }
}

impl PayloadBinding<Accept> for AcceptPayload {
    fn bind(&mut self, user: NonNull<Accept>) {
        self.user.bind(user);
    }

    fn clear(&mut self) {
        self.user.clear();
    }
}

impl PayloadBinding<SendTo> for SendToPayload {
    fn bind(&mut self, user: NonNull<SendTo>) {
        self.user.bind(user);
    }

    fn clear(&mut self) {
        self.user.clear();
    }
}

impl PayloadBinding<UdpRecvFrom> for UdpRecvFromPayload {
    fn bind(&mut self, user: NonNull<UdpRecvFrom>) {
        self.user.bind(user);
    }

    fn clear(&mut self) {
        self.user.clear();
    }
}

impl PayloadBinding<Open> for OpenPayload {
    fn bind(&mut self, user: NonNull<Open>) {
        self.user.bind(user);
    }

    fn clear(&mut self) {
        self.user.clear();
    }
}

/// Alias for the platform-specific IOCP kernel operation.
pub type IocpOp = IocpKernelOp;

// ============================================================================
// Op Definitions
// ============================================================================

impl IocpOpSpec for ReadFixed {
    type KernelPayload = KernelRef<Self>;
    type Completion = usize;

    const PAYLOAD_KIND: OpKind = OpKind::ReadFixed;

    fn new_kernel_payload(user: &Self) -> Self::KernelPayload {
        kernel_ref(user)
    }

    fn submit(
        header: &mut OverlappedEntry,
        payload: &mut Self::KernelPayload,
        ctx: &mut SubmitContext,
    ) -> IocpResult<SubmissionResult> {
        submit::submit_read_fixed(header, payload, ctx)
    }

    unsafe fn get_fd(payload: &Self::KernelPayload) -> Option<IoFd> {
        unsafe { submit::get_fd_read_fixed(payload) }
    }

    fn map_completion(_payload: &Self, res: DriverResult<usize>) -> DriverResult<Self::Completion> {
        res
    }
}

impl IocpOpSpec for ReadRaw {
    type KernelPayload = KernelRef<Self>;
    type Completion = usize;

    const PAYLOAD_KIND: OpKind = OpKind::ReadFixed;

    fn new_kernel_payload(user: &Self) -> Self::KernelPayload {
        kernel_ref(user)
    }

    fn submit(
        header: &mut OverlappedEntry,
        payload: &mut Self::KernelPayload,
        ctx: &mut SubmitContext,
    ) -> IocpResult<SubmissionResult> {
        submit::submit_read_raw(header, payload, ctx)
    }

    fn map_completion(_payload: &Self, res: DriverResult<usize>) -> DriverResult<Self::Completion> {
        res
    }
}

impl IocpOpSpec for WriteFixed {
    type KernelPayload = KernelRef<Self>;
    type Completion = usize;

    const PAYLOAD_KIND: OpKind = OpKind::WriteFixed;

    fn new_kernel_payload(user: &Self) -> Self::KernelPayload {
        kernel_ref(user)
    }

    fn submit(
        header: &mut OverlappedEntry,
        payload: &mut Self::KernelPayload,
        ctx: &mut SubmitContext,
    ) -> IocpResult<SubmissionResult> {
        submit::submit_write_fixed(header, payload, ctx)
    }

    unsafe fn get_fd(payload: &Self::KernelPayload) -> Option<IoFd> {
        unsafe { submit::get_fd_write_fixed(payload) }
    }

    fn map_completion(_payload: &Self, res: DriverResult<usize>) -> DriverResult<Self::Completion> {
        res
    }
}

impl IocpOpSpec for WriteRaw {
    type KernelPayload = KernelRef<Self>;
    type Completion = usize;

    const PAYLOAD_KIND: OpKind = OpKind::WriteFixed;

    fn new_kernel_payload(user: &Self) -> Self::KernelPayload {
        kernel_ref(user)
    }

    fn submit(
        header: &mut OverlappedEntry,
        payload: &mut Self::KernelPayload,
        ctx: &mut SubmitContext,
    ) -> IocpResult<SubmissionResult> {
        submit::submit_write_raw(header, payload, ctx)
    }

    fn map_completion(_payload: &Self, res: DriverResult<usize>) -> DriverResult<Self::Completion> {
        res
    }
}

impl IocpOpSpec for Recv {
    type KernelPayload = KernelRef<Self>;
    type Completion = usize;

    const PAYLOAD_KIND: OpKind = OpKind::Recv;

    fn new_kernel_payload(user: &Self) -> Self::KernelPayload {
        kernel_ref(user)
    }

    fn submit(
        header: &mut OverlappedEntry,
        payload: &mut Self::KernelPayload,
        ctx: &mut SubmitContext,
    ) -> IocpResult<SubmissionResult> {
        submit::submit_recv(header, payload, ctx)
    }

    unsafe fn get_fd(payload: &Self::KernelPayload) -> Option<IoFd> {
        unsafe { submit::get_fd_recv(payload) }
    }

    fn map_completion(_payload: &Self, res: DriverResult<usize>) -> DriverResult<Self::Completion> {
        res
    }
}

impl IocpOpSpec for OpSend {
    type KernelPayload = KernelRef<Self>;
    type Completion = usize;

    const PAYLOAD_KIND: OpKind = OpKind::Send;

    fn new_kernel_payload(user: &Self) -> Self::KernelPayload {
        kernel_ref(user)
    }

    fn submit(
        header: &mut OverlappedEntry,
        payload: &mut Self::KernelPayload,
        ctx: &mut SubmitContext,
    ) -> IocpResult<SubmissionResult> {
        submit::submit_send(header, payload, ctx)
    }

    unsafe fn get_fd(payload: &Self::KernelPayload) -> Option<IoFd> {
        unsafe { submit::get_fd_send(payload) }
    }

    fn map_completion(_payload: &Self, res: DriverResult<usize>) -> DriverResult<Self::Completion> {
        res
    }
}

impl IocpOpSpec for UdpRecv {
    type KernelPayload = KernelRef<Self>;
    type Completion = usize;

    const PAYLOAD_KIND: OpKind = OpKind::UdpRecv;

    fn new_kernel_payload(user: &Self) -> Self::KernelPayload {
        kernel_ref(user)
    }

    fn submit(
        header: &mut OverlappedEntry,
        payload: &mut Self::KernelPayload,
        ctx: &mut SubmitContext,
    ) -> IocpResult<SubmissionResult> {
        submit::submit_udp_recv(header, payload, ctx)
    }

    unsafe fn get_fd(payload: &Self::KernelPayload) -> Option<IoFd> {
        unsafe { submit::get_fd_udp_recv(payload) }
    }

    fn map_completion(_payload: &Self, res: DriverResult<usize>) -> DriverResult<Self::Completion> {
        res
    }
}

impl IocpOpSpec for UdpSend {
    type KernelPayload = KernelRef<Self>;
    type Completion = usize;

    const PAYLOAD_KIND: OpKind = OpKind::UdpSend;

    fn new_kernel_payload(user: &Self) -> Self::KernelPayload {
        kernel_ref(user)
    }

    fn submit(
        header: &mut OverlappedEntry,
        payload: &mut Self::KernelPayload,
        ctx: &mut SubmitContext,
    ) -> IocpResult<SubmissionResult> {
        submit::submit_udp_send(header, payload, ctx)
    }

    unsafe fn get_fd(payload: &Self::KernelPayload) -> Option<IoFd> {
        unsafe { submit::get_fd_udp_send(payload) }
    }

    fn map_completion(_payload: &Self, res: DriverResult<usize>) -> DriverResult<Self::Completion> {
        res
    }
}

impl IocpOpSpec for Close {
    type KernelPayload = KernelRef<Self>;
    type Completion = usize;

    const PAYLOAD_KIND: OpKind = OpKind::Close;

    fn new_kernel_payload(user: &Self) -> Self::KernelPayload {
        kernel_ref(user)
    }

    fn submit(
        header: &mut OverlappedEntry,
        payload: &mut Self::KernelPayload,
        ctx: &mut SubmitContext,
    ) -> IocpResult<SubmissionResult> {
        submit::submit_close(header, payload, ctx)
    }

    unsafe fn get_fd(payload: &Self::KernelPayload) -> Option<IoFd> {
        unsafe { submit::get_fd_close(payload) }
    }

    fn map_completion(_payload: &Self, res: DriverResult<usize>) -> DriverResult<Self::Completion> {
        res
    }
}

impl IocpOpSpec for Fsync {
    type KernelPayload = KernelRef<Self>;
    type Completion = usize;

    const PAYLOAD_KIND: OpKind = OpKind::Fsync;

    fn new_kernel_payload(user: &Self) -> Self::KernelPayload {
        kernel_ref(user)
    }

    fn submit(
        header: &mut OverlappedEntry,
        payload: &mut Self::KernelPayload,
        ctx: &mut SubmitContext,
    ) -> IocpResult<SubmissionResult> {
        submit::submit_fsync(header, payload, ctx)
    }

    unsafe fn get_fd(payload: &Self::KernelPayload) -> Option<IoFd> {
        unsafe { submit::get_fd_fsync(payload) }
    }

    fn map_completion(_payload: &Self, res: DriverResult<usize>) -> DriverResult<Self::Completion> {
        res
    }
}

impl IocpOpSpec for FsyncRaw {
    type KernelPayload = KernelRef<Self>;
    type Completion = usize;

    const PAYLOAD_KIND: OpKind = OpKind::Fsync;

    fn new_kernel_payload(user: &Self) -> Self::KernelPayload {
        kernel_ref(user)
    }

    fn submit(
        header: &mut OverlappedEntry,
        payload: &mut Self::KernelPayload,
        ctx: &mut SubmitContext,
    ) -> IocpResult<SubmissionResult> {
        submit::submit_fsync_raw(header, payload, ctx)
    }

    fn map_completion(_payload: &Self, res: DriverResult<usize>) -> DriverResult<Self::Completion> {
        res
    }
}

impl IocpOpSpec for SyncFileRange {
    type KernelPayload = KernelRef<Self>;
    type Completion = usize;

    const PAYLOAD_KIND: OpKind = OpKind::SyncFileRange;

    fn new_kernel_payload(user: &Self) -> Self::KernelPayload {
        kernel_ref(user)
    }

    fn submit(
        header: &mut OverlappedEntry,
        payload: &mut Self::KernelPayload,
        ctx: &mut SubmitContext,
    ) -> IocpResult<SubmissionResult> {
        submit::submit_sync_range(header, payload, ctx)
    }

    unsafe fn get_fd(payload: &Self::KernelPayload) -> Option<IoFd> {
        unsafe { submit::get_fd_sync_range(payload) }
    }

    fn map_completion(_payload: &Self, res: DriverResult<usize>) -> DriverResult<Self::Completion> {
        res
    }
}

impl IocpOpSpec for SyncFileRangeRaw {
    type KernelPayload = KernelRef<Self>;
    type Completion = usize;

    const PAYLOAD_KIND: OpKind = OpKind::SyncFileRange;

    fn new_kernel_payload(user: &Self) -> Self::KernelPayload {
        kernel_ref(user)
    }

    fn submit(
        header: &mut OverlappedEntry,
        payload: &mut Self::KernelPayload,
        ctx: &mut SubmitContext,
    ) -> IocpResult<SubmissionResult> {
        submit::submit_sync_range_raw(header, payload, ctx)
    }

    fn map_completion(_payload: &Self, res: DriverResult<usize>) -> DriverResult<Self::Completion> {
        res
    }
}

impl IocpOpSpec for Fallocate {
    type KernelPayload = KernelRef<Self>;
    type Completion = usize;

    const PAYLOAD_KIND: OpKind = OpKind::Fallocate;

    fn new_kernel_payload(user: &Self) -> Self::KernelPayload {
        kernel_ref(user)
    }

    fn submit(
        header: &mut OverlappedEntry,
        payload: &mut Self::KernelPayload,
        ctx: &mut SubmitContext,
    ) -> IocpResult<SubmissionResult> {
        submit::submit_fallocate(header, payload, ctx)
    }

    unsafe fn get_fd(payload: &Self::KernelPayload) -> Option<IoFd> {
        unsafe { submit::get_fd_fallocate(payload) }
    }

    fn map_completion(_payload: &Self, res: DriverResult<usize>) -> DriverResult<Self::Completion> {
        res
    }
}

impl IocpOpSpec for FallocateRaw {
    type KernelPayload = KernelRef<Self>;
    type Completion = usize;

    const PAYLOAD_KIND: OpKind = OpKind::Fallocate;

    fn new_kernel_payload(user: &Self) -> Self::KernelPayload {
        kernel_ref(user)
    }

    fn submit(
        header: &mut OverlappedEntry,
        payload: &mut Self::KernelPayload,
        ctx: &mut SubmitContext,
    ) -> IocpResult<SubmissionResult> {
        submit::submit_fallocate_raw(header, payload, ctx)
    }

    fn map_completion(_payload: &Self, res: DriverResult<usize>) -> DriverResult<Self::Completion> {
        res
    }
}

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

    fn map_completion(_payload: &Self, res: DriverResult<usize>) -> DriverResult<Self::Completion> {
        res
    }
}

impl IocpOpSpec for Connect {
    type KernelPayload = KernelRef<Self>;
    type Completion = usize;

    const PAYLOAD_KIND: OpKind = OpKind::Connect;

    fn new_kernel_payload(user: &Self) -> Self::KernelPayload {
        kernel_ref(user)
    }

    fn submit(
        header: &mut OverlappedEntry,
        payload: &mut Self::KernelPayload,
        ctx: &mut SubmitContext,
    ) -> IocpResult<SubmissionResult> {
        submit::submit_connect(header, payload, ctx)
    }

    unsafe fn on_complete(
        header: &mut OverlappedEntry,
        payload: &mut Self::KernelPayload,
        result: usize,
        ext: &Extensions,
    ) -> IocpResult<usize> {
        unsafe { submit::on_complete_connect(header, payload, result, ext) }
    }

    unsafe fn get_fd(payload: &Self::KernelPayload) -> Option<IoFd> {
        unsafe { submit::get_fd_connect(payload) }
    }

    fn map_completion(_payload: &Self, res: DriverResult<usize>) -> DriverResult<Self::Completion> {
        res
    }
}

impl IocpOpSpec for UdpConnect {
    type KernelPayload = KernelRef<Self>;
    type Completion = usize;

    const PAYLOAD_KIND: OpKind = OpKind::UdpConnect;

    fn new_kernel_payload(user: &Self) -> Self::KernelPayload {
        kernel_ref(user)
    }

    fn submit(
        header: &mut OverlappedEntry,
        payload: &mut Self::KernelPayload,
        ctx: &mut SubmitContext,
    ) -> IocpResult<SubmissionResult> {
        submit::submit_udp_connect(header, payload, ctx)
    }

    unsafe fn on_complete(
        header: &mut OverlappedEntry,
        payload: &mut Self::KernelPayload,
        result: usize,
        ext: &Extensions,
    ) -> IocpResult<usize> {
        unsafe { submit::on_complete_udp_connect(header, payload, result, ext) }
    }

    unsafe fn get_fd(payload: &Self::KernelPayload) -> Option<IoFd> {
        unsafe { submit::get_fd_udp_connect(payload) }
    }

    fn map_completion(_payload: &Self, res: DriverResult<usize>) -> DriverResult<Self::Completion> {
        res
    }
}

impl IocpOpSpec for Accept {
    type KernelPayload = AcceptPayload;
    type Completion = OwnedRawHandle;

    const PAYLOAD_KIND: OpKind = OpKind::Accept;

    fn new_kernel_payload(_user: &Self) -> Self::KernelPayload {
        AcceptPayload {
            user: PayloadRef::unbound(),
            accept_buffer: [0; ACCEPT_EX_OUTPUT_BUFFER_LEN],
            accept_socket: None,
        }
    }

    fn submit(
        header: &mut OverlappedEntry,
        payload: &mut Self::KernelPayload,
        ctx: &mut SubmitContext,
    ) -> IocpResult<SubmissionResult> {
        submit::submit_accept(header, payload, ctx)
    }

    unsafe fn on_complete(
        header: &mut OverlappedEntry,
        payload: &mut Self::KernelPayload,
        result: usize,
        ext: &Extensions,
    ) -> IocpResult<usize> {
        unsafe { submit::on_complete_accept(header, payload, result, ext) }
    }

    fn completion_cleanup(
        _payload: &mut Self::KernelPayload,
        result: &IocpResult<usize>,
    ) -> CompletionCleanupGuard {
        submit::completion_cleanup_close_socket(result)
    }

    unsafe fn get_fd(payload: &Self::KernelPayload) -> Option<IoFd> {
        unsafe { submit::get_fd_accept(payload) }
    }

    fn map_completion(_payload: &Self, res: DriverResult<usize>) -> DriverResult<Self::Completion> {
        res.map(|raw| unsafe {
            OwnedRawHandle::from_raw_owned(RawHandle::new(IocpHandle::for_socket(raw as _)))
        })
    }
}

impl IocpOpSpec for SendTo {
    type KernelPayload = SendToPayload;
    type Completion = usize;

    const PAYLOAD_KIND: OpKind = OpKind::SendTo;

    fn new_kernel_payload(user: &Self) -> Self::KernelPayload {
        let (addr, _raw_addr_len) = crate::net::addr::socket_addr_to_storage(user.addr);
        let addr_len = match user.addr {
            std::net::SocketAddr::V4(_) => std::mem::size_of::<SOCKADDR_IN>() as i32,
            std::net::SocketAddr::V6(_) => std::mem::size_of::<SOCKADDR_IN6>() as i32,
        };
        SendToPayload {
            user: PayloadRef::unbound(),
            addr,
            addr_len,
        }
    }

    fn submit(
        header: &mut OverlappedEntry,
        payload: &mut Self::KernelPayload,
        ctx: &mut SubmitContext,
    ) -> IocpResult<SubmissionResult> {
        submit::submit_send_to(header, payload, ctx)
    }

    unsafe fn get_fd(payload: &Self::KernelPayload) -> Option<IoFd> {
        unsafe { submit::get_fd_send_to(payload) }
    }

    fn map_completion(_payload: &Self, res: DriverResult<usize>) -> DriverResult<Self::Completion> {
        res
    }
}

impl IocpOpSpec for UdpRecvFrom {
    type KernelPayload = UdpRecvFromPayload;
    type Completion = usize;

    const PAYLOAD_KIND: OpKind = OpKind::UdpRecvFrom;

    fn new_kernel_payload(_user: &Self) -> Self::KernelPayload {
        UdpRecvFromPayload {
            user: PayloadRef::unbound(),
            addr: SockAddrStorage::default(),
        }
    }

    fn submit(
        header: &mut OverlappedEntry,
        payload: &mut Self::KernelPayload,
        ctx: &mut SubmitContext,
    ) -> IocpResult<SubmissionResult> {
        submit::submit_udp_recv_from(header, payload, ctx)
    }

    unsafe fn on_complete(
        header: &mut OverlappedEntry,
        payload: &mut Self::KernelPayload,
        result: usize,
        ext: &Extensions,
    ) -> IocpResult<usize> {
        unsafe { submit::on_complete_udp_recv_from(header, payload, result, ext) }
    }

    unsafe fn get_fd(payload: &Self::KernelPayload) -> Option<IoFd> {
        unsafe { submit::get_fd_udp_recv_from(payload) }
    }

    fn map_completion(_payload: &Self, res: DriverResult<usize>) -> DriverResult<Self::Completion> {
        res
    }
}

impl IocpOpSpec for Open {
    type KernelPayload = OpenPayload;
    type Completion = OwnedRawHandle;

    const PAYLOAD_KIND: OpKind = OpKind::Open;

    fn new_kernel_payload(_user: &Self) -> Self::KernelPayload {
        OpenPayload {
            user: PayloadRef::unbound(),
        }
    }

    fn submit(
        header: &mut OverlappedEntry,
        payload: &mut Self::KernelPayload,
        ctx: &mut SubmitContext,
    ) -> IocpResult<SubmissionResult> {
        submit::submit_open(header, payload, ctx)
    }

    fn completion_cleanup(
        _payload: &mut Self::KernelPayload,
        result: &IocpResult<usize>,
    ) -> CompletionCleanupGuard {
        submit::completion_cleanup_close_file(result)
    }

    fn map_completion(_payload: &Self, res: DriverResult<usize>) -> DriverResult<Self::Completion> {
        res.map(|raw| unsafe {
            OwnedRawHandle::from_raw_owned(RawHandle::new(IocpHandle::for_file(raw as _)))
        })
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

    fn map_completion(_payload: &Self, res: DriverResult<usize>) -> DriverResult<Self::Completion> {
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
