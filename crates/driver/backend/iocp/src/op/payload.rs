use veloq_std::{any::type_name, mem::size_of, ptr::NonNull};

use windows_sys::Win32::Networking::WinSock::SOCKADDR_STORAGE;

use crate::{
    config::OwnedRawHandle,
    error::{IocpError, IocpResult},
    net::addr::SockAddrStorage,
    op::{
        Accept, AcceptMulti, Close, Connect, Fallocate, FallocateRaw, Fsync, FsyncRaw, OpSend,
        Open, ReadFixed, ReadRaw, Recv, RecvMulti, RecvProvided, SendTo, SyncFileRange,
        SyncFileRangeRaw, Timeout, UdpConnect, UdpRecv, UdpRecvFrom, UdpSend, Wakeup, WriteFixed,
        WriteRaw, spec::PayloadBinding,
    },
};

use diagweave::prelude::*;

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
    /// 下面三个操作 IOCP 提交不了（见 `op.rs` 的 `impl_iocp_unsupported_op!`）。变体仍然
    /// 要在：提交路径先把 payload 擦除进 slot、再由 `submit` 同步失败把它原样取回，所以
    /// 即使一条完成都不会产生，这一趟往返也是走完整的。
    AcceptMulti(AcceptMulti),
    RecvProvided(RecvProvided),
    RecvMulti(RecvMulti),
}

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
    /// 一个 IOCP 提交不了的操作的内核 payload。
    ///
    /// 没有字段，因为没有内核请求可以描述：它唯一的用途是让 `IocpKernelOp` 有个能被构造出
    /// 来的形状，好让提交路径走到 `submit` 那一步同步失败。三个不支持的操作共用它，也共用
    /// 同一张 vtable。
    Unsupported,
}

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
    pub(crate) unsafe fn as_ref(&self) -> IocpResult<&T> {
        let user = self.user.ok_or_else(|| {
            IocpError::InvalidState
                .to_report()
                .with_ctx("payload_type", type_name::<T>())
                .attach_note("IOCP user payload used before binding")
        })?;
        // SAFETY: the payload is bound to the live slot payload before submission.
        Ok(unsafe { user.as_ref() })
    }

    #[inline]
    pub(crate) unsafe fn as_mut(&mut self) -> IocpResult<&mut T> {
        let mut user = self.user.ok_or_else(|| {
            IocpError::InvalidState
                .to_report()
                .with_ctx("payload_type", type_name::<T>())
                .attach_note("IOCP user payload used before binding")
        })?;
        // SAFETY: the payload is bound to the live slot payload before submission.
        Ok(unsafe { user.as_mut() })
    }
}

pub(crate) struct KernelRef<T> {
    pub(crate) user: PayloadRef<T>,
}

/// Payload for the socket accept operation.
pub(crate) const ACCEPT_EX_ADDR_SECTION_LEN: usize = size_of::<SOCKADDR_STORAGE>() + 16;
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

pub(crate) fn kernel_ref<T>(_user: &T) -> KernelRef<T> {
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
