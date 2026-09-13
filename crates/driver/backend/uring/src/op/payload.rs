pub(crate) use veloq_driver_core::op::types::{
    Accept as CoreAccept, AcceptMulti as CoreAcceptMulti, AcceptedSocket, Close as CoreClose,
    Connect as CoreConnect, Fallocate as CoreFallocate, FallocateRaw as CoreFallocateRaw,
    Fsync as CoreFsync, FsyncRaw as CoreFsyncRaw, Open, ProvidedBuf, ReadFixed as CoreReadFixed,
    ReadRaw as CoreReadRaw, Recv as CoreRecv, RecvMulti as CoreRecvMulti,
    RecvProvided as CoreRecvProvided, Send as CoreSend, SendTo as CoreSendTo,
    SyncFileRange as CoreSyncFileRange, SyncFileRangeRaw as CoreSyncFileRangeRaw, Timeout,
    UdpConnect as CoreUdpConnect, UdpRecv as CoreUdpRecv, UdpRecvFrom as CoreUdpRecvFrom,
    UdpSend as CoreUdpSend, Wakeup as CoreWakeup, WriteFixed as CoreWriteFixed,
    WriteRaw as CoreWriteRaw,
};

use crate::{
    config::{SockAddrStorage, UringRawHandle},
    error::UringResult,
    net::{bounded_sockaddr_bytes, socket_addr_to_storage, to_socket_addr},
};
use io_uring::types::Timespec;
use veloq_buf::BufIoRangeError;
use veloq_std::{
    marker::{PhantomData, PhantomPinned},
    mem,
    net::SocketAddr,
    ptr,
};

pub(crate) type ReadFixed = CoreReadFixed<UringRawHandle>;
pub(crate) type ReadRaw = CoreReadRaw<UringRawHandle>;
pub(crate) type WriteFixed = CoreWriteFixed<UringRawHandle>;
pub(crate) type WriteRaw = CoreWriteRaw<UringRawHandle>;
pub(crate) type Recv = CoreRecv<UringRawHandle>;
pub(crate) type RecvProvided = CoreRecvProvided<UringRawHandle>;
pub(crate) type RecvMulti = CoreRecvMulti<UringRawHandle>;
pub(crate) type OpSend = CoreSend<UringRawHandle>;
pub(crate) type UdpRecv = CoreUdpRecv<UringRawHandle>;
pub(crate) type UdpSend = CoreUdpSend<UringRawHandle>;
pub(crate) type Connect = CoreConnect<UringRawHandle, SockAddrStorage>;
pub(crate) type UdpConnect = CoreUdpConnect<UringRawHandle, SockAddrStorage>;
pub(crate) type Close = CoreClose<UringRawHandle>;
pub(crate) type Fsync = CoreFsync<UringRawHandle>;
pub(crate) type FsyncRaw = CoreFsyncRaw<UringRawHandle>;
pub(crate) type SyncFileRange = CoreSyncFileRange<UringRawHandle>;
pub(crate) type SyncFileRangeRaw = CoreSyncFileRangeRaw<UringRawHandle>;
pub(crate) type Fallocate = CoreFallocate<UringRawHandle>;
pub(crate) type FallocateRaw = CoreFallocateRaw<UringRawHandle>;
pub(crate) type Accept = CoreAccept<UringRawHandle, SockAddrStorage>;
pub(crate) type AcceptMulti = CoreAcceptMulti<UringRawHandle>;
pub(crate) type SendTo = CoreSendTo<UringRawHandle>;
pub(crate) type UdpRecvFrom = CoreUdpRecvFrom<UringRawHandle>;
pub(crate) type Wakeup = CoreWakeup<UringRawHandle>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum UringPayloadTag {
    ReadFixed,
    Read,
    ReadRaw,
    WriteFixed,
    Write,
    WriteRaw,
    Recv,
    RecvProvided,
    RecvMulti,
    ProvidedBuf,
    OpSend,
    Send,
    UdpRecv,
    UdpSend,
    Connect,
    UdpConnect,
    Close,
    Fsync,
    FsyncRaw,
    SyncFileRange,
    SyncRange,
    SyncFileRangeRaw,
    SyncRangeRaw,
    Fallocate,
    FallocateRaw,
    Accept,
    AcceptMulti,
    AcceptedSocket,
    SendTo,
    UdpRecvFrom,
    Open,
    Wakeup,
    Timeout,
}

impl UringPayloadTag {
    pub(crate) const fn name(self) -> &'static str {
        match self {
            Self::ReadFixed => "ReadFixed",
            Self::Read => "Read",
            Self::ReadRaw => "ReadRaw",
            Self::WriteFixed => "WriteFixed",
            Self::Write => "Write",
            Self::WriteRaw => "WriteRaw",
            Self::Recv => "Recv",
            Self::RecvProvided => "RecvProvided",
            Self::RecvMulti => "RecvMulti",
            Self::ProvidedBuf => "ProvidedBuf",
            Self::OpSend => "OpSend",
            Self::Send => "Send",
            Self::UdpRecv => "UdpRecv",
            Self::UdpSend => "UdpSend",
            Self::Connect => "Connect",
            Self::UdpConnect => "UdpConnect",
            Self::Close => "Close",
            Self::Fsync => "Fsync",
            Self::FsyncRaw => "FsyncRaw",
            Self::SyncFileRange => "SyncFileRange",
            Self::SyncRange => "SyncRange",
            Self::SyncFileRangeRaw => "SyncFileRangeRaw",
            Self::SyncRangeRaw => "SyncRangeRaw",
            Self::Fallocate => "Fallocate",
            Self::FallocateRaw => "FallocateRaw",
            Self::Accept => "Accept",
            Self::AcceptMulti => "AcceptMulti",
            Self::AcceptedSocket => "AcceptedSocket",
            Self::SendTo => "SendTo",
            Self::UdpRecvFrom => "UdpRecvFrom",
            Self::Open => "Open",
            Self::Wakeup => "Wakeup",
            Self::Timeout => "Timeout",
        }
    }
}

pub enum UringUserPayload {
    ReadFixed(ReadFixed),
    ReadRaw(ReadRaw),
    WriteFixed(WriteFixed),
    WriteRaw(WriteRaw),
    Recv(Recv),
    /// provided-buffer recv 的**提交** payload：提交时还没有 buffer 可言。
    RecvProvided(RecvProvided),
    /// multishot provided-buffer recv 的**提交** payload：一直留在 slot 里直到操作终止。
    RecvMulti(RecvMulti),
    /// provided-buffer recv **每条完成**的产物：内核在数据到达时才从环里挑出来的那个
    /// buffer（`None` 表示这条完成一个 buffer 都没消费，例如 `-ENOBUFS`）。
    ///
    /// 单发 [`RecvProvided`] 与 multishot [`RecvMulti`] 共用它——「产物不是提交物」与
    /// 「一次提交多条完成」是两件正交的事，这个变体只表达前者。
    ProvidedBuf(ProvidedBuf),
    OpSend(OpSend),
    UdpRecv(UdpRecv),
    UdpSend(UdpSend),
    Connect(Connect),
    UdpConnect(UdpConnect),
    Close(Close),
    Fsync(Fsync),
    FsyncRaw(FsyncRaw),
    SyncFileRange(SyncFileRange),
    SyncFileRangeRaw(SyncFileRangeRaw),
    Fallocate(Fallocate),
    FallocateRaw(FallocateRaw),
    Accept(Accept),
    /// multishot accept 的**提交** payload：一直留在 slot 里直到操作终止。
    AcceptMulti(AcceptMulti),
    /// multishot accept **每条完成**的产物。与上一个变体的区别见
    /// [`veloq_driver_core::op::IntoPlatformOp`] 的 `SubmitPayload` / `RecordPayload`。
    AcceptedSocket(AcceptedSocket),
    SendTo(SendTo),
    UdpRecvFrom(UdpRecvFrom),
    Open(Open),
    Wakeup(Wakeup),
    Timeout(Timeout),
}

impl UringUserPayload {
    pub(crate) const fn tag(&self) -> UringPayloadTag {
        match self {
            Self::ReadFixed(_) => UringPayloadTag::ReadFixed,
            Self::ReadRaw(_) => UringPayloadTag::ReadRaw,
            Self::WriteFixed(_) => UringPayloadTag::WriteFixed,
            Self::WriteRaw(_) => UringPayloadTag::WriteRaw,
            Self::Recv(_) => UringPayloadTag::Recv,
            Self::RecvProvided(_) => UringPayloadTag::RecvProvided,
            Self::RecvMulti(_) => UringPayloadTag::RecvMulti,
            Self::ProvidedBuf(_) => UringPayloadTag::ProvidedBuf,
            Self::OpSend(_) => UringPayloadTag::OpSend,
            Self::UdpRecv(_) => UringPayloadTag::UdpRecv,
            Self::UdpSend(_) => UringPayloadTag::UdpSend,
            Self::Connect(_) => UringPayloadTag::Connect,
            Self::UdpConnect(_) => UringPayloadTag::UdpConnect,
            Self::Close(_) => UringPayloadTag::Close,
            Self::Fsync(_) => UringPayloadTag::Fsync,
            Self::FsyncRaw(_) => UringPayloadTag::FsyncRaw,
            Self::SyncFileRange(_) => UringPayloadTag::SyncFileRange,
            Self::SyncFileRangeRaw(_) => UringPayloadTag::SyncFileRangeRaw,
            Self::Fallocate(_) => UringPayloadTag::Fallocate,
            Self::FallocateRaw(_) => UringPayloadTag::FallocateRaw,
            Self::Accept(_) => UringPayloadTag::Accept,
            Self::AcceptMulti(_) => UringPayloadTag::AcceptMulti,
            Self::AcceptedSocket(_) => UringPayloadTag::AcceptedSocket,
            Self::SendTo(_) => UringPayloadTag::SendTo,
            Self::UdpRecvFrom(_) => UringPayloadTag::UdpRecvFrom,
            Self::Open(_) => UringPayloadTag::Open,
            Self::Wakeup(_) => UringPayloadTag::Wakeup,
            Self::Timeout(_) => UringPayloadTag::Timeout,
        }
    }
}

pub(crate) struct KernelRef<T> {
    pub(crate) marker: PhantomData<T>,
}

pub(crate) fn kernel_ref<T>(_user: &T) -> KernelRef<T> {
    KernelRef {
        marker: PhantomData,
    }
}

pub(crate) struct AcceptPayload {}

/// Kernel payload for [`SendTo`].
///
/// # Safety / Self-Referential Layout
///
/// `msghdr.msg_name` and `msghdr.msg_iov` contain self-referential pointers to `msg_name`
/// and `iovec` within this structure, which are populated during `make_sqe_send_to`.
/// Once populated, this payload MUST NOT be moved in memory until the operation completes
/// or fails in the kernel. `PhantomPinned` enforces !Unpin in the type system.
pub(crate) struct SendToPayload {
    msg_name: libc::sockaddr_storage,
    msg_namelen: libc::socklen_t,
    iovec: [libc::iovec; 1],
    msghdr: libc::msghdr,
    _pin: PhantomPinned,
}

/// Kernel payload for [`UdpRecvFrom`].
///
/// # Safety / Self-Referential Layout
///
/// `msghdr.msg_name` and `msghdr.msg_iov` contain self-referential pointers to `msg_name`
/// and `iovec` within this structure, which are populated during `make_sqe_udp_recv_from`.
/// Once populated, this payload MUST NOT be moved in memory until the operation completes
/// or fails in the kernel. `PhantomPinned` enforces !Unpin in the type system.
pub(crate) struct UdpRecvFromPayload {
    msg_name: libc::sockaddr_storage,
    iovec: [libc::iovec; 1],
    msghdr: libc::msghdr,
    _pin: PhantomPinned,
}

/// A read-only SQE pointer backed by a [`SendToPayload`].
pub(crate) struct SendMsgView<'a> {
    msghdr: &'a libc::msghdr,
}

impl SendMsgView<'_> {
    #[inline]
    pub(crate) fn as_ptr(&self) -> *const libc::msghdr {
        self.msghdr
    }
}

/// A writable SQE pointer backed by a [`UdpRecvFromPayload`].
pub(crate) struct RecvMsgView<'a> {
    msghdr: &'a mut libc::msghdr,
}

impl RecvMsgView<'_> {
    #[inline]
    pub(crate) fn into_ptr(self) -> *mut libc::msghdr {
        self.msghdr
    }
}

pub(crate) struct OpenPayload {}

pub(crate) struct WakeupPayload {
    pub(crate) buf: [u8; 8],
}

pub(crate) struct TimeoutPayload {
    pub(crate) ts: Timespec,
}

fn zeroed_sockaddr_storage() -> libc::sockaddr_storage {
    // C socket storage is intentionally zero-initialized before make_sqe fills it.
    unsafe { mem::zeroed() }
}

fn zeroed_msghdr() -> libc::msghdr {
    // msghdr pointer fields are populated immediately before submission.
    unsafe { mem::zeroed() }
}

impl AcceptPayload {
    #[inline]
    pub(crate) const fn new() -> Self {
        Self {}
    }
}

impl SendToPayload {
    #[inline]
    pub(crate) fn new() -> Self {
        Self {
            msg_name: zeroed_sockaddr_storage(),
            msg_namelen: 0,
            iovec: [libc::iovec {
                iov_base: ptr::null_mut(),
                iov_len: 0,
            }],
            msghdr: zeroed_msghdr(),
            _pin: PhantomPinned,
        }
    }

    /// Initializes the address buffer, iovec, and `sendmsg` header for submission.
    ///
    /// # Safety
    ///
    /// The caller must keep `self` at the same address until the kernel has stopped using the
    /// returned message view. The view stores a pointer into `self`, and the SQE can outlive this
    /// Rust borrow until its completion or cancellation is observed.
    pub(crate) unsafe fn init_send_to<'a>(
        &'a mut self,
        user: &mut SendTo,
    ) -> Result<SendMsgView<'a>, BufIoRangeError> {
        let (ptr, len) = user.buf.checked_write_range(user.buf_offset)?;
        self.iovec[0].iov_base = ptr as *mut _;
        self.iovec[0].iov_len = len as usize;

        let (msg_name, msg_namelen) = socket_addr_to_storage(user.addr);
        self.msg_name = msg_name.0;
        self.msg_namelen = msg_namelen;
        self.msghdr.msg_name = ptr::addr_of_mut!(self.msg_name).cast();
        self.msghdr.msg_namelen = self.msg_namelen;
        self.msghdr.msg_iov = self.iovec.as_mut_ptr();
        self.msghdr.msg_iovlen = 1;

        Ok(SendMsgView {
            msghdr: &self.msghdr,
        })
    }

    #[cfg(test)]
    pub(crate) fn test_pointers(
        &mut self,
    ) -> (*mut libc::c_void, *mut libc::iovec, *const libc::msghdr) {
        (
            ptr::addr_of_mut!(self.msg_name).cast(),
            self.iovec.as_mut_ptr(),
            ptr::addr_of!(self.msghdr),
        )
    }
}

impl UdpRecvFromPayload {
    #[inline]
    pub(crate) fn new() -> Self {
        Self {
            msg_name: zeroed_sockaddr_storage(),
            iovec: [libc::iovec {
                iov_base: ptr::null_mut(),
                iov_len: 0,
            }],
            msghdr: zeroed_msghdr(),
            _pin: PhantomPinned,
        }
    }

    /// Initializes the address buffer, iovec, and `recvmsg` header for submission.
    ///
    /// # Safety
    ///
    /// The caller must keep `self` at the same address until the kernel has stopped using the
    /// returned message view. The view stores pointers into `self`, and the SQE can outlive this
    /// Rust borrow until its completion or cancellation is observed.
    pub(crate) unsafe fn init_recv_from<'a>(
        &'a mut self,
        user: &mut UdpRecvFrom,
    ) -> Result<RecvMsgView<'a>, BufIoRangeError> {
        let (ptr, len) = user.buf.checked_read_range(user.buf_offset)?;
        self.iovec[0].iov_base = ptr as *mut _;
        self.iovec[0].iov_len = len as usize;

        self.msghdr.msg_name = ptr::addr_of_mut!(self.msg_name).cast();
        self.msghdr.msg_namelen = mem::size_of::<libc::sockaddr_storage>() as _;
        self.msghdr.msg_iov = self.iovec.as_mut_ptr();
        self.msghdr.msg_iovlen = 1;

        Ok(RecvMsgView {
            msghdr: &mut self.msghdr,
        })
    }

    pub(crate) fn finish_recv_from(&self) -> UringResult<SocketAddr> {
        let addr_bytes = bounded_sockaddr_bytes(
            &self.msg_name,
            self.msghdr.msg_namelen as usize,
            "uring.op.payload.finish_recv_from",
        )?;
        to_socket_addr(addr_bytes)
    }

    #[cfg(test)]
    pub(crate) fn test_pointers(
        &mut self,
    ) -> (*mut libc::c_void, *mut libc::iovec, *const libc::msghdr) {
        (
            ptr::addr_of_mut!(self.msg_name).cast(),
            self.iovec.as_mut_ptr(),
            ptr::addr_of!(self.msghdr),
        )
    }

    #[cfg(test)]
    pub(crate) fn test_set_received_address(
        &mut self,
        storage: libc::sockaddr_storage,
        len: usize,
    ) {
        self.msg_name = storage;
        self.msghdr.msg_namelen = len as _;
    }
}

impl OpenPayload {
    #[inline]
    pub(crate) const fn new() -> Self {
        Self {}
    }
}

impl WakeupPayload {
    #[inline]
    pub(crate) const fn new() -> Self {
        Self { buf: [0; 8] }
    }
}

impl TimeoutPayload {
    #[inline]
    pub(crate) fn new() -> Self {
        Self {
            ts: Timespec::new(),
        }
    }
}

pub(crate) enum UringOpPayload {
    Read(KernelRef<ReadFixed>),
    ReadRaw(KernelRef<ReadRaw>),
    Write(KernelRef<WriteFixed>),
    WriteRaw(KernelRef<WriteRaw>),
    Recv(KernelRef<Recv>),
    RecvProvided(KernelRef<RecvProvided>),
    RecvMulti(KernelRef<RecvMulti>),
    Send(KernelRef<OpSend>),
    UdpRecv(KernelRef<UdpRecv>),
    UdpSend(KernelRef<UdpSend>),
    Connect(KernelRef<Connect>),
    UdpConnect(KernelRef<UdpConnect>),
    Close(KernelRef<Close>),
    Fsync(KernelRef<Fsync>),
    FsyncRaw(KernelRef<FsyncRaw>),
    SyncRange(KernelRef<SyncFileRange>),
    SyncRangeRaw(KernelRef<SyncFileRangeRaw>),
    Fallocate(KernelRef<Fallocate>),
    FallocateRaw(KernelRef<FallocateRaw>),
    Accept(AcceptPayload),
    AcceptMulti(KernelRef<AcceptMulti>),
    SendTo(SendToPayload),
    UdpRecvFrom(UdpRecvFromPayload),
    Open(OpenPayload),
    Wakeup(WakeupPayload),
    Timeout(TimeoutPayload),
}

impl UringOpPayload {
    pub(crate) const fn tag(&self) -> UringPayloadTag {
        match self {
            Self::Read(_) => UringPayloadTag::Read,
            Self::ReadRaw(_) => UringPayloadTag::ReadRaw,
            Self::Write(_) => UringPayloadTag::Write,
            Self::WriteRaw(_) => UringPayloadTag::WriteRaw,
            Self::Recv(_) => UringPayloadTag::Recv,
            Self::RecvProvided(_) => UringPayloadTag::RecvProvided,
            Self::RecvMulti(_) => UringPayloadTag::RecvMulti,
            Self::Send(_) => UringPayloadTag::Send,
            Self::UdpRecv(_) => UringPayloadTag::UdpRecv,
            Self::UdpSend(_) => UringPayloadTag::UdpSend,
            Self::Connect(_) => UringPayloadTag::Connect,
            Self::UdpConnect(_) => UringPayloadTag::UdpConnect,
            Self::Close(_) => UringPayloadTag::Close,
            Self::Fsync(_) => UringPayloadTag::Fsync,
            Self::FsyncRaw(_) => UringPayloadTag::FsyncRaw,
            Self::SyncRange(_) => UringPayloadTag::SyncRange,
            Self::SyncRangeRaw(_) => UringPayloadTag::SyncRangeRaw,
            Self::Fallocate(_) => UringPayloadTag::Fallocate,
            Self::FallocateRaw(_) => UringPayloadTag::FallocateRaw,
            Self::Accept(_) => UringPayloadTag::Accept,
            Self::AcceptMulti(_) => UringPayloadTag::AcceptMulti,
            Self::SendTo(_) => UringPayloadTag::SendTo,
            Self::UdpRecvFrom(_) => UringPayloadTag::UdpRecvFrom,
            Self::Open(_) => UringPayloadTag::Open,
            Self::Wakeup(_) => UringPayloadTag::Wakeup,
            Self::Timeout(_) => UringPayloadTag::Timeout,
        }
    }
}
