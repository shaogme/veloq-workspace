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

use crate::config::{SockAddrStorage, UringRawHandle};
use io_uring::types::Timespec;
use veloq_std::{
    marker::{PhantomData, PhantomPinned},
    mem, ptr,
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
    pub(crate) msg_name: libc::sockaddr_storage,
    pub(crate) msg_namelen: libc::socklen_t,
    pub(crate) iovec: [libc::iovec; 1],
    pub(crate) msghdr: libc::msghdr,
    pub(crate) _pin: PhantomPinned,
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
    pub(crate) msg_name: libc::sockaddr_storage,
    pub(crate) iovec: [libc::iovec; 1],
    pub(crate) msghdr: libc::msghdr,
    pub(crate) _pin: PhantomPinned,
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
