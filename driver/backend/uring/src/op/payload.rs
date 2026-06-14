use veloq_driver_core::op::{
    Accept as CoreAccept, Close as CoreClose, Connect as CoreConnect, Fallocate as CoreFallocate,
    FallocateRaw as CoreFallocateRaw, Fsync as CoreFsync, FsyncRaw as CoreFsyncRaw,
    ReadFixed as CoreReadFixed, ReadRaw as CoreReadRaw, Recv as CoreRecv, Send as CoreSend,
    SendTo as CoreSendTo, SyncFileRange as CoreSyncFileRange,
    SyncFileRangeRaw as CoreSyncFileRangeRaw, UdpConnect as CoreUdpConnect, UdpRecv as CoreUdpRecv,
    UdpRecvFrom as CoreUdpRecvFrom, UdpSend as CoreUdpSend, Wakeup as CoreWakeup,
    WriteFixed as CoreWriteFixed, WriteRaw as CoreWriteRaw,
};

pub(crate) use veloq_driver_core::op::{Open, Timeout};

use crate::config::SockAddrStorage;
use crate::config::UringRawHandle;

pub(crate) type ReadFixed = CoreReadFixed;
pub(crate) type ReadRaw = CoreReadRaw<UringRawHandle>;
pub(crate) type WriteFixed = CoreWriteFixed;
pub(crate) type WriteRaw = CoreWriteRaw<UringRawHandle>;
pub(crate) type Recv = CoreRecv;
pub(crate) type OpSend = CoreSend;
pub(crate) type UdpRecv = CoreUdpRecv;
pub(crate) type UdpSend = CoreUdpSend;
pub(crate) type Connect = CoreConnect<SockAddrStorage>;
pub(crate) type UdpConnect = CoreUdpConnect<SockAddrStorage>;
pub(crate) type Close = CoreClose;
pub(crate) type Fsync = CoreFsync;
pub(crate) type FsyncRaw = CoreFsyncRaw<UringRawHandle>;
pub(crate) type SyncFileRange = CoreSyncFileRange;
pub(crate) type SyncFileRangeRaw = CoreSyncFileRangeRaw<UringRawHandle>;
pub(crate) type Fallocate = CoreFallocate;
pub(crate) type FallocateRaw = CoreFallocateRaw<UringRawHandle>;
pub(crate) type Accept = CoreAccept<SockAddrStorage>;
pub(crate) type SendTo = CoreSendTo;
pub(crate) type UdpRecvFrom = CoreUdpRecvFrom;
pub(crate) type Wakeup = CoreWakeup;

pub enum UringUserPayload {
    ReadFixed(ReadFixed),
    ReadRaw(ReadRaw),
    WriteFixed(WriteFixed),
    WriteRaw(WriteRaw),
    Recv(Recv),
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
    SendTo(SendTo),
    UdpRecvFrom(UdpRecvFrom),
    Open(Open),
    Wakeup(Wakeup),
    Timeout(Timeout),
}

// SAFETY: all payload variants are moved between driver-owned slots and completion queues.
unsafe impl Send for UringUserPayload {}

pub(crate) struct KernelRef<T> {
    pub(crate) marker: std::marker::PhantomData<T>,
}

pub(crate) fn kernel_ref<T>(_user: &T) -> KernelRef<T> {
    KernelRef {
        marker: std::marker::PhantomData,
    }
}

pub(crate) struct AcceptPayload {}

pub(crate) struct SendToPayload {
    pub(crate) msg_name: libc::sockaddr_storage,
    pub(crate) msg_namelen: libc::socklen_t,
    pub(crate) iovec: [libc::iovec; 1],
    pub(crate) msghdr: libc::msghdr,
}

pub(crate) struct UdpRecvFromPayload {
    pub(crate) msg_name: libc::sockaddr_storage,
    pub(crate) iovec: [libc::iovec; 1],
    pub(crate) msghdr: libc::msghdr,
}

pub(crate) struct OpenPayload {}

pub(crate) struct WakeupPayload {
    pub(crate) buf: [u8; 8],
}

pub(crate) struct TimeoutPayload {
    pub(crate) ts: io_uring::types::Timespec,
}

fn zeroed_sockaddr_storage() -> libc::sockaddr_storage {
    // C socket storage is intentionally zero-initialized before make_sqe fills it.
    unsafe { std::mem::zeroed() }
}

fn zeroed_msghdr() -> libc::msghdr {
    // msghdr pointer fields are populated immediately before submission.
    unsafe { std::mem::zeroed() }
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
                iov_base: std::ptr::null_mut(),
                iov_len: 0,
            }],
            msghdr: zeroed_msghdr(),
        }
    }
}

impl UdpRecvFromPayload {
    #[inline]
    pub(crate) fn new() -> Self {
        Self {
            msg_name: zeroed_sockaddr_storage(),
            iovec: [libc::iovec {
                iov_base: std::ptr::null_mut(),
                iov_len: 0,
            }],
            msghdr: zeroed_msghdr(),
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
            ts: io_uring::types::Timespec::new(),
        }
    }
}

pub(crate) enum UringOpPayload {
    Read(KernelRef<ReadFixed>),
    ReadRaw(KernelRef<ReadRaw>),
    Write(KernelRef<WriteFixed>),
    WriteRaw(KernelRef<WriteRaw>),
    Recv(KernelRef<Recv>),
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
    SendTo(SendToPayload),
    UdpRecvFrom(UdpRecvFromPayload),
    Open(OpenPayload),
    Wakeup(WakeupPayload),
    Timeout(TimeoutPayload),
}
