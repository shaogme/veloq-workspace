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

pub(crate) struct KernelRef<T> {
    pub(crate) _marker: std::marker::PhantomData<T>,
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
    pub(crate) ts: [i64; 2],
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
