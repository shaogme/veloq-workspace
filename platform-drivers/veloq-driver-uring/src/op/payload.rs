use std::ptr::NonNull;

use veloq_driver_core::op::{
    Accept as CoreAccept, Close as CoreClose, Connect as CoreConnect, Fallocate as CoreFallocate,
    Fsync as CoreFsync, ReadFixed as CoreReadFixed, Recv as CoreRecv, Send as CoreSend,
    SendTo as CoreSendTo, SyncFileRange as CoreSyncFileRange, UdpRecv as CoreUdpRecv,
    UdpRecvStream as CoreUdpRecvStream, UdpSend as CoreUdpSend, Wakeup as CoreWakeup,
    WriteFixed as CoreWriteFixed,
};

pub(crate) use veloq_driver_core::op::{Open, Timeout};

use crate::config::{RawHandle, SockAddrStorage};

pub(crate) type ReadFixed = CoreReadFixed<RawHandle>;
pub(crate) type WriteFixed = CoreWriteFixed<RawHandle>;
pub(crate) type Recv = CoreRecv<RawHandle>;
pub(crate) type OpSend = CoreSend<RawHandle>;
pub(crate) type UdpRecv = CoreUdpRecv<RawHandle>;
pub(crate) type UdpSend = CoreUdpSend<RawHandle>;
pub(crate) type Connect = CoreConnect<RawHandle, SockAddrStorage>;
pub(crate) type Close = CoreClose<RawHandle>;
pub(crate) type Fsync = CoreFsync<RawHandle>;
pub(crate) type SyncFileRange = CoreSyncFileRange<RawHandle>;
pub(crate) type Fallocate = CoreFallocate<RawHandle>;
pub(crate) type Accept = CoreAccept<RawHandle, SockAddrStorage>;
pub(crate) type SendTo = CoreSendTo<RawHandle>;
pub(crate) type UdpRecvStream = CoreUdpRecvStream<RawHandle>;
pub(crate) type Wakeup = CoreWakeup<RawHandle>;

pub(crate) struct KernelRef<T> {
    pub(crate) user: NonNull<T>,
}

pub(crate) struct AcceptPayload {
    pub(crate) user: NonNull<Accept>,
}

pub(crate) struct SendToPayload {
    pub(crate) user: NonNull<SendTo>,
    pub(crate) msg_name: libc::sockaddr_storage,
    pub(crate) msg_namelen: libc::socklen_t,
    pub(crate) iovec: [libc::iovec; 1],
    pub(crate) msghdr: libc::msghdr,
}

pub(crate) struct UdpRecvStreamPayload {
    pub(crate) user: NonNull<UdpRecvStream>,
    pub(crate) msg_name: libc::sockaddr_storage,
    pub(crate) iovec: [libc::iovec; 1],
    pub(crate) msghdr: libc::msghdr,
}

pub(crate) struct OpenPayload {
    pub(crate) user: NonNull<Open>,
}

pub(crate) struct WakeupPayload {
    pub(crate) user: NonNull<Wakeup>,
    pub(crate) buf: [u8; 8],
}

pub(crate) struct TimeoutPayload {
    pub(crate) user: NonNull<Timeout>,
    pub(crate) ts: [i64; 2],
}

pub(crate) enum UringOpPayload {
    Read(KernelRef<ReadFixed>),
    Write(KernelRef<WriteFixed>),
    Recv(KernelRef<Recv>),
    Send(KernelRef<OpSend>),
    UdpRecv(KernelRef<UdpRecv>),
    UdpSend(KernelRef<UdpSend>),
    Connect(KernelRef<Connect>),
    Close(KernelRef<Close>),
    Fsync(KernelRef<Fsync>),
    SyncRange(KernelRef<SyncFileRange>),
    Fallocate(KernelRef<Fallocate>),
    Accept(AcceptPayload),
    SendTo(SendToPayload),
    UdpRecvStream(UdpRecvStreamPayload),
    Open(OpenPayload),
    Wakeup(WakeupPayload),
    Timeout(TimeoutPayload),
}
