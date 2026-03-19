use std::mem::ManuallyDrop;
use std::ptr::NonNull;

use veloq_driver_core::op::{
    Accept as CoreAccept, Close as CoreClose, Connect as CoreConnect, Fallocate as CoreFallocate,
    Fsync as CoreFsync, ReadFixed as CoreReadFixed, Recv as CoreRecv,
    Send as CoreSend, SendTo as CoreSendTo, SyncFileRange as CoreSyncFileRange,
    UdpRecvStream as CoreUdpRecvStream, UdpRefill as CoreUdpRefill, Wakeup as CoreWakeup,
    WriteFixed as CoreWriteFixed,
};

pub(crate) use veloq_driver_core::op::{Open, Timeout};

use crate::config::{RawHandle, SockAddrStorage};

pub(crate) type ReadFixed = CoreReadFixed<RawHandle>;
pub(crate) type WriteFixed = CoreWriteFixed<RawHandle>;
pub(crate) type Recv = CoreRecv<RawHandle>;
pub(crate) type OpSend = CoreSend<RawHandle>;
pub(crate) type Connect = CoreConnect<RawHandle, SockAddrStorage>;
pub(crate) type Close = CoreClose<RawHandle>;
pub(crate) type Fsync = CoreFsync<RawHandle>;
pub(crate) type SyncFileRange = CoreSyncFileRange<RawHandle>;
pub(crate) type Fallocate = CoreFallocate<RawHandle>;
pub(crate) type Accept = CoreAccept<RawHandle, SockAddrStorage>;
pub(crate) type SendTo = CoreSendTo<RawHandle>;
pub(crate) type UdpRecvStream = CoreUdpRecvStream<RawHandle>;
pub(crate) type UdpRefill = CoreUdpRefill<RawHandle>;
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

#[repr(C)]
pub(crate) union UringOpPayload {
    pub(crate) read: ManuallyDrop<KernelRef<ReadFixed>>,
    pub(crate) write: ManuallyDrop<KernelRef<WriteFixed>>,
    pub(crate) recv: ManuallyDrop<KernelRef<Recv>>,
    pub(crate) send: ManuallyDrop<KernelRef<OpSend>>,
    pub(crate) connect: ManuallyDrop<KernelRef<Connect>>,
    pub(crate) close: ManuallyDrop<KernelRef<Close>>,
    pub(crate) fsync: ManuallyDrop<KernelRef<Fsync>>,
    pub(crate) sync_range: ManuallyDrop<KernelRef<SyncFileRange>>,
    pub(crate) fallocate: ManuallyDrop<KernelRef<Fallocate>>,
    pub(crate) accept: ManuallyDrop<AcceptPayload>,
    pub(crate) send_to: ManuallyDrop<SendToPayload>,
    pub(crate) udp_recv_stream: ManuallyDrop<UdpRecvStreamPayload>,
    pub(crate) udp_refill: ManuallyDrop<KernelRef<UdpRefill>>,
    pub(crate) open: ManuallyDrop<OpenPayload>,
    pub(crate) wakeup: ManuallyDrop<WakeupPayload>,
    pub(crate) timeout: ManuallyDrop<TimeoutPayload>,
}
