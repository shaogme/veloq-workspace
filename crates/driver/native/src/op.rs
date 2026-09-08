use veloq_std::{future::Future, marker::Send as StdSend};

use futures_core::Stream;

use crate::SockAddrStorage;

use veloq_driver_core::{
    IoFd as CoreIoFd,
    op::types::{
        Accept as CoreAccept, AcceptMulti as CoreAcceptMulti, Close as CoreClose,
        Connect as CoreConnect, Fallocate as CoreFallocate, FallocateRaw as CoreFallocateRaw,
        Fsync as CoreFsync, FsyncRaw as CoreFsyncRaw, ReadFixed as CoreReadFixed,
        ReadRaw as CoreReadRaw, Recv as CoreRecv, RecvMulti as CoreRecvMulti,
        RecvProvided as CoreRecvProvided, Send as CoreSend, SendTo as CoreSendTo,
        SyncFileRange as CoreSyncFileRange, SyncFileRangeRaw as CoreSyncFileRangeRaw,
        UdpConnect as CoreUdpConnect, UdpRecv as CoreUdpRecv, UdpRecvFrom as CoreUdpRecvFrom,
        UdpSend as CoreUdpSend, Wakeup as CoreWakeup, WriteFixed as CoreWriteFixed,
        WriteRaw as CoreWriteRaw,
    },
};
pub use veloq_driver_core::{
    op::{
        DetachedOp, DetachedSubmitter, DriverProvider, IntoPlatformOp, LocalOp, LocalSubmitter, Op,
        OpItem, OpKind, OpResult, OpSubmitter as CoreOpSubmitter, SingleShotOp,
        types::{AcceptedSocket, Open, ProvidedBuf, Timeout, UdpRecvPacket, UdpRecvPacketBuf},
    },
    slot::{SlotCompletion, SlotError, SlotOp, SlotPayload},
};

/// The platform's raw handle type, as chosen by the active backend.
#[cfg(unix)]
pub type PlatformRawHandle = veloq_driver_uring::UringRawHandle;
/// The platform's raw handle type, as chosen by the active backend.
#[cfg(windows)]
pub type PlatformRawHandle = veloq_driver_iocp::IocpHandle;

/// Descriptor handed out by the platform driver.
///
/// [`veloq_driver_core::IoFd`] is generic over the backend's handle type so that a descriptor
/// can carry the handle itself when it is not registered. This alias pins it to the active
/// backend once, which is why nothing above this crate mentions the parameter.
pub type IoFd = CoreIoFd<PlatformRawHandle>;

pub type ReadFixed = CoreReadFixed<PlatformRawHandle>;
pub type ReadRaw = CoreReadRaw<PlatformRawHandle>;
pub type WriteFixed = CoreWriteFixed<PlatformRawHandle>;
pub type WriteRaw = CoreWriteRaw<PlatformRawHandle>;
pub type Recv = CoreRecv<PlatformRawHandle>;
/// Only the io_uring backend implements this operation — IOCP has no provided buffers, so its
/// `capabilities().provided_buffers` is always `false` and nothing ever submits one.
pub type RecvProvided = CoreRecvProvided<PlatformRawHandle>;
/// Only the io_uring backend implements this operation, and only from Linux 6.0 — see
/// `capabilities().recv_multi`, which is `false` everywhere else.
pub type RecvMulti = CoreRecvMulti<PlatformRawHandle>;
pub type Send = CoreSend<PlatformRawHandle>;
pub type UdpRecv = CoreUdpRecv<PlatformRawHandle>;
pub type UdpSend = CoreUdpSend<PlatformRawHandle>;
pub type Close = CoreClose<PlatformRawHandle>;
pub type Fsync = CoreFsync<PlatformRawHandle>;
pub type FsyncRaw = CoreFsyncRaw<PlatformRawHandle>;
pub type SendTo = CoreSendTo<PlatformRawHandle>;
pub type SyncFileRange = CoreSyncFileRange<PlatformRawHandle>;
pub type SyncFileRangeRaw = CoreSyncFileRangeRaw<PlatformRawHandle>;
pub type Fallocate = CoreFallocate<PlatformRawHandle>;
pub type FallocateRaw = CoreFallocateRaw<PlatformRawHandle>;
pub type UdpRecvFrom = CoreUdpRecvFrom<PlatformRawHandle>;
pub type Wakeup = CoreWakeup<PlatformRawHandle>;

pub type FileSyncFileRangeRaw = CoreSyncFileRangeRaw<PlatformRawHandle>;
pub type UdpConnect = CoreUdpConnect<PlatformRawHandle, SockAddrStorage>;
pub type Connect = CoreConnect<PlatformRawHandle, SockAddrStorage>;
pub type Accept = CoreAccept<PlatformRawHandle, SockAddrStorage>;
pub type AcceptMulti = CoreAcceptMulti<PlatformRawHandle>;

pub trait OpSubmitter<'a, P: DriverProvider>: Clone + StdSend + Sync {
    /// 单发提交：`await` 得到唯一的那条完成。
    type Future<T: SingleShotOp<P::SlotSpec> + StdSend>: Future<Output = OpItem<T, P::SlotSpec>>;

    /// 流式提交：一次提交、多条完成。与 [`Self::Future`] 是同一个具体类型的两种看法。
    type Stream<T: IntoPlatformOp<P::SlotSpec> + StdSend>: Stream<Item = OpItem<T, P::SlotSpec>>;

    fn submit<T>(&self, op: Op<T>, provider: P) -> Self::Future<T>
    where
        T: SingleShotOp<P::SlotSpec> + StdSend;

    fn submit_stream<T>(&self, op: Op<T>, provider: P) -> Self::Stream<T>
    where
        T: IntoPlatformOp<P::SlotSpec> + StdSend;

    fn from_current_context() -> Self;
}

impl<'a, P: DriverProvider> OpSubmitter<'a, P> for LocalSubmitter<P> {
    type Future<T: SingleShotOp<P::SlotSpec> + StdSend> = LocalOp<'a, T, P>;
    type Stream<T: IntoPlatformOp<P::SlotSpec> + StdSend> = LocalOp<'a, T, P>;

    fn submit<T>(&self, op: Op<T>, provider: P) -> LocalOp<'a, T, P>
    where
        T: SingleShotOp<P::SlotSpec> + StdSend,
    {
        <LocalSubmitter<P> as CoreOpSubmitter<'a, P>>::submit(self, op, provider)
    }

    fn submit_stream<T>(&self, op: Op<T>, provider: P) -> LocalOp<'a, T, P>
    where
        T: IntoPlatformOp<P::SlotSpec> + StdSend,
    {
        <LocalSubmitter<P> as CoreOpSubmitter<'a, P>>::submit_stream(self, op, provider)
    }

    fn from_current_context() -> Self {
        <LocalSubmitter<P> as CoreOpSubmitter<'a, P>>::from_current_context()
    }
}

impl<'a, P: DriverProvider> OpSubmitter<'a, P> for DetachedSubmitter {
    type Future<T: SingleShotOp<P::SlotSpec> + StdSend> = DetachedOp<T, P::SlotSpec>;
    type Stream<T: IntoPlatformOp<P::SlotSpec> + StdSend> = DetachedOp<T, P::SlotSpec>;

    fn submit<T>(&self, op: Op<T>, provider: P) -> Self::Future<T>
    where
        T: SingleShotOp<P::SlotSpec> + StdSend,
    {
        <DetachedSubmitter as CoreOpSubmitter<'a, P>>::submit(self, op, provider)
    }

    fn submit_stream<T>(&self, op: Op<T>, provider: P) -> Self::Stream<T>
    where
        T: IntoPlatformOp<P::SlotSpec> + StdSend,
    {
        <DetachedSubmitter as CoreOpSubmitter<'a, P>>::submit_stream(self, op, provider)
    }

    fn from_current_context() -> Self {
        <DetachedSubmitter as CoreOpSubmitter<'a, P>>::from_current_context()
    }
}
