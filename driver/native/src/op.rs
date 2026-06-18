use std::{future::Future, marker::Send as StdSend};

use crate::SockAddrStorage;

pub use veloq_driver_core::{
    IoFd,
    op::{
        DetachedOp, DetachedSubmitter, DriverProvider, IntoPlatformOp, LocalOp, LocalSubmitter, Op,
        OpKind, OpLifecycle, OpResult, OpSubmitter as CoreOpSubmitter,
        types::{
            Accept as CoreAccept, Close, Connect as CoreConnect, Fallocate, FallocateRaw, Fsync,
            FsyncRaw, Open, ReadFixed, ReadRaw, Recv, Send, SendTo, SyncFileRange,
            SyncFileRangeRaw, Timeout, UdpConnect as CoreUdpConnect, UdpRecv, UdpRecvFrom,
            UdpRecvPacket, UdpRecvPacketBuf, UdpSend, Wakeup, WriteFixed, WriteRaw,
        },
    },
    slot::{SlotCompletion, SlotError, SlotOp, SlotPayload},
};

#[cfg(unix)]
type FileRawHandle = veloq_driver_uring::UringRawHandle;
#[cfg(windows)]
type FileRawHandle = veloq_driver_iocp::IocpHandle;

pub type FileSyncFileRangeRaw = SyncFileRangeRaw<FileRawHandle>;
pub type UdpConnect = CoreUdpConnect<SockAddrStorage>;
pub type Connect = CoreConnect<SockAddrStorage>;
pub type Accept = CoreAccept<SockAddrStorage>;

pub trait OpSubmitter<'a, P: DriverProvider>: Clone + StdSend + Sync {
    type Future<T: IntoPlatformOp<P::SlotSpec> + StdSend>: Future<
        Output = OpResult<T::Output, SlotError<P::SlotSpec>, T::Completion>,
    >;

    fn submit<T>(&self, op: Op<T>, provider: P) -> Self::Future<T>
    where
        T: IntoPlatformOp<P::SlotSpec> + StdSend;

    fn from_current_context() -> Self;
}

impl<'a, P: DriverProvider> OpSubmitter<'a, P> for LocalSubmitter<P> {
    type Future<T: IntoPlatformOp<P::SlotSpec> + StdSend> = LocalOp<'a, T, P>;

    fn submit<T>(&self, op: Op<T>, provider: P) -> LocalOp<'a, T, P>
    where
        T: IntoPlatformOp<P::SlotSpec> + StdSend,
    {
        <LocalSubmitter<P> as CoreOpSubmitter<'a, P>>::submit(self, op, provider)
    }

    fn from_current_context() -> Self {
        <LocalSubmitter<P> as CoreOpSubmitter<'a, P>>::from_current_context()
    }
}

impl<'a, P: DriverProvider> OpSubmitter<'a, P> for DetachedSubmitter {
    type Future<T: IntoPlatformOp<P::SlotSpec> + StdSend> = DetachedOp<T, P::SlotSpec>;

    fn submit<T>(&self, op: Op<T>, provider: P) -> Self::Future<T>
    where
        T: IntoPlatformOp<P::SlotSpec> + StdSend,
    {
        <DetachedSubmitter as CoreOpSubmitter<'a, P>>::submit(self, op, provider)
    }

    fn from_current_context() -> Self {
        <DetachedSubmitter as CoreOpSubmitter<'a, P>>::from_current_context()
    }
}
