use std::future::Future;

use crate::SockAddrStorage;

pub type IoFd = veloq_driver_core::IoFd;
pub type ReadRaw<H> = veloq_driver_core::op::types::ReadRaw<H>;
pub type WriteRaw<H> = veloq_driver_core::op::types::WriteRaw<H>;
pub type FsyncRaw<H> = veloq_driver_core::op::types::FsyncRaw<H>;
pub type SyncFileRangeRaw<H> = veloq_driver_core::op::types::SyncFileRangeRaw<H>;
pub type FallocateRaw<H> = veloq_driver_core::op::types::FallocateRaw<H>;

#[cfg(unix)]
type FileRawHandle = veloq_driver_uring::UringRawHandle;
#[cfg(windows)]
type FileRawHandle = veloq_driver_iocp::IocpHandle;

pub type FileSyncFileRangeRaw = veloq_driver_core::op::types::SyncFileRangeRaw<FileRawHandle>;
pub type ReadFixed = veloq_driver_core::op::types::ReadFixed;
pub type WriteFixed = veloq_driver_core::op::types::WriteFixed;
pub type Recv = veloq_driver_core::op::types::Recv;
pub type Send = veloq_driver_core::op::types::Send;
pub type UdpRecv = veloq_driver_core::op::types::UdpRecv;
pub type UdpSend = veloq_driver_core::op::types::UdpSend;
pub type UdpConnect = veloq_driver_core::op::types::UdpConnect<SockAddrStorage>;
pub type Connect = veloq_driver_core::op::types::Connect<SockAddrStorage>;
pub type Close = veloq_driver_core::op::types::Close;
pub type Fsync = veloq_driver_core::op::types::Fsync;
pub type Wakeup = veloq_driver_core::op::types::Wakeup;
pub type Accept = veloq_driver_core::op::types::Accept<SockAddrStorage>;
pub type SendTo = veloq_driver_core::op::types::SendTo;
pub type SyncFileRange = veloq_driver_core::op::types::SyncFileRange;
pub type Fallocate = veloq_driver_core::op::types::Fallocate;
pub type UdpRecvFrom = veloq_driver_core::op::types::UdpRecvFrom;

pub use veloq_driver_core::op::{
    DetachedOp, DetachedSubmitter, DriverProvider, IntoPlatformOp, LocalSubmitter, Op, OpKind,
    OpLifecycle, OpResult,
    types::{Open, Timeout, UdpRecvPacket, UdpRecvPacketBuf},
};

pub type LocalOp<'a, T, P> = veloq_driver_core::op::LocalOp<'a, T, P>;

pub trait OpSubmitter<'a, P: DriverProvider>: Clone + std::marker::Send + Sync {
    type Future<
        T: IntoPlatformOp<
                P::Op,
                DriverCompletion = P::Completion,
                ErasedPayload = P::UP,
                Error = P::Error,
            >
            + std::marker::Send,
    >: Future<Output = OpResult<T::Output, P::Error, <T as IntoPlatformOp<P::Op>>::Completion>>;

    fn submit<T>(&self, op: Op<T>, provider: P) -> Self::Future<T>
    where
        T: IntoPlatformOp<
                P::Op,
                DriverCompletion = P::Completion,
                ErasedPayload = P::UP,
                Error = P::Error,
            > + std::marker::Send;

    fn from_current_context() -> Self;
}

impl<'a, P: veloq_driver_core::op::DriverProvider> OpSubmitter<'a, P> for LocalSubmitter<P> {
    type Future<
        T: IntoPlatformOp<
                P::Op,
                DriverCompletion = P::Completion,
                ErasedPayload = P::UP,
                Error = P::Error,
            > + std::marker::Send,
    > = LocalOp<'a, T, P>;

    fn submit<T>(&self, op: Op<T>, provider: P) -> LocalOp<'a, T, P>
    where
        T: IntoPlatformOp<
                P::Op,
                DriverCompletion = P::Completion,
                ErasedPayload = P::UP,
                Error = P::Error,
            > + std::marker::Send,
    {
        <LocalSubmitter<P> as veloq_driver_core::op::OpSubmitter<'a, P>>::submit(self, op, provider)
    }

    fn from_current_context() -> Self {
        <LocalSubmitter<P> as veloq_driver_core::op::OpSubmitter<'a, P>>::from_current_context()
    }
}

impl<'a, P: veloq_driver_core::op::DriverProvider> OpSubmitter<'a, P> for DetachedSubmitter {
    type Future<
        T: IntoPlatformOp<
                P::Op,
                DriverCompletion = P::Completion,
                ErasedPayload = P::UP,
                Error = P::Error,
            > + std::marker::Send,
    > = DetachedOp<T, P::SlotSpec>;

    fn submit<T>(&self, op: Op<T>, provider: P) -> Self::Future<T>
    where
        T: IntoPlatformOp<
                P::Op,
                DriverCompletion = P::Completion,
                ErasedPayload = P::UP,
                Error = P::Error,
            > + std::marker::Send,
    {
        <DetachedSubmitter as veloq_driver_core::op::OpSubmitter<'a, P>>::submit(self, op, provider)
    }

    fn from_current_context() -> Self {
        <DetachedSubmitter as veloq_driver_core::op::OpSubmitter<'a, P>>::from_current_context()
    }
}
