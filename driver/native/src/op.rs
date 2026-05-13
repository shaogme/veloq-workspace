use std::cell::RefCell;
use std::future::Future;
use std::rc::Rc;

use crate::SockAddrStorage;
use crate::driver::{Driver, PlatformDriver};

pub type IoFd = veloq_driver_core::IoFd;
pub type ReadRaw<H> = veloq_driver_core::op::ReadRaw<H>;
pub type WriteRaw<H> = veloq_driver_core::op::WriteRaw<H>;
pub type FsyncRaw<H> = veloq_driver_core::op::FsyncRaw<H>;
pub type SyncFileRangeRaw<H> = veloq_driver_core::op::SyncFileRangeRaw<H>;
pub type FallocateRaw<H> = veloq_driver_core::op::FallocateRaw<H>;

#[cfg(unix)]
type FileRawHandle = veloq_driver_uring::UringRawHandle;
#[cfg(windows)]
type FileRawHandle = veloq_driver_iocp::IocpHandle;

pub type FileReadRaw = veloq_driver_core::op::ReadRaw<FileRawHandle>;
pub type FileWriteRaw = veloq_driver_core::op::WriteRaw<FileRawHandle>;
pub type FileFsyncRaw = veloq_driver_core::op::FsyncRaw<FileRawHandle>;
pub type FileSyncFileRangeRaw = veloq_driver_core::op::SyncFileRangeRaw<FileRawHandle>;
pub type FileFallocateRaw = veloq_driver_core::op::FallocateRaw<FileRawHandle>;
pub type ReadFixed = veloq_driver_core::op::ReadFixed;
pub type WriteFixed = veloq_driver_core::op::WriteFixed;
pub type Recv = veloq_driver_core::op::Recv;
pub type Send = veloq_driver_core::op::Send;
pub type UdpRecv = veloq_driver_core::op::UdpRecv;
pub type UdpSend = veloq_driver_core::op::UdpSend;
pub type UdpConnect = veloq_driver_core::op::UdpConnect<SockAddrStorage>;
pub type Connect = veloq_driver_core::op::Connect<SockAddrStorage>;
pub type Close = veloq_driver_core::op::Close;
pub type Fsync = veloq_driver_core::op::Fsync;
pub type Wakeup = veloq_driver_core::op::Wakeup;
pub type Accept = veloq_driver_core::op::Accept<SockAddrStorage>;
pub type SendTo = veloq_driver_core::op::SendTo;
pub type SyncFileRange = veloq_driver_core::op::SyncFileRange;
pub type Fallocate = veloq_driver_core::op::Fallocate;
pub type UdpRecvFrom = veloq_driver_core::op::UdpRecvFrom;

pub use veloq_driver_core::op::{
    DetachedOp, DetachedSubmitter, IntoPlatformOp, LocalSubmitter, Op, OpKind, OpLifecycle,
    OpResult, Open, Timeout, UdpRecvPacket, UdpRecvPacketBuf,
};

pub type LocalOp<T> = veloq_driver_core::op::LocalOp<T, PlatformDriver>;

pub trait OpSubmitter: Clone + std::marker::Send + Sync {
    type Future<
        T: IntoPlatformOp<
                <PlatformDriver as Driver>::Op,
                DriverCompletion = <PlatformDriver as Driver>::Completion,
                ErasedPayload = <PlatformDriver as Driver>::UP,
            > + std::marker::Send,
    >: Future<
        Output = OpResult<
            T::Output,
            <T as IntoPlatformOp<<PlatformDriver as Driver>::Op>>::Completion,
        >,
    >;

    fn submit<T>(&self, op: Op<T>, driver: Rc<RefCell<PlatformDriver>>) -> Self::Future<T>
    where
        T: IntoPlatformOp<
                <PlatformDriver as Driver>::Op,
                DriverCompletion = <PlatformDriver as Driver>::Completion,
                ErasedPayload = <PlatformDriver as Driver>::UP,
            > + std::marker::Send;

    fn from_current_context() -> Self;
}

impl OpSubmitter for LocalSubmitter {
    type Future<
        T: IntoPlatformOp<
                <PlatformDriver as Driver>::Op,
                DriverCompletion = <PlatformDriver as Driver>::Completion,
                ErasedPayload = <PlatformDriver as Driver>::UP,
            > + std::marker::Send,
    > = LocalOp<T>;

    fn submit<T>(&self, op: Op<T>, driver: Rc<RefCell<PlatformDriver>>) -> LocalOp<T>
    where
        T: IntoPlatformOp<
                <PlatformDriver as Driver>::Op,
                DriverCompletion = <PlatformDriver as Driver>::Completion,
                ErasedPayload = <PlatformDriver as Driver>::UP,
            > + std::marker::Send,
    {
        <LocalSubmitter as veloq_driver_core::op::OpSubmitter<PlatformDriver>>::submit(
            self, op, driver,
        )
    }

    fn from_current_context() -> Self {
        <LocalSubmitter as veloq_driver_core::op::OpSubmitter<PlatformDriver>>::from_current_context(
        )
    }
}

impl OpSubmitter for DetachedSubmitter {
    type Future<
        T: IntoPlatformOp<
                <PlatformDriver as Driver>::Op,
                DriverCompletion = <PlatformDriver as Driver>::Completion,
                ErasedPayload = <PlatformDriver as Driver>::UP,
            > + std::marker::Send,
    > = DetachedOp<T, <PlatformDriver as Driver>::Op, <PlatformDriver as Driver>::Completion>;

    fn submit<T>(&self, op: Op<T>, driver: Rc<RefCell<PlatformDriver>>) -> Self::Future<T>
    where
        T: IntoPlatformOp<
                <PlatformDriver as Driver>::Op,
                DriverCompletion = <PlatformDriver as Driver>::Completion,
                ErasedPayload = <PlatformDriver as Driver>::UP,
            > + std::marker::Send,
    {
        <DetachedSubmitter as veloq_driver_core::op::OpSubmitter<PlatformDriver>>::submit(
            self, op, driver,
        )
    }

    fn from_current_context() -> Self {
        <DetachedSubmitter as veloq_driver_core::op::OpSubmitter<PlatformDriver>>::from_current_context()
    }
}
