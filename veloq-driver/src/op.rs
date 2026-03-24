use std::cell::RefCell;
use std::future::Future;
use std::rc::Rc;

use crate::SockAddrStorage;
use crate::driver::{Driver, PlatformDriver};
#[cfg(windows)]
use veloq_driver_iocp::IocpHandle as PlatformRawHandle;
#[cfg(not(windows))]
use veloq_driver_uring::UringRawHandle as PlatformRawHandle;

pub type IoFd = veloq_driver_core::IoFd<PlatformRawHandle>;
pub type ReadFixed = veloq_driver_core::op::ReadFixed<PlatformRawHandle>;
pub type WriteFixed = veloq_driver_core::op::WriteFixed<PlatformRawHandle>;
pub type Recv = veloq_driver_core::op::Recv<PlatformRawHandle>;
pub type Send = veloq_driver_core::op::Send<PlatformRawHandle>;
pub type UdpRecv = veloq_driver_core::op::UdpRecv<PlatformRawHandle>;
pub type UdpSend = veloq_driver_core::op::UdpSend<PlatformRawHandle>;
pub type Connect = veloq_driver_core::op::Connect<PlatformRawHandle, SockAddrStorage>;
pub type Close = veloq_driver_core::op::Close<PlatformRawHandle>;
pub type Fsync = veloq_driver_core::op::Fsync<PlatformRawHandle>;
pub type Wakeup = veloq_driver_core::op::Wakeup<PlatformRawHandle>;
pub type Accept = veloq_driver_core::op::Accept<PlatformRawHandle, SockAddrStorage>;
pub type SendTo = veloq_driver_core::op::SendTo<PlatformRawHandle>;
pub type SyncFileRange = veloq_driver_core::op::SyncFileRange<PlatformRawHandle>;
pub type Fallocate = veloq_driver_core::op::Fallocate<PlatformRawHandle>;
pub type UdpRecvStream = veloq_driver_core::op::UdpRecvStream<PlatformRawHandle>;

pub use veloq_driver_core::op::{
    DetachedOp, DetachedSubmitter, IntoPlatformOp, LocalSubmitter, Op, OpKind, OpLifecycle,
    OpResult, Open, Timeout, UdpRecvPacket,
};

pub type LocalOp<T> = veloq_driver_core::op::LocalOp<T, PlatformDriver>;

pub trait OpSubmitter: Clone + std::marker::Send + Sync + 'static {
    type Future<
        T: IntoPlatformOp<
                <PlatformDriver as Driver>::Op,
                DriverCompletion = <PlatformDriver as Driver>::Completion,
            > + std::marker::Send
            + 'static,
    >: Future<Output = OpResult<T, <T as IntoPlatformOp<<PlatformDriver as Driver>::Op>>::Completion>>;

    fn submit<T>(&self, op: Op<T>, driver: Rc<RefCell<PlatformDriver>>) -> Self::Future<T>
    where
        T: IntoPlatformOp<
                <PlatformDriver as Driver>::Op,
                DriverCompletion = <PlatformDriver as Driver>::Completion,
            > + std::marker::Send
            + 'static;

    fn from_current_context() -> std::io::Result<Self>;
}

impl OpSubmitter for LocalSubmitter {
    type Future<
        T: IntoPlatformOp<
                <PlatformDriver as Driver>::Op,
                DriverCompletion = <PlatformDriver as Driver>::Completion,
            > + std::marker::Send
            + 'static,
    > = LocalOp<T>;

    fn submit<T>(&self, op: Op<T>, driver: Rc<RefCell<PlatformDriver>>) -> LocalOp<T>
    where
        T: IntoPlatformOp<
                <PlatformDriver as Driver>::Op,
                DriverCompletion = <PlatformDriver as Driver>::Completion,
            > + std::marker::Send
            + 'static,
    {
        <LocalSubmitter as veloq_driver_core::op::OpSubmitter<PlatformDriver>>::submit(
            self, op, driver,
        )
    }

    fn from_current_context() -> std::io::Result<Self> {
        <LocalSubmitter as veloq_driver_core::op::OpSubmitter<PlatformDriver>>::from_current_context(
        )
    }
}

impl OpSubmitter for DetachedSubmitter {
    type Future<
        T: IntoPlatformOp<
                <PlatformDriver as Driver>::Op,
                DriverCompletion = <PlatformDriver as Driver>::Completion,
            > + std::marker::Send
            + 'static,
    > = DetachedOp<T, <PlatformDriver as Driver>::Op, <PlatformDriver as Driver>::Completion>;

    fn submit<T>(&self, op: Op<T>, driver: Rc<RefCell<PlatformDriver>>) -> Self::Future<T>
    where
        T: IntoPlatformOp<
                <PlatformDriver as Driver>::Op,
                DriverCompletion = <PlatformDriver as Driver>::Completion,
            > + std::marker::Send
            + 'static,
    {
        <DetachedSubmitter as veloq_driver_core::op::OpSubmitter<PlatformDriver>>::submit(
            self, op, driver,
        )
    }

    fn from_current_context() -> std::io::Result<Self> {
        <DetachedSubmitter as veloq_driver_core::op::OpSubmitter<PlatformDriver>>::from_current_context()
    }
}
