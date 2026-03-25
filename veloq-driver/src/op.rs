use std::cell::RefCell;
use std::future::Future;
use std::rc::Rc;

use crate::SockAddrStorage;
use crate::driver::{Driver, PlatformDriver};

pub type IoFd = veloq_driver_core::IoFd;
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
pub type UdpRecvStream = veloq_driver_core::op::UdpRecvStream;

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

    fn from_current_context() -> Self;
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

    fn from_current_context() -> Self {
        <DetachedSubmitter as veloq_driver_core::op::OpSubmitter<PlatformDriver>>::from_current_context()
    }
}
