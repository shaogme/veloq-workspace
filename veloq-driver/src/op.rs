use std::cell::RefCell;
use std::future::Future;
use std::rc::Rc;

use crate::driver::{Driver, PlatformDriver};
use crate::{RawHandle, SockAddrStorage};

pub type IoFd = veloq_driver_core::IoFd<RawHandle>;
pub type ReadFixed = veloq_driver_core::op::ReadFixed<RawHandle>;
pub type WriteFixed = veloq_driver_core::op::WriteFixed<RawHandle>;
pub type Recv = veloq_driver_core::op::Recv<RawHandle>;
pub type Send = veloq_driver_core::op::Send<RawHandle>;
pub type Connect = veloq_driver_core::op::Connect<RawHandle, SockAddrStorage>;
pub type Close = veloq_driver_core::op::Close<RawHandle>;
pub type Fsync = veloq_driver_core::op::Fsync<RawHandle>;
pub type Wakeup = veloq_driver_core::op::Wakeup<RawHandle>;
pub type Accept = veloq_driver_core::op::Accept<RawHandle, SockAddrStorage>;
pub type SendTo = veloq_driver_core::op::SendTo<RawHandle>;
pub type SyncFileRange = veloq_driver_core::op::SyncFileRange<RawHandle>;
pub type Fallocate = veloq_driver_core::op::Fallocate<RawHandle>;
pub type UdpRecvStream = veloq_driver_core::op::UdpRecvStream<RawHandle>;
pub type UdpRefill = veloq_driver_core::op::UdpRefill<RawHandle>;

pub use veloq_driver_core::op::{
    DetachedOp, DetachedSubmitter, IntoPlatformOp, LocalSubmitter, Op, OpKind, OpLifecycle,
    OpResult, Open, Timeout, UdpRecvDatagram,
};

pub type LocalOp<T> = veloq_driver_core::op::LocalOp<T, PlatformDriver>;

pub trait OpSubmitter: Clone + std::marker::Send + Sync + 'static {
    type Future<T: IntoPlatformOp<<PlatformDriver as Driver>::Op> + std::marker::Send + 'static>:
        Future<Output = OpResult<T>>;

    fn submit<T>(&self, op: Op<T>, driver: Rc<RefCell<PlatformDriver>>) -> Self::Future<T>
    where
        T: IntoPlatformOp<<PlatformDriver as Driver>::Op> + std::marker::Send + 'static;

    fn from_current_context() -> std::io::Result<Self>;
}

impl OpSubmitter for LocalSubmitter {
    type Future<T: IntoPlatformOp<<PlatformDriver as Driver>::Op> + std::marker::Send + 'static> =
        LocalOp<T>;

    fn submit<T>(&self, op: Op<T>, driver: Rc<RefCell<PlatformDriver>>) -> LocalOp<T>
    where
        T: IntoPlatformOp<<PlatformDriver as Driver>::Op> + std::marker::Send + 'static,
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
    type Future<T: IntoPlatformOp<<PlatformDriver as Driver>::Op> + std::marker::Send + 'static> =
        DetachedOp<T, <PlatformDriver as Driver>::Op>;

    fn submit<T>(&self, op: Op<T>, driver: Rc<RefCell<PlatformDriver>>) -> Self::Future<T>
    where
        T: IntoPlatformOp<<PlatformDriver as Driver>::Op> + std::marker::Send + 'static,
    {
        <DetachedSubmitter as veloq_driver_core::op::OpSubmitter<PlatformDriver>>::submit(
            self, op, driver,
        )
    }

    fn from_current_context() -> std::io::Result<Self> {
        <DetachedSubmitter as veloq_driver_core::op::OpSubmitter<PlatformDriver>>::from_current_context()
    }
}
