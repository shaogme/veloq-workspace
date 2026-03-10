use std::cell::RefCell;
use std::future::Future;
use std::rc::Rc;

use crate::driver::{Driver, PlatformDriver};

pub use veloq_driver_core::op::{
    Accept, Close, Connect, DetachedOp, DetachedSubmitter, Fallocate, Fsync, IntoPlatformOp, IoFd,
    LocalSubmitter, Op, OpKind, OpLifecycle, OpResult, Open, ReadFixed, Recv, Send, SendTo,
    SyncFileRange, Timeout, UdpRecvDatagram, UdpRecvStream, UdpRefill, Wakeup, WriteFixed,
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
