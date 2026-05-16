pub mod slot {
    pub use veloq_driver_core::slot::*;
}

pub use veloq_driver_core::driver::{
    DriveMode, DriveOutcome, Driver, DriverControlCommand, RegisterFd, RemoteWaker,
};

#[cfg(target_os = "linux")]
pub type PlatformDriver<'a> = veloq_driver_uring::UringDriver<'a>;
#[cfg(target_os = "linux")]
pub type PlatformOp = veloq_driver_uring::op::UringOp;
#[cfg(target_os = "linux")]
pub type PlatformUP = veloq_driver_uring::op::UringUserPayload;

#[cfg(target_os = "windows")]
pub use veloq_driver_iocp::CloseMode;
#[cfg(target_os = "windows")]
pub type PlatformDriver<'a> = veloq_driver_iocp::IocpDriver<'a>;
#[cfg(target_os = "windows")]
pub type PlatformOp = veloq_driver_iocp::op::IocpKernelOp;
#[cfg(target_os = "windows")]
pub type PlatformUP = veloq_driver_iocp::op::IocpUserPayload;

#[cfg(feature = "test-hooks")]
pub use veloq_driver_core::driver::test_hooks;
