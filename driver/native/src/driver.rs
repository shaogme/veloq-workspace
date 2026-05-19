pub mod slot {
    pub use veloq_driver_core::slot::*;
}

pub use veloq_driver_core::driver::{
    ContextDriverProvider, DriveMode, DriveOutcome, Driver, DriverControlCommand, RegisterFd,
    RemoteWaker, RuntimeContextDriver,
};

#[cfg(target_os = "linux")]
pub type PlatformDriver<'a> = veloq_driver_uring::UringDriver<'a>;
#[cfg(target_os = "linux")]
pub type PlatformOp = veloq_driver_uring::UringOp;
#[cfg(target_os = "linux")]
pub type PlatformUP = veloq_driver_uring::UringUserPayload;

#[cfg(target_os = "windows")]
pub use veloq_driver_iocp::CloseMode;
#[cfg(target_os = "windows")]
pub type PlatformDriver<'a> = veloq_driver_iocp::IocpDriver<'a>;
#[cfg(target_os = "windows")]
pub type PlatformOp = veloq_driver_iocp::IocpKernelOp;
#[cfg(target_os = "windows")]
pub type PlatformUP = veloq_driver_iocp::IocpUserPayload;

#[cfg(feature = "test-hooks")]
pub use veloq_driver_core::driver::test_hooks;
