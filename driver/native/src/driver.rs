pub mod slot {
    pub use veloq_driver_core::slot::*;
}

pub use veloq_driver_core::driver::{
    DriveMode, DriveOutcome, Driver, DriverControlCommand, RegisterFd, RemoteWaker,
};

#[cfg(target_os = "linux")]
pub type PlatformDriver<'a> = veloq_driver_uring::UringDriver<'a>;

#[cfg(target_os = "windows")]
pub use veloq_driver_iocp::CloseMode;
#[cfg(target_os = "windows")]
pub type PlatformDriver<'a> = veloq_driver_iocp::IocpDriver<'a>;

#[cfg(feature = "test-hooks")]
pub use veloq_driver_core::driver::test_hooks;
