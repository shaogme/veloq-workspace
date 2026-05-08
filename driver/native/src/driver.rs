pub mod op_registry {
    pub use veloq_driver_core::op_registry::*;
}

pub mod slot {
    pub use veloq_driver_core::slot::*;
}

pub use veloq_driver_core::driver::{
    DriveMode, DriveOutcome, Driver, DriverControlCommand, RegisterFd, RemoteWaker,
};

#[cfg(target_os = "linux")]
pub use veloq_driver_uring::UringDriver as PlatformDriver;

#[cfg(target_os = "windows")]
pub use veloq_driver_iocp::CloseMode;
#[cfg(target_os = "windows")]
pub use veloq_driver_iocp::IocpDriver as PlatformDriver;

#[cfg(feature = "test-hooks")]
pub use veloq_driver_core::driver::test_hooks;
