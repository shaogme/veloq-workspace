pub use veloq_driver_core::{DriverReport, DriverResult};

#[cfg(unix)]
pub use veloq_driver_uring::{UringError as Error, UringResult as PlatformResult};

#[cfg(windows)]
pub use veloq_driver_iocp::{IocpError as Error, IocpResult as PlatformResult};
