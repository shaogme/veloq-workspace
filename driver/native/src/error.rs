pub use veloq_driver_core::{
    DriverCoreError, DriverReport, DriverResult, driver_core_error,
    driver_core_error_fallback_errno, driver_error, driver_report_to_event_res,
};

#[cfg(unix)]
pub use veloq_driver_uring::{UringError as Error, UringResult as PlatformResult};

#[cfg(windows)]
pub use veloq_driver_iocp::{IocpError as Error, IocpResult as PlatformResult};
