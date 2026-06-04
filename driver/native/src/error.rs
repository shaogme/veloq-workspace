pub use veloq_driver_core::{
    DriverErrorKind, DriverErrorReport, DriverResult, ResultAsDriverExt, driver_error,
    driver_error_kind_fallback_errno, driver_error_report_to_event_res,
};

#[cfg(unix)]
pub use veloq_driver_uring::{UringError as Error, UringResult as PlatformResult};

#[cfg(windows)]
pub use veloq_driver_iocp::{IocpError as Error, IocpResult as PlatformResult};
