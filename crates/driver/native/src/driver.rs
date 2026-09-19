pub mod slot {
    pub use veloq_driver_core::slot::*;
}

pub use veloq_driver_core::driver::{
    BufferRegistrationStatus, ContextDriverProvider, DriveMode, DriveOutcome, Driver, DriverRaw,
    RegisterFd, RemoteWaker, RuntimeContextDriver,
};

#[cfg(any(target_os = "linux", target_os = "android"))]
pub type PlatformDriver<'a> = veloq_driver_uring::UringDriver<'a>;
#[cfg(any(target_os = "linux", target_os = "android"))]
pub type PlatformOp = veloq_driver_uring::UringOp;
#[cfg(any(target_os = "linux", target_os = "android"))]
pub type PlatformUP = veloq_driver_uring::UringUserPayload;
/// The active backend's [`veloq_driver_core::slot::SlotSpec`].
///
/// Both backends spell it as an uninhabited marker type with no lifetime, so unlike
/// [`PlatformDriver`] this alias takes no parameter — which is what lets facade-level types
/// name driver futures without threading the driver's lifetime through.
#[cfg(any(target_os = "linux", target_os = "android"))]
pub type PlatformSlotSpec = veloq_driver_uring::UringSlotSpec;
#[cfg(target_os = "windows")]
pub type PlatformSlotSpec = veloq_driver_iocp::IocpSlotSpec;

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
