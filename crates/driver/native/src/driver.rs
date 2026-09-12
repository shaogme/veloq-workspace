pub mod slot {
    pub use veloq_driver_core::slot::*;
}

pub use veloq_driver_core::driver::{
    BufferRegistrationStatus, ContextDriverProvider, DriveMode, DriveOutcome, Driver,
    DriverCapabilities, DriverCapability, DriverRaw, RegisterFd, RemoteWaker, RuntimeContextDriver,
};

#[cfg(target_os = "linux")]
pub type PlatformDriver<'a> = veloq_driver_uring::UringDriver<'a>;
#[cfg(target_os = "linux")]
pub type PlatformOp = veloq_driver_uring::UringOp;
#[cfg(target_os = "linux")]
pub type PlatformUP = veloq_driver_uring::UringUserPayload;
/// The active backend's [`veloq_driver_core::slot::SlotSpec`].
///
/// Both backends spell it as an uninhabited marker type with no lifetime, so unlike
/// [`PlatformDriver`] this alias takes no parameter — which is what lets facade-level types
/// name driver futures without threading the driver's lifetime through.
#[cfg(target_os = "linux")]
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
