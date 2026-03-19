use std::num::NonZeroU32;

#[cfg(windows)]
pub use veloq_driver_iocp::{BufferRegistrationMode, IocpConfig};

#[cfg(not(windows))]
pub use veloq_driver_uring::{BufferRegistrationMode, IoMode, UringConfig};

/// I/O submission mode.
#[cfg(windows)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum IoMode {
    /// Interrupt-based I/O.
    #[default]
    Interrupt,
    /// Polling-based I/O with a specific timeout.
    Polling(NonZeroU32),
}

/// Configuration for the IOCP driver (Shim for non-Windows platforms).
#[cfg(not(windows))]
#[derive(Debug, Clone)]
pub struct IocpConfig {
    /// Number of entries in the completion port.
    pub entries: NonZeroU32,
    /// Mode for buffer registration.
    pub registration_mode: BufferRegistrationMode,
}

#[cfg(not(windows))]
impl IocpConfig {
    /// Sets the registration mode.
    pub fn registration_mode(mut self, mode: BufferRegistrationMode) -> Self {
        self.registration_mode = mode;
        self
    }
}

#[cfg(not(windows))]
impl Default for IocpConfig {
    fn default() -> Self {
        Self {
            entries: unsafe { NonZeroU32::new_unchecked(1024) },
            registration_mode: BufferRegistrationMode::Strict,
        }
    }
}

/// Configuration for the io_uring driver (Shim for Windows platform).
#[cfg(windows)]
#[derive(Debug, Clone)]
pub struct UringConfig {
    /// I/O mode (Interrupt or Polling).
    pub mode: IoMode,
    /// Number of entries in the ring.
    pub entries: NonZeroU32,
    /// Mode for buffer registration.
    pub registration_mode: BufferRegistrationMode,
}

#[cfg(windows)]
impl UringConfig {
    /// Sets the registration mode.
    pub fn registration_mode(mut self, mode: BufferRegistrationMode) -> Self {
        self.registration_mode = mode;
        self
    }
}

#[cfg(windows)]
impl Default for UringConfig {
    fn default() -> Self {
        Self {
            mode: IoMode::Interrupt,
            entries: unsafe { NonZeroU32::new_unchecked(1024) },
            registration_mode: BufferRegistrationMode::Strict,
        }
    }
}
