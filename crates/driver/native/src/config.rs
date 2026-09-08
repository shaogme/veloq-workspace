use veloq_std::num::NonZeroU32;

#[cfg(windows)]
use veloq_std::num::{NonZeroU16, NonZeroUsize};

#[cfg(windows)]
pub use veloq_driver_iocp::{BufferRegistrationMode, IocpConfig};
use veloq_std::nz;

#[cfg(not(windows))]
pub use veloq_driver_uring::{
    BufferRegistrationMode, FileTableExhaustion, IoMode, ProvidedBufConfig, UringConfig,
};

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
            entries: nz!(1024),
            registration_mode: BufferRegistrationMode::Strict,
        }
    }
}

/// Behaviour once the registered file table is full (Shim for Windows platform).
#[cfg(windows)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum FileTableExhaustion {
    /// Hand out unregistered descriptors instead.
    #[default]
    Fallback,
    /// Reject the registration with an error.
    Fail,
}

/// Shape of the provided-buffer ring (Shim for Windows platform).
#[cfg(windows)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProvidedBufConfig {
    /// Number of ring entries.
    pub entries: NonZeroU16,
    /// Capacity of each buffer.
    pub buf_size: NonZeroUsize,
}

#[cfg(windows)]
impl ProvidedBufConfig {
    pub fn new(entries: NonZeroU16, buf_size: NonZeroUsize) -> Self {
        Self { entries, buf_size }
    }
}

#[cfg(windows)]
impl Default for ProvidedBufConfig {
    fn default() -> Self {
        Self {
            entries: nz!(256u16),
            buf_size: nz!(4096),
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
    /// Provided-buffer ring to register, or `None` to run without one.
    pub provided_buffers: Option<ProvidedBufConfig>,
    /// Size of the kernel's registered file table, independent of `entries`.
    pub file_table_capacity: u32,
    /// Behaviour once `file_table_capacity` entries are in use.
    pub file_table_exhaustion: FileTableExhaustion,
}

#[cfg(windows)]
impl UringConfig {
    /// Sets the registration mode.
    pub fn registration_mode(mut self, mode: BufferRegistrationMode) -> Self {
        self.registration_mode = mode;
        self
    }

    /// Sets the provided-buffer ring.
    pub fn provided_buffers(mut self, provided_buffers: Option<ProvidedBufConfig>) -> Self {
        self.provided_buffers = provided_buffers;
        self
    }

    /// Sets the registered file table capacity.
    pub fn file_table_capacity(mut self, capacity: u32) -> Self {
        self.file_table_capacity = capacity;
        self
    }

    /// Sets what happens once the registered file table is full.
    pub fn file_table_exhaustion(mut self, exhaustion: FileTableExhaustion) -> Self {
        self.file_table_exhaustion = exhaustion;
        self
    }
}

#[cfg(windows)]
impl Default for UringConfig {
    fn default() -> Self {
        Self {
            mode: IoMode::Interrupt,
            entries: nz!(1024),
            registration_mode: BufferRegistrationMode::Strict,
            provided_buffers: None,
            file_table_capacity: 1024,
            file_table_exhaustion: FileTableExhaustion::Fallback,
        }
    }
}
