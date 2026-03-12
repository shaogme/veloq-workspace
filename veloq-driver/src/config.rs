#[cfg(windows)]
pub use veloq_driver_iocp::{BufferRegistrationMode, IocpConfig};
#[cfg(not(windows))]
pub use veloq_driver_uring::{BufferRegistrationMode, IoMode, IocpConfig, UringConfig};

#[cfg(windows)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IoMode {
    Interrupt,
    Polling(std::num::NonZeroU32),
}

#[cfg(windows)]
#[derive(Debug, Clone)]
pub struct UringConfig {
    pub mode: IoMode,
    pub entries: std::num::NonZeroU32,
    pub registration_mode: BufferRegistrationMode,
}

#[cfg(windows)]
impl AsRef<UringConfig> for UringConfig {
    fn as_ref(&self) -> &UringConfig {
        self
    }
}

#[cfg(windows)]
impl Default for UringConfig {
    fn default() -> Self {
        Self {
            mode: IoMode::Interrupt,
            entries: std::num::NonZeroU32::new(1024).unwrap(),
            registration_mode: BufferRegistrationMode::Strict,
        }
    }
}

#[cfg(windows)]
impl UringConfig {
    pub fn registration_mode(mut self, mode: BufferRegistrationMode) -> Self {
        self.registration_mode = mode;
        self
    }
}
