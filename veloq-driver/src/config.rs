use std::num::NonZeroU32;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum BufferRegistrationMode {
    #[default]
    Strict,
    Compatible,
}

impl BufferRegistrationMode {
    #[inline]
    pub const fn is_strict(self) -> bool {
        matches!(self, Self::Strict)
    }
}

/// I/O Driver Operation Mode
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IoMode {
    /// Interrupt driven mode (syscalls + waiting)
    Interrupt,
    /// Polling mode (SQPOLL on Linux, busy-wait on Windows)
    Polling(NonZeroU32),
}

#[derive(Debug, Clone)]
pub struct UringConfig {
    pub mode: IoMode,
    pub entries: NonZeroU32,
    pub registration_mode: BufferRegistrationMode,
}

impl AsRef<UringConfig> for UringConfig {
    fn as_ref(&self) -> &UringConfig {
        self
    }
}

impl Default for UringConfig {
    fn default() -> Self {
        Self {
            mode: IoMode::Interrupt,
            entries: NonZeroU32::new(1024).unwrap(),
            registration_mode: BufferRegistrationMode::Strict,
        }
    }
}

#[derive(Debug, Clone)]
pub struct IocpConfig {
    pub entries: NonZeroU32,
    pub registration_mode: BufferRegistrationMode,
}

impl AsRef<IocpConfig> for IocpConfig {
    fn as_ref(&self) -> &IocpConfig {
        self
    }
}

impl Default for IocpConfig {
    fn default() -> Self {
        Self {
            entries: NonZeroU32::new(1024).unwrap(),
            registration_mode: BufferRegistrationMode::Strict,
        }
    }
}

impl UringConfig {
    pub fn registration_mode(mut self, mode: BufferRegistrationMode) -> Self {
        self.registration_mode = mode;
        self
    }
}

impl IocpConfig {
    pub fn registration_mode(mut self, mode: BufferRegistrationMode) -> Self {
        self.registration_mode = mode;
        self
    }
}
