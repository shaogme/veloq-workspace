use std::num::NonZeroU32;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(transparent)]
pub struct RawHandle {
    pub fd: i32,
}

impl From<i32> for RawHandle {
    fn from(fd: i32) -> Self {
        Self { fd }
    }
}

impl From<usize> for RawHandle {
    fn from(fd: usize) -> Self {
        Self { fd: fd as i32 }
    }
}

impl From<RawHandle> for usize {
    fn from(handle: RawHandle) -> Self {
        handle.fd as usize
    }
}

#[repr(transparent)]
#[derive(Clone, Copy)]
pub struct SockAddrStorage(pub libc::sockaddr_storage);

impl Default for SockAddrStorage {
    fn default() -> Self {
        Self(unsafe { std::mem::zeroed() })
    }
}

pub type IoFd = veloq_driver_core::IoFd<RawHandle>;

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IoMode {
    Interrupt,
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

impl UringConfig {
    pub fn registration_mode(mut self, mode: BufferRegistrationMode) -> Self {
        self.registration_mode = mode;
        self
    }
}
