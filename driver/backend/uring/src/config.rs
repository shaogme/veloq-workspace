use std::{mem, num::NonZeroU32};
use veloq_driver_core::{
    BorrowedRawHandle as CoreBorrowedRawHandle, OwnedRawHandle as CoreOwnedRawHandle,
    RawHandle as CoreRawHandle, RawHandleMeta,
};
pub use veloq_driver_core::{IoFd, RawHandleKind};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UringRawHandle {
    File { fd: i32 },
    Socket { fd: i32 },
}

impl UringRawHandle {
    #[inline]
    pub const fn for_file(fd: i32) -> Self {
        Self::File { fd }
    }

    #[inline]
    pub fn for_socket(fd: i32) -> Self {
        Self::Socket { fd }
    }

    #[inline]
    pub const fn as_fd(self) -> i32 {
        match self {
            Self::File { fd } => fd,
            Self::Socket { fd, .. } => fd,
        }
    }
}

impl RawHandleMeta for UringRawHandle {
    #[inline]
    fn kind(self) -> RawHandleKind {
        match self {
            Self::File { .. } => RawHandleKind::File,
            Self::Socket { .. } => RawHandleKind::Socket,
        }
    }

    #[inline]
    fn close(self) {
        let fd = self.as_fd();
        if fd >= 0 {
            // SAFETY: `fd` is owned by this value.
            unsafe {
                libc::close(fd);
            }
        }
    }
}

pub type RawHandle = CoreRawHandle<UringRawHandle>;
pub type OwnedRawHandle = CoreOwnedRawHandle<UringRawHandle>;
pub type BorrowedRawHandle<'a> = CoreBorrowedRawHandle<'a, UringRawHandle>;

#[repr(transparent)]
#[derive(Clone, Copy)]
pub struct SockAddrStorage(pub libc::sockaddr_storage);

impl Default for SockAddrStorage {
    fn default() -> Self {
        Self(unsafe { mem::zeroed() })
    }
}

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
            // SAFETY: 1024 is non-zero.
            entries: unsafe { NonZeroU32::new_unchecked(1024) },
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
