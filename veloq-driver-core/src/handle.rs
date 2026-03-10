#[cfg(target_os = "windows")]
use std::os::raw::c_void;

/// Cross-platform raw OS handle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(target_os = "windows", repr(transparent))]
pub struct RawHandle {
    #[cfg(unix)]
    pub fd: std::os::fd::RawFd,
    #[cfg(windows)]
    pub handle: windows_sys::Win32::Foundation::HANDLE,
}

unsafe impl Send for RawHandle {}
unsafe impl Sync for RawHandle {}

#[cfg(unix)]
impl std::ops::Deref for RawHandle {
    type Target = std::os::fd::RawFd;

    fn deref(&self) -> &Self::Target {
        &self.fd
    }
}

#[cfg(unix)]
impl From<RawHandle> for std::os::fd::RawFd {
    fn from(handle: RawHandle) -> Self {
        handle.fd
    }
}

#[cfg(unix)]
impl From<i32> for RawHandle {
    fn from(fd: i32) -> Self {
        RawHandle { fd }
    }
}

#[cfg(unix)]
impl From<usize> for RawHandle {
    fn from(handle: usize) -> Self {
        RawHandle { fd: handle as i32 }
    }
}

#[cfg(unix)]
impl From<RawHandle> for usize {
    fn from(handle: RawHandle) -> Self {
        handle.fd as usize
    }
}

#[cfg(windows)]
impl From<*mut c_void> for RawHandle {
    fn from(handle: *mut c_void) -> Self {
        RawHandle {
            handle: handle as _,
        }
    }
}

#[cfg(windows)]
impl From<usize> for RawHandle {
    fn from(handle: usize) -> Self {
        RawHandle {
            handle: handle as _,
        }
    }
}

#[cfg(all(windows, target_pointer_width = "64"))]
impl From<u64> for RawHandle {
    fn from(handle: u64) -> Self {
        RawHandle {
            handle: handle as _,
        }
    }
}

#[cfg(all(windows, target_pointer_width = "32"))]
impl From<u32> for RawHandle {
    fn from(handle: u32) -> Self {
        RawHandle {
            handle: handle as _,
        }
    }
}

#[cfg(windows)]
impl std::ops::Deref for RawHandle {
    type Target = windows_sys::Win32::Foundation::HANDLE;

    fn deref(&self) -> &Self::Target {
        &self.handle
    }
}

#[cfg(windows)]
impl From<RawHandle> for windows_sys::Win32::Foundation::HANDLE {
    fn from(handle: RawHandle) -> Self {
        handle.handle
    }
}

#[cfg(windows)]
impl From<RawHandle> for usize {
    fn from(handle: RawHandle) -> Self {
        handle.handle as usize
    }
}

#[cfg(unix)]
pub type SockAddrStorage = libc::sockaddr_storage;
#[cfg(windows)]
pub type SockAddrStorage = windows_sys::Win32::Networking::WinSock::SOCKADDR_STORAGE;

/// Represents the source of an IO operation: either a raw handle or a registered index.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IoFd {
    /// A raw system handle (fd on Unix, HANDLE on Windows).
    Raw(RawHandle),
    /// A registered index for pre-registered file descriptors.
    Fixed(u32),
}

impl IoFd {
    /// Returns the raw handle if this is a Raw variant.
    pub fn raw(&self) -> Option<RawHandle> {
        match self {
            Self::Raw(fd) => Some(*fd),
            Self::Fixed(_) => None,
        }
    }
}

impl From<RawHandle> for IoFd {
    fn from(handle: RawHandle) -> Self {
        Self::Raw(handle)
    }
}
