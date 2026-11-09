use core::ffi::CStr;

use crate::alloc_crate as alloc;
use crate::io::{ErrorKind, os_error::RawOsError};

use alloc::{format, string::String};
use libc::{
    E2BIG, EACCES, EADDRINUSE, EADDRNOTAVAIL, EAGAIN, EBUSY, ECONNABORTED, ECONNREFUSED,
    ECONNRESET, EDEADLK, EDQUOT, EEXIST, EFBIG, EHOSTUNREACH, EINPROGRESS, EINTR, EINVAL, EISDIR,
    ELOOP, EMFILE, EMLINK, ENAMETOOLONG, ENETDOWN, ENETUNREACH, ENFILE, ENOENT, ENOMEM, ENOSPC,
    ENOSYS, ENOTCONN, ENOTDIR, ENOTEMPTY, EOPNOTSUPP, EPERM, EPIPE, EROFS, ESPIPE, ESTALE,
    ETIMEDOUT, ETXTBSY, EWOULDBLOCK, EXDEV, c_char, c_int, strerror_r,
};

#[cfg(target_os = "linux")]
mod linux_errno {
    pub use libc::__errno_location as current;
}

#[cfg(target_os = "android")]
mod android_errno {
    pub use libc::__errno as current;
}

#[inline]
fn last_os_error() -> RawOsError {
    #[cfg(target_os = "linux")]
    unsafe {
        *linux_errno::current() as RawOsError
    }

    #[cfg(target_os = "android")]
    unsafe {
        *android_errno::current() as RawOsError
    }
}

fn error_string(errno: RawOsError) -> String {
    let mut buf = [0u8; 256];
    let ret = unsafe { strerror_r(errno as c_int, buf.as_mut_ptr() as *mut c_char, buf.len()) };
    if ret == 0
        && let Ok(s) = CStr::from_bytes_until_nul(&buf)
        && let Ok(valid_str) = s.to_str()
    {
        return String::from(valid_str);
    }
    format!("OS error {errno}")
}

fn decode_error_kind(errno: RawOsError) -> ErrorKind {
    match errno {
        E2BIG => ErrorKind::ArgumentListTooLong,
        EACCES | EPERM => ErrorKind::PermissionDenied,
        EADDRINUSE => ErrorKind::AddrInUse,
        EADDRNOTAVAIL => ErrorKind::AddrNotAvailable,
        EBUSY => ErrorKind::ResourceBusy,
        ECONNABORTED => ErrorKind::ConnectionAborted,
        ECONNREFUSED => ErrorKind::ConnectionRefused,
        ECONNRESET => ErrorKind::ConnectionReset,
        EDEADLK => ErrorKind::Deadlock,
        EDQUOT => ErrorKind::QuotaExceeded,
        EEXIST => ErrorKind::AlreadyExists,
        EFBIG => ErrorKind::FileTooLarge,
        EHOSTUNREACH => ErrorKind::HostUnreachable,
        EINTR => ErrorKind::Interrupted,
        EINVAL => ErrorKind::InvalidInput,
        EISDIR => ErrorKind::IsADirectory,
        ELOOP => ErrorKind::FilesystemLoop,
        EMFILE | ENFILE => ErrorKind::TooManyOpenFiles,
        EMLINK => ErrorKind::TooManyLinks,
        ENAMETOOLONG => ErrorKind::InvalidFilename,
        ENETDOWN => ErrorKind::NetworkDown,
        ENETUNREACH => ErrorKind::NetworkUnreachable,
        ENOENT => ErrorKind::NotFound,
        ENOMEM => ErrorKind::OutOfMemory,
        ENOSPC => ErrorKind::StorageFull,
        ENOSYS => ErrorKind::Unsupported,
        ENOTCONN => ErrorKind::NotConnected,
        ENOTDIR => ErrorKind::NotADirectory,
        ENOTEMPTY => ErrorKind::DirectoryNotEmpty,
        EPIPE => ErrorKind::BrokenPipe,
        EROFS => ErrorKind::ReadOnlyFilesystem,
        ESPIPE => ErrorKind::NotSeekable,
        ESTALE => ErrorKind::StaleNetworkFileHandle,
        ETIMEDOUT => ErrorKind::TimedOut,
        ETXTBSY => ErrorKind::ExecutableFileBusy,
        EXDEV => ErrorKind::CrossesDevices,
        EINPROGRESS => ErrorKind::InProgress,
        EOPNOTSUPP => ErrorKind::Unsupported,
        x if x == EAGAIN || x == EWOULDBLOCK => ErrorKind::WouldBlock,
        _ => ErrorKind::Uncategorized,
    }
}

pub(crate) struct Platform;

impl Platform {
    pub(crate) fn last_os_error() -> RawOsError {
        last_os_error()
    }

    pub(crate) fn error_string(errno: RawOsError) -> String {
        error_string(errno)
    }

    pub(crate) fn decode_error_kind(errno: RawOsError) -> ErrorKind {
        decode_error_kind(errno)
    }
}
