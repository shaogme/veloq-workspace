use core::ffi::CStr;

use crate::alloc_crate as alloc;
use crate::io::{ErrorKind, os_error::RawOsError};

use alloc::{format, string::String};

#[inline]
pub fn last_os_error() -> RawOsError {
    unsafe { *libc::__errno_location() as RawOsError }
}

pub fn error_string(errno: RawOsError) -> String {
    let mut buf = [0u8; 256];
    let ret = unsafe {
        libc::strerror_r(
            errno as libc::c_int,
            buf.as_mut_ptr() as *mut libc::c_char,
            buf.len(),
        )
    };
    if ret == 0
        && let Ok(s) = CStr::from_bytes_until_nul(&buf)
        && let Ok(valid_str) = s.to_str()
    {
        return String::from(valid_str);
    }
    format!("OS error {errno}")
}

pub fn decode_error_kind(errno: RawOsError) -> ErrorKind {
    match errno {
        libc::E2BIG => ErrorKind::ArgumentListTooLong,
        libc::EACCES | libc::EPERM => ErrorKind::PermissionDenied,
        libc::EADDRINUSE => ErrorKind::AddrInUse,
        libc::EADDRNOTAVAIL => ErrorKind::AddrNotAvailable,
        libc::EBUSY => ErrorKind::ResourceBusy,
        libc::ECONNABORTED => ErrorKind::ConnectionAborted,
        libc::ECONNREFUSED => ErrorKind::ConnectionRefused,
        libc::ECONNRESET => ErrorKind::ConnectionReset,
        libc::EDEADLK => ErrorKind::Deadlock,
        libc::EDQUOT => ErrorKind::QuotaExceeded,
        libc::EEXIST => ErrorKind::AlreadyExists,
        libc::EFBIG => ErrorKind::FileTooLarge,
        libc::EHOSTUNREACH => ErrorKind::HostUnreachable,
        libc::EINTR => ErrorKind::Interrupted,
        libc::EINVAL => ErrorKind::InvalidInput,
        libc::EISDIR => ErrorKind::IsADirectory,
        libc::ELOOP => ErrorKind::FilesystemLoop,
        libc::EMFILE | libc::ENFILE => ErrorKind::TooManyOpenFiles,
        libc::EMLINK => ErrorKind::TooManyLinks,
        libc::ENAMETOOLONG => ErrorKind::InvalidFilename,
        libc::ENETDOWN => ErrorKind::NetworkDown,
        libc::ENETUNREACH => ErrorKind::NetworkUnreachable,
        libc::ENOENT => ErrorKind::NotFound,
        libc::ENOMEM => ErrorKind::OutOfMemory,
        libc::ENOSPC => ErrorKind::StorageFull,
        libc::ENOSYS => ErrorKind::Unsupported,
        libc::ENOTCONN => ErrorKind::NotConnected,
        libc::ENOTDIR => ErrorKind::NotADirectory,
        libc::ENOTEMPTY => ErrorKind::DirectoryNotEmpty,
        libc::EPIPE => ErrorKind::BrokenPipe,
        libc::EROFS => ErrorKind::ReadOnlyFilesystem,
        libc::ESPIPE => ErrorKind::NotSeekable,
        libc::ESTALE => ErrorKind::StaleNetworkFileHandle,
        libc::ETIMEDOUT => ErrorKind::TimedOut,
        libc::ETXTBSY => ErrorKind::ExecutableFileBusy,
        libc::EXDEV => ErrorKind::CrossesDevices,
        libc::EINPROGRESS => ErrorKind::InProgress,
        libc::EOPNOTSUPP => ErrorKind::Unsupported,
        x if x == libc::EAGAIN || x == libc::EWOULDBLOCK => ErrorKind::WouldBlock,
        _ => ErrorKind::Uncategorized,
    }
}
