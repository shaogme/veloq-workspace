use core::ptr;

use crate::alloc_crate as alloc;
use crate::io::{ErrorKind, os_error::RawOsError};

use alloc::{format, string::String};
use windows_sys::Win32::Foundation::{
    ERROR_ACCESS_DENIED, ERROR_ALREADY_EXISTS, ERROR_BAD_NET_NAME, ERROR_BAD_NETPATH,
    ERROR_BAD_PATHNAME, ERROR_BROKEN_PIPE, ERROR_BUSY, ERROR_CALL_NOT_IMPLEMENTED,
    ERROR_CANT_RESOLVE_FILENAME, ERROR_DIR_NOT_EMPTY, ERROR_DIRECTORY, ERROR_DISK_FULL,
    ERROR_DISK_QUOTA_EXCEEDED, ERROR_FILE_EXISTS, ERROR_FILE_NOT_FOUND, ERROR_FILE_TOO_LARGE,
    ERROR_FILENAME_EXCED_RANGE, ERROR_HANDLE_DISK_FULL, ERROR_HOST_UNREACHABLE,
    ERROR_INVALID_DRIVE, ERROR_INVALID_NAME, ERROR_INVALID_PARAMETER, ERROR_NETWORK_UNREACHABLE,
    ERROR_NO_DATA, ERROR_NOT_ENOUGH_MEMORY, ERROR_NOT_SAME_DEVICE, ERROR_OPERATION_ABORTED,
    ERROR_OUTOFMEMORY, ERROR_PATH_NOT_FOUND, ERROR_POSSIBLE_DEADLOCK, ERROR_SEEK_ON_DEVICE,
    ERROR_SEM_TIMEOUT, ERROR_TIMEOUT, ERROR_TOO_MANY_LINKS, ERROR_TOO_MANY_OPEN_FILES,
    ERROR_WRITE_PROTECT, GetLastError, WAIT_TIMEOUT,
};
use windows_sys::Win32::Networking::WinSock::{
    WSAEACCES, WSAEADDRINUSE, WSAEADDRNOTAVAIL, WSAECONNABORTED, WSAECONNREFUSED, WSAECONNRESET,
    WSAEDQUOT, WSAEHOSTUNREACH, WSAEINVAL, WSAEMFILE, WSAENETDOWN, WSAENETUNREACH, WSAENOTCONN,
    WSAESHUTDOWN, WSAETIMEDOUT, WSAEWOULDBLOCK,
};
use windows_sys::Win32::System::Diagnostics::Debug::{
    FORMAT_MESSAGE_FROM_SYSTEM, FORMAT_MESSAGE_IGNORE_INSERTS, FormatMessageW,
};

#[inline]
pub fn last_os_error() -> RawOsError {
    unsafe { GetLastError() as RawOsError }
}

pub fn error_string(code: RawOsError) -> String {
    let mut buf = [0u16; 1024];
    let flags = FORMAT_MESSAGE_FROM_SYSTEM | FORMAT_MESSAGE_IGNORE_INSERTS;
    let res = unsafe {
        FormatMessageW(
            flags,
            ptr::null(),
            code as u32,
            0,
            buf.as_mut_ptr(),
            buf.len() as u32,
            ptr::null(),
        )
    };
    if res == 0 {
        return format!("OS error {code}");
    }
    let mut msg = String::from_utf16_lossy(&buf[..res as usize]);
    let trimmed_len = msg
        .trim_end_matches(|c: char| c == '\r' || c == '\n' || c.is_whitespace())
        .len();
    msg.truncate(trimmed_len);
    if msg.is_empty() {
        format!("OS error {code}")
    } else {
        msg
    }
}

pub fn decode_error_kind(errno: RawOsError) -> ErrorKind {
    match errno as u32 {
        ERROR_ACCESS_DENIED => ErrorKind::PermissionDenied,
        ERROR_ALREADY_EXISTS | ERROR_FILE_EXISTS => ErrorKind::AlreadyExists,
        ERROR_BROKEN_PIPE | ERROR_NO_DATA => ErrorKind::BrokenPipe,
        ERROR_FILE_NOT_FOUND | ERROR_PATH_NOT_FOUND | ERROR_INVALID_DRIVE | ERROR_BAD_NETPATH
        | ERROR_BAD_NET_NAME => ErrorKind::NotFound,
        ERROR_INVALID_NAME | ERROR_BAD_PATHNAME | ERROR_FILENAME_EXCED_RANGE => {
            ErrorKind::InvalidFilename
        }
        ERROR_INVALID_PARAMETER => ErrorKind::InvalidInput,
        ERROR_NOT_ENOUGH_MEMORY | ERROR_OUTOFMEMORY => ErrorKind::OutOfMemory,
        ERROR_SEM_TIMEOUT | WAIT_TIMEOUT | ERROR_OPERATION_ABORTED | ERROR_TIMEOUT => {
            ErrorKind::TimedOut
        }
        ERROR_CALL_NOT_IMPLEMENTED => ErrorKind::Unsupported,
        ERROR_HOST_UNREACHABLE => ErrorKind::HostUnreachable,
        ERROR_NETWORK_UNREACHABLE => ErrorKind::NetworkUnreachable,
        ERROR_DIRECTORY => ErrorKind::NotADirectory,
        ERROR_DIR_NOT_EMPTY => ErrorKind::DirectoryNotEmpty,
        ERROR_WRITE_PROTECT => ErrorKind::ReadOnlyFilesystem,
        ERROR_DISK_FULL | ERROR_HANDLE_DISK_FULL => ErrorKind::StorageFull,
        ERROR_SEEK_ON_DEVICE => ErrorKind::NotSeekable,
        ERROR_DISK_QUOTA_EXCEEDED => ErrorKind::QuotaExceeded,
        ERROR_FILE_TOO_LARGE => ErrorKind::FileTooLarge,
        ERROR_BUSY => ErrorKind::ResourceBusy,
        ERROR_POSSIBLE_DEADLOCK => ErrorKind::Deadlock,
        ERROR_NOT_SAME_DEVICE => ErrorKind::CrossesDevices,
        ERROR_TOO_MANY_LINKS => ErrorKind::TooManyLinks,
        ERROR_TOO_MANY_OPEN_FILES => ErrorKind::TooManyOpenFiles,
        ERROR_CANT_RESOLVE_FILENAME => ErrorKind::FilesystemLoop,
        _ => match errno {
            WSAEACCES => ErrorKind::PermissionDenied,
            WSAEADDRINUSE => ErrorKind::AddrInUse,
            WSAEADDRNOTAVAIL => ErrorKind::AddrNotAvailable,
            WSAECONNABORTED => ErrorKind::ConnectionAborted,
            WSAECONNREFUSED => ErrorKind::ConnectionRefused,
            WSAECONNRESET => ErrorKind::ConnectionReset,
            WSAEINVAL => ErrorKind::InvalidInput,
            WSAENOTCONN => ErrorKind::NotConnected,
            WSAEWOULDBLOCK => ErrorKind::WouldBlock,
            WSAETIMEDOUT => ErrorKind::TimedOut,
            WSAEHOSTUNREACH => ErrorKind::HostUnreachable,
            WSAENETDOWN => ErrorKind::NetworkDown,
            WSAENETUNREACH => ErrorKind::NetworkUnreachable,
            WSAEDQUOT => ErrorKind::QuotaExceeded,
            WSAEMFILE => ErrorKind::TooManyOpenFiles,
            WSAESHUTDOWN => ErrorKind::BrokenPipe,
            _ => ErrorKind::Uncategorized,
        },
    }
}
