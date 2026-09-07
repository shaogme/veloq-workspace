use core::fmt::{self, Display, Formatter};

#[cfg(feature = "std")]
use std::io::ErrorKind as StdErrorKind;

/// A list specifying general categories of I/O error.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[non_exhaustive]
pub enum ErrorKind {
    /// An entity was not found, often a file.
    NotFound,
    /// The operation lacked the necessary privileges to complete.
    PermissionDenied,
    /// The connection was refused by the remote server.
    ConnectionRefused,
    /// The connection was reset by the remote server.
    ConnectionReset,
    /// The remote host is not reachable.
    HostUnreachable,
    /// The network containing the remote host is not reachable.
    NetworkUnreachable,
    /// The connection was aborted (terminated) by the remote server.
    ConnectionAborted,
    /// The network operation failed because it was not connected yet.
    NotConnected,
    /// A socket address could not be bound because the address is already in use elsewhere.
    AddrInUse,
    /// A nonexistent interface was requested or the requested address was not local.
    AddrNotAvailable,
    /// The system's networking is down.
    NetworkDown,
    /// The operation failed because a pipe was closed.
    BrokenPipe,
    /// An entity already exists, often a file.
    AlreadyExists,
    /// The operation needs to block to complete, but the blocking operation was requested to not occur.
    WouldBlock,
    /// A filesystem object is, unexpectedly, not a directory.
    NotADirectory,
    /// The filesystem object is, unexpectedly, a directory.
    IsADirectory,
    /// A non-empty directory was specified where an empty directory was expected.
    DirectoryNotEmpty,
    /// The filesystem or storage medium is read-only, but a write operation was attempted.
    ReadOnlyFilesystem,
    /// Loop in the filesystem or IO subsystem; often, too many levels of symbolic links.
    FilesystemLoop,
    /// Stale network file handle.
    StaleNetworkFileHandle,
    /// A parameter was incorrect.
    InvalidInput,
    /// Data not valid for the operation were encountered.
    InvalidData,
    /// The I/O operation's timeout expired, causing it to be canceled.
    TimedOut,
    /// An error returned when an operation could not be completed because a call to write returned `Ok(0)`.
    WriteZero,
    /// The underlying storage (typically, a filesystem) is full.
    StorageFull,
    /// Seek on unseekable file.
    NotSeekable,
    /// Filesystem quota or some other kind of quota was exceeded.
    QuotaExceeded,
    /// File larger than allowed or supported.
    FileTooLarge,
    /// Resource is busy.
    ResourceBusy,
    /// Executable file is busy.
    ExecutableFileBusy,
    /// Deadlock (avoided).
    Deadlock,
    /// Cross-device or cross-filesystem (hard) link or rename.
    CrossesDevices,
    /// Too many (hard) links to the same filesystem object.
    TooManyLinks,
    /// A filename was invalid.
    InvalidFilename,
    /// Program argument list too long.
    ArgumentListTooLong,
    /// This operation was interrupted.
    Interrupted,
    /// This operation is unsupported on this platform.
    Unsupported,
    /// An error returned when an operation could not be completed because an "end of file" was reached prematurely.
    UnexpectedEof,
    /// An operation could not be completed, because it failed to allocate enough memory.
    OutOfMemory,
    /// The operation was partially successful and needs to be checked later on due to not blocking.
    InProgress,
    /// The process or the whole system has reached its limit on the number of open files or sockets.
    TooManyOpenFiles,
    /// A custom error that does not fall under any other I/O error kind.
    Other,
    /// Any I/O error from the standard library that's not part of this list.
    Uncategorized,
}

impl ErrorKind {
    /// Returns a static string slice describing the error category.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::AddrInUse => "address in use",
            Self::AddrNotAvailable => "address not available",
            Self::AlreadyExists => "entity already exists",
            Self::ArgumentListTooLong => "argument list too long",
            Self::BrokenPipe => "broken pipe",
            Self::ConnectionAborted => "connection aborted",
            Self::ConnectionRefused => "connection refused",
            Self::ConnectionReset => "connection reset",
            Self::CrossesDevices => "cross-device link or rename",
            Self::Deadlock => "deadlock",
            Self::DirectoryNotEmpty => "directory not empty",
            Self::ExecutableFileBusy => "executable file busy",
            Self::FileTooLarge => "file too large",
            Self::FilesystemLoop => "filesystem loop or indirection limit (e.g. symlink loop)",
            Self::HostUnreachable => "host unreachable",
            Self::InProgress => "in progress",
            Self::Interrupted => "operation interrupted",
            Self::InvalidData => "invalid data",
            Self::InvalidFilename => "invalid filename",
            Self::InvalidInput => "invalid input parameter",
            Self::IsADirectory => "is a directory",
            Self::NetworkDown => "network down",
            Self::NetworkUnreachable => "network unreachable",
            Self::NotADirectory => "not a directory",
            Self::NotConnected => "not connected",
            Self::NotFound => "entity not found",
            Self::NotSeekable => "seek on unseekable file",
            Self::Other => "other error",
            Self::OutOfMemory => "out of memory",
            Self::PermissionDenied => "permission denied",
            Self::QuotaExceeded => "quota exceeded",
            Self::ReadOnlyFilesystem => "read-only filesystem or storage medium",
            Self::ResourceBusy => "resource busy",
            Self::StaleNetworkFileHandle => "stale network file handle",
            Self::StorageFull => "no storage space",
            Self::TimedOut => "timed out",
            Self::TooManyLinks => "too many links",
            Self::TooManyOpenFiles => "too many open files",
            Self::Uncategorized => "uncategorized error",
            Self::UnexpectedEof => "unexpected end of file",
            Self::Unsupported => "unsupported",
            Self::WouldBlock => "operation would block",
            Self::WriteZero => "write zero",
        }
    }
}

impl Display for ErrorKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[cfg(feature = "std")]
impl From<ErrorKind> for StdErrorKind {
    fn from(kind: ErrorKind) -> Self {
        match kind {
            ErrorKind::NotFound => Self::NotFound,
            ErrorKind::PermissionDenied => Self::PermissionDenied,
            ErrorKind::ConnectionRefused => Self::ConnectionRefused,
            ErrorKind::ConnectionReset => Self::ConnectionReset,
            ErrorKind::HostUnreachable => Self::HostUnreachable,
            ErrorKind::NetworkUnreachable => Self::NetworkUnreachable,
            ErrorKind::ConnectionAborted => Self::ConnectionAborted,
            ErrorKind::NotConnected => Self::NotConnected,
            ErrorKind::AddrInUse => Self::AddrInUse,
            ErrorKind::AddrNotAvailable => Self::AddrNotAvailable,
            ErrorKind::NetworkDown => Self::NetworkDown,
            ErrorKind::BrokenPipe => Self::BrokenPipe,
            ErrorKind::AlreadyExists => Self::AlreadyExists,
            ErrorKind::WouldBlock => Self::WouldBlock,
            ErrorKind::NotADirectory => Self::NotADirectory,
            ErrorKind::IsADirectory => Self::IsADirectory,
            ErrorKind::DirectoryNotEmpty => Self::DirectoryNotEmpty,
            ErrorKind::ReadOnlyFilesystem => Self::ReadOnlyFilesystem,
            ErrorKind::StaleNetworkFileHandle => Self::StaleNetworkFileHandle,
            ErrorKind::InvalidInput => Self::InvalidInput,
            ErrorKind::InvalidData => Self::InvalidData,
            ErrorKind::TimedOut => Self::TimedOut,
            ErrorKind::WriteZero => Self::WriteZero,
            ErrorKind::StorageFull => Self::StorageFull,
            ErrorKind::NotSeekable => Self::NotSeekable,
            ErrorKind::QuotaExceeded => Self::QuotaExceeded,
            ErrorKind::FileTooLarge => Self::FileTooLarge,
            ErrorKind::ResourceBusy => Self::ResourceBusy,
            ErrorKind::ExecutableFileBusy => Self::ExecutableFileBusy,
            ErrorKind::Deadlock => Self::Deadlock,
            ErrorKind::CrossesDevices => Self::CrossesDevices,
            ErrorKind::TooManyLinks => Self::TooManyLinks,
            ErrorKind::InvalidFilename => Self::InvalidFilename,
            ErrorKind::ArgumentListTooLong => Self::ArgumentListTooLong,
            ErrorKind::Interrupted => Self::Interrupted,
            ErrorKind::Unsupported => Self::Unsupported,
            ErrorKind::UnexpectedEof => Self::UnexpectedEof,
            ErrorKind::OutOfMemory => Self::OutOfMemory,
            ErrorKind::Other => Self::Other,
            ErrorKind::FilesystemLoop
            | ErrorKind::InProgress
            | ErrorKind::TooManyOpenFiles
            | ErrorKind::Uncategorized => Self::Other,
        }
    }
}

#[cfg(feature = "std")]
impl From<StdErrorKind> for ErrorKind {
    fn from(kind: StdErrorKind) -> Self {
        match kind {
            StdErrorKind::NotFound => Self::NotFound,
            StdErrorKind::PermissionDenied => Self::PermissionDenied,
            StdErrorKind::ConnectionRefused => Self::ConnectionRefused,
            StdErrorKind::ConnectionReset => Self::ConnectionReset,
            StdErrorKind::HostUnreachable => Self::HostUnreachable,
            StdErrorKind::NetworkUnreachable => Self::NetworkUnreachable,
            StdErrorKind::ConnectionAborted => Self::ConnectionAborted,
            StdErrorKind::NotConnected => Self::NotConnected,
            StdErrorKind::AddrInUse => Self::AddrInUse,
            StdErrorKind::AddrNotAvailable => Self::AddrNotAvailable,
            StdErrorKind::NetworkDown => Self::NetworkDown,
            StdErrorKind::BrokenPipe => Self::BrokenPipe,
            StdErrorKind::AlreadyExists => Self::AlreadyExists,
            StdErrorKind::WouldBlock => Self::WouldBlock,
            StdErrorKind::NotADirectory => Self::NotADirectory,
            StdErrorKind::IsADirectory => Self::IsADirectory,
            StdErrorKind::DirectoryNotEmpty => Self::DirectoryNotEmpty,
            StdErrorKind::ReadOnlyFilesystem => Self::ReadOnlyFilesystem,
            StdErrorKind::StaleNetworkFileHandle => Self::StaleNetworkFileHandle,
            StdErrorKind::InvalidInput => Self::InvalidInput,
            StdErrorKind::InvalidData => Self::InvalidData,
            StdErrorKind::TimedOut => Self::TimedOut,
            StdErrorKind::WriteZero => Self::WriteZero,
            StdErrorKind::StorageFull => Self::StorageFull,
            StdErrorKind::NotSeekable => Self::NotSeekable,
            StdErrorKind::QuotaExceeded => Self::QuotaExceeded,
            StdErrorKind::FileTooLarge => Self::FileTooLarge,
            StdErrorKind::ResourceBusy => Self::ResourceBusy,
            StdErrorKind::ExecutableFileBusy => Self::ExecutableFileBusy,
            StdErrorKind::Deadlock => Self::Deadlock,
            StdErrorKind::CrossesDevices => Self::CrossesDevices,
            StdErrorKind::TooManyLinks => Self::TooManyLinks,
            StdErrorKind::InvalidFilename => Self::InvalidFilename,
            StdErrorKind::ArgumentListTooLong => Self::ArgumentListTooLong,
            StdErrorKind::Interrupted => Self::Interrupted,
            StdErrorKind::Unsupported => Self::Unsupported,
            StdErrorKind::UnexpectedEof => Self::UnexpectedEof,
            StdErrorKind::OutOfMemory => Self::OutOfMemory,
            StdErrorKind::Other => Self::Other,
            _ => Self::Uncategorized,
        }
    }
}
