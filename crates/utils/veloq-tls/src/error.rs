use alloc::boxed::Box;
use core::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TlsErrorKind {
    AllocationFailed,
    SetFailed(i32),
    Uninitialized,
    RecursiveAccess,
}

impl fmt::Display for TlsErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TlsErrorKind::AllocationFailed => write!(f, "Failed to allocate TLS index"),
            TlsErrorKind::SetFailed(code) => {
                write!(f, "Failed to set TLS value: error code {}", code)
            }
            TlsErrorKind::Uninitialized => {
                write!(f, "TLS value is uninitialized or being initialized")
            }
            TlsErrorKind::RecursiveAccess => write!(
                f,
                "TLS recursive access or modification during replacement detected"
            ),
        }
    }
}

impl core::error::Error for TlsErrorKind {}

pub enum TlsError<T> {
    AllocationFailed { val: Box<T> },
    SetFailed { code: i32, val: Box<T> },
    Uninitialized { val: Box<T> },
    RecursiveAccess { val: Box<T> },
}

impl<T> fmt::Debug for TlsError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TlsError::AllocationFailed { .. } => f.debug_struct("AllocationFailed").finish(),
            TlsError::SetFailed { code, .. } => {
                f.debug_struct("SetFailed").field("code", code).finish()
            }
            TlsError::Uninitialized { .. } => f.debug_struct("Uninitialized").finish(),
            TlsError::RecursiveAccess { .. } => f.debug_struct("RecursiveAccess").finish(),
        }
    }
}

impl<T> TlsError<T> {
    pub fn kind(&self) -> TlsErrorKind {
        match self {
            TlsError::AllocationFailed { .. } => TlsErrorKind::AllocationFailed,
            TlsError::SetFailed { code, .. } => TlsErrorKind::SetFailed(*code),
            TlsError::Uninitialized { .. } => TlsErrorKind::Uninitialized,
            TlsError::RecursiveAccess { .. } => TlsErrorKind::RecursiveAccess,
        }
    }

    pub fn into_val(self) -> Box<T> {
        match self {
            TlsError::AllocationFailed { val } => val,
            TlsError::SetFailed { val, .. } => val,
            TlsError::Uninitialized { val } => val,
            TlsError::RecursiveAccess { val } => val,
        }
    }
}

impl<T> fmt::Display for TlsError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.kind())
    }
}

impl<T> core::error::Error for TlsError<T> {}

impl<T> PartialEq for TlsError<T> {
    fn eq(&self, other: &Self) -> bool {
        self.kind() == other.kind()
    }
}

impl<T> Eq for TlsError<T> {}
