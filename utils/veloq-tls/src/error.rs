use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TlsError {
    AllocationFailed,
    SetFailed(i32),
}

impl fmt::Display for TlsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TlsError::AllocationFailed => write!(f, "Failed to allocate TLS index"),
            TlsError::SetFailed(code) => write!(f, "Failed to set TLS value: error code {}", code),
        }
    }
}

impl std::error::Error for TlsError {}
