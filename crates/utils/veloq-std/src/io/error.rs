use core::error::Error as StdError;
use core::fmt::{self, Debug, Display, Formatter};
use core::result::Result as CoreResult;

use crate::alloc_crate as alloc;
use crate::io::{
    ErrorKind,
    os_error::{self, RawOsError},
};

#[cfg(feature = "std")]
use std::io::{Error as StdIoError, ErrorKind as StdErrorKind};

use alloc::boxed::Box;

/// A specialized [`Result`] type for I/O operations.
pub type Result<T> = CoreResult<T, Error>;

/// A constant message representation of an I/O error without heap allocation.
#[repr(align(4))]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SimpleMessage {
    /// General category of the error.
    pub kind: ErrorKind,
    /// Detailed static error message description.
    pub message: &'static str,
}

struct Custom {
    kind: ErrorKind,
    error: Box<dyn StdError + Send + Sync>,
}

impl Debug for Custom {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("Custom")
            .field("kind", &self.kind)
            .field("error", &self.error)
            .finish()
    }
}

enum Repr {
    Os(RawOsError),
    Simple(ErrorKind),
    SimpleMessage(&'static SimpleMessage),
    Custom(Box<Custom>),
}

/// The error type for I/O operations in `veloq-std`.
pub struct Error {
    repr: Repr,
}

impl Error {
    /// Constant error for invalid UTF-8 byte sequences.
    pub const INVALID_UTF8: Self = Self::from_static_message(&SimpleMessage {
        kind: ErrorKind::InvalidData,
        message: "stream did not contain valid UTF-8",
    });

    /// Constant error for reaching unexpected EOF when reading exact amount.
    pub const READ_EXACT_EOF: Self = Self::from_static_message(&SimpleMessage {
        kind: ErrorKind::UnexpectedEof,
        message: "failed to fill whole buffer",
    });

    /// Constant error for unexpected EOF.
    pub const UNEXPECTED_EOF: Self = Self::from_static_message(&SimpleMessage {
        kind: ErrorKind::UnexpectedEof,
        message: "unexpected end of file",
    });

    /// Constant error for write returning zero.
    pub const WRITE_ZERO: Self = Self::from_static_message(&SimpleMessage {
        kind: ErrorKind::WriteZero,
        message: "failed to write whole buffer",
    });

    /// Constant error for operation timeouts.
    pub const TIMED_OUT: Self = Self::from_static_message(&SimpleMessage {
        kind: ErrorKind::TimedOut,
        message: "operation timed out",
    });

    /// Creates a new I/O error from a known kind of error and arbitrary error payload.
    pub fn new<E>(kind: ErrorKind, error: E) -> Self
    where
        E: Into<Box<dyn StdError + Send + Sync>>,
    {
        Self {
            repr: Repr::Custom(Box::new(Custom {
                kind,
                error: error.into(),
            })),
        }
    }

    /// Creates a new I/O error from an arbitrary error payload with `ErrorKind::Other`.
    pub fn other<E>(error: E) -> Self
    where
        E: Into<Box<dyn StdError + Send + Sync>>,
    {
        Self::new(ErrorKind::Other, error)
    }

    /// Creates an error representing the last OS error that occurred.
    #[must_use]
    #[inline]
    pub fn last_os_error() -> Self {
        Self::from_raw_os_error(os_error::last_os_error())
    }

    /// Creates an instance of [`Error`] from a particular OS error code.
    #[must_use]
    #[inline]
    pub fn from_raw_os_error(code: RawOsError) -> Self {
        Self {
            repr: Repr::Os(code),
        }
    }

    /// Creates an error from a static [`SimpleMessage`].
    #[must_use]
    #[inline]
    pub const fn from_static_message(msg: &'static SimpleMessage) -> Self {
        Self {
            repr: Repr::SimpleMessage(msg),
        }
    }

    /// Returns the OS error that this error represents (if any).
    #[must_use]
    #[inline]
    pub fn raw_os_error(&self) -> Option<RawOsError> {
        match self.repr {
            Repr::Os(code) => Some(code),
            _ => None,
        }
    }

    /// Returns the corresponding [`ErrorKind`] for this error.
    #[must_use]
    #[inline]
    pub fn kind(&self) -> ErrorKind {
        match &self.repr {
            Repr::Os(code) => os_error::decode_error_kind(*code),
            Repr::Simple(kind) => *kind,
            Repr::SimpleMessage(msg) => msg.kind,
            Repr::Custom(c) => c.kind,
        }
    }

    /// Returns a reference to the inner error wrapped by this error (if any).
    #[must_use]
    #[inline]
    pub fn get_ref(&self) -> Option<&(dyn StdError + Send + Sync + 'static)> {
        match &self.repr {
            Repr::Custom(c) => Some(&*c.error),
            _ => None,
        }
    }

    /// Returns a mutable reference to the inner error wrapped by this error (if any).
    #[must_use]
    #[inline]
    pub fn get_mut(&mut self) -> Option<&mut (dyn StdError + Send + Sync + 'static)> {
        match &mut self.repr {
            Repr::Custom(c) => Some(&mut *c.error),
            _ => None,
        }
    }

    /// Consumes the `Error`, returning its inner error (if any).
    #[must_use = "`self` will be dropped if the result is not used"]
    #[inline]
    pub fn into_inner(self) -> Option<Box<dyn StdError + Send + Sync>> {
        match self.repr {
            Repr::Custom(c) => Some(c.error),
            _ => None,
        }
    }

    /// Attempts to downcast the custom boxed error to `E`.
    #[inline]
    pub fn downcast<E: StdError + 'static>(self) -> CoreResult<Box<E>, Self> {
        match self.repr {
            Repr::Custom(c) => match c.error.downcast::<E>() {
                Ok(downcasted) => Ok(downcasted),
                Err(error) => Err(Self {
                    repr: Repr::Custom(Box::new(Custom {
                        kind: c.kind,
                        error,
                    })),
                }),
            },
            _ => Err(self),
        }
    }

    /// Returns `true` if this error represents an interrupted operation.
    #[must_use]
    #[inline]
    pub fn is_interrupted(&self) -> bool {
        self.kind() == ErrorKind::Interrupted
    }
}

impl From<ErrorKind> for Error {
    #[inline]
    fn from(kind: ErrorKind) -> Self {
        Self {
            repr: Repr::Simple(kind),
        }
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match &self.repr {
            Repr::Os(code) => {
                let detail = os_error::error_string(*code);
                write!(f, "{detail} (os error {code})")
            }
            Repr::Simple(kind) => Display::fmt(kind, f),
            Repr::SimpleMessage(msg) => f.write_str(msg.message),
            Repr::Custom(c) => Display::fmt(&c.error, f),
        }
    }
}

impl Debug for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match &self.repr {
            Repr::Os(code) => f
                .debug_struct("Os")
                .field("code", code)
                .field("kind", &os_error::decode_error_kind(*code))
                .field("message", &os_error::error_string(*code))
                .finish(),
            Repr::Simple(kind) => f.debug_tuple("Kind").field(kind).finish(),
            Repr::SimpleMessage(msg) => f
                .debug_struct("Error")
                .field("kind", &msg.kind)
                .field("message", &msg.message)
                .finish(),
            Repr::Custom(c) => Debug::fmt(c, f),
        }
    }
}

impl StdError for Error {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match &self.repr {
            Repr::Custom(c) => c.error.source(),
            _ => None,
        }
    }
}

#[cfg(feature = "std")]
impl From<Error> for StdIoError {
    fn from(err: Error) -> Self {
        match err.repr {
            Repr::Os(code) => Self::from_raw_os_error(code),
            Repr::Simple(kind) => Self::from(StdErrorKind::from(kind)),
            Repr::SimpleMessage(msg) => Self::new(StdErrorKind::from(msg.kind), msg.message),
            Repr::Custom(c) => Self::new(StdErrorKind::from(c.kind), c.error),
        }
    }
}

#[cfg(feature = "std")]
impl From<StdIoError> for Error {
    fn from(err: StdIoError) -> Self {
        if let Some(code) = err.raw_os_error() {
            Self::from_raw_os_error(code)
        } else {
            let kind = ErrorKind::from(err.kind());
            if let Some(inner) = err.into_inner() {
                Self::new(kind, inner)
            } else {
                Self::from(kind)
            }
        }
    }
}
