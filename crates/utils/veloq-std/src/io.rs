pub mod error;
pub mod kind;
pub mod os_error;

pub use error::{Error, Result, SimpleMessage};
pub use kind::ErrorKind;
pub use os_error::RawOsError;

#[cfg(feature = "std")]
pub use std::io::{Read, Seek, Write};

/// Creates a new I/O error from a known kind of error and a constant string literal.
///
/// This macro does not allocate heap memory and can be used in `const` contexts.
#[macro_export]
macro_rules! const_io_error {
    ($kind:expr, $message:expr $(,)?) => {
        $crate::io::Error::from_static_message(
            const {
                &$crate::io::SimpleMessage {
                    kind: $kind,
                    message: $message,
                }
            },
        )
    };
}
