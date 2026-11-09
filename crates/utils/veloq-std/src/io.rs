mod cursor;
mod error;
mod impls;
mod kind;
mod os_error;
mod stdio;
mod traits;

pub use cursor::Cursor;
pub use error::{Error, Result, SimpleMessage};
pub use kind::ErrorKind;
pub use os_error::RawOsError;
pub use stdio::{Stderr, StderrLock, Stdin, StdinLock, Stdout, StdoutLock};
pub use traits::{
    Bytes, Chain, Empty, Io, IoSlice, IoSliceMut, Read, Repeat, Seek, SeekFrom, Sink, Take, Write,
};

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
