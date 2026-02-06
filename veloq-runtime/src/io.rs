use std::future::Future;
use std::io;
use veloq_buf::FixedBuf;

pub mod buffer {
    pub use veloq_buf::*;
}

#[cfg(feature = "compat")]
pub mod compat;
#[cfg(feature = "compat")]
pub use compat::Compat;

/// Async buffered reading trait.
/// Suitable for underlying asynchronous read operations that require passing FixedBuf ownership.
pub trait AsyncBufRead {
    /// Read data into the buffer.
    /// Returns the number of bytes read and the original buffer.
    fn read(&self, buf: FixedBuf) -> impl Future<Output = (io::Result<usize>, FixedBuf)>;
}

/// Async buffered writing trait.
/// Suitable for underlying asynchronous write operations that require passing FixedBuf ownership.
pub trait AsyncBufWrite {
    /// Write data from the buffer.
    /// Returns the number of bytes written and the original buffer.
    fn write(&self, buf: FixedBuf) -> impl Future<Output = (io::Result<usize>, FixedBuf)>;

    /// Flush the buffer (e.g., sync file to disk).
    fn flush(&self) -> impl Future<Output = io::Result<()>>;

    /// Close the writing end (e.g., TCP shutdown).
    fn shutdown(&self) -> impl Future<Output = io::Result<()>>;
}
