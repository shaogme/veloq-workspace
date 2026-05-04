use std::future::Future;
use std::io;

use veloq_buf::FixedBuf;

pub mod buffer {
    pub use veloq_buf::*;
}

/// Async buffered reading trait.
///
/// Suitable for underlying asynchronous read operations that require passing
/// `FixedBuf` ownership.
pub trait AsyncBufRead {
    fn read(&self, buf: FixedBuf) -> impl Future<Output = io::Result<(usize, FixedBuf)>>;

    fn read_exact(&self, buf: FixedBuf) -> impl Future<Output = io::Result<(usize, FixedBuf)>>;
}

/// Async buffered writing trait.
///
/// Suitable for underlying asynchronous write operations that require passing
/// `FixedBuf` ownership.
pub trait AsyncBufWrite {
    fn write(&self, buf: FixedBuf) -> impl Future<Output = io::Result<(usize, FixedBuf)>>;

    fn write_all(&self, buf: FixedBuf) -> impl Future<Output = io::Result<(usize, FixedBuf)>>;

    fn flush(&self) -> impl Future<Output = io::Result<()>>;

    fn shutdown(&self) -> impl Future<Output = io::Result<()>>;
}
