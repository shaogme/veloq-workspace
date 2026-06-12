use std::{error::Error, future::Future};

use veloq_buf::FixedBuf;

pub mod buffer {
    pub use veloq_buf::*;
}

/// Async buffered reading trait.
///
/// Suitable for underlying asynchronous read operations that require passing
/// `FixedBuf` ownership.
pub trait AsyncBufRead {
    type Error: Error;

    fn read(&self, buf: FixedBuf) -> impl Future<Output = Result<(usize, FixedBuf), Self::Error>>;

    fn read_exact(
        &self,
        buf: FixedBuf,
    ) -> impl Future<Output = Result<(usize, FixedBuf), Self::Error>>;
}

/// Async buffered writing trait.
///
/// Suitable for underlying asynchronous write operations that require passing
/// `FixedBuf` ownership.
pub trait AsyncBufWrite {
    type Error: Error;

    fn write(&self, buf: FixedBuf) -> impl Future<Output = Result<(usize, FixedBuf), Self::Error>>;

    fn write_all(
        &self,
        buf: FixedBuf,
    ) -> impl Future<Output = Result<(usize, FixedBuf), Self::Error>>;

    fn flush(&self) -> impl Future<Output = Result<(), Self::Error>>;

    fn shutdown(&self) -> impl Future<Output = Result<(), Self::Error>>;
}
