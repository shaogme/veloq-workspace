//! Platform-specific operating-system handles and conversion traits.
//!
//! This module provides non-standard library implementations of platform-specific
//! I/O handles, file descriptors, and extension traits that operate without depending
//! on the standard library.

#[cfg(windows)]
mod cvt;

#[cfg(unix)]
pub mod unix;

#[cfg(windows)]
pub mod windows;

#[cfg(windows)]
pub(crate) use cvt::Cvt;
#[cfg(unix)]
pub(crate) use unix::Cvt;

#[cfg(unix)]
pub use unix::fd;
