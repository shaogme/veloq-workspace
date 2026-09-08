//! Platform-specific operating-system handles and conversion traits.
//!
//! This module provides non-standard library implementations of platform-specific
//! I/O handles, file descriptors, and extension traits that operate without depending
//! on the standard library.

#[cfg(unix)]
pub mod unix;

#[cfg(unix)]
pub use unix::fd;

#[cfg(windows)]
pub mod windows;
