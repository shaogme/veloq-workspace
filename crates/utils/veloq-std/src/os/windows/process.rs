//! Windows-specific extensions to primitives in the [`process`](crate::process) module.

use crate::process::{ExitStatus, sys::ExitStatus as SysExitStatus};

/// Windows-specific extensions to [`ExitStatus`].
pub trait ExitStatusExt {
    /// Creates a new `ExitStatus` from the raw underlying `u32` return value of a process.
    fn from_raw(raw: u32) -> Self;

    /// Returns the raw underlying `u32` return value of a process.
    fn into_raw(self) -> u32;
}

impl ExitStatusExt for ExitStatus {
    #[inline]
    fn from_raw(raw: u32) -> Self {
        Self(SysExitStatus(raw))
    }

    #[inline]
    fn into_raw(self) -> u32 {
        self.0.0
    }
}
