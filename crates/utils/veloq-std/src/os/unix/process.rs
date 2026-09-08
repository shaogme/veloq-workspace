//! Unix-specific extensions to primitives in the [`process`](crate::process) module.

use crate::process::{ExitStatus, sys::ExitStatus as SysExitStatus};

/// Unix-specific extensions to [`ExitStatus`].
pub trait ExitStatusExt {
    /// Creates a new `ExitStatus` from the raw underlying integer status value from `wait`.
    fn from_raw(raw: i32) -> Self;

    /// If the process was terminated by a signal, returns that signal.
    fn signal(&self) -> Option<i32>;

    /// Whether the process was terminated by a signal and dumped core.
    fn core_dumped(&self) -> bool;

    /// If the process was stopped by a signal, returns that signal.
    fn stopped_signal(&self) -> Option<i32>;

    /// Whether the process was continued from a job control stop.
    fn continued(&self) -> bool;

    /// Returns the underlying raw integer status value.
    fn into_raw(self) -> i32;
}

impl ExitStatusExt for ExitStatus {
    #[inline]
    fn from_raw(raw: i32) -> Self {
        Self(SysExitStatus(raw as _))
    }

    #[inline]
    fn signal(&self) -> Option<i32> {
        let raw = self.0.0;
        if libc::WIFSIGNALED(raw) {
            Some(libc::WTERMSIG(raw))
        } else {
            None
        }
    }

    #[inline]
    fn core_dumped(&self) -> bool {
        let raw = self.0.0;
        libc::WIFSIGNALED(raw) && libc::WCOREDUMP(raw)
    }

    #[inline]
    fn stopped_signal(&self) -> Option<i32> {
        let raw = self.0.0;
        if libc::WIFSTOPPED(raw) {
            Some(libc::WSTOPSIG(raw))
        } else {
            None
        }
    }

    #[inline]
    fn continued(&self) -> bool {
        let raw = self.0.0;
        libc::WIFCONTINUED(raw)
    }

    #[inline]
    fn into_raw(self) -> i32 {
        self.0.0
    }
}
