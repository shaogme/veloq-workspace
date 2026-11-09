//! Unix-specific operating system primitives and I/O safety types.

use core::ffi::c_int;

use crate::io::{Error, Result};

pub(crate) struct Cvt;

pub(crate) trait IsMinusOne {
    fn is_minus_one(&self) -> bool;
}

macro_rules! impl_is_minus_one {
    ($($t:ident)*) => ($(impl IsMinusOne for $t {
        #[inline]
        fn is_minus_one(&self) -> bool {
            *self < 0
        }
    })*)
}

impl_is_minus_one! { i8 i16 i32 i64 isize }

/// Checks if the signed integer is negative (< 0, commonly -1 on Unix)
/// and if so, returns the last OS error (`errno`).
#[inline]
fn cvt_inner<T: IsMinusOne>(t: T) -> Result<T> {
    if t.is_minus_one() {
        Err(Error::last_os_error())
    } else {
        Ok(t)
    }
}

/// Executes a closure that returns a `Result<T>` in a loop,
/// retrying if it fails with `ErrorKind::Interrupted` (`EINTR`).
fn cvt_r_inner<T, F: FnMut() -> Result<T>>(mut f: F) -> Result<T> {
    loop {
        match f() {
            Err(ref e) if e.is_interrupted() => {}
            other => return other,
        }
    }
}

/// A variant of `cvt` for `getaddrinfo` which returns 0 on success.
#[inline]
fn cvt_gai_inner(err: c_int) -> Result<()> {
    if Cvt::is_success(err) {
        Ok(())
    } else {
        Err(Error::from_raw_os_error(err))
    }
}

impl Cvt {
    fn is_success(error: i32) -> bool {
        error == 0
    }

    #[inline]
    pub(crate) fn cvt<T: IsMinusOne>(value: T) -> Result<T> {
        cvt_inner(value)
    }

    pub(crate) fn cvt_r<T, F: FnMut() -> Result<T>>(f: F) -> Result<T> {
        cvt_r_inner(f)
    }

    #[inline]
    pub(crate) fn cvt_gai(error: c_int) -> Result<()> {
        cvt_gai_inner(error)
    }
}

mod ffi_impl {
    use crate::{
        alloc_crate::vec::Vec,
        ffi::{OsStr, OsString},
    };

    /// Platform-specific extensions to [`OsString`].
    pub trait OsStringExt {
        /// Creates an [`OsString`] from a byte vector.
        fn from_vec(vec: Vec<u8>) -> Self;

        /// Yields the underlying byte vector of this [`OsString`].
        fn into_vec(self) -> Vec<u8>;
    }

    impl OsStringExt for OsString {
        #[inline]
        fn from_vec(vec: Vec<u8>) -> OsString {
            unsafe { OsString::from_encoded_bytes_unchecked(vec) }
        }

        #[inline]
        fn into_vec(self) -> Vec<u8> {
            self.into_encoded_bytes()
        }
    }

    /// Platform-specific extensions to [`OsStr`].
    pub trait OsStrExt {
        /// Creates an [`OsStr`] from a byte slice.
        fn from_bytes(slice: &[u8]) -> &Self;

        /// Gets the underlying byte view of the [`OsStr`] slice.
        fn as_bytes(&self) -> &[u8];
    }

    impl OsStrExt for OsStr {
        #[inline]
        fn from_bytes(slice: &[u8]) -> &OsStr {
            unsafe { OsStr::from_encoded_bytes_unchecked(slice) }
        }

        #[inline]
        fn as_bytes(&self) -> &[u8] {
            self.as_encoded_bytes()
        }
    }
}

mod process_impl {
    use crate::process::{ExitStatus, sys::ExitStatus as SysExitStatus};
    use libc::{WCOREDUMP, WIFCONTINUED, WIFSIGNALED, WIFSTOPPED, WSTOPSIG, WTERMSIG};

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
            ExitStatus::from_raw(SysExitStatus::from_raw(raw as _))
        }

        #[inline]
        fn signal(&self) -> Option<i32> {
            let raw = self.raw().raw();
            if WIFSIGNALED(raw) {
                Some(WTERMSIG(raw))
            } else {
                None
            }
        }

        #[inline]
        fn core_dumped(&self) -> bool {
            let raw = self.raw().raw();
            WIFSIGNALED(raw) && WCOREDUMP(raw)
        }

        #[inline]
        fn stopped_signal(&self) -> Option<i32> {
            let raw = self.raw().raw();
            if WIFSTOPPED(raw) {
                Some(WSTOPSIG(raw))
            } else {
                None
            }
        }

        #[inline]
        fn continued(&self) -> bool {
            let raw = self.raw().raw();
            WIFCONTINUED(raw)
        }

        #[inline]
        fn into_raw(self) -> i32 {
            self.raw().raw()
        }
    }
}

pub mod fs {
    mod impls;

    pub use impls::*;
}

pub mod ffi {
    pub use super::ffi_impl::{OsStrExt, OsStringExt};
}

pub mod process {
    pub use super::process_impl::ExitStatusExt;
}

pub mod raw {
    pub use core::ffi::*;
}
