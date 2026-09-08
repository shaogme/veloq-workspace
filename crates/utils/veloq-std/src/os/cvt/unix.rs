//! Unix-specific error-checking and retry helpers.

use crate::io::{Error, Result};

pub trait IsMinusOne {
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
pub fn cvt<T: IsMinusOne>(t: T) -> Result<T> {
    if t.is_minus_one() {
        Err(Error::last_os_error())
    } else {
        Ok(t)
    }
}

/// Executes a closure that returns a `Result<T>` in a loop,
/// retrying if it fails with `ErrorKind::Interrupted` (`EINTR`).
pub fn cvt_r<T, F: FnMut() -> Result<T>>(mut f: F) -> Result<T> {
    loop {
        match f() {
            Err(ref e) if e.is_interrupted() => {}
            other => return other,
        }
    }
}

/// A variant of `cvt` for functions that return 0 on success and a positive error number on failure
/// (such as POSIX thread functions).
#[inline]
pub fn cvt_nz(error: libc::c_int) -> Result<()> {
    if error == 0 {
        Ok(())
    } else {
        Err(Error::from_raw_os_error(error))
    }
}

/// A variant of `cvt` for `getaddrinfo` which returns 0 on success.
#[inline]
pub fn cvt_gai(err: libc::c_int) -> Result<()> {
    if err == 0 {
        Ok(())
    } else {
        Err(Error::from_raw_os_error(err))
    }
}
