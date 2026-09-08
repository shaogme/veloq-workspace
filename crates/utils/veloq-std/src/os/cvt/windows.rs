//! Windows-specific error-checking and retry helpers.

use crate::{
    io::{Error, Result},
    os::windows::net,
};

pub trait IsZero {
    fn is_zero(&self) -> bool;
}

macro_rules! impl_is_zero {
    ($($t:ident)*) => ($(impl IsZero for $t {
        #[inline]
        fn is_zero(&self) -> bool {
            *self == 0
        }
    })*)
}

impl_is_zero! { i8 i16 i32 i64 isize u8 u16 u32 u64 usize }

/// Win32 API helper: checks if the return value is zero (FALSE) and returns `last_os_error()`.
#[inline]
pub fn cvt<I: IsZero>(i: I) -> Result<I> {
    if i.is_zero() {
        Err(Error::last_os_error())
    } else {
        Ok(i)
    }
}

/// Win32 API helper: checks if the return value is zero (success) and returns error on non-zero.
#[inline]
pub fn cvt_nz<I: IsZero>(i: I) -> Result<()> {
    if i.is_zero() {
        Ok(())
    } else {
        Err(Error::last_os_error())
    }
}

pub trait IsMinusOne {
    fn is_minus_one(&self) -> bool;
}

macro_rules! impl_is_minus_one {
    ($($t:ident)*) => ($(impl IsMinusOne for $t {
        #[inline]
        fn is_minus_one(&self) -> bool {
            *self == -1
        }
    })*)
}

impl_is_minus_one! { i8 i16 i32 i64 isize }

/// Checks if the signed integer is the Windows constant `SOCKET_ERROR` (-1)
/// and if so, returns the last error from the Windows socket interface.
#[inline]
pub fn cvt_socket<T: IsMinusOne>(t: T) -> Result<T> {
    if t.is_minus_one() {
        Err(net::last_error())
    } else {
        Ok(t)
    }
}

/// A variant of `cvt` for `getaddrinfo` which returns 0 for success.
#[inline]
pub fn cvt_gai(err: i32) -> Result<()> {
    if err == 0 {
        Ok(())
    } else {
        Err(net::last_error())
    }
}

/// Executes a closure and checks the return value for `SOCKET_ERROR`.
#[inline]
pub fn cvt_r<T: IsMinusOne, F: FnMut() -> T>(mut f: F) -> Result<T> {
    cvt_socket(f())
}
