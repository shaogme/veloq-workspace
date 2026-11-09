//! Windows-specific error-checking and retry helpers.

use crate::{
    io::{Error, Result},
    os::windows::net::NetApi,
};

pub(crate) struct Cvt;

pub(crate) trait IsZero {
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
fn cvt_inner<I: IsZero>(i: I) -> Result<I> {
    if i.is_zero() {
        Err(Error::last_os_error())
    } else {
        Ok(i)
    }
}

pub(crate) trait IsMinusOne {
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
fn cvt_socket_inner<T: IsMinusOne>(t: T) -> Result<T> {
    if t.is_minus_one() {
        Err(NetApi::last_error())
    } else {
        Ok(t)
    }
}

/// A variant of `cvt` for `getaddrinfo` which returns 0 for success.
#[inline]
fn cvt_gai_inner(err: i32) -> Result<()> {
    if Cvt::is_success(err) {
        Ok(())
    } else {
        Err(NetApi::last_error())
    }
}

impl Cvt {
    fn is_success(error: i32) -> bool {
        error == 0
    }

    #[inline]
    pub(crate) fn cvt<I: IsZero>(value: I) -> Result<I> {
        cvt_inner(value)
    }

    #[inline]
    pub(crate) fn cvt_socket<T: IsMinusOne>(value: T) -> Result<T> {
        cvt_socket_inner(value)
    }

    #[inline]
    pub(crate) fn cvt_gai(error: i32) -> Result<()> {
        cvt_gai_inner(error)
    }
}
