use crate::alloc_crate as alloc;
use crate::io::{ErrorKind, os_error::RawOsError};

use alloc::{format, string::String};

#[inline]
fn last_os_error() -> RawOsError {
    0
}

fn error_string(code: RawOsError) -> String {
    format!("OS error {code}")
}

fn decode_error_kind(_code: RawOsError) -> ErrorKind {
    ErrorKind::Uncategorized
}

pub(crate) struct Platform;

impl Platform {
    pub(crate) fn last_os_error() -> RawOsError {
        last_os_error()
    }

    pub(crate) fn error_string(code: RawOsError) -> String {
        error_string(code)
    }

    pub(crate) fn decode_error_kind(code: RawOsError) -> ErrorKind {
        decode_error_kind(code)
    }
}
