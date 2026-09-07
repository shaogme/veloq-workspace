use crate::alloc_crate as alloc;
use crate::io::{ErrorKind, os_error::RawOsError};

use alloc::{format, string::String};

#[inline]
pub fn last_os_error() -> RawOsError {
    0
}

pub fn error_string(code: RawOsError) -> String {
    format!("OS error {code}")
}

pub fn decode_error_kind(_code: RawOsError) -> ErrorKind {
    ErrorKind::Uncategorized
}
