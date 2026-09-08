//! Unix-specific extensions to primitives in the [`ffi`](crate::ffi) module.

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
