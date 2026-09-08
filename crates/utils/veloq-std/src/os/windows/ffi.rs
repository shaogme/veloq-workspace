//! Windows-specific extensions to primitives in the [`ffi`](crate::ffi) module.

use crate::ffi::{
    OsStr, OsString,
    os_str::{Buf, wtf8::Wtf8Buf},
};

pub use crate::ffi::os_str::wtf8::EncodeWide;

/// Windows-specific extensions to [`OsString`].
pub trait OsStringExt {
    /// Creates an `OsString` from a potentially ill-formed UTF-16 slice of
    /// 16-bit code units.
    fn from_wide(wide: &[u16]) -> Self;
}

impl OsStringExt for OsString {
    #[inline]
    fn from_wide(wide: &[u16]) -> OsString {
        OsString {
            inner: Buf {
                inner: Wtf8Buf::from_wide(wide),
            },
        }
    }
}

/// Windows-specific extensions to [`OsStr`].
pub trait OsStrExt {
    /// Re-encodes an `OsStr` as a wide character sequence, i.e., potentially
    /// ill-formed UTF-16.
    fn encode_wide(&self) -> EncodeWide<'_>;
}

impl OsStrExt for OsStr {
    #[inline]
    fn encode_wide(&self) -> EncodeWide<'_> {
        self.inner.inner.encode_wide()
    }
}
