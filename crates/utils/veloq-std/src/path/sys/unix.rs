use crate::{
    ffi::OsStr,
    path::{Path, Prefix},
};

pub const MAIN_SEPARATOR: char = '/';
pub const MAIN_SEPARATOR_STR: &str = "/";

#[inline]
pub const fn is_sep_byte(b: u8) -> bool {
    b == b'/'
}

#[inline]
pub const fn is_verbatim_sep(b: u8) -> bool {
    b == b'/'
}

#[inline]
pub fn parse_prefix(_: &OsStr) -> Option<Prefix<'_>> {
    None
}

pub const HAS_PREFIXES: bool = false;

#[inline]
pub fn is_absolute(path: &Path) -> bool {
    path.has_root()
}
