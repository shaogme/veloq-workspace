use crate::{
    ffi::OsStr,
    path::{Path, Prefix},
};

pub const MAIN_SEPARATOR: char = '\\';

pub const MAIN_SEPARATOR_STR: &str = "\\";

pub(crate) struct Platform;

struct PrefixParserSlice<'a, 'b> {
    path: &'a OsStr,
    prefix: &'b [u8],
    index: usize,
}

struct PrefixParser<'a, const LEN: usize> {
    path: &'a OsStr,
    prefix: [u8; LEN],
}

impl<'a, const LEN: usize> PrefixParser<'a, LEN> {
    #[inline]
    fn get_prefix(path: &OsStr) -> [u8; LEN] {
        let mut prefix = [0; LEN];
        for (i, &ch) in path.as_encoded_bytes().iter().take(LEN).enumerate() {
            prefix[i] = if ch == b'/' { b'\\' } else { ch };
        }
        prefix
    }

    fn new(path: &'a OsStr) -> Self {
        Self {
            path,
            prefix: Self::get_prefix(path),
        }
    }

    fn as_slice(&self) -> PrefixParserSlice<'a, '_> {
        PrefixParserSlice {
            path: self.path,
            prefix: &self.prefix[..LEN.min(self.path.len())],
            index: 0,
        }
    }
}

impl<'a> PrefixParserSlice<'a, '_> {
    fn strip_prefix(&self, prefix: &str) -> Option<Self> {
        self.prefix[self.index..]
            .starts_with(prefix.as_bytes())
            .then_some(Self {
                index: self.index + prefix.len(),
                ..*self
            })
    }

    fn prefix_bytes(&self) -> &'a [u8] {
        &self.path.as_encoded_bytes()[..self.index]
    }

    fn finish(self) -> &'a OsStr {
        unsafe { OsStr::from_encoded_bytes_unchecked(&self.path.as_encoded_bytes()[self.index..]) }
    }
}

impl Platform {
    pub(super) const HAS_PREFIXES: bool = true;

    #[inline]
    pub(crate) const fn is_sep_byte(byte: u8) -> bool {
        byte == b'\\' || byte == b'/'
    }

    #[inline]
    pub(crate) const fn is_verbatim_sep(byte: u8) -> bool {
        byte == b'\\'
    }

    #[inline]
    pub(crate) fn is_absolute(path: &Path) -> bool {
        path.has_root() && path.prefix().is_some()
    }

    pub(crate) fn parse_prefix(path: &OsStr) -> Option<Prefix<'_>> {
        use Prefix::{DeviceNS, Disk, UNC, Verbatim, VerbatimDisk, VerbatimUNC};

        let parser = PrefixParser::<8>::new(path);
        let parser = parser.as_slice();
        if let Some(parser) = parser.strip_prefix(r"\\") {
            if let Some(parser) = parser.strip_prefix(r"?\")
                && !parser.prefix_bytes().contains(&b'/')
            {
                if let Some(parser) = parser.strip_prefix(r"UNC\") {
                    let path = parser.finish();
                    let (server, path) = Self::parse_next_component(path, true);
                    let (share, _) = Self::parse_next_component(path, true);

                    Some(VerbatimUNC(server, share))
                } else {
                    let path = parser.finish();

                    if let Some(drive) = Self::parse_drive_exact(path) {
                        Some(VerbatimDisk(drive))
                    } else {
                        let (prefix, _) = Self::parse_next_component(path, true);
                        Some(Verbatim(prefix))
                    }
                }
            } else if let Some(parser) = parser.strip_prefix(r".\") {
                let path = parser.finish();
                let (prefix, _) = Self::parse_next_component(path, false);
                Some(DeviceNS(prefix))
            } else {
                let path = parser.finish();
                let (server, path) = Self::parse_next_component(path, false);
                let (share, _) = Self::parse_next_component(path, false);

                if !server.is_empty() && !share.is_empty() {
                    Some(UNC(server, share))
                } else {
                    None
                }
            }
        } else {
            Some(Disk(Self::parse_drive(path)?))
        }
    }

    fn parse_drive(path: &OsStr) -> Option<u8> {
        fn is_valid_drive_letter(drive: &u8) -> bool {
            drive.is_ascii_alphabetic()
        }

        match path.as_encoded_bytes() {
            [drive, b':', ..] if is_valid_drive_letter(drive) => Some(drive.to_ascii_uppercase()),
            _ => None,
        }
    }

    fn parse_drive_exact(path: &OsStr) -> Option<u8> {
        if path
            .as_encoded_bytes()
            .get(2)
            .map(|&x| Self::is_sep_byte(x))
            .unwrap_or(true)
        {
            Self::parse_drive(path)
        } else {
            None
        }
    }

    fn parse_next_component(path: &OsStr, verbatim: bool) -> (&OsStr, &OsStr) {
        let separator = if verbatim {
            Self::is_verbatim_sep
        } else {
            Self::is_sep_byte
        };

        match path.as_encoded_bytes().iter().position(|&x| separator(x)) {
            Some(separator_start) => {
                let separator_end = separator_start + 1;
                let component = &path.as_encoded_bytes()[..separator_start];
                let path = &path.as_encoded_bytes()[separator_end..];
                unsafe {
                    (
                        OsStr::from_encoded_bytes_unchecked(component),
                        OsStr::from_encoded_bytes_unchecked(path),
                    )
                }
            }
            None => (path, OsStr::new("")),
        }
    }
}
