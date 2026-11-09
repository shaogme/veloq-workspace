//! Path components and platform-specific path parsing.

#[cfg(windows)]
mod windows;

#[cfg(windows)]
pub(super) use self::windows::Platform;

#[cfg(windows)]
pub use self::windows::{MAIN_SEPARATOR, MAIN_SEPARATOR_STR};

use crate::{ffi::OsStr, path::Path};

use core::{
    cmp, fmt,
    hash::{Hash, Hasher},
    iter::FusedIterator,
};

/// Windows path prefixes, e.g., `C:` or `\\server\share`.
#[derive(Copy, Clone, Debug, Hash, PartialOrd, Ord, PartialEq, Eq)]
pub enum Prefix<'a> {
    /// Verbatim prefix, e.g., `\\?\cat_pics`.
    Verbatim(&'a OsStr),
    /// Verbatim prefix using Windows' Uniform Naming Convention, e.g., `\\?\UNC\server\share`.
    VerbatimUNC(&'a OsStr, &'a OsStr),
    /// Verbatim disk prefix, e.g., `\\?\C:`.
    VerbatimDisk(u8),
    /// Device namespace prefix, e.g., `\\.\COM42`.
    DeviceNS(&'a OsStr),
    /// Prefix using Windows' Uniform Naming Convention, e.g., `\\server\share`.
    UNC(&'a OsStr, &'a OsStr),
    /// Prefix `C:` for the given disk drive.
    Disk(u8),
}

impl<'a> Prefix<'a> {
    #[inline]
    pub(super) fn len(&self) -> usize {
        match *self {
            Prefix::Verbatim(x) => 4 + x.len(),
            Prefix::VerbatimUNC(x, y) => 8 + x.len() + if !y.is_empty() { 1 + y.len() } else { 0 },
            Prefix::VerbatimDisk(_) => 6,
            Prefix::UNC(x, y) => 2 + x.len() + if !y.is_empty() { 1 + y.len() } else { 0 },
            Prefix::DeviceNS(x) => 4 + x.len(),
            Prefix::Disk(_) => 2,
        }
    }

    /// Determines if the prefix is verbatim, i.e., begins with `\\?\`.
    #[inline]
    pub fn is_verbatim(&self) -> bool {
        matches!(
            *self,
            Prefix::Verbatim(_) | Prefix::VerbatimDisk(_) | Prefix::VerbatimUNC(..)
        )
    }

    #[inline]
    pub(super) fn is_drive(self) -> bool {
        matches!(self, Prefix::Disk(_))
    }

    #[inline]
    fn has_implicit_root(&self) -> bool {
        !self.is_drive()
    }
}

#[cfg(not(windows))]
pub const MAIN_SEPARATOR: char = '/';

#[cfg(not(windows))]
pub const MAIN_SEPARATOR_STR: &str = "/";

#[cfg(not(windows))]
pub(super) struct Platform;

#[cfg(not(windows))]
impl Platform {
    pub(super) const HAS_PREFIXES: bool = false;

    #[inline]
    pub(super) const fn is_sep_byte(byte: u8) -> bool {
        byte == b'/'
    }

    #[inline]
    pub(super) const fn is_verbatim_sep(byte: u8) -> bool {
        byte == b'/'
    }

    #[inline]
    pub(super) fn parse_prefix(_: &OsStr) -> Option<Prefix<'_>> {
        None
    }

    #[inline]
    pub(super) fn is_absolute(path: &Path) -> bool {
        path.has_root()
    }
}

/// A structure wrapping a Windows path prefix as well as its unparsed string representation.
#[derive(Copy, Clone, Eq, Debug)]
pub struct PrefixComponent<'a> {
    raw: &'a OsStr,
    parsed: Prefix<'a>,
}

impl<'a> PrefixComponent<'a> {
    /// Returns the parsed prefix data.
    #[inline]
    pub fn kind(&self) -> Prefix<'a> {
        self.parsed
    }

    /// Returns the raw [`OsStr`] slice for this prefix.
    #[inline]
    pub fn as_os_str(&self) -> &'a OsStr {
        self.raw
    }
}

impl<'a> PartialEq for PrefixComponent<'a> {
    #[inline]
    fn eq(&self, other: &PrefixComponent<'a>) -> bool {
        self.parsed == other.parsed
    }
}

impl<'a> PartialOrd for PrefixComponent<'a> {
    #[inline]
    fn partial_cmp(&self, other: &PrefixComponent<'a>) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for PrefixComponent<'_> {
    #[inline]
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        Ord::cmp(&self.parsed, &other.parsed)
    }
}

impl Hash for PrefixComponent<'_> {
    fn hash<H: Hasher>(&self, h: &mut H) {
        self.parsed.hash(h);
    }
}

/// A single component of a path.
#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub enum Component<'a> {
    /// A Windows path prefix, e.g., `C:` or `\\server\share`.
    Prefix(PrefixComponent<'a>),
    /// The root directory component.
    RootDir,
    /// A reference to the current directory, i.e., `.`.
    CurDir,
    /// A reference to the parent directory, i.e., `..`.
    ParentDir,
    /// A normal component, e.g., `a` and `b` in `a/b`.
    Normal(&'a OsStr),
}

impl<'a> Component<'a> {
    /// Extracts the underlying [`OsStr`] slice.
    pub fn as_os_str(self) -> &'a OsStr {
        match self {
            Component::Prefix(p) => p.as_os_str(),
            Component::RootDir => OsStr::new(MAIN_SEPARATOR_STR),
            Component::CurDir => OsStr::new("."),
            Component::ParentDir => OsStr::new(".."),
            Component::Normal(path) => path,
        }
    }
}

impl AsRef<OsStr> for Component<'_> {
    #[inline]
    fn as_ref(&self) -> &OsStr {
        self.as_os_str()
    }
}

impl AsRef<Path> for Component<'_> {
    #[inline]
    fn as_ref(&self) -> &Path {
        Path::new(self.as_os_str())
    }
}

fn has_physical_root(s: &[u8], prefix: Option<Prefix<'_>>) -> bool {
    let path = if let Some(p) = prefix {
        &s[p.len()..]
    } else {
        s
    };
    !path.is_empty() && Platform::is_sep_byte(path[0])
}

#[derive(Copy, Clone, PartialEq, PartialOrd, Debug)]
enum State {
    Prefix = 0,
    StartDir = 1,
    Body = 2,
    Done = 3,
}

/// An iterator over the [`Component`]s of a [`Path`].
#[derive(Clone)]
pub struct Components<'a> {
    path: &'a [u8],
    prefix: Option<Prefix<'a>>,
    has_physical_root: bool,
    front: State,
    back: State,
}

/// An iterator over the [`Component`]s of a [`Path`], as [`OsStr`] slices.
#[derive(Clone)]
pub struct Iter<'a> {
    inner: Components<'a>,
}

impl<'a> Components<'a> {
    pub(super) fn iter_after<'left, 'right, I, J>(mut iter: I, mut prefix: J) -> Option<I>
    where
        I: Iterator<Item = Component<'left>> + Clone,
        J: Iterator<Item = Component<'right>>,
    {
        loop {
            let mut iter_next = iter.clone();
            match (iter_next.next(), prefix.next()) {
                (Some(ref x), Some(ref y)) if x == y => (),
                (Some(_), Some(_)) => return None,
                (Some(_), None) => return Some(iter),
                (None, None) => return Some(iter),
                (None, Some(_)) => return None,
            }
            iter = iter_next;
        }
    }

    pub(super) fn rsplit_file_at_dot(file: &OsStr) -> (Option<&OsStr>, Option<&OsStr>) {
        if file.as_encoded_bytes() == b".." {
            return (Some(file), None);
        }
        let mut iter = file.as_encoded_bytes().rsplitn(2, |b| *b == b'.');
        let after = iter.next();
        let before = iter.next();
        if before == Some(b"") {
            (Some(file), None)
        } else {
            unsafe {
                (
                    before.map(|s| OsStr::from_encoded_bytes_unchecked(s)),
                    after.map(|s| OsStr::from_encoded_bytes_unchecked(s)),
                )
            }
        }
    }

    pub(super) fn split_file_at_dot(file: &OsStr) -> (&OsStr, Option<&OsStr>) {
        let slice = file.as_encoded_bytes();
        if slice == b".." {
            return (file, None);
        }
        let i = match slice[1..].iter().position(|b| *b == b'.') {
            Some(i) => i + 1,
            None => return (file, None),
        };
        let before = &slice[..i];
        let after = &slice[i + 1..];
        unsafe {
            (
                OsStr::from_encoded_bytes_unchecked(before),
                Some(OsStr::from_encoded_bytes_unchecked(after)),
            )
        }
    }

    pub(super) fn validate_extension(extension: &OsStr) {
        for &b in extension.as_encoded_bytes() {
            if Platform::is_sep_byte(b) {
                panic!("extension cannot contain path separators: {extension:?}");
            }
        }
    }

    pub(super) fn compare(mut left: Components<'_>, mut right: Components<'_>) -> cmp::Ordering {
        if left.prefix.is_none() && right.prefix.is_none() && left.front == right.front {
            let first_difference =
                match left.path.iter().zip(right.path).position(|(&a, &b)| a != b) {
                    None if left.path.len() == right.path.len() => return cmp::Ordering::Equal,
                    None => left.path.len().min(right.path.len()),
                    Some(diff) => diff,
                };

            if let Some(previous_sep) = left.path[..first_difference]
                .iter()
                .rposition(|&b| left.is_sep_byte(b))
            {
                let mismatched_component_start = previous_sep + 1;
                left.path = &left.path[mismatched_component_start..];
                left.front = State::Body;
                right.path = &right.path[mismatched_component_start..];
                right.front = State::Body;
            }
        }

        Iterator::cmp(left, right)
    }

    pub(super) fn new(path: &'a Path) -> Self {
        let bytes = path.as_u8_slice();
        let prefix = Platform::parse_prefix(path.as_os_str());

        Self {
            path: bytes,
            prefix,
            has_physical_root: has_physical_root(bytes, prefix),
            front: if Platform::HAS_PREFIXES {
                State::Prefix
            } else {
                State::StartDir
            },
            back: State::Body,
        }
    }

    pub(super) fn prefix(&self) -> Option<Prefix<'a>> {
        self.prefix
    }

    pub(super) fn is_bare_drive_prefix(&self) -> bool {
        self.prefix_len() > 0
            && self.prefix_len() == self.path.len()
            && self.prefix.map(Prefix::is_drive).unwrap_or(false)
    }

    #[inline]
    pub(super) fn prefix_len(&self) -> usize {
        if !Platform::HAS_PREFIXES {
            return 0;
        }
        self.prefix.as_ref().map(Prefix::len).unwrap_or(0)
    }

    #[inline]
    pub(super) fn prefix_verbatim(&self) -> bool {
        if !Platform::HAS_PREFIXES {
            return false;
        }
        self.prefix
            .as_ref()
            .map(Prefix::is_verbatim)
            .unwrap_or(false)
    }

    #[inline]
    pub(super) fn prefix_remaining(&self) -> usize {
        if !Platform::HAS_PREFIXES {
            return 0;
        }
        if self.front == State::Prefix {
            self.prefix_len()
        } else {
            0
        }
    }

    #[inline]
    fn len_before_body(&self) -> usize {
        let root = if self.front <= State::StartDir && self.has_physical_root {
            1
        } else {
            0
        };
        let cur_dir = if self.front <= State::StartDir && self.include_cur_dir() {
            1
        } else {
            0
        };
        self.prefix_remaining() + root + cur_dir
    }

    #[inline]
    fn finished(&self) -> bool {
        self.front == State::Done || self.back == State::Done || self.front > self.back
    }

    #[inline]
    fn is_sep_byte(&self, b: u8) -> bool {
        if self.prefix_verbatim() {
            Platform::is_verbatim_sep(b)
        } else {
            Platform::is_sep_byte(b)
        }
    }

    /// Extracts a slice corresponding to the portion of the path remaining for iteration.
    #[must_use]
    pub fn as_path(&self) -> &'a Path {
        let mut comps = self.clone();
        if comps.front == State::Body {
            comps.trim_left();
        }
        if comps.back == State::Body {
            comps.trim_right();
        }
        unsafe { Path::from_u8_slice(comps.path) }
    }

    pub(super) fn has_root(&self) -> bool {
        if self.has_physical_root {
            return true;
        }
        if Platform::HAS_PREFIXES
            && let Some(p) = self.prefix
            && p.has_implicit_root()
        {
            return true;
        }
        false
    }

    fn include_cur_dir(&self) -> bool {
        if self.has_root() {
            return false;
        }
        let slice = &self.path[self.prefix_remaining()..];
        match slice {
            [b'.'] => true,
            [b'.', b, ..] => self.is_sep_byte(*b),
            _ => false,
        }
    }

    unsafe fn parse_single_component<'b>(&self, comp: &'b [u8]) -> Option<Component<'b>> {
        match comp {
            b"." if Platform::HAS_PREFIXES && self.prefix_verbatim() => Some(Component::CurDir),
            b"." => None,
            b".." => Some(Component::ParentDir),
            b"" => None,
            _ => Some(Component::Normal(unsafe {
                OsStr::from_encoded_bytes_unchecked(comp)
            })),
        }
    }

    fn parse_next_component(&self) -> (usize, Option<Component<'a>>) {
        debug_assert!(self.front == State::Body);
        let (extra, comp) = match self.path.iter().position(|b| self.is_sep_byte(*b)) {
            None => (0, self.path),
            Some(i) => (1, &self.path[..i]),
        };
        (comp.len() + extra, unsafe {
            self.parse_single_component(comp)
        })
    }

    fn parse_next_component_back(&self) -> (usize, Option<Component<'a>>) {
        debug_assert!(self.back == State::Body);
        let start = self.len_before_body();
        let (extra, comp) = match self.path[start..]
            .iter()
            .rposition(|b| self.is_sep_byte(*b))
        {
            None => (0, &self.path[start..]),
            Some(i) => (1, &self.path[start + i + 1..]),
        };
        (comp.len() + extra, unsafe {
            self.parse_single_component(comp)
        })
    }

    fn trim_left(&mut self) {
        while !self.path.is_empty() {
            let (size, comp) = self.parse_next_component();
            if comp.is_some() {
                return;
            } else {
                self.path = &self.path[size..];
            }
        }
    }

    fn trim_right(&mut self) {
        while self.path.len() > self.len_before_body() {
            let (size, comp) = self.parse_next_component_back();
            if comp.is_some() {
                return;
            } else {
                self.path = &self.path[..self.path.len() - size];
            }
        }
    }
}

impl AsRef<Path> for Components<'_> {
    #[inline]
    fn as_ref(&self) -> &Path {
        self.as_path()
    }
}

impl AsRef<OsStr> for Components<'_> {
    #[inline]
    fn as_ref(&self) -> &OsStr {
        self.as_path().as_os_str()
    }
}

impl fmt::Debug for Components<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        struct DebugHelper<'a>(&'a Path);

        impl fmt::Debug for DebugHelper<'_> {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.debug_list().entries(self.0.components()).finish()
            }
        }

        f.debug_tuple("Components")
            .field(&DebugHelper(self.as_path()))
            .finish()
    }
}

impl fmt::Debug for Iter<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        struct DebugHelper<'a>(&'a Path);

        impl fmt::Debug for DebugHelper<'_> {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.debug_list().entries(self.0.iter()).finish()
            }
        }

        f.debug_tuple("Iter")
            .field(&DebugHelper(self.as_path()))
            .finish()
    }
}

impl<'a> Iter<'a> {
    pub(super) fn new(inner: Components<'a>) -> Self {
        Self { inner }
    }

    /// Extracts a slice corresponding to the portion of the path remaining for iteration.
    #[must_use]
    #[inline]
    pub fn as_path(&self) -> &'a Path {
        self.inner.as_path()
    }
}

impl AsRef<Path> for Iter<'_> {
    #[inline]
    fn as_ref(&self) -> &Path {
        self.as_path()
    }
}

impl AsRef<OsStr> for Iter<'_> {
    #[inline]
    fn as_ref(&self) -> &OsStr {
        self.as_path().as_os_str()
    }
}

impl<'a> Iterator for Iter<'a> {
    type Item = &'a OsStr;

    #[inline]
    fn next(&mut self) -> Option<&'a OsStr> {
        self.inner.next().map(Component::as_os_str)
    }
}

impl<'a> DoubleEndedIterator for Iter<'a> {
    #[inline]
    fn next_back(&mut self) -> Option<&'a OsStr> {
        self.inner.next_back().map(Component::as_os_str)
    }
}

impl FusedIterator for Iter<'_> {}

impl<'a> Iterator for Components<'a> {
    type Item = Component<'a>;

    fn next(&mut self) -> Option<Component<'a>> {
        while !self.finished() {
            match self.front {
                State::Body if !self.path.is_empty() => {
                    let (size, comp) = self.parse_next_component();
                    self.path = &self.path[size..];
                    if comp.is_some() {
                        return comp;
                    }
                }
                State::Body => {
                    self.front = State::Done;
                }
                State::StartDir => {
                    self.front = State::Body;
                    if self.has_physical_root {
                        debug_assert!(!self.path.is_empty());
                        self.path = &self.path[1..];
                        return Some(Component::RootDir);
                    } else if Platform::HAS_PREFIXES
                        && let Some(p) = self.prefix
                    {
                        if p.has_implicit_root() && !p.is_verbatim() {
                            return Some(Component::RootDir);
                        }
                    } else if self.include_cur_dir() {
                        debug_assert!(!self.path.is_empty());
                        self.path = &self.path[1..];
                        return Some(Component::CurDir);
                    }
                }
                _ if !Platform::HAS_PREFIXES => unreachable!(),
                State::Prefix if self.prefix_len() == 0 => {
                    self.front = State::StartDir;
                }
                State::Prefix => {
                    self.front = State::StartDir;
                    debug_assert!(self.prefix_len() <= self.path.len());
                    let raw = &self.path[..self.prefix_len()];
                    self.path = &self.path[self.prefix_len()..];
                    return Some(Component::Prefix(PrefixComponent {
                        raw: unsafe { OsStr::from_encoded_bytes_unchecked(raw) },
                        parsed: self.prefix.unwrap(),
                    }));
                }
                State::Done => unreachable!(),
            }
        }
        None
    }
}

impl<'a> DoubleEndedIterator for Components<'a> {
    fn next_back(&mut self) -> Option<Component<'a>> {
        while !self.finished() {
            match self.back {
                State::Body if self.path.len() > self.len_before_body() => {
                    let (size, comp) = self.parse_next_component_back();
                    self.path = &self.path[..self.path.len() - size];
                    if comp.is_some() {
                        return comp;
                    }
                }
                State::Body => {
                    self.back = State::StartDir;
                }
                State::StartDir => {
                    self.back = if Platform::HAS_PREFIXES {
                        State::Prefix
                    } else {
                        State::Done
                    };
                    if self.has_physical_root {
                        self.path = &self.path[..self.path.len() - 1];
                        return Some(Component::RootDir);
                    } else if Platform::HAS_PREFIXES
                        && let Some(p) = self.prefix
                    {
                        if p.has_implicit_root() && !p.is_verbatim() {
                            return Some(Component::RootDir);
                        }
                    } else if self.include_cur_dir() {
                        self.path = &self.path[..self.path.len() - 1];
                        return Some(Component::CurDir);
                    }
                }
                _ if !Platform::HAS_PREFIXES => unreachable!(),
                State::Prefix if self.prefix_len() > 0 => {
                    self.back = State::Done;
                    return Some(Component::Prefix(PrefixComponent {
                        raw: unsafe { OsStr::from_encoded_bytes_unchecked(self.path) },
                        parsed: self.prefix.unwrap(),
                    }));
                }
                State::Prefix => {
                    self.back = State::Done;
                    return None;
                }
                State::Done => unreachable!(),
            }
        }
        None
    }
}

impl FusedIterator for Components<'_> {}

impl<'a> PartialEq for Components<'a> {
    #[inline]
    fn eq(&self, other: &Components<'a>) -> bool {
        if self.path.len() == other.path.len()
            && self.front == other.front
            && self.back == State::Body
            && other.back == State::Body
            && self.prefix_verbatim() == other.prefix_verbatim()
            && self.path == other.path
        {
            return true;
        }
        Iterator::eq(self.clone().rev(), other.clone().rev())
    }
}

impl Eq for Components<'_> {}

impl<'a> PartialOrd for Components<'a> {
    #[inline]
    fn partial_cmp(&self, other: &Components<'a>) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Components<'_> {
    #[inline]
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        Components::compare(self.clone(), other.clone())
    }
}

/// An iterator over [`Path`] and its ancestors.
#[derive(Copy, Clone, Debug)]
pub struct Ancestors<'a> {
    next: Option<&'a Path>,
}

impl<'a> Ancestors<'a> {
    pub(super) fn new(next: &'a Path) -> Self {
        Self { next: Some(next) }
    }
}

impl<'a> Iterator for Ancestors<'a> {
    type Item = &'a Path;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        let next = self.next;
        self.next = next.and_then(Path::parent);
        next
    }
}

impl FusedIterator for Ancestors<'_> {}
