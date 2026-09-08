//! Cross-platform path manipulation without depending on the standard library.
//!
//! This module provides two types, [`PathBuf`] and [`Path`] (akin to [`String`]
//! and [`str`]), for working with paths abstractly. These types are thin wrappers
//! around [`OsString`] and [`OsStr`] respectively.

use core::{
    borrow::Borrow,
    cmp,
    error::Error,
    fmt,
    hash::{Hash, Hasher},
    iter::FusedIterator,
    ops::{Deref, DerefMut},
    str::FromStr,
};

use crate::{
    alloc_crate::{
        borrow::{Cow, ToOwned},
        boxed::Box,
        collections::TryReserveError,
        rc::Rc,
        string::String,
        sync::Arc,
        vec::Vec,
    },
    ffi::{Display as OsStrDisplay, OsStr, OsString},
};

use self::sys::{HAS_PREFIXES, is_absolute, is_sep_byte, is_verbatim_sep, parse_prefix};

#[cfg(feature = "std")]
use std::path::{Path as StdPath, PathBuf as StdPathBuf};

mod sys;

#[cfg(test)]
mod tests;

pub use self::sys::{MAIN_SEPARATOR, MAIN_SEPARATOR_STR};

/// Determines whether the character is one of the permitted path separators
/// for the current platform.
#[inline]
pub const fn is_separator(c: char) -> bool {
    c.is_ascii() && is_sep_byte(c as u8)
}

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
    fn len(&self) -> usize {
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
    fn is_drive(&self) -> bool {
        matches!(*self, Prefix::Disk(_))
    }

    #[inline]
    fn has_implicit_root(&self) -> bool {
        !self.is_drive()
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

fn iter_after<'a, 'b, I, J>(mut iter: I, mut prefix: J) -> Option<I>
where
    I: Iterator<Item = Component<'a>> + Clone,
    J: Iterator<Item = Component<'b>>,
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

fn has_physical_root(s: &[u8], prefix: Option<Prefix<'_>>) -> bool {
    let path = if let Some(p) = prefix {
        &s[p.len()..]
    } else {
        s
    };
    !path.is_empty() && is_sep_byte(path[0])
}

fn rsplit_file_at_dot(file: &OsStr) -> (Option<&OsStr>, Option<&OsStr>) {
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

fn split_file_at_dot(file: &OsStr) -> (&OsStr, Option<&OsStr>) {
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

fn validate_extension(extension: &OsStr) {
    for &b in extension.as_encoded_bytes() {
        if is_sep_byte(b) {
            panic!("extension cannot contain path separators: {extension:?}");
        }
    }
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
    #[inline]
    fn prefix_len(&self) -> usize {
        if !HAS_PREFIXES {
            return 0;
        }
        self.prefix.as_ref().map(Prefix::len).unwrap_or(0)
    }

    #[inline]
    fn prefix_verbatim(&self) -> bool {
        if !HAS_PREFIXES {
            return false;
        }
        self.prefix
            .as_ref()
            .map(Prefix::is_verbatim)
            .unwrap_or(false)
    }

    #[inline]
    fn prefix_remaining(&self) -> usize {
        if !HAS_PREFIXES {
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
            is_verbatim_sep(b)
        } else {
            is_sep_byte(b)
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

    fn has_root(&self) -> bool {
        if self.has_physical_root {
            return true;
        }
        if HAS_PREFIXES
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
            b"." if HAS_PREFIXES && self.prefix_verbatim() => Some(Component::CurDir),
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
                    } else if HAS_PREFIXES && let Some(p) = self.prefix {
                        if p.has_implicit_root() && !p.is_verbatim() {
                            return Some(Component::RootDir);
                        }
                    } else if self.include_cur_dir() {
                        debug_assert!(!self.path.is_empty());
                        self.path = &self.path[1..];
                        return Some(Component::CurDir);
                    }
                }
                _ if !HAS_PREFIXES => unreachable!(),
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
                    self.back = if HAS_PREFIXES {
                        State::Prefix
                    } else {
                        State::Done
                    };
                    if self.has_physical_root {
                        self.path = &self.path[..self.path.len() - 1];
                        return Some(Component::RootDir);
                    } else if HAS_PREFIXES && let Some(p) = self.prefix {
                        if p.has_implicit_root() && !p.is_verbatim() {
                            return Some(Component::RootDir);
                        }
                    } else if self.include_cur_dir() {
                        self.path = &self.path[..self.path.len() - 1];
                        return Some(Component::CurDir);
                    }
                }
                _ if !HAS_PREFIXES => unreachable!(),
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
        compare_components(self.clone(), other.clone())
    }
}

fn compare_components(mut left: Components<'_>, mut right: Components<'_>) -> cmp::Ordering {
    if left.prefix.is_none() && right.prefix.is_none() && left.front == right.front {
        let first_difference = match left.path.iter().zip(right.path).position(|(&a, &b)| a != b) {
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

/// An iterator over [`Path`] and its ancestors.
#[derive(Copy, Clone, Debug)]
pub struct Ancestors<'a> {
    next: Option<&'a Path>,
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

/// An owned, mutable path (akin to [`String`]).
#[derive(Clone, Default)]
pub struct PathBuf {
    inner: OsString,
}

impl PathBuf {
    /// Allocates an empty `PathBuf`.
    #[must_use]
    #[inline]
    pub const fn new() -> PathBuf {
        PathBuf {
            inner: OsString::new(),
        }
    }

    /// Creates a new `PathBuf` with a given capacity.
    #[must_use]
    #[inline]
    pub fn with_capacity(capacity: usize) -> PathBuf {
        PathBuf {
            inner: OsString::with_capacity(capacity),
        }
    }

    /// Coerces to a [`Path`] slice.
    #[must_use]
    #[inline]
    pub fn as_path(&self) -> &Path {
        self
    }

    /// Consumes and leaks the `PathBuf`, returning a mutable reference to the contents.
    #[inline]
    pub fn leak<'a>(self) -> &'a mut Path {
        Path::from_inner_mut(self.inner.leak())
    }

    /// Extends `self` with `path`.
    pub fn push<P: AsRef<Path>>(&mut self, path: P) {
        self._push(path.as_ref())
    }

    fn _push(&mut self, path: &Path) {
        let buf = self.inner.as_encoded_bytes();
        let mut need_sep = buf.last().map(|c| !is_sep_byte(*c)).unwrap_or(false);

        let comps = self.components();

        if comps.prefix_len() > 0
            && comps.prefix_len() == comps.path.len()
            && comps.prefix.unwrap().is_drive()
        {
            need_sep = false;
        }

        let need_clear = path.is_absolute() || path.prefix().is_some();

        if need_clear {
            self.inner.clear();
        } else if comps.prefix_verbatim() && !path.inner.is_empty() {
            let mut buf: Vec<_> = comps.collect();
            for c in path.components() {
                match c {
                    Component::RootDir => {
                        buf.truncate(1);
                        buf.push(c);
                    }
                    Component::CurDir => (),
                    Component::ParentDir => {
                        if let Some(Component::Normal(_)) = buf.last() {
                            buf.pop();
                        }
                    }
                    _ => buf.push(c),
                }
            }

            let mut res = OsString::new();
            let mut need_sep = false;

            for c in buf {
                if need_sep && c != Component::RootDir {
                    res.push(MAIN_SEPARATOR_STR);
                }
                res.push(c.as_os_str());

                need_sep = match c {
                    Component::RootDir => false,
                    Component::Prefix(prefix) => {
                        !prefix.parsed.is_drive() && prefix.parsed.len() > 0
                    }
                    _ => true,
                };
            }

            self.inner = res;
            return;
        } else if path.has_root() {
            let prefix_len = self.components().prefix_remaining();
            self.inner.truncate(prefix_len);
        } else if need_sep {
            self.inner.push(MAIN_SEPARATOR_STR);
        }

        self.inner.push(path);
    }

    /// Truncates `self` to [`self.parent`].
    pub fn pop(&mut self) -> bool {
        match self.parent().map(|p| p.as_u8_slice().len()) {
            Some(len) => {
                self.inner.truncate(len);
                true
            }
            None => false,
        }
    }

    /// Updates [`self.file_name`] to `file_name`.
    pub fn set_file_name<S: AsRef<OsStr>>(&mut self, file_name: S) {
        self._set_file_name(file_name.as_ref())
    }

    fn _set_file_name(&mut self, file_name: &OsStr) {
        if self.file_name().is_some() {
            let popped = self.pop();
            debug_assert!(popped);
        }
        self.push(file_name);
    }

    /// Updates [`self.extension`] to `Some(extension)` or to `None` if empty.
    pub fn set_extension<S: AsRef<OsStr>>(&mut self, extension: S) -> bool {
        self._set_extension(extension.as_ref())
    }

    fn _set_extension(&mut self, extension: &OsStr) -> bool {
        validate_extension(extension);

        let file_stem = match self.file_stem() {
            None => return false,
            Some(f) => f.as_encoded_bytes(),
        };

        let end_file_stem = file_stem[file_stem.len()..].as_ptr() as usize;
        let start = self.inner.as_encoded_bytes().as_ptr() as usize;
        self.inner.truncate(end_file_stem.wrapping_sub(start));

        let new = extension.as_encoded_bytes();
        if !new.is_empty() {
            self.inner.reserve_exact(new.len() + 1);
            self.inner.push(".");
            self.inner.push(extension);
        }

        true
    }

    /// Appends [`self.extension`] with `extension`.
    pub fn add_extension<S: AsRef<OsStr>>(&mut self, extension: S) -> bool {
        self._add_extension(extension.as_ref())
    }

    fn _add_extension(&mut self, extension: &OsStr) -> bool {
        validate_extension(extension);

        let file_name = match self.file_name() {
            None => return false,
            Some(f) => f.as_encoded_bytes(),
        };

        let new = extension.as_encoded_bytes();
        if !new.is_empty() {
            let end_file_name = file_name[file_name.len()..].as_ptr() as usize;
            let start = self.inner.as_encoded_bytes().as_ptr() as usize;
            self.inner.truncate(end_file_name.wrapping_sub(start));

            self.inner.reserve_exact(new.len() + 1);
            self.inner.push(".");
            self.inner.push(extension);
        }

        true
    }

    /// Yields a mutable reference to the underlying [`OsString`] instance.
    #[must_use]
    #[inline]
    pub fn as_mut_os_string(&mut self) -> &mut OsString {
        &mut self.inner
    }

    /// Consumes the `PathBuf`, yielding its internal [`OsString`] storage.
    #[must_use = "`self` will be dropped if the result is not used"]
    #[inline]
    pub fn into_os_string(self) -> OsString {
        self.inner
    }

    /// Converts the `PathBuf` into a `String` if it contains valid Unicode data.
    pub fn into_string(self) -> Result<String, PathBuf> {
        self.into_os_string().into_string().map_err(PathBuf::from)
    }

    /// Converts this `PathBuf` into a [boxed](Box) [`Path`].
    #[must_use = "`self` will be dropped if the result is not used"]
    #[inline]
    pub fn into_boxed_path(self) -> Box<Path> {
        let rw = Box::into_raw(self.inner.into_boxed_os_str()) as *mut Path;
        unsafe { Box::from_raw(rw) }
    }

    /// Invokes [`capacity`] on the underlying instance of [`OsString`].
    #[must_use]
    #[inline]
    pub fn capacity(&self) -> usize {
        self.inner.capacity()
    }

    /// Invokes [`clear`] on the underlying instance of [`OsString`].
    #[inline]
    pub fn clear(&mut self) {
        self.inner.clear()
    }

    /// Invokes [`reserve`] on the underlying instance of [`OsString`].
    #[inline]
    pub fn reserve(&mut self, additional: usize) {
        self.inner.reserve(additional)
    }

    /// Invokes [`try_reserve`] on the underlying instance of [`OsString`].
    #[inline]
    pub fn try_reserve(&mut self, additional: usize) -> Result<(), TryReserveError> {
        self.inner.try_reserve(additional)
    }

    /// Invokes [`reserve_exact`] on the underlying instance of [`OsString`].
    #[inline]
    pub fn reserve_exact(&mut self, additional: usize) {
        self.inner.reserve_exact(additional)
    }

    /// Invokes [`try_reserve_exact`] on the underlying instance of [`OsString`].
    #[inline]
    pub fn try_reserve_exact(&mut self, additional: usize) -> Result<(), TryReserveError> {
        self.inner.try_reserve_exact(additional)
    }

    /// Invokes [`shrink_to_fit`] on the underlying instance of [`OsString`].
    #[inline]
    pub fn shrink_to_fit(&mut self) {
        self.inner.shrink_to_fit()
    }

    /// Invokes [`shrink_to`] on the underlying instance of [`OsString`].
    #[inline]
    pub fn shrink_to(&mut self, min_capacity: usize) {
        self.inner.shrink_to(min_capacity)
    }
}

/// A slice of a path.
#[repr(transparent)]
pub struct Path {
    inner: OsStr,
}

/// An error returned from [`Path::strip_prefix`] if the prefix was not found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StripPrefixError(());

impl Path {
    unsafe fn from_u8_slice(s: &[u8]) -> &Path {
        unsafe { Path::new(OsStr::from_encoded_bytes_unchecked(s)) }
    }

    pub(crate) fn as_u8_slice(&self) -> &[u8] {
        self.inner.as_encoded_bytes()
    }

    /// Directly wraps a string slice as a `Path` slice.
    #[inline]
    pub fn new<S: AsRef<OsStr> + ?Sized>(s: &S) -> &Path {
        unsafe { &*(s.as_ref() as *const OsStr as *const Path) }
    }

    fn from_inner_mut(inner: &mut OsStr) -> &mut Path {
        unsafe { &mut *(inner as *mut OsStr as *mut Path) }
    }

    /// Yields the underlying [`OsStr`] slice.
    #[must_use]
    #[inline]
    pub fn as_os_str(&self) -> &OsStr {
        &self.inner
    }

    /// Yields a mutable reference to the underlying [`OsStr`] slice.
    #[must_use]
    #[inline]
    pub fn as_mut_os_str(&mut self) -> &mut OsStr {
        &mut self.inner
    }

    /// Yields a [`&str`] slice if the `Path` is valid unicode.
    #[must_use]
    #[inline]
    pub fn to_str(&self) -> Option<&str> {
        self.inner.to_str()
    }

    /// Converts a `Path` to a [`Cow<str>`].
    #[must_use]
    #[inline]
    pub fn to_string_lossy(&self) -> Cow<'_, str> {
        self.inner.to_string_lossy()
    }

    /// Yields the underlying byte slice.
    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        self.inner.as_encoded_bytes()
    }

    /// Copies `self` into a new `PathBuf`.
    #[must_use]
    #[inline]
    pub fn to_path_buf(&self) -> PathBuf {
        PathBuf::from(self.inner.to_os_string())
    }

    /// Returns `true` if the `Path` is absolute.
    #[must_use]
    #[inline]
    pub fn is_absolute(&self) -> bool {
        is_absolute(self)
    }

    /// Returns `true` if the `Path` is relative.
    #[must_use]
    #[inline]
    pub fn is_relative(&self) -> bool {
        !self.is_absolute()
    }

    pub(crate) fn prefix(&self) -> Option<Prefix<'_>> {
        self.components().prefix
    }

    /// Returns `true` if the `Path` has a root.
    #[must_use]
    #[inline]
    pub fn has_root(&self) -> bool {
        self.components().has_root()
    }

    /// Returns the `Path` without its final component, if there is one.
    #[must_use]
    pub fn parent(&self) -> Option<&Path> {
        let mut comps = self.components();
        let comp = comps.next_back();
        comp.and_then(|p| match p {
            Component::Normal(_) | Component::CurDir | Component::ParentDir => {
                Some(comps.as_path())
            }
            _ => None,
        })
    }

    /// Produces an iterator over `Path` and its ancestors.
    #[inline]
    pub fn ancestors(&self) -> Ancestors<'_> {
        Ancestors { next: Some(self) }
    }

    /// Returns the final component of the `Path`, if there is one.
    #[must_use]
    pub fn file_name(&self) -> Option<&OsStr> {
        self.components().next_back().and_then(|p| match p {
            Component::Normal(p) => Some(p),
            _ => None,
        })
    }

    /// Returns a path that, when joined onto `base`, yields `self`.
    pub fn strip_prefix<P: AsRef<Path>>(&self, base: P) -> Result<&Path, StripPrefixError> {
        self._strip_prefix(base.as_ref())
    }

    fn _strip_prefix(&self, base: &Path) -> Result<&Path, StripPrefixError> {
        iter_after(self.components(), base.components())
            .map(|c| c.as_path())
            .ok_or(StripPrefixError(()))
    }

    /// Determines whether `base` is a prefix of `self`.
    #[must_use]
    pub fn starts_with<P: AsRef<Path>>(&self, base: P) -> bool {
        self._starts_with(base.as_ref())
    }

    fn _starts_with(&self, base: &Path) -> bool {
        iter_after(self.components(), base.components()).is_some()
    }

    /// Determines whether `child` is a suffix of `self`.
    #[must_use]
    pub fn ends_with<P: AsRef<Path>>(&self, child: P) -> bool {
        self._ends_with(child.as_ref())
    }

    fn _ends_with(&self, child: &Path) -> bool {
        iter_after(self.components().rev(), child.components().rev()).is_some()
    }

    /// Checks whether the `Path` is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Extracts the stem (non-extension) portion of [`self.file_name`].
    #[must_use]
    pub fn file_stem(&self) -> Option<&OsStr> {
        self.file_name()
            .map(rsplit_file_at_dot)
            .and_then(|(before, after)| before.or(after))
    }

    /// Extracts the prefix of [`self.file_name`].
    #[must_use]
    pub fn file_prefix(&self) -> Option<&OsStr> {
        self.file_name()
            .map(split_file_at_dot)
            .map(|(before, _after)| before)
    }

    /// Extracts the extension of [`self.file_name`], if possible.
    #[must_use]
    pub fn extension(&self) -> Option<&OsStr> {
        self.file_name()
            .map(rsplit_file_at_dot)
            .and_then(|(before, after)| before.and(after))
    }

    /// Checks whether the path ends in a trailing separator.
    #[must_use]
    #[inline]
    pub fn has_trailing_sep(&self) -> bool {
        self.as_os_str()
            .as_encoded_bytes()
            .last()
            .copied()
            .is_some_and(is_sep_byte)
    }

    /// Ensures that a path has a trailing separator.
    #[must_use]
    #[inline]
    pub fn with_trailing_sep(&self) -> Cow<'_, Path> {
        if self.has_trailing_sep() {
            Cow::Borrowed(self)
        } else {
            Cow::Owned(self.join(""))
        }
    }

    /// Trims a trailing separator from a path, if possible.
    #[must_use]
    #[inline]
    pub fn trim_trailing_sep(&self) -> &Path {
        if self.has_trailing_sep() && (!self.has_root() || self.parent().is_some()) {
            let mut bytes = self.inner.as_encoded_bytes();
            while let Some((last, init)) = bytes.split_last()
                && is_sep_byte(*last)
            {
                bytes = init;
            }
            Path::new(unsafe { OsStr::from_encoded_bytes_unchecked(bytes) })
        } else {
            self
        }
    }

    /// Creates an owned [`PathBuf`] with `path` adjoined to `self`.
    #[must_use]
    pub fn join<P: AsRef<Path>>(&self, path: P) -> PathBuf {
        let mut buf = self.to_path_buf();
        buf.push(path);
        buf
    }

    /// Creates an owned [`PathBuf`] like `self` but with the given file name.
    #[must_use]
    pub fn with_file_name<S: AsRef<OsStr>>(&self, file_name: S) -> PathBuf {
        let mut buf = self.to_path_buf();
        buf.set_file_name(file_name);
        buf
    }

    /// Creates an owned [`PathBuf`] like `self` but with the given extension.
    pub fn with_extension<S: AsRef<OsStr>>(&self, extension: S) -> PathBuf {
        let mut buf = self.to_path_buf();
        buf.set_extension(extension);
        buf
    }

    /// Creates an owned [`PathBuf`] like `self` but with the extension added.
    pub fn with_added_extension<S: AsRef<OsStr>>(&self, extension: S) -> PathBuf {
        let mut new_path = self.to_path_buf();
        new_path.add_extension(extension);
        new_path
    }

    /// Produces an iterator over the [`Component`]s of the path.
    pub fn components(&self) -> Components<'_> {
        let prefix = parse_prefix(self.as_os_str());
        Components {
            path: self.as_u8_slice(),
            prefix,
            has_physical_root: has_physical_root(self.as_u8_slice(), prefix),
            front: if HAS_PREFIXES {
                State::Prefix
            } else {
                State::StartDir
            },
            back: State::Body,
        }
    }

    /// Produces an iterator over the path's components viewed as [`OsStr`] slices.
    #[inline]
    pub fn iter(&self) -> Iter<'_> {
        Iter {
            inner: self.components(),
        }
    }

    /// Returns an object that implements [`Display`] for safely printing paths.
    #[must_use]
    #[inline]
    pub fn display(&self) -> Display<'_> {
        Display {
            inner: self.inner.display(),
        }
    }

    /// Returns the same path as `&Path`.
    #[inline]
    pub const fn as_path(&self) -> &Path {
        self
    }

    /// Converts a [`Box<Path>`](Box) into a [`PathBuf`].
    #[must_use = "`self` will be dropped if the result is not used"]
    pub fn into_path_buf(self: Box<Self>) -> PathBuf {
        let rw = Box::into_raw(self) as *mut OsStr;
        let inner = unsafe { Box::from_raw(rw) };
        PathBuf {
            inner: OsString::from(inner),
        }
    }
}

/// Helper struct for safely printing paths with [`format!`] and `{}`.
pub struct Display<'a> {
    inner: OsStrDisplay<'a>,
}

impl fmt::Debug for Display<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&self.inner, f)
    }
}

impl fmt::Display for Display<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.inner, f)
    }
}

impl fmt::Display for StripPrefixError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        "prefix not found".fmt(f)
    }
}

impl Error for StripPrefixError {}

impl Deref for Path {
    type Target = OsStr;

    #[inline]
    fn deref(&self) -> &OsStr {
        &self.inner
    }
}

impl DerefMut for Path {
    #[inline]
    fn deref_mut(&mut self) -> &mut OsStr {
        &mut self.inner
    }
}

impl Deref for PathBuf {
    type Target = Path;

    #[inline]
    fn deref(&self) -> &Path {
        Path::new(&self.inner)
    }
}

impl DerefMut for PathBuf {
    #[inline]
    fn deref_mut(&mut self) -> &mut Path {
        Path::from_inner_mut(&mut self.inner)
    }
}

impl Borrow<Path> for PathBuf {
    #[inline]
    fn borrow(&self) -> &Path {
        self.deref()
    }
}

impl ToOwned for Path {
    type Owned = PathBuf;

    #[inline]
    fn to_owned(&self) -> PathBuf {
        self.to_path_buf()
    }

    #[inline]
    fn clone_into(&self, target: &mut PathBuf) {
        self.inner.clone_into(&mut target.inner);
    }
}

impl From<&Path> for Box<Path> {
    fn from(path: &Path) -> Box<Path> {
        let boxed: Box<OsStr> = Box::from(path.as_os_str());
        unsafe { Box::from_raw(Box::into_raw(boxed) as *mut Path) }
    }
}

impl From<&mut Path> for Box<Path> {
    fn from(path: &mut Path) -> Box<Path> {
        Self::from(&*path)
    }
}

impl From<Cow<'_, Path>> for Box<Path> {
    #[inline]
    fn from(cow: Cow<'_, Path>) -> Box<Path> {
        match cow {
            Cow::Borrowed(path) => Box::from(path),
            Cow::Owned(path) => Box::from(path),
        }
    }
}

impl From<Box<Path>> for PathBuf {
    #[inline]
    fn from(boxed: Box<Path>) -> PathBuf {
        boxed.into_path_buf()
    }
}

impl From<PathBuf> for Box<Path> {
    #[inline]
    fn from(p: PathBuf) -> Box<Path> {
        p.into_boxed_path()
    }
}

impl Clone for Box<Path> {
    #[inline]
    fn clone(&self) -> Self {
        self.to_path_buf().into_boxed_path()
    }
}

impl From<PathBuf> for Arc<Path> {
    #[inline]
    fn from(s: PathBuf) -> Arc<Path> {
        let arc: Arc<OsStr> = Arc::from(s.into_os_string());
        unsafe { Arc::from_raw(Arc::into_raw(arc) as *const Path) }
    }
}

impl From<&Path> for Arc<Path> {
    #[inline]
    fn from(s: &Path) -> Arc<Path> {
        let arc: Arc<OsStr> = Arc::from(s.as_os_str());
        unsafe { Arc::from_raw(Arc::into_raw(arc) as *const Path) }
    }
}

impl From<PathBuf> for Rc<Path> {
    #[inline]
    fn from(s: PathBuf) -> Rc<Path> {
        let rc: Rc<OsStr> = Rc::from(s.into_os_string());
        unsafe { Rc::from_raw(Rc::into_raw(rc) as *const Path) }
    }
}

impl From<&Path> for Rc<Path> {
    #[inline]
    fn from(s: &Path) -> Rc<Path> {
        let rc: Rc<OsStr> = Rc::from(s.as_os_str());
        unsafe { Rc::from_raw(Rc::into_raw(rc) as *const Path) }
    }
}

impl<'a> From<&'a Path> for Cow<'a, Path> {
    #[inline]
    fn from(s: &'a Path) -> Cow<'a, Path> {
        Cow::Borrowed(s)
    }
}

impl<'a> From<PathBuf> for Cow<'a, Path> {
    #[inline]
    fn from(s: PathBuf) -> Cow<'a, Path> {
        Cow::Owned(s)
    }
}

impl<'a> From<&'a PathBuf> for Cow<'a, Path> {
    #[inline]
    fn from(p: &'a PathBuf) -> Cow<'a, Path> {
        Cow::Borrowed(p.as_path())
    }
}

impl<'a> From<Cow<'a, Path>> for PathBuf {
    #[inline]
    fn from(p: Cow<'a, Path>) -> Self {
        p.into_owned()
    }
}

impl<T: ?Sized + AsRef<OsStr>> From<&T> for PathBuf {
    #[inline]
    fn from(s: &T) -> PathBuf {
        PathBuf::from(s.as_ref().to_os_string())
    }
}

impl From<OsString> for PathBuf {
    #[inline]
    fn from(inner: OsString) -> Self {
        Self { inner }
    }
}

impl From<PathBuf> for OsString {
    #[inline]
    fn from(path_buf: PathBuf) -> Self {
        path_buf.inner
    }
}

impl From<String> for PathBuf {
    #[inline]
    fn from(s: String) -> Self {
        PathBuf::from(OsString::from(s))
    }
}

impl FromStr for PathBuf {
    type Err = core::convert::Infallible;

    #[inline]
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(PathBuf::from(s))
    }
}

impl<P: AsRef<Path>> FromIterator<P> for PathBuf {
    fn from_iter<I: IntoIterator<Item = P>>(iter: I) -> PathBuf {
        let mut buf = PathBuf::new();
        buf.extend(iter);
        buf
    }
}

impl<P: AsRef<Path>> Extend<P> for PathBuf {
    fn extend<I: IntoIterator<Item = P>>(&mut self, iter: I) {
        iter.into_iter().for_each(move |p| self.push(p.as_ref()));
    }
}

impl AsRef<Path> for Path {
    #[inline]
    fn as_ref(&self) -> &Path {
        self
    }
}

impl AsRef<Path> for PathBuf {
    #[inline]
    fn as_ref(&self) -> &Path {
        self
    }
}

impl AsRef<Path> for OsStr {
    #[inline]
    fn as_ref(&self) -> &Path {
        Path::new(self)
    }
}

impl AsRef<Path> for Cow<'_, OsStr> {
    #[inline]
    fn as_ref(&self) -> &Path {
        Path::new(self)
    }
}

impl AsRef<Path> for OsString {
    #[inline]
    fn as_ref(&self) -> &Path {
        Path::new(self)
    }
}

impl AsRef<Path> for str {
    #[inline]
    fn as_ref(&self) -> &Path {
        Path::new(self)
    }
}

impl AsRef<Path> for String {
    #[inline]
    fn as_ref(&self) -> &Path {
        Path::new(self)
    }
}

impl AsRef<OsStr> for Path {
    #[inline]
    fn as_ref(&self) -> &OsStr {
        &self.inner
    }
}

impl AsRef<OsStr> for PathBuf {
    #[inline]
    fn as_ref(&self) -> &OsStr {
        &self.inner
    }
}

impl AsRef<[u8]> for Path {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl<'a> IntoIterator for &'a Path {
    type Item = &'a OsStr;
    type IntoIter = Iter<'a>;

    #[inline]
    fn into_iter(self) -> Iter<'a> {
        self.iter()
    }
}

impl<'a> IntoIterator for &'a PathBuf {
    type Item = &'a OsStr;
    type IntoIter = Iter<'a>;

    #[inline]
    fn into_iter(self) -> Iter<'a> {
        self.iter()
    }
}

impl Hash for Path {
    fn hash<H: Hasher>(&self, h: &mut H) {
        let bytes = self.as_u8_slice();
        let (prefix_len, verbatim) = match parse_prefix(&self.inner) {
            Some(prefix) => {
                prefix.hash(h);
                (prefix.len(), prefix.is_verbatim())
            }
            None => (0, false),
        };
        let bytes = &bytes[prefix_len..];

        let mut component_start = 0;
        let mut chunk_bits: usize = 0;

        for i in 0..bytes.len() {
            let is_sep = if verbatim {
                is_verbatim_sep(bytes[i])
            } else {
                is_sep_byte(bytes[i])
            };
            if is_sep {
                if i > component_start {
                    let to_hash = &bytes[component_start..i];
                    chunk_bits = chunk_bits.wrapping_add(to_hash.len());
                    chunk_bits = chunk_bits.rotate_right(2);
                    h.write(to_hash);
                }

                component_start = i + 1;

                let tail = &bytes[component_start..];

                if !verbatim {
                    component_start += match tail {
                        [b'.'] => 1,
                        [b'.', sep, ..] if is_sep_byte(*sep) => 1,
                        _ => 0,
                    };
                }
            }
        }

        if component_start < bytes.len() {
            let to_hash = &bytes[component_start..];
            chunk_bits = chunk_bits.wrapping_add(to_hash.len());
            chunk_bits = chunk_bits.rotate_right(2);
            h.write(to_hash);
        }

        h.write_usize(chunk_bits);
    }
}

impl Hash for PathBuf {
    #[inline]
    fn hash<H: Hasher>(&self, h: &mut H) {
        self.as_path().hash(h)
    }
}

impl PartialEq for Path {
    #[inline]
    fn eq(&self, other: &Path) -> bool {
        self.components() == other.components()
    }
}

impl Eq for Path {}

impl PartialOrd for Path {
    #[inline]
    fn partial_cmp(&self, other: &Path) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Path {
    #[inline]
    fn cmp(&self, other: &Path) -> cmp::Ordering {
        compare_components(self.components(), other.components())
    }
}

impl PartialEq for PathBuf {
    #[inline]
    fn eq(&self, other: &PathBuf) -> bool {
        self.components() == other.components()
    }
}

impl Eq for PathBuf {}

impl PartialOrd for PathBuf {
    #[inline]
    fn partial_cmp(&self, other: &PathBuf) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for PathBuf {
    #[inline]
    fn cmp(&self, other: &PathBuf) -> cmp::Ordering {
        compare_components(self.components(), other.components())
    }
}

macro_rules! impl_cmp {
    ($lhs:ty, $rhs: ty) => {
        impl PartialEq<$rhs> for $lhs {
            #[inline]
            fn eq(&self, other: &$rhs) -> bool {
                <Path as PartialEq>::eq(self, other)
            }
        }

        impl PartialEq<$lhs> for $rhs {
            #[inline]
            fn eq(&self, other: &$lhs) -> bool {
                <Path as PartialEq>::eq(self, other)
            }
        }

        impl PartialOrd<$rhs> for $lhs {
            #[inline]
            fn partial_cmp(&self, other: &$rhs) -> Option<cmp::Ordering> {
                <Path as PartialOrd>::partial_cmp(self, other)
            }
        }

        impl PartialOrd<$lhs> for $rhs {
            #[inline]
            fn partial_cmp(&self, other: &$lhs) -> Option<cmp::Ordering> {
                <Path as PartialOrd>::partial_cmp(self, other)
            }
        }
    };
}

impl_cmp!(PathBuf, Path);
impl_cmp!(PathBuf, &Path);
impl_cmp!(Cow<'_, Path>, Path);
impl_cmp!(Cow<'_, Path>, &Path);
impl_cmp!(Cow<'_, Path>, PathBuf);

macro_rules! impl_cmp_os_str {
    ($lhs:ty, $rhs: ty) => {
        impl PartialEq<$rhs> for $lhs {
            #[inline]
            fn eq(&self, other: &$rhs) -> bool {
                <Path as PartialEq>::eq(self, other.as_ref())
            }
        }

        impl PartialEq<$lhs> for $rhs {
            #[inline]
            fn eq(&self, other: &$lhs) -> bool {
                <Path as PartialEq>::eq(self.as_ref(), other)
            }
        }

        impl PartialOrd<$rhs> for $lhs {
            #[inline]
            fn partial_cmp(&self, other: &$rhs) -> Option<cmp::Ordering> {
                <Path as PartialOrd>::partial_cmp(self, other.as_ref())
            }
        }

        impl PartialOrd<$lhs> for $rhs {
            #[inline]
            fn partial_cmp(&self, other: &$lhs) -> Option<cmp::Ordering> {
                <Path as PartialOrd>::partial_cmp(self.as_ref(), other)
            }
        }
    };
}

impl_cmp_os_str!(PathBuf, OsStr);
impl_cmp_os_str!(PathBuf, &OsStr);
impl_cmp_os_str!(PathBuf, Cow<'_, OsStr>);
impl_cmp_os_str!(PathBuf, OsString);
impl_cmp_os_str!(Path, OsStr);
impl_cmp_os_str!(Path, &OsStr);
impl_cmp_os_str!(Path, Cow<'_, OsStr>);
impl_cmp_os_str!(Path, OsString);
impl_cmp_os_str!(&Path, OsStr);
impl_cmp_os_str!(&Path, Cow<'_, OsStr>);
impl_cmp_os_str!(&Path, OsString);
impl_cmp_os_str!(Cow<'_, Path>, OsStr);
impl_cmp_os_str!(Cow<'_, Path>, &OsStr);
impl_cmp_os_str!(Cow<'_, Path>, OsString);

impl cmp::PartialEq<str> for Path {
    #[inline]
    fn eq(&self, other: &str) -> bool {
        let other: &OsStr = other.as_ref();
        self == other
    }
}

impl cmp::PartialEq<Path> for str {
    #[inline]
    fn eq(&self, other: &Path) -> bool {
        other == self
    }
}

impl cmp::PartialEq<String> for Path {
    #[inline]
    fn eq(&self, other: &String) -> bool {
        self == other.as_str()
    }
}

impl cmp::PartialEq<Path> for String {
    #[inline]
    fn eq(&self, other: &Path) -> bool {
        self.as_str() == other
    }
}

impl cmp::PartialEq<str> for PathBuf {
    #[inline]
    fn eq(&self, other: &str) -> bool {
        self.as_path() == other
    }
}

impl cmp::PartialEq<PathBuf> for str {
    #[inline]
    fn eq(&self, other: &PathBuf) -> bool {
        self == other.as_path()
    }
}

impl cmp::PartialEq<String> for PathBuf {
    #[inline]
    fn eq(&self, other: &String) -> bool {
        self.as_path() == other.as_str()
    }
}

impl cmp::PartialEq<PathBuf> for String {
    #[inline]
    fn eq(&self, other: &PathBuf) -> bool {
        self.as_str() == other.as_path()
    }
}

impl cmp::PartialEq<&str> for Path {
    #[inline]
    fn eq(&self, other: &&str) -> bool {
        self == *other
    }
}

impl cmp::PartialEq<Path> for &str {
    #[inline]
    fn eq(&self, other: &Path) -> bool {
        *self == other
    }
}

impl cmp::PartialEq<&str> for PathBuf {
    #[inline]
    fn eq(&self, other: &&str) -> bool {
        self.as_path() == *other
    }
}

impl cmp::PartialEq<PathBuf> for &str {
    #[inline]
    fn eq(&self, other: &PathBuf) -> bool {
        *self == other.as_path()
    }
}

impl fmt::Debug for Path {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&self.inner, f)
    }
}

impl fmt::Display for Path {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.display(), f)
    }
}

impl fmt::Debug for PathBuf {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&**self, f)
    }
}

impl fmt::Display for PathBuf {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.display(), f)
    }
}

#[cfg(feature = "std")]
impl AsRef<Path> for StdPath {
    #[inline]
    fn as_ref(&self) -> &Path {
        unsafe { Path::from_u8_slice(self.as_os_str().as_encoded_bytes()) }
    }
}

#[cfg(feature = "std")]
impl AsRef<Path> for StdPathBuf {
    #[inline]
    fn as_ref(&self) -> &Path {
        unsafe { Path::from_u8_slice(self.as_os_str().as_encoded_bytes()) }
    }
}
