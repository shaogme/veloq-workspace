//! Cross-platform path manipulation without depending on the standard library.
//!
//! This module provides two types, [`PathBuf`] and [`Path`] (akin to [`String`]
//! and [`str`]), for working with paths abstractly. These types are thin wrappers
//! around [`OsString`] and [`OsStr`] respectively.

#[cfg(test)]
mod tests;

use crate::{
    alloc_crate::{
        borrow::{Cow, ToOwned},
        boxed::Box,
        rc::Rc,
        string::String,
        sync::Arc,
    },
    ffi::{Display as OsStrDisplay, OsStr, OsString},
    fs::Fs,
    path::{
        Ancestors, Component, Components, Iter, PathBuf, Prefix,
        components::Platform as PathPlatform,
    },
};
use core::{
    cmp,
    error::Error,
    fmt,
    hash::{Hash, Hasher},
    ops::{Deref, DerefMut},
};

#[cfg(feature = "std")]
use std::path::{Path as StdPath, PathBuf as StdPathBuf};

/// A slice of a path.
#[repr(transparent)]
pub struct Path {
    inner: OsStr,
}

/// Helper struct for safely printing paths with [`format!`] and `{}`.
pub struct Display<'a> {
    inner: OsStrDisplay<'a>,
}

/// An error returned from [`Path::strip_prefix`] if the prefix was not found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StripPrefixError(());

impl Path {
    pub(super) unsafe fn from_u8_slice(s: &[u8]) -> &Path {
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

    pub(super) fn from_inner_mut(inner: &mut OsStr) -> &mut Path {
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

    /// Returns `true` if the path points to an existing file or directory.
    ///
    /// Errors while querying the path, including invalid path encodings, are
    /// reported as `false`.
    #[must_use]
    #[inline]
    pub fn exists(&self) -> bool {
        Fs::path_exists(self)
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
    /// Returns whether `character` separates path components on this platform.
    pub fn is_separator(character: char) -> bool {
        character.is_ascii() && PathPlatform::is_sep_byte(character as u8)
    }

    pub fn is_absolute(&self) -> bool {
        PathPlatform::is_absolute(self)
    }

    /// Returns `true` if the `Path` is relative.
    #[must_use]
    #[inline]
    pub fn is_relative(&self) -> bool {
        !self.is_absolute()
    }

    pub(crate) fn prefix(&self) -> Option<Prefix<'_>> {
        self.components().prefix()
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
        Ancestors::new(self)
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
        Components::iter_after(self.components(), base.components())
            .map(|c| c.as_path())
            .ok_or(StripPrefixError(()))
    }

    /// Determines whether `base` is a prefix of `self`.
    #[must_use]
    pub fn starts_with<P: AsRef<Path>>(&self, base: P) -> bool {
        self._starts_with(base.as_ref())
    }

    fn _starts_with(&self, base: &Path) -> bool {
        Components::iter_after(self.components(), base.components()).is_some()
    }

    /// Determines whether `child` is a suffix of `self`.
    #[must_use]
    pub fn ends_with<P: AsRef<Path>>(&self, child: P) -> bool {
        self._ends_with(child.as_ref())
    }

    fn _ends_with(&self, child: &Path) -> bool {
        Components::iter_after(self.components().rev(), child.components().rev()).is_some()
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
            .map(Components::rsplit_file_at_dot)
            .and_then(|(before, after)| before.or(after))
    }

    /// Extracts the prefix of [`self.file_name`].
    #[must_use]
    pub fn file_prefix(&self) -> Option<&OsStr> {
        self.file_name()
            .map(Components::split_file_at_dot)
            .map(|(before, _after)| before)
    }

    /// Extracts the extension of [`self.file_name`], if possible.
    #[must_use]
    pub fn extension(&self) -> Option<&OsStr> {
        self.file_name()
            .map(Components::rsplit_file_at_dot)
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
            .is_some_and(PathPlatform::is_sep_byte)
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
                && PathPlatform::is_sep_byte(*last)
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
        Components::new(self)
    }

    /// Produces an iterator over the path's components viewed as [`OsStr`] slices.
    #[inline]
    pub fn iter(&self) -> Iter<'_> {
        Iter::new(self.components())
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
        PathBuf::from_inner(OsString::from(inner))
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

impl ToOwned for Path {
    type Owned = PathBuf;

    #[inline]
    fn to_owned(&self) -> PathBuf {
        self.to_path_buf()
    }

    #[inline]
    fn clone_into(&self, target: &mut PathBuf) {
        self.inner.clone_into(target.as_mut_os_string());
    }
}

impl From<PathBuf> for Box<Path> {
    #[inline]
    fn from(p: PathBuf) -> Box<Path> {
        p.into_boxed_path()
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

impl Clone for Box<Path> {
    #[inline]
    fn clone(&self) -> Self {
        self.to_path_buf().into_boxed_path()
    }
}

impl From<&Path> for Arc<Path> {
    #[inline]
    fn from(s: &Path) -> Arc<Path> {
        let arc: Arc<OsStr> = Arc::from(s.as_os_str());
        unsafe { Arc::from_raw(Arc::into_raw(arc) as *const Path) }
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

impl AsRef<Path> for Path {
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

impl Hash for Path {
    fn hash<H: Hasher>(&self, h: &mut H) {
        let bytes = self.as_u8_slice();
        let (prefix_len, verbatim) = match PathPlatform::parse_prefix(&self.inner) {
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
                PathPlatform::is_verbatim_sep(bytes[i])
            } else {
                PathPlatform::is_sep_byte(bytes[i])
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
                        [b'.', sep, ..] if PathPlatform::is_sep_byte(*sep) => 1,
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
        Components::compare(self.components(), other.components())
    }
}

impl_cmp!(Cow<'_, Path>, Path);

impl_cmp!(Cow<'_, Path>, &Path);

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

impl PartialEq<PathBuf> for Path {
    #[inline]
    fn eq(&self, other: &PathBuf) -> bool {
        <Path as PartialEq>::eq(self, other.as_path())
    }
}

impl PartialOrd<PathBuf> for Path {
    #[inline]
    fn partial_cmp(&self, other: &PathBuf) -> Option<cmp::Ordering> {
        <Path as PartialOrd>::partial_cmp(self, other.as_path())
    }
}

impl PartialEq<PathBuf> for &Path {
    #[inline]
    fn eq(&self, other: &PathBuf) -> bool {
        <Path as PartialEq>::eq(self, other.as_path())
    }
}

impl PartialOrd<PathBuf> for &Path {
    #[inline]
    fn partial_cmp(&self, other: &PathBuf) -> Option<cmp::Ordering> {
        <Path as PartialOrd>::partial_cmp(self, other.as_path())
    }
}
