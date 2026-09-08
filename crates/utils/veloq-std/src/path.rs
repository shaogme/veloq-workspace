//! Cross-platform path manipulation without depending on the standard library.

use core::{borrow::Borrow, fmt, ops::Deref};

use crate::alloc_crate::string::String;

#[cfg(feature = "std")]
use std::path::{Path as StdPath, PathBuf as StdPathBuf};

#[inline]
pub(crate) const fn is_separator_byte(b: u8) -> bool {
    b == b'/' || (cfg!(windows) && b == b'\\')
}

/// A slice of a path.
#[derive(Eq, PartialEq, Ord, PartialOrd, Hash)]
#[repr(transparent)]
pub struct Path {
    inner: str,
}

impl Path {
    /// Directly wraps a string slice as a `Path` slice.
    #[inline]
    pub fn new<S: AsRef<str> + ?Sized>(s: &S) -> &Path {
        unsafe { &*(s.as_ref() as *const str as *const Path) }
    }

    /// Yields the underlying [`str`] slice.
    #[inline]
    pub fn as_str(&self) -> &str {
        &self.inner
    }

    /// Yields the underlying byte slice.
    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        self.inner.as_bytes()
    }

    /// Creates an owned [`PathBuf`] with `path` adjoined to `self`.
    pub fn join<P: AsRef<Path>>(&self, path: P) -> PathBuf {
        let mut buf = self.to_path_buf();
        buf.push(path);
        buf
    }

    /// Returns the `Path` without its final component, if there is one.
    pub fn parent(&self) -> Option<&Path> {
        let bytes = self.as_bytes();
        let mut idx = bytes.len();
        while idx > 0 && is_separator_byte(bytes[idx - 1]) {
            idx -= 1;
        }
        while idx > 0 && !is_separator_byte(bytes[idx - 1]) {
            idx -= 1;
        }
        while idx > 1 && is_separator_byte(bytes[idx - 1]) {
            idx -= 1;
        }
        if idx == 0 {
            None
        } else {
            Some(Path::new(&self.inner[..idx]))
        }
    }

    /// Returns the final component of the `Path`, if there is one.
    pub fn file_name(&self) -> Option<&str> {
        let bytes = self.as_bytes();
        let mut end = bytes.len();
        while end > 0 && is_separator_byte(bytes[end - 1]) {
            end -= 1;
        }
        if end == 0 {
            return None;
        }
        let mut start = end;
        while start > 0 && !is_separator_byte(bytes[start - 1]) {
            start -= 1;
        }
        Some(&self.inner[start..end])
    }

    /// Returns the extension of the `Path`, if there is one.
    pub fn extension(&self) -> Option<&str> {
        let file_name = self.file_name()?;
        let dot_idx = file_name.rfind('.')?;
        if dot_idx == 0 || dot_idx == file_name.len() - 1 {
            None
        } else {
            Some(&file_name[dot_idx + 1..])
        }
    }

    /// Returns `true` if the `Path` is absolute.
    pub fn is_absolute(&self) -> bool {
        let bytes = self.as_bytes();
        if bytes.is_empty() {
            return false;
        }
        if bytes.starts_with(b"/") {
            return true;
        }
        #[cfg(windows)]
        {
            (bytes.len() >= 3 && bytes[1] == b':' && (bytes[2] == b'/' || bytes[2] == b'\\'))
                || bytes.starts_with(b"\\")
        }
        #[cfg(not(windows))]
        {
            false
        }
    }

    /// Returns `true` if the `Path` is relative.
    #[inline]
    pub fn is_relative(&self) -> bool {
        !self.is_absolute()
    }

    /// Copies `self` into a new `PathBuf`.
    #[inline]
    pub fn to_path_buf(&self) -> PathBuf {
        PathBuf::from(self)
    }
}

/// An owned, mutable path.
#[derive(Clone, Default, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct PathBuf {
    inner: String,
}

impl PathBuf {
    /// Allocates an empty `PathBuf`.
    #[inline]
    pub fn new() -> Self {
        Self {
            inner: String::new(),
        }
    }

    /// Creates a new `PathBuf` with a given capacity.
    #[inline]
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            inner: String::with_capacity(capacity),
        }
    }

    /// Coerces to a [`Path`] slice.
    #[inline]
    pub fn as_path(&self) -> &Path {
        Path::new(&self.inner)
    }

    /// Extends `self` with `path`.
    pub fn push<P: AsRef<Path>>(&mut self, path: P) {
        let path = path.as_ref();
        if path.is_absolute() {
            self.inner.clear();
            self.inner.push_str(path.as_str());
            return;
        }

        if !self.inner.is_empty() {
            let last_byte = self.inner.as_bytes()[self.inner.len() - 1];
            if !is_separator_byte(last_byte) {
                #[cfg(windows)]
                self.inner.push('\\');

                #[cfg(not(windows))]
                self.inner.push('/');
            }
        }
        self.inner.push_str(path.as_str());
    }

    /// Truncates `self` to its [`parent`].
    pub fn pop(&mut self) -> bool {
        match self.as_path().parent().map(|p| p.as_str().len()) {
            Some(len) => {
                self.inner.truncate(len);
                true
            }
            None => false,
        }
    }

    /// Truncates `self` to the empty string.
    #[inline]
    pub fn clear(&mut self) {
        self.inner.clear();
    }
}

impl AsRef<Path> for Path {
    #[inline]
    fn as_ref(&self) -> &Path {
        self
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
        Path::new(self.as_str())
    }
}

impl AsRef<Path> for PathBuf {
    #[inline]
    fn as_ref(&self) -> &Path {
        self.as_path()
    }
}

impl AsRef<str> for Path {
    #[inline]
    fn as_ref(&self) -> &str {
        &self.inner
    }
}

impl AsRef<[u8]> for Path {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        self.inner.as_bytes()
    }
}

impl Deref for Path {
    type Target = str;

    #[inline]
    fn deref(&self) -> &str {
        &self.inner
    }
}

impl Deref for PathBuf {
    type Target = Path;

    #[inline]
    fn deref(&self) -> &Path {
        self.as_path()
    }
}

impl Borrow<Path> for PathBuf {
    #[inline]
    fn borrow(&self) -> &Path {
        self.as_path()
    }
}

impl From<&str> for PathBuf {
    #[inline]
    fn from(s: &str) -> Self {
        Self {
            inner: String::from(s),
        }
    }
}

impl From<String> for PathBuf {
    #[inline]
    fn from(inner: String) -> Self {
        Self { inner }
    }
}

impl From<&Path> for PathBuf {
    #[inline]
    fn from(p: &Path) -> Self {
        Self {
            inner: String::from(&p.inner),
        }
    }
}

impl fmt::Debug for Path {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&self.inner, f)
    }
}

impl fmt::Display for Path {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.inner, f)
    }
}

impl fmt::Debug for PathBuf {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&self.inner, f)
    }
}

impl fmt::Display for PathBuf {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.inner, f)
    }
}

#[cfg(feature = "std")]
impl AsRef<Path> for StdPath {
    #[inline]
    fn as_ref(&self) -> &Path {
        Path::new(self.to_str().expect("path must be valid utf-8"))
    }
}

#[cfg(feature = "std")]
impl AsRef<Path> for StdPathBuf {
    #[inline]
    fn as_ref(&self) -> &Path {
        Path::new(self.to_str().expect("path must be valid utf-8"))
    }
}
