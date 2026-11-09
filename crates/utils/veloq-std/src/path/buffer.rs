//! Owned path storage and its standard trait implementations.

use crate::{
    alloc_crate::{
        borrow::Cow, boxed::Box, collections::TryReserveError, rc::Rc, string::String, sync::Arc,
        vec::Vec,
    },
    ffi::{OsStr, OsString},
    path::{
        MAIN_SEPARATOR_STR, Path,
        components::{Component, Components, Iter, Platform as PathPlatform},
    },
};
use core::{
    borrow::Borrow,
    cmp,
    convert::Infallible,
    fmt,
    hash::{Hash, Hasher},
    ops::{Deref, DerefMut},
    str::FromStr,
};

/// An owned, mutable path (akin to [`String`]).
#[derive(Clone, Default)]
pub struct PathBuf {
    inner: OsString,
}

impl PathBuf {
    pub(super) fn from_inner(inner: OsString) -> Self {
        Self { inner }
    }

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
        let mut need_sep = buf
            .last()
            .map(|c| !PathPlatform::is_sep_byte(*c))
            .unwrap_or(false);

        let comps = self.components();

        if comps.is_bare_drive_prefix() {
            need_sep = false;
        }

        let need_clear = path.is_absolute() || path.prefix().is_some();

        if need_clear {
            self.inner.clear();
        } else if comps.prefix_verbatim() && !path.as_os_str().is_empty() {
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
                        !prefix.kind().is_drive() && prefix.kind().len() > 0
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
        Components::validate_extension(extension);

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
        Components::validate_extension(extension);

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

impl From<Box<Path>> for PathBuf {
    #[inline]
    fn from(boxed: Box<Path>) -> PathBuf {
        boxed.into_path_buf()
    }
}

impl From<PathBuf> for Arc<Path> {
    #[inline]
    fn from(s: PathBuf) -> Arc<Path> {
        let arc: Arc<OsStr> = Arc::from(s.into_os_string());
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
    type Err = Infallible;

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

impl AsRef<Path> for PathBuf {
    #[inline]
    fn as_ref(&self) -> &Path {
        self
    }
}

impl AsRef<OsStr> for PathBuf {
    #[inline]
    fn as_ref(&self) -> &OsStr {
        &self.inner
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

impl Hash for PathBuf {
    #[inline]
    fn hash<H: Hasher>(&self, h: &mut H) {
        self.as_path().hash(h)
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
        Components::compare(self.components(), other.components())
    }
}

impl_cmp!(Cow<'_, Path>, PathBuf);

impl_cmp_os_str!(PathBuf, OsStr);

impl_cmp_os_str!(PathBuf, &OsStr);

impl_cmp_os_str!(PathBuf, Cow<'_, OsStr>);

impl_cmp_os_str!(PathBuf, OsString);

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

impl PartialEq<Path> for PathBuf {
    #[inline]
    fn eq(&self, other: &Path) -> bool {
        <Path as PartialEq>::eq(self.as_path(), other)
    }
}

impl PartialOrd<Path> for PathBuf {
    #[inline]
    fn partial_cmp(&self, other: &Path) -> Option<cmp::Ordering> {
        <Path as PartialOrd>::partial_cmp(self.as_path(), other)
    }
}

impl PartialEq<&Path> for PathBuf {
    #[inline]
    fn eq(&self, other: &&Path) -> bool {
        <Path as PartialEq>::eq(self.as_path(), *other)
    }
}

impl PartialOrd<&Path> for PathBuf {
    #[inline]
    fn partial_cmp(&self, other: &&Path) -> Option<cmp::Ordering> {
        <Path as PartialOrd>::partial_cmp(self.as_path(), *other)
    }
}
