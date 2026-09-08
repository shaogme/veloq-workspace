use core::{
    borrow::Borrow,
    cmp,
    convert::Infallible,
    fmt,
    hash::{Hash, Hasher},
    ops::{self, RangeBounds, RangeFull},
    str::FromStr,
};

use crate::alloc_crate::{
    borrow::Cow, boxed::Box, collections::TryReserveError, rc::Rc, string::String, sync::Arc,
    vec::Vec,
};

#[cfg(windows)]
pub(crate) mod wtf8;

#[cfg(not(windows))]
mod bytes;

#[cfg(not(windows))]
pub(crate) use bytes::{Buf, Slice};

#[cfg(windows)]
pub(crate) use wtf8::{Buf, Slice};

#[cfg(test)]
mod tests;

#[derive(Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct OsString {
    pub(crate) inner: Buf,
}

#[repr(transparent)]
pub struct OsStr {
    pub(crate) inner: Slice,
}

impl OsString {
    #[must_use]
    #[inline]
    pub const fn new() -> OsString {
        OsString { inner: Buf::new() }
    }

    /// Converts bytes to an `OsString` without checking that the bytes contain
    /// valid data.
    ///
    /// # Safety
    ///
    /// Callers must pass in bytes that originated as a mixture of validated UTF-8
    /// and bytes from [`OsStr::as_encoded_bytes`].
    #[inline]
    pub unsafe fn from_encoded_bytes_unchecked(bytes: Vec<u8>) -> Self {
        OsString {
            inner: unsafe { Buf::from_encoded_bytes_unchecked(bytes) },
        }
    }

    #[must_use]
    #[inline]
    pub fn as_os_str(&self) -> &OsStr {
        self
    }

    #[inline]
    pub fn into_encoded_bytes(self) -> Vec<u8> {
        self.inner.into_encoded_bytes()
    }

    #[inline]
    pub fn into_string(self) -> Result<String, OsString> {
        self.inner.into_string().map_err(|inner| OsString { inner })
    }

    #[inline]
    pub fn push<T: AsRef<OsStr>>(&mut self, s: T) {
        self.inner.push_slice(&s.as_ref().inner);
    }

    #[must_use]
    #[inline]
    pub fn with_capacity(capacity: usize) -> OsString {
        OsString {
            inner: Buf::with_capacity(capacity),
        }
    }

    #[inline]
    pub fn clear(&mut self) {
        self.inner.clear();
    }

    #[must_use]
    #[inline]
    pub fn capacity(&self) -> usize {
        self.inner.capacity()
    }

    #[inline]
    pub fn reserve(&mut self, additional: usize) {
        self.inner.reserve(additional);
    }

    #[inline]
    pub fn try_reserve(&mut self, additional: usize) -> Result<(), TryReserveError> {
        self.inner.try_reserve(additional)
    }

    #[inline]
    pub fn reserve_exact(&mut self, additional: usize) {
        self.inner.reserve_exact(additional);
    }

    #[inline]
    pub fn try_reserve_exact(&mut self, additional: usize) -> Result<(), TryReserveError> {
        self.inner.try_reserve_exact(additional)
    }

    #[inline]
    pub fn shrink_to_fit(&mut self) {
        self.inner.shrink_to_fit();
    }

    #[inline]
    pub fn shrink_to(&mut self, min_capacity: usize) {
        self.inner.shrink_to(min_capacity);
    }

    #[must_use = "`self` will be dropped if the result is not used"]
    pub fn into_boxed_os_str(self) -> Box<OsStr> {
        let rw = Box::into_raw(self.inner.into_box()) as *mut OsStr;
        unsafe { Box::from_raw(rw) }
    }

    #[inline]
    pub fn leak<'a>(self) -> &'a mut OsStr {
        OsStr::from_inner_mut(self.inner.leak())
    }

    #[inline]
    pub fn truncate(&mut self, len: usize) {
        if len <= self.len() {
            self.as_os_str().inner.check_public_boundary(len);
            unsafe { self.inner.truncate_unchecked(len) };
        }
    }
}

impl OsStr {
    #[inline]
    pub fn new<S: AsRef<OsStr> + ?Sized>(s: &S) -> &OsStr {
        s.as_ref()
    }

    /// Converts a slice of bytes to an `OsStr` slice without checking that the string
    /// contains valid data.
    ///
    /// # Safety
    ///
    /// Callers must pass in bytes that originated as a mixture of validated UTF-8
    /// and bytes from [`OsStr::as_encoded_bytes`].
    #[inline]
    pub unsafe fn from_encoded_bytes_unchecked(bytes: &[u8]) -> &Self {
        Self::from_inner(unsafe { Slice::from_encoded_bytes_unchecked(bytes) })
    }

    #[inline]
    pub(crate) const fn from_inner(inner: &Slice) -> &OsStr {
        unsafe { &*(inner as *const Slice as *const OsStr) }
    }

    #[inline]
    pub(crate) fn from_inner_mut(inner: &mut Slice) -> &mut OsStr {
        unsafe { &mut *(inner as *mut Slice as *mut OsStr) }
    }

    #[must_use = "this returns the result of the operation, without modifying the original"]
    #[inline]
    pub fn to_str(&self) -> Option<&str> {
        self.inner.to_str().ok()
    }

    #[must_use = "this returns the result of the operation, without modifying the original"]
    #[inline]
    pub fn to_string_lossy(&self) -> Cow<'_, str> {
        self.inner.to_string_lossy()
    }

    #[must_use = "this returns the result of the operation, without modifying the original"]
    #[inline]
    pub fn to_os_string(&self) -> OsString {
        OsString {
            inner: self.inner.to_owned(),
        }
    }

    #[must_use]
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.inner.inner.is_empty()
    }

    #[must_use]
    #[inline]
    pub fn len(&self) -> usize {
        self.inner.inner.len()
    }

    #[must_use = "`self` will be dropped if the result is not used"]
    pub fn into_os_string(self: Box<Self>) -> OsString {
        let boxed = unsafe { Box::from_raw(Box::into_raw(self) as *mut Slice) };
        OsString {
            inner: Buf::from_box(boxed),
        }
    }

    pub fn split_at(&self, mid: usize) -> (&OsStr, &OsStr) {
        self.inner.check_public_boundary(mid);
        unsafe { self.split_at_unchecked(mid) }
    }

    pub fn split_at_checked(&self, mid: usize) -> Option<(&OsStr, &OsStr)> {
        self.inner.try_check_public_boundary(mid)?;
        unsafe { Some(self.split_at_unchecked(mid)) }
    }

    unsafe fn split_at_unchecked(&self, mid: usize) -> (&OsStr, &OsStr) {
        let bytes = self.as_encoded_bytes();
        assert!(mid <= bytes.len());
        let (first, second) = bytes.split_at(mid);
        unsafe {
            (
                Self::from_encoded_bytes_unchecked(first),
                Self::from_encoded_bytes_unchecked(second),
            )
        }
    }

    #[inline]
    pub fn as_encoded_bytes(&self) -> &[u8] {
        self.inner.as_encoded_bytes()
    }

    pub fn slice_encoded_bytes<R: RangeBounds<usize>>(&self, range: R) -> &Self {
        let encoded_bytes = self.as_encoded_bytes();
        let len = encoded_bytes.len();
        let start = match range.start_bound() {
            ops::Bound::Included(&n) => n,
            ops::Bound::Excluded(&n) => n
                .checked_add(1)
                .expect("attempted to index slice up to maximum usize"),
            ops::Bound::Unbounded => 0,
        };
        let end = match range.end_bound() {
            ops::Bound::Included(&n) => n
                .checked_add(1)
                .expect("attempted to index slice up to maximum usize"),
            ops::Bound::Excluded(&n) => n,
            ops::Bound::Unbounded => len,
        };
        assert!(
            start <= end,
            "slice index starts at {start} but ends at {end}"
        );
        assert!(
            end <= len,
            "range end index {end} out of range for slice of length {len}"
        );

        self.inner.check_public_boundary(start);
        self.inner.check_public_boundary(end);

        unsafe { Self::from_encoded_bytes_unchecked(&encoded_bytes[start..end]) }
    }

    #[inline]
    pub fn make_ascii_lowercase(&mut self) {
        self.inner.make_ascii_lowercase();
    }

    #[inline]
    pub fn make_ascii_uppercase(&mut self) {
        self.inner.make_ascii_uppercase();
    }

    #[must_use = "to lowercase the value in-place, use `make_ascii_lowercase`"]
    pub fn to_ascii_lowercase(&self) -> OsString {
        OsString {
            inner: self.inner.to_ascii_lowercase(),
        }
    }

    #[must_use = "to uppercase the value in-place, use `make_ascii_uppercase`"]
    pub fn to_ascii_uppercase(&self) -> OsString {
        OsString {
            inner: self.inner.to_ascii_uppercase(),
        }
    }

    #[must_use]
    #[inline]
    pub fn is_ascii(&self) -> bool {
        self.inner.is_ascii()
    }

    pub fn eq_ignore_ascii_case<S: AsRef<OsStr>>(&self, other: S) -> bool {
        self.inner.eq_ignore_ascii_case(&other.as_ref().inner)
    }

    #[must_use = "this does not display the `OsStr`; it returns an object that can be displayed"]
    #[inline]
    pub fn display(&self) -> Display<'_> {
        Display { os_str: self }
    }

    #[inline]
    pub const fn as_os_str(&self) -> &OsStr {
        self
    }
}

pub struct Display<'a> {
    os_str: &'a OsStr,
}

impl fmt::Debug for Display<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&self.os_str, f)
    }
}

impl fmt::Display for Display<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.os_str.inner, f)
    }
}

pub trait OsStrJoin {
    fn join(&self, sep: &OsStr) -> OsString;
}

impl<S: Borrow<OsStr>> OsStrJoin for [S] {
    fn join(&self, sep: &OsStr) -> OsString {
        let Some((first, suffix)) = self.split_first() else {
            return OsString::new();
        };
        let mut res = first.borrow().to_os_string();
        for item in suffix {
            res.push(sep);
            res.push(item.borrow());
        }
        res
    }
}

impl From<String> for OsString {
    #[inline]
    fn from(s: String) -> OsString {
        OsString {
            inner: Buf::from_string(s),
        }
    }
}

impl<T: ?Sized + AsRef<OsStr>> From<&T> for OsString {
    #[inline]
    fn from(s: &T) -> OsString {
        s.as_ref().to_os_string()
    }
}

impl ops::Index<RangeFull> for OsString {
    type Output = OsStr;

    #[inline]
    fn index(&self, _index: RangeFull) -> &OsStr {
        OsStr::from_inner(self.inner.as_slice())
    }
}

impl ops::IndexMut<RangeFull> for OsString {
    #[inline]
    fn index_mut(&mut self, _index: RangeFull) -> &mut OsStr {
        OsStr::from_inner_mut(self.inner.as_mut_slice())
    }
}

impl ops::Deref for OsString {
    type Target = OsStr;

    #[inline]
    fn deref(&self) -> &OsStr {
        &self[..]
    }
}

impl ops::DerefMut for OsString {
    #[inline]
    fn deref_mut(&mut self) -> &mut OsStr {
        &mut self[..]
    }
}

impl Default for OsString {
    #[inline]
    fn default() -> OsString {
        OsString::new()
    }
}

impl Clone for OsString {
    #[inline]
    fn clone(&self) -> Self {
        OsString {
            inner: self.inner.clone(),
        }
    }

    #[inline]
    fn clone_from(&mut self, source: &Self) {
        self.inner.clone_from(&source.inner);
    }
}

impl fmt::Debug for OsString {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&**self, formatter)
    }
}

impl fmt::Write for OsString {
    fn write_str(&mut self, s: &str) -> fmt::Result {
        self.push(s);
        Ok(())
    }
}

impl PartialEq<str> for OsString {
    #[inline]
    fn eq(&self, other: &str) -> bool {
        self.as_os_str() == other
    }
}

impl PartialEq<OsString> for str {
    #[inline]
    fn eq(&self, other: &OsString) -> bool {
        other.as_os_str() == self
    }
}

impl PartialEq<&str> for OsString {
    #[inline]
    fn eq(&self, other: &&str) -> bool {
        **self == **other
    }
}

impl PartialEq<OsString> for &str {
    #[inline]
    fn eq(&self, other: &OsString) -> bool {
        **other == **self
    }
}

impl PartialEq<OsStr> for OsString {
    #[inline]
    fn eq(&self, other: &OsStr) -> bool {
        &**self == other
    }
}

impl PartialEq<OsString> for OsStr {
    #[inline]
    fn eq(&self, other: &OsString) -> bool {
        self == &**other
    }
}

impl PartialEq<&OsStr> for OsString {
    #[inline]
    fn eq(&self, other: &&OsStr) -> bool {
        &**self == *other
    }
}

impl PartialEq<OsString> for &OsStr {
    #[inline]
    fn eq(&self, other: &OsString) -> bool {
        *self == &**other
    }
}

impl PartialOrd<str> for OsString {
    #[inline]
    fn partial_cmp(&self, other: &str) -> Option<cmp::Ordering> {
        (**self).partial_cmp(other)
    }
}

impl PartialOrd<OsStr> for OsString {
    #[inline]
    fn partial_cmp(&self, other: &OsStr) -> Option<cmp::Ordering> {
        (**self).partial_cmp(other)
    }
}

impl PartialOrd<OsString> for OsStr {
    #[inline]
    fn partial_cmp(&self, other: &OsString) -> Option<cmp::Ordering> {
        self.partial_cmp(&**other)
    }
}

impl PartialOrd<&OsStr> for OsString {
    #[inline]
    fn partial_cmp(&self, other: &&OsStr) -> Option<cmp::Ordering> {
        (**self).partial_cmp(*other)
    }
}

impl PartialOrd<OsString> for &OsStr {
    #[inline]
    fn partial_cmp(&self, other: &OsString) -> Option<cmp::Ordering> {
        (*self).partial_cmp(&**other)
    }
}

impl From<&OsStr> for Box<OsStr> {
    #[inline]
    fn from(s: &OsStr) -> Box<OsStr> {
        s.to_os_string().into_boxed_os_str()
    }
}

impl From<&mut OsStr> for Box<OsStr> {
    #[inline]
    fn from(s: &mut OsStr) -> Box<OsStr> {
        Self::from(&*s)
    }
}

impl From<Cow<'_, OsStr>> for Box<OsStr> {
    #[inline]
    fn from(cow: Cow<'_, OsStr>) -> Box<OsStr> {
        match cow {
            Cow::Borrowed(s) => Box::from(s),
            Cow::Owned(s) => Box::from(s),
        }
    }
}

impl From<Box<OsStr>> for OsString {
    #[inline]
    fn from(boxed: Box<OsStr>) -> OsString {
        boxed.into_os_string()
    }
}

impl From<OsString> for Box<OsStr> {
    #[inline]
    fn from(s: OsString) -> Box<OsStr> {
        s.into_boxed_os_str()
    }
}

impl Clone for Box<OsStr> {
    #[inline]
    fn clone(&self) -> Self {
        self.to_os_string().into_boxed_os_str()
    }
}

impl From<OsString> for Arc<OsStr> {
    #[inline]
    fn from(s: OsString) -> Arc<OsStr> {
        let arc = s.inner.to_arc();
        unsafe { Arc::from_raw(Arc::into_raw(arc) as *const OsStr) }
    }
}

impl From<&OsStr> for Arc<OsStr> {
    #[inline]
    fn from(s: &OsStr) -> Arc<OsStr> {
        let arc = s.inner.to_arc();
        unsafe { Arc::from_raw(Arc::into_raw(arc) as *const OsStr) }
    }
}

impl From<&mut OsStr> for Arc<OsStr> {
    #[inline]
    fn from(s: &mut OsStr) -> Arc<OsStr> {
        Arc::from(&*s)
    }
}

impl From<OsString> for Rc<OsStr> {
    #[inline]
    fn from(s: OsString) -> Rc<OsStr> {
        let rc = s.inner.to_rc();
        unsafe { Rc::from_raw(Rc::into_raw(rc) as *const OsStr) }
    }
}

impl From<&OsStr> for Rc<OsStr> {
    #[inline]
    fn from(s: &OsStr) -> Rc<OsStr> {
        let rc = s.inner.to_rc();
        unsafe { Rc::from_raw(Rc::into_raw(rc) as *const OsStr) }
    }
}

impl From<&mut OsStr> for Rc<OsStr> {
    #[inline]
    fn from(s: &mut OsStr) -> Rc<OsStr> {
        Rc::from(&*s)
    }
}

impl<'a> From<OsString> for Cow<'a, OsStr> {
    #[inline]
    fn from(s: OsString) -> Cow<'a, OsStr> {
        Cow::Owned(s)
    }
}

impl<'a> From<&'a OsStr> for Cow<'a, OsStr> {
    #[inline]
    fn from(s: &'a OsStr) -> Cow<'a, OsStr> {
        Cow::Borrowed(s)
    }
}

impl<'a> From<&'a OsString> for Cow<'a, OsStr> {
    #[inline]
    fn from(s: &'a OsString) -> Cow<'a, OsStr> {
        Cow::Borrowed(s.as_os_str())
    }
}

impl<'a> From<Cow<'a, OsStr>> for OsString {
    #[inline]
    fn from(s: Cow<'a, OsStr>) -> Self {
        s.into_owned()
    }
}

impl<'a> TryFrom<&'a OsStr> for &'a str {
    type Error = core::str::Utf8Error;

    #[inline]
    fn try_from(value: &'a OsStr) -> Result<Self, Self::Error> {
        value.inner.to_str()
    }
}

impl Default for Box<OsStr> {
    #[inline]
    fn default() -> Box<OsStr> {
        let rw = Box::into_raw(Slice::empty_box()) as *mut OsStr;
        unsafe { Box::from_raw(rw) }
    }
}

impl Default for &OsStr {
    #[inline]
    fn default() -> Self {
        OsStr::new("")
    }
}

impl PartialEq for OsStr {
    #[inline]
    fn eq(&self, other: &OsStr) -> bool {
        self.as_encoded_bytes().eq(other.as_encoded_bytes())
    }
}

impl PartialEq<str> for OsStr {
    #[inline]
    fn eq(&self, other: &str) -> bool {
        *self == *OsStr::new(other)
    }
}

impl PartialEq<OsStr> for str {
    #[inline]
    fn eq(&self, other: &OsStr) -> bool {
        *other == *OsStr::new(self)
    }
}

impl PartialEq<&str> for OsStr {
    #[inline]
    fn eq(&self, other: &&str) -> bool {
        *self == **other
    }
}

impl PartialEq<OsStr> for &str {
    #[inline]
    fn eq(&self, other: &OsStr) -> bool {
        **self == *other
    }
}

impl Eq for OsStr {}

impl PartialOrd for OsStr {
    #[inline]
    fn partial_cmp(&self, other: &OsStr) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialOrd<str> for OsStr {
    #[inline]
    fn partial_cmp(&self, other: &str) -> Option<cmp::Ordering> {
        self.partial_cmp(OsStr::new(other))
    }
}

impl Ord for OsStr {
    #[inline]
    fn cmp(&self, other: &OsStr) -> cmp::Ordering {
        self.as_encoded_bytes().cmp(other.as_encoded_bytes())
    }
}

impl Hash for OsStr {
    #[inline]
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.as_encoded_bytes().hash(state);
    }
}

impl fmt::Debug for OsStr {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&self.inner, formatter)
    }
}

impl Borrow<OsStr> for OsString {
    #[inline]
    fn borrow(&self) -> &OsStr {
        &self[..]
    }
}

impl ToOwned for OsStr {
    type Owned = OsString;

    #[inline]
    fn to_owned(&self) -> OsString {
        self.to_os_string()
    }

    #[inline]
    fn clone_into(&self, target: &mut OsString) {
        self.inner.clone_into(&mut target.inner);
    }
}

impl AsRef<OsStr> for OsStr {
    #[inline]
    fn as_ref(&self) -> &OsStr {
        self
    }
}

impl AsRef<OsStr> for OsString {
    #[inline]
    fn as_ref(&self) -> &OsStr {
        self
    }
}

impl AsRef<OsStr> for str {
    #[inline]
    fn as_ref(&self) -> &OsStr {
        OsStr::from_inner(Slice::from_str(self))
    }
}

impl AsRef<OsStr> for String {
    #[inline]
    fn as_ref(&self) -> &OsStr {
        (**self).as_ref()
    }
}

impl FromStr for OsString {
    type Err = Infallible;

    #[inline]
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(OsString::from(s))
    }
}

impl Extend<OsString> for OsString {
    #[inline]
    fn extend<T: IntoIterator<Item = OsString>>(&mut self, iter: T) {
        for s in iter {
            self.push(&s);
        }
    }
}

impl<'a> Extend<&'a OsStr> for OsString {
    #[inline]
    fn extend<T: IntoIterator<Item = &'a OsStr>>(&mut self, iter: T) {
        for s in iter {
            self.push(s);
        }
    }
}

impl<'a> Extend<Cow<'a, OsStr>> for OsString {
    #[inline]
    fn extend<T: IntoIterator<Item = Cow<'a, OsStr>>>(&mut self, iter: T) {
        for s in iter {
            self.push(&s);
        }
    }
}

impl FromIterator<OsString> for OsString {
    #[inline]
    fn from_iter<I: IntoIterator<Item = OsString>>(iter: I) -> Self {
        let mut iterator = iter.into_iter();
        match iterator.next() {
            None => OsString::new(),
            Some(mut buf) => {
                buf.extend(iterator);
                buf
            }
        }
    }
}

impl<'a> FromIterator<&'a OsStr> for OsString {
    #[inline]
    fn from_iter<I: IntoIterator<Item = &'a OsStr>>(iter: I) -> Self {
        let mut buf = Self::new();
        for s in iter {
            buf.push(s);
        }
        buf
    }
}

impl<'a> FromIterator<Cow<'a, OsStr>> for OsString {
    #[inline]
    fn from_iter<I: IntoIterator<Item = Cow<'a, OsStr>>>(iter: I) -> Self {
        let mut iterator = iter.into_iter();
        match iterator.next() {
            None => OsString::new(),
            Some(Cow::Owned(mut buf)) => {
                buf.extend(iterator);
                buf
            }
            Some(Cow::Borrowed(buf)) => {
                let mut os_buf = OsString::from(buf);
                os_buf.extend(iterator);
                os_buf
            }
        }
    }
}
