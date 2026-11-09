use super::{EncodeWide, Wtf8, Wtf8Buf};
use crate::alloc_crate::{
    borrow::Cow, boxed::Box, collections::TryReserveError, rc::Rc, string::String, sync::Arc,
    vec::Vec,
};
use core::{fmt, str};

#[derive(Hash, PartialEq, Eq, PartialOrd, Ord)]
#[repr(transparent)]
pub struct Buf {
    inner: Wtf8Buf,
}

#[derive(PartialEq, Eq, PartialOrd, Ord)]
#[repr(transparent)]
pub struct Slice {
    inner: Wtf8,
}

impl fmt::Debug for Buf {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self.as_slice(), f)
    }
}

impl fmt::Display for Buf {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.inner, f)
    }
}

impl fmt::Debug for Slice {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&self.inner, f)
    }
}

impl fmt::Display for Slice {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.inner, f)
    }
}

impl Clone for Buf {
    #[inline]
    fn clone(&self) -> Self {
        Buf {
            inner: self.inner.clone(),
        }
    }

    #[inline]
    fn clone_from(&mut self, source: &Self) {
        self.inner.clone_from(&source.inner);
    }
}

impl Buf {
    #[inline]
    pub(crate) fn from_wide(wide: &[u16]) -> Self {
        Self {
            inner: Wtf8Buf::from_wide(wide),
        }
    }

    #[inline]
    pub const fn new() -> Buf {
        Buf {
            inner: Wtf8Buf::new(),
        }
    }

    #[inline]
    pub fn into_encoded_bytes(self) -> Vec<u8> {
        self.inner.into_bytes()
    }

    #[inline]
    pub unsafe fn from_encoded_bytes_unchecked(s: Vec<u8>) -> Self {
        Buf {
            inner: unsafe { Wtf8Buf::from_bytes_unchecked(s) },
        }
    }

    #[inline]
    pub fn into_string(self) -> Result<String, Buf> {
        self.inner.into_string().map_err(|b| Buf { inner: b })
    }

    #[inline]
    pub const fn from_string(s: String) -> Buf {
        Buf {
            inner: Wtf8Buf::from_string(s),
        }
    }

    #[inline]
    pub fn with_capacity(capacity: usize) -> Buf {
        Buf {
            inner: Wtf8Buf::with_capacity(capacity),
        }
    }

    #[inline]
    pub fn clear(&mut self) {
        self.inner.clear();
    }

    #[inline]
    pub fn capacity(&self) -> usize {
        self.inner.capacity()
    }

    #[inline]
    pub fn push_slice(&mut self, s: &Slice) {
        self.inner.push_wtf8(&s.inner);
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

    #[inline]
    pub fn as_slice(&self) -> &Slice {
        unsafe { &*(self.inner.as_slice() as *const Wtf8 as *const Slice) }
    }

    #[inline]
    pub fn as_mut_slice(&mut self) -> &mut Slice {
        unsafe { &mut *(self.inner.as_mut_slice() as *mut Wtf8 as *mut Slice) }
    }

    #[inline]
    pub fn leak<'a>(self) -> &'a mut Slice {
        let leaked = self.inner.leak();
        unsafe { &mut *(leaked as *mut Wtf8 as *mut Slice) }
    }

    #[inline]
    pub fn into_box(self) -> Box<Slice> {
        let boxed = self.inner.into_box();
        unsafe { Box::from_raw(Box::into_raw(boxed) as *mut Slice) }
    }

    #[inline]
    pub fn from_box(boxed: Box<Slice>) -> Buf {
        let inner: Box<Wtf8> = unsafe { Box::from_raw(Box::into_raw(boxed) as *mut Wtf8) };
        Buf {
            inner: Wtf8Buf::from_box(inner),
        }
    }

    #[inline]
    pub fn to_arc(&self) -> Arc<Slice> {
        self.as_slice().to_arc()
    }

    #[inline]
    pub fn to_rc(&self) -> Rc<Slice> {
        self.as_slice().to_rc()
    }

    #[inline]
    pub unsafe fn truncate_unchecked(&mut self, len: usize) {
        self.inner.truncate(len);
    }
}

impl Slice {
    #[inline]
    pub(crate) fn len(&self) -> usize {
        self.inner.len()
    }

    #[inline]
    pub(crate) fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    #[inline]
    pub(crate) fn encode_wide(&self) -> EncodeWide<'_> {
        self.inner.encode_wide()
    }

    #[inline]
    pub fn as_encoded_bytes(&self) -> &[u8] {
        self.inner.as_bytes()
    }

    #[inline]
    pub unsafe fn from_encoded_bytes_unchecked(s: &[u8]) -> &Slice {
        unsafe { &*(Wtf8::from_bytes_unchecked(s) as *const Wtf8 as *const Slice) }
    }

    #[track_caller]
    #[inline]
    pub fn check_public_boundary(&self, index: usize) {
        self.inner.check_utf8_boundary(index);
    }

    #[inline]
    pub fn try_check_public_boundary(&self, index: usize) -> Option<()> {
        self.inner.try_check_utf8_boundary(index).ok()
    }

    #[inline]
    pub fn from_str(s: &str) -> &Slice {
        unsafe { &*(Wtf8::from_str(s) as *const Wtf8 as *const Slice) }
    }

    #[inline]
    pub fn to_str(&self) -> Result<&str, str::Utf8Error> {
        self.inner.as_str()
    }

    #[inline]
    pub fn to_string_lossy(&self) -> Cow<'_, str> {
        self.inner.to_string_lossy()
    }

    #[inline]
    pub fn to_owned(&self) -> Buf {
        Buf {
            inner: self.inner.to_owned(),
        }
    }

    #[inline]
    pub fn clone_into(&self, buf: &mut Buf) {
        self.inner.clone_into(&mut buf.inner);
    }

    #[inline]
    pub fn empty_box() -> Box<Slice> {
        let boxed = Wtf8::empty_box();
        unsafe { Box::from_raw(Box::into_raw(boxed) as *mut Slice) }
    }

    #[inline]
    pub fn to_arc(&self) -> Arc<Slice> {
        let arc = self.inner.to_arc();
        unsafe { Arc::from_raw(Arc::into_raw(arc) as *const Slice) }
    }

    #[inline]
    pub fn to_rc(&self) -> Rc<Slice> {
        let rc = self.inner.to_rc();
        unsafe { Rc::from_raw(Rc::into_raw(rc) as *const Slice) }
    }

    #[inline]
    pub fn make_ascii_lowercase(&mut self) {
        self.inner.make_ascii_lowercase();
    }

    #[inline]
    pub fn make_ascii_uppercase(&mut self) {
        self.inner.make_ascii_uppercase();
    }

    #[inline]
    pub fn to_ascii_lowercase(&self) -> Buf {
        Buf {
            inner: self.inner.to_ascii_lowercase(),
        }
    }

    #[inline]
    pub fn to_ascii_uppercase(&self) -> Buf {
        Buf {
            inner: self.inner.to_ascii_uppercase(),
        }
    }

    #[inline]
    pub fn is_ascii(&self) -> bool {
        self.inner.is_ascii()
    }

    #[inline]
    pub fn eq_ignore_ascii_case(&self, other: &Self) -> bool {
        self.inner.eq_ignore_ascii_case(&other.inner)
    }
}
