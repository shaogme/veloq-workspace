use core::{
    fmt::{self, Write},
    str,
};

use crate::alloc_crate::{
    borrow::Cow, boxed::Box, collections::TryReserveError, rc::Rc, string::String, sync::Arc,
    vec::Vec,
};

#[derive(Hash, PartialEq, Eq, PartialOrd, Ord)]
#[repr(transparent)]
pub struct Buf {
    pub(crate) inner: Vec<u8>,
}

#[derive(PartialEq, Eq, PartialOrd, Ord)]
#[repr(transparent)]
pub struct Slice {
    pub(crate) inner: [u8],
}

impl fmt::Debug for Buf {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self.as_slice(), f)
    }
}

impl fmt::Display for Buf {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self.as_slice(), f)
    }
}

impl fmt::Debug for Slice {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_char('"')?;
        for chunk in self.inner.utf8_chunks() {
            for c in chunk.valid().chars() {
                for ec in c.escape_debug() {
                    f.write_char(ec)?;
                }
            }
            for &b in chunk.invalid() {
                write!(f, "\\x{:02x}", b)?;
            }
        }
        f.write_char('"')
    }
}

impl fmt::Display for Slice {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.to_string_lossy(), f)
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
    pub const fn new() -> Buf {
        Buf { inner: Vec::new() }
    }

    #[inline]
    pub fn into_encoded_bytes(self) -> Vec<u8> {
        self.inner
    }

    #[inline]
    pub unsafe fn from_encoded_bytes_unchecked(s: Vec<u8>) -> Self {
        Buf { inner: s }
    }

    #[inline]
    pub fn into_string(self) -> Result<String, Buf> {
        String::from_utf8(self.inner).map_err(|p| Buf {
            inner: p.into_bytes(),
        })
    }

    #[inline]
    pub const fn from_string(s: String) -> Buf {
        Buf {
            inner: s.into_bytes(),
        }
    }

    #[inline]
    pub fn with_capacity(capacity: usize) -> Buf {
        Buf {
            inner: Vec::with_capacity(capacity),
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
        self.inner.extend_from_slice(&s.inner);
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
        unsafe { &*(self.inner.as_slice() as *const [u8] as *const Slice) }
    }

    #[inline]
    pub fn as_mut_slice(&mut self) -> &mut Slice {
        unsafe { &mut *(self.inner.as_mut_slice() as *mut [u8] as *mut Slice) }
    }

    #[inline]
    pub fn leak<'a>(self) -> &'a mut Slice {
        let leaked = self.inner.leak();
        unsafe { &mut *(leaked as *mut [u8] as *mut Slice) }
    }

    #[inline]
    pub fn into_box(self) -> Box<Slice> {
        let boxed = self.inner.into_boxed_slice();
        unsafe { Box::from_raw(Box::into_raw(boxed) as *mut Slice) }
    }

    #[inline]
    pub fn from_box(boxed: Box<Slice>) -> Buf {
        let inner: Box<[u8]> = unsafe { Box::from_raw(Box::into_raw(boxed) as *mut [u8]) };
        Buf {
            inner: inner.into_vec(),
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
    pub fn as_encoded_bytes(&self) -> &[u8] {
        &self.inner
    }

    #[inline]
    pub unsafe fn from_encoded_bytes_unchecked(s: &[u8]) -> &Slice {
        unsafe { &*(s as *const [u8] as *const Slice) }
    }

    #[track_caller]
    #[inline]
    pub fn check_public_boundary(&self, index: usize) {
        if self.try_check_public_boundary(index).is_none() {
            panic!("byte index {index} is not an OsStr boundary");
        }
    }

    #[track_caller]
    #[inline]
    pub fn try_check_public_boundary(&self, index: usize) -> Option<()> {
        if index == 0 || index == self.inner.len() {
            return Some(());
        }
        if index < self.inner.len()
            && (self.inner[index - 1].is_ascii() || self.inner[index].is_ascii())
        {
            return Some(());
        }

        return slow_path(&self.inner, index);

        #[track_caller]
        #[inline(never)]
        fn slow_path(bytes: &[u8], index: usize) -> Option<()> {
            let (before, after) = bytes.split_at_checked(index)?;

            let after = after.get(..4).unwrap_or(after);
            match str::from_utf8(after) {
                Ok(_) => return Some(()),
                Err(err) if err.valid_up_to() != 0 => return Some(()),
                Err(_) => (),
            }

            for len in 2..=4.min(index) {
                let before = &before[index - len..];
                if str::from_utf8(before).is_ok() {
                    return Some(());
                }
            }

            None
        }
    }

    #[inline]
    pub fn from_str(s: &str) -> &Slice {
        unsafe { Slice::from_encoded_bytes_unchecked(s.as_bytes()) }
    }

    #[inline]
    pub fn to_str(&self) -> Result<&str, str::Utf8Error> {
        str::from_utf8(&self.inner)
    }

    #[inline]
    pub fn to_string_lossy(&self) -> Cow<'_, str> {
        String::from_utf8_lossy(&self.inner)
    }

    #[inline]
    pub fn to_owned(&self) -> Buf {
        Buf {
            inner: self.inner.to_vec(),
        }
    }

    #[inline]
    pub fn clone_into(&self, buf: &mut Buf) {
        self.inner.clone_into(&mut buf.inner);
    }

    #[inline]
    pub fn empty_box() -> Box<Slice> {
        let boxed: Box<[u8]> = Default::default();
        unsafe { Box::from_raw(Box::into_raw(boxed) as *mut Slice) }
    }

    #[inline]
    pub fn to_arc(&self) -> Arc<Slice> {
        let arc: Arc<[u8]> = Arc::from(&self.inner);
        unsafe { Arc::from_raw(Arc::into_raw(arc) as *const Slice) }
    }

    #[inline]
    pub fn to_rc(&self) -> Rc<Slice> {
        let rc: Rc<[u8]> = Rc::from(&self.inner);
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
