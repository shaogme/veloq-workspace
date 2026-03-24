use crate::Handle;
use core::marker::PhantomData;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RawHandleKind {
    File,
    Socket,
}

pub trait RawHandleMeta: Handle {
    fn kind(self) -> RawHandleKind;
    fn close(self);
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RawHandle<H: Handle> {
    raw: H,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BorrowedRawHandle<'a, H: Handle> {
    raw: RawHandle<H>,
    _marker: PhantomData<&'a RawHandle<H>>,
}

#[derive(Debug, PartialEq, Eq)]
pub struct OwnedRawHandle<H: RawHandleMeta> {
    raw: RawHandle<H>,
}

impl<H: Handle> RawHandle<H> {
    #[inline]
    pub const fn raw(self) -> H {
        self.raw
    }
}

impl<H: RawHandleMeta> RawHandle<H> {
    #[inline]
    pub const fn new(raw: H) -> Self {
        Self { raw }
    }

    #[inline]
    pub fn kind(self) -> RawHandleKind {
        self.raw.kind()
    }

    #[inline]
    pub const fn borrow(&self) -> BorrowedRawHandle<'_, H> {
        BorrowedRawHandle {
            raw: *self,
            _marker: PhantomData,
        }
    }

    #[inline]
    pub fn is_socket(self) -> bool {
        matches!(self.kind(), RawHandleKind::Socket)
    }

    #[inline]
    pub fn is_file(self) -> bool {
        matches!(self.kind(), RawHandleKind::File)
    }
}

impl<'a, H: RawHandleMeta> BorrowedRawHandle<'a, H> {
    #[inline]
    pub const fn raw(self) -> H {
        self.raw.raw()
    }

    #[inline]
    pub fn kind(self) -> RawHandleKind {
        self.raw.kind()
    }

    #[inline]
    pub fn is_socket(self) -> bool {
        self.raw.is_socket()
    }

    #[inline]
    pub fn is_file(self) -> bool {
        self.raw.is_file()
    }
}

impl<H: RawHandleMeta> OwnedRawHandle<H> {
    #[inline]
    pub const fn raw(&self) -> H {
        self.raw.raw()
    }

    /// # Safety
    ///
    /// 调用方必须保证 `raw` 拥有唯一所有权。
    #[inline]
    pub const unsafe fn from_raw_owned(raw: RawHandle<H>) -> Self {
        Self { raw }
    }

    #[inline]
    pub fn into_raw(self) -> RawHandle<H> {
        let this = core::mem::ManuallyDrop::new(self);
        this.raw
    }

    #[inline]
    pub fn kind(&self) -> RawHandleKind {
        self.raw.kind()
    }

    #[inline]
    pub const fn borrow(&self) -> BorrowedRawHandle<'_, H> {
        self.raw.borrow()
    }

    #[inline]
    pub fn is_socket(&self) -> bool {
        self.raw.is_socket()
    }

    #[inline]
    pub fn is_file(&self) -> bool {
        self.raw.is_file()
    }
}

impl<H: RawHandleMeta> Drop for OwnedRawHandle<H> {
    fn drop(&mut self) {
        self.raw.raw().close();
    }
}
