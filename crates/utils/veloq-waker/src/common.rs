use veloq_std::{
    ptr,
    task::{RawWaker, RawWakerVTable, Waker},
};

pub(crate) const TAG_MASK: usize = 0b11;
pub(crate) const REGISTERED: usize = 0b01;
pub(crate) const WAKING: usize = 0b10;
pub(crate) const REGISTERING: usize = 0b11;

// A const NOOP_VTABLE as Waker::noop vtable cannot be accessed in const context.
static NOOP_VTABLE: RawWakerVTable = RawWakerVTable::new(
    |_| RawWaker::new(ptr::null(), &NOOP_VTABLE),
    |_| (),
    |_| (),
    |_| (),
);
pub(crate) const NOOP_PTR: *mut RawWakerVTable =
    &NOOP_VTABLE as *const RawWakerVTable as *mut RawWakerVTable;

pub(crate) trait TaggedPointerExt {
    fn set(self, tag: usize) -> Self;
    fn unset(self, tag: usize) -> Self;
    fn tag(self) -> usize;
}

impl<T> TaggedPointerExt for *mut T {
    #[inline(always)]
    fn set(self, tag: usize) -> Self {
        (((self as usize) & !TAG_MASK) | tag) as *mut T
    }
    #[inline(always)]
    fn unset(self, tag: usize) -> Self {
        ((self as usize) & !tag) as *mut T
    }
    #[inline(always)]
    fn tag(self) -> usize {
        (self as usize) & TAG_MASK
    }
}

pub(crate) trait WakerExt {
    fn vtable_ptr(&self) -> *mut RawWakerVTable;
}

impl WakerExt for Waker {
    #[inline(always)]
    fn vtable_ptr(&self) -> *mut RawWakerVTable {
        self.vtable() as *const RawWakerVTable as *mut RawWakerVTable
    }
}
