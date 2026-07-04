pub use core::cell::{Cell, RefCell, RefMut};

#[cfg(not(feature = "loom"))]
#[repr(transparent)]
pub struct UnsafeCell<T: ?Sized> {
    cell: core::cell::UnsafeCell<T>,
}

#[cfg(not(feature = "loom"))]
impl<T: ?Sized + core::fmt::Debug> core::fmt::Debug for UnsafeCell<T> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        unsafe { (*self.cell.get()).fmt(f) }
    }
}

#[cfg(not(feature = "loom"))]
impl<T> UnsafeCell<T> {
    pub const fn new(value: T) -> Self {
        Self {
            cell: core::cell::UnsafeCell::new(value),
        }
    }
}

#[cfg(not(feature = "loom"))]
impl<T: ?Sized> UnsafeCell<T> {
    /// # Safety
    ///
    /// The caller must ensure that there are no other references to the underlying data while the closure is executing.
    pub unsafe fn with_mut<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&mut T) -> R,
    {
        unsafe { f(&mut *self.cell.get()) }
    }

    /// # Safety
    ///
    /// The caller must ensure that there are no mutable references to the underlying data while the closure is executing.
    pub unsafe fn with<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&T) -> R,
    {
        unsafe { f(&*self.cell.get()) }
    }
}

#[cfg(not(feature = "loom"))]
impl<T> UnsafeCell<T> {
    pub fn into_inner(self) -> T {
        self.cell.into_inner()
    }
}

#[cfg(feature = "loom")]
#[repr(transparent)]
pub struct UnsafeCell<T: ?Sized> {
    inner: loom::cell::UnsafeCell<T>,
}

#[cfg(feature = "loom")]
impl<T: ?Sized + core::fmt::Debug> core::fmt::Debug for UnsafeCell<T> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        self.inner.get().with(|ptr| unsafe { (*ptr).fmt(f) })
    }
}

#[cfg(feature = "loom")]
impl<T> UnsafeCell<T> {
    pub fn new(data: T) -> Self {
        Self {
            inner: loom::cell::UnsafeCell::new(data),
        }
    }

    pub fn into_inner(self) -> T {
        self.inner.into_inner()
    }
}

#[cfg(feature = "loom")]
impl<T: ?Sized> UnsafeCell<T> {
    /// # Safety
    ///
    /// The caller must ensure that there are no mutable references to the underlying data while the closure is executing.
    pub unsafe fn with<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&T) -> R,
    {
        self.inner.get().with(|ptr| unsafe { f(&*ptr) })
    }

    /// # Safety
    ///
    /// The caller must ensure that there are no other references to the underlying data while the closure is executing.
    pub unsafe fn with_mut<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&mut T) -> R,
    {
        self.inner.get_mut().with(|ptr| unsafe { f(&mut *ptr) })
    }
}
