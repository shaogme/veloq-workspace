use core::{
    cell::UnsafeCell as CoreUnsafeCell,
    fmt::{self, Debug, Formatter},
};

#[cfg(feature = "loom")]
use loom::cell::UnsafeCell as LoomCell;

pub use core::cell::{Cell, RefCell, RefMut};

#[repr(transparent)]
pub struct NativeUnsafeCell<T: ?Sized> {
    cell: CoreUnsafeCell<T>,
}

impl<T: ?Sized + Debug> Debug for NativeUnsafeCell<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        unsafe { (*self.cell.get()).fmt(f) }
    }
}

impl<T> NativeUnsafeCell<T> {
    pub const fn new(value: T) -> Self {
        Self {
            cell: CoreUnsafeCell::new(value),
        }
    }

    pub fn into_inner(self) -> T {
        self.cell.into_inner()
    }
}

impl<T: ?Sized> NativeUnsafeCell<T> {
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

#[cfg(feature = "loom")]
#[repr(transparent)]
pub struct LoomUnsafeCell<T: ?Sized> {
    inner: LoomCell<T>,
}

#[cfg(feature = "loom")]
impl<T: ?Sized + Debug> Debug for LoomUnsafeCell<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        self.inner.get().with(|ptr| unsafe { (*ptr).fmt(f) })
    }
}

#[cfg(feature = "loom")]
impl<T> LoomUnsafeCell<T> {
    pub fn new(data: T) -> Self {
        Self {
            inner: LoomCell::new(data),
        }
    }

    pub fn into_inner(self) -> T {
        self.inner.into_inner()
    }
}

#[cfg(feature = "loom")]
impl<T: ?Sized> LoomUnsafeCell<T> {
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

#[cfg(not(feature = "loom"))]
pub type UnsafeCell<T> = NativeUnsafeCell<T>;

#[cfg(feature = "loom")]
pub type UnsafeCell<T> = LoomUnsafeCell<T>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_native_unsafe_cell() {
        let cell = NativeUnsafeCell::new(42);
        unsafe {
            cell.with(|val| {
                assert_eq!(*val, 42);
            });
            cell.with_mut(|val| {
                *val = 100;
            });
            cell.with(|val| {
                assert_eq!(*val, 100);
            });
        }
        assert_eq!(cell.into_inner(), 100);
    }

    #[cfg(feature = "loom")]
    #[test]
    fn test_loom_unsafe_cell() {
        loom::model(|| {
            let cell = LoomUnsafeCell::new(42);
            unsafe {
                cell.with(|val| {
                    assert_eq!(*val, 42);
                });
                cell.with_mut(|val| {
                    *val = 100;
                });
                cell.with(|val| {
                    assert_eq!(*val, 100);
                });
            }
            assert_eq!(cell.into_inner(), 100);
        });
    }

    #[cfg(not(feature = "loom"))]
    #[test]
    fn test_unsafe_cell_alias() {
        let cell = UnsafeCell::new(10);
        unsafe {
            cell.with(|val| {
                assert_eq!(*val, 10);
            });
        }
        assert_eq!(cell.into_inner(), 10);
    }

    #[cfg(feature = "loom")]
    #[test]
    fn test_unsafe_cell_alias() {
        loom::model(|| {
            let cell = UnsafeCell::new(10);
            unsafe {
                cell.with(|val| {
                    assert_eq!(*val, 10);
                });
            }
            assert_eq!(cell.into_inner(), 10);
        });
    }
}
