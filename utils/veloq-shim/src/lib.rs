#![no_std]

#[cfg(not(feature = "loom"))]
pub mod atomic {
    pub use core::sync::atomic::{
        AtomicBool as CoreAtomicBool, AtomicI8 as CoreAtomicI8, AtomicI16 as CoreAtomicI16,
        AtomicI32 as CoreAtomicI32, AtomicI64 as CoreAtomicI64, AtomicIsize as CoreAtomicIsize,
        AtomicPtr as CoreAtomicPtr, AtomicU8 as CoreAtomicU8, AtomicU16 as CoreAtomicU16,
        AtomicU32 as CoreAtomicU32, AtomicU64 as CoreAtomicU64, AtomicUsize as CoreAtomicUsize,
        Ordering, fence,
    };

    macro_rules! impl_atomic {
        ($name:ident, $inner:ty, $std_name:ident) => {
            #[derive(Debug, Default)]
            #[repr(transparent)]
            pub struct $name {
                inner: $std_name,
            }

            impl From<$inner> for $name {
                fn from(v: $inner) -> Self {
                    Self::new(v)
                }
            }

            impl $name {
                pub const fn new(v: $inner) -> Self {
                    Self {
                        inner: $std_name::new(v),
                    }
                }

                pub fn get_mut(&mut self) -> &mut $inner {
                    self.inner.get_mut()
                }

                pub fn into_inner(self) -> $inner {
                    self.inner.into_inner()
                }

                pub fn load(&self, order: Ordering) -> $inner {
                    self.inner.load(order)
                }

                pub fn store(&self, val: $inner, order: Ordering) {
                    self.inner.store(val, order)
                }

                pub fn swap(&self, val: $inner, order: Ordering) -> $inner {
                    self.inner.swap(val, order)
                }

                pub fn compare_exchange(
                    &self,
                    current: $inner,
                    new: $inner,
                    success: Ordering,
                    failure: Ordering,
                ) -> Result<$inner, $inner> {
                    self.inner.compare_exchange(current, new, success, failure)
                }

                pub fn compare_exchange_weak(
                    &self,
                    current: $inner,
                    new: $inner,
                    success: Ordering,
                    failure: Ordering,
                ) -> Result<$inner, $inner> {
                    self.inner
                        .compare_exchange_weak(current, new, success, failure)
                }

                pub fn with_mut<R, F: FnOnce(&mut $inner) -> R>(&mut self, f: F) -> R {
                    f(self.inner.get_mut())
                }
            }
        };
    }

    macro_rules! impl_atomic_int {
        ($name:ident, $inner:ty, $std_name:ident) => {
            impl_atomic!($name, $inner, $std_name);
            impl $name {
                pub fn fetch_add(&self, val: $inner, order: Ordering) -> $inner {
                    self.inner.fetch_add(val, order)
                }
                pub fn fetch_sub(&self, val: $inner, order: Ordering) -> $inner {
                    self.inner.fetch_sub(val, order)
                }
                pub fn fetch_and(&self, val: $inner, order: Ordering) -> $inner {
                    self.inner.fetch_and(val, order)
                }
                pub fn fetch_nand(&self, val: $inner, order: Ordering) -> $inner {
                    self.inner.fetch_nand(val, order)
                }
                pub fn fetch_or(&self, val: $inner, order: Ordering) -> $inner {
                    self.inner.fetch_or(val, order)
                }
                pub fn fetch_xor(&self, val: $inner, order: Ordering) -> $inner {
                    self.inner.fetch_xor(val, order)
                }
                pub fn fetch_max(&self, val: $inner, order: Ordering) -> $inner {
                    self.inner.fetch_max(val, order)
                }
                pub fn fetch_min(&self, val: $inner, order: Ordering) -> $inner {
                    self.inner.fetch_min(val, order)
                }
            }
        };
    }

    impl_atomic!(AtomicBool, bool, CoreAtomicBool);
    impl AtomicBool {
        pub fn fetch_and(&self, val: bool, order: Ordering) -> bool {
            self.inner.fetch_and(val, order)
        }
        pub fn fetch_nand(&self, val: bool, order: Ordering) -> bool {
            self.inner.fetch_nand(val, order)
        }
        pub fn fetch_or(&self, val: bool, order: Ordering) -> bool {
            self.inner.fetch_or(val, order)
        }
        pub fn fetch_xor(&self, val: bool, order: Ordering) -> bool {
            self.inner.fetch_xor(val, order)
        }
    }

    impl_atomic_int!(AtomicI8, i8, CoreAtomicI8);
    impl_atomic_int!(AtomicU8, u8, CoreAtomicU8);
    impl_atomic_int!(AtomicI16, i16, CoreAtomicI16);
    impl_atomic_int!(AtomicU16, u16, CoreAtomicU16);
    impl_atomic_int!(AtomicI32, i32, CoreAtomicI32);
    impl_atomic_int!(AtomicU32, u32, CoreAtomicU32);
    impl_atomic_int!(AtomicI64, i64, CoreAtomicI64);
    impl_atomic_int!(AtomicU64, u64, CoreAtomicU64);
    impl_atomic_int!(AtomicIsize, isize, CoreAtomicIsize);
    impl_atomic_int!(AtomicUsize, usize, CoreAtomicUsize);

    #[derive(Debug)]
    #[repr(transparent)]
    pub struct AtomicPtr<T> {
        inner: CoreAtomicPtr<T>,
    }

    impl<T> Default for AtomicPtr<T> {
        fn default() -> Self {
            Self::new(core::ptr::null_mut())
        }
    }

    impl<T> From<*mut T> for AtomicPtr<T> {
        fn from(p: *mut T) -> Self {
            Self::new(p)
        }
    }

    impl<T> AtomicPtr<T> {
        pub const fn new(p: *mut T) -> Self {
            Self {
                inner: CoreAtomicPtr::new(p),
            }
        }
        pub fn get_mut(&mut self) -> &mut *mut T {
            self.inner.get_mut()
        }
        pub fn into_inner(self) -> *mut T {
            self.inner.into_inner()
        }
        pub fn load(&self, order: Ordering) -> *mut T {
            self.inner.load(order)
        }
        pub fn store(&self, ptr: *mut T, order: Ordering) {
            self.inner.store(ptr, order)
        }
        pub fn swap(&self, ptr: *mut T, order: Ordering) -> *mut T {
            self.inner.swap(ptr, order)
        }
        pub fn compare_exchange(
            &self,
            current: *mut T,
            new: *mut T,
            success: Ordering,
            failure: Ordering,
        ) -> Result<*mut T, *mut T> {
            self.inner.compare_exchange(current, new, success, failure)
        }
        pub fn compare_exchange_weak(
            &self,
            current: *mut T,
            new: *mut T,
            success: Ordering,
            failure: Ordering,
        ) -> Result<*mut T, *mut T> {
            self.inner
                .compare_exchange_weak(current, new, success, failure)
        }
        pub fn with_mut<R, F: FnOnce(&mut *mut T) -> R>(&mut self, f: F) -> R {
            f(self.inner.get_mut())
        }
    }
}

#[cfg(feature = "loom")]
pub mod atomic {
    pub use loom::sync::atomic::*;
}

#[cfg(feature = "alloc")]
extern crate alloc;

#[cfg(feature = "alloc")]
#[cfg(not(feature = "loom"))]
pub use alloc::sync::Arc;

#[cfg(feature = "loom")]
pub use loom::sync::Arc;

#[cfg(feature = "loom")]
pub use loom::thread;

pub mod cell {
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

        pub fn get(&self) -> *const T {
            self.cell.get() as *const T
        }

        pub fn get_mut(&self) -> *mut T {
            self.cell.get()
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
        pub fn get(&self) -> *const T {
            self.inner.get().with(|ptr| ptr)
        }

        pub fn get_mut(&self) -> *mut T {
            self.inner.get_mut().with(|ptr| ptr)
        }

        pub unsafe fn with<F, R>(&self, f: F) -> R
        where
            F: FnOnce(&T) -> R,
        {
            // SAFETY: Caller guarantees safety related to the reference usage.
            // Loom tracks the immutable access duration of the closure.
            self.inner.get().with(|ptr| unsafe { f(&*ptr) })
        }

        pub unsafe fn with_mut<F, R>(&self, f: F) -> R
        where
            F: FnOnce(&mut T) -> R,
        {
            // SAFETY: Caller guarantees safety related to the reference usage.
            // Loom tracks the mutable access duration of the closure.
            self.inner.get_mut().with(|ptr| unsafe { f(&mut *ptr) })
        }
    }
}
