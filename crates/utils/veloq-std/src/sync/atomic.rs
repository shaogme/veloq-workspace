pub use core::sync::atomic::Ordering;

#[cfg(not(feature = "loom"))]
pub use core::sync::atomic::fence;

#[cfg(feature = "loom")]
pub use loom::sync::atomic::fence;

pub use core::sync::atomic::compiler_fence;

mod native_impl {
    use core::sync::atomic::{self, Ordering};

    macro_rules! impl_atomic {
        ($name:ident, $inner:ty, $core_name:ident) => {
            #[derive(Debug, Default)]
            #[repr(transparent)]
            pub struct $name {
                inner: atomic::$core_name,
            }

            impl From<$inner> for $name {
                fn from(v: $inner) -> Self {
                    Self::new(v)
                }
            }

            impl $name {
                pub const fn new(v: $inner) -> Self {
                    Self {
                        inner: atomic::$core_name::new(v),
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

                pub fn fetch_update<F>(
                    &self,
                    set_order: Ordering,
                    fetch_order: Ordering,
                    f: F,
                ) -> Result<$inner, $inner>
                where
                    F: FnMut($inner) -> Option<$inner>,
                {
                    self.inner.fetch_update(set_order, fetch_order, f)
                }

                pub fn as_ptr(&self) -> *mut $inner {
                    self.inner.as_ptr()
                }

                pub fn with_mut<R, F: FnOnce(&mut $inner) -> R>(&mut self, f: F) -> R {
                    f(self.inner.get_mut())
                }
            }
        };
    }

    macro_rules! impl_atomic_int {
        ($name:ident, $inner:ty, $core_name:ident) => {
            impl_atomic!($name, $inner, $core_name);
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

    impl_atomic!(NativeAtomicBool, bool, AtomicBool);
    impl NativeAtomicBool {
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
        pub fn fetch_not(&self, order: Ordering) -> bool {
            self.inner.fetch_not(order)
        }
    }

    impl_atomic_int!(NativeAtomicI8, i8, AtomicI8);
    impl_atomic_int!(NativeAtomicU8, u8, AtomicU8);
    impl_atomic_int!(NativeAtomicI16, i16, AtomicI16);
    impl_atomic_int!(NativeAtomicU16, u16, AtomicU16);
    impl_atomic_int!(NativeAtomicI32, i32, AtomicI32);
    impl_atomic_int!(NativeAtomicU32, u32, AtomicU32);
    impl_atomic_int!(NativeAtomicI64, i64, AtomicI64);
    impl_atomic_int!(NativeAtomicU64, u64, AtomicU64);
    impl_atomic_int!(NativeAtomicIsize, isize, AtomicIsize);
    impl_atomic_int!(NativeAtomicUsize, usize, AtomicUsize);

    #[derive(Debug)]
    #[repr(transparent)]
    pub struct NativeAtomicPtr<T> {
        inner: atomic::AtomicPtr<T>,
    }

    impl<T> Default for NativeAtomicPtr<T> {
        fn default() -> Self {
            Self::new(core::ptr::null_mut())
        }
    }

    impl<T> From<*mut T> for NativeAtomicPtr<T> {
        fn from(p: *mut T) -> Self {
            Self::new(p)
        }
    }

    impl<T> NativeAtomicPtr<T> {
        pub const fn new(p: *mut T) -> Self {
            Self {
                inner: atomic::AtomicPtr::new(p),
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
        pub fn fetch_update<F>(
            &self,
            set_order: Ordering,
            fetch_order: Ordering,
            f: F,
        ) -> Result<*mut T, *mut T>
        where
            F: FnMut(*mut T) -> Option<*mut T>,
        {
            self.inner.fetch_update(set_order, fetch_order, f)
        }
        pub fn as_ptr(&self) -> *mut *mut T {
            self.inner.as_ptr()
        }
        pub fn with_mut<R, F: FnOnce(&mut *mut T) -> R>(&mut self, f: F) -> R {
            f(self.inner.get_mut())
        }
    }
}

pub use native_impl::*;

#[cfg(feature = "loom")]
mod loom_impl {
    use core::sync::atomic::Ordering;
    use loom::sync::atomic as loom_atomic;

    macro_rules! impl_loom_atomic {
        ($name:ident, $inner:ty, $loom_type:ident) => {
            #[derive(Debug, Default)]
            #[repr(transparent)]
            pub struct $name {
                inner: loom_atomic::$loom_type,
            }

            impl From<$inner> for $name {
                #[track_caller]
                fn from(v: $inner) -> Self {
                    Self::new(v)
                }
            }

            impl $name {
                #[track_caller]
                pub fn new(v: $inner) -> Self {
                    Self {
                        inner: loom_atomic::$loom_type::new(v),
                    }
                }

                #[track_caller]
                pub fn into_inner(self) -> $inner {
                    self.inner.into_inner()
                }

                #[track_caller]
                pub fn load(&self, order: Ordering) -> $inner {
                    self.inner.load(order)
                }

                #[track_caller]
                pub fn store(&self, val: $inner, order: Ordering) {
                    self.inner.store(val, order)
                }

                #[track_caller]
                pub fn swap(&self, val: $inner, order: Ordering) -> $inner {
                    self.inner.swap(val, order)
                }

                #[track_caller]
                pub fn compare_exchange(
                    &self,
                    current: $inner,
                    new: $inner,
                    success: Ordering,
                    failure: Ordering,
                ) -> Result<$inner, $inner> {
                    self.inner.compare_exchange(current, new, success, failure)
                }

                #[track_caller]
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

                #[track_caller]
                pub fn fetch_update<F>(
                    &self,
                    set_order: Ordering,
                    fetch_order: Ordering,
                    f: F,
                ) -> Result<$inner, $inner>
                where
                    F: FnMut($inner) -> Option<$inner>,
                {
                    self.inner.fetch_update(set_order, fetch_order, f)
                }
            }
        };
    }

    macro_rules! impl_loom_atomic_int {
        ($name:ident, $inner:ty, $loom_type:ident) => {
            impl_loom_atomic!($name, $inner, $loom_type);
            impl $name {
                #[track_caller]
                pub fn fetch_add(&self, val: $inner, order: Ordering) -> $inner {
                    self.inner.fetch_add(val, order)
                }
                #[track_caller]
                pub fn fetch_sub(&self, val: $inner, order: Ordering) -> $inner {
                    self.inner.fetch_sub(val, order)
                }
                #[track_caller]
                pub fn fetch_and(&self, val: $inner, order: Ordering) -> $inner {
                    self.inner.fetch_and(val, order)
                }
                #[track_caller]
                pub fn fetch_nand(&self, val: $inner, order: Ordering) -> $inner {
                    self.inner.fetch_nand(val, order)
                }
                #[track_caller]
                pub fn fetch_or(&self, val: $inner, order: Ordering) -> $inner {
                    self.inner.fetch_or(val, order)
                }
                #[track_caller]
                pub fn fetch_xor(&self, val: $inner, order: Ordering) -> $inner {
                    self.inner.fetch_xor(val, order)
                }
                #[track_caller]
                pub fn fetch_max(&self, val: $inner, order: Ordering) -> $inner {
                    self.inner.fetch_max(val, order)
                }
                #[track_caller]
                pub fn fetch_min(&self, val: $inner, order: Ordering) -> $inner {
                    self.inner.fetch_min(val, order)
                }
                #[track_caller]
                pub fn with_mut<R, F: FnOnce(&mut $inner) -> R>(&mut self, f: F) -> R {
                    self.inner.with_mut(f)
                }
            }
        };
    }

    impl_loom_atomic!(LoomAtomicBool, bool, AtomicBool);
    impl LoomAtomicBool {
        #[track_caller]
        pub fn fetch_and(&self, val: bool, order: Ordering) -> bool {
            self.inner.fetch_and(val, order)
        }
        #[track_caller]
        pub fn fetch_nand(&self, val: bool, order: Ordering) -> bool {
            self.inner.fetch_nand(val, order)
        }
        #[track_caller]
        pub fn fetch_or(&self, val: bool, order: Ordering) -> bool {
            self.inner.fetch_or(val, order)
        }
        #[track_caller]
        pub fn fetch_xor(&self, val: bool, order: Ordering) -> bool {
            self.inner.fetch_xor(val, order)
        }
        #[track_caller]
        pub fn fetch_not(&self, order: Ordering) -> bool {
            self.inner.fetch_xor(true, order)
        }
        #[track_caller]
        pub fn with_mut<R, F: FnOnce(&mut bool) -> R>(&mut self, f: F) -> R {
            let mut val = unsafe { self.inner.unsync_load() };
            let res = f(&mut val);
            self.inner.store(val, Ordering::SeqCst);
            res
        }
    }

    impl_loom_atomic_int!(LoomAtomicI8, i8, AtomicI8);
    impl_loom_atomic_int!(LoomAtomicU8, u8, AtomicU8);
    impl_loom_atomic_int!(LoomAtomicI16, i16, AtomicI16);
    impl_loom_atomic_int!(LoomAtomicU16, u16, AtomicU16);
    impl_loom_atomic_int!(LoomAtomicI32, i32, AtomicI32);
    impl_loom_atomic_int!(LoomAtomicU32, u32, AtomicU32);
    impl_loom_atomic_int!(LoomAtomicI64, i64, AtomicI64);
    impl_loom_atomic_int!(LoomAtomicU64, u64, AtomicU64);
    impl_loom_atomic_int!(LoomAtomicIsize, isize, AtomicIsize);
    impl_loom_atomic_int!(LoomAtomicUsize, usize, AtomicUsize);

    #[derive(Debug)]
    #[repr(transparent)]
    pub struct LoomAtomicPtr<T> {
        inner: loom_atomic::AtomicPtr<T>,
    }

    impl<T> Default for LoomAtomicPtr<T> {
        fn default() -> Self {
            Self::new(core::ptr::null_mut())
        }
    }

    impl<T> From<*mut T> for LoomAtomicPtr<T> {
        #[track_caller]
        fn from(p: *mut T) -> Self {
            Self::new(p)
        }
    }

    impl<T> LoomAtomicPtr<T> {
        #[track_caller]
        pub fn new(p: *mut T) -> Self {
            Self {
                inner: loom_atomic::AtomicPtr::new(p),
            }
        }
        #[track_caller]
        pub fn into_inner(self) -> *mut T {
            self.inner.into_inner()
        }
        #[track_caller]
        pub fn load(&self, order: Ordering) -> *mut T {
            self.inner.load(order)
        }
        #[track_caller]
        pub fn store(&self, ptr: *mut T, order: Ordering) {
            self.inner.store(ptr, order)
        }
        #[track_caller]
        pub fn swap(&self, ptr: *mut T, order: Ordering) -> *mut T {
            self.inner.swap(ptr, order)
        }
        #[track_caller]
        pub fn compare_exchange(
            &self,
            current: *mut T,
            new: *mut T,
            success: Ordering,
            failure: Ordering,
        ) -> Result<*mut T, *mut T> {
            self.inner.compare_exchange(current, new, success, failure)
        }
        #[track_caller]
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
        #[track_caller]
        pub fn fetch_update<F>(
            &self,
            set_order: Ordering,
            fetch_order: Ordering,
            f: F,
        ) -> Result<*mut T, *mut T>
        where
            F: FnMut(*mut T) -> Option<*mut T>,
        {
            self.inner.fetch_update(set_order, fetch_order, f)
        }
        #[track_caller]
        pub fn with_mut<R, F: FnOnce(&mut *mut T) -> R>(&mut self, f: F) -> R {
            self.inner.with_mut(f)
        }
    }
}

#[cfg(feature = "loom")]
pub use loom_impl::*;

#[cfg(not(feature = "loom"))]
pub type AtomicBool = NativeAtomicBool;
#[cfg(not(feature = "loom"))]
pub type AtomicI8 = NativeAtomicI8;
#[cfg(not(feature = "loom"))]
pub type AtomicI16 = NativeAtomicI16;
#[cfg(not(feature = "loom"))]
pub type AtomicI32 = NativeAtomicI32;
#[cfg(not(feature = "loom"))]
pub type AtomicI64 = NativeAtomicI64;
#[cfg(not(feature = "loom"))]
pub type AtomicIsize = NativeAtomicIsize;
#[cfg(not(feature = "loom"))]
pub type AtomicPtr<T> = NativeAtomicPtr<T>;
#[cfg(not(feature = "loom"))]
pub type AtomicU8 = NativeAtomicU8;
#[cfg(not(feature = "loom"))]
pub type AtomicU16 = NativeAtomicU16;
#[cfg(not(feature = "loom"))]
pub type AtomicU32 = NativeAtomicU32;
#[cfg(not(feature = "loom"))]
pub type AtomicU64 = NativeAtomicU64;
#[cfg(not(feature = "loom"))]
pub type AtomicUsize = NativeAtomicUsize;

#[cfg(feature = "loom")]
pub type AtomicBool = LoomAtomicBool;
#[cfg(feature = "loom")]
pub type AtomicI8 = LoomAtomicI8;
#[cfg(feature = "loom")]
pub type AtomicI16 = LoomAtomicI16;
#[cfg(feature = "loom")]
pub type AtomicI32 = LoomAtomicI32;
#[cfg(feature = "loom")]
pub type AtomicI64 = LoomAtomicI64;
#[cfg(feature = "loom")]
pub type AtomicIsize = LoomAtomicIsize;
#[cfg(feature = "loom")]
pub type AtomicPtr<T> = LoomAtomicPtr<T>;
#[cfg(feature = "loom")]
pub type AtomicU8 = LoomAtomicU8;
#[cfg(feature = "loom")]
pub type AtomicU16 = LoomAtomicU16;
#[cfg(feature = "loom")]
pub type AtomicU32 = LoomAtomicU32;
#[cfg(feature = "loom")]
pub type AtomicU64 = LoomAtomicU64;
#[cfg(feature = "loom")]
pub type AtomicUsize = LoomAtomicUsize;
