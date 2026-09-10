use core::panic::{RefUnwindSafe, UnwindSafe};

use crate::{
    cell::NativeUnsafeCell,
    convert::Infallible,
    fmt,
    marker::PhantomData,
    mem::{self, MaybeUninit},
    sync::once::{NativeOnce, OnceExclusiveState},
};

#[cfg(feature = "loom")]
use crate::{cell::LoomUnsafeCell, sync::once::LoomOnce};

macro_rules! impl_once_lock {
    (
        $(#[$meta:meta])*
        struct $name:ident,
        once: $once_ty:ident,
        cell: $cell_ty:ident,
        $(const_new: $is_const:ident)?
    ) => {
        $(#[$meta])*
        pub struct $name<T> {
            once: $once_ty,
            value: $cell_ty<MaybeUninit<T>>,
            _marker: PhantomData<T>,
        }

        impl<T> $name<T> {
            $(impl_once_lock!(@new_fn $is_const, $once_ty, $cell_ty);)?

            #[inline]
            pub fn get(&self) -> Option<&T> {
                if self.initialized() {
                    Some(unsafe { self.get_unchecked() })
                } else {
                    None
                }
            }

            #[inline]
            pub fn get_mut(&mut self) -> Option<&mut T> {
                if self.initialized_mut() {
                    Some(unsafe { self.get_unchecked_mut() })
                } else {
                    None
                }
            }

            #[inline]
            pub fn wait(&self) -> &T {
                self.once.wait_force();
                unsafe { self.get_unchecked() }
            }

            #[inline]
            pub fn set(&self, value: T) -> Result<(), T> {
                match self.try_insert(value) {
                    Ok(_) => Ok(()),
                    Err((_, value)) => Err(value),
                }
            }

            #[inline]
            pub fn try_insert(&self, value: T) -> Result<&T, (&T, T)> {
                let mut value = Some(value);
                let res = self.get_or_init(|| value.take().unwrap());
                match value {
                    None => Ok(res),
                    Some(value) => Err((res, value)),
                }
            }

            #[inline]
            pub fn get_or_init<F>(&self, f: F) -> &T
            where
                F: FnOnce() -> T,
            {
                match self.get_or_try_init(|| Ok::<T, Infallible>(f())) {
                    Ok(val) => val,
                    Err(e) => match e {},
                }
            }

            #[inline]
            pub fn get_mut_or_init<F>(&mut self, f: F) -> &mut T
            where
                F: FnOnce() -> T,
            {
                match self.get_mut_or_try_init(|| Ok::<T, Infallible>(f())) {
                    Ok(val) => val,
                    Err(e) => match e {},
                }
            }

            #[inline]
            pub fn get_or_try_init<F, E>(&self, f: F) -> Result<&T, E>
            where
                F: FnOnce() -> Result<T, E>,
            {
                if let Some(value) = self.get() {
                    return Ok(value);
                }
                self.initialize(f)?;
                Ok(unsafe { self.get_unchecked() })
            }

            #[inline]
            pub fn get_mut_or_try_init<F, E>(&mut self, f: F) -> Result<&mut T, E>
            where
                F: FnOnce() -> Result<T, E>,
            {
                if self.get_mut().is_none() {
                    self.initialize(f)?;
                }
                Ok(unsafe { self.get_unchecked_mut() })
            }

            #[inline]
            pub fn into_inner(mut self) -> Option<T> {
                self.take()
            }

            #[inline]
            pub fn take(&mut self) -> Option<T> {
                if self.initialized_mut() {
                    self.once = $once_ty::new();
                    unsafe { Some(self.value.with_mut(|val| val.assume_init_read())) }
                } else {
                    None
                }
            }

            #[inline]
            fn initialized(&self) -> bool {
                self.once.is_completed()
            }

            #[inline]
            fn initialized_mut(&mut self) -> bool {
                let state = self.once.state();
                matches!(state, OnceExclusiveState::Complete)
            }

            #[cold]
            fn initialize<F, E>(&self, f: F) -> Result<(), E>
            where
                F: FnOnce() -> Result<T, E>,
            {
                let mut res: Result<(), E> = Ok(());
                let slot = &self.value;

                self.once.call_once_force(|p| match f() {
                    Ok(value) => {
                        unsafe {
                            slot.with_mut(|val| {
                                val.write(value);
                            })
                        };
                    }
                    Err(e) => {
                        res = Err(e);
                        p.poison();
                    }
                });
                res
            }

            #[inline]
            unsafe fn get_unchecked(&self) -> &T {
                debug_assert!(self.initialized());
                unsafe {
                    self.value.with(|val| {
                        let r = val.assume_init_ref();
                        mem::transmute::<&T, &T>(r)
                    })
                }
            }

            #[inline]
            unsafe fn get_unchecked_mut(&mut self) -> &mut T {
                debug_assert!(self.initialized_mut());
                unsafe {
                    self.value.with_mut(|val| {
                        let r = val.assume_init_mut();
                        mem::transmute::<&mut T, &mut T>(r)
                    })
                }
            }
        }

        unsafe impl<T: Sync + Send> Sync for $name<T> {}
        unsafe impl<T: Send> Send for $name<T> {}

        impl<T: RefUnwindSafe + UnwindSafe> RefUnwindSafe for $name<T> {}
        impl<T: UnwindSafe> UnwindSafe for $name<T> {}

        impl<T> Default for $name<T> {
            #[inline]
            fn default() -> Self {
                Self::new()
            }
        }

        impl<T: fmt::Debug> fmt::Debug for $name<T> {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                let mut d = f.debug_tuple(stringify!($name));
                match self.get() {
                    Some(v) => d.field(v),
                    None => d.field(&format_args!("<uninit>")),
                };
                d.finish()
            }
        }

        impl<T: Clone> Clone for $name<T> {
            #[inline]
            fn clone(&self) -> Self {
                let cell = Self::new();
                if let Some(value) = self.get() {
                    match cell.set(value.clone()) {
                        Ok(()) => (),
                        Err(_) => unreachable!(),
                    }
                }
                cell
            }
        }

        impl<T> From<T> for $name<T> {
            #[inline]
            fn from(value: T) -> Self {
                let cell = Self::new();
                match cell.set(value) {
                    Ok(()) => cell,
                    Err(_) => unreachable!(),
                }
            }
        }

        impl<T: PartialEq> PartialEq for $name<T> {
            #[inline]
            fn eq(&self, other: &Self) -> bool {
                self.get() == other.get()
            }
        }

        impl<T: Eq> Eq for $name<T> {}

        impl<T> Drop for $name<T> {
            #[inline]
            fn drop(&mut self) {
                if self.initialized_mut() {
                    unsafe { self.value.with_mut(|val| val.assume_init_drop()) };
                }
            }
        }
    };

    (@new_fn const, $once_ty:ident, $cell_ty:ident) => {
        #[inline]
        #[must_use]
        pub const fn new() -> Self {
            Self {
                once: $once_ty::new(),
                value: $cell_ty::new(MaybeUninit::uninit()),
                _marker: PhantomData,
            }
        }
    };

    (@new_fn non_const, $once_ty:ident, $cell_ty:ident) => {
        #[inline]
        #[must_use]
        #[track_caller]
        pub fn new() -> Self {
            Self {
                once: $once_ty::new(),
                value: $cell_ty::new(MaybeUninit::uninit()),
                _marker: PhantomData,
            }
        }
    };
}

impl_once_lock!(
    struct NativeOnceLock,
    once: NativeOnce,
    cell: NativeUnsafeCell,
    const_new: const
);

#[cfg(feature = "loom")]
impl_once_lock!(
    struct LoomOnceLock,
    once: LoomOnce,
    cell: LoomUnsafeCell,
    const_new: non_const
);

#[cfg(not(feature = "loom"))]
pub type OnceLock<T> = NativeOnceLock<T>;

#[cfg(feature = "loom")]
pub type OnceLock<T> = LoomOnceLock<T>;
