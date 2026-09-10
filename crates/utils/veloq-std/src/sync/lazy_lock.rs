use core::panic::{RefUnwindSafe, UnwindSafe};

use crate::{
    cell::NativeUnsafeCell,
    fmt,
    marker::PhantomData,
    mem::{self, ManuallyDrop},
    ops::{Deref, DerefMut},
    ptr,
    sync::once::{NativeOnce, OnceExclusiveState},
};

#[cfg(feature = "loom")]
use crate::{cell::LoomUnsafeCell, sync::once::LoomOnce};

union Data<T, F> {
    value: ManuallyDrop<T>,
    f: ManuallyDrop<F>,
}

macro_rules! impl_lazy_lock {
    (
        struct $name:ident,
        once: $once_ty:ident,
        cell: $cell_ty:ident,
        new: $new_kind:ident
    ) => {
        pub struct $name<T, F = fn() -> T> {
            once: $once_ty,
            data: $cell_ty<Data<T, F>>,
            _marker: PhantomData<T>,
        }

        impl<T, F: FnOnce() -> T> $name<T, F> {
            impl_lazy_lock!(@new_fn $new_kind, $once_ty, $cell_ty);

            #[inline]
            pub fn force(this: &Self) -> &T {
                this.once.call_once_force(|state| {
                    if state.is_poisoned() {
                        panic_poisoned();
                    }

                    let f = unsafe {
                        this.data.with_mut(|data| ManuallyDrop::take(&mut data.f))
                    };
                    let value = f();
                    unsafe {
                        this.data.with_mut(|data| {
                            data.value = ManuallyDrop::new(value);
                        });
                    }
                });

                unsafe { this.get_unchecked() }
            }

            #[inline]
            pub fn force_mut(this: &mut Self) -> &mut T {
                Self::force(&*this);
                unsafe { this.get_unchecked_mut() }
            }
        }

        impl<T, F> $name<T, F> {
            #[inline]
            pub fn get(this: &Self) -> Option<&T> {
                if this.once.is_completed() {
                    Some(unsafe { this.get_unchecked() })
                } else {
                    None
                }
            }

            #[inline]
            pub fn get_mut(this: &mut Self) -> Option<&mut T> {
                if matches!(this.once.state(), OnceExclusiveState::Complete) {
                    Some(unsafe { this.get_unchecked_mut() })
                } else {
                    None
                }
            }

            #[inline]
            pub fn into_inner(mut this: Self) -> Result<T, F> {
                let state = this.once.state();
                if matches!(state, OnceExclusiveState::Poisoned) {
                    panic_poisoned();
                }

                let this = ManuallyDrop::new(this);
                let data = unsafe { ptr::read(&this.data) }.into_inner();
                match state {
                    OnceExclusiveState::Incomplete => {
                        Err(unsafe { ManuallyDrop::into_inner(data.f) })
                    }
                    OnceExclusiveState::Complete => {
                        Ok(unsafe { ManuallyDrop::into_inner(data.value) })
                    }
                    OnceExclusiveState::Poisoned => unreachable!(),
                }
            }

            #[inline]
            unsafe fn get_unchecked(&self) -> &T {
                unsafe {
                    self.data.with(|data| {
                        let value = &data.value;
                        mem::transmute::<&ManuallyDrop<T>, &T>(value)
                    })
                }
            }

            #[inline]
            unsafe fn get_unchecked_mut(&mut self) -> &mut T {
                unsafe {
                    self.data.with_mut(|data| {
                        let value = &mut data.value;
                        mem::transmute::<&mut ManuallyDrop<T>, &mut T>(value)
                    })
                }
            }
        }

        unsafe impl<T: Send, F: Send> Send for $name<T, F> {}
        unsafe impl<T: Sync + Send, F: Send> Sync for $name<T, F> {}

        impl<T: RefUnwindSafe + UnwindSafe, F: UnwindSafe> RefUnwindSafe for $name<T, F> {}
        impl<T: UnwindSafe, F: UnwindSafe> UnwindSafe for $name<T, F> {}

        impl<T: Default> Default for $name<T> {
            #[inline]
            fn default() -> Self {
                Self::new(T::default)
            }
        }

        impl<T: fmt::Debug, F> fmt::Debug for $name<T, F> {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                let mut d = f.debug_tuple("LazyLock");
                match Self::get(self) {
                    Some(value) => d.field(value),
                    None => d.field(&format_args!("<uninit>")),
                };
                d.finish()
            }
        }

        impl<T, F> From<T> for $name<T, F> {
            #[inline]
            fn from(value: T) -> Self {
                Self {
                    once: $once_ty::new_complete(),
                    data: $cell_ty::new(Data {
                        value: ManuallyDrop::new(value),
                    }),
                    _marker: PhantomData,
                }
            }
        }

        impl<T, F> Drop for $name<T, F> {
            #[inline]
            fn drop(&mut self) {
                match self.once.state() {
                    OnceExclusiveState::Incomplete => unsafe {
                        self.data.with_mut(|data| ManuallyDrop::drop(&mut data.f));
                    },
                    OnceExclusiveState::Complete => unsafe {
                        self.data
                            .with_mut(|data| ManuallyDrop::drop(&mut data.value));
                    },
                    OnceExclusiveState::Poisoned => {}
                }
            }
        }

        impl<T, F: FnOnce() -> T> Deref for $name<T, F> {
            type Target = T;

            #[inline]
            fn deref(&self) -> &Self::Target {
                Self::force(self)
            }
        }

        impl<T, F: FnOnce() -> T> DerefMut for $name<T, F> {
            #[inline]
            fn deref_mut(&mut self) -> &mut Self::Target {
                Self::force_mut(self)
            }
        }
    };

    (@new_fn const, $once_ty:ident, $cell_ty:ident) => {
        #[inline]
        #[must_use]
        pub const fn new(f: F) -> Self {
            Self {
                once: $once_ty::new(),
                data: $cell_ty::new(Data {
                    f: ManuallyDrop::new(f),
                }),
                _marker: PhantomData,
            }
        }
    };

    (@new_fn non_const, $once_ty:ident, $cell_ty:ident) => {
        #[inline]
        #[must_use]
        #[track_caller]
        pub fn new(f: F) -> Self {
            Self {
                once: $once_ty::new(),
                data: $cell_ty::new(Data {
                    f: ManuallyDrop::new(f),
                }),
                _marker: PhantomData,
            }
        }
    };
}

impl_lazy_lock!(
    struct NativeLazyLock,
    once: NativeOnce,
    cell: NativeUnsafeCell,
    new: const
);

#[cfg(feature = "loom")]
impl_lazy_lock!(
    struct LoomLazyLock,
    once: LoomOnce,
    cell: LoomUnsafeCell,
    new: non_const
);

#[cfg(not(feature = "loom"))]
pub type LazyLock<T, F = fn() -> T> = NativeLazyLock<T, F>;

#[cfg(feature = "loom")]
pub type LazyLock<T, F = fn() -> T> = LoomLazyLock<T, F>;

#[cold]
#[inline(never)]
fn panic_poisoned() -> ! {
    panic!("LazyLock instance has previously been poisoned")
}
