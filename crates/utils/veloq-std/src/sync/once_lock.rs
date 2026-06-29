use crate::{cell, fmt, marker, mem, sync};

use core::{
    convert::Infallible,
    panic::{RefUnwindSafe, UnwindSafe},
};

use cell::UnsafeCell;
use marker::PhantomData;
use mem::MaybeUninit;
use sync::{Once, once::OnceExclusiveState};

pub struct OnceLock<T> {
    once: Once,
    value: UnsafeCell<MaybeUninit<T>>,
    _marker: PhantomData<T>,
}

impl<T> OnceLock<T> {
    #[inline]
    #[must_use]
    pub const fn new() -> OnceLock<T> {
        OnceLock {
            once: Once::new(),
            value: UnsafeCell::new(MaybeUninit::uninit()),
            _marker: PhantomData,
        }
    }

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
            self.once = Once::new();
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
        match state {
            OnceExclusiveState::Complete => true,
            _ => false,
        }
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

unsafe impl<T: Sync + Send> Sync for OnceLock<T> {}
unsafe impl<T: Send> Send for OnceLock<T> {}

impl<T: RefUnwindSafe + UnwindSafe> RefUnwindSafe for OnceLock<T> {}
impl<T: UnwindSafe> UnwindSafe for OnceLock<T> {}

impl<T> Default for OnceLock<T> {
    #[inline]
    fn default() -> OnceLock<T> {
        OnceLock::new()
    }
}

impl<T: fmt::Debug> fmt::Debug for OnceLock<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut d = f.debug_tuple("OnceLock");
        match self.get() {
            Some(v) => d.field(v),
            None => d.field(&format_args!("<uninit>")),
        };
        d.finish()
    }
}

impl<T: Clone> Clone for OnceLock<T> {
    #[inline]
    fn clone(&self) -> OnceLock<T> {
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

impl<T> From<T> for OnceLock<T> {
    #[inline]
    fn from(value: T) -> Self {
        let cell = Self::new();
        match cell.set(value) {
            Ok(()) => cell,
            Err(_) => unreachable!(),
        }
    }
}

impl<T: PartialEq> PartialEq for OnceLock<T> {
    #[inline]
    fn eq(&self, other: &OnceLock<T>) -> bool {
        self.get() == other.get()
    }
}

impl<T: Eq> Eq for OnceLock<T> {}

impl<T> Drop for OnceLock<T> {
    #[inline]
    fn drop(&mut self) {
        if self.initialized_mut() {
            unsafe { self.value.with_mut(|val| val.assume_init_drop()) };
        }
    }
}
