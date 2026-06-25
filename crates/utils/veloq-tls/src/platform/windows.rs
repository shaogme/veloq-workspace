use crate::{PlatformKey, TlsErrorKind, is_sentinel};
use alloc::boxed::Box;
use core::{
    ffi::c_void,
    hint::spin_loop,
    sync::atomic::{AtomicU32, Ordering},
};
use windows_sys::Win32::{
    Foundation::GetLastError,
    System::Threading::{FLS_OUT_OF_INDEXES, FlsAlloc, FlsFree, FlsGetValue, FlsSetValue},
};

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) struct Key(u32);

impl PlatformKey for Key {
    #[inline]
    unsafe fn free(self) {
        unsafe {
            FlsFree(self.0);
        }
    }

    #[inline]
    unsafe fn get_value<T>(self) -> *mut T {
        unsafe { FlsGetValue(self.0) as *mut T }
    }

    #[inline]
    unsafe fn set_value<T>(self, ptr: *mut T) -> Result<(), i32> {
        unsafe {
            let res = FlsSetValue(self.0, ptr as _);
            if res == 0 {
                Err(GetLastError() as i32)
            } else {
                Ok(())
            }
        }
    }
}

impl Key {
    #[inline]
    fn alloc<T>() -> Result<Self, TlsErrorKind> {
        unsafe {
            let key = FlsAlloc(Some(tls_destructor::<T>));
            if key == FLS_OUT_OF_INDEXES {
                Err(TlsErrorKind::AllocationFailed)
            } else {
                Ok(Key(key))
            }
        }
    }
}

pub(crate) struct AtomicKey(AtomicU32);

impl AtomicKey {
    pub const fn new() -> Self {
        Self(AtomicU32::new(0))
    }

    #[inline]
    pub fn get<T>(&self) -> Result<Key, TlsErrorKind> {
        let mut val = self.0.load(Ordering::Acquire);
        if val > 1 {
            return Ok(Key(val - 2));
        }

        loop {
            if val == 0 {
                match self
                    .0
                    .compare_exchange_weak(0, 1, Ordering::Acquire, Ordering::Relaxed)
                {
                    Ok(_) => match Key::alloc::<T>() {
                        Ok(k) => {
                            let stored = k.0 + 2;
                            self.0.store(stored, Ordering::Release);
                            return Ok(k);
                        }
                        Err(e) => {
                            self.0.store(0, Ordering::Release);
                            return Err(e);
                        }
                    },
                    Err(current) => {
                        val = current;
                    }
                }
            } else if val > 1 {
                return Ok(Key(val - 2));
            } else {
                spin_loop();
                val = self.0.load(Ordering::Acquire);
            }
        }
    }

    #[inline]
    pub fn take(&mut self) -> Option<Key> {
        let val = *self.0.get_mut();
        if val > 1 { Some(Key(val - 2)) } else { None }
    }
}

unsafe extern "system" fn tls_destructor<T>(ptr: *const c_void) {
    if !ptr.is_null() && !is_sentinel(ptr) {
        unsafe {
            let _ = Box::from_raw(ptr as *mut T);
        }
    }
}
