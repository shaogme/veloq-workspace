use crate::{Platform, TlsErrorKind, is_sentinel};
use alloc::boxed::Box;
use core::ffi::c_void;
use windows_sys::Win32::{
    Foundation::GetLastError,
    System::Threading::{FLS_OUT_OF_INDEXES, FlsAlloc, FlsFree, FlsGetValue, FlsSetValue},
};

pub(crate) struct PlatformImpl;

impl Platform for PlatformImpl {
    type Key = u32;

    #[inline]
    fn alloc_key<T>() -> Result<Self::Key, TlsErrorKind> {
        unsafe {
            let key = FlsAlloc(Some(tls_destructor::<T>));
            if key == FLS_OUT_OF_INDEXES {
                Err(TlsErrorKind::AllocationFailed)
            } else {
                Ok(key)
            }
        }
    }

    #[inline]
    unsafe fn free_key(key: Self::Key) {
        unsafe {
            FlsFree(key);
        }
    }

    #[inline]
    unsafe fn get_value<T>(key: Self::Key) -> *mut T {
        unsafe { FlsGetValue(key) as *mut T }
    }

    #[inline]
    unsafe fn set_value<T>(key: Self::Key, ptr: *mut T) -> Result<(), i32> {
        unsafe {
            let res = FlsSetValue(key, ptr as _);
            if res == 0 {
                Err(GetLastError() as i32)
            } else {
                Ok(())
            }
        }
    }
}

unsafe extern "system" fn tls_destructor<T>(ptr: *const c_void) {
    if !ptr.is_null() && !is_sentinel(ptr) {
        unsafe {
            let _ = Box::from_raw(ptr as *mut T);
        }
    }
}
