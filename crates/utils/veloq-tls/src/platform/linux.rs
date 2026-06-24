use crate::{Platform, TlsErrorKind, is_sentinel};
use alloc::boxed::Box;
use libc::{
    c_void, pthread_getspecific, pthread_key_create, pthread_key_delete, pthread_setspecific,
};

pub(crate) struct PlatformImpl;

impl Platform for PlatformImpl {
    type Key = libc::pthread_key_t;

    #[inline]
    fn alloc_key<T>() -> Result<Self::Key, TlsErrorKind> {
        unsafe {
            let mut key = 0;
            let res = pthread_key_create(&mut key, Some(tls_destructor::<T>));
            if res != 0 {
                Err(TlsErrorKind::AllocationFailed)
            } else {
                Ok(key)
            }
        }
    }

    #[inline]
    unsafe fn free_key(key: Self::Key) {
        unsafe {
            pthread_key_delete(key);
        }
    }

    #[inline]
    unsafe fn get_value<T>(key: Self::Key) -> *mut T {
        unsafe { pthread_getspecific(key) as *mut T }
    }

    #[inline]
    unsafe fn set_value<T>(key: Self::Key, ptr: *mut T) -> Result<(), i32> {
        unsafe {
            let res = pthread_setspecific(key, ptr as _);
            if res != 0 { Err(res as i32) } else { Ok(()) }
        }
    }
}

unsafe extern "C" fn tls_destructor<T>(ptr: *mut c_void) {
    if !ptr.is_null() && !is_sentinel(ptr) {
        unsafe {
            let _ = Box::from_raw(ptr as *mut T);
        }
    }
}
