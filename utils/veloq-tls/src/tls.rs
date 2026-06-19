use crate::{RawKey, ResetGuard, TlsError, TlsErrorKind, is_sentinel, sentinel_ptr};
use once_cell::sync::OnceCell;
use std::marker::PhantomData;

#[cfg(windows)]
use windows_sys::Win32::System::Threading::{
    FLS_OUT_OF_INDEXES, FlsAlloc, FlsFree, FlsGetValue, FlsSetValue,
};

#[cfg(unix)]
use libc::{pthread_getspecific, pthread_key_create, pthread_key_delete, pthread_setspecific};

/// A high-performance thread-local storage wrapper using platform-native TLS.
///
/// This version supports an initialization closure and access to the value via a closure.
///
/// # Safety and Lifecycle
///
/// Because platform-native TLS destructor behaviors vary and key allocation is constrained by OS resources:
/// - Do not frequently allocate and drop `Tls` instances. It is highly recommended to store them in `static` variables.
/// - When a `Tls` instance is dropped, destructor functions will **not** be triggered automatically for existing values in other threads, which may cause memory leaks.
/// - If a `Tls` instance is dropped prematurely, subsequent accesses from other threads or cleanup upon thread exit may lead to undefined behavior (UB).
/// - You must guarantee that the lifetime of the `Tls` instance is longer than all threads accessing it.
pub struct Tls<T, F = fn() -> T> {
    key: OnceCell<RawKey>,
    init: F,
    marker: PhantomData<T>,
}

impl<T, F: Fn() -> T> Tls<T, F> {
    /// Creates a new `Tls` instance with an initialization closure.
    ///
    /// This should typically be stored in a `static` variable.
    pub const fn new(init: F) -> Self {
        Self {
            key: OnceCell::new(),
            init,
            marker: PhantomData,
        }
    }

    #[inline]
    fn get_key(&self) -> Result<RawKey, TlsErrorKind> {
        self.key
            .get_or_try_init(|| {
                #[cfg(windows)]
                {
                    let key = unsafe { FlsAlloc(Some(tls_destructor::<T>)) };
                    if key == FLS_OUT_OF_INDEXES {
                        return Err(TlsErrorKind::AllocationFailed);
                    }
                    Ok(key)
                }
                #[cfg(unix)]
                {
                    let mut key = 0;
                    let res = unsafe { pthread_key_create(&mut key, Some(tls_destructor::<T>)) };
                    if res != 0 {
                        return Err(TlsErrorKind::AllocationFailed);
                    }
                    Ok(key)
                }
            })
            .copied()
    }

    /// Executes a closure with a reference to the value stored in TLS for the current thread.
    ///
    /// If no value has been set, the initialization closure is called.
    ///
    /// # Panics
    ///
    /// Panics if recursive initialization of the TLS variable is detected for the current thread.
    #[inline(always)]
    pub fn with<R>(&self, f: impl FnOnce(&T) -> R) -> R {
        let key = self.get_key().expect("TLS key allocation failed");
        let raw_ptr = {
            #[cfg(windows)]
            unsafe {
                FlsGetValue(key) as *const T
            }
            #[cfg(unix)]
            unsafe {
                pthread_getspecific(key) as *const T
            }
        };

        if !raw_ptr.is_null() {
            if is_sentinel(raw_ptr) {
                panic!("TLS recursive initialization detected!");
            }
            return f(unsafe { &*raw_ptr });
        }

        // Set sentinel to detect recursive initialization
        let sentinel = sentinel_ptr::<T>();
        #[cfg(windows)]
        unsafe {
            let res = FlsSetValue(key, sentinel as _);
            if res == 0 {
                panic!(
                    "Failed to set TLS sentinel: error code {}",
                    std::io::Error::last_os_error().raw_os_error().unwrap_or(0)
                );
            }
        }
        #[cfg(unix)]
        unsafe {
            let res = pthread_setspecific(key, sentinel as _);
            if res != 0 {
                panic!("Failed to set TLS sentinel: error code {}", res);
            }
        }

        // Use ResetGuard to guarantee sentinel cleanup in case of closure panic or set failure
        let guard = ResetGuard::new(key);

        // Initialize using the closure
        let val = (self.init)();
        let owned_ptr = Box::into_raw(Box::new(val));

        #[cfg(windows)]
        unsafe {
            let res = FlsSetValue(key, owned_ptr as _);
            if res == 0 {
                let _ = Box::from_raw(owned_ptr);
                panic!(
                    "Failed to set TLS value: error code {}",
                    std::io::Error::last_os_error().raw_os_error().unwrap_or(0)
                );
            }
        }
        #[cfg(unix)]
        unsafe {
            let res = pthread_setspecific(key, owned_ptr as _);
            if res != 0 {
                let _ = Box::from_raw(owned_ptr);
                panic!("Failed to set TLS value: error code {}", res);
            }
        }

        guard.cancel();
        f(unsafe { &*owned_ptr })
    }

    /// Executes a closure with a reference to the value stored in TLS for the current thread without initializing it.
    ///
    /// Returns `Err(TlsErrorKind::Uninitialized)` if no value has been set for this thread or if it is currently being initialized.
    #[inline(always)]
    pub fn try_with<R>(&self, f: impl FnOnce(&T) -> R) -> Result<R, TlsErrorKind> {
        let key = self.get_key()?;
        let raw_ptr = {
            #[cfg(windows)]
            unsafe {
                FlsGetValue(key) as *const T
            }
            #[cfg(unix)]
            unsafe {
                pthread_getspecific(key) as *const T
            }
        };

        if raw_ptr.is_null() || is_sentinel(raw_ptr) {
            Err(TlsErrorKind::Uninitialized)
        } else {
            Ok(unsafe { f(&*raw_ptr) })
        }
    }

    /// Sets an owned value into TLS for the current thread.
    ///
    /// If there was a previously stored value, it is returned.
    ///
    /// # Errors
    ///
    /// Returns `Err(TlsError::RecursiveAccess)` if recursive access or modification during replacement is detected.
    #[inline(always)]
    pub fn set_owned(&self, val: impl Into<Box<T>>) -> Result<Option<Box<T>>, TlsError<T>> {
        let val = val.into();
        let key = match self.get_key() {
            Ok(k) => k,
            Err(TlsErrorKind::AllocationFailed) => {
                return Err(TlsError::AllocationFailed { val });
            }
            Err(_) => unreachable!(),
        };
        let old_ptr = {
            #[cfg(windows)]
            unsafe {
                FlsGetValue(key) as *mut T
            }
            #[cfg(unix)]
            unsafe {
                pthread_getspecific(key) as *mut T
            }
        };

        if !old_ptr.is_null() && is_sentinel(old_ptr) {
            return Err(TlsError::RecursiveAccess { val });
        }

        // Guard the newly set sentinel/null state during actual allocation and storage
        let guard = ResetGuard::new(key);
        let owned_ptr = Box::into_raw(val);

        #[cfg(windows)]
        {
            let res = unsafe { FlsSetValue(key, owned_ptr as _) };
            if res == 0 {
                let val = unsafe { Box::from_raw(owned_ptr) };
                return Err(TlsError::SetFailed {
                    code: std::io::Error::last_os_error().raw_os_error().unwrap_or(0),
                    val,
                });
            }
        }
        #[cfg(unix)]
        {
            let res = unsafe { pthread_setspecific(key, owned_ptr as _) };
            if res != 0 {
                let val = unsafe { Box::from_raw(owned_ptr) };
                return Err(TlsError::SetFailed {
                    code: res as i32,
                    val,
                });
            }
        }

        guard.cancel();

        if old_ptr.is_null() {
            Ok(None)
        } else {
            Ok(Some(unsafe { Box::from_raw(old_ptr) }))
        }
    }

    /// Takes the owned value out of the TLS for the current thread, returning it.
    #[inline(always)]
    pub fn take(&self) -> Option<Box<T>> {
        let key = self.get_key().ok()?;
        let old_ptr = {
            #[cfg(windows)]
            unsafe {
                FlsGetValue(key) as *mut T
            }
            #[cfg(unix)]
            unsafe {
                pthread_getspecific(key) as *mut T
            }
        };

        if old_ptr.is_null() || is_sentinel(old_ptr) {
            None
        } else {
            #[cfg(windows)]
            unsafe {
                FlsSetValue(key, std::ptr::null_mut());
            }
            #[cfg(unix)]
            unsafe {
                pthread_setspecific(key, std::ptr::null_mut());
            }

            Some(unsafe { Box::from_raw(old_ptr) })
        }
    }
}

unsafe impl<T: Send, F: Send> Send for Tls<T, F> {}
unsafe impl<T, F: Sync> Sync for Tls<T, F> {}

impl<T, F> Drop for Tls<T, F> {
    fn drop(&mut self) {
        if let Some(&key) = self.key.get() {
            #[cfg(windows)]
            unsafe {
                FlsFree(key);
            }
            #[cfg(unix)]
            unsafe {
                pthread_key_delete(key);
            }
        }
    }
}

#[cfg(unix)]
unsafe extern "C" fn tls_destructor<T>(ptr: *mut libc::c_void) {
    if !ptr.is_null() && !is_sentinel(ptr) {
        unsafe {
            let _ = Box::from_raw(ptr as *mut T);
        }
    }
}

#[cfg(windows)]
unsafe extern "system" fn tls_destructor<T>(ptr: *const std::ffi::c_void) {
    if !ptr.is_null() && !is_sentinel(ptr) {
        unsafe {
            let _ = Box::from_raw(ptr as *mut T);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::veloq_tls;
    use std::thread;

    veloq_tls! {
        static MACRO_TLS_INT: i32 = 100;
        pub static MACRO_TLS_STR: String = "hello_macro".to_string();
    }

    static TEST_TLS: Tls<i32> = Tls::new(|| 42);

    #[test]
    fn test_basic_get_init() {
        TEST_TLS.with(|v| {
            assert_eq!(*v, 42);
        });
    }

    #[test]
    fn test_thread_isolation() {
        thread::spawn(move || {
            TEST_TLS.with(|v| {
                assert_eq!(*v, 42);
            });
        })
        .join()
        .unwrap();

        TEST_TLS.with(|v| {
            assert_eq!(*v, 42);
        });
    }

    #[test]
    #[should_panic(expected = "TLS recursive initialization detected!")]
    fn test_reentrancy_detection() {
        static RECURSIVE_TLS: Tls<i32> = Tls::new(|| RECURSIVE_TLS.with(|x| *x));
        RECURSIVE_TLS.with(|_| {});
    }

    #[test]
    fn test_set_owned_and_try_with() {
        let local_tls: Tls<String> = Tls::new(|| "default".to_string());
        local_tls.with(|s| assert_eq!(s, "default"));

        assert!(local_tls.set_owned("hello".to_string()).unwrap().is_none());
        local_tls.with(|s| assert_eq!(s, "hello"));

        let old = local_tls.set_owned("world".to_string()).unwrap().unwrap();
        assert_eq!(*old, "hello");
        local_tls.with(|s| assert_eq!(s, "world"));

        let taken = local_tls.take().unwrap();
        assert_eq!(*taken, "world");

        assert_eq!(
            local_tls.try_with(|s| s.clone()),
            Err(TlsErrorKind::Uninitialized)
        );
    }

    #[test]
    fn test_set_owned_recursive_error() {
        static REC_TLS: Tls<i32> = Tls::new(|| {
            let res = REC_TLS.set_owned(100);
            let err = res.unwrap_err();
            assert_eq!(err.kind(), TlsErrorKind::RecursiveAccess);
            assert_eq!(*err.into_val(), 100);
            42
        });
        REC_TLS.with(|_| {});
    }

    #[test]
    fn test_veloq_tls_macro() {
        MACRO_TLS_INT.with(|v| {
            assert_eq!(*v, 100);
        });
        MACRO_TLS_STR.with(|v| {
            assert_eq!(v, "hello_macro");
        });
    }
}
