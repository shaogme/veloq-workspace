use once_cell::sync::OnceCell;
use std::fmt;
use std::marker::PhantomData;

#[cfg(windows)]
use windows_sys::Win32::System::Threading::{
    FLS_OUT_OF_INDEXES, FlsAlloc, FlsFree, FlsGetValue, FlsSetValue,
};

#[cfg(unix)]
use libc::{pthread_getspecific, pthread_key_create, pthread_key_delete, pthread_setspecific};

#[cfg(windows)]
type RawKey = u32;
#[cfg(unix)]
type RawKey = libc::pthread_key_t;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TlsError {
    AllocationFailed,
    SetFailed(i32),
}

impl fmt::Display for TlsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TlsError::AllocationFailed => write!(f, "Failed to allocate TLS index"),
            TlsError::SetFailed(code) => write!(f, "Failed to set TLS value: error code {}", code),
        }
    }
}

impl std::error::Error for TlsError {}

/// A high-performance thread-local storage wrapper using platform-native TLS.
///
/// This version supports an initialization closure and access to the value via a closure.
pub struct Tls<T, F = fn() -> T> {
    key: OnceCell<RawKey>,
    init: F,
    _marker: PhantomData<T>,
}

impl<T, F: Fn() -> T> Tls<T, F> {
    /// Creates a new `Tls` instance with an initialization closure.
    ///
    /// This should typically be stored in a `static` variable.
    pub const fn new(init: F) -> Self {
        Self {
            key: OnceCell::new(),
            init,
            _marker: PhantomData,
        }
    }

    #[inline]
    fn get_key(&self) -> Result<RawKey, TlsError> {
        self.key
            .get_or_try_init(|| {
                #[cfg(windows)]
                {
                    let key = unsafe { FlsAlloc(Some(tls_destructor::<T>)) };
                    if key == FLS_OUT_OF_INDEXES {
                        return Err(TlsError::AllocationFailed);
                    }
                    Ok(key)
                }
                #[cfg(unix)]
                {
                    let mut key = 0;
                    let res = unsafe { pthread_key_create(&mut key, Some(tls_destructor::<T>)) };
                    if res != 0 {
                        return Err(TlsError::AllocationFailed);
                    }
                    Ok(key)
                }
            })
            .copied()
    }

    /// Executes a closure with a reference to the value stored in TLS for the current thread.
    ///
    /// If no value has been set, the initialization closure is called.
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
            return f(unsafe { &*raw_ptr });
        }

        // Initialize using the closure
        let val = (self.init)();
        let owned_ptr = Box::into_raw(Box::new(val));

        #[cfg(windows)]
        unsafe {
            let res = FlsSetValue(key, owned_ptr as _);
            if res == 0 {
                let _ = Box::from_raw(owned_ptr);
                panic!("Failed to set TLS value");
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

        f(unsafe { &*owned_ptr })
    }

    /// Executes a closure with a reference to the value stored in TLS for the current thread without initializing it.
    ///
    /// Returns `None` if no value has been set for this thread.
    #[inline(always)]
    pub fn try_with<R>(&self, f: impl FnOnce(&T) -> R) -> Option<R> {
        let key = self.get_key().ok()?;
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

        if raw_ptr.is_null() {
            None
        } else {
            Some(unsafe { f(&*raw_ptr) })
        }
    }

    /// Sets an owned value into TLS for the current thread.
    ///
    /// If there was a previously stored value, it will be dropped.
    #[inline(always)]
    pub fn set_owned(&self, val: impl Into<Box<T>>) -> Result<(), TlsError> {
        let key = self.get_key()?;
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

        if !old_ptr.is_null() {
            unsafe {
                let _ = Box::from_raw(old_ptr);
            }
        }

        let owned_ptr = Box::into_raw(val.into());

        #[cfg(windows)]
        {
            let res = unsafe { FlsSetValue(key, owned_ptr as _) };
            if res == 0 {
                unsafe {
                    let _ = Box::from_raw(owned_ptr);
                }
                return Err(TlsError::AllocationFailed);
            }
        }
        #[cfg(unix)]
        {
            let res = unsafe { pthread_setspecific(key, owned_ptr as _) };
            if res != 0 {
                unsafe {
                    let _ = Box::from_raw(owned_ptr);
                }
                return Err(TlsError::AllocationFailed);
            }
        }

        Ok(())
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

        if old_ptr.is_null() {
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

unsafe impl<T, F> Send for Tls<T, F> {}
unsafe impl<T, F> Sync for Tls<T, F> {}

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
    if !ptr.is_null() {
        unsafe {
            let _ = Box::from_raw(ptr as *mut T);
        }
    }
}

#[cfg(windows)]
unsafe extern "system" fn tls_destructor<T>(ptr: *const std::ffi::c_void) {
    if !ptr.is_null() {
        unsafe {
            let _ = Box::from_raw(ptr as *mut T);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

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
}
