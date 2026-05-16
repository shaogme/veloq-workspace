use std::fmt;
use std::marker::PhantomData;
use std::ptr::NonNull;
use std::sync::OnceLock;

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

const OWNED_TAG: usize = 1;

/// A high-performance thread-local storage wrapper using platform-native TLS.
///
/// This version supports an initialization closure and direct access to `&T`.
pub struct Tls<T, F = fn() -> T> {
    key: OnceLock<RawKey>,
    init: F,
    _marker: PhantomData<T>,
}

impl<T, F: Fn() -> T> Tls<T, F> {
    /// Creates a new `Tls` instance with an initialization closure.
    ///
    /// This should typically be stored in a `static` variable.
    pub const fn new(init: F) -> Self {
        Self {
            key: OnceLock::new(),
            init,
            _marker: PhantomData,
        }
    }

    #[inline]
    fn get_key(&self) -> Result<RawKey, TlsError> {
        if let Some(key) = self.key.get() {
            return Ok(*key);
        }

        let new_key = {
            #[cfg(windows)]
            {
                let key = unsafe { FlsAlloc(Some(tls_destructor::<T>)) };
                if key == FLS_OUT_OF_INDEXES {
                    return Err(TlsError::AllocationFailed);
                }
                key
            }
            #[cfg(unix)]
            {
                let mut key = 0;
                let res = unsafe { pthread_key_create(&mut key, Some(tls_destructor::<T>)) };
                if res != 0 {
                    return Err(TlsError::AllocationFailed);
                }
                key
            }
        };

        match self.key.set(new_key) {
            Ok(()) => Ok(new_key),
            Err(_) => {
                // Another thread initialized it first, free the redundant key.
                #[cfg(windows)]
                unsafe {
                    FlsFree(new_key);
                }
                #[cfg(unix)]
                unsafe {
                    pthread_key_delete(new_key);
                }
                Ok(*self.key.get().expect("OnceLock should be initialized"))
            }
        }
    }

    /// Gets the value stored in TLS for the current thread.
    ///
    /// If no value has been set, the initialization closure is called.
    #[inline(always)]
    pub fn get(&self) -> &T {
        if let Some(val) = self.try_get() {
            return val;
        }

        let key = self.get_key().expect("TLS key allocation failed");
        // Initialize using the closure
        let val = (self.init)();
        let owned_ptr = Box::into_raw(Box::new(val)) as usize;
        // Tag it as owned
        let tagged_ptr = owned_ptr | OWNED_TAG;

        #[cfg(windows)]
        unsafe {
            FlsSetValue(key, tagged_ptr as _);
        }
        #[cfg(unix)]
        unsafe {
            pthread_getspecific(key, tagged_ptr as _);
        }

        unsafe { &*(owned_ptr as *const T) }
    }

    /// Gets the value stored in TLS for the current thread without initializing it.
    ///
    /// Returns `None` if no value has been set for this thread.
    #[inline(always)]
    pub fn try_get(&self) -> Option<&T> {
        let key = self.get_key().ok()?;
        let raw_ptr = {
            #[cfg(windows)]
            unsafe {
                FlsGetValue(key) as usize
            }
            #[cfg(unix)]
            unsafe {
                pthread_getspecific(key) as usize
            }
        };

        if raw_ptr == 0 {
            None
        } else {
            let actual_ptr = (raw_ptr & !OWNED_TAG) as *const T;
            Some(unsafe { &*actual_ptr })
        }
    }

    /// Sets the pointer stored in TLS for the current thread.
    ///
    /// If a `Some(ptr)` is provided, it is stored as an unowned pointer.
    /// If a previous value was owned by this `Tls` instance, it will be dropped.
    #[inline(always)]
    pub fn set(&self, ptr: Option<NonNull<T>>) -> Result<(), TlsError> {
        let key = self.get_key()?;

        // Check if we need to drop a previous owned value
        let old_ptr = {
            #[cfg(windows)]
            unsafe {
                FlsGetValue(key) as usize
            }
            #[cfg(unix)]
            unsafe {
                pthread_getspecific(key) as usize
            }
        };

        if old_ptr != 0 && (old_ptr & OWNED_TAG) != 0 {
            let actual_old_ptr = (old_ptr & !OWNED_TAG) as *mut T;
            unsafe {
                let _ = Box::from_raw(actual_old_ptr);
            }
        }

        let new_raw_ptr = ptr.map(|p| p.as_ptr() as usize).unwrap_or(0);

        #[cfg(windows)]
        {
            let res = unsafe { FlsSetValue(key, new_raw_ptr as _) };
            if res == 0 {
                return Err(TlsError::SetFailed(0));
            }
        }
        #[cfg(unix)]
        {
            let res = unsafe { pthread_setspecific(key, new_raw_ptr as _) };
            if res != 0 {
                return Err(TlsError::SetFailed(res));
            }
        }
        Ok(())
    }
}

unsafe impl<T, F> Send for Tls<T, F> {}
unsafe impl<T, F> Sync for Tls<T, F> {}

#[cfg(unix)]
unsafe extern "C" fn tls_destructor<T>(ptr: *mut libc::c_void) {
    let raw_ptr = ptr as usize;
    if raw_ptr != 0 && (raw_ptr & OWNED_TAG) != 0 {
        let actual_ptr = (raw_ptr & !OWNED_TAG) as *mut T;
        unsafe {
            let _ = Box::from_raw(actual_ptr);
        }
    }
}

#[cfg(windows)]
unsafe extern "system" fn tls_destructor<T>(ptr: *const std::ffi::c_void) {
    let raw_ptr = ptr as usize;
    if raw_ptr != 0 && (raw_ptr & OWNED_TAG) != 0 {
        let actual_ptr = (raw_ptr & !OWNED_TAG) as *mut T;
        unsafe {
            let _ = Box::from_raw(actual_ptr);
        }
    }
}

/// A guard that clears the TLS slot when dropped.
pub struct TlsGuard<'a, 'b, T, F = fn() -> T>
where
    F: Fn() -> T,
{
    tls: &'a Tls<T, F>,
    _marker: PhantomData<&'b mut T>,
}

impl<'a, 'b, T, F: Fn() -> T> TlsGuard<'a, 'b, T, F> {
    /// Creates a new `TlsGuard` and sets the TLS value.
    pub fn new(tls: &'a Tls<T, F>, ptr: &'b mut T) -> Result<Self, TlsError> {
        tls.set(Some(NonNull::from(ptr)))?;
        Ok(Self {
            tls,
            _marker: PhantomData,
        })
    }
}

impl<T, F: Fn() -> T> Drop for TlsGuard<'_, '_, T, F> {
    fn drop(&mut self) {
        let _ = self.tls.set(None);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    static TEST_TLS: Tls<i32> = Tls::new(|| 42);

    #[test]
    fn test_basic_get_init() {
        assert_eq!(*TEST_TLS.get(), 42);
    }

    #[test]
    fn test_set_override() {
        let mut val = 100;
        TEST_TLS.set(Some(NonNull::from(&mut val))).unwrap();
        assert_eq!(*TEST_TLS.get(), 100);

        TEST_TLS.set(None).unwrap();
        assert_eq!(*TEST_TLS.get(), 42);
    }

    #[test]
    fn test_thread_isolation() {
        thread::spawn(move || {
            assert_eq!(*TEST_TLS.get(), 42);
            let mut val = 200;
            TEST_TLS.set(Some(NonNull::from(&mut val))).unwrap();
            assert_eq!(*TEST_TLS.get(), 200);
        })
        .join()
        .unwrap();

        assert_eq!(*TEST_TLS.get(), 42);
    }

    #[test]
    fn test_guard() {
        {
            let mut val = 1000;
            let _guard = TlsGuard::new(&TEST_TLS, &mut val).unwrap();
            assert_eq!(*TEST_TLS.get(), 1000);
        }
        assert_eq!(*TEST_TLS.get(), 42);
    }
}
