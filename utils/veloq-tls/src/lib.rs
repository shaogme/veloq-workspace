use std::fmt;
use std::marker::PhantomData;
use std::ptr::NonNull;
use std::sync::OnceLock;

#[cfg(windows)]
use windows_sys::Win32::System::Threading::{
    TLS_OUT_OF_INDEXES, TlsAlloc, TlsFree, TlsGetValue, TlsSetValue,
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
pub struct Tls<T> {
    key: OnceLock<RawKey>,
    _marker: PhantomData<T>,
}

impl<T> Default for Tls<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> Tls<T> {
    /// Creates a new `Tls` instance.
    ///
    /// This should typically be stored in a `static` variable.
    pub const fn new() -> Self {
        Self {
            key: OnceLock::new(),
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
                let key = unsafe { TlsAlloc() };
                if key == TLS_OUT_OF_INDEXES {
                    return Err(TlsError::AllocationFailed);
                }
                key
            }
            #[cfg(unix)]
            {
                let mut key = 0;
                let res = unsafe { pthread_key_create(&mut key, None) };
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
                    TlsFree(new_key);
                }
                #[cfg(unix)]
                unsafe {
                    pthread_key_delete(new_key);
                }
                Ok(*self.key.get().expect("OnceLock should be initialized"))
            }
        }
    }

    /// Gets the pointer stored in TLS for the current thread.
    ///
    /// Returns `None` if no value has been set for this thread or if key allocation fails.
    #[inline(always)]
    pub fn get(&self) -> Option<NonNull<T>> {
        let key = self.get_key().ok()?;
        #[cfg(windows)]
        {
            let ptr = unsafe { TlsGetValue(key) as *mut T };
            NonNull::new(ptr)
        }
        #[cfg(unix)]
        {
            let ptr = unsafe { pthread_getspecific(key) as *mut T };
            NonNull::new(ptr)
        }
    }

    /// Sets the pointer stored in TLS for the current thread.
    #[inline(always)]
    pub fn set(&self, ptr: Option<NonNull<T>>) -> Result<(), TlsError> {
        let key = self.get_key()?;
        let raw_ptr = ptr.map(|p| p.as_ptr()).unwrap_or(std::ptr::null_mut());
        #[cfg(windows)]
        {
            let res = unsafe { TlsSetValue(key, raw_ptr as _) };
            if res == 0 {
                return Err(TlsError::SetFailed(0)); // Windows doesn't easily provide the error code without GetLastError
            }
        }
        #[cfg(unix)]
        {
            let res = unsafe { pthread_setspecific(key, raw_ptr as _) };
            if res != 0 {
                return Err(TlsError::SetFailed(res));
            }
        }
        Ok(())
    }
}

unsafe impl<T> Send for Tls<T> {}
unsafe impl<T> Sync for Tls<T> {}

/// A guard that clears the TLS slot when dropped.
pub struct TlsGuard<'a, T> {
    tls: &'a Tls<T>,
    _marker: PhantomData<T>,
}

impl<'a, T> TlsGuard<'a, T> {
    /// Creates a new `TlsGuard` and sets the TLS value.
    ///
    /// If TLS allocation or setting fails, this returns an error.
    pub fn new(tls: &'a Tls<T>, ptr: NonNull<T>) -> Result<Self, TlsError> {
        tls.set(Some(ptr))?;
        Ok(Self {
            tls,
            _marker: PhantomData,
        })
    }
}

impl<T> Drop for TlsGuard<'_, T> {
    fn drop(&mut self) {
        let _ = self.tls.set(None);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    static TEST_TLS: Tls<i32> = Tls::new();

    #[test]
    fn test_basic_get_set() {
        assert!(TEST_TLS.get().is_none());

        let mut val = 42;
        TEST_TLS.set(Some(NonNull::from(&mut val))).unwrap();
        assert_eq!(unsafe { *TEST_TLS.get().unwrap().as_ptr() }, 42);

        TEST_TLS.set(None).unwrap();
        assert!(TEST_TLS.get().is_none());
    }

    #[test]
    fn test_thread_isolation() {
        let dangling = std::ptr::dangling_mut::<i32>() as usize;
        TEST_TLS
            .set(Some(NonNull::new(std::ptr::dangling_mut::<i32>()).unwrap()))
            .unwrap();

        thread::spawn(move || {
            assert!(TEST_TLS.get().is_none());
            TEST_TLS
                .set(Some(NonNull::new(std::ptr::dangling_mut::<i32>()).unwrap()))
                .unwrap();
            assert_eq!(TEST_TLS.get().unwrap().as_ptr() as usize, dangling);
        })
        .join()
        .unwrap();

        assert_eq!(TEST_TLS.get().unwrap().as_ptr() as usize, dangling);
        TEST_TLS.set(None).unwrap();
    }

    #[test]
    fn test_guard() {
        assert!(TEST_TLS.get().is_none());
        {
            let mut val = 100;
            let _guard = TlsGuard::new(&TEST_TLS, NonNull::from(&mut val)).unwrap();
            assert_eq!(unsafe { *TEST_TLS.get().unwrap().as_ptr() }, 100);
        }
        assert!(TEST_TLS.get().is_none());
    }
}
