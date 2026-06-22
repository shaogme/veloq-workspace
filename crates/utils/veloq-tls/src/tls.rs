use crate::{RawKey, ResetGuard, TlsError, TlsErrorKind, is_sentinel, sentinel_ptr};
use alloc::boxed::Box;
use core::{
    hint::spin_loop,
    marker::PhantomData,
    ptr::null_mut,
    sync::atomic::{AtomicUsize, Ordering},
};

#[cfg(windows)]
use windows_sys::Win32::System::Threading::{
    FLS_OUT_OF_INDEXES, FlsAlloc, FlsFree, FlsGetValue, FlsSetValue,
};

#[cfg(windows)]
unsafe extern "system" {
    fn GetLastError() -> u32;
}

#[cfg(windows)]
use core::ffi::c_void;

#[cfg(unix)]
use libc::{
    c_void, pthread_getspecific, pthread_key_create, pthread_key_delete, pthread_setspecific,
};

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
pub struct Tls<T> {
    key: AtomicUsize,
    marker: PhantomData<T>,
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
            key: AtomicUsize::new(0),
            marker: PhantomData,
        }
    }

    #[inline]
    fn get_key(&self) -> Result<RawKey, TlsErrorKind> {
        let mut val = self.key.load(Ordering::Acquire);
        if val > 1 {
            return Ok((val - 2) as RawKey);
        }

        loop {
            match self
                .key
                .compare_exchange_weak(0, 1, Ordering::Acquire, Ordering::Relaxed)
            {
                Ok(_) => {
                    let res = unsafe {
                        #[cfg(windows)]
                        {
                            let key = FlsAlloc(Some(tls_destructor::<T>));
                            if key == FLS_OUT_OF_INDEXES {
                                Err(TlsErrorKind::AllocationFailed)
                            } else {
                                Ok(key)
                            }
                        }
                        #[cfg(unix)]
                        {
                            let mut key = 0;
                            let res = pthread_key_create(&mut key, Some(tls_destructor::<T>));
                            if res != 0 {
                                Err(TlsErrorKind::AllocationFailed)
                            } else {
                                Ok(key)
                            }
                        }
                    };

                    match res {
                        Ok(k) => {
                            let stored = (k as usize) + 2;
                            self.key.store(stored, Ordering::Release);
                            return Ok(k);
                        }
                        Err(e) => {
                            self.key.store(0, Ordering::Release);
                            return Err(e);
                        }
                    }
                }
                Err(current) => {
                    if current > 1 {
                        return Ok((current - 2) as RawKey);
                    }
                    spin_loop();
                    val = self.key.load(Ordering::Acquire);
                    if val > 1 {
                        return Ok((val - 2) as RawKey);
                    }
                }
            }
        }
    }

    /// Helper to retrieve the TLS value pointer, optionally initializing it.
    ///
    /// # Errors
    ///
    /// - Returns `Err(TlsErrorKind::AllocationFailed)` if key allocation fails.
    /// - Returns `Err(TlsErrorKind::RecursiveAccess)` if recursive initialization is detected.
    /// - Returns `Err(TlsErrorKind::Uninitialized)` if no initializer is provided and the value is not set.
    ///
    /// # Panics
    ///
    /// Panics if setting sentinel or TLS value fails.
    #[inline(always)]
    fn get_initialized_ptr(&self) -> Result<*const T, TlsErrorKind> {
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

        if !raw_ptr.is_null() {
            if is_sentinel(raw_ptr) {
                return Err(TlsErrorKind::RecursiveAccess);
            }
            return Ok(raw_ptr);
        }
        Err(TlsErrorKind::Uninitialized)
    }

    /// Helper to retrieve the TLS value pointer, optionally initializing it.
    ///
    /// # Errors
    ///
    /// - Returns `Err(TlsErrorKind::AllocationFailed)` if key allocation fails.
    /// - Returns `Err(TlsErrorKind::RecursiveAccess)` if recursive initialization is detected.
    /// - Returns `Err(TlsErrorKind::Uninitialized)` if no initializer is provided and the value is not set.
    ///
    /// # Panics
    ///
    /// Panics if setting sentinel or TLS value fails.
    #[inline(always)]
    fn get_or_try_init<I>(&self, init: I) -> Result<*const T, TlsErrorKind>
    where
        I: FnOnce() -> T,
    {
        match self.get_initialized_ptr() {
            Ok(raw_ptr) => Ok(raw_ptr),
            Err(TlsErrorKind::Uninitialized) => {
                let key = self.get_key()?;
                // Set sentinel to detect recursive initialization
                let sentinel = sentinel_ptr::<T>();
                #[cfg(windows)]
                unsafe {
                    let res = FlsSetValue(key, sentinel as _);
                    if res == 0 {
                        panic!("Failed to set TLS sentinel: error code {}", GetLastError());
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
                let val = init();
                let owned_ptr = Box::into_raw(Box::new(val));

                #[cfg(windows)]
                unsafe {
                    let res = FlsSetValue(key, owned_ptr as _);
                    if res == 0 {
                        let _ = Box::from_raw(owned_ptr);
                        panic!("Failed to set TLS value: error code {}", GetLastError());
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
                Ok(owned_ptr)
            }
            Err(e) => Err(e),
        }
    }

    /// Executes a closure with a reference to the value stored in TLS for the current thread.
    ///
    /// # Panics
    ///
    /// Panics if the TLS value is uninitialized, or if recursive access/key allocation/set fails.
    pub fn with<R>(&self, f: impl FnOnce(&T) -> R) -> R {
        match self.try_with(f) {
            Ok(r) => r,
            Err(TlsErrorKind::Uninitialized) => {
                panic!("TLS value is uninitialized!");
            }
            Err(TlsErrorKind::RecursiveAccess) => {
                panic!("TLS recursive initialization detected!");
            }
            Err(TlsErrorKind::AllocationFailed) => {
                panic!("TLS key allocation failed");
            }
            Err(TlsErrorKind::SetFailed(code)) => {
                panic!("TLS set value failed with error code: {}", code);
            }
        }
    }

    /// Executes a closure with a reference to the value stored in TLS for the current thread,
    /// initializing it with the provided closure if it has not been set yet.
    ///
    /// # Panics
    ///
    /// Panics if recursive initialization of the TLS variable is detected for the current thread,
    /// or if TLS key allocation/set fails.
    pub fn with_or_init<R>(&self, f: impl FnOnce(&T) -> R, init: impl FnOnce() -> T) -> R {
        match self.try_with_or_init(f, init) {
            Ok(r) => r,
            Err(TlsErrorKind::Uninitialized) => unreachable!(),
            Err(TlsErrorKind::RecursiveAccess) => {
                panic!("TLS recursive initialization detected!");
            }
            Err(TlsErrorKind::AllocationFailed) => {
                panic!("TLS key allocation failed");
            }
            Err(TlsErrorKind::SetFailed(code)) => {
                panic!("TLS set value failed with error code: {}", code);
            }
        }
    }

    /// Executes a closure with a reference to the value stored in TLS for the current thread,
    /// initializing it with the default value of `T` if it has not been set yet.
    ///
    /// # Panics
    ///
    /// Panics if recursive initialization of the TLS variable is detected for the current thread,
    /// or if TLS key allocation/set fails.
    pub fn with_or_default<R>(&self, f: impl FnOnce(&T) -> R) -> R
    where
        T: Default,
    {
        self.with_or_init(f, T::default)
    }

    /// Executes a closure with a reference to the value stored in TLS for the current thread without initializing it.
    ///
    /// Returns `Err(TlsErrorKind::Uninitialized)` if no value has been set for this thread or if it is currently being initialized.
    pub fn try_with<R>(&self, f: impl FnOnce(&T) -> R) -> Result<R, TlsErrorKind> {
        match self.get_initialized_ptr() {
            Ok(raw_ptr) => Ok(unsafe { f(&*raw_ptr) }),
            Err(TlsErrorKind::RecursiveAccess) => Err(TlsErrorKind::Uninitialized),
            Err(e) => Err(e),
        }
    }

    /// Executes a closure with a reference to the value stored in TLS for the current thread,
    /// initializing it with the provided closure if it has not been set yet.
    ///
    /// # Panics
    ///
    /// Panics if recursive initialization of the TLS variable is detected for the current thread,
    /// or if TLS key allocation/set fails.
    pub fn try_with_or_init<R>(
        &self,
        f: impl FnOnce(&T) -> R,
        init: impl FnOnce() -> T,
    ) -> Result<R, TlsErrorKind> {
        self.get_or_try_init(init)
            .map(|raw_ptr| f(unsafe { &*raw_ptr }))
    }

    /// Executes a closure with a reference to the value stored in TLS for the current thread,
    /// initializing it with the default value of `T` if it has not been set yet.
    ///
    /// # Panics
    ///
    /// Panics if recursive initialization of the TLS variable is detected for the current thread,
    /// or if TLS key allocation/set fails.
    pub fn try_with_or_default<R>(&self, f: impl FnOnce(&T) -> R) -> Result<R, TlsErrorKind>
    where
        T: Default,
    {
        self.try_with_or_init(f, T::default)
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
                    code: unsafe { GetLastError() } as i32,
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
                FlsSetValue(key, null_mut());
            }
            #[cfg(unix)]
            unsafe {
                pthread_setspecific(key, null_mut());
            }

            Some(unsafe { Box::from_raw(old_ptr) })
        }
    }
}

unsafe impl<T> Send for Tls<T> {}
unsafe impl<T> Sync for Tls<T> {}

impl<T> Drop for Tls<T> {
    fn drop(&mut self) {
        let val = self.key.load(Ordering::Acquire);
        if val > 1 {
            let key = (val - 2) as RawKey;
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
unsafe extern "C" fn tls_destructor<T>(ptr: *mut c_void) {
    if !ptr.is_null() && !is_sentinel(ptr) {
        unsafe {
            let _ = Box::from_raw(ptr as *mut T);
        }
    }
}

#[cfg(windows)]
unsafe extern "system" fn tls_destructor<T>(ptr: *const c_void) {
    if !ptr.is_null() && !is_sentinel(ptr) {
        unsafe {
            let _ = Box::from_raw(ptr as *mut T);
        }
    }
}

#[cfg(test)]
mod tests {
    extern crate std;
    use super::*;
    use crate::veloq_tls;
    use alloc::string::{String, ToString};
    use std::thread;

    veloq_tls! {
        static MACRO_TLS_INT: i32 = 100;
        pub static MACRO_TLS_STR: String = "hello_macro".to_string();
    }

    static TEST_TLS: Tls<i32> = Tls::new();

    #[test]
    fn test_basic_get_init() {
        TEST_TLS
            .try_with_or_init(
                |v| {
                    assert_eq!(*v, 42);
                },
                || 42,
            )
            .unwrap();
    }

    #[test]
    fn test_thread_isolation() {
        thread::spawn(move || {
            TEST_TLS
                .try_with_or_init(
                    |v| {
                        assert_eq!(*v, 42);
                    },
                    || 42,
                )
                .unwrap();
        })
        .join()
        .unwrap();

        TEST_TLS
            .try_with_or_init(
                |v| {
                    assert_eq!(*v, 42);
                },
                || 42,
            )
            .unwrap();
    }

    #[test]
    #[should_panic(expected = "TLS recursive initialization detected!")]
    fn test_reentrancy_detection() {
        static RECURSIVE_TLS: Tls<i32> = Tls::new();
        RECURSIVE_TLS
            .try_with_or_init(
                |_| {},
                || {
                    RECURSIVE_TLS
                        .try_with_or_init(|x| *x, || 42)
                        .expect("TLS recursive initialization detected!")
                },
            )
            .unwrap();
    }

    #[test]
    fn test_set_owned_and_try_with() {
        let local_tls: Tls<String> = Tls::new();

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
        static REC_TLS: Tls<i32> = Tls::new();
        REC_TLS
            .try_with_or_init(
                |_| {},
                || {
                    let res = REC_TLS.set_owned(100);
                    let err = res.unwrap_err();
                    assert_eq!(err.kind(), TlsErrorKind::RecursiveAccess);
                    assert_eq!(*err.into_val(), 100);
                    42
                },
            )
            .unwrap();
    }

    #[test]
    fn test_veloq_tls_macro() {
        MACRO_TLS_INT
            .try_with_or_init(
                |v| {
                    assert_eq!(*v, 100);
                },
                || 100,
            )
            .unwrap();
        MACRO_TLS_STR
            .try_with_or_init(
                |v| {
                    assert_eq!(v, "hello_macro");
                },
                || "hello_macro".to_string(),
            )
            .unwrap();
    }
}
