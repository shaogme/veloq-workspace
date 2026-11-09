#[cfg(feature = "loom")]
pub(crate) mod loom;

#[cfg(not(feature = "loom"))]
use self::native::Platform;

use crate::{sync::atomic, time::Duration};

#[cfg(any(target_os = "linux", target_os = "android"))]
mod linux {
    use crate::{sync::atomic::NativeAtomicU32, time::Duration};
    use veloq_futex::{FutexError, WaitOutcome, wait, wake};

    fn fail(operation: &str, error: FutexError) -> ! {
        panic!("veloq-std {operation} failed: {error}");
    }

    pub(crate) struct Platform;

    impl Platform {
        pub(crate) fn wait_on_address(address: &NativeAtomicU32, expected: u32) {
            let result = unsafe { wait(address.as_ptr() as *const u32, expected, None) };
            if let Err(error) = result {
                fail("futex wait", error);
            }
        }

        pub(crate) fn wake_all_by_address(address: &NativeAtomicU32) {
            let result = unsafe { wake(address.as_ptr() as *const u32, i32::MAX as u32) };
            if let Err(error) = result {
                fail("futex wake_all", error);
            }
        }

        pub(crate) fn wait_on_address_timeout(
            address: &NativeAtomicU32,
            expected: u32,
            timeout: Option<Duration>,
        ) -> bool {
            let result = unsafe { wait(address.as_ptr() as *const u32, expected, timeout) };
            match result {
                Ok(WaitOutcome::Woken) => false,
                Ok(WaitOutcome::TimedOut) => true,
                Err(error) => fail("futex wait with timeout", error),
            }
        }

        pub(crate) fn wake_by_address(address: &NativeAtomicU32) {
            let result = unsafe { wake(address.as_ptr() as *const u32, 1) };
            if let Err(error) = result {
                fail("futex wake_one", error);
            }
        }
    }
}

#[cfg(target_os = "windows")]
mod windows {
    use crate::{ffi::c_void, sync::atomic::NativeAtomicU32, time::Duration};
    use windows_sys::Win32::{
        Foundation::GetLastError,
        System::Threading::{INFINITE, WaitOnAddress, WakeByAddressAll, WakeByAddressSingle},
    };

    pub(crate) struct Platform;

    impl Platform {
        pub(crate) fn wait_on_address(address: &NativeAtomicU32, expected: u32) {
            unsafe {
                let _ = WaitOnAddress(
                    address.as_ptr() as *mut c_void,
                    &expected as *const u32 as *const c_void,
                    4,
                    INFINITE,
                );
            }
        }

        pub(crate) fn wake_all_by_address(address: &NativeAtomicU32) {
            unsafe {
                WakeByAddressAll(address.as_ptr() as *const c_void);
            }
        }

        pub(crate) fn wait_on_address_timeout(
            address: &NativeAtomicU32,
            expected: u32,
            timeout: Option<Duration>,
        ) -> bool {
            let ms = match timeout {
                Some(dur) => {
                    let millis = dur.as_millis();
                    let rounded_millis = if dur.subsec_nanos() % 1_000_000 == 0 {
                        millis
                    } else {
                        millis.saturating_add(1)
                    };
                    if rounded_millis > INFINITE as u128 {
                        INFINITE
                    } else {
                        rounded_millis as u32
                    }
                }
                None => INFINITE,
            };
            unsafe {
                let res = WaitOnAddress(
                    address.as_ptr() as *mut c_void,
                    &expected as *const u32 as *const c_void,
                    4,
                    ms,
                );
                if res == 0 {
                    let err = GetLastError();
                    err == 1460 // ERROR_TIMEOUT
                } else {
                    false
                }
            }
        }

        pub(crate) fn wake_by_address(address: &NativeAtomicU32) {
            unsafe {
                WakeByAddressSingle(address.as_ptr() as *const c_void);
            }
        }
    }
}

pub(crate) mod native {
    #[cfg(any(target_os = "linux", target_os = "android"))]
    pub(crate) use super::linux::Platform;
    #[cfg(target_os = "windows")]
    pub(crate) use super::windows::Platform;
}

#[cfg(feature = "loom")]
pub(crate) mod loom_sys {
    use crate::sync::atomic::LoomAtomicU32;

    pub(crate) fn wait_on_address(address: &LoomAtomicU32, expected: u32) {
        super::loom::wait_on_address(address, expected);
    }

    pub(crate) fn wake_all_by_address(address: &LoomAtomicU32) {
        super::loom::wake_all_by_address(address);
    }
}

pub(crate) struct Wait;

impl Wait {
    #[cfg(not(feature = "loom"))]
    pub(crate) fn wait_on_address(address: &atomic::NativeAtomicU32, expected: u32) {
        Platform::wait_on_address(address, expected);
    }

    #[cfg(feature = "loom")]
    pub(crate) fn wait_on_address(address: &atomic::LoomAtomicU32, expected: u32) {
        loom_sys::wait_on_address(address, expected);
    }

    #[cfg(not(feature = "loom"))]
    pub(crate) fn wait_on_address_timeout(
        address: &atomic::NativeAtomicU32,
        expected: u32,
        timeout: Option<Duration>,
    ) -> bool {
        Platform::wait_on_address_timeout(address, expected, timeout)
    }

    #[cfg(not(feature = "loom"))]
    pub(crate) fn wake_by_address(address: &atomic::NativeAtomicU32) {
        Platform::wake_by_address(address);
    }

    #[cfg(not(feature = "loom"))]
    pub(crate) fn wake_all_by_address(address: &atomic::NativeAtomicU32) {
        Platform::wake_all_by_address(address);
    }

    #[cfg(feature = "loom")]
    pub(crate) fn wake_all_by_address(address: &atomic::LoomAtomicU32) {
        sys::loom_sys::wake_all_by_address(address);
    }
}
