use crate::{ffi::c_void, sync::atomic::NativeAtomicU32};
use windows_sys::Win32::{
    Foundation::GetLastError,
    System::Threading::{INFINITE, WaitOnAddress, WakeByAddressAll, WakeByAddressSingle},
};

#[cfg(not(feature = "loom"))]
use crate::time::Duration;

pub fn wait_on_address(address: &NativeAtomicU32, expected: u32) {
    unsafe {
        let _ = WaitOnAddress(
            address.as_ptr() as *mut c_void,
            &expected as *const u32 as *const c_void,
            4,
            INFINITE,
        );
    }
}

pub fn wake_all_by_address(address: &NativeAtomicU32) {
    unsafe {
        WakeByAddressAll(address.as_ptr() as *const c_void);
    }
}

#[cfg(not(feature = "loom"))]
pub fn wait_on_address_timeout(
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

#[cfg(not(feature = "loom"))]
pub fn wake_by_address(address: &NativeAtomicU32) {
    unsafe {
        WakeByAddressSingle(address.as_ptr() as *const c_void);
    }
}
