use crate::{ffi::c_void, sync::atomic::AtomicU32, time::Duration};
use windows_sys::Win32::{
    Foundation::GetLastError,
    System::Threading::{INFINITE, WaitOnAddress, WakeByAddressAll, WakeByAddressSingle},
};

pub fn wait_on_address(address: &AtomicU32, expected: u32) {
    wait_on_address_timeout(address, expected, None);
}

pub fn wait_on_address_timeout(
    address: &AtomicU32,
    expected: u32,
    timeout: Option<Duration>,
) -> bool {
    let ms = match timeout {
        Some(dur) => {
            if dur.as_millis() > INFINITE as u128 {
                INFINITE
            } else {
                dur.as_millis() as u32
            }
        }
        None => INFINITE,
    };
    unsafe {
        let res = WaitOnAddress(
            address as *const AtomicU32 as *const c_void as *mut c_void,
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

pub fn wake_by_address(address: &AtomicU32) {
    unsafe {
        WakeByAddressSingle(address as *const AtomicU32 as *const c_void);
    }
}

pub fn wake_all_by_address(address: &AtomicU32) {
    unsafe {
        WakeByAddressAll(address as *const AtomicU32 as *const c_void);
    }
}
