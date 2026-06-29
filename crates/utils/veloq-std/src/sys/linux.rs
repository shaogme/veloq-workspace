use crate::{
    ffi::c_void,
    ptr::{null, null_mut},
    sync::atomic::AtomicU32,
    time::Duration,
};
use libc::{FUTEX_PRIVATE_FLAG, FUTEX_WAIT, FUTEX_WAKE, SYS_futex, syscall, timespec};

pub fn wait_on_address(address: &AtomicU32, expected: u32) {
    wait_on_address_timeout(address, expected, None);
}

pub fn wait_on_address_timeout(
    address: &AtomicU32,
    expected: u32,
    timeout: Option<Duration>,
) -> bool {
    let timespec_timeout = timeout.map(|dur| timespec {
        tv_sec: dur.as_secs() as _,
        tv_nsec: dur.subsec_nanos() as _,
    });
    let timeout_ptr = match timespec_timeout {
        Some(ref ts) => ts as *const timespec,
        None => null(),
    };
    unsafe {
        let res = syscall(
            SYS_futex,
            address as *const AtomicU32 as *const c_void as *mut c_void,
            FUTEX_WAIT | FUTEX_PRIVATE_FLAG,
            expected as i32,
            timeout_ptr,
            null_mut::<c_void>(),
            0,
        );
        if res < 0 {
            let err = *libc::__errno_location();
            err == libc::ETIMEDOUT
        } else {
            false
        }
    }
}

pub fn wake_by_address(address: &AtomicU32) {
    unsafe {
        let _ = syscall(
            SYS_futex,
            address as *const AtomicU32 as *const c_void as *mut c_void,
            FUTEX_WAKE | FUTEX_PRIVATE_FLAG,
            1,
        );
    }
}

pub fn wake_all_by_address(address: &AtomicU32) {
    unsafe {
        let _ = syscall(
            SYS_futex,
            address as *const AtomicU32 as *const c_void as *mut c_void,
            FUTEX_WAKE | FUTEX_PRIVATE_FLAG,
            libc::INT_MAX,
        );
    }
}
