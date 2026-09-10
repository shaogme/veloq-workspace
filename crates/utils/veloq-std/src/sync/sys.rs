#[cfg(any(target_os = "linux", target_os = "android"))]
pub(crate) mod linux;

#[cfg(target_os = "windows")]
pub(crate) mod windows;

#[cfg(all(any(target_os = "linux", target_os = "android"), not(feature = "loom")))]
pub use linux::{wait_on_address, wait_on_address_timeout, wake_by_address};

#[cfg(all(target_os = "windows", not(feature = "loom")))]
pub use windows::{wait_on_address, wait_on_address_timeout, wake_by_address};

pub(crate) mod native {
    use crate::{sync::atomic::NativeAtomicU32, time::Duration};

    #[cfg(any(target_os = "linux", target_os = "android"))]
    pub fn wait_on_address(address: &NativeAtomicU32, expected: u32) {
        super::linux::wait_on_address(address, expected);
    }

    #[cfg(any(target_os = "linux", target_os = "android"))]
    pub fn wait_on_address_timeout(
        address: &NativeAtomicU32,
        expected: u32,
        timeout: Option<Duration>,
    ) -> bool {
        super::linux::wait_on_address_timeout(address, expected, timeout)
    }

    #[cfg(any(target_os = "linux", target_os = "android"))]
    pub fn wake_by_address(address: &NativeAtomicU32) {
        super::linux::wake_by_address(address);
    }

    #[cfg(any(target_os = "linux", target_os = "android"))]
    pub fn wake_all_by_address(address: &NativeAtomicU32) {
        super::linux::wake_all_by_address(address);
    }

    #[cfg(target_os = "windows")]
    pub fn wait_on_address(address: &NativeAtomicU32, expected: u32) {
        super::windows::wait_on_address(address, expected);
    }

    #[cfg(target_os = "windows")]
    pub fn wait_on_address_timeout(
        address: &NativeAtomicU32,
        expected: u32,
        timeout: Option<Duration>,
    ) -> bool {
        super::windows::wait_on_address_timeout(address, expected, timeout)
    }

    #[cfg(target_os = "windows")]
    pub fn wake_by_address(address: &NativeAtomicU32) {
        super::windows::wake_by_address(address);
    }

    #[cfg(target_os = "windows")]
    pub fn wake_all_by_address(address: &NativeAtomicU32) {
        super::windows::wake_all_by_address(address);
    }
}

#[cfg(feature = "loom")]
pub(crate) mod loom;

#[cfg(feature = "loom")]
#[allow(unused_imports)]
pub use loom::{wait_on_address, wait_on_address_timeout, wake_all_by_address, wake_by_address};

#[cfg(feature = "loom")]
pub(crate) mod loom_sys {
    use crate::sync::atomic::LoomAtomicU32;

    pub fn wait_on_address(address: &LoomAtomicU32, expected: u32) {
        super::loom::wait_on_address(address, expected);
    }

    pub fn wake_all_by_address(address: &LoomAtomicU32) {
        super::loom::wake_all_by_address(address);
    }
}
