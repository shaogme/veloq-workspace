use crate::{sync::atomic::AtomicU32, time::Duration};

pub fn wait_on_address(address: &AtomicU32, expected: u32) {
    wait_on_address_timeout(address, expected, None);
}

pub fn wait_on_address_timeout(
    address: &AtomicU32,
    expected: u32,
    timeout: Option<Duration>,
) -> bool {
    use core::sync::atomic::Ordering;
    let is_timeout = timeout.is_some();
    let mut limit = if is_timeout { 10 } else { 1000 };

    while address.load(Ordering::Acquire) == expected {
        if is_timeout {
            if limit == 0 {
                return true;
            }
            limit -= 1;
        }
        loom::thread::yield_now();
    }
    false
}

pub fn wake_by_address(_address: &AtomicU32) {}

pub fn wake_all_by_address(_address: &AtomicU32) {}
