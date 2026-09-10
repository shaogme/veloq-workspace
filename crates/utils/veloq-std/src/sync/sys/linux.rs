use crate::sync::atomic::NativeAtomicU32;
use veloq_futex::{FutexError, wait, wake};

#[cfg(not(feature = "loom"))]
use crate::time::Duration;
#[cfg(not(feature = "loom"))]
use veloq_futex::WaitOutcome;

fn fail(operation: &str, error: FutexError) -> ! {
    panic!("veloq-std {operation} failed: {error}");
}

pub fn wait_on_address(address: &NativeAtomicU32, expected: u32) {
    let result = unsafe { wait(address.as_ptr() as *const u32, expected, None) };
    if let Err(error) = result {
        fail("futex wait", error);
    }
}

pub fn wake_all_by_address(address: &NativeAtomicU32) {
    let result = unsafe { wake(address.as_ptr() as *const u32, i32::MAX as u32) };
    if let Err(error) = result {
        fail("futex wake_all", error);
    }
}

#[cfg(not(feature = "loom"))]
pub fn wait_on_address_timeout(
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

#[cfg(not(feature = "loom"))]
pub fn wake_by_address(address: &NativeAtomicU32) {
    let result = unsafe { wake(address.as_ptr() as *const u32, 1) };
    if let Err(error) = result {
        fail("futex wake_one", error);
    }
}
