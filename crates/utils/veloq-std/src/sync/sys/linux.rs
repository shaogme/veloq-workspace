use crate::{sync::atomic::AtomicU32, time::Duration};
use veloq_futex::{FutexError, WaitOutcome, wait, wake};

fn fail(operation: &str, error: FutexError) -> ! {
    panic!("veloq-std {operation} failed: {error}");
}

pub fn wait_on_address(address: &AtomicU32, expected: u32) {
    let result = unsafe { wait(address as *const AtomicU32 as *const u32, expected, None) };
    if let Err(error) = result {
        fail("futex wait", error);
    }
}

pub fn wait_on_address_timeout(
    address: &AtomicU32,
    expected: u32,
    timeout: Option<Duration>,
) -> bool {
    let result = unsafe { wait(address as *const AtomicU32 as *const u32, expected, timeout) };
    match result {
        Ok(WaitOutcome::Woken) => false,
        Ok(WaitOutcome::TimedOut) => true,
        Err(error) => fail("futex wait with timeout", error),
    }
}

pub fn wake_by_address(address: &AtomicU32) {
    let result = unsafe { wake(address as *const AtomicU32 as *const u32, 1) };
    if let Err(error) = result {
        fail("futex wake_one", error);
    }
}

pub fn wake_all_by_address(address: &AtomicU32) {
    let result = unsafe { wake(address as *const AtomicU32 as *const u32, i32::MAX as u32) };
    if let Err(error) = result {
        fail("futex wake_all", error);
    }
}
