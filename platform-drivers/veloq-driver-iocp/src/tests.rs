pub(crate) mod basic;
pub(crate) mod io_tests;
pub(crate) mod net;
pub(crate) mod net_udp;

use std::io;
use std::sync::atomic::Ordering;
use veloq_driver_core::driver::{Driver, encode_completion_token, event_res_to_io};
use veloq_driver_core::slot::SlotTable;

use crate::driver::IocpDriver;

pub(crate) fn remote_free_contains(driver: &IocpDriver, needle: usize) -> bool {
    let mut cur = driver.ops.shared.remote_free_head.load(Ordering::Acquire);
    while cur != SlotTable::<crate::ops::IocpOp, crate::ops::OverlappedEntry>::NULL_INDEX {
        if cur == needle {
            return true;
        }
        cur = driver.ops.shared.slots[cur]
            .next_free
            .load(Ordering::Relaxed);
    }
    false
}

pub(crate) fn wait_completion(
    driver: &mut IocpDriver,
    user_data: usize,
    generation: u32,
    timeout: std::time::Duration,
) -> io::Result<usize> {
    let start = std::time::Instant::now();
    let token = encode_completion_token(user_data, generation);
    loop {
        if start.elapsed() > timeout {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                format!(
                    "wait completion timed out: user_data={}, generation={}",
                    user_data, generation
                ),
            ));
        }
        driver.process_completions();
        if let Some(ev) = driver.try_take_completion(token) {
            return event_res_to_io(ev.res);
        }
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
}
