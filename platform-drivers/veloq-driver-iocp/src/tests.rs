pub(crate) mod basic;
pub(crate) mod io_tests;
pub(crate) mod net;
pub(crate) mod net_udp;

use std::sync::atomic::Ordering;
use veloq_driver_core::driver::{
    Driver, PollRecordResult, encode_completion_token, event_res_to_result,
};
use veloq_driver_core::slot::SlotTable;

use crate::driver::IocpDriver;
use crate::error::{IocpError, IocpResult, from_io_error};

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
) -> IocpResult<usize> {
    let start = std::time::Instant::now();
    let token = encode_completion_token(user_data, generation);
    loop {
        if start.elapsed() > timeout {
            return Err(
                error_stack::Report::new(IocpError::CompletionWait).attach(format!(
                    "wait completion timed out: user_data={}, generation={}",
                    user_data, generation
                )),
            );
        }
        driver.process_completions();
        match driver.try_take_completion(token) {
            PollRecordResult::Ready(record) => {
                return event_res_to_result(record.event.res).map_err(|e| {
                    from_io_error(IocpError::CompletionWait, "iocp.tests.wait_completion", e)
                });
            }
            PollRecordResult::Stale => {
                return Err(error_stack::Report::new(IocpError::CompletionWait)
                    .attach("stale completion record (generation mismatch)"));
            }
            PollRecordResult::Pending => {}
        }
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
}
