pub(crate) mod basic;
pub(crate) mod io_tests;
pub(crate) mod net;
pub(crate) mod net_udp;

use core::convert::TryFrom;
use diagweave::report::Report;
use std::sync::atomic::Ordering;
use veloq_driver_core::driver::{
    DriveMode, Driver, PollRecordResult, encode_completion_token, event_res_to_result,
};
use veloq_driver_core::driver_error_report_to_event_res;
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
            return Err(Report::new(IocpError::CompletionWait).attach_note(format!(
                "wait completion timed out: user_data={}, generation={}",
                user_data, generation
            )));
        }
        let _ = driver.drive(DriveMode::Poll);
        let completion_table = driver.completion_table();
        match completion_table.try_take(token) {
            PollRecordResult::Ready(record) => {
                return event_res_to_result(record.event.res).map_err(|e| {
                    let code = driver_error_report_to_event_res(&e);
                    let io_error = std::io::Error::from_raw_os_error(-code);
                    from_io_error(
                        IocpError::CompletionWait,
                        "iocp.tests.wait_completion",
                        io_error,
                    )
                });
            }
            PollRecordResult::Stale => {
                return Err(Report::new(IocpError::CompletionWait)
                    .attach_note("stale completion record (generation mismatch)"));
            }
            PollRecordResult::Pending => {}
        }
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
}

pub(crate) fn completion_os_error_code(err: &Report<IocpError>) -> Option<i32> {
    err.error_code().and_then(|code| i32::try_from(code).ok())
}
