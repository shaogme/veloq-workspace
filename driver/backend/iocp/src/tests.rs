pub(crate) mod basic;
pub(crate) mod io_tests;
pub(crate) mod net;
pub(crate) mod net_udp;

use core::convert::TryFrom;
use diagweave::prelude::*;
use std::sync::atomic::Ordering;
use veloq_driver_core::driver::{
    CompletionRecord, DriveMode, Driver, DriverSubmitResult, PollRecordResult,
    encode_completion_token, event_res_to_result,
};
use veloq_driver_core::op::{IntoPlatformOp, OpCompletion};
use veloq_driver_core::slot::SlotTable;

use crate::driver::IocpDriver;
use crate::error::{IocpError, IocpResult, iocp_report_to_event_res};
use crate::op::{IocpOp, IocpUserPayload};

pub(crate) fn remote_free_contains(driver: &IocpDriver, needle: usize) -> bool {
    let mut cur = driver.ops.shared.remote_free_head.load(Ordering::Acquire);
    while cur != SlotTable::<crate::op::slot::IocpSlotSpec>::NULL_INDEX {
        if cur == needle {
            return true;
        }
        cur = driver.ops.shared.slots[cur]
            .next_free
            .load(Ordering::Relaxed);
    }
    false
}

pub(crate) fn submit_test_op<T>(driver: &mut IocpDriver, data: T) -> (usize, u32)
where
    T: IntoPlatformOp<
            IocpOp,
            DriverCompletion = usize,
            ErasedPayload = IocpUserPayload,
            Error = IocpError,
        >,
{
    let (iocp_kernel, payload) = IntoPlatformOp::<IocpOp>::into_kernel_and_payload(data);
    let mut iocp_op = Some(iocp_kernel);
    let (user_data, generation) = driver.reserve_op().expect("reserve op failed");
    driver.slot_set_payload(user_data, T::payload_into_erased(payload));
    match driver.submit(user_data, &mut iocp_op) {
        DriverSubmitResult::Submitted(_) => {}
        DriverSubmitResult::Failed { report, status } => {
            panic!("submit op failed: status={status:?}, error={report}")
        }
    }
    (user_data, generation)
}

pub(crate) fn complete_from_record<T>(
    record: CompletionRecord<IocpUserPayload, IocpError>,
) -> OpCompletion<T::Output, IocpError, T::Completion>
where
    T: IntoPlatformOp<
            IocpOp,
            DriverCompletion = usize,
            ErasedPayload = IocpUserPayload,
            Error = IocpError,
        >,
{
    let payload_erased = record.payload.expect("completion payload missing");
    let payload = T::payload_from_erased(payload_erased);
    let res = record
        .detail
        .unwrap_or_else(|| event_res_to_result::<usize, IocpError>(record.event.res));
    T::complete(payload, res)
}

pub(crate) fn wait_completion_record(
    driver: &mut IocpDriver,
    user_data: usize,
    generation: u32,
    timeout: std::time::Duration,
) -> IocpResult<CompletionRecord<IocpUserPayload, IocpError>> {
    let start = std::time::Instant::now();
    let token = encode_completion_token(user_data, generation);
    loop {
        if start.elapsed() > timeout {
            return IocpError::CompletionWait.attach_note(format!(
                "wait completion timed out: user_data={}, generation={}",
                user_data, generation
            ));
        }
        let _ = driver.drive(DriveMode::Poll);
        let completion_table = driver.completion_table();
        match completion_table.try_take_record(token) {
            PollRecordResult::Ready(record) => return Ok(record),
            PollRecordResult::Stale => {
                return IocpError::CompletionWait
                    .attach_note("stale completion record (generation mismatch)");
            }
            PollRecordResult::Pending => {}
        }
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
}

pub(crate) fn wait_completion(
    driver: &mut IocpDriver,
    user_data: usize,
    generation: u32,
    timeout: std::time::Duration,
) -> IocpResult<usize> {
    let record = wait_completion_record(driver, user_data, generation, timeout)?;
    event_res_to_result::<usize, IocpError>(record.event.res).map_err(|e| {
        let code = iocp_report_to_event_res(&e);
        let io_error = std::io::Error::from_raw_os_error(-code);
        IocpError::CompletionWait.io_report("iocp.tests.wait_completion", io_error)
    })
}

pub(crate) fn completion_os_error_code(err: &Report<IocpError>) -> Option<i32> {
    err.error_code().and_then(|code| i32::try_from(code).ok())
}
