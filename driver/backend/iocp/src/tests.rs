pub(crate) mod basic;
pub(crate) mod io_tests;
pub(crate) mod net;
pub(crate) mod net_udp;

use core::convert::TryFrom;
use diagweave::prelude::*;
use std::{
    io, thread,
    time::{Duration, Instant},
};
use veloq_driver_core::{
    driver::{
        CompletionRecord, CompletionValue, DriveMode, Driver, DriverSubmitResult, OpToken,
        PollRecordResult,
    },
    op::{IntoPlatformOp, OpCompletion},
};

use crate::{
    driver::IocpDriver,
    error::{IocpError, IocpResult, iocp_report_to_event_res},
    op::{IocpOp, IocpSlotSpec, IocpUserPayload},
};

pub(crate) fn remote_free_contains(driver: &IocpDriver, needle: usize) -> bool {
    driver.debug_remote_free_contains(needle)
}

pub(crate) fn submit_test_op<T>(driver: &mut IocpDriver, data: T) -> OpToken
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
    let mut slot = driver.reserve_op().expect("reserve op failed");
    slot.set_payload(T::payload_into_erased(payload));
    match slot.submit(&mut iocp_op) {
        DriverSubmitResult::Submitted(_) => {
            let submitted = slot.persist().token();
            submitted
        }
        DriverSubmitResult::Failed { report, status } => {
            panic!("submit op failed: status={status:?}, error={report}")
        }
    }
}

pub(crate) fn complete_from_record<T>(
    record: CompletionRecord<IocpSlotSpec>,
) -> OpCompletion<T::Output, IocpError, T::Completion>
where
    T: IntoPlatformOp<
            IocpOp,
            DriverCompletion = usize,
            ErasedPayload = IocpUserPayload,
            Error = IocpError,
        >,
{
    let CompletionRecord {
        event,
        payload: payload_erased,
        mut detail,
        mut cleanup,
    } = record;
    let payload = T::try_payload_from_erased(payload_erased).expect("completion payload type");
    let res = detail
        .take()
        .unwrap_or_else(|| usize::from_event_res::<IocpError>(event.res()));
    cleanup.disarm();
    T::complete(payload, res)
}

pub(crate) fn wait_completion_record(
    driver: &mut IocpDriver,
    token: OpToken,
    timeout: Duration,
) -> IocpResult<CompletionRecord<IocpSlotSpec>> {
    let start = Instant::now();
    loop {
        if start.elapsed() > timeout {
            return IocpError::CompletionWait
                .with_ctx("user_data", token.index())
                .with_ctx("generation", token.generation())
                .with_ctx("timeout_ms", timeout.as_millis() as u64)
                .attach_note("wait completion timed out");
        }
        let _ = driver.drive(DriveMode::Poll)?;
        let completion_table = driver.completion_table();
        match completion_table.try_take_record(token)? {
            PollRecordResult::Ready(record) => return Ok(record),
            PollRecordResult::Unavailable { kind, attach } => {
                return IocpError::CompletionWait
                    .with_ctx("completion_token", attach.token.raw())
                    .with_ctx("completion_anomaly", format!("{:?}", kind.reason()))
                    .attach_note("completion record unavailable");
            }
            PollRecordResult::Pending => {}
        }
        thread::sleep(Duration::from_millis(5));
    }
}

pub(crate) fn wait_completion(
    driver: &mut IocpDriver,
    token: OpToken,
    timeout: Duration,
) -> IocpResult<usize> {
    let record = wait_completion_record(driver, token, timeout)?;
    usize::from_event_res::<IocpError>(record.event.res()).map_err(|e| {
        let code = iocp_report_to_event_res(&e);
        let io_error = io::Error::from_raw_os_error(-code);
        IocpError::CompletionWait.io_report("iocp.tests.wait_completion", io_error)
    })
}

pub(crate) fn completion_os_error_code(err: &Report<IocpError>) -> Option<i32> {
    err.error_code().and_then(|code| i32::try_from(code).ok())
}
