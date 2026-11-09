mod basic;
mod io_tests;
mod net;
mod net_udp;

use diagweave::prelude::*;
use tracing::Level;
use tracing_subscriber::{fmt, fmt::TestWriter};
use veloq_driver_core::{
    BufferRegistrationStatus, CompletionRecord, CompletionValue, DriveMode, Driver,
    DriverSubmitResult, IntoPlatformOp, OpCompletion, OpToken, PollRecordResult,
};
use veloq_std::{
    cell::RefCell,
    convert::TryFrom,
    format, io,
    sync::OnceLock,
    thread::ThreadApi,
    time::{Duration, Instant},
    vec,
    vec::Vec,
};

use crate::{
    driver::IocpDriver,
    error::{IocpError, IocpResult},
    op::IocpSlotSpec,
};
use veloq_buf::{
    AnyBufPool, BufError, BufResult, BufferRegion, BufferRegistrar, LockedMemoryBudget,
    PoolInitContext, PoolTopology, UniformSlot, heap::ChunkId,
};

pub(super) struct TestSupport;

impl TestSupport {
    pub(super) fn init_test_logger() {
        static INIT: OnceLock<()> = OnceLock::new();
        INIT.get_or_init(|| {
            let _ = fmt()
                .with_writer(TestWriter::new())
                .with_max_level(Level::TRACE)
                .try_init();
        });
    }
}

struct DriverBufferRegistrar<'driver, 'runtime> {
    driver: RefCell<&'driver mut IocpDriver<'runtime>>,
}

impl BufferRegistrar for DriverBufferRegistrar<'_, '_> {
    fn register(&self, regions: &mut [BufferRegion]) -> BufResult<Vec<ChunkId>> {
        let mut registered = Vec::with_capacity(regions.len());
        let mut driver = self.driver.borrow_mut();
        for region in regions {
            let id = region.id();
            let request = region.registration_request();
            let status = driver
                .register_buffer(request)
                .map_err(|report| BufError::Other(format!("{report:#}")))?;
            if matches!(status, BufferRegistrationStatus::Registered) {
                registered.push(id);
            }
        }
        Ok(registered)
    }
}

impl TestSupport {
    pub(super) fn build_registered_pool<'driver, 'runtime>(
        driver: &'driver mut IocpDriver<'runtime>,
        topology: &UniformSlot,
        budget: LockedMemoryBudget,
        worker_id: usize,
    ) -> AnyBufPool {
        let pool_state = topology
            .init(
                PoolInitContext::new(1, vec![worker_id], budget)
                    .expect("pool initialization context"),
            )
            .expect("initialize test buffer pool");
        let pool = {
            let registrar = DriverBufferRegistrar {
                driver: RefCell::new(driver),
            };
            topology
                .build(&pool_state, worker_id, &registrar)
                .expect("build buffer pool with registered chunks")
        };
        driver
            .attach_buffer_pool(pool.clone())
            .expect("attach buffer pool to driver");
        pool
    }

    pub(super) fn slot_is_idle(driver: &mut IocpDriver, index: usize) -> bool {
        driver.debug_slot_is_idle(index)
    }

    pub(super) fn submit_test_op<T>(driver: &mut IocpDriver, data: T) -> OpToken
    where
        T: IntoPlatformOp<IocpSlotSpec>,
    {
        let (iocp_kernel, payload) = IntoPlatformOp::<IocpSlotSpec>::into_kernel_and_payload(data);
        let mut iocp_op = Some(iocp_kernel);
        let mut slot = driver.reserve_op().expect("reserve op failed");
        slot.set_payload(T::payload_into_erased(payload));
        match slot.submit(&mut iocp_op) {
            DriverSubmitResult::Submitted(_) => slot.persist().token(),
            DriverSubmitResult::Failed { report, status } => {
                panic!("submit op failed: status={status:?}, error={report}")
            }
        }
    }

    pub(super) fn complete_from_record<T>(
        record: CompletionRecord<IocpSlotSpec>,
    ) -> OpCompletion<T::Output, IocpError, T::Completion>
    where
        T: IntoPlatformOp<IocpSlotSpec>,
    {
        let parts = record.into_parts();
        let event = parts.event;
        let payload_erased = parts.payload;
        let mut detail = parts.detail;
        let mut cleanup = parts.cleanup;
        let payload =
            T::try_record_from_erased(payload_erased.expect("completion record payload type"))
                .expect("completion payload type");
        let res = detail
            .take()
            .unwrap_or_else(|| usize::from_event_res::<IocpError>(event.res()));
        cleanup.disarm();
        T::complete(payload, res)
    }

    pub(super) fn wait_completion_record(
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
                        .with_ctx("completion_token", attach.token().raw())
                        .with_ctx("completion_anomaly", format!("{:?}", kind.reason()))
                        .attach_note("completion record unavailable");
                }
                PollRecordResult::Pending => {}
            }
            let _ = ThreadApi::sleep(Duration::from_millis(5));
        }
    }

    pub(super) fn wait_completion(
        driver: &mut IocpDriver,
        token: OpToken,
        timeout: Duration,
    ) -> IocpResult<usize> {
        let record = Self::wait_completion_record(driver, token, timeout)?;
        usize::from_event_res::<IocpError>(record.event().res()).map_err(|error| {
            let code = IocpError::report_to_event_res(&error);
            let io_error = io::Error::from_raw_os_error(-code);
            IocpError::CompletionWait.io_report("iocp.tests.wait_completion", io_error)
        })
    }

    pub(super) fn completion_os_error_code(err: &Report<IocpError>) -> Option<i32> {
        err.error_code().and_then(|code| i32::try_from(code).ok())
    }
}
