use std::os::fd::AsRawFd;

use veloq_buf::NoopRegistrar;
use veloq_driver_core::driver::{Driver, DriverSubmitResult, RegisterFd, SubmitStatus};
use veloq_driver_core::op::{Fsync, IntoPlatformOp};
use veloq_driver_uring::{
    RawHandle, UringConfig, UringDriver, UringError, UringOp, UringRawHandle, UringUserPayload,
};

fn new_driver_or_skip() -> Option<UringDriver<'static>> {
    match UringDriver::new(UringConfig::default(), Box::new(NoopRegistrar)) {
        Ok(driver) => Some(driver),
        Err(report) => {
            eprintln!("skipping uring test: {report}");
            None
        }
    }
}

#[test]
fn stale_registered_fd_generation_rejected_on_submit() {
    let Some(mut driver) = new_driver_or_skip() else {
        return;
    };

    let first = std::fs::File::open("Cargo.toml").unwrap();
    let first_raw = RawHandle::new(UringRawHandle::for_file(first.as_raw_fd()));
    let stale_fd = driver
        .register_files(vec![RegisterFd::Borrowed(first_raw.borrow())])
        .unwrap()
        .into_iter()
        .next()
        .unwrap();
    driver.unregister_files(vec![stale_fd]).unwrap();

    let second = std::fs::File::open("Cargo.toml").unwrap();
    let second_raw = RawHandle::new(UringRawHandle::for_file(second.as_raw_fd()));
    let fresh_fd = driver
        .register_files(vec![RegisterFd::Borrowed(second_raw.borrow())])
        .unwrap()
        .into_iter()
        .next()
        .unwrap();

    assert_eq!(stale_fd.fixed_index(), fresh_fd.fixed_index());
    assert_ne!(stale_fd.generation(), fresh_fd.generation());

    let op = Fsync {
        fd: stale_fd,
        datasync: false,
    };
    let (uring_kernel, payload) = op.into_kernel_and_payload();
    let mut uring_op: Option<UringOp> = Some(uring_kernel);
    let mut slot = driver.reserve_op().expect("reserve op failed");
    slot.set_payload(<Fsync as IntoPlatformOp<UringOp>>::payload_into_erased(
        payload,
    ));

    match slot.submit(&mut uring_op) {
        DriverSubmitResult::Failed {
            report,
            status: SubmitStatus::Void,
        } => {
            assert_eq!(*report.inner(), UringError::ResolveFd);
        }
        DriverSubmitResult::Failed { status, .. } => {
            panic!("stale fd submit should fail before in-flight state, got {status:?}")
        }
        DriverSubmitResult::Submitted(_) => panic!("stale fd submit unexpectedly succeeded"),
    }

    let recovered = slot.recover_payload();
    assert!(
        matches!(recovered, Some(UringUserPayload::Fsync(_))),
        "payload should be recoverable after void failure"
    );

    driver.unregister_files(vec![fresh_fd]).unwrap();
}
