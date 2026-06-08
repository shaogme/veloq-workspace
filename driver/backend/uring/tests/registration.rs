use std::num::NonZeroU32;
use std::os::fd::AsRawFd;

use veloq_buf::NoopRegistrar;
use veloq_driver_core::driver::{Driver, DriverSubmitResult, RegisterFd, SubmitStatus};
use veloq_driver_core::op::{Fsync, IntoPlatformOp};
use veloq_driver_uring::{
    IoFd, RawHandle, UringConfig, UringDriver, UringError, UringOp, UringRawHandle, UringResult,
    UringUserPayload,
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

fn new_driver_with_entries_or_skip(entries: u32) -> Option<UringDriver<'static>> {
    let config = UringConfig {
        entries: NonZeroU32::new(entries).unwrap(),
        ..UringConfig::default()
    };
    match UringDriver::new(config, Box::new(NoopRegistrar)) {
        Ok(driver) => Some(driver),
        Err(report) => {
            eprintln!("skipping uring test with {entries} entries: {report}");
            None
        }
    }
}

fn raw_file(file: &std::fs::File) -> RawHandle {
    RawHandle::new(UringRawHandle::for_file(file.as_raw_fd()))
}

fn invalid_file_handle() -> RawHandle {
    RawHandle::new(UringRawHandle::for_file(i32::MAX))
}

fn open_cargo_files<const N: usize>() -> [std::fs::File; N] {
    std::array::from_fn(|_| std::fs::File::open("Cargo.toml").unwrap())
}

fn register_borrowed_files(
    driver: &mut UringDriver<'static>,
    files: &[std::fs::File],
) -> Vec<IoFd> {
    let raw_files = files.iter().map(raw_file).collect::<Vec<_>>();
    let registrations = raw_files
        .iter()
        .map(|raw| RegisterFd::Borrowed(raw.borrow()))
        .collect::<Vec<_>>();
    driver.register_files(registrations).unwrap()
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

#[test]
fn failed_single_registration_restores_popped_slot() {
    let Some(mut driver) = new_driver_with_entries_or_skip(4) else {
        return;
    };

    let invalid = invalid_file_handle();
    assert!(
        driver
            .register_files(vec![RegisterFd::Borrowed(invalid.borrow())])
            .is_err()
    );

    let files = open_cargo_files::<3>();
    let fds = register_borrowed_files(&mut driver, &files);
    assert_eq!(fds.len(), files.len());

    driver.unregister_files(fds).unwrap();
}

#[test]
fn failed_batch_registration_rolls_back_successful_prefix() {
    let Some(mut driver) = new_driver_with_entries_or_skip(4) else {
        return;
    };

    let first = std::fs::File::open("Cargo.toml").unwrap();
    let first_raw = raw_file(&first);
    let invalid = invalid_file_handle();
    assert!(
        driver
            .register_files(vec![
                RegisterFd::Borrowed(first_raw.borrow()),
                RegisterFd::Borrowed(invalid.borrow()),
            ])
            .is_err()
    );

    let files = open_cargo_files::<3>();
    let fds = register_borrowed_files(&mut driver, &files);
    assert_eq!(fds.len(), files.len());

    driver.unregister_files(fds).unwrap();
}

#[test]
fn exhausted_batch_registration_does_not_partially_register() {
    let Some(mut driver) = new_driver_with_entries_or_skip(4) else {
        return;
    };

    let too_many_files = open_cargo_files::<4>();
    assert!(register_borrowed_files_result(&mut driver, &too_many_files).is_err());

    let files = open_cargo_files::<3>();
    let fds = register_borrowed_files(&mut driver, &files);
    assert_eq!(fds.len(), files.len());

    driver.unregister_files(fds).unwrap();
}

fn register_borrowed_files_result(
    driver: &mut UringDriver<'static>,
    files: &[std::fs::File],
) -> UringResult<Vec<IoFd>> {
    let raw_files = files.iter().map(raw_file).collect::<Vec<_>>();
    let registrations = raw_files
        .iter()
        .map(|raw| RegisterFd::Borrowed(raw.borrow()))
        .collect::<Vec<_>>();
    driver.register_files(registrations)
}
