#![cfg(not(feature = "loom"))]
#![cfg(any(target_os = "linux", target_os = "android"))]

use veloq_std::{
    array,
    fs::File,
    mem,
    num::NonZeroU32,
    thread,
    time::{Duration, Instant},
    vec,
    vec::Vec,
};

use veloq_buf::NoopRegistrar;
use veloq_driver_core::driver::{
    CancelRequest, CompletionRecord, CompletionValue, DriveMode, Driver, DriverSubmitResult,
    PollRecordResult, RegisterFd, SubmitStatus,
};

#[cfg(feature = "test-hooks")]
use veloq_driver_core::driver::test_hooks::{DriverTestHooks, RegisterFilesUpdateOutcome};
use veloq_driver_core::op::{
    IntoPlatformOp,
    types::{Close as CoreClose, Fsync as CoreFsync, Timeout as CoreTimeout},
};
use veloq_driver_uring::{
    FileTableExhaustion, IoFd, OwnedRawHandle, RawHandle, UringConfig, UringDriveLimits,
    UringDriver, UringError, UringOp, UringRawHandle, UringResult, UringSlotSpec,
};

type Close = CoreClose<UringRawHandle>;
type Fsync = CoreFsync<UringRawHandle>;
type Timeout = CoreTimeout;

fn new_driver_or_skip() -> Option<UringDriver<'static>> {
    static REGISTRAR: NoopRegistrar = NoopRegistrar;
    match UringDriver::new(UringConfig::default(), &REGISTRAR) {
        Ok(driver) => Some(driver),
        Err(report) => {
            eprintln!("skipping uring test: {report}");
            None
        }
    }
}

#[test]
fn software_timer_stays_out_of_sq_backlog_when_sq_is_full_and_cancelled() {
    static REGISTRAR: NoopRegistrar = NoopRegistrar;
    let config = UringConfig {
        entries: NonZeroU32::new(1).expect("non-zero SQ capacity"),
        drive_limits: UringDriveLimits::for_entries(1),
        ..UringConfig::default()
    };
    let Ok(mut driver) = UringDriver::new(config, &REGISTRAR) else {
        eprintln!("skipping timer/SQ-full test: io_uring setup unavailable");
        return;
    };

    // Driver construction arms the eventfd waker, so the one-entry SQ is already full. A
    // software timer must still be accepted without creating a backlog entry or touching SQ.
    let token = submit_test_op(
        &mut driver,
        Timeout {
            duration: Duration::from_secs(1),
        },
    );
    assert_eq!(
        driver
            .cancel_op(CancelRequest::user_visible(token))
            .expect("timer cancellation should succeed"),
        veloq_driver_core::driver::CancelSubmitOutcome::CompletedLocally
    );

    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        let record = match driver.completion_table().try_take_record(token).unwrap() {
            PollRecordResult::Ready(record) => Some(record),
            PollRecordResult::Pending => None,
            PollRecordResult::Unavailable { kind, .. } => {
                panic!("timer cancellation record unavailable: {kind:?}")
            }
        };
        if let Some(mut record) = record {
            assert_eq!(record.event.res(), -libc::ECANCELED);
            record.cleanup.disarm();
            break;
        }
        assert!(
            Instant::now() < deadline,
            "timer cancellation did not settle"
        );
        driver.drive(DriveMode::Poll).expect("drive failed");
    }
}

/// A driver whose kernel file table holds `capacity` entries, one of which the eventfd waker
/// claims during construction.
fn new_driver_with_file_table_or_skip(
    capacity: u32,
    exhaustion: FileTableExhaustion,
) -> Option<UringDriver<'static>> {
    let config = UringConfig {
        entries: NonZeroU32::new(64).unwrap(),
        drive_limits: UringDriveLimits::for_entries(64),
        file_table_capacity: capacity,
        file_table_exhaustion: exhaustion,
        ..UringConfig::default()
    };
    static REGISTRAR: NoopRegistrar = NoopRegistrar;
    match UringDriver::new(config, &REGISTRAR) {
        Ok(mut driver) if capacity > 0 => {
            let file = File::open("Cargo.toml").ok()?;
            let raw = raw_file(&file);
            let registered = driver
                .register_files(vec![RegisterFd::Borrowed(raw.borrow())])
                .ok()?;
            let is_registered = registered.first().is_some_and(|fd| fd.is_registered());
            driver.unregister_files(registered).ok()?;
            if !is_registered {
                eprintln!("skipping uring test with unavailable sparse file registration");
                None
            } else {
                Some(driver)
            }
        }
        Ok(driver) => Some(driver),
        Err(report) => {
            eprintln!("skipping uring test with file table capacity {capacity}: {report}");
            None
        }
    }
}

fn raw_file(file: &File) -> RawHandle {
    RawHandle::new(UringRawHandle::for_file(file.as_raw_fd()))
}

fn owned_file_fd(fd: i32) -> OwnedRawHandle {
    // SAFETY: every caller transfers one uniquely owned raw fd into this wrapper.
    unsafe { OwnedRawHandle::from_raw_owned(RawHandle::new(UringRawHandle::for_file(fd))) }
}

fn invalid_file_handle() -> RawHandle {
    RawHandle::new(UringRawHandle::for_file(i32::MAX))
}

fn open_cargo_files<const N: usize>() -> [File; N] {
    array::from_fn(|_| File::open("Cargo.toml").unwrap())
}

fn register_borrowed_files(driver: &mut UringDriver<'static>, files: &[File]) -> Vec<IoFd> {
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

    let first = File::open("Cargo.toml").unwrap();
    let first_raw = RawHandle::new(UringRawHandle::for_file(first.as_raw_fd()));
    let stale_fd = driver
        .register_files(vec![RegisterFd::Borrowed(first_raw.borrow())])
        .unwrap()
        .into_iter()
        .next()
        .unwrap();
    let Some(stale_generation) = stale_fd.generation() else {
        eprintln!("skipping fixed-file generation assertion: backend returned a direct fd");
        return;
    };
    driver.unregister_files(vec![stale_fd]).unwrap();

    let second = File::open("Cargo.toml").unwrap();
    let second_raw = RawHandle::new(UringRawHandle::for_file(second.as_raw_fd()));
    let fresh_fd = driver
        .register_files(vec![RegisterFd::Borrowed(second_raw.borrow())])
        .unwrap()
        .into_iter()
        .next()
        .unwrap();

    assert_eq!(stale_fd.fixed_index(), fresh_fd.fixed_index());
    assert_ne!(Some(stale_generation), fresh_fd.generation());

    assert_stale_fsync_is_rejected(&mut driver, stale_fd);

    driver.unregister_files(vec![fresh_fd]).unwrap();
}

/// Submits an `Fsync` on `fd` and asserts it is rejected before going in flight.
fn assert_fsync_is_rejected_with(
    driver: &mut UringDriver<'static>,
    fd: IoFd,
    expected_error: UringError,
) {
    let op = Fsync {
        fd,
        datasync: false,
    };
    let (uring_kernel, payload) =
        <Fsync as IntoPlatformOp<UringSlotSpec>>::into_kernel_and_payload(op);
    let mut uring_op: Option<UringOp> = Some(uring_kernel);
    let mut slot = driver.reserve_op().expect("reserve op failed");
    slot.set_payload(<Fsync as IntoPlatformOp<UringSlotSpec>>::payload_into_erased(payload));

    match slot.submit(&mut uring_op) {
        DriverSubmitResult::Failed {
            report,
            status: SubmitStatus::Void,
        } => {
            assert_eq!(*report.inner(), expected_error);
        }
        DriverSubmitResult::Failed { status, .. } => {
            panic!("stale fd submit should fail before in-flight state, got {status:?}")
        }
        DriverSubmitResult::Submitted(_) => panic!("stale fd submit unexpectedly succeeded"),
    }

    let recovered = slot.recover_payload();
    assert!(
        recovered.is_some_and(|payload| {
            <Fsync as IntoPlatformOp<UringSlotSpec>>::try_record_from_erased(payload).is_ok()
        }),
        "payload should be recoverable after void failure"
    );
}

fn assert_stale_fsync_is_rejected(driver: &mut UringDriver<'static>, fd: IoFd) {
    assert_fsync_is_rejected_with(driver, fd, UringError::ResolveFd);
}

fn assert_close_is_rejected_with(
    driver: &mut UringDriver<'static>,
    fd: IoFd,
    expected_error: UringError,
) {
    let op = Close { fd };
    let (uring_kernel, payload) =
        <Close as IntoPlatformOp<UringSlotSpec>>::into_kernel_and_payload(op);
    let mut uring_op: Option<UringOp> = Some(uring_kernel);
    let mut slot = driver.reserve_op().expect("reserve op failed");
    slot.set_payload(<Close as IntoPlatformOp<UringSlotSpec>>::payload_into_erased(payload));

    match slot.submit(&mut uring_op) {
        DriverSubmitResult::Failed {
            report,
            status: SubmitStatus::Void,
        } => assert_eq!(*report.inner(), expected_error),
        DriverSubmitResult::Failed { status, .. } => {
            panic!("Close should fail before in-flight state, got {status:?}")
        }
        DriverSubmitResult::Submitted(_) => panic!("Close unexpectedly succeeded"),
    }

    let recovered = slot.recover_payload();
    assert!(
        recovered.is_some_and(|payload| {
            <Close as IntoPlatformOp<UringSlotSpec>>::try_record_from_erased(payload).is_ok()
        }),
        "payload should be recoverable after void failure"
    );
}

#[test]
fn unknown_single_registration_poisoned_file_table_is_fail_stop() {
    let Some(mut driver) = new_driver_with_file_table_or_skip(4, FileTableExhaustion::Fail) else {
        return;
    };

    let invalid = invalid_file_handle();
    let report = driver
        .register_files(vec![RegisterFd::Borrowed(invalid.borrow())])
        .expect_err("an uncertain kernel update must poison the file table");
    assert_eq!(*report.inner(), UringError::FileTablePoisoned);

    let files = open_cargo_files::<3>();
    let report = register_borrowed_files_result(&mut driver, &files)
        .expect_err("a poisoned table must reject future fixed registrations");
    assert_eq!(*report.inner(), UringError::FileTablePoisoned);
}

#[cfg(feature = "test-hooks")]
#[test]
fn partial_batch_registration_rolls_back_successful_prefix() {
    let Some(mut driver) = new_driver_with_file_table_or_skip(4, FileTableExhaustion::Fail) else {
        return;
    };

    let files = open_cargo_files::<2>();
    let raw_files = files.iter().map(raw_file).collect::<Vec<_>>();
    let registrations = raw_files
        .iter()
        .map(|raw| RegisterFd::Borrowed(raw.borrow()))
        .collect::<Vec<_>>();
    (&mut driver as &mut dyn DriverTestHooks).debug_inject_register_files_update_sequence(&[
        RegisterFilesUpdateOutcome::Updated(1),
        RegisterFilesUpdateOutcome::Actual,
    ]);
    let report = driver
        .register_files(registrations)
        .expect_err("a short update must abort the batch");
    assert_eq!(*report.inner(), UringError::Registration);
    assert!(!(&driver as &dyn DriverTestHooks).debug_file_table_poisoned());

    let files = open_cargo_files::<3>();
    let fds = register_borrowed_files(&mut driver, &files);
    assert_eq!(fds.len(), files.len());

    driver.unregister_files(fds).unwrap();
}

#[cfg(feature = "test-hooks")]
#[test]
fn rejected_registration_releases_batch_without_rollback() {
    let Some(mut driver) = new_driver_with_file_table_or_skip(2, FileTableExhaustion::Fallback)
    else {
        return;
    };

    let file = File::open("Cargo.toml").unwrap();
    let raw = raw_file(&file);
    {
        let hooks = &mut driver as &mut dyn DriverTestHooks;
        hooks.debug_inject_register_files_update_failure(libc::EIO);
    }

    let report = driver
        .register_files(vec![RegisterFd::Borrowed(raw.borrow())])
        .expect_err("a rejected update must abort the batch");
    assert_eq!(*report.inner(), UringError::Registration);
    assert!(!(&driver as &dyn DriverTestHooks).debug_file_table_poisoned());

    let replacement = File::open("Cargo.toml").unwrap();
    let replacement_raw = raw_file(&replacement);
    let second = driver
        .register_files(vec![RegisterFd::Borrowed(replacement_raw.borrow())])
        .expect("the rejected batch must release its reserved slot");
    assert!(second[0].is_registered());
    assert_eq!(
        (&driver as &dyn DriverTestHooks).debug_register_files_update_outcomes_pending(),
        0
    );
    driver.unregister_files(second).unwrap();

    let snapshot = driver.completion_diagnostics_snapshot();
    assert_eq!(snapshot.backend.file_table_rollback_failures, 0);
    assert_eq!(snapshot.backend.file_table_poisonings, 0);
}

#[cfg(feature = "test-hooks")]
#[test]
fn short_registration_then_rollback_failure_uses_the_same_terminal_error() {
    let Some(mut driver) = new_driver_with_file_table_or_skip(3, FileTableExhaustion::Fail) else {
        return;
    };

    let files = open_cargo_files::<2>();
    let raw_files = files.iter().map(raw_file).collect::<Vec<_>>();
    let registrations = raw_files
        .iter()
        .map(|raw| RegisterFd::Borrowed(raw.borrow()))
        .collect::<Vec<_>>();
    {
        let hooks = &mut driver as &mut dyn DriverTestHooks;
        hooks.debug_inject_register_files_update_sequence(&[
            RegisterFilesUpdateOutcome::Updated(1),
            RegisterFilesUpdateOutcome::Error(libc::EIO),
        ]);
    }

    let report = driver
        .register_files(registrations)
        .expect_err("short update followed by rollback failure must poison");
    assert_eq!(*report.inner(), UringError::FileTablePoisoned);
    assert!(report.context().contains_key("start_index"));
    assert!(report.context().contains_key("requested_files"));
    assert!(report.context().contains_key("updated_files"));
    assert!((&driver as &dyn DriverTestHooks).debug_file_table_poisoned());

    let snapshot = driver.completion_diagnostics_snapshot();
    assert_eq!(snapshot.backend.file_table_rollback_failures, 1);
    assert_eq!(snapshot.backend.file_table_poisonings, 1);
}

#[cfg(feature = "test-hooks")]
#[test]
fn successful_batch_rollback_keeps_the_file_table_healthy() {
    let Some(mut driver) = new_driver_with_file_table_or_skip(2, FileTableExhaustion::Fail) else {
        return;
    };

    let failed = File::open("Cargo.toml").unwrap();
    let failed_raw = raw_file(&failed);
    {
        let hooks = &mut driver as &mut dyn DriverTestHooks;
        hooks.debug_inject_register_files_update_sequence(&[
            RegisterFilesUpdateOutcome::Error(libc::EIO),
            RegisterFilesUpdateOutcome::Actual,
        ]);
    }
    let report = driver
        .register_files(vec![RegisterFd::Borrowed(failed_raw.borrow())])
        .expect_err("the injected initial registration must fail");
    assert_eq!(*report.inner(), UringError::Registration);
    assert!(!(&driver as &dyn DriverTestHooks).debug_file_table_poisoned());

    let healthy = File::open("Cargo.toml").unwrap();
    let healthy_raw = raw_file(&healthy);
    let fd = driver
        .register_files(vec![RegisterFd::Borrowed(healthy_raw.borrow())])
        .expect("a successful rollback must return the slot");
    driver.unregister_files(fd).unwrap();
}

#[cfg(feature = "test-hooks")]
#[test]
fn failed_registered_unregister_poison_keeps_owned_entry_until_driver_drop() {
    let Some(mut driver) = new_driver_with_file_table_or_skip(2, FileTableExhaustion::Fail) else {
        return;
    };

    let file = File::open("Cargo.toml").unwrap();
    let raw_fd = file.as_raw_fd();
    mem::forget(file);
    let owned =
        unsafe { OwnedRawHandle::from_raw_owned(RawHandle::new(UringRawHandle::for_file(raw_fd))) };
    let fd = driver
        .register_files(vec![RegisterFd::Owned(owned)])
        .unwrap()
        .into_iter()
        .next()
        .unwrap();

    {
        let hooks = &mut driver as &mut dyn DriverTestHooks;
        hooks.debug_inject_register_files_update_failure(libc::EIO);
    }
    let report = driver
        .unregister_files(vec![fd])
        .expect_err("an uncertain unregister update must poison the table");
    assert_eq!(*report.inner(), UringError::FileTablePoisoned);
    assert!((&driver as &dyn DriverTestHooks).debug_file_table_poisoned());
    assert_eq!(
        driver
            .completion_diagnostics_snapshot()
            .backend
            .file_table_poisonings,
        1
    );
    assert!(
        unsafe { libc::fcntl(raw_fd, libc::F_GETFD) } >= 0,
        "owned entry must remain live while the poisoned driver exists"
    );

    drop(driver);
    assert_eq!(
        unsafe { libc::fcntl(raw_fd, libc::F_GETFD) },
        -1,
        "owned entry must be released after the ring is dropped"
    );
}

#[cfg(feature = "test-hooks")]
#[test]
fn poisoned_file_table_rejects_old_registered_sqes_before_kernel_submission() {
    let Some(mut driver) = new_driver_with_file_table_or_skip(4, FileTableExhaustion::Fail) else {
        return;
    };

    let valid = File::open("Cargo.toml").unwrap();
    let valid_fd = driver
        .register_files(vec![RegisterFd::Borrowed(raw_file(&valid).borrow())])
        .unwrap()
        .into_iter()
        .next()
        .unwrap();

    let failed = open_cargo_files::<2>();
    let failed_raw = failed.iter().map(raw_file).collect::<Vec<_>>();
    let failed_registrations = failed_raw
        .iter()
        .map(|raw| RegisterFd::Borrowed(raw.borrow()))
        .collect::<Vec<_>>();
    {
        let hooks = &mut driver as &mut dyn DriverTestHooks;
        hooks.debug_inject_register_files_update_sequence(&[
            RegisterFilesUpdateOutcome::Updated(1),
            RegisterFilesUpdateOutcome::Error(libc::EIO),
        ]);
    }
    driver
        .register_files(failed_registrations)
        .expect_err("injected rollback failure must poison the table");

    assert_fsync_is_rejected_with(&mut driver, valid_fd, UringError::FileTablePoisoned);
}

#[cfg(feature = "test-hooks")]
#[test]
fn a_close_already_in_flight_forgets_owned_entry_after_poison() {
    let Some(mut driver) = new_driver_with_file_table_or_skip(4, FileTableExhaustion::Fail) else {
        return;
    };

    let file = File::open("Cargo.toml").unwrap();
    let raw_fd = file.as_raw_fd();
    mem::forget(file);
    let owned =
        unsafe { OwnedRawHandle::from_raw_owned(RawHandle::new(UringRawHandle::for_file(raw_fd))) };
    let fd = driver
        .register_files(vec![RegisterFd::Owned(owned)])
        .unwrap()
        .into_iter()
        .next()
        .unwrap();
    let close_token = submit_test_op(&mut driver, Close { fd });

    let failed = open_cargo_files::<2>();
    let failed_raw = failed.iter().map(raw_file).collect::<Vec<_>>();
    let failed_registrations = failed_raw
        .iter()
        .map(|raw| RegisterFd::Borrowed(raw.borrow()))
        .collect::<Vec<_>>();
    {
        let hooks = &mut driver as &mut dyn DriverTestHooks;
        hooks.debug_inject_register_files_update_sequence(&[
            RegisterFilesUpdateOutcome::Updated(1),
            RegisterFilesUpdateOutcome::Error(libc::EIO),
        ]);
    }
    driver
        .register_files(failed_registrations)
        .expect_err("the table must be poisoned while Close is in flight");

    let (closed, drive_error) =
        wait_completion_with_driver_error(&mut driver, close_token, Duration::from_secs(5));
    assert_eq!(closed, 0);
    assert_eq!(drive_error, Some(UringError::FileTablePoisoned));

    let reopened = File::open("Cargo.toml").unwrap();
    drop(driver);
    reopened
        .metadata()
        .expect("forgotten owned entry must not close a reused fd");
}

#[test]
fn exhausted_batch_registration_does_not_partially_register() {
    let Some(mut driver) = new_driver_with_file_table_or_skip(4, FileTableExhaustion::Fail) else {
        return;
    };

    // The waker holds one of the four entries, so a batch of four cannot fit.
    let too_many_files = open_cargo_files::<4>();
    assert!(register_borrowed_files_result(&mut driver, &too_many_files).is_err());

    let files = open_cargo_files::<3>();
    let fds = register_borrowed_files(&mut driver, &files);
    assert_eq!(fds.len(), files.len());

    driver.unregister_files(fds).unwrap();
}

fn register_borrowed_files_result(
    driver: &mut UringDriver<'static>,
    files: &[File],
) -> UringResult<Vec<IoFd>> {
    let raw_files = files.iter().map(raw_file).collect::<Vec<_>>();
    let registrations = raw_files
        .iter()
        .map(|raw| RegisterFd::Borrowed(raw.borrow()))
        .collect::<Vec<_>>();
    driver.register_files(registrations)
}

fn wait_completion(
    driver: &mut UringDriver<'static>,
    token: veloq_driver_core::driver::OpToken,
    timeout: Duration,
) -> usize {
    let start = Instant::now();
    loop {
        if start.elapsed() > timeout {
            panic!("wait_completion timed out");
        }
        let _ = driver.drive(DriveMode::Poll).expect("drive failed");
        let table = driver.completion_table();
        match table.try_take_record(token).unwrap() {
            PollRecordResult::Ready(record) => {
                let CompletionRecord {
                    event,
                    payload: _,
                    mut detail,
                    mut cleanup,
                    // 这些测试提交的都是单发操作。
                    continuation: _,
                } = record;
                cleanup.disarm();
                return detail
                    .take()
                    .unwrap_or_else(|| usize::from_event_res::<UringError>(event.res()))
                    .expect("completion reported error");
            }
            PollRecordResult::Unavailable { kind, .. } => {
                panic!("completion record unavailable: {kind:?}");
            }
            PollRecordResult::Pending => {}
        }
        let _ = thread::sleep(Duration::from_millis(5));
    }
}

#[cfg(feature = "test-hooks")]
fn wait_completion_with_driver_error(
    driver: &mut UringDriver<'static>,
    token: veloq_driver_core::driver::OpToken,
    timeout: Duration,
) -> (usize, Option<UringError>) {
    let start = Instant::now();
    loop {
        if start.elapsed() > timeout {
            panic!("wait_completion timed out");
        }
        let drive_error = driver
            .drive(DriveMode::Poll)
            .err()
            .map(|report| *report.inner());
        let table = driver.completion_table();
        match table.try_take_record(token).unwrap() {
            PollRecordResult::Ready(record) => {
                let CompletionRecord {
                    event,
                    payload: _,
                    mut detail,
                    mut cleanup,
                    continuation: _,
                } = record;
                cleanup.disarm();
                let result = detail
                    .take()
                    .unwrap_or_else(|| usize::from_event_res::<UringError>(event.res()))
                    .expect("completion reported error");
                return (result, drive_error);
            }
            PollRecordResult::Unavailable { kind, .. } => {
                panic!("completion record unavailable: {kind:?}");
            }
            PollRecordResult::Pending => {}
        }
        let _ = thread::sleep(Duration::from_millis(5));
    }
}

fn submit_test_op<T>(
    driver: &mut UringDriver<'static>,
    data: T,
) -> veloq_driver_core::driver::OpToken
where
    T: IntoPlatformOp<UringSlotSpec>,
{
    let (uring_kernel, payload) =
        <T as IntoPlatformOp<UringSlotSpec>>::into_kernel_and_payload(data);
    let mut uring_op: Option<UringOp> = Some(uring_kernel);
    let mut slot = driver.reserve_op().expect("reserve op failed");
    slot.set_payload(T::payload_into_erased(payload));
    match slot.submit(&mut uring_op) {
        DriverSubmitResult::Submitted(_) => slot.persist().token(),
        DriverSubmitResult::Failed { report, status } => {
            panic!("submit op failed: status={status:?}, error={report}")
        }
    }
}

#[test]
fn close_owned_registered_file() {
    let Some(mut driver) = new_driver_or_skip() else {
        return;
    };

    let file = File::open("Cargo.toml").unwrap();
    let raw_fd = file.as_raw_fd();
    let owned =
        unsafe { OwnedRawHandle::from_raw_owned(RawHandle::new(UringRawHandle::for_file(raw_fd))) };
    let fd = driver
        .register_files(vec![RegisterFd::Owned(owned)])
        .unwrap()
        .into_iter()
        .next()
        .unwrap();
    let Some(index) = fd.fixed_index() else {
        eprintln!("skipping fixed-file generation assertion: backend returned a direct fd");
        return;
    };

    let token = submit_test_op(&mut driver, Close { fd });
    let closed = wait_completion(&mut driver, token, Duration::from_secs(5));
    assert_eq!(closed, 0);

    let fsync_fd = driver
        .register_files(vec![RegisterFd::Borrowed(
            RawHandle::new(UringRawHandle::for_file(raw_fd)).borrow(),
        )])
        .unwrap()
        .into_iter()
        .next()
        .unwrap();
    let stale_fd = IoFd::fixed_with_generation(index, fd.generation().unwrap());
    assert_stale_fsync_is_rejected(&mut driver, stale_fd);

    driver.unregister_files(vec![fsync_fd]).unwrap();
}

#[cfg(feature = "test-hooks")]
#[test]
fn close_cleanup_error_quarantines_registered_owned_slot() {
    let Some(mut driver) = new_driver_with_file_table_or_skip(2, FileTableExhaustion::Fail) else {
        return;
    };

    let file = File::open("Cargo.toml").unwrap();
    let raw_fd = file.as_raw_fd();
    mem::forget(file);
    let owned =
        unsafe { OwnedRawHandle::from_raw_owned(RawHandle::new(UringRawHandle::for_file(raw_fd))) };
    let fd = driver
        .register_files(vec![RegisterFd::Owned(owned)])
        .unwrap()
        .into_iter()
        .next()
        .unwrap();

    let hooks = &mut driver as &mut dyn DriverTestHooks;
    hooks.debug_inject_register_files_update_failure(libc::EIO);
    let token = submit_test_op(&mut driver, Close { fd });
    let (closed, cleanup_error) =
        wait_completion_with_driver_error(&mut driver, token, Duration::from_secs(5));

    assert_eq!(closed, 0);
    assert_eq!(cleanup_error, Some(UringError::FileTableQuarantined));
    assert_stale_fsync_is_rejected(&mut driver, fd);

    let replacement = File::open("Cargo.toml").unwrap();
    let replacement_raw = raw_file(&replacement);
    let report = driver
        .register_files(vec![RegisterFd::Borrowed(replacement_raw.borrow())])
        .expect_err("waker plus quarantined slot must exhaust fixed capacity");
    assert_eq!(*report.inner(), UringError::InvalidState);
}

#[cfg(feature = "test-hooks")]
#[test]
fn close_cleanup_short_update_quarantines_without_releasing_slot() {
    let Some(mut driver) = new_driver_with_file_table_or_skip(2, FileTableExhaustion::Fail) else {
        return;
    };

    let file = File::open("Cargo.toml").unwrap();
    let raw_fd = file.as_raw_fd();
    mem::forget(file);
    let owned =
        unsafe { OwnedRawHandle::from_raw_owned(RawHandle::new(UringRawHandle::for_file(raw_fd))) };
    let fd = driver
        .register_files(vec![RegisterFd::Owned(owned)])
        .unwrap()
        .into_iter()
        .next()
        .unwrap();

    let hooks = &mut driver as &mut dyn DriverTestHooks;
    hooks.debug_inject_register_files_update_sequence(&[RegisterFilesUpdateOutcome::Updated(0)]);
    let token = submit_test_op(&mut driver, Close { fd });
    let (closed, cleanup_error) =
        wait_completion_with_driver_error(&mut driver, token, Duration::from_secs(5));

    assert_eq!(closed, 0);
    assert_eq!(cleanup_error, Some(UringError::FileTableQuarantined));
    assert_stale_fsync_is_rejected(&mut driver, fd);
    let snapshot = driver.completion_diagnostics_snapshot();
    assert_eq!(snapshot.backend.file_table_cleanup_failures, 1);
    assert_eq!(snapshot.backend.file_table_cleanup_short_updates, 1);
    assert_eq!(snapshot.backend.file_table_quarantines, 1);
}

#[cfg(feature = "test-hooks")]
#[test]
fn quarantined_slot_uses_direct_fallback_when_fixed_capacity_is_degraded() {
    let Some(mut driver) = new_driver_with_file_table_or_skip(2, FileTableExhaustion::Fallback)
    else {
        return;
    };

    let file = File::open("Cargo.toml").unwrap();
    let raw_fd = file.as_raw_fd();
    mem::forget(file);
    let owned =
        unsafe { OwnedRawHandle::from_raw_owned(RawHandle::new(UringRawHandle::for_file(raw_fd))) };
    let fd = driver
        .register_files(vec![RegisterFd::Owned(owned)])
        .unwrap()
        .into_iter()
        .next()
        .unwrap();

    let hooks = &mut driver as &mut dyn DriverTestHooks;
    hooks.debug_inject_register_files_update_failure(libc::EIO);
    let token = submit_test_op(&mut driver, Close { fd });
    let (closed, cleanup_error) =
        wait_completion_with_driver_error(&mut driver, token, Duration::from_secs(5));
    assert_eq!(closed, 0);
    assert_eq!(cleanup_error, Some(UringError::FileTableQuarantined));

    let replacement = File::open("Cargo.toml").unwrap();
    let replacement_raw = raw_file(&replacement);
    let fallback = driver
        .register_files(vec![RegisterFd::Borrowed(replacement_raw.borrow())])
        .unwrap()
        .into_iter()
        .next()
        .unwrap();
    assert!(fallback.is_direct());
    driver.unregister_files(vec![fallback]).unwrap();
}

#[cfg(feature = "test-hooks")]
#[test]
fn close_cleanup_failure_does_not_close_reused_raw_fd_on_driver_drop() {
    let Some(mut driver) = new_driver_with_file_table_or_skip(2, FileTableExhaustion::Fail) else {
        return;
    };

    let file = File::open("Cargo.toml").unwrap();
    let raw_fd = file.as_raw_fd();
    mem::forget(file);
    let owned =
        unsafe { OwnedRawHandle::from_raw_owned(RawHandle::new(UringRawHandle::for_file(raw_fd))) };
    let fd = driver
        .register_files(vec![RegisterFd::Owned(owned)])
        .unwrap()
        .into_iter()
        .next()
        .unwrap();

    let hooks = &mut driver as &mut dyn DriverTestHooks;
    hooks.debug_inject_register_files_update_failure(libc::EIO);
    let token = submit_test_op(&mut driver, Close { fd });
    let (closed, cleanup_error) =
        wait_completion_with_driver_error(&mut driver, token, Duration::from_secs(5));
    assert_eq!(closed, 0);
    assert_eq!(cleanup_error, Some(UringError::FileTableQuarantined));

    let replacement = File::open("Cargo.toml").unwrap();
    let duplicate = unsafe { libc::dup2(replacement.as_raw_fd(), raw_fd) };
    assert_eq!(duplicate, raw_fd);
    drop(driver);
    replacement
        .metadata()
        .expect("driver drop must not close a reused raw fd");
}

/// Registers one file past the kernel table and returns its direct descriptor.
fn register_one_beyond(driver: &mut UringDriver<'static>) -> (File, IoFd) {
    let file = File::open("Cargo.toml").unwrap();
    let raw = raw_file(&file);
    let fd = driver
        .register_files(vec![RegisterFd::Borrowed(raw.borrow())])
        .expect("registration must fall back instead of failing")
        .into_iter()
        .next()
        .unwrap();
    assert_eq!(
        fd,
        IoFd::direct(UringRawHandle::for_file(file.as_raw_fd())),
        "a descriptor past the kernel table must carry its own raw fd"
    );
    (file, fd)
}

#[test]
fn a_full_file_table_falls_back_to_unregistered_descriptors() {
    // Capacity 1 is entirely consumed by the waker, so any user file overflows.
    let Some(mut driver) = new_driver_with_file_table_or_skip(1, FileTableExhaustion::Fallback)
    else {
        return;
    };

    let (_file, fd) = register_one_beyond(&mut driver);

    let token = submit_test_op(
        &mut driver,
        Fsync {
            fd,
            datasync: false,
        },
    );
    let result = wait_completion(&mut driver, token, Duration::from_secs(5));
    assert_eq!(result, 0, "fsync on a fallback descriptor must succeed");

    driver.unregister_files(vec![fd]).unwrap();
}

#[test]
fn a_disabled_file_table_serves_every_descriptor_as_a_raw_fd() {
    // Capacity 0 means even the waker eventfd is submitted unregistered.
    let Some(mut driver) = new_driver_with_file_table_or_skip(0, FileTableExhaustion::Fallback)
    else {
        return;
    };

    let (_file, fd) = register_one_beyond(&mut driver);

    let token = submit_test_op(
        &mut driver,
        Fsync {
            fd,
            datasync: false,
        },
    );
    let result = wait_completion(&mut driver, token, Duration::from_secs(5));
    assert_eq!(
        result, 0,
        "fsync without a registered file table must succeed"
    );

    driver.unregister_files(vec![fd]).unwrap();
}

/// Pins the trade a fallback descriptor makes, so it cannot be weakened silently.
///
/// A registered descriptor is an index plus a generation, and releasing its slot bumps that
/// generation so the old descriptor is rejected (see
/// `stale_registered_fd_generation_rejected_on_submit`). A fallback descriptor has no slot to
/// bump: it *is* the fd. Unregistering one therefore leaves nothing behind that a later submit
/// could catch, which is exactly why direct descriptors carry no use-after-close protection.
#[test]
fn a_fallback_descriptor_has_no_generation_to_invalidate() {
    let Some(mut driver) = new_driver_with_file_table_or_skip(1, FileTableExhaustion::Fallback)
    else {
        return;
    };

    let (first, fd) = register_one_beyond(&mut driver);
    assert!(fd.is_direct());
    assert_eq!(fd.fixed_index(), None);
    assert_eq!(fd.generation(), None);

    driver.unregister_files(vec![fd]).unwrap();

    // Registering the *same* still-open file again yields an identical descriptor: there is no
    // generation in it that unregistering could have moved on.
    let raw = raw_file(&first);
    let again = driver
        .register_files(vec![RegisterFd::Borrowed(raw.borrow())])
        .unwrap()
        .into_iter()
        .next()
        .unwrap();
    assert_eq!(again, fd);

    driver.unregister_files(vec![again]).unwrap();
}

#[test]
fn close_owned_fallback_file() {
    let Some(mut driver) = new_driver_with_file_table_or_skip(1, FileTableExhaustion::Fallback)
    else {
        return;
    };

    let file = File::open("Cargo.toml").unwrap();
    let raw_fd = file.as_raw_fd();
    mem::forget(file);
    let owned =
        unsafe { OwnedRawHandle::from_raw_owned(RawHandle::new(UringRawHandle::for_file(raw_fd))) };
    let fd = driver
        .register_files(vec![RegisterFd::Owned(owned)])
        .unwrap()
        .into_iter()
        .next()
        .unwrap();
    assert!(fd.is_direct(), "expected a fallback descriptor");

    let token = submit_test_op(&mut driver, Close { fd });
    let closed = wait_completion(&mut driver, token, Duration::from_secs(5));
    assert_eq!(closed, 0);

    // The kernel already closed this fd, so the driver must have *forgotten* the handle it
    // owned rather than dropped it. Linux hands out the lowest free fd, so reopening usually
    // lands on the very number just released — unregistering the dead descriptor must not
    // close that new file out from under us.
    let reopened = File::open("Cargo.toml").unwrap();
    driver.unregister_files(vec![fd]).unwrap();
    if reopened.as_raw_fd() == raw_fd {
        reopened
            .metadata()
            .expect("closing a retired fallback descriptor must not close the reused fd");
    }
}

#[test]
fn duplicate_owned_direct_registration_preserves_the_existing_owner() {
    let Some(mut driver) = new_driver_with_file_table_or_skip(0, FileTableExhaustion::Fallback)
    else {
        return;
    };

    let file = File::open("Cargo.toml").unwrap();
    let raw_fd = file.as_raw_fd();
    mem::forget(file);
    let first = driver
        .register_files(vec![RegisterFd::Owned(owned_file_fd(raw_fd))])
        .unwrap()
        .into_iter()
        .next()
        .unwrap();
    assert!(first.is_direct());
    assert!(first.direct_owner().is_some());

    let report = driver
        .register_files(vec![RegisterFd::Owned(owned_file_fd(raw_fd))])
        .expect_err("the same raw fd must not receive a second owned registration");
    assert_eq!(*report.inner(), UringError::DuplicateOwnedFd);
    assert!(report.context().contains_key("raw_fd"));
    assert!(unsafe { libc::fcntl(raw_fd, libc::F_GETFD) } >= 0);

    driver.unregister_files(vec![first]).unwrap();
    assert_eq!(unsafe { libc::fcntl(raw_fd, libc::F_GETFD) }, -1);
}

#[test]
fn duplicate_owned_fixed_registration_preserves_the_existing_owner() {
    let Some(mut driver) = new_driver_with_file_table_or_skip(2, FileTableExhaustion::Fail) else {
        return;
    };

    let file = File::open("Cargo.toml").unwrap();
    let raw_fd = file.as_raw_fd();
    mem::forget(file);
    let first = driver
        .register_files(vec![RegisterFd::Owned(owned_file_fd(raw_fd))])
        .unwrap()
        .into_iter()
        .next()
        .unwrap();
    assert!(first.is_registered());

    let report = driver
        .register_files(vec![RegisterFd::Owned(owned_file_fd(raw_fd))])
        .expect_err("a fixed owner must reject a second owned registration");
    assert_eq!(*report.inner(), UringError::DuplicateOwnedFd);
    assert!(report.context().contains_key("existing_file_index"));
    assert!(unsafe { libc::fcntl(raw_fd, libc::F_GETFD) } >= 0);

    driver.unregister_files(vec![first]).unwrap();
    assert_eq!(unsafe { libc::fcntl(raw_fd, libc::F_GETFD) }, -1);
}

#[test]
fn borrowed_and_owned_descriptors_may_share_a_raw_fd() {
    let Some(mut driver) = new_driver_with_file_table_or_skip(0, FileTableExhaustion::Fallback)
    else {
        return;
    };

    let file = File::open("Cargo.toml").unwrap();
    let raw_fd = file.as_raw_fd();
    let raw = raw_file(&file);
    mem::forget(file);
    let descriptors = driver
        .register_files(vec![
            RegisterFd::Borrowed(raw.borrow()),
            RegisterFd::Owned(owned_file_fd(raw_fd)),
        ])
        .unwrap();
    assert_eq!(descriptors.len(), 2);
    assert_eq!(descriptors[0].direct_owner(), None);
    assert!(descriptors[1].direct_owner().is_some());

    driver.unregister_files(descriptors).unwrap();
    assert_eq!(unsafe { libc::fcntl(raw_fd, libc::F_GETFD) }, -1);
}

#[test]
fn stale_owned_direct_descriptor_cannot_unregister_or_submit_to_a_reused_fd() {
    let Some(mut driver) = new_driver_with_file_table_or_skip(0, FileTableExhaustion::Fallback)
    else {
        return;
    };

    let first_file = File::open("Cargo.toml").unwrap();
    let raw_fd = first_file.as_raw_fd();
    mem::forget(first_file);
    let replacement_source = File::open("Cargo.toml").unwrap();
    assert_ne!(replacement_source.as_raw_fd(), raw_fd);

    let first = driver
        .register_files(vec![RegisterFd::Owned(owned_file_fd(raw_fd))])
        .unwrap()
        .into_iter()
        .next()
        .unwrap();
    driver.unregister_files(vec![first]).unwrap();

    assert_eq!(
        unsafe { libc::dup2(replacement_source.as_raw_fd(), raw_fd) },
        raw_fd
    );
    drop(replacement_source);
    let replacement = driver
        .register_files(vec![RegisterFd::Owned(owned_file_fd(raw_fd))])
        .unwrap()
        .into_iter()
        .next()
        .unwrap();
    assert_ne!(first.direct_owner(), replacement.direct_owner());
    assert!(unsafe { libc::fcntl(raw_fd, libc::F_GETFD) } >= 0);

    driver.unregister_files(vec![first]).unwrap();
    assert!(unsafe { libc::fcntl(raw_fd, libc::F_GETFD) } >= 0);
    assert_stale_fsync_is_rejected(&mut driver, first);
    assert_close_is_rejected_with(&mut driver, first, UringError::InvalidInput);

    driver.unregister_files(vec![replacement]).unwrap();
    assert_eq!(unsafe { libc::fcntl(raw_fd, libc::F_GETFD) }, -1);
}

#[test]
fn duplicate_owned_batch_is_rejected_before_claiming_fixed_slots() {
    let Some(mut driver) = new_driver_with_file_table_or_skip(2, FileTableExhaustion::Fail) else {
        return;
    };

    let duplicate_file = File::open("Cargo.toml").unwrap();
    let duplicate_fd = duplicate_file.as_raw_fd();
    mem::forget(duplicate_file);
    let report = driver
        .register_files(vec![
            RegisterFd::Owned(owned_file_fd(duplicate_fd)),
            RegisterFd::Owned(owned_file_fd(duplicate_fd)),
        ])
        .expect_err("a duplicate owned fd in one batch must be rejected");
    assert_eq!(*report.inner(), UringError::DuplicateOwnedFd);
    assert_eq!(unsafe { libc::fcntl(duplicate_fd, libc::F_GETFD) }, -1);

    let valid_file = File::open("Cargo.toml").unwrap();
    let valid_fd = valid_file.as_raw_fd();
    mem::forget(valid_file);
    let valid = driver
        .register_files(vec![RegisterFd::Owned(owned_file_fd(valid_fd))])
        .unwrap()
        .into_iter()
        .next()
        .unwrap();
    assert!(
        valid.is_registered(),
        "the rejected batch must not claim a slot"
    );
    driver.unregister_files(vec![valid]).unwrap();
    assert_eq!(unsafe { libc::fcntl(valid_fd, libc::F_GETFD) }, -1);
}

#[test]
fn close_borrowed_registered_file_is_rejected() {
    let Some(mut driver) = new_driver_or_skip() else {
        return;
    };

    let file = File::open("Cargo.toml").unwrap();
    let raw = raw_file(&file);
    let fd = driver
        .register_files(vec![RegisterFd::Borrowed(raw.borrow())])
        .unwrap()
        .into_iter()
        .next()
        .unwrap();

    let op = Close { fd };
    let (uring_kernel, payload) =
        <Close as IntoPlatformOp<UringSlotSpec>>::into_kernel_and_payload(op);
    let mut uring_op: Option<UringOp> = Some(uring_kernel);
    let mut slot = driver.reserve_op().expect("reserve op failed");
    slot.set_payload(<Close as IntoPlatformOp<UringSlotSpec>>::payload_into_erased(payload));

    match slot.submit(&mut uring_op) {
        DriverSubmitResult::Failed {
            report,
            status: SubmitStatus::Void,
        } => {
            assert_eq!(*report.inner(), UringError::InvalidInput);
        }
        DriverSubmitResult::Failed { status, .. } => {
            panic!("borrowed Close should fail before in-flight state, got {status:?}")
        }
        DriverSubmitResult::Submitted(_) => panic!("borrowed Close unexpectedly succeeded"),
    }

    let recovered = slot.recover_payload();
    assert!(
        recovered.is_some_and(|payload| {
            <Close as IntoPlatformOp<UringSlotSpec>>::try_record_from_erased(payload).is_ok()
        }),
        "payload should be recoverable after void failure"
    );

    driver.unregister_files(vec![fd]).unwrap();
}
