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
    CompletionRecord, CompletionValue, DriveMode, Driver, DriverSubmitResult, PollRecordResult,
    RegisterFd, SubmitStatus,
};
use veloq_driver_core::op::{
    IntoPlatformOp,
    types::{Close as CoreClose, Fsync as CoreFsync},
};
use veloq_driver_uring::{
    FileTableExhaustion, IoFd, OwnedRawHandle, RawHandle, UringConfig, UringDriver, UringError,
    UringOp, UringRawHandle, UringResult, UringSlotSpec, UringUserPayload,
};

type Close = CoreClose<UringRawHandle>;
type Fsync = CoreFsync<UringRawHandle>;

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

/// A driver whose kernel file table holds `capacity` entries, one of which the eventfd waker
/// claims during construction.
fn new_driver_with_file_table_or_skip(
    capacity: u32,
    exhaustion: FileTableExhaustion,
) -> Option<UringDriver<'static>> {
    let config = UringConfig {
        entries: NonZeroU32::new(64).unwrap(),
        file_table_capacity: capacity,
        file_table_exhaustion: exhaustion,
        ..UringConfig::default()
    };
    static REGISTRAR: NoopRegistrar = NoopRegistrar;
    match UringDriver::new(config, &REGISTRAR) {
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
    assert_ne!(stale_fd.generation(), fresh_fd.generation());

    assert_stale_fsync_is_rejected(&mut driver, stale_fd);

    driver.unregister_files(vec![fresh_fd]).unwrap();
}

/// Submits an `Fsync` on `fd` and asserts it is rejected before going in flight.
fn assert_stale_fsync_is_rejected(driver: &mut UringDriver<'static>, fd: IoFd) {
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
}

#[test]
fn failed_single_registration_restores_popped_slot() {
    let Some(mut driver) = new_driver_with_file_table_or_skip(4, FileTableExhaustion::Fail) else {
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
    let Some(mut driver) = new_driver_with_file_table_or_skip(4, FileTableExhaustion::Fail) else {
        return;
    };

    let first = File::open("Cargo.toml").unwrap();
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
    let index = fd
        .fixed_index()
        .expect("a default file table registers descriptors");

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
        matches!(recovered, Some(UringUserPayload::Close(_))),
        "payload should be recoverable after void failure"
    );

    driver.unregister_files(vec![fd]).unwrap();
}
