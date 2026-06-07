use crate::config::IocpConfig;
use crate::driver::IocpDriver;
use crate::ext::Extensions;
use crate::tests::{submit_test_op, wait_completion};
use std::os::windows::io::{AsRawHandle, IntoRawHandle};
use std::time::Duration;
use veloq_buf::NoopRegistrar;
use veloq_driver_core::driver::{Driver, DriverSubmitResult, RegisterFd, SubmitStatus};
use veloq_driver_core::op::{Close, Fsync, IntoPlatformOp};
use windows_sys::Win32::Networking::WinSock::{WSACleanup, WSADATA, WSAStartup};

struct TestWinsockGuard;

impl Drop for TestWinsockGuard {
    fn drop(&mut self) {
        unsafe {
            WSACleanup();
        }
    }
}

fn init_winsock() -> TestWinsockGuard {
    // Ensure Winsock is initialized for the current process/thread.
    // WSAStartup is reference-counted and paired with TestWinsockGuard cleanup.
    unsafe {
        let mut data: WSADATA = std::mem::zeroed();
        let ret = WSAStartup(0x0202, &mut data);
        assert_eq!(ret, 0, "WSAStartup failed: {ret}");
    }
    TestWinsockGuard
}

fn submit_expect_void_failure<T>(driver: &mut IocpDriver<'_>, op: T, context: &str)
where
    T: IntoPlatformOp<
            crate::IocpOp,
            DriverCompletion = usize,
            ErasedPayload = crate::IocpUserPayload,
            Error = crate::IocpError,
        >,
{
    let (iocp_kernel, payload) = op.into_kernel_and_payload();
    let mut iocp_op = Some(iocp_kernel);
    let mut slot = driver.reserve_op().expect("reserve op failed");
    slot.set_payload(T::payload_into_erased(payload));

    match slot.submit(&mut iocp_op) {
        DriverSubmitResult::Failed {
            report: _,
            status: SubmitStatus::Void,
        } => {}
        DriverSubmitResult::Failed { status, .. } => {
            panic!("{context}: submit should fail before in-flight state, got {status:?}")
        }
        DriverSubmitResult::Submitted(_) => panic!("{context}: submit unexpectedly succeeded"),
    }

    let recovered = slot.recover_payload();
    assert!(
        recovered.is_some(),
        "{context}: payload should be recoverable after void failure"
    );
}

#[test]
fn test_extensions_load() {
    let _winsock = init_winsock();
    let ext = Extensions::new();
    assert!(ext.is_ok(), "Extensions should load on Windows");
}

#[test]
fn test_driver_creation() {
    let _driver = IocpDriver::new(IocpConfig::default(), Box::new(NoopRegistrar));
    assert!(_driver.is_ok(), "Driver should be created");
}

#[test]
fn test_register_files() {
    let mut driver = IocpDriver::new(IocpConfig::default(), Box::new(NoopRegistrar)).unwrap();
    let handle = std::fs::File::open("Cargo.toml").unwrap();
    let raw = crate::config::RawHandle::new(crate::config::IocpHandle::for_file(
        handle.as_raw_handle() as _,
    ));
    let fds = driver
        .register_files(vec![RegisterFd::Borrowed(raw.borrow())])
        .unwrap();
    assert_eq!(fds.len(), 1);
    driver.unregister_files(fds).unwrap();
}

#[test]
fn test_register_borrowed_file_keeps_weak_ownership() {
    let mut driver = IocpDriver::new(IocpConfig::default(), Box::new(NoopRegistrar)).unwrap();
    let handle = std::fs::File::open("Cargo.toml").unwrap();
    let raw = crate::config::RawHandle::new(crate::config::IocpHandle::for_file(
        handle.as_raw_handle() as _,
    ));
    let fd = driver
        .register_files(vec![RegisterFd::Borrowed(raw.borrow())])
        .unwrap()
        .into_iter()
        .next()
        .unwrap();

    let idx = fd.fixed_index() as usize;

    assert!(
        matches!(
            driver.debug_registered_file(idx),
            Some(crate::config::RegisteredHandle::Weak(_))
        ),
        "borrowed file registration must not transfer ownership to driver"
    );
}

#[test]
fn test_stale_registered_fd_generation_rejected_on_submit() {
    let mut driver = IocpDriver::new(IocpConfig::default(), Box::new(NoopRegistrar)).unwrap();
    let first = std::fs::File::open("Cargo.toml").unwrap();
    let first_raw = crate::config::RawHandle::new(crate::config::IocpHandle::for_file(
        first.as_raw_handle() as _,
    ));
    let stale_fd = driver
        .register_files(vec![RegisterFd::Borrowed(first_raw.borrow())])
        .unwrap()
        .into_iter()
        .next()
        .unwrap();
    driver.unregister_files(vec![stale_fd]).unwrap();

    let second = std::fs::File::open("Cargo.toml").unwrap();
    let second_raw = crate::config::RawHandle::new(crate::config::IocpHandle::for_file(
        second.as_raw_handle() as _,
    ));
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
    let (iocp_kernel, payload) = op.into_kernel_and_payload();
    let mut iocp_op = Some(iocp_kernel);
    let mut slot = driver.reserve_op().expect("reserve op failed");
    slot.set_payload(<Fsync as IntoPlatformOp<crate::IocpOp>>::payload_into_erased(payload));

    match slot.submit(&mut iocp_op) {
        DriverSubmitResult::Failed {
            report: _,
            status: SubmitStatus::Void,
        } => {}
        DriverSubmitResult::Failed { status, .. } => {
            panic!("stale fd submit should fail before in-flight state, got {status:?}")
        }
        DriverSubmitResult::Submitted(_) => panic!("stale fd submit unexpectedly succeeded"),
    }

    let recovered = slot.recover_payload();
    assert!(
        recovered.is_some(),
        "payload should be recoverable after void failure"
    );
    driver.unregister_files(vec![fresh_fd]).unwrap();
}

#[test]
fn test_close_owned_registered_file_unregisters_and_rejects_stale_fd() {
    let mut driver = IocpDriver::new(IocpConfig::default(), Box::new(NoopRegistrar)).unwrap();
    let handle = std::fs::File::open("Cargo.toml").unwrap();
    let raw = crate::RawHandle::new(crate::IocpHandle::for_file(handle.into_raw_handle() as _));
    let owned = unsafe { crate::OwnedRawHandle::from_raw_owned(raw) };
    let fd = driver
        .register_files(vec![RegisterFd::Owned(owned)])
        .unwrap()
        .into_iter()
        .next()
        .unwrap();
    let idx = fd.fixed_index() as usize;

    let (user_data, generation) = submit_test_op(&mut driver, Close { fd });
    let closed = wait_completion(&mut driver, user_data, generation, Duration::from_secs(5))
        .expect("close completion failed");
    assert_eq!(closed, 0);
    assert!(
        driver.debug_registered_file(idx).is_none(),
        "Close must remove owned registered file from registry"
    );

    submit_expect_void_failure(
        &mut driver,
        Fsync {
            fd,
            datasync: false,
        },
        "stale fd after Close",
    );
}

#[test]
fn test_close_borrowed_registered_file_is_rejected_without_unregistering() {
    let mut driver = IocpDriver::new(IocpConfig::default(), Box::new(NoopRegistrar)).unwrap();
    let handle = std::fs::File::open("Cargo.toml").unwrap();
    let raw = crate::RawHandle::new(crate::IocpHandle::for_file(handle.as_raw_handle() as _));
    let fd = driver
        .register_files(vec![RegisterFd::Borrowed(raw.borrow())])
        .unwrap()
        .into_iter()
        .next()
        .unwrap();
    let idx = fd.fixed_index() as usize;

    submit_expect_void_failure(&mut driver, Close { fd }, "borrowed fd Close");
    assert!(
        matches!(
            driver.debug_registered_file(idx),
            Some(crate::RegisteredHandle::Weak(_))
        ),
        "borrowed Close must leave the weak registry entry intact"
    );

    driver.unregister_files(vec![fd]).unwrap();
}

#[test]
fn test_rio_extensions_load() {
    let _winsock = init_winsock();
    let _ext = Extensions::new().expect("RIO Extensions should load");
}
