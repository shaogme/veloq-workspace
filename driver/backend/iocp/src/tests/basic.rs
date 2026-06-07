use crate::config::IocpConfig;
use crate::driver::IocpDriver;
use crate::ext::Extensions;
use std::os::windows::io::AsRawHandle;
use veloq_buf::NoopRegistrar;
use veloq_driver_core::driver::{Driver, DriverSubmitResult, RegisterFd, SubmitStatus};
use veloq_driver_core::op::{Fsync, IntoPlatformOp};
use windows_sys::Win32::Networking::WinSock::{WSADATA, WSAStartup};

fn init_winsock() {
    // Ensure Winsock is initialized for the current process/thread.
    // WSAStartup is reference-counted and safe to call multiple times.
    unsafe {
        let mut data: WSADATA = std::mem::zeroed();
        let _ = WSAStartup(0x0202, &mut data);
    }
}

#[test]
fn test_extensions_load() {
    init_winsock();
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
            driver.handles.registered_file(idx),
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
    let (user_data, _) = driver.reserve_op().expect("reserve op failed");
    driver.slot_set_payload(
        user_data,
        <Fsync as IntoPlatformOp<crate::IocpOp>>::payload_into_erased(payload),
    );

    match driver.submit(user_data, &mut iocp_op) {
        DriverSubmitResult::Failed {
            report: _,
            status: SubmitStatus::Void,
        } => {}
        DriverSubmitResult::Failed { status, .. } => {
            panic!("stale fd submit should fail before in-flight state, got {status:?}")
        }
        DriverSubmitResult::Submitted(_) => panic!("stale fd submit unexpectedly succeeded"),
    }

    let recovered = driver.slot_take_payload(user_data);
    assert!(
        recovered.is_some(),
        "payload should be recoverable after void failure"
    );
    driver.unregister_files(vec![fresh_fd]).unwrap();
}

#[test]
fn test_rio_extensions_load() {
    init_winsock();
    let _ext = Extensions::new().expect("RIO Extensions should load");
}
