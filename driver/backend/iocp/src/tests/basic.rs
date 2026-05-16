use crate::config::IocpConfig;
use crate::driver::IocpDriver;
use crate::ext::Extensions;
use std::os::windows::io::AsRawHandle;
use veloq_buf::NoopRegistrar;
use veloq_driver_core::driver::RegisterFd;
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
            driver.registered_files[idx],
            Some(crate::config::RegisteredHandle::Weak(_))
        ),
        "borrowed file registration must not transfer ownership to driver"
    );
}

#[test]
fn test_rio_extensions_load() {
    init_winsock();
    let _ext = Extensions::new().expect("RIO Extensions should load");
}
