use crate::config::IocpConfig;
use crate::driver::IocpDriver;
use crate::ext::Extensions;
use std::io;
use std::os::windows::io::AsRawHandle;
use veloq_driver_core::driver::RegisterFd;

#[test]
fn test_extensions_load() {
    let ext = Extensions::new();
    assert!(ext.is_ok(), "Extensions should load on Windows");
}

#[test]
fn test_driver_creation() {
    let _driver: Result<IocpDriver, io::Error> = IocpDriver::new(IocpConfig::default());
    assert!(_driver.is_ok(), "Driver should be created");
}

#[test]
fn test_register_files() {
    let mut driver = IocpDriver::new(IocpConfig::default()).unwrap();
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
    let mut driver = IocpDriver::new(IocpConfig::default()).unwrap();
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
    let _ext = Extensions::new().expect("RIO Extensions should load");
}

#[test]
fn test_socket_lifecycle_control_cleanup_unreg() {
    let mut driver = IocpDriver::new(IocpConfig::default()).unwrap();
    let socket = crate::Socket::new_tcp_v4().unwrap();
    let raw = socket.into_owned_raw();
    let fd = driver
        .register_files(vec![RegisterFd::Borrowed(raw.borrow())])
        .unwrap()
        .into_iter()
        .next()
        .unwrap();

    let lifecycle = driver.socket_lifecycle_handle();
    lifecycle
        .schedule_socket_cleanup(crate::RawHandle::new(raw.raw()), Some(fd))
        .unwrap();
    let _ = driver.poll_completion().unwrap();

    let idx = fd.fixed_index() as usize;
    assert!(driver.registered_files[idx].is_none());
    assert!(driver.free_slots.contains(&idx));
}
