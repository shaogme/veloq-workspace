use crate::config::IocpConfig;
use crate::driver::IocpDriver;
use crate::ext::Extensions;
use std::io;

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
    let raw = crate::config::RawHandle {
        handle: std::os::windows::io::AsRawHandle::as_raw_handle(&handle) as _,
    };
    let fds = driver.register_files(&[raw]).unwrap();
    assert_eq!(fds.len(), 1);
    driver.unregister_files(fds).unwrap();
}

#[test]
fn test_rio_extensions_load() {
    let _ext = Extensions::new().expect("RIO Extensions should load");
}
