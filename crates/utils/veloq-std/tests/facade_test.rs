#![cfg(feature = "std")]

use veloq_std::{
    cmp,
    ffi::CString,
    hint,
    io::{Error, ErrorKind, Read, Result, Seek, Write},
    net::{TcpListener, TcpStream, UdpSocket},
};

#[test]
fn core_facade_exports_are_available() {
    assert_eq!(cmp::min(3, 5), 3);
    assert_eq!(hint::black_box(7), 7);

    let c_string = CString::new("veloq").expect("CString export should accept strings");
    assert_eq!(c_string.to_bytes(), b"veloq");
}

#[test]
fn std_facade_exports_are_available() {
    fn assert_read<T: Read>() {}
    fn assert_seek<T: Seek>() {}
    fn assert_write<T: Write>() {}
    fn assert_send_sync<T: Send + Sync>() {}

    assert_read::<&[u8]>();
    assert_seek::<std::io::Cursor<Vec<u8>>>();
    assert_write::<Vec<u8>>();
    assert_send_sync::<TcpListener>();
    assert_send_sync::<TcpStream>();
    assert_send_sync::<UdpSocket>();

    let result: Result<()> = Ok(());
    assert!(result.is_ok());
    assert_eq!(
        Error::from(ErrorKind::WouldBlock).kind(),
        ErrorKind::WouldBlock
    );
}

#[cfg(unix)]
#[test]
fn unix_fd_facade_exports_are_available() {
    use veloq_std::os::fd::{AsRawFd, RawFd};

    let file = std::fs::File::open("Cargo.toml").expect("workspace manifest should exist");
    let fd: RawFd = file.as_raw_fd();
    assert!(fd >= 0);
}

#[cfg(windows)]
#[test]
fn windows_handle_facade_exports_are_available() {
    use veloq_std::os::windows::io::{
        AsRawHandle, AsRawSocket, IntoRawHandle, IntoRawSocket, RawHandle, RawSocket,
    };

    fn assert_handle<T: AsRawHandle>() {}
    fn assert_socket<T: AsRawSocket>() {}
    fn assert_into_handle<T: IntoRawHandle>() {}
    fn assert_into_socket<T: IntoRawSocket>() {}

    assert_handle::<std::fs::File>();
    assert_into_handle::<std::fs::File>();
    assert_socket::<std::net::TcpStream>();
    assert_into_socket::<std::net::TcpStream>();

    let _: Option<RawHandle> = None;
    let _: Option<RawSocket> = None;
}
