#![cfg(feature = "std")]

use veloq_std::{
    cmp,
    ffi::CString,
    hint,
    io::{Cursor, Error, ErrorKind, Read, Result, Seek, Stderr, Stdin, Stdout, Write},
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
    assert_seek::<Cursor<Vec<u8>>>();
    assert_write::<Vec<u8>>();
    assert_read::<Stdin>();
    assert_write::<Stdout>();
    assert_write::<Stderr>();
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

#[cfg(windows)]
#[test]
fn windows_socket_try_clone_test() {
    use std::net::TcpListener as StdTcpListener;
    use veloq_std::os::windows::io::{AsRawSocket, AsSocket, OwnedSocket};

    let listener = StdTcpListener::bind("127.0.0.1:0").expect("bind listener");
    let borrowed = listener.as_socket();
    let cloned_owned: OwnedSocket = borrowed.try_clone_to_owned().expect("try_clone_to_owned");
    let cloned_again = cloned_owned.try_clone().expect("OwnedSocket::try_clone");
    assert_ne!(cloned_owned.as_raw_socket(), cloned_again.as_raw_socket());
}
