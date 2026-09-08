#![cfg_attr(not(test), no_std)]

pub mod config;
pub mod driver;
pub mod error;
pub mod multishot;
pub mod op;

#[cfg(unix)]
pub use veloq_driver_uring::{
    BorrowedRawHandle, OwnedRawHandle, RawHandle, RawHandleKind, SockAddrStorage, Socket,
    peer_addr_of_handle, socket_addr_to_storage, to_socket_addr,
};

#[cfg(windows)]
pub use veloq_driver_iocp::{
    BorrowedRawHandle, OwnedRawHandle, RawHandle, RawHandleKind, SockAddrStorage, Socket,
    peer_addr_of_handle, socket_addr_to_storage, to_socket_addr,
};
