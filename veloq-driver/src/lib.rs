pub mod config;
pub mod driver;
pub mod op;

pub use veloq_driver_core::error;

#[cfg(unix)]
pub use veloq_driver_uring::{
    BorrowedRawHandle, OwnedRawHandle, RawHandle, RawHandleKind, SockAddrStorage, Socket,
    socket_addr_to_storage, to_socket_addr,
};

#[cfg(windows)]
pub use veloq_driver_iocp::{
    BorrowedRawHandle, OwnedRawHandle, RawHandle, RawHandleKind, SockAddrStorage, Socket,
    socket_addr_to_storage, to_socket_addr,
};
