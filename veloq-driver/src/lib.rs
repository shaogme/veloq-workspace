pub mod config;
pub mod driver;
pub mod op;

#[cfg(unix)]
pub use veloq_driver_uring::{
    RawHandle, SockAddrStorage, Socket, socket_addr_to_storage, to_socket_addr,
};

#[cfg(windows)]
pub use veloq_driver_iocp::{
    RawHandle, SockAddrStorage, Socket, socket_addr_to_storage, to_socket_addr,
};
