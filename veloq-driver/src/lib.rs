pub mod config;
pub mod driver;
pub mod op;

#[cfg(unix)]
pub use veloq_driver_uring::{RawHandle, SockAddrStorage};

#[cfg(windows)]
pub use veloq_driver_iocp::{RawHandle, SockAddrStorage};

#[cfg(unix)]
mod unix;
#[cfg(unix)]
pub use unix::Socket;

#[cfg(windows)]
mod windows;
#[cfg(windows)]
pub use windows::Socket;

#[cfg(unix)]
pub use unix::{socket_addr_to_storage, to_socket_addr};
#[cfg(windows)]
pub use windows::{socket_addr_to_storage, to_socket_addr};
