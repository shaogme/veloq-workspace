pub mod config;
pub mod driver;
pub mod op;

#[cfg(unix)]
mod unix;
#[cfg(unix)]
pub use unix::Socket;

#[cfg(windows)]
mod windows;
#[cfg(windows)]
pub use windows::Socket;

#[cfg(unix)]
pub use unix::{RawHandle, SockAddrStorage, socket_addr_to_storage, to_socket_addr};
#[cfg(windows)]
pub use windows::{
    RawHandle, SOCKADDR_STORAGE as SockAddrStorage, socket_addr_to_storage, to_socket_addr,
};

unsafe impl std::marker::Send for RawHandle {}
unsafe impl std::marker::Sync for RawHandle {}
