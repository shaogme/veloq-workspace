pub mod common;
pub mod config;
pub mod driver;
pub mod error;
pub mod ext;
pub mod net;
pub mod op;
pub mod rio;
pub mod win32;

#[cfg(test)]
pub mod tests;

// Re-exports used by the Windows backend and its callers.
pub use config::{
    BorrowedRawHandle, BufferRegistrationMode, IoFd, IocpConfig, IocpHandle, OwnedRawHandle,
    RawHandle, RawHandleKind, RegisteredHandle, SocketKey,
};
pub use driver::{CloseMode, IocpDriver, IocpOpState};
pub use error::{IocpError, IocpResult};
pub use net::addr::{SockAddrStorage, socket_addr_to_storage, to_socket_addr};
pub use net::socket::Socket;
pub use win32::{IoCompletionPort, OwnedHandle, SafeSocket};
