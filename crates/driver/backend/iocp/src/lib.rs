#![cfg(target_os = "windows")]
#![no_std]

mod common;
mod config;
mod diagnostics;
mod driver;
mod error;
mod ext;
mod net;
mod op;
mod rio;
mod win32;

#[cfg(test)]
mod tests;

use veloq_std::net::SocketAddr;

// Re-exports used by the Windows backend and its callers.
pub use config::{
    BorrowedRawHandle, DirectOwnerId, IoFd, IocpConfig, IocpHandle, OwnedRawHandle, RawHandle,
    RawHandleKind, RegisteredHandle, SocketKey,
};
pub use diagnostics::{
    IocpCompletionDiagnostics, IocpCompletionDiagnosticsSnapshot, RioCompletionDiagnosticsSnapshot,
};
pub use driver::{CloseMode, IocpDriver, IocpOpState};
pub use error::{IocpError, IocpResult};
pub use net::{SockAddrStorage, Socket};
pub use op::{IocpKernelOp, IocpOp, IocpSlotSpec, IocpUserPayload};
pub use win32::{IoCompletionPort, OwnedHandle, SafeSocket};

pub fn socket_addr_to_storage(addr: SocketAddr) -> (SockAddrStorage, i32) {
    SockAddrStorage::from_socket_addr(addr)
}

pub fn to_socket_addr(buf: &[u8]) -> IocpResult<SocketAddr> {
    SockAddrStorage::to_socket_addr(buf)
}

pub fn peer_addr_of_handle(handle: IocpHandle) -> IocpResult<SocketAddr> {
    Socket::peer_addr_of_handle(handle)
}
