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

// Re-exports used by the Windows backend and its callers.
pub use config::{
    BorrowedRawHandle, BufferRegistrationMode, IoFd, IocpConfig, IocpHandle, OwnedRawHandle,
    RawHandle, RawHandleKind, RegisteredHandle, SocketKey,
};
pub use diagnostics::{
    IocpCompletionDiagnostics, IocpCompletionDiagnosticsSnapshot, RioCompletionDiagnosticsSnapshot,
};
pub use driver::{CloseMode, IocpDriver, IocpOpState};
pub use error::{IocpError, IocpResult};
pub use net::addr::{SockAddrStorage, socket_addr_to_storage, to_socket_addr};
pub use net::socket::Socket;
pub use op::{IocpKernelOp, IocpOp, IocpUserPayload};
pub use win32::{IoCompletionPort, OwnedHandle, SafeSocket};
