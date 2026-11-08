//! Linux `io_uring` 驱动后端。
//!
//! [`UringDriver`] 暴露分离的 capability baseline/effective 视图、完成诊断和
//! provided-buffer 统计快照；快照类型的字段保持不可变且私有，调用方应通过其访问器读取
//! 值，以避免依赖内部 bookkeeping 布局。

#![cfg(any(target_os = "linux", target_os = "android"))]
#![no_std]
#![deny(warnings)]

mod config;
mod diagnostics;
mod driver;
mod error;
mod net;
mod op;

#[cfg(test)]
mod test_alloc;

use libc::socklen_t;
use veloq_std::net::SocketAddr;

pub use config::{
    BorrowedRawHandle, DirectOwnerId, FileTableExhaustion, IoFd, IoMode, MAX_PROVIDED_BUF_ENTRIES,
    OwnedRawHandle, ProvidedBufConfig, RawHandle, RawHandleKind, SetupFlags, SetupPolicy,
    SockAddrStorage, UringConfig, UringDriveLimits, UringRawHandle,
};
pub use diagnostics::{
    KERNEL_BASELINE, UringCompletionDiagnostics, UringCompletionDiagnosticsSnapshot,
};
pub use driver::{ProvidedBufferSnapshot, UringDriver, UringOpState};
pub use error::{UringError, UringResult};
pub use net::Socket;
pub use op::{UringOp, UringSlotSpec, UringUserPayload};

pub fn peer_addr_of_handle(handle: UringRawHandle) -> UringResult<SocketAddr> {
    Socket::peer_addr_of_handle(handle)
}

pub fn socket_addr_to_storage(addr: SocketAddr) -> (SockAddrStorage, socklen_t) {
    SockAddrStorage::from_socket_addr(addr)
}

pub fn to_socket_addr(buf: &[u8]) -> UringResult<SocketAddr> {
    SockAddrStorage::decode(buf)
}

#[cfg(test)]
pub(crate) mod test_serial {
    use veloq_std::sync::NativeMutex;

    pub(crate) static FD_TEST_LOCK: NativeMutex<()> = NativeMutex::new(());
}
