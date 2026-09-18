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

pub use config::{
    BorrowedRawHandle, BufferRegistrationMode, DirectOwnerId, FileTableExhaustion, IoFd, IoMode,
    MAX_PROVIDED_BUF_ENTRIES, OwnedRawHandle, ProvidedBufConfig, RawHandle, RawHandleKind,
    SetupFlags, SetupPolicy, SockAddrStorage, UringConfig, UringDriveLimits, UringRawHandle,
};
pub use diagnostics::{
    KERNEL_BASELINE, UringCapabilitySnapshot, UringCompletionDiagnostics,
    UringCompletionDiagnosticsSnapshot, UringSetupSnapshot,
};
pub use driver::{ProvidedBufStats, UringDriver, UringOpState};
pub use error::{UringError, UringResult};
pub use net::{Socket, peer_addr_of_handle, socket_addr_to_storage, to_socket_addr};
pub use op::{UringOp, UringSlotSpec, UringUserPayload};
