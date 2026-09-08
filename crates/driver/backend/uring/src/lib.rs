#![cfg_attr(not(test), no_std)]
#![deny(warnings)]

mod config;
mod diagnostics;
mod driver;
mod error;
mod net;
mod op;

pub use config::{
    BorrowedRawHandle, BufferRegistrationMode, FileTableExhaustion, IoFd, IoMode,
    MAX_PROVIDED_BUF_ENTRIES, OwnedRawHandle, ProvidedBufConfig, RawHandle, RawHandleKind,
    SockAddrStorage, UringConfig, UringRawHandle,
};
pub use diagnostics::{UringCompletionDiagnostics, UringCompletionDiagnosticsSnapshot};
pub use driver::{ProvidedBufStats, UringDriver, UringOpState};
pub use error::{UringError, UringResult};
pub use net::{Socket, peer_addr_of_handle, socket_addr_to_storage, to_socket_addr};
pub use op::{UringOp, UringSlotSpec, UringUserPayload};
