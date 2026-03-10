pub mod op_registry {
    pub use veloq_driver_core::op_registry::*;
}

pub mod slot {
    pub use veloq_driver_core::slot::*;
}

pub use veloq_driver_core::driver::{
    CompletionEvent, CompletionRecord, CompletionSidecar, CompletionTable, Driver, Outcome,
    PlatformOp, RemoteWaker, SharedCompletionQueue, SharedCompletionTable, SubmitBinder,
    decode_completion_token, encode_completion_token, event_res_to_io,
};

#[cfg(target_os = "linux")]
pub(crate) mod uring;

#[cfg(target_os = "linux")]
pub use uring::UringDriver as PlatformDriver;

#[cfg(target_os = "windows")]
pub(crate) mod iocp;

#[cfg(target_os = "windows")]
pub use iocp::CloseMode;
#[cfg(target_os = "windows")]
pub use iocp::IocpDriver as PlatformDriver;

#[cfg(feature = "test-hooks")]
pub use veloq_driver_core::driver::test_hooks;
