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
pub use veloq_driver_uring::UringDriver as PlatformDriver;

#[cfg(target_os = "windows")]
pub use veloq_driver_iocp::CloseMode;
#[cfg(target_os = "windows")]
pub use veloq_driver_iocp::IocpDriver as PlatformDriver;

pub use veloq_driver_core::driver::test_hooks;
