//! Veloq's minimal `io_uring` userspace ABI.
//!
//! Stages 3 and 4 own ring setup, queue access, submission, and resource registration.

#![cfg(any(target_os = "linux", target_os = "android"))]
#![cfg_attr(not(feature = "std"), no_std)]
#![deny(warnings)]

mod internal;
pub(crate) mod sys;

pub use internal::{
    Builder, EnterArgs, EnterFlags, IoUring, KernelCapabilities, OpcodeProbeSnapshot, Parameters,
    Probe, ProbeStatus, ResourceKind, ResourceLayout, ResourceRegistration,
    ResourceRegistrationCapability, ResourceRegistrationState, RingConfig, RingLayout, RingParts,
    SUPPORTED_ABI_ARCHITECTURES, SetupFlags, SetupPolicy, SubmissionQueue, SubmitError,
    SubmitReceipt, SubmitResult, Submitter,
};

use veloq_std::io;

#[cfg(not(any(
    target_arch = "aarch64",
    target_arch = "loongarch64",
    target_arch = "powerpc64",
    target_arch = "riscv64",
    target_arch = "x86_64",
)))]
compile_error!(
    "veloq-io-uring ABI is supported only on x86_64, aarch64, riscv64, loongarch64, and powerpc64"
);

/// The error boundary for every syscall and memory-mapping operation.
pub type Result<T> = io::Result<T>;

pub mod cqueue {
    pub use crate::internal::{CompletionQueue, CqueueEntry as Entry, CqueueFlags as Flags};
}

pub mod opcode {
    pub use crate::internal::{
        Accept, AcceptMulti, AsyncCancel, Close, Connect, Fallocate, Fsync, Nop, OpenAt, Read,
        ReadFixed, Recv, RecvMsg, RecvMsgMulti, RecvMulti, Send, SendMsg, SyncFileRange, Timeout,
        Write, WriteFixed,
    };
}

pub mod squeue {
    pub use crate::internal::{
        PushError, SubmissionEntry as Entry, SubmissionFlags as Flags, SubmissionQueue,
    };
}

pub mod types {
    pub use crate::internal::{
        AsyncCancelFlags, BufRingEntry, BufRingItem, Fd, Fixed, FsyncFlags, ProvidedBufRing,
        SubmitArgs, TimeoutFlags, Timespec, UseFixed,
    };
}
