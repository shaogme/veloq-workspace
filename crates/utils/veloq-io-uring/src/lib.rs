//! Veloq's minimal `io_uring` userspace ABI.
//!
//! Stages 3 and 4 own ring setup, queue access, submission, and resource registration.

#![cfg(any(target_os = "linux", target_os = "android"))]
#![cfg_attr(not(feature = "std"), no_std)]
#![deny(warnings)]

use veloq_std::io;

/// The error boundary for every syscall and memory-mapping operation.
pub type Result<T> = io::Result<T>;

pub mod cqueue;
pub mod opcode;
pub mod squeue;
pub mod types;

mod mmap;
mod register;
mod ring;
mod submit;
mod sys;

pub use register::{
    KernelCapabilities, OpcodeProbeSnapshot, Probe, ProbeStatus, ResourceKind, ResourceLayout,
    ResourceRegistration, ResourceRegistrationCapability, ResourceRegistrationState,
    SUPPORTED_ABI_ARCHITECTURES,
};
pub use ring::{Builder, IoUring, Parameters, RingConfig, RingLayout, SetupFlags, SetupPolicy};
pub use squeue::SubmissionQueue;
pub use submit::{EnterArgs, EnterFlags, SubmitError, SubmitReceipt, SubmitResult, Submitter};
