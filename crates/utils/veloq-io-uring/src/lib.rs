//! Veloq's minimal `io_uring` userspace ABI.
//!
//! Stage 2 owns ring setup, queue access, submission, and resource registration.

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

pub use register::Probe;
pub use ring::{Builder, IoUring, Parameters};
pub use squeue::SubmissionQueue;
pub use submit::{EnterFlags, Submitter};
