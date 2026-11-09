//! Unix-specific operating system primitives and I/O safety types.

pub mod fd;
mod impls;

pub use fd as io;
pub(crate) use impls::Cvt;
pub use impls::{ffi, fs, process, raw};
