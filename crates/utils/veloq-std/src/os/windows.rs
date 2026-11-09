//! Windows-specific operating system primitives, handles, and conversion traits.

pub mod io;

mod process_impl {
    //! Windows-specific extensions to primitives in the [`process`](crate::process) module.

    use crate::process::{ExitStatus, sys::ExitStatus as SysExitStatus};

    /// Windows-specific extensions to [`ExitStatus`].
    pub trait ExitStatusExt {
        /// Creates a new `ExitStatus` from the raw underlying `u32` return value of a process.
        fn from_raw(raw: u32) -> Self;

        /// Returns the raw underlying `u32` return value of a process.
        fn into_raw(self) -> u32;
    }

    impl ExitStatusExt for ExitStatus {
        #[inline]
        fn from_raw(raw: u32) -> Self {
            ExitStatus::from_raw(SysExitStatus::from_raw(raw))
        }

        #[inline]
        fn into_raw(self) -> u32 {
            self.raw().raw()
        }
    }
}

pub mod ffi {
    pub use crate::ffi::{EncodeWide, OsStrExt, OsStringExt};
}

pub mod fs {
    mod impls;

    pub use impls::*;
}

pub(crate) mod net {
    pub(crate) use super::io::Winsock as NetApi;
}

pub mod process {
    pub use super::process_impl::ExitStatusExt;
}

pub mod raw {
    //! Windows-specific raw types.

    use super::io::{RawHandle, RawSocket};

    pub type HANDLE = RawHandle;
    pub type SOCKET = RawSocket;
}
