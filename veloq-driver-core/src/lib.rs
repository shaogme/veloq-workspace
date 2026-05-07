pub mod driver;
pub mod error;
pub mod net;
pub mod op;
pub mod op_registry;
pub mod raw_handle;
pub mod slot;

pub use raw_handle::{BorrowedRawHandle, OwnedRawHandle, RawHandle, RawHandleKind, RawHandleMeta};

/// Platform-neutral handle trait implemented by driver-defined handle types.
pub trait Handle: Copy + Send + Sync + 'static {}

impl<T> Handle for T where T: Copy + Send + Sync + 'static {}

/// Platform-neutral socket address storage trait implemented by driver-defined types.
pub trait SockAddr: Default + Send + 'static {}

impl<T> SockAddr for T where T: Default + Send + 'static {}

/// Platform-neutral per-slot sidecar trait implemented by driver-defined types.
pub trait SlotSidecar: Default + Send + 'static {}

impl<T> SlotSidecar for T where T: Default + Send + 'static {}

/// Represents the source of an IO operation as a registered descriptor index.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct IoFd {
    fixed_index: u32,
    generation: u64,
}

impl IoFd {
    /// Creates an IO descriptor from a registered descriptor index.
    #[inline]
    pub const fn fixed(index: u32) -> Self {
        Self {
            fixed_index: index,
            generation: 0,
        }
    }

    /// Creates an IO descriptor from a registered descriptor index and generation.
    #[inline]
    pub const fn fixed_with_generation(index: u32, generation: u64) -> Self {
        Self {
            fixed_index: index,
            generation,
        }
    }

    /// Returns the registered descriptor index.
    #[inline]
    pub const fn fixed_index(self) -> u32 {
        self.fixed_index
    }

    /// Returns the descriptor generation.
    #[inline]
    pub const fn generation(self) -> u64 {
        self.generation
    }
}
