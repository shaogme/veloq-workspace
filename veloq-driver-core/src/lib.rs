pub mod driver;
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

/// Represents the source of an IO operation: either a raw handle or a registered index.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IoFd<H: RawHandleMeta> {
    /// A raw system handle token interpreted by each driver.
    Raw(RawHandle<H>),
    /// A registered index for pre-registered file descriptors.
    Fixed(u32),
}

impl<H: RawHandleMeta> IoFd<H> {
    /// Returns the raw handle if this is a Raw variant.
    pub fn raw(&self) -> Option<RawHandle<H>> {
        match self {
            Self::Raw(fd) => Some(*fd),
            Self::Fixed(_) => None,
        }
    }

    /// Returns a borrowed raw handle view if this is a Raw variant.
    #[inline]
    pub const fn raw_ref(&self) -> Option<&RawHandle<H>> {
        match self {
            Self::Raw(fd) => Some(fd),
            Self::Fixed(_) => None,
        }
    }
}

impl<H: RawHandleMeta> From<RawHandle<H>> for IoFd<H> {
    fn from(handle: RawHandle<H>) -> Self {
        Self::Raw(handle)
    }
}
