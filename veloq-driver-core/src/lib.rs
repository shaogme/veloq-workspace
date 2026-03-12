pub mod driver;
pub mod net;
pub mod op;
pub mod op_registry;
pub mod slot;

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
pub enum IoFd<H: Handle> {
    /// A raw system handle token interpreted by each driver.
    Raw(H),
    /// A registered index for pre-registered file descriptors.
    Fixed(u32),
}

impl<H: Handle> IoFd<H> {
    /// Returns the raw handle if this is a Raw variant.
    pub fn raw(&self) -> Option<H> {
        match self {
            Self::Raw(fd) => Some(*fd),
            Self::Fixed(_) => None,
        }
    }
}

impl<H: Handle> From<H> for IoFd<H> {
    fn from(handle: H) -> Self {
        Self::Raw(handle)
    }
}
