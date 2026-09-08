#![cfg_attr(not(test), no_std)]

use diagweave::prelude::*;
use veloq_std::{
    error::Error,
    fmt,
    marker::{PhantomData, Send, Sync},
    mem,
    net::SocketAddr,
};

pub mod driver;
pub mod op;
pub mod slot;

// ============================================================================
// Core Traits
// ============================================================================

/// Platform-neutral handle trait implemented by driver-defined handle types.
pub trait Handle: Copy + Send + Sync {}

impl<T> Handle for T where T: Copy + Send + Sync {}

/// Platform-neutral socket address storage trait implemented by driver-defined types.
pub trait SockAddr: Default + Send {}

impl<T> SockAddr for T where T: Default + Send {}

/// Platform-neutral per-slot sidecar trait implemented by driver-defined types.
pub trait SlotSidecar: Default + Send {}

impl<T> SlotSidecar for T where T: Default + Send {}

// ============================================================================
// IoFd
// ============================================================================

/// Names the descriptor an operation runs against.
///
/// A descriptor reaches the kernel one of two ways, and which one it is decided when the
/// driver handed the descriptor out — not at submission time:
///
/// - [`Registered`](Self::Registered) is an index into the driver's descriptor registry paired
///   with the generation that registration was handed out under. The generation is what makes
///   use-after-close detectable: releasing a slot advances it, so a stale descriptor is
///   rejected instead of silently naming whatever took the slot's place.
/// - [`Direct`](Self::Direct) carries the platform handle itself, for descriptors that live
///   outside the registry. Submitting one needs no registry lookup at all, and its
///   [`RawHandleKind`] comes from the handle rather than from a table — but **it carries no
///   generation**, so a `Direct` descriptor whose handle has been closed will silently name
///   whatever the platform later assigns that same number. Producing one is a backend's
///   decision; a backend that keeps every descriptor in its registry never does.
///
/// `H` is the backend's raw handle type, so this enum stays free of platform types the way a
/// bare index did. Backends alias it once (`type IoFd = IoFd<UringRawHandle>`) and their own
/// code never mentions the parameter.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum IoFd<H: Handle> {
    /// A slot in the driver's descriptor registry.
    Registered {
        /// Index of the registry slot.
        index: u32,
        /// Generation the slot carried when this descriptor was handed out.
        generation: u64,
    },
    /// A platform handle submitted as-is, with no registry entry behind it.
    Direct(H),
}

impl<H: Handle> IoFd<H> {
    /// Creates a registered descriptor at generation zero.
    pub const fn fixed(index: u32) -> Self {
        Self::Registered {
            index,
            generation: 0,
        }
    }

    /// Creates a registered descriptor with an explicit generation.
    pub const fn fixed_with_generation(index: u32, generation: u64) -> Self {
        Self::Registered { index, generation }
    }

    /// Creates a descriptor that carries the platform handle directly.
    pub const fn direct(handle: H) -> Self {
        Self::Direct(handle)
    }

    /// Returns the registry index and generation, or `None` for a direct descriptor.
    pub const fn registered_parts(self) -> Option<(u32, u64)> {
        match self {
            Self::Registered { index, generation } => Some((index, generation)),
            Self::Direct(_) => None,
        }
    }

    /// Returns the registry index, or `None` for a direct descriptor.
    pub const fn fixed_index(self) -> Option<u32> {
        match self {
            Self::Registered { index, .. } => Some(index),
            Self::Direct(_) => None,
        }
    }

    /// Returns the registration generation, or `None` for a direct descriptor.
    pub const fn generation(self) -> Option<u64> {
        match self {
            Self::Registered { generation, .. } => Some(generation),
            Self::Direct(_) => None,
        }
    }

    /// Returns the platform handle, or `None` for a registered descriptor.
    pub const fn direct_handle(self) -> Option<H> {
        match self {
            Self::Direct(handle) => Some(handle),
            Self::Registered { .. } => None,
        }
    }

    /// Whether this descriptor names a registry slot.
    pub const fn is_registered(self) -> bool {
        matches!(self, Self::Registered { .. })
    }

    /// Whether this descriptor carries its platform handle directly.
    pub const fn is_direct(self) -> bool {
        matches!(self, Self::Direct(_))
    }
}

impl<H: RawHandleMeta> IoFd<H> {
    /// The handle kind, when it can be told without consulting the registry.
    ///
    /// A registered descriptor returns `None`: only the registry knows what it points at.
    pub fn direct_kind(self) -> Option<RawHandleKind> {
        match self {
            Self::Direct(handle) => Some(handle.kind()),
            Self::Registered { .. } => None,
        }
    }
}

/// Renders a descriptor for diagnostics.
///
/// Backends attach this under a single `fd` context key rather than separate index and
/// generation keys, because a direct descriptor has neither.
impl<H: Handle + fmt::Debug> fmt::Display for IoFd<H> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Registered { index, generation } => {
                write!(f, "registered(index={index}, generation={generation})")
            }
            Self::Direct(handle) => write!(f, "direct({handle:?})"),
        }
    }
}

// ============================================================================
// Error System (formerly error.rs)
// ============================================================================

set! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub DriverCoreError = {
        #[display("system error")]
        System,
        #[display("internal error")]
        Internal,
    }
}

pub type DriverResult<T, E> = Result<T, Report<E>>;
pub type DriverReport<E> = Report<E>;

pub trait DriverError: Error + Send + Sync + 'static + Sized {
    fn from_core_report(report: Report<DriverCoreError>) -> Report<Self>;
}

// ============================================================================
// Raw Handles (formerly raw_handle.rs)
// ============================================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RawHandleKind {
    File,
    Socket,
}

pub trait RawHandleMeta: Handle {
    fn kind(self) -> RawHandleKind;
    fn close(self);
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RawHandle<H: Handle> {
    raw: H,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BorrowedRawHandle<'a, H: Handle> {
    raw: RawHandle<H>,
    marker: PhantomData<&'a RawHandle<H>>,
}

#[derive(Debug, PartialEq, Eq)]
pub struct OwnedRawHandle<H: RawHandleMeta> {
    raw: RawHandle<H>,
}

impl<H: Handle> RawHandle<H> {
    pub const fn raw(self) -> H {
        self.raw
    }
}

impl<H: RawHandleMeta> RawHandle<H> {
    pub const fn new(raw: H) -> Self {
        Self { raw }
    }

    pub fn kind(self) -> RawHandleKind {
        self.raw.kind()
    }

    pub const fn borrow(&self) -> BorrowedRawHandle<'_, H> {
        BorrowedRawHandle {
            raw: *self,
            marker: PhantomData,
        }
    }

    pub fn is_socket(self) -> bool {
        matches!(self.kind(), RawHandleKind::Socket)
    }

    pub fn is_file(self) -> bool {
        matches!(self.kind(), RawHandleKind::File)
    }
}

impl<'a, H: RawHandleMeta> BorrowedRawHandle<'a, H> {
    pub const fn raw(self) -> H {
        self.raw.raw()
    }

    pub fn kind(self) -> RawHandleKind {
        self.raw.kind()
    }

    pub fn is_socket(self) -> bool {
        self.raw.is_socket()
    }

    pub fn is_file(self) -> bool {
        self.raw.is_file()
    }
}

impl<H: RawHandleMeta> OwnedRawHandle<H> {
    pub const fn raw(&self) -> H {
        self.raw.raw()
    }

    /// # Safety
    ///
    /// 调用方必须保证 `raw` 拥有唯一所有权。
    pub const unsafe fn from_raw_owned(raw: RawHandle<H>) -> Self {
        Self { raw }
    }

    pub fn into_raw(self) -> RawHandle<H> {
        let this = mem::ManuallyDrop::new(self);
        this.raw
    }

    pub fn kind(&self) -> RawHandleKind {
        self.raw.kind()
    }

    pub const fn borrow(&self) -> BorrowedRawHandle<'_, H> {
        self.raw.borrow()
    }

    pub fn is_socket(&self) -> bool {
        self.raw.is_socket()
    }

    pub fn is_file(&self) -> bool {
        self.raw.is_file()
    }
}

impl<H: RawHandleMeta> Drop for OwnedRawHandle<H> {
    fn drop(&mut self) {
        self.raw.raw().close();
    }
}

// ============================================================================
// Network Abstractions (formerly net.rs)
// ============================================================================

/// 平台套接字抽象，由各 driver 后端提供具体实现。
pub trait PlatformSocket: Sized + Send {
    type Handle: RawHandleMeta;
    type Error: Error + Send + Sync;

    fn new_tcp_v4() -> Result<Self, Report<Self::Error>>;
    fn new_tcp_v6() -> Result<Self, Report<Self::Error>>;
    fn new_udp_v4() -> Result<Self, Report<Self::Error>>;
    fn new_udp_v6() -> Result<Self, Report<Self::Error>>;

    fn bind(&self, addr: SocketAddr) -> Result<(), Report<Self::Error>>;
    fn listen(&self, backlog: i32) -> Result<(), Report<Self::Error>>;
    fn connect(&self, addr: SocketAddr) -> Result<(), Report<Self::Error>>;

    fn into_owned_raw(self) -> OwnedRawHandle<Self::Handle>;

    /// # Safety
    ///
    /// `handle` 必须是有效底层句柄，并满足所有权语义。
    unsafe fn from_raw(handle: Self::Handle) -> Self;

    fn local_addr(&self) -> Result<SocketAddr, Report<Self::Error>>;

    fn set_nodelay(&self, nodelay: bool) -> Result<(), Report<Self::Error>>;
    fn set_recv_buffer_size(&self, size: usize) -> Result<(), Report<Self::Error>>;
    fn set_send_buffer_size(&self, size: usize) -> Result<(), Report<Self::Error>>;
    fn set_reuse_address(&self, reuse: bool) -> Result<(), Report<Self::Error>>;
    fn set_keepalive(&self, keepalive: bool) -> Result<(), Report<Self::Error>>;
    fn set_ttl(&self, ttl: u32) -> Result<(), Report<Self::Error>>;
    fn set_broadcast(&self, broadcast: bool) -> Result<(), Report<Self::Error>>;
}

/// 平台地址存储编解码抽象。
pub trait SocketAddrCodec: SockAddr {
    type Len: Copy + Send;
    type Error: Error + Send + Sync;

    fn to_socket_addr(buf: &[u8]) -> Result<SocketAddr, Report<Self::Error>>;
    fn socket_addr_to_storage(addr: SocketAddr) -> (Self, Self::Len);
}
