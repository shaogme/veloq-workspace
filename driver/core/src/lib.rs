use core::convert::TryFrom;
use core::fmt;
use core::marker::PhantomData;

use diagweave::{report::Report, set};
use std::net::SocketAddr;

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

// ============================================================================
// Error System (formerly error.rs)
// ============================================================================

set! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub DriverErrorKind = {
        #[display("invalid input")]
        InvalidInput,
        #[display("invalid state")]
        InvalidState,
        #[display("submission failed")]
        Submission,
        #[display("completion failed")]
        Completion,
        #[display("registration failed")]
        Registration,
        #[display("socket operation failed")]
        Socket,
        #[display("timeout")]
        Timeout,
        #[display("unsupported")]
        Unsupported,
        #[display("internal error")]
        Internal,
        #[display("system error")]
        System,
    }
}

pub type DriverResult<T> = Result<T, Report<DriverErrorKind>>;
pub type DriverErrorReport = Report<DriverErrorKind>;

#[inline]
fn neg_code(code: i32) -> Option<i32> {
    (code != 0).then_some(-code.abs())
}

#[inline]
fn diag_code_i32(report: &DriverErrorReport) -> Option<i32> {
    report
        .error_code()
        .and_then(|code| i32::try_from(code).ok())
        .and_then(neg_code)
}

#[inline]
pub fn driver_error_kind_fallback_errno(kind: DriverErrorKind) -> i32 {
    match kind {
        DriverErrorKind::InvalidInput => 22, // EINVAL
        DriverErrorKind::InvalidState => 5,  // EIO
        DriverErrorKind::Submission => 11,   // EAGAIN
        DriverErrorKind::Completion => 5,    // EIO
        DriverErrorKind::Registration => 12, // ENOMEM
        DriverErrorKind::Socket => 5,        // EIO
        DriverErrorKind::Timeout => 110,     // ETIMEDOUT
        DriverErrorKind::Unsupported => 95,  // EOPNOTSUPP
        DriverErrorKind::Internal => 5,      // EIO
        DriverErrorKind::System => 5,        // EIO
    }
}

#[inline]
pub fn driver_error_report_to_event_res(report: &DriverErrorReport) -> i32 {
    if let Some(res) = diag_code_i32(report) {
        return res;
    }
    -driver_error_kind_fallback_errno(*report.inner())
}

#[inline]
pub fn driver_error(
    kind: DriverErrorKind,
    scope: &'static str,
    detail: impl ToString,
) -> DriverErrorReport {
    let detail = detail.to_string();
    Report::new(kind)
        .with_ctx("scope", scope)
        .attach_note(detail)
}

#[inline]
pub fn driver_os_error(
    kind: DriverErrorKind,
    scope: &'static str,
    code: i32,
    detail: impl ToString,
) -> DriverErrorReport {
    let detail = detail.to_string();
    Report::new(kind)
        .with_ctx("scope", scope)
        .set_error_code(code)
        .attach_note(detail)
}

pub trait ResultAsDriverExt<T, E> {
    fn to_driver_result(
        self,
        kind: DriverErrorKind,
        scope: &'static str,
        detail: impl ToString,
    ) -> DriverResult<T>;
}

impl<T, E> ResultAsDriverExt<T, E> for Result<T, Report<E>>
where
    E: fmt::Debug + fmt::Display + std::error::Error + Send + Sync + 'static,
{
    fn to_driver_result(
        self,
        kind: DriverErrorKind,
        scope: &'static str,
        detail: impl ToString,
    ) -> DriverResult<T> {
        let detail = detail.to_string();
        self.map_err(|report| {
            tracing::error!(kind = %kind, scope = %scope, detail = %detail, "driver error report");
            report
                .set_accumulate_src_chain(true)
                .map_err(|_| kind)
                .with_ctx("scope", scope)
                .attach_note(detail)
                .attach_note("driver error report captured")
        })
    }
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
    _marker: PhantomData<&'a RawHandle<H>>,
}

#[derive(Debug, PartialEq, Eq)]
pub struct OwnedRawHandle<H: RawHandleMeta> {
    raw: RawHandle<H>,
}

impl<H: Handle> RawHandle<H> {
    #[inline]
    pub const fn raw(self) -> H {
        self.raw
    }
}

impl<H: RawHandleMeta> RawHandle<H> {
    #[inline]
    pub const fn new(raw: H) -> Self {
        Self { raw }
    }

    #[inline]
    pub fn kind(self) -> RawHandleKind {
        self.raw.kind()
    }

    #[inline]
    pub const fn borrow(&self) -> BorrowedRawHandle<'_, H> {
        BorrowedRawHandle {
            raw: *self,
            _marker: PhantomData,
        }
    }

    #[inline]
    pub fn is_socket(self) -> bool {
        matches!(self.kind(), RawHandleKind::Socket)
    }

    #[inline]
    pub fn is_file(self) -> bool {
        matches!(self.kind(), RawHandleKind::File)
    }
}

impl<'a, H: RawHandleMeta> BorrowedRawHandle<'a, H> {
    #[inline]
    pub const fn raw(self) -> H {
        self.raw.raw()
    }

    #[inline]
    pub fn kind(self) -> RawHandleKind {
        self.raw.kind()
    }

    #[inline]
    pub fn is_socket(self) -> bool {
        self.raw.is_socket()
    }

    #[inline]
    pub fn is_file(self) -> bool {
        self.raw.is_file()
    }
}

impl<H: RawHandleMeta> OwnedRawHandle<H> {
    #[inline]
    pub const fn raw(&self) -> H {
        self.raw.raw()
    }

    /// # Safety
    ///
    /// 调用方必须保证 `raw` 拥有唯一所有权。
    #[inline]
    pub const unsafe fn from_raw_owned(raw: RawHandle<H>) -> Self {
        Self { raw }
    }

    #[inline]
    pub fn into_raw(self) -> RawHandle<H> {
        let this = core::mem::ManuallyDrop::new(self);
        this.raw
    }

    #[inline]
    pub fn kind(&self) -> RawHandleKind {
        self.raw.kind()
    }

    #[inline]
    pub const fn borrow(&self) -> BorrowedRawHandle<'_, H> {
        self.raw.borrow()
    }

    #[inline]
    pub fn is_socket(&self) -> bool {
        self.raw.is_socket()
    }

    #[inline]
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
    type Error: std::error::Error + Send + Sync;

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
    type Error: std::error::Error + Send + Sync;

    fn to_socket_addr(buf: &[u8]) -> Result<SocketAddr, Report<Self::Error>>;
    fn socket_addr_to_storage(addr: SocketAddr) -> (Self, Self::Len);
}
