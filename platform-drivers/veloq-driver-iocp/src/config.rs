use crate::rio::SocketActorKey;
use std::marker::PhantomData;
use std::num::NonZeroU32;
use std::sync::atomic::{AtomicU32, Ordering};
use veloq_buf::nz;
use veloq_driver_core::IoFd as CoreIoFd;
use windows_sys::Win32::Foundation::{CloseHandle, HANDLE, INVALID_HANDLE_VALUE};
use windows_sys::Win32::Networking::WinSock::{INVALID_SOCKET, SOCKET, closesocket};

/// Specifies how buffers are registered and validated.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum BufferRegistrationMode {
    /// Strict registration with validation.
    #[default]
    Strict,
    /// Compatible registration for fallback.
    Compatible,
}

impl BufferRegistrationMode {
    /// Returns true if the mode is strict.
    #[inline]
    pub const fn is_strict(self) -> bool {
        matches!(self, Self::Strict)
    }
}

/// Configuration for the IOCP driver.
#[derive(Debug, Clone)]
pub struct IocpConfig {
    /// Number of entries in the completion port.
    pub entries: NonZeroU32,
    /// Mode for buffer registration.
    pub registration_mode: BufferRegistrationMode,
}

impl AsRef<IocpConfig> for IocpConfig {
    fn as_ref(&self) -> &IocpConfig {
        self
    }
}

impl Default for IocpConfig {
    fn default() -> Self {
        Self {
            entries: nz!(1024),
            registration_mode: BufferRegistrationMode::Strict,
        }
    }
}

impl IocpConfig {
    /// Sets the registration mode.
    pub fn registration_mode(mut self, mode: BufferRegistrationMode) -> Self {
        self.registration_mode = mode;
        self
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RawHandleKind {
    File,
    Socket,
}

/// A raw Windows handle wrapper.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RawHandle {
    File {
        handle: HANDLE,
    },
    Socket {
        socket: SOCKET,
        // Monotonic socket generation used to avoid HANDLE reuse aliasing in RIO actor mapping.
        generation: u32,
    },
}

/// Owned handle wrapper that is fully responsible for resource ownership.
#[derive(Debug, PartialEq, Eq)]
pub struct OwnedRawHandle {
    raw: RawHandle,
}

/// Registered descriptor entry used by driver-side fixed-file table.
#[derive(Debug, PartialEq, Eq)]
pub enum RegisteredHandle {
    /// Driver owns lifecycle (used for file handles).
    Owned(OwnedRawHandle),
    /// Driver only keeps a weak/raw view (used for socket handles).
    Weak(RawHandle),
}

/// Borrowed handle view tied to a caller-controlled lifetime.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BorrowedRawHandle<'a> {
    raw: RawHandle,
    _marker: PhantomData<&'a RawHandle>,
}

// SAFETY: Windows HANDLEs are thread-safe and can be sent across threads.
unsafe impl Send for RawHandle {}
// SAFETY: Windows HANDLEs can be accessed from multiple threads simultaneously.
unsafe impl Sync for RawHandle {}

impl From<usize> for RawHandle {
    fn from(handle: usize) -> Self {
        Self::for_file(handle as HANDLE)
    }
}

impl From<RawHandle> for usize {
    fn from(handle: RawHandle) -> Self {
        handle.as_handle() as usize
    }
}

static NEXT_SOCKET_GENERATION: AtomicU32 = AtomicU32::new(1);

#[inline]
fn alloc_socket_generation() -> u32 {
    let generation = NEXT_SOCKET_GENERATION.fetch_add(1, Ordering::Relaxed);
    if generation == 0 {
        NEXT_SOCKET_GENERATION.store(1, Ordering::Relaxed);
        1
    } else {
        generation
    }
}

impl RawHandle {
    #[inline]
    pub const fn for_file(handle: HANDLE) -> Self {
        Self::File { handle }
    }

    #[inline]
    pub fn for_socket(handle: HANDLE) -> Self {
        Self::Socket {
            socket: handle as SOCKET,
            generation: alloc_socket_generation(),
        }
    }

    #[inline]
    pub(crate) const fn actor_key(self) -> SocketActorKey {
        SocketActorKey::new(self.as_handle(), self.generation())
    }

    #[inline]
    pub const fn as_handle(self) -> HANDLE {
        match self {
            Self::File { handle } => handle,
            Self::Socket { socket, .. } => socket as HANDLE,
        }
    }

    #[inline]
    pub const fn generation(self) -> u32 {
        match self {
            Self::File { .. } => 0,
            Self::Socket { generation, .. } => generation,
        }
    }

    #[inline]
    pub fn as_socket(self) -> SOCKET {
        match self {
            Self::File { handle } => handle as SOCKET,
            Self::Socket { socket, .. } => socket,
        }
    }

    #[inline]
    pub const fn kind(self) -> RawHandleKind {
        match self {
            Self::File { .. } => RawHandleKind::File,
            Self::Socket { .. } => RawHandleKind::Socket,
        }
    }

    #[inline]
    pub const fn is_socket(self) -> bool {
        matches!(self, Self::Socket { .. })
    }

    #[inline]
    pub const fn is_file(self) -> bool {
        matches!(self, Self::File { .. })
    }

    #[inline]
    pub const fn borrow(&self) -> BorrowedRawHandle<'_> {
        BorrowedRawHandle {
            raw: *self,
            _marker: PhantomData,
        }
    }

    /// # Safety
    ///
    /// The caller must guarantee that `self` is uniquely owned.
    #[inline]
    pub unsafe fn into_owned(self) -> OwnedRawHandle {
        // SAFETY: forwarded from caller contract.
        unsafe { OwnedRawHandle::from_raw_owned(self) }
    }
}

impl OwnedRawHandle {
    #[inline]
    pub const fn as_raw(&self) -> RawHandle {
        self.raw
    }

    #[inline]
    pub const fn borrow(&self) -> BorrowedRawHandle<'_> {
        BorrowedRawHandle {
            raw: self.as_raw(),
            _marker: PhantomData,
        }
    }

    #[inline]
    pub const fn kind(&self) -> RawHandleKind {
        self.raw.kind()
    }

    #[inline]
    pub const fn as_handle(&self) -> HANDLE {
        self.as_raw().as_handle()
    }

    #[inline]
    pub fn as_socket(&self) -> SOCKET {
        self.as_raw().as_socket()
    }

    /// # Safety
    ///
    /// The caller must guarantee that `raw` is uniquely owned.
    #[inline]
    pub unsafe fn from_raw_owned(raw: RawHandle) -> Self {
        Self { raw }
    }

    /// Consumes ownership and returns raw handle metadata without closing it.
    #[inline]
    pub fn into_raw(self) -> RawHandle {
        let this = std::mem::ManuallyDrop::new(self);
        this.raw
    }
}

impl Drop for OwnedRawHandle {
    fn drop(&mut self) {
        match self.raw {
            RawHandle::File { handle } => {
                if !handle.is_null() && handle != INVALID_HANDLE_VALUE {
                    // SAFETY: `handle` is owned by this value.
                    unsafe { CloseHandle(handle) };
                }
            }
            RawHandle::Socket { socket, .. } => {
                if socket != INVALID_SOCKET {
                    // SAFETY: `socket` is owned by this value.
                    unsafe { closesocket(socket) };
                }
            }
        }
    }
}

impl<'a> BorrowedRawHandle<'a> {
    #[inline]
    pub const fn as_raw(self) -> RawHandle {
        self.raw
    }

    #[inline]
    pub const fn as_handle(self) -> HANDLE {
        self.raw.as_handle()
    }

    #[inline]
    pub fn as_socket(self) -> SOCKET {
        self.raw.as_socket()
    }

    #[inline]
    pub const fn kind(self) -> RawHandleKind {
        self.raw.kind()
    }

    #[inline]
    pub const fn is_socket(self) -> bool {
        self.raw.is_socket()
    }

    #[inline]
    pub const fn generation(self) -> u32 {
        self.raw.generation()
    }
}

impl From<OwnedRawHandle> for RawHandle {
    fn from(value: OwnedRawHandle) -> Self {
        value.into_raw()
    }
}

impl RegisteredHandle {
    #[inline]
    pub fn as_raw(&self) -> RawHandle {
        match self {
            Self::Owned(h) => h.as_raw(),
            Self::Weak(h) => *h,
        }
    }
}

impl<'a> From<BorrowedRawHandle<'a>> for RawHandle {
    fn from(value: BorrowedRawHandle<'a>) -> Self {
        value.raw
    }
}

/// Type alias for I/O descriptors using RawHandle.
pub type IoFd = CoreIoFd<RawHandle>;
