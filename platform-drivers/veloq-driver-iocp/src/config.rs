use crate::rio::SocketActorKey;
use std::marker::PhantomData;
use std::num::NonZeroU32;
use std::sync::atomic::{AtomicU32, Ordering};
use veloq_buf::nz;
use veloq_driver_core::IoFd as CoreIoFd;
use windows_sys::Win32::Foundation::HANDLE;
use windows_sys::Win32::Networking::WinSock::SOCKET;

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

/// A raw Windows handle wrapper.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub struct RawHandle {
    /// The underlying Windows HANDLE.
    pub handle: HANDLE,
    /// Monotonic socket generation used to avoid HANDLE reuse aliasing in RIO actor mapping.
    pub generation: u32,
}

/// Owned handle wrapper for internal ownership-oriented APIs.
///
/// Note: this type currently models ownership at the type level only and does
/// not perform implicit close on drop.
#[derive(Debug, Clone, PartialEq, Eq)]
#[repr(transparent)]
pub struct OwnedRawHandle {
    raw: RawHandle,
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
        handle.handle as usize
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
        Self {
            handle,
            generation: 0,
        }
    }

    #[inline]
    pub fn for_socket(handle: HANDLE) -> Self {
        Self {
            handle,
            generation: alloc_socket_generation(),
        }
    }

    #[inline]
    pub(crate) const fn actor_key(self) -> SocketActorKey {
        SocketActorKey::new(self.handle, self.generation)
    }

    #[inline]
    pub const fn as_handle(self) -> HANDLE {
        self.handle
    }

    #[inline]
    pub fn as_socket(self) -> SOCKET {
        self.handle as SOCKET
    }

    #[inline]
    pub const fn borrow(&self) -> BorrowedRawHandle<'_> {
        BorrowedRawHandle {
            raw: *self,
            _marker: PhantomData,
        }
    }

    #[inline]
    pub const fn into_owned(self) -> OwnedRawHandle {
        OwnedRawHandle { raw: self }
    }
}

impl OwnedRawHandle {
    #[inline]
    pub const fn as_raw(&self) -> RawHandle {
        self.raw
    }

    #[inline]
    pub const fn borrow(&self) -> BorrowedRawHandle<'_> {
        self.raw.borrow()
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
    pub const fn generation(self) -> u32 {
        self.raw.generation
    }
}

impl From<RawHandle> for OwnedRawHandle {
    fn from(value: RawHandle) -> Self {
        value.into_owned()
    }
}

impl From<OwnedRawHandle> for RawHandle {
    fn from(value: OwnedRawHandle) -> Self {
        value.raw
    }
}

impl<'a> From<BorrowedRawHandle<'a>> for RawHandle {
    fn from(value: BorrowedRawHandle<'a>) -> Self {
        value.raw
    }
}

/// Type alias for I/O descriptors using RawHandle.
pub type IoFd = CoreIoFd<RawHandle>;
