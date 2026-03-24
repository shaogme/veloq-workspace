use crate::rio::SocketActorKey;
use std::num::NonZeroU32;
use std::sync::atomic::{AtomicU32, Ordering};
use veloq_buf::nz;
use veloq_driver_core::{
    BorrowedRawHandle as CoreBorrowedRawHandle, IoFd as CoreIoFd,
    OwnedRawHandle as CoreOwnedRawHandle, RawHandle as CoreRawHandle, RawHandleMeta,
};
use windows_sys::Win32::Foundation::{CloseHandle, HANDLE, INVALID_HANDLE_VALUE};
use windows_sys::Win32::Networking::WinSock::{INVALID_SOCKET, SOCKET, closesocket};

pub use veloq_driver_core::RawHandleKind;

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
pub struct IocpHandle {
    handle: HANDLE,
    kind: RawHandleKind,
    // Monotonic socket generation used to avoid HANDLE reuse aliasing in RIO actor mapping.
    generation: u32,
}

// SAFETY: Windows HANDLEs are thread-safe and can be sent across threads.
unsafe impl Send for IocpHandle {}
// SAFETY: Windows HANDLEs can be accessed from multiple threads simultaneously.
unsafe impl Sync for IocpHandle {}

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

impl IocpHandle {
    #[inline]
    pub const fn for_file(handle: HANDLE) -> Self {
        Self {
            handle,
            kind: RawHandleKind::File,
            generation: 0,
        }
    }

    #[inline]
    pub fn for_socket(handle: HANDLE) -> Self {
        Self {
            handle,
            kind: RawHandleKind::Socket,
            generation: alloc_socket_generation(),
        }
    }

    #[inline]
    pub(crate) const fn actor_key(self) -> SocketActorKey {
        SocketActorKey::new(self.as_handle(), self.generation())
    }

    #[inline]
    pub const fn as_handle(self) -> HANDLE {
        self.handle
    }

    #[inline]
    pub const fn generation(self) -> u32 {
        self.generation
    }

    #[inline]
    pub fn as_socket(self) -> SOCKET {
        self.handle as SOCKET
    }

    #[inline]
    pub const fn kind(self) -> RawHandleKind {
        self.kind
    }

    #[inline]
    pub const fn is_socket(self) -> bool {
        matches!(self.kind, RawHandleKind::Socket)
    }

    #[inline]
    pub const fn is_file(self) -> bool {
        matches!(self.kind, RawHandleKind::File)
    }
}

impl RawHandleMeta for IocpHandle {
    #[inline]
    fn kind(self) -> RawHandleKind {
        self.kind
    }

    #[inline]
    fn close(self) {
        match self.kind {
            RawHandleKind::File => {
                let handle = self.handle;
                if !handle.is_null() && handle != INVALID_HANDLE_VALUE {
                    // SAFETY: `handle` is owned by this value.
                    unsafe { CloseHandle(handle) };
                }
            }
            RawHandleKind::Socket => {
                let socket = self.handle as SOCKET;
                if socket != INVALID_SOCKET {
                    // SAFETY: `socket` is owned by this value.
                    unsafe { closesocket(socket) };
                }
            }
        }
    }
}

pub type RawHandle = CoreRawHandle<IocpHandle>;
pub type OwnedRawHandle = CoreOwnedRawHandle<IocpHandle>;
pub type BorrowedRawHandle<'a> = CoreBorrowedRawHandle<'a, IocpHandle>;

/// Registered descriptor entry used by driver-side fixed-file table.
#[derive(Debug, PartialEq, Eq)]
pub enum RegisteredHandle {
    /// Driver owns lifecycle (used for file handles).
    Owned(OwnedRawHandle),
    /// Driver only keeps a weak/raw view (used for socket handles).
    Weak(RawHandle),
}

impl RegisteredHandle {
    #[inline]
    pub fn as_raw(&self) -> RawHandle {
        match self {
            Self::Owned(h) => RawHandle::new(h.raw()),
            Self::Weak(h) => *h,
        }
    }
}

/// Type alias for I/O descriptors using RawHandle.
pub type IoFd = CoreIoFd<RawHandle>;
