use std::cell::Cell;
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum IocpHandle {
    File {
        handle: HANDLE,
    },
    Socket {
        handle: HANDLE,
        // Per-thread allocated socket generation used to avoid HANDLE reuse aliasing in RIO actor mapping.
        generation: u64,
    },
}

// SAFETY: Windows HANDLEs are thread-safe and can be sent across threads.
unsafe impl Send for IocpHandle {}
// SAFETY: Windows HANDLEs can be accessed from multiple threads simultaneously.
unsafe impl Sync for IocpHandle {}

static NEXT_THREAD_TAG: AtomicU32 = AtomicU32::new(1);

thread_local! {
    #[allow(clippy::missing_const_for_thread_local)]
    static LOCAL_SOCKET_COUNTER: Cell<u32> = const { Cell::new(1) };
}

thread_local! {
    #[allow(clippy::missing_const_for_thread_local)]
    static LOCAL_THREAD_TAG: Cell<u32> = const { Cell::new(0) };
}

#[inline]
fn alloc_thread_tag() -> u32 {
    let tag = NEXT_THREAD_TAG.fetch_add(1, Ordering::Relaxed);
    if tag == 0 { 1 } else { tag }
}

#[inline]
fn current_thread_tag() -> u32 {
    LOCAL_THREAD_TAG.with(|tag| {
        let current = tag.get();
        if current != 0 {
            return current;
        }
        let allocated = alloc_thread_tag();
        tag.set(allocated);
        allocated
    })
}

#[inline]
fn alloc_socket_generation() -> u64 {
    let thread_tag = current_thread_tag();
    let local_counter = LOCAL_SOCKET_COUNTER.with(|counter| {
        let current = counter.get();
        let next = current.wrapping_add(1);
        counter.set(if next == 0 { 1 } else { next });
        current
    });
    ((thread_tag as u64) << 32) | local_counter as u64
}

impl IocpHandle {
    #[inline]
    pub const fn for_file(handle: HANDLE) -> Self {
        Self::File { handle }
    }

    #[inline]
    pub fn for_socket(handle: HANDLE) -> Self {
        Self::Socket {
            handle,
            generation: alloc_socket_generation(),
        }
    }

    #[inline]
    pub(crate) const fn actor_key(self) -> SocketKey {
        self
    }

    #[inline]
    pub const fn as_handle(self) -> HANDLE {
        match self {
            Self::File { handle } | Self::Socket { handle, .. } => handle,
        }
    }

    #[inline]
    pub fn as_socket(self) -> SOCKET {
        self.as_handle() as SOCKET
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
}

impl RawHandleMeta for IocpHandle {
    #[inline]
    fn kind(self) -> RawHandleKind {
        match self {
            Self::File { .. } => RawHandleKind::File,
            Self::Socket { .. } => RawHandleKind::Socket,
        }
    }

    #[inline]
    fn close(self) {
        match self {
            Self::File { handle } => {
                if !handle.is_null() && handle != INVALID_HANDLE_VALUE {
                    // SAFETY: `handle` is owned by this value.
                    unsafe { CloseHandle(handle) };
                }
            }
            Self::Socket { handle, .. } => {
                let socket = handle as SOCKET;
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
pub type SocketKey = IocpHandle;

/// Registered descriptor entry used by driver-side fixed-file table.
#[derive(Debug, PartialEq, Eq)]
pub enum RegisteredHandle {
    /// Driver owns lifecycle (used for file handles).
    Owned(OwnedRawHandle),
    /// Driver only keeps a weak/raw view (used for borrowed handles).
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

    #[inline]
    pub fn as_borrowed(&self) -> BorrowedRawHandle<'_> {
        match self {
            Self::Owned(h) => h.borrow(),
            Self::Weak(h) => h.borrow(),
        }
    }
}

/// Type alias for I/O descriptors using RawHandle.
pub type IoFd = CoreIoFd;
