pub use veloq_driver_core::RawHandleKind;
use veloq_driver_core::{
    BorrowedRawHandle as CoreBorrowedRawHandle, IoFd as CoreIoFd,
    OwnedRawHandle as CoreOwnedRawHandle, RawHandle as CoreRawHandle, RawHandleMeta,
};
use veloq_std::{
    mem,
    num::{NonZeroU16, NonZeroU32, NonZeroUsize},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UringRawHandle {
    File { fd: i32 },
    Socket { fd: i32 },
}

impl UringRawHandle {
    #[inline]
    pub const fn for_file(fd: i32) -> Self {
        Self::File { fd }
    }

    #[inline]
    pub fn for_socket(fd: i32) -> Self {
        Self::Socket { fd }
    }

    #[inline]
    pub const fn as_fd(self) -> i32 {
        match self {
            Self::File { fd } => fd,
            Self::Socket { fd, .. } => fd,
        }
    }
}

impl RawHandleMeta for UringRawHandle {
    #[inline]
    fn kind(self) -> RawHandleKind {
        match self {
            Self::File { .. } => RawHandleKind::File,
            Self::Socket { .. } => RawHandleKind::Socket,
        }
    }

    #[inline]
    fn close(self) {
        let fd = self.as_fd();
        if fd >= 0 {
            // SAFETY: `fd` is owned by this value.
            unsafe {
                libc::close(fd);
            }
        }
    }
}

/// Type alias for I/O descriptors using [`UringRawHandle`].
pub type IoFd = CoreIoFd<UringRawHandle>;
pub type RawHandle = CoreRawHandle<UringRawHandle>;
pub type OwnedRawHandle = CoreOwnedRawHandle<UringRawHandle>;
pub type BorrowedRawHandle<'a> = CoreBorrowedRawHandle<'a, UringRawHandle>;

#[repr(transparent)]
#[derive(Clone, Copy)]
pub struct SockAddrStorage(pub libc::sockaddr_storage);

impl Default for SockAddrStorage {
    fn default() -> Self {
        Self(unsafe { mem::zeroed() })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum BufferRegistrationMode {
    #[default]
    Strict,
    Compatible,
}

impl BufferRegistrationMode {
    #[inline]
    pub const fn is_strict(self) -> bool {
        matches!(self, Self::Strict)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IoMode {
    Interrupt,
    Polling(NonZeroU32),
}

/// What happens once every entry of the kernel's registered file table is taken.
///
/// The table is a fixed-size kernel allocation, so it cannot grow on demand. The number of
/// descriptors a server keeps open is an independent dimension from it, which means any fixed
/// capacity can be reached by a legitimate workload.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum FileTableExhaustion {
    /// Hand out unregistered descriptors instead. Submissions for them carry the raw fd rather
    /// than a fixed index, so they lose the registered-file fast path but keep working.
    #[default]
    Fallback,
    /// Reject the registration with an error.
    Fail,
}

impl FileTableExhaustion {
    #[inline]
    pub const fn falls_back(self) -> bool {
        matches!(self, Self::Fallback)
    }
}

/// Shape of the kernel's provided-buffer ring (`IORING_REGISTER_PBUF_RING`, Linux 5.19+).
///
/// The driver keeps `entries` buffers of `buf_size` bytes published to the kernel; an operation
/// submitted with `IOSQE_BUFFER_SELECT` gets one of them assigned only once data actually
/// arrives. That late binding is the point: idle connections stop pinning receive buffers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProvidedBufConfig {
    /// Number of ring entries. Must be a power of two and at most [`MAX_PROVIDED_BUF_ENTRIES`]
    /// — the kernel enforces both, so a bad value simply leaves provided buffers disabled.
    pub entries: NonZeroU16,
    /// Capacity of each buffer.
    pub buf_size: NonZeroUsize,
}

/// The kernel's hard cap on `ring_entries` for `IORING_REGISTER_PBUF_RING`.
pub const MAX_PROVIDED_BUF_ENTRIES: u16 = 1 << 15;

impl Default for ProvidedBufConfig {
    fn default() -> Self {
        Self {
            entries: NonZeroU16::new(256).expect("256 is non-zero"),
            // One `veloq-buf` slot: the pool serves this size out of its order-0 fast path.
            buf_size: NonZeroUsize::new(4096).expect("4096 is non-zero"),
        }
    }
}

impl ProvidedBufConfig {
    pub fn new(entries: NonZeroU16, buf_size: NonZeroUsize) -> Self {
        Self { entries, buf_size }
    }
}

#[derive(Debug, Clone)]
pub struct UringConfig {
    pub mode: IoMode,
    pub entries: NonZeroU32,
    pub registration_mode: BufferRegistrationMode,
    /// Provided-buffer ring to register, or `None` to run without one.
    ///
    /// Off by default: the ring costs `entries * buf_size` of pool memory per worker whether or
    /// not anything ever selects a buffer from it, and only operations that explicitly ask for
    /// buffer selection can use it.
    pub provided_buffers: Option<ProvidedBufConfig>,
    /// Size of the kernel's registered (fixed) file table.
    ///
    /// Independent of [`Self::entries`]: submission queue depth bounds how many operations are
    /// in flight, this bounds how many descriptors are registered at once. `0` disables the
    /// table entirely and submits every descriptor as a raw fd; driver construction fails if
    /// the value is larger than the driver — or the kernel — is willing to allocate.
    pub file_table_capacity: u32,
    /// Behaviour once `file_table_capacity` entries are in use.
    pub file_table_exhaustion: FileTableExhaustion,
}

impl AsRef<UringConfig> for UringConfig {
    fn as_ref(&self) -> &UringConfig {
        self
    }
}

impl Default for UringConfig {
    fn default() -> Self {
        Self {
            mode: IoMode::Interrupt,
            // SAFETY: 1024 is non-zero.
            entries: unsafe { NonZeroU32::new_unchecked(1024) },
            registration_mode: BufferRegistrationMode::Strict,
            provided_buffers: None,
            file_table_capacity: DEFAULT_FILE_TABLE_CAPACITY,
            file_table_exhaustion: FileTableExhaustion::Fallback,
        }
    }
}

/// Matches the historical capacity, which was pinned to the default ring depth.
const DEFAULT_FILE_TABLE_CAPACITY: u32 = 1024;

impl UringConfig {
    pub fn registration_mode(mut self, mode: BufferRegistrationMode) -> Self {
        self.registration_mode = mode;
        self
    }

    pub fn provided_buffers(mut self, provided_buffers: Option<ProvidedBufConfig>) -> Self {
        self.provided_buffers = provided_buffers;
        self
    }

    pub fn file_table_capacity(mut self, capacity: u32) -> Self {
        self.file_table_capacity = capacity;
        self
    }

    pub fn file_table_exhaustion(mut self, exhaustion: FileTableExhaustion) -> Self {
        self.file_table_exhaustion = exhaustion;
        self
    }
}
