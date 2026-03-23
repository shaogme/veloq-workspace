use std::num::NonZeroU32;
use std::sync::atomic::{AtomicU32, Ordering};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RawHandleKind {
    File,
    Socket,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RawHandle {
    File { fd: i32 },
    Socket { fd: i32, generation: u32 },
}

#[derive(Debug, PartialEq, Eq)]
pub struct OwnedRawHandle {
    raw: RawHandle,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BorrowedRawHandle<'a> {
    raw: RawHandle,
    _marker: std::marker::PhantomData<&'a RawHandle>,
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

impl From<i32> for RawHandle {
    fn from(fd: i32) -> Self {
        Self::for_file(fd)
    }
}

impl From<usize> for RawHandle {
    fn from(fd: usize) -> Self {
        Self::for_file(fd as i32)
    }
}

impl From<RawHandle> for usize {
    fn from(handle: RawHandle) -> Self {
        handle.as_fd() as usize
    }
}

impl RawHandle {
    #[inline]
    pub const fn for_file(fd: i32) -> Self {
        Self::File { fd }
    }

    #[inline]
    pub fn for_socket(fd: i32) -> Self {
        Self::Socket {
            fd,
            generation: alloc_socket_generation(),
        }
    }

    #[inline]
    pub const fn as_fd(self) -> i32 {
        match self {
            Self::File { fd } => fd,
            Self::Socket { fd, .. } => fd,
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
    pub const fn kind(self) -> RawHandleKind {
        match self {
            Self::File { .. } => RawHandleKind::File,
            Self::Socket { .. } => RawHandleKind::Socket,
        }
    }

    #[inline]
    pub const fn is_file(self) -> bool {
        matches!(self, Self::File { .. })
    }

    #[inline]
    pub const fn is_socket(self) -> bool {
        matches!(self, Self::Socket { .. })
    }

    #[inline]
    pub const fn borrow(&self) -> BorrowedRawHandle<'_> {
        BorrowedRawHandle {
            raw: *self,
            _marker: std::marker::PhantomData,
        }
    }

    /// # Safety
    ///
    /// 调用方必须保证该句柄具有唯一所有权。
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
            raw: self.raw,
            _marker: std::marker::PhantomData,
        }
    }

    #[inline]
    pub const fn kind(&self) -> RawHandleKind {
        self.raw.kind()
    }

    #[inline]
    pub const fn as_fd(&self) -> i32 {
        self.raw.as_fd()
    }

    /// # Safety
    ///
    /// 调用方必须保证该句柄具有唯一所有权。
    #[inline]
    pub unsafe fn from_raw_owned(raw: RawHandle) -> Self {
        Self { raw }
    }

    #[inline]
    pub fn into_raw(self) -> RawHandle {
        let this = std::mem::ManuallyDrop::new(self);
        this.raw
    }
}

impl Drop for OwnedRawHandle {
    fn drop(&mut self) {
        let fd = self.raw.as_fd();
        if fd >= 0 {
            // SAFETY: `fd` is owned by this value.
            unsafe {
                libc::close(fd);
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
    pub const fn as_fd(self) -> i32 {
        self.raw.as_fd()
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

impl<'a> From<BorrowedRawHandle<'a>> for RawHandle {
    fn from(value: BorrowedRawHandle<'a>) -> Self {
        value.raw
    }
}

#[repr(transparent)]
#[derive(Clone, Copy)]
pub struct SockAddrStorage(pub libc::sockaddr_storage);

impl Default for SockAddrStorage {
    fn default() -> Self {
        Self(unsafe { std::mem::zeroed() })
    }
}

pub type IoFd = veloq_driver_core::IoFd<RawHandle>;

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

#[derive(Debug, Clone)]
pub struct UringConfig {
    pub mode: IoMode,
    pub entries: NonZeroU32,
    pub registration_mode: BufferRegistrationMode,
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
            entries: NonZeroU32::new(1024).unwrap(),
            registration_mode: BufferRegistrationMode::Strict,
        }
    }
}

impl UringConfig {
    pub fn registration_mode(mut self, mode: BufferRegistrationMode) -> Self {
        self.registration_mode = mode;
        self
    }
}
