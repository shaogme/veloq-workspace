use veloq_blocking::BlockingPoolConfig;
use veloq_buf::LockedMemoryBudget;
use veloq_driver_core::{
    BorrowedRawHandle as CoreBorrowedRawHandle, IoFd as CoreIoFd,
    OwnedRawHandle as CoreOwnedRawHandle, RawHandle as CoreRawHandle, RawHandleMeta,
};
use veloq_std::{
    num::{NonZeroU32, NonZeroUsize},
    nz,
};
use windows_sys::Win32::{
    Foundation::{CloseHandle, HANDLE, INVALID_HANDLE_VALUE},
    Networking::WinSock::{INVALID_SOCKET, SOCKET, closesocket},
};

pub use veloq_driver_core::{DirectOwnerId, RawHandleKind};

/// IOCP driver 的配置。
/// Configuration for the IOCP driver.
#[derive(Debug, Clone)]
pub struct IocpConfig {
    /// completion port 的条目容量。
    /// Number of entries in the completion port.
    entries: NonZeroU32,
    locked_memory_budget: LockedMemoryBudget,
    /// 阻塞线程池配置。
    /// Configuration for the blocking thread pool.
    blocking_pool: BlockingPoolConfig,
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
            locked_memory_budget: LockedMemoryBudget::default(),
            blocking_pool: BlockingPoolConfig::default(),
        }
    }
}

impl IocpConfig {
    /// 返回完成端口容量。 / Returns the completion-port capacity.
    pub const fn entries(&self) -> NonZeroU32 {
        self.entries
    }

    pub fn get_locked_memory_budget(&self) -> LockedMemoryBudget {
        self.locked_memory_budget.clone()
    }

    /// 返回阻塞线程池配置。 / Returns the blocking-pool configuration.
    pub const fn blocking_pool(&self) -> &BlockingPoolConfig {
        &self.blocking_pool
    }

    /// 设置完成端口容量。 / Sets the completion-port capacity.
    pub fn with_entries(mut self, entries: NonZeroU32) -> Self {
        self.entries = entries;
        self
    }

    pub fn locked_memory_limit(mut self, limit_bytes: usize) -> Self {
        self.locked_memory_budget = LockedMemoryBudget::new(limit_bytes);
        self
    }

    pub fn locked_memory_budget(mut self, budget: LockedMemoryBudget) -> Self {
        self.locked_memory_budget = budget;
        self
    }

    /// 设置阻塞线程池配置。 / Sets the blocking-pool configuration.
    pub fn with_blocking_pool(mut self, config: BlockingPoolConfig) -> Self {
        self.blocking_pool = config;
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
        rio: bool,
        // 线程分配的 socket generation 用于避免 RIO actor 映射因 HANDLE 重用而混淆。
        // Per-thread socket generation prevents HANDLE reuse from aliasing RIO actor mappings.
        generation: u64,
    },
}

pub type SocketKey = IocpHandle;

// SAFETY: Windows HANDLE 可在线程间安全传递。
// SAFETY: Windows HANDLEs are thread-safe and can be sent across threads.
unsafe impl Send for IocpHandle {}

// SAFETY: Windows HANDLE 可由多个线程同时访问。
// SAFETY: Windows HANDLEs can be accessed from multiple threads simultaneously.
unsafe impl Sync for IocpHandle {}

impl IocpHandle {
    #[inline]
    pub const fn for_file(handle: HANDLE) -> Self {
        Self::File { handle }
    }

    #[inline]
    pub const fn for_socket(handle: HANDLE) -> Self {
        Self::Socket {
            handle,
            rio: false,
            generation: 0,
        }
    }

    #[inline]
    pub const fn for_rio_socket(handle: HANDLE) -> Self {
        Self::Socket {
            handle,
            rio: true,
            generation: 0,
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
    pub const fn uses_rio(self) -> bool {
        matches!(self, Self::Socket { rio: true, .. })
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
                    // SAFETY: `handle` 由当前值持有。
                    // SAFETY: `handle` is owned by this value.
                    unsafe { CloseHandle(handle) };
                }
            }
            Self::Socket { handle, .. } => {
                let socket = handle as SOCKET;
                if socket != INVALID_SOCKET {
                    // SAFETY: `socket` 由当前值持有。
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct IocpAssociation {
    port_raw: NonZeroUsize,
    completion_key: usize,
}

impl IocpAssociation {
    #[inline]
    pub(crate) const fn new(port_raw: NonZeroUsize, completion_key: usize) -> Self {
        Self {
            port_raw,
            completion_key,
        }
    }

    #[inline]
    pub(crate) const fn port_raw(self) -> usize {
        self.port_raw.get()
    }

    #[inline]
    pub(crate) const fn completion_key(self) -> usize {
        self.completion_key
    }
}

/// driver 固定文件表中的已注册描述符条目。
/// Registered descriptor entry used by the driver-side fixed-file table.
#[derive(Debug, PartialEq, Eq)]
pub enum RegisteredHandle {
    /// driver 拥有生命周期（用于文件句柄）。
    /// The driver owns the lifecycle (used for file handles).
    Owned(OwnedRawHandle),
    /// driver 仅保留弱引用或原始句柄视图（用于借用句柄）。
    /// The driver keeps only a weak or raw view (used for borrowed handles).
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

pub(crate) struct RegisteredSlot {
    pub(crate) handle: Option<RegisteredHandle>,
    pub(crate) generation: u64,
    pub(crate) association: Option<IocpAssociation>,
}

/// 使用 `RawHandle` 的 I/O 描述符类型别名。
/// Type alias for I/O descriptors using `RawHandle`.
pub type IoFd = CoreIoFd<IocpHandle>;
