use veloq_driver_core::{
    BorrowedRawHandle as CoreBorrowedRawHandle, IoFd as CoreIoFd,
    OwnedRawHandle as CoreOwnedRawHandle, RawHandle as CoreRawHandle, RawHandleMeta,
};
pub use veloq_driver_core::{DirectOwnerId, RawHandleKind};
pub use veloq_io_uring::{SetupFlags, SetupPolicy};
use veloq_std::{
    mem,
    num::{NonZeroU16, NonZeroU32, NonZeroUsize},
    time::Duration,
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
    /// Fixed-buffer registration is required before a submission can proceed.
    #[default]
    Strict,
    /// Fixed-buffer registration is an optimization; valid operations use raw I/O when it is
    /// unavailable.
    Compatible,
}

impl BufferRegistrationMode {
    #[inline]
    pub const fn is_strict(self) -> bool {
        matches!(self, Self::Strict)
    }

    #[inline]
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Strict => "strict",
            Self::Compatible => "compatible",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IoMode {
    Interrupt,
    Polling(NonZeroU32),
}

/// Per-drive fairness and safety budgets for the io_uring backend.
///
/// The limits are deliberately part of the backend configuration.  A completion burst must not
/// be able to turn one driver poll into an unbounded loop, and an exhausted budget must be
/// visible to the caller's next drive rather than silently dropping work.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UringDriveLimits {
    pub max_control_events: usize,
    pub max_cancel_actions: usize,
    pub max_backlog_actions: usize,
    pub max_submit_rounds: usize,
    pub max_cqe_batch: usize,
    pub max_cqes_per_drive: usize,
    pub max_timer_expirations: usize,
    /// Maximum number of CQEs a high-water emergency drain may consume in one batch.
    pub emergency_drain_limit: usize,
    /// Maximum time a cancel ENOENT may remain deferred while CQE collection is not exhausted.
    pub cancel_reconcile_timeout: Duration,
}

impl UringDriveLimits {
    /// Creates limits sized for a ring with `entries` slots.
    pub const fn for_entries(entries: usize) -> Self {
        Self {
            max_control_events: entries,
            max_cancel_actions: entries,
            max_backlog_actions: entries,
            max_submit_rounds: 2,
            max_cqe_batch: entries,
            max_cqes_per_drive: entries,
            max_timer_expirations: entries,
            emergency_drain_limit: entries,
            cancel_reconcile_timeout: Duration::from_secs(1),
        }
    }

    pub(crate) fn validate(self, ring_entries: usize) -> Result<(), &'static str> {
        if self.max_control_events == 0
            || self.max_cancel_actions == 0
            || self.max_backlog_actions == 0
            || self.max_submit_rounds == 0
            || self.max_cqe_batch == 0
            || self.max_cqes_per_drive == 0
            || self.max_timer_expirations == 0
            || self.emergency_drain_limit == 0
        {
            return Err("uring drive budgets must be non-zero");
        }
        if self.max_cqe_batch > ring_entries {
            return Err("max_cqe_batch cannot exceed ring entries");
        }
        if self.emergency_drain_limit > ring_entries {
            return Err("emergency_drain_limit cannot exceed ring entries");
        }
        if self.cancel_reconcile_timeout.is_zero() {
            return Err("cancel_reconcile_timeout must be non-zero");
        }
        Ok(())
    }
}

impl Default for UringDriveLimits {
    fn default() -> Self {
        Self::for_entries(1024)
    }
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
    /// Setup flags required or negotiated while creating the ring.
    pub setup_policy: SetupPolicy,
    pub drive_limits: UringDriveLimits,
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
    /// table entirely and submits every descriptor as a raw fd. With [`FileTableExhaustion::Fail`],
    /// driver construction also fails when the kernel cannot provide the requested table.
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
            setup_policy: SetupPolicy::default(),
            drive_limits: UringDriveLimits::default(),
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
    /// Sets the required/best-effort/disabled setup flag policy.
    pub fn setup_policy(mut self, setup_policy: SetupPolicy) -> Self {
        self.setup_policy = setup_policy;
        self
    }

    pub fn drive_limits(mut self, limits: UringDriveLimits) -> Self {
        self.drive_limits = limits;
        self
    }

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

#[cfg(test)]
mod tests {
    use super::UringDriveLimits;

    #[test]
    fn drive_limits_require_cqe_budgets_to_fit_the_ring() {
        assert!(UringDriveLimits::for_entries(64).validate(64).is_ok());
        assert_eq!(
            UringDriveLimits::default().validate(64),
            Err("max_cqe_batch cannot exceed ring entries")
        );
    }
}
