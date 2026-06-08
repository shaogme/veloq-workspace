use std::num::NonZeroUsize;

use veloq_buf::nz;

pub use veloq_blocking::BlockingPoolConfig;
pub use veloq_driver_native::config::{BufferRegistrationMode, IocpConfig, UringConfig};

#[derive(Debug, Clone)]
pub struct Config {
    #[cfg(not(windows))]
    uring: UringConfig,
    #[cfg(windows)]
    iocp: IocpConfig,
    worker_threads: Option<NonZeroUsize>,
    direct_io: bool,
    blocking_pool: BlockingPoolConfig,
    queue_capacity: NonZeroUsize,
}

#[cfg(not(windows))]
impl AsRef<UringConfig> for Config {
    fn as_ref(&self) -> &UringConfig {
        &self.uring
    }
}

#[cfg(windows)]
impl AsRef<IocpConfig> for Config {
    fn as_ref(&self) -> &IocpConfig {
        &self.iocp
    }
}

impl Default for Config {
    fn default() -> Self {
        Self::new()
    }
}

impl Config {
    pub fn new() -> Self {
        Self {
            #[cfg(not(windows))]
            uring: UringConfig::default(),
            #[cfg(windows)]
            iocp: IocpConfig::default(),
            worker_threads: None,
            direct_io: false,
            blocking_pool: BlockingPoolConfig::default(),
            queue_capacity: nz!(1024),
        }
    }

    #[cfg(not(windows))]
    pub fn uring(mut self, uring: UringConfig) -> Self {
        self.uring = uring;
        self
    }

    #[cfg(windows)]
    pub fn uring(self, _uring: UringConfig) -> Self {
        self
    }

    #[cfg(windows)]
    pub fn iocp(mut self, iocp: IocpConfig) -> Self {
        self.iocp = iocp;
        self
    }

    #[cfg(not(windows))]
    pub fn iocp(self, _iocp: IocpConfig) -> Self {
        self
    }

    pub fn worker_threads(mut self, worker_threads: usize) -> Self {
        self.worker_threads = Some(NonZeroUsize::new(worker_threads).unwrap_or(nz!(1)));
        self
    }

    pub fn direct_io(mut self, direct_io: bool) -> Self {
        self.direct_io = direct_io;
        self
    }

    pub fn queue_capacity(mut self, capacity: NonZeroUsize) -> Self {
        self.queue_capacity = capacity;
        self
    }

    pub fn blocking_pool(mut self, blocking_pool: BlockingPoolConfig) -> Self {
        self.blocking_pool = blocking_pool;
        self
    }

    #[cfg(windows)]
    pub fn iocp_registration_mode(mut self, mode: BufferRegistrationMode) -> Self {
        self.iocp.registration_mode = mode;
        self
    }

    #[cfg(not(windows))]
    pub fn iocp_registration_mode(self, _mode: BufferRegistrationMode) -> Self {
        self
    }

    #[cfg(not(windows))]
    pub fn uring_registration_mode(mut self, mode: BufferRegistrationMode) -> Self {
        self.uring.registration_mode = mode;
        self
    }

    #[cfg(windows)]
    pub fn uring_registration_mode(self, _mode: BufferRegistrationMode) -> Self {
        self
    }

    // ============ Internal Getters ============

    pub(crate) fn get_worker_threads_opt(&self) -> Option<NonZeroUsize> {
        self.worker_threads
    }

    pub(crate) fn get_blocking_pool_config(&self) -> &BlockingPoolConfig {
        &self.blocking_pool
    }

    pub(crate) fn get_queue_capacity(&self) -> NonZeroUsize {
        self.queue_capacity
    }

    pub fn registration_mode(&self) -> BufferRegistrationMode {
        #[cfg(windows)]
        {
            self.iocp.registration_mode
        }
        #[cfg(not(windows))]
        {
            self.uring.registration_mode
        }
    }
}
