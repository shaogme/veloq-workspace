use std::num::NonZeroUsize;

use veloq_buf::nz;

pub use veloq_blocking::BlockingPoolConfig;
pub use veloq_driver::config::{BufferRegistrationMode, IocpConfig, UringConfig};

#[derive(Debug, Clone)]
pub struct Config {
    #[cfg(not(windows))]
    uring: UringConfig,
    #[cfg(windows)]
    iocp: IocpConfig,
    worker_threads: Option<NonZeroUsize>,
    direct_io: bool,
    blocking_pool: BlockingPoolConfig,
    internal_queue_capacity: usize,
}

impl AsRef<UringConfig> for Config {
    fn as_ref(&self) -> &UringConfig {
        #[cfg(not(windows))]
        {
            &self.uring
        }
        #[cfg(windows)]
        {
            static DEFAULT_URING: UringConfig = UringConfig {
                mode: veloq_driver::config::IoMode::Interrupt,
                entries: unsafe { std::num::NonZeroU32::new_unchecked(1024) },
                registration_mode: BufferRegistrationMode::Strict,
            };
            &DEFAULT_URING
        }
    }
}

impl AsRef<IocpConfig> for Config {
    fn as_ref(&self) -> &IocpConfig {
        #[cfg(windows)]
        {
            &self.iocp
        }
        #[cfg(not(windows))]
        {
            static DEFAULT_IOCP: IocpConfig = IocpConfig {
                entries: unsafe { std::num::NonZeroU32::new_unchecked(1024) },
                registration_mode: BufferRegistrationMode::Strict,
            };
            &DEFAULT_IOCP
        }
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            #[cfg(not(windows))]
            uring: UringConfig::default(),
            #[cfg(windows)]
            iocp: IocpConfig::default(),
            worker_threads: None,
            direct_io: false,
            blocking_pool: BlockingPoolConfig::default(),
            internal_queue_capacity: 1024,
        }
    }
}

impl Config {
    pub fn new() -> Self {
        #[allow(clippy::default_trait_access)]
        Self::default()
    }

    #[allow(unused_mut)]
    pub fn uring(mut self, _uring: UringConfig) -> Self {
        #[cfg(not(windows))]
        {
            self.uring = _uring;
        }
        self
    }

    #[allow(unused_mut)]
    pub fn iocp(mut self, _iocp: IocpConfig) -> Self {
        #[cfg(windows)]
        {
            self.iocp = _iocp;
        }
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

    pub fn internal_queue_capacity(mut self, capacity: usize) -> Self {
        self.internal_queue_capacity = capacity;
        self
    }

    pub fn blocking_pool(mut self, blocking_pool: BlockingPoolConfig) -> Self {
        self.blocking_pool = blocking_pool;
        self
    }

    // ============ Internal Getters ============

    pub(crate) fn worker_threads_opt(&self) -> Option<NonZeroUsize> {
        self.worker_threads
    }

    pub(crate) fn blocking_pool_config(&self) -> &BlockingPoolConfig {
        &self.blocking_pool
    }

    pub(crate) fn queue_capacity(&self) -> usize {
        self.internal_queue_capacity
    }

    #[cfg(not(windows))]
    pub(crate) fn get_uring(&self) -> &UringConfig {
        &self.uring
    }

    #[cfg(windows)]
    pub(crate) fn get_iocp(&self) -> &IocpConfig {
        &self.iocp
    }
}
