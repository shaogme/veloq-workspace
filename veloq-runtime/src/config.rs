use std::num::NonZeroUsize;

use veloq_buf::nz;

pub use veloq_blocking::BlockingPoolConfig;
pub use veloq_driver::config::{BufferRegistrationMode, IocpConfig, UringConfig};

#[derive(Debug, Clone)]
pub struct Config {
    pub uring: UringConfig,
    pub iocp: IocpConfig,
    pub worker_threads: Option<NonZeroUsize>,
    pub direct_io: bool,
    pub blocking_pool: BlockingPoolConfig,
    pub internal_queue_capacity: usize,
}

impl AsRef<UringConfig> for Config {
    fn as_ref(&self) -> &UringConfig {
        &self.uring
    }
}

impl AsRef<IocpConfig> for Config {
    fn as_ref(&self) -> &IocpConfig {
        &self.iocp
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            uring: UringConfig::default(),
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
        Self::default()
    }

    pub fn uring(self, uring: UringConfig) -> Self {
        Self { uring, ..self }
    }

    pub fn iocp(self, iocp: IocpConfig) -> Self {
        Self { iocp, ..self }
    }

    pub fn worker_threads(self, worker_threads: usize) -> Self {
        Self {
            worker_threads: Some(NonZeroUsize::new(worker_threads).unwrap_or(nz!(1))),
            ..self
        }
    }

    pub fn direct_io(self, direct_io: bool) -> Self {
        Self { direct_io, ..self }
    }

    pub fn internal_queue_capacity(self, capacity: usize) -> Self {
        Self {
            internal_queue_capacity: capacity,
            ..self
        }
    }

    pub fn blocking_pool(self, blocking_pool: BlockingPoolConfig) -> Self {
        Self {
            blocking_pool,
            ..self
        }
    }
}
