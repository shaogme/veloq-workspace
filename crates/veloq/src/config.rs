use veloq_std::{num::NonZeroUsize, nz};

pub use veloq_blocking::BlockingPoolConfig;
pub use veloq_driver_native::config::{
    BufferRegistrationMode, FileTableExhaustion, IocpConfig, ProvidedBufConfig, UringConfig,
    UringDriveLimits,
};

#[cfg(not(windows))]
pub use veloq_driver_native::config::{SetupFlags, SetupPolicy};

#[derive(Debug, Clone)]
pub struct Config {
    #[cfg(not(windows))]
    uring: UringConfig,
    #[cfg(windows)]
    iocp: IocpConfig,
    worker_threads: Option<NonZeroUsize>,
    direct_io: bool,
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

    pub fn worker_threads(mut self, worker_threads: Option<NonZeroUsize>) -> Self {
        self.worker_threads = worker_threads;
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

    pub fn blocking_pool(self, blocking_pool: BlockingPoolConfig) -> Self {
        #[cfg(windows)]
        {
            let mut this = self;
            this.iocp.blocking_pool = blocking_pool;
            this
        }
        #[cfg(not(windows))]
        {
            let _ = blocking_pool;
            self
        }
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

    /// 为每个 worker 配置并注册一组 provided buffer（io_uring 5.19+）。
    ///
    /// provided buffer 默认使用 [`ProvidedBufConfig::default`]，此方法仅用于自定义环的
    /// entries 和单个 buffer 容量；provided buffer 始终开启。
    #[cfg(not(windows))]
    pub fn uring_provided_buffers(mut self, provided_buffers: ProvidedBufConfig) -> Self {
        self.uring.provided_buffers = provided_buffers;
        self
    }

    /// IOCP 没有 provided buffer，这里保留跨平台配置 API。
    #[cfg(windows)]
    pub fn uring_provided_buffers(self, _provided_buffers: ProvidedBufConfig) -> Self {
        self
    }

    // ============ Internal Getters ============

    pub(crate) fn get_worker_threads_opt(&self) -> Option<NonZeroUsize> {
        self.worker_threads
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
