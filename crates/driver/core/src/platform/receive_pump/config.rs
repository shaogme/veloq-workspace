use veloq_std::{fmt, num::NonZeroUsize, time::Duration};

/// The largest datagram capacity accepted by the platform-neutral validation layer.
pub const MAX_UDP_DATAGRAM_CAPACITY: usize = u16::MAX as usize;

/// Configuration used internally by a backend receive pump.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReceivePumpConfig {
    kernel_capacity: NonZeroUsize,
    queue_capacity: NonZeroUsize,
    datagram_capacity: NonZeroUsize,
    close_timeout: Duration,
}

/// Platform-neutral configuration accepted by the UDP facade.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UdpReceiveConfig {
    kernel_capacity: NonZeroUsize,
    queue_capacity: NonZeroUsize,
    datagram_capacity: NonZeroUsize,
    close_timeout: Duration,
}

/// Errors returned while validating the platform-neutral UDP receive configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UdpReceiveBuildError {
    InvalidConfig,
}

impl UdpReceiveConfig {
    pub const fn new(
        kernel_capacity: NonZeroUsize,
        queue_capacity: NonZeroUsize,
        datagram_capacity: NonZeroUsize,
        close_timeout: Duration,
    ) -> Self {
        Self {
            kernel_capacity,
            queue_capacity,
            datagram_capacity,
            close_timeout,
        }
    }

    pub const fn kernel_capacity(self) -> NonZeroUsize {
        self.kernel_capacity
    }

    pub const fn queue_capacity(self) -> NonZeroUsize {
        self.queue_capacity
    }

    pub const fn datagram_capacity(self) -> NonZeroUsize {
        self.datagram_capacity
    }

    pub const fn close_timeout(self) -> Duration {
        self.close_timeout
    }

    /// Validate the only configuration invariants shared by all receive backends.
    pub fn validate(self) -> Result<Self, UdpReceiveBuildError> {
        let memory_entries = self
            .queue_capacity
            .get()
            .checked_add(self.kernel_capacity.get());
        let memory_budget =
            memory_entries.and_then(|entries| entries.checked_mul(self.datagram_capacity.get()));
        if self.kernel_capacity.get() > u32::MAX as usize
            || self.datagram_capacity.get() > MAX_UDP_DATAGRAM_CAPACITY
            || self.close_timeout.is_zero()
            || memory_budget.is_none()
        {
            return Err(UdpReceiveBuildError::InvalidConfig);
        }
        Ok(self)
    }

    /// Convert the public contract into the backend state-machine configuration.
    pub const fn into_pump_config(self) -> ReceivePumpConfig {
        ReceivePumpConfig {
            kernel_capacity: self.kernel_capacity,
            queue_capacity: self.queue_capacity,
            datagram_capacity: self.datagram_capacity,
            close_timeout: self.close_timeout,
        }
    }
}

impl From<UdpReceiveConfig> for ReceivePumpConfig {
    fn from(config: UdpReceiveConfig) -> Self {
        config.into_pump_config()
    }
}

/// Errors that affect pump transitions or become the terminal error returned after the final
/// already-completed datagram.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReceivePumpError {
    ReceiveConfigInvalid,
    ReceiveAlreadyOwned,
    ReceiverNotReady,
    InitialReceiveSubmitFailed,
    ReplacementSubmitFailed,
    ReceiveQueueStalled,
    ReceiveBufferExhausted,
    ReceiveEof,
    ReceiveOsError { code: i32 },
    ReceiveCancelled,
    ReceiveContextCorrupt,
    DriverShutdown,
    InvalidTransition,
}

impl ReceivePumpConfig {
    pub const fn new(
        kernel_capacity: NonZeroUsize,
        queue_capacity: NonZeroUsize,
        datagram_capacity: NonZeroUsize,
        close_timeout: Duration,
    ) -> Self {
        Self {
            kernel_capacity,
            queue_capacity,
            datagram_capacity,
            close_timeout,
        }
    }

    pub const fn kernel_capacity(self) -> NonZeroUsize {
        self.kernel_capacity
    }

    pub const fn queue_capacity(self) -> NonZeroUsize {
        self.queue_capacity
    }

    pub const fn datagram_capacity(self) -> NonZeroUsize {
        self.datagram_capacity
    }

    pub const fn close_timeout(self) -> Duration {
        self.close_timeout
    }

    pub fn validate(self, max_outstanding_receive: usize) -> Result<Self, ReceivePumpError> {
        let public = UdpReceiveConfig {
            kernel_capacity: self.kernel_capacity,
            queue_capacity: self.queue_capacity,
            datagram_capacity: self.datagram_capacity,
            close_timeout: self.close_timeout,
        };
        if self.kernel_capacity.get() > max_outstanding_receive || public.validate().is_err() {
            return Err(ReceivePumpError::ReceiveConfigInvalid);
        }
        Ok(self)
    }

    /// Maximum payload memory for user-held output buffers plus backend-held pending payloads.
    pub fn memory_budget_bytes(self) -> Result<usize, ReceivePumpError> {
        self.queue_capacity
            .get()
            .checked_add(self.kernel_capacity.get())
            .and_then(|entries| entries.checked_mul(self.datagram_capacity.get()))
            .ok_or(ReceivePumpError::ReceiveConfigInvalid)
    }

    pub const fn pending_capacity(self) -> usize {
        self.kernel_capacity.get()
    }
}

impl fmt::Display for ReceivePumpError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ReceiveConfigInvalid => f.write_str("invalid receive configuration"),
            Self::ReceiveAlreadyOwned => f.write_str("socket receive is already owned"),
            Self::ReceiverNotReady => f.write_str("receiver is not ready"),
            Self::InitialReceiveSubmitFailed => f.write_str("initial receive submit failed"),
            Self::ReplacementSubmitFailed => f.write_str("replacement receive submit failed"),
            Self::ReceiveQueueStalled => f.write_str("receive queue is stalled"),
            Self::ReceiveBufferExhausted => f.write_str("receive output buffer is exhausted"),
            Self::ReceiveEof => f.write_str("receive reached EOF"),
            Self::ReceiveOsError { code } => write!(f, "receive OS error ({code})"),
            Self::ReceiveCancelled => f.write_str("receive was cancelled"),
            Self::ReceiveContextCorrupt => f.write_str("receive request context is corrupt"),
            Self::DriverShutdown => f.write_str("driver is shutting down"),
            Self::InvalidTransition => f.write_str("invalid receive pump transition"),
        }
    }
}
