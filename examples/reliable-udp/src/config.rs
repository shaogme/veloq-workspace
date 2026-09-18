use veloq::std::{
    error::Error as StdError,
    fmt,
    num::{NonZeroU32, NonZeroUsize},
    time::Duration,
};

use veloq_wheel::WheelConfig;

use crate::packet::HEADER_LEN;

const MAX_SAFE_DATAGRAM_SIZE: usize = 65_507;
const MAX_SEND_WINDOW: usize = 64;
const MAX_RECEIVE_WINDOW: usize = 64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfigError {
    DatagramTooSmall { size: usize, header_len: usize },
    DatagramTooLarge { size: usize, max: usize },
    SendWindowTooLarge { size: usize, max: usize },
    ReceiveWindowTooLarge { size: usize, max: usize },
    MessageSizeTooLarge { size: usize, max: usize },
    MessageSizeWireOverflow,
    FragmentCountTooLarge { size: u32, max: u32 },
    ReassemblyBudgetTooSmall { budget: usize, message: usize },
    InboundBudgetTooSmall { budget: usize, message: usize },
    ZeroBudget { name: &'static str },
    TimeoutOrder,
    TimeoutTooShort { name: &'static str },
    TimeoutRangeOverflow { name: &'static str },
    HandshakeRetryBudgetOverflow,
    MemoryBudgetOverflow,
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DatagramTooSmall { size, header_len } => {
                write!(
                    f,
                    "datagram size {size} must exceed header length {header_len}"
                )
            }
            Self::DatagramTooLarge { size, max } => {
                write!(f, "datagram size {size} exceeds safe limit {max}")
            }
            Self::SendWindowTooLarge { size, max } => {
                write!(f, "send window {size} exceeds protocol limit {max}")
            }
            Self::ReceiveWindowTooLarge { size, max } => {
                write!(f, "receive window {size} exceeds wire limit {max}")
            }
            Self::MessageSizeTooLarge { size, max } => {
                write!(f, "message size {size} exceeds wire limit {max}")
            }
            Self::MessageSizeWireOverflow => {
                f.write_str("message size cannot be represented by the wire format")
            }
            Self::FragmentCountTooLarge { size, max } => {
                write!(f, "fragment count {size} exceeds protocol limit {max}")
            }
            Self::ReassemblyBudgetTooSmall { budget, message } => {
                write!(
                    f,
                    "reassembly budget {budget} is smaller than message size {message}"
                )
            }
            Self::InboundBudgetTooSmall { budget, message } => {
                write!(
                    f,
                    "inbound budget {budget} is smaller than message size {message}"
                )
            }
            Self::ZeroBudget { name } => write!(f, "{name} must be non-zero"),
            Self::TimeoutOrder => f.write_str("RTO values must satisfy min <= initial <= max"),
            Self::TimeoutTooShort { name } => {
                write!(f, "{name} must be longer than the wheel base tick")
            }
            Self::TimeoutRangeOverflow { name } => {
                write!(f, "{name} cannot be represented by the wheel clock")
            }
            Self::HandshakeRetryBudgetOverflow => {
                f.write_str("handshake retry schedule exceeds the handshake deadline")
            }
            Self::MemoryBudgetOverflow => f.write_str("configured memory budget overflows usize"),
        }
    }
}

impl StdError for ConfigError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Config {
    pub max_datagram_size: NonZeroUsize,
    pub max_message_size: NonZeroUsize,
    pub max_fragments_per_message: NonZeroU32,
    pub max_reassembly_messages: NonZeroUsize,
    pub max_reassembly_bytes: NonZeroUsize,
    pub max_inbound_bytes: NonZeroUsize,
    pub send_window: NonZeroUsize,
    pub receive_window: NonZeroUsize,
    pub command_capacity: NonZeroUsize,
    pub accept_capacity: NonZeroUsize,
    pub max_connections: NonZeroUsize,
    pub pending_send_capacity: NonZeroUsize,
    pub initial_rto: Duration,
    pub min_rto: Duration,
    pub max_rto: Duration,
    pub max_retries: u8,
    pub ack_delay: Duration,
    pub ack_batch_size: NonZeroUsize,
    pub reassembly_timeout: Duration,
    pub message_ack_timeout: Duration,
    pub message_ack_max_retries: u8,
    pub handshake_initial_rto: Duration,
    pub handshake_max_rto: Duration,
    pub handshake_deadline: Duration,
    pub handshake_max_retries: u8,
    pub inbound_capacity: NonZeroUsize,
    pub outbound_capacity: NonZeroUsize,
    pub close_timeout: Duration,
    pub wheel: WheelConfig,
}

impl Default for Config {
    fn default() -> Self {
        Self::builder()
            .build()
            .expect("default reliable UDP configuration must be valid")
    }
}

impl Config {
    pub fn builder() -> ConfigBuilder {
        ConfigBuilder::default()
    }

    pub fn max_fragment_payload(&self) -> usize {
        self.max_datagram_size.get() - HEADER_LEN
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        validate_values(self)
    }
}

#[derive(Debug, Clone)]
pub struct ConfigBuilder {
    config: Config,
}

impl Default for ConfigBuilder {
    fn default() -> Self {
        let wheel = WheelConfig::default();
        Self {
            config: Config {
                max_datagram_size: non_zero(1_200),
                max_message_size: non_zero(4 * 1024 * 1024),
                max_fragments_per_message: non_zero_u32(4_096),
                max_reassembly_messages: non_zero(64),
                max_reassembly_bytes: non_zero(4 * 1024 * 1024),
                max_inbound_bytes: non_zero(8 * 1024 * 1024),
                send_window: non_zero(32),
                receive_window: non_zero(32),
                command_capacity: non_zero(64),
                accept_capacity: non_zero(64),
                max_connections: non_zero(256),
                pending_send_capacity: non_zero(64),
                initial_rto: Duration::from_millis(200),
                min_rto: Duration::from_millis(50),
                max_rto: Duration::from_secs(5),
                max_retries: 8,
                ack_delay: Duration::from_millis(5),
                ack_batch_size: non_zero(2),
                reassembly_timeout: Duration::from_secs(5),
                message_ack_timeout: Duration::from_secs(1),
                message_ack_max_retries: 5,
                handshake_initial_rto: Duration::from_millis(50),
                handshake_max_rto: Duration::from_millis(250),
                handshake_deadline: Duration::from_secs(1),
                handshake_max_retries: 5,
                inbound_capacity: non_zero(256),
                outbound_capacity: non_zero(256),
                close_timeout: Duration::from_secs(5),
                wheel,
            },
        }
    }
}

impl ConfigBuilder {
    pub fn max_datagram_size(mut self, value: NonZeroUsize) -> Self {
        self.config.max_datagram_size = value;
        self
    }

    pub fn max_message_size(mut self, value: NonZeroUsize) -> Self {
        self.config.max_message_size = value;
        self
    }

    pub fn max_fragments_per_message(mut self, value: NonZeroU32) -> Self {
        self.config.max_fragments_per_message = value;
        self
    }

    pub fn max_reassembly_messages(mut self, value: NonZeroUsize) -> Self {
        self.config.max_reassembly_messages = value;
        self
    }

    pub fn max_reassembly_bytes(mut self, value: NonZeroUsize) -> Self {
        self.config.max_reassembly_bytes = value;
        self
    }

    pub fn max_inbound_bytes(mut self, value: NonZeroUsize) -> Self {
        self.config.max_inbound_bytes = value;
        self
    }

    pub fn send_window(mut self, value: NonZeroUsize) -> Self {
        self.config.send_window = value;
        self
    }

    pub fn receive_window(mut self, value: NonZeroUsize) -> Self {
        self.config.receive_window = value;
        self
    }

    pub fn command_capacity(mut self, value: NonZeroUsize) -> Self {
        self.config.command_capacity = value;
        self
    }

    pub fn accept_capacity(mut self, value: NonZeroUsize) -> Self {
        self.config.accept_capacity = value;
        self
    }

    pub fn max_connections(mut self, value: NonZeroUsize) -> Self {
        self.config.max_connections = value;
        self
    }

    pub fn pending_send_capacity(mut self, value: NonZeroUsize) -> Self {
        self.config.pending_send_capacity = value;
        self
    }

    pub fn initial_rto(mut self, value: Duration) -> Self {
        self.config.initial_rto = value;
        self
    }

    pub fn min_rto(mut self, value: Duration) -> Self {
        self.config.min_rto = value;
        self
    }

    pub fn max_rto(mut self, value: Duration) -> Self {
        self.config.max_rto = value;
        self
    }

    pub fn max_retries(mut self, value: u8) -> Self {
        self.config.max_retries = value;
        self
    }

    pub fn ack_delay(mut self, value: Duration) -> Self {
        self.config.ack_delay = value;
        self
    }

    pub fn ack_batch_size(mut self, value: NonZeroUsize) -> Self {
        self.config.ack_batch_size = value;
        self
    }

    pub fn reassembly_timeout(mut self, value: Duration) -> Self {
        self.config.reassembly_timeout = value;
        self
    }

    pub fn message_ack_timeout(mut self, value: Duration) -> Self {
        self.config.message_ack_timeout = value;
        self
    }

    pub fn message_ack_max_retries(mut self, value: u8) -> Self {
        self.config.message_ack_max_retries = value;
        self
    }

    pub fn handshake_initial_rto(mut self, value: Duration) -> Self {
        self.config.handshake_initial_rto = value;
        self
    }

    pub fn handshake_max_rto(mut self, value: Duration) -> Self {
        self.config.handshake_max_rto = value;
        self
    }

    pub fn handshake_deadline(mut self, value: Duration) -> Self {
        self.config.handshake_deadline = value;
        self
    }

    pub fn handshake_max_retries(mut self, value: u8) -> Self {
        self.config.handshake_max_retries = value;
        self
    }

    pub fn inbound_capacity(mut self, value: NonZeroUsize) -> Self {
        self.config.inbound_capacity = value;
        self
    }

    pub fn outbound_capacity(mut self, value: NonZeroUsize) -> Self {
        self.config.outbound_capacity = value;
        self
    }

    pub fn close_timeout(mut self, value: Duration) -> Self {
        self.config.close_timeout = value;
        self
    }

    pub fn wheel(mut self, value: WheelConfig) -> Self {
        self.config.wheel = value;
        self
    }

    pub fn build(self) -> Result<Config, ConfigError> {
        validate_values(&self.config)?;
        Ok(self.config)
    }
}

fn validate_values(config: &Config) -> Result<(), ConfigError> {
    let datagram_size = config.max_datagram_size.get();
    if datagram_size <= HEADER_LEN {
        return Err(ConfigError::DatagramTooSmall {
            size: datagram_size,
            header_len: HEADER_LEN,
        });
    }
    if datagram_size > MAX_SAFE_DATAGRAM_SIZE {
        return Err(ConfigError::DatagramTooLarge {
            size: datagram_size,
            max: MAX_SAFE_DATAGRAM_SIZE,
        });
    }

    let send_window = config.send_window.get();
    if send_window > MAX_SEND_WINDOW {
        return Err(ConfigError::SendWindowTooLarge {
            size: send_window,
            max: MAX_SEND_WINDOW,
        });
    }
    let receive_window = config.receive_window.get();
    if receive_window > MAX_RECEIVE_WINDOW {
        return Err(ConfigError::ReceiveWindowTooLarge {
            size: receive_window,
            max: MAX_RECEIVE_WINDOW,
        });
    }

    let max_fragment_payload =
        datagram_size
            .checked_sub(HEADER_LEN)
            .ok_or(ConfigError::DatagramTooSmall {
                size: datagram_size,
                header_len: HEADER_LEN,
            })?;
    let max_message_size = config.max_message_size.get();
    if u64::try_from(max_message_size).is_err() {
        return Err(ConfigError::MessageSizeWireOverflow);
    }
    if max_message_size > u32::MAX as usize {
        return Err(ConfigError::MessageSizeTooLarge {
            size: max_message_size,
            max: u32::MAX as usize,
        });
    }
    let max_wire_message_size = max_fragment_payload
        .checked_mul(config.max_fragments_per_message.get() as usize)
        .ok_or(ConfigError::MemoryBudgetOverflow)?;
    if max_message_size > max_wire_message_size {
        return Err(ConfigError::MessageSizeTooLarge {
            size: max_message_size,
            max: max_wire_message_size,
        });
    }
    if config.max_reassembly_bytes.get() < max_message_size {
        return Err(ConfigError::ReassemblyBudgetTooSmall {
            budget: config.max_reassembly_bytes.get(),
            message: max_message_size,
        });
    }
    if config.max_inbound_bytes.get() < max_message_size {
        return Err(ConfigError::InboundBudgetTooSmall {
            budget: config.max_inbound_bytes.get(),
            message: max_message_size,
        });
    }
    if !(config.min_rto <= config.initial_rto && config.initial_rto <= config.max_rto) {
        return Err(ConfigError::TimeoutOrder);
    }

    let tick = config.wheel.base_tick();
    for (name, timeout) in [
        ("min_rto", config.min_rto),
        ("initial_rto", config.initial_rto),
        ("max_rto", config.max_rto),
        ("handshake_initial_rto", config.handshake_initial_rto),
        ("handshake_max_rto", config.handshake_max_rto),
        ("handshake_deadline", config.handshake_deadline),
        ("close_timeout", config.close_timeout),
        ("reassembly_timeout", config.reassembly_timeout),
        ("message_ack_timeout", config.message_ack_timeout),
    ] {
        if timeout <= tick {
            return Err(ConfigError::TimeoutTooShort { name });
        }
        if timeout.as_nanos().div_ceil(tick.as_nanos()) > u128::from(u64::MAX) {
            return Err(ConfigError::TimeoutRangeOverflow { name });
        }
    }

    if !(tick < config.handshake_initial_rto
        && config.handshake_initial_rto <= config.handshake_max_rto)
    {
        return Err(ConfigError::TimeoutOrder);
    }
    if config.handshake_deadline <= config.handshake_initial_rto {
        return Err(ConfigError::TimeoutOrder);
    }

    let mut retry_delay = config.handshake_initial_rto;
    let mut retry_budget = Duration::ZERO;
    for _ in 0..config.handshake_max_retries {
        retry_budget = retry_budget
            .checked_add(retry_delay)
            .ok_or(ConfigError::HandshakeRetryBudgetOverflow)?;
        retry_delay = retry_delay
            .checked_mul(2)
            .unwrap_or(config.handshake_max_rto)
            .min(config.handshake_max_rto);
    }
    if retry_budget > config.handshake_deadline {
        return Err(ConfigError::HandshakeRetryBudgetOverflow);
    }

    for (name, timeout) in [("ack_delay", config.ack_delay)] {
        if !timeout.is_zero() && timeout.as_nanos().div_ceil(tick.as_nanos()) > u128::from(u64::MAX)
        {
            return Err(ConfigError::TimeoutRangeOverflow { name });
        }
    }

    let queued = config
        .send_window
        .get()
        .checked_add(config.receive_window.get())
        .and_then(|value| value.checked_add(config.pending_send_capacity.get()))
        .and_then(|value| value.checked_add(config.inbound_capacity.get()))
        .and_then(|value| value.checked_add(config.outbound_capacity.get()))
        .ok_or(ConfigError::MemoryBudgetOverflow)?;
    let frame_budget = datagram_size
        .checked_mul(queued)
        .ok_or(ConfigError::MemoryBudgetOverflow)?;
    let message_budget = config
        .pending_send_capacity
        .get()
        .checked_mul(max_message_size)
        .and_then(|value| value.checked_add(config.max_reassembly_bytes.get()))
        .and_then(|value| value.checked_add(config.max_inbound_bytes.get()))
        .ok_or(ConfigError::MemoryBudgetOverflow)?;
    let per_connection = frame_budget
        .checked_add(message_budget)
        .ok_or(ConfigError::MemoryBudgetOverflow)?;
    per_connection
        .checked_mul(config.max_connections.get())
        .ok_or(ConfigError::MemoryBudgetOverflow)?;
    Ok(())
}

fn non_zero(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).expect("configuration defaults must be non-zero")
}

fn non_zero_u32(value: u32) -> NonZeroU32 {
    NonZeroU32::new(value).expect("configuration defaults must be non-zero")
}
