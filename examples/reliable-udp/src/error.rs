use veloq::std::{error::Error as StdError, fmt, result::Result as StdResult};

use veloq_wheel::TimerError;

use crate::{config::ConfigError, cookie::CookieError, packet::PacketError};

pub type Result<T> = StdResult<T, Error>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Error {
    Config(ConfigError),
    Packet(PacketError),
    Timer(TimerError),
    Io,
    InvalidState,
    UnknownConnection,
    TooManyConnections,
    EndpointClosed,
    ConnectionClosed,
    ConnectionReset,
    HandshakeTimeout,
    CloseTimeout,
    RetransmitExhausted,
    MessageTooLarge,
    FragmentCountExceeded,
    InvalidFragment,
    ReassemblyLimit,
    ReassemblyTimeout,
    MessageAckTimeout,
    SendWindowClosed,
    ReceiveWindowClosed,
    OutboundQueueFull,
    CookieConfiguration,
    CookieExpired,
    CookieKeyUnavailable,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Config(error) => write!(f, "invalid reliable UDP configuration: {error}"),
            Self::Packet(error) => write!(f, "invalid reliable UDP packet: {error}"),
            Self::Timer(error) => write!(f, "reliable UDP timer failure: {error}"),
            Self::Io => f.write_str("reliable UDP socket I/O failed"),
            Self::InvalidState => f.write_str("operation is invalid for the session state"),
            Self::UnknownConnection => f.write_str("packet belongs to an unknown connection"),
            Self::TooManyConnections => f.write_str("endpoint connection limit reached"),
            Self::EndpointClosed => f.write_str("endpoint is closed"),
            Self::ConnectionClosed => f.write_str("connection is closed"),
            Self::ConnectionReset => f.write_str("connection was reset by the peer"),
            Self::HandshakeTimeout => f.write_str("connection handshake timed out"),
            Self::CloseTimeout => f.write_str("connection close timed out"),
            Self::RetransmitExhausted => f.write_str("maximum packet retransmissions exceeded"),
            Self::MessageTooLarge => f.write_str("message exceeds the configured message size"),
            Self::FragmentCountExceeded => f.write_str("message has too many fragments"),
            Self::InvalidFragment => f.write_str("message fragment metadata is invalid"),
            Self::ReassemblyLimit => f.write_str("message reassembly resource limit reached"),
            Self::ReassemblyTimeout => f.write_str("message reassembly timed out"),
            Self::MessageAckTimeout => f.write_str("message acknowledgement timed out"),
            Self::SendWindowClosed => f.write_str("send window or pending queue is full"),
            Self::ReceiveWindowClosed => f.write_str("receive window is full"),
            Self::OutboundQueueFull => f.write_str("endpoint outbound queue is full"),
            Self::CookieConfiguration => f.write_str("invalid cookie key or configuration"),
            Self::CookieExpired => f.write_str("handshake cookie expired"),
            Self::CookieKeyUnavailable => f.write_str("handshake cookie key is unavailable"),
        }
    }
}

impl StdError for Error {}

impl From<ConfigError> for Error {
    fn from(error: ConfigError) -> Self {
        Self::Config(error)
    }
}

impl From<PacketError> for Error {
    fn from(error: PacketError) -> Self {
        Self::Packet(error)
    }
}

impl From<CookieError> for Error {
    fn from(_: CookieError) -> Self {
        Self::CookieConfiguration
    }
}

impl From<TimerError> for Error {
    fn from(error: TimerError) -> Self {
        Self::Timer(error)
    }
}
