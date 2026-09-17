use veloq_std::{
    fmt,
    num::NonZeroU64,
    ops::{BitOr, BitOrAssign},
    vec::Vec,
};

pub const MAGIC: [u8; 2] = *b"VQ";
pub const VERSION: u8 = 1;
pub const HEADER_LEN: usize = 42;

const HALF_SEQUENCE_SPACE: u64 = 1 << 63;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PacketErrorKind {
    Truncated,
    BadMagic,
    UnsupportedVersion,
    InvalidHeaderLength,
    InvalidFlags,
    InvalidConnectionId,
    InvalidSequence,
    InvalidAck,
    InvalidPayloadLength,
    PayloadTooLarge,
    DatagramTooLarge,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PacketError {
    pub kind: PacketErrorKind,
}

impl PacketError {
    const fn new(kind: PacketErrorKind) -> Self {
        Self { kind }
    }
}

impl fmt::Display for PacketError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let message = match self.kind {
            PacketErrorKind::Truncated => "datagram is shorter than the protocol header",
            PacketErrorKind::BadMagic => "packet magic does not match VQ",
            PacketErrorKind::UnsupportedVersion => "packet version is not supported",
            PacketErrorKind::InvalidHeaderLength => "packet header length is invalid",
            PacketErrorKind::InvalidFlags => "packet flags contain an invalid combination",
            PacketErrorKind::InvalidConnectionId => "connection ID must be non-zero",
            PacketErrorKind::InvalidSequence => "data packet sequence must be non-zero",
            PacketErrorKind::InvalidAck => "acknowledgement fields are invalid",
            PacketErrorKind::InvalidPayloadLength => "payload length does not match datagram size",
            PacketErrorKind::PayloadTooLarge => "payload cannot be represented by the wire format",
            PacketErrorKind::DatagramTooLarge => "encoded datagram exceeds the configured limit",
        };
        f.write_str(message)
    }
}

impl veloq_std::error::Error for PacketError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub struct ConnectionId(NonZeroU64);

impl ConnectionId {
    pub const fn new(value: u64) -> Option<Self> {
        match NonZeroU64::new(value) {
            Some(value) => Some(Self(value)),
            None => None,
        }
    }

    pub const fn get(self) -> u64 {
        self.0.get()
    }
}

impl fmt::Display for ConnectionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.get())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub struct MessageSequence(NonZeroU64);

impl MessageSequence {
    pub const fn new(value: u64) -> Option<Self> {
        match NonZeroU64::new(value) {
            Some(value) => Some(Self(value)),
            None => None,
        }
    }

    pub const fn get(self) -> u64 {
        self.0.get()
    }

    pub const fn next(self) -> Self {
        match self.get().checked_add(1) {
            Some(value) => match Self::new(value) {
                Some(sequence) => sequence,
                None => Self::new(1).expect("one is a valid message sequence"),
            },
            None => Self::new(1).expect("one is a valid message sequence"),
        }
    }

    /// Returns the forward distance in the sequence space, excluding zero.
    ///
    /// A distance of `None` means that the two values are at least half a
    /// sequence space apart and therefore cannot be ordered unambiguously.
    pub fn forward_distance(from: Self, to: Self) -> Option<u64> {
        if from == to {
            return Some(0);
        }
        let mut distance = to.get().wrapping_sub(from.get());
        if from.get() > to.get() {
            distance = distance.wrapping_sub(1);
        }
        if distance < HALF_SEQUENCE_SPACE {
            Some(distance)
        } else {
            None
        }
    }

    pub fn is_after(self, other: Self) -> bool {
        Self::forward_distance(other, self).is_some_and(|distance| distance != 0)
    }
}

impl fmt::Display for MessageSequence {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.get())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub struct Flags(u8);

impl Flags {
    pub const NONE: Self = Self(0);
    pub const SYN: Self = Self(1 << 0);
    pub const SYN_ACK: Self = Self(1 << 1);
    pub const DATA: Self = Self(1 << 2);
    pub const ACK: Self = Self(1 << 3);
    pub const FIN: Self = Self(1 << 4);
    pub const FIN_ACK: Self = Self(1 << 5);
    pub const RST: Self = Self(1 << 6);
    pub const PING: Self = Self(1 << 7);

    pub const fn bits(self) -> u8 {
        self.0
    }

    pub const fn from_bits(bits: u8) -> Option<Self> {
        // The first wire version assigns all eight bits to named flags.
        // Invalid combinations are rejected by `is_valid`.
        Some(Self(bits))
    }

    pub const fn contains(self, other: Self) -> bool {
        self.0 & other.0 == other.0
    }

    pub const fn is_valid(self) -> bool {
        self.0 == Self::SYN.0
            || self.0 == Self::SYN_ACK.0
            || self.0 == Self::ACK.0
            || self.0 == Self::DATA.0
            || self.0 == (Self::DATA.0 | Self::ACK.0)
            || self.0 == Self::FIN.0
            || self.0 == Self::FIN_ACK.0
            || self.0 == (Self::FIN.0 | Self::ACK.0)
            || self.0 == Self::RST.0
            || self.0 == Self::PING.0
            || self.0 == (Self::PING.0 | Self::ACK.0)
    }
}

impl BitOr for Flags {
    type Output = Self;

    fn bitor(self, rhs: Self) -> Self::Output {
        Self(self.0 | rhs.0)
    }
}

impl BitOrAssign for Flags {
    fn bitor_assign(&mut self, rhs: Self) {
        self.0 |= rhs.0;
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Ack {
    largest: Option<MessageSequence>,
    bitmap: u64,
}

impl Ack {
    pub const fn empty() -> Self {
        Self {
            largest: None,
            bitmap: 0,
        }
    }

    pub const fn new(largest: Option<MessageSequence>, bitmap: u64) -> Option<Self> {
        if largest.is_none() && bitmap != 0 {
            None
        } else {
            Some(Self { largest, bitmap })
        }
    }

    pub const fn largest(self) -> Option<MessageSequence> {
        self.largest
    }

    pub const fn bitmap(self) -> u64 {
        self.bitmap
    }

    pub fn acknowledges(self, sequence: MessageSequence) -> bool {
        let Some(largest) = self.largest else {
            return false;
        };
        if sequence == largest {
            return true;
        }
        let Some(distance) = MessageSequence::forward_distance(sequence, largest) else {
            return false;
        };
        if !(1..=64).contains(&distance) {
            return false;
        }
        self.bitmap & (1u64 << (distance - 1)) != 0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AckObserve {
    NewLargest,
    NewOutOfOrder,
    Duplicate,
    TooOld,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AckWindow {
    largest: Option<MessageSequence>,
    bitmap: u64,
}

impl Default for AckWindow {
    fn default() -> Self {
        Self::new()
    }
}

impl AckWindow {
    pub const fn new() -> Self {
        Self {
            largest: None,
            bitmap: 0,
        }
    }

    pub const fn is_empty(self) -> bool {
        self.largest.is_none()
    }

    pub const fn largest(self) -> Option<MessageSequence> {
        self.largest
    }

    pub const fn bitmap(self) -> u64 {
        self.bitmap
    }

    pub const fn ack(self) -> Ack {
        Ack {
            largest: self.largest,
            bitmap: self.bitmap,
        }
    }

    pub fn contains(self, sequence: MessageSequence) -> bool {
        self.ack().acknowledges(sequence)
    }

    pub fn observe(&mut self, sequence: MessageSequence) -> AckObserve {
        let Some(largest) = self.largest else {
            self.largest = Some(sequence);
            return AckObserve::NewLargest;
        };

        if sequence == largest {
            return AckObserve::Duplicate;
        }

        if let Some(distance) = MessageSequence::forward_distance(largest, sequence)
            && distance != 0
        {
            if distance <= 64 {
                self.bitmap = (self.bitmap << distance) | (1u64 << (distance - 1));
            } else {
                self.bitmap = 0;
            }
            self.largest = Some(sequence);
            return AckObserve::NewLargest;
        }

        let Some(distance) = MessageSequence::forward_distance(sequence, largest) else {
            return AckObserve::TooOld;
        };
        if !(1..=64).contains(&distance) {
            return AckObserve::TooOld;
        }

        let mask = 1u64 << (distance - 1);
        if self.bitmap & mask != 0 {
            AckObserve::Duplicate
        } else {
            self.bitmap |= mask;
            AckObserve::NewOutOfOrder
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Packet {
    pub flags: Flags,
    pub connection_id: ConnectionId,
    pub sequence: u64,
    pub ack_largest: u64,
    pub ack_bitmap: u64,
    pub receive_window: u16,
    pub payload: Vec<u8>,
}

/// 借用式数据报视图。
///
/// 该视图只解析固定头部，不复制 payload。端点接收泵可以保留底层
/// `FixedBuf` 的所有权，协议层只在确认接纳消息后复制 payload；这避免了先把
/// 整个 UDP 数据报复制到 `Vec` 再进行头部解析。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PacketRef<'a> {
    pub flags: Flags,
    pub connection_id: ConnectionId,
    pub sequence: u64,
    pub ack_largest: u64,
    pub ack_bitmap: u64,
    pub receive_window: u16,
    pub payload: &'a [u8],
}

impl Packet {
    pub fn new(
        flags: Flags,
        connection_id: ConnectionId,
        sequence: u64,
        ack: Ack,
        receive_window: u16,
        payload: Vec<u8>,
    ) -> Self {
        Self {
            flags,
            connection_id,
            sequence,
            ack_largest: ack.largest.map_or(0, MessageSequence::get),
            ack_bitmap: ack.bitmap,
            receive_window,
            payload,
        }
    }

    pub fn ack(&self) -> Result<Ack, PacketError> {
        ack_from_wire(self.ack_largest, self.ack_bitmap)
    }

    pub fn encode(&self) -> Result<Vec<u8>, PacketError> {
        self.encode_with_limit(usize::MAX)
    }

    pub fn encode_with_limit(&self, max_datagram_size: usize) -> Result<Vec<u8>, PacketError> {
        self.validate()?;
        let payload_len = u16::try_from(self.payload.len())
            .map_err(|_| PacketError::new(PacketErrorKind::PayloadTooLarge))?;
        let total_len = HEADER_LEN
            .checked_add(usize::from(payload_len))
            .ok_or(PacketError::new(PacketErrorKind::PayloadTooLarge))?;
        if total_len > max_datagram_size {
            return Err(PacketError::new(PacketErrorKind::DatagramTooLarge));
        }

        let mut datagram = Vec::with_capacity(total_len);
        datagram.extend_from_slice(&MAGIC);
        datagram.push(VERSION);
        datagram.push(self.flags.bits());
        datagram.extend_from_slice(&(HEADER_LEN as u16).to_le_bytes());
        datagram.extend_from_slice(&self.connection_id.get().to_le_bytes());
        datagram.extend_from_slice(&self.sequence.to_le_bytes());
        datagram.extend_from_slice(&self.ack_largest.to_le_bytes());
        datagram.extend_from_slice(&self.ack_bitmap.to_le_bytes());
        datagram.extend_from_slice(&self.receive_window.to_le_bytes());
        datagram.extend_from_slice(&payload_len.to_le_bytes());
        datagram.extend_from_slice(&self.payload);
        Ok(datagram)
    }

    pub fn decode(datagram: &[u8]) -> Result<Self, PacketError> {
        PacketRef::decode(datagram).map(PacketRef::to_owned)
    }

    fn validate(&self) -> Result<(), PacketError> {
        if !self.flags.is_valid() {
            return Err(PacketError::new(PacketErrorKind::InvalidFlags));
        }
        if self.connection_id.get() == 0 {
            return Err(PacketError::new(PacketErrorKind::InvalidConnectionId));
        }
        if self.flags.contains(Flags::DATA) && self.sequence == 0 {
            return Err(PacketError::new(PacketErrorKind::InvalidSequence));
        }
        ack_from_wire(self.ack_largest, self.ack_bitmap)?;
        if self.ack_largest != 0 && !self.flags.contains(Flags::ACK) {
            return Err(PacketError::new(PacketErrorKind::InvalidAck));
        }
        if self.payload.len() > usize::from(u16::MAX) {
            return Err(PacketError::new(PacketErrorKind::PayloadTooLarge));
        }
        Ok(())
    }
}

impl<'a> PacketRef<'a> {
    pub fn decode(datagram: &'a [u8]) -> Result<Self, PacketError> {
        if datagram.len() < HEADER_LEN {
            return Err(PacketError::new(PacketErrorKind::Truncated));
        }
        if datagram[0..2] != MAGIC {
            return Err(PacketError::new(PacketErrorKind::BadMagic));
        }
        if datagram[2] != VERSION {
            return Err(PacketError::new(PacketErrorKind::UnsupportedVersion));
        }

        let header_len = usize::from(read_u16(datagram, 4));
        if header_len != HEADER_LEN {
            return Err(PacketError::new(PacketErrorKind::InvalidHeaderLength));
        }
        let flags = Flags::from_bits(datagram[3])
            .filter(|flags| flags.is_valid())
            .ok_or(PacketError::new(PacketErrorKind::InvalidFlags))?;
        let connection_id = ConnectionId::new(read_u64(datagram, 6))
            .ok_or(PacketError::new(PacketErrorKind::InvalidConnectionId))?;
        let sequence = read_u64(datagram, 14);
        if flags.contains(Flags::DATA) && sequence == 0 {
            return Err(PacketError::new(PacketErrorKind::InvalidSequence));
        }
        let ack_largest = read_u64(datagram, 22);
        let ack_bitmap = read_u64(datagram, 30);
        ack_from_wire(ack_largest, ack_bitmap)?;
        if ack_largest != 0 && !flags.contains(Flags::ACK) {
            return Err(PacketError::new(PacketErrorKind::InvalidAck));
        }
        let payload_len = usize::from(read_u16(datagram, 40));
        let expected_len = header_len
            .checked_add(payload_len)
            .ok_or(PacketError::new(PacketErrorKind::InvalidPayloadLength))?;
        if expected_len != datagram.len() {
            return Err(PacketError::new(PacketErrorKind::InvalidPayloadLength));
        }

        Ok(Self {
            flags,
            connection_id,
            sequence,
            ack_largest,
            ack_bitmap,
            receive_window: read_u16(datagram, 38),
            payload: &datagram[header_len..expected_len],
        })
    }

    pub fn ack(&self) -> Result<Ack, PacketError> {
        ack_from_wire(self.ack_largest, self.ack_bitmap)
    }

    pub fn to_owned(self) -> Packet {
        Packet {
            flags: self.flags,
            connection_id: self.connection_id,
            sequence: self.sequence,
            ack_largest: self.ack_largest,
            ack_bitmap: self.ack_bitmap,
            receive_window: self.receive_window,
            payload: self.payload.to_vec(),
        }
    }
}

fn ack_from_wire(largest: u64, bitmap: u64) -> Result<Ack, PacketError> {
    let largest = MessageSequence::new(largest);
    if largest.is_none() && bitmap != 0 {
        return Err(PacketError::new(PacketErrorKind::InvalidAck));
    }
    Ok(Ack { largest, bitmap })
}

fn read_u16(data: &[u8], offset: usize) -> u16 {
    u16::from_le_bytes([data[offset], data[offset + 1]])
}

fn read_u64(data: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes([
        data[offset],
        data[offset + 1],
        data[offset + 2],
        data[offset + 3],
        data[offset + 4],
        data[offset + 5],
        data[offset + 6],
        data[offset + 7],
    ])
}

pub(crate) fn non_zero_sequence(value: u64) -> Result<MessageSequence, PacketError> {
    MessageSequence::new(value).ok_or(PacketError::new(PacketErrorKind::InvalidSequence))
}
