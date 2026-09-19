use veloq::{
    buf::FixedBuf,
    runtime::context::Ctx,
    std::{
        error::Error as StdError,
        fmt,
        num::{NonZeroU32, NonZeroU64, NonZeroUsize},
        ops::Range,
    },
};

pub const MAGIC: [u8; 2] = *b"VQ";
pub const VERSION: u8 = 4;
pub const HEADER_LEN: usize = 80;
pub const COOKIE_LEN: usize = 32;
pub const CONTROL_VERSION: u8 = 1;
pub const STREAM_RESET_MAX_PAYLOAD: usize = 16;

const HALF_SEQUENCE_SPACE: u64 = 1 << 63;
const FRAME_SEQUENCE_OFFSET: usize = 16;
const ACK_LARGEST_OFFSET: usize = 24;
const ACK_BITMAP_OFFSET: usize = 32;
const RECEIVE_WINDOW_OFFSET: usize = 40;
const PAYLOAD_LEN_OFFSET: usize = 42;
const STREAM_ID_OFFSET: usize = 44;
const STREAM_SEQUENCE_OFFSET: usize = 48;
const MESSAGE_ID_OFFSET: usize = 56;
const FRAGMENT_INDEX_OFFSET: usize = 64;
const FRAGMENT_COUNT_OFFSET: usize = 68;
const MESSAGE_LEN_OFFSET: usize = 72;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PacketErrorKind {
    Truncated,
    BadMagic,
    UnsupportedVersion,
    InvalidFrameType,
    InvalidHeaderLength,
    InvalidAckFlag,
    NonZeroReserved,
    InvalidConnectionId,
    InvalidStreamId,
    InvalidSequence,
    InvalidAck,
    InvalidPayloadLength,
    InvalidCookiePayload,
    CookiePayloadTooLarge,
    PayloadTooLarge,
    DatagramTooLarge,
    InvalidFragment,
    FragmentCountExceeded,
    MessageTooLarge,
    InvalidControlPayload,
    BufferAllocationFailed,
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
            PacketErrorKind::InvalidFrameType => "packet frame type is invalid",
            PacketErrorKind::InvalidHeaderLength => "packet header length is invalid",
            PacketErrorKind::InvalidAckFlag => "acknowledgement flag is invalid",
            PacketErrorKind::NonZeroReserved => "reserved header byte must be zero",
            PacketErrorKind::InvalidConnectionId => "connection ID must be non-zero",
            PacketErrorKind::InvalidStreamId => "stream ID is invalid for this frame",
            PacketErrorKind::InvalidSequence => "packet sequence is invalid",
            PacketErrorKind::InvalidAck => "acknowledgement fields are invalid",
            PacketErrorKind::InvalidPayloadLength => "payload length does not match datagram size",
            PacketErrorKind::InvalidCookiePayload => "handshake cookie payload is invalid",
            PacketErrorKind::CookiePayloadTooLarge => "handshake cookie payload is too large",
            PacketErrorKind::PayloadTooLarge => "payload cannot be represented by the wire format",
            PacketErrorKind::DatagramTooLarge => "encoded datagram exceeds the configured limit",
            PacketErrorKind::InvalidFragment => "fragment metadata or payload length is invalid",
            PacketErrorKind::FragmentCountExceeded => "fragment count exceeds the configured limit",
            PacketErrorKind::MessageTooLarge => "message cannot be represented by the wire format",
            PacketErrorKind::InvalidControlPayload => "control payload is invalid",
            PacketErrorKind::BufferAllocationFailed => "packet buffer allocation failed",
        };
        f.write_str(message)
    }
}

impl StdError for PacketError {}

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

macro_rules! sequence_type {
    ($name:ident, $display:literal) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
        #[repr(transparent)]
        pub struct $name(NonZeroU64);

        impl $name {
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
                        None => Self::new(1).expect("one is a valid sequence"),
                    },
                    None => Self::new(1).expect("one is a valid sequence"),
                }
            }

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

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, $display, self.get())
            }
        }
    };
}

sequence_type!(FrameSequence, "{}");
sequence_type!(StreamSequence, "{}");
sequence_type!(MessageId, "{}");

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[repr(transparent)]
pub struct StreamId(NonZeroU32);

impl StreamId {
    pub const fn new(value: u32) -> Option<Self> {
        match NonZeroU32::new(value) {
            Some(value) => Some(Self(value)),
            None => None,
        }
    }

    pub const fn get(self) -> u32 {
        self.0.get()
    }

    pub const fn is_client_initiated(self) -> bool {
        self.get() & 1 == 1
    }

    pub const fn is_server_initiated(self) -> bool {
        !self.is_client_initiated()
    }

    pub const fn next_after(self) -> Self {
        let value = match self.get().checked_add(2) {
            Some(value) => value,
            None => {
                if self.is_client_initiated() {
                    1
                } else {
                    2
                }
            }
        };
        Self::new(value).expect("stream ID is non-zero")
    }
}

impl fmt::Display for StreamId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.get())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum FrameType {
    Syn = 1,
    SynAck = 2,
    HandshakeAck = 3,
    Ack = 4,
    Data = 5,
    MessageAck = 6,
    StreamOpen = 7,
    StreamOpenAck = 8,
    StreamFin = 9,
    StreamReset = 10,
    MaxStreamData = 11,
    MaxData = 12,
    MaxStreams = 13,
    Fin = 14,
    FinAck = 15,
    Rst = 16,
    Ping = 17,
}

impl FrameType {
    pub const fn from_wire(value: u8) -> Option<Self> {
        Some(match value {
            1 => Self::Syn,
            2 => Self::SynAck,
            3 => Self::HandshakeAck,
            4 => Self::Ack,
            5 => Self::Data,
            6 => Self::MessageAck,
            7 => Self::StreamOpen,
            8 => Self::StreamOpenAck,
            9 => Self::StreamFin,
            10 => Self::StreamReset,
            11 => Self::MaxStreamData,
            12 => Self::MaxData,
            13 => Self::MaxStreams,
            14 => Self::Fin,
            15 => Self::FinAck,
            16 => Self::Rst,
            17 => Self::Ping,
            _ => return None,
        })
    }

    pub const fn is_reliable(self) -> bool {
        !matches!(
            self,
            Self::Syn
                | Self::SynAck
                | Self::HandshakeAck
                | Self::Ack
                | Self::MessageAck
                | Self::Rst
        )
    }

    pub const fn is_stream(self) -> bool {
        matches!(
            self,
            Self::Data
                | Self::MessageAck
                | Self::StreamOpen
                | Self::StreamOpenAck
                | Self::StreamFin
                | Self::StreamReset
                | Self::MaxStreamData
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Ack {
    largest: Option<FrameSequence>,
    bitmap: u64,
}

impl Ack {
    pub const fn empty() -> Self {
        Self {
            largest: None,
            bitmap: 0,
        }
    }

    pub const fn new(largest: Option<FrameSequence>, bitmap: u64) -> Option<Self> {
        if largest.is_none() && bitmap != 0 {
            None
        } else {
            Some(Self { largest, bitmap })
        }
    }

    pub const fn largest(self) -> Option<FrameSequence> {
        self.largest
    }

    pub const fn bitmap(self) -> u64 {
        self.bitmap
    }

    pub fn acknowledges(self, sequence: FrameSequence) -> bool {
        let Some(largest) = self.largest else {
            return false;
        };
        if sequence == largest {
            return true;
        }
        let Some(distance) = FrameSequence::forward_distance(sequence, largest) else {
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
    largest: Option<FrameSequence>,
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

    pub const fn largest(self) -> Option<FrameSequence> {
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

    pub fn contains(self, sequence: FrameSequence) -> bool {
        self.ack().acknowledges(sequence)
    }

    pub fn observe(&mut self, sequence: FrameSequence) -> AckObserve {
        let Some(largest) = self.largest else {
            self.largest = Some(sequence);
            return AckObserve::NewLargest;
        };
        if sequence == largest {
            return AckObserve::Duplicate;
        }
        if let Some(distance) = FrameSequence::forward_distance(largest, sequence)
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
        let Some(distance) = FrameSequence::forward_distance(sequence, largest) else {
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

pub trait PacketBufAllocator {
    fn alloc_packet_buf(&self, capacity: NonZeroUsize, len: usize)
    -> Result<FixedBuf, PacketError>;
}

#[derive(Debug, Clone, Copy, Default)]
pub struct HeapPacketBufAllocator;

impl PacketBufAllocator for HeapPacketBufAllocator {
    fn alloc_packet_buf(
        &self,
        capacity: NonZeroUsize,
        len: usize,
    ) -> Result<FixedBuf, PacketError> {
        FixedBuf::alloc_heap(capacity, len)
            .map_err(|_| PacketError::new(PacketErrorKind::BufferAllocationFailed))
    }
}

impl<'rt> PacketBufAllocator for Ctx<'rt> {
    fn alloc_packet_buf(
        &self,
        capacity: NonZeroUsize,
        len: usize,
    ) -> Result<FixedBuf, PacketError> {
        self.try_alloc(capacity, len)
            .map_err(|_| PacketError::new(PacketErrorKind::BufferAllocationFailed))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PacketMeta {
    pub frame_type: FrameType,
    pub has_ack: bool,
    pub connection_id: ConnectionId,
    pub frame_sequence: u64,
    pub ack_largest: u64,
    pub ack_bitmap: u64,
    pub receive_window: u16,
    pub payload_range: Range<usize>,
    pub stream_id: u32,
    pub stream_sequence: u64,
    pub message_id: u64,
    pub fragment_index: u32,
    pub fragment_count: u32,
    pub message_len: u64,
}

impl PacketMeta {
    pub fn ack(&self) -> Result<Ack, PacketError> {
        ack_from_wire(self.ack_largest, self.ack_bitmap)
    }
}

#[derive(Debug)]
pub struct Packet {
    meta: PacketMeta,
    datagram: FixedBuf,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PacketRef<'a> {
    pub frame_type: FrameType,
    pub has_ack: bool,
    pub connection_id: ConnectionId,
    pub frame_sequence: u64,
    pub ack_largest: u64,
    pub ack_bitmap: u64,
    pub receive_window: u16,
    pub stream_id: u32,
    pub stream_sequence: u64,
    pub message_id: u64,
    pub fragment_index: u32,
    pub fragment_count: u32,
    pub message_len: u64,
    pub payload: &'a [u8],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StreamDataPacket<'a> {
    pub connection_id: ConnectionId,
    pub frame_sequence: FrameSequence,
    pub stream_id: StreamId,
    pub stream_sequence: StreamSequence,
    pub ack: Ack,
    pub receive_window: u16,
    pub message_id: MessageId,
    pub fragment_index: u32,
    pub fragment_count: u32,
    pub message_len: u64,
    pub payload: &'a [u8],
    pub max_fragment_payload: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StreamOpenPayload {
    pub frame_window: u16,
    pub byte_credit: u64,
}

impl StreamOpenPayload {
    pub const WIRE_LEN: usize = 12;

    pub fn encode(self) -> [u8; Self::WIRE_LEN] {
        let mut bytes = [0; Self::WIRE_LEN];
        bytes[0] = CONTROL_VERSION;
        bytes[2..4].copy_from_slice(&self.frame_window.to_le_bytes());
        bytes[4..12].copy_from_slice(&self.byte_credit.to_le_bytes());
        bytes
    }

    pub fn decode(payload: &[u8]) -> Result<Self, PacketError> {
        if payload.len() != Self::WIRE_LEN || payload[0] != CONTROL_VERSION || payload[1] != 0 {
            return Err(PacketError::new(PacketErrorKind::InvalidControlPayload));
        }
        let frame_window = u16::from_le_bytes([payload[2], payload[3]]);
        if frame_window == 0 {
            return Err(PacketError::new(PacketErrorKind::InvalidControlPayload));
        }
        Ok(Self {
            frame_window,
            byte_credit: read_u64(payload, 4),
        })
    }
}

pub fn encode_credit_payload(credit: u64) -> [u8; 8] {
    credit.to_le_bytes()
}

pub fn decode_credit_payload(payload: &[u8]) -> Result<u64, PacketError> {
    if payload.len() != 8 {
        return Err(PacketError::new(PacketErrorKind::InvalidControlPayload));
    }
    Ok(read_u64(payload, 0))
}

pub fn encode_max_streams_payload(ordinal: u32) -> [u8; 4] {
    ordinal.to_le_bytes()
}

pub fn decode_max_streams_payload(payload: &[u8]) -> Result<u32, PacketError> {
    if payload.len() != 4 {
        return Err(PacketError::new(PacketErrorKind::InvalidControlPayload));
    }
    Ok(read_u32(payload, 0))
}

struct PacketFields<'a> {
    frame_type: FrameType,
    has_ack: bool,
    connection_id: ConnectionId,
    frame_sequence: u64,
    ack: Ack,
    receive_window: u16,
    stream_id: u32,
    stream_sequence: u64,
    message_id: u64,
    fragment_index: u32,
    fragment_count: u32,
    message_len: u64,
    payload: &'a [u8],
    max_fragment_payload: Option<usize>,
}

impl Packet {
    #[allow(clippy::too_many_arguments)]
    pub fn encode_frame<A: PacketBufAllocator + ?Sized>(
        allocator: &A,
        max_datagram_size: NonZeroUsize,
        frame_type: FrameType,
        has_ack: bool,
        connection_id: ConnectionId,
        frame_sequence: Option<FrameSequence>,
        stream_id: Option<StreamId>,
        ack: Ack,
        receive_window: u16,
        payload: &[u8],
    ) -> Result<Self, PacketError> {
        Self::encode_fields(
            allocator,
            max_datagram_size,
            PacketFields {
                frame_type,
                has_ack,
                connection_id,
                frame_sequence: frame_sequence.map_or(0, FrameSequence::get),
                ack,
                receive_window,
                stream_id: stream_id.map_or(0, StreamId::get),
                stream_sequence: 0,
                message_id: 0,
                fragment_index: 0,
                fragment_count: 0,
                message_len: 0,
                payload,
                max_fragment_payload: None,
            },
        )
    }

    pub fn encode_stream_data_into_with_limit<A: PacketBufAllocator + ?Sized>(
        allocator: &A,
        max_datagram_size: NonZeroUsize,
        data: StreamDataPacket<'_>,
    ) -> Result<Self, PacketError> {
        Self::encode_fields(
            allocator,
            max_datagram_size,
            PacketFields {
                frame_type: FrameType::Data,
                has_ack: data.ack.largest().is_some(),
                connection_id: data.connection_id,
                frame_sequence: data.frame_sequence.get(),
                ack: data.ack,
                receive_window: data.receive_window,
                stream_id: data.stream_id.get(),
                stream_sequence: data.stream_sequence.get(),
                message_id: data.message_id.get(),
                fragment_index: data.fragment_index,
                fragment_count: data.fragment_count,
                message_len: data.message_len,
                payload: data.payload,
                max_fragment_payload: Some(data.max_fragment_payload),
            },
        )
    }

    pub fn encode_stream_message_ack_into_with_limit<A: PacketBufAllocator + ?Sized>(
        allocator: &A,
        max_datagram_size: NonZeroUsize,
        connection_id: ConnectionId,
        stream_id: StreamId,
        ack: Ack,
        receive_window: u16,
        message_id: MessageId,
    ) -> Result<Self, PacketError> {
        Self::encode_fields(
            allocator,
            max_datagram_size,
            PacketFields {
                frame_type: FrameType::MessageAck,
                has_ack: ack.largest().is_some(),
                connection_id,
                frame_sequence: 0,
                ack,
                receive_window,
                stream_id: stream_id.get(),
                stream_sequence: 0,
                message_id: message_id.get(),
                fragment_index: 0,
                fragment_count: 0,
                message_len: 0,
                payload: &[],
                max_fragment_payload: None,
            },
        )
    }

    fn encode_fields<A: PacketBufAllocator + ?Sized>(
        allocator: &A,
        max_datagram_size: NonZeroUsize,
        fields: PacketFields<'_>,
    ) -> Result<Self, PacketError> {
        let payload_len = u16::try_from(fields.payload.len())
            .map_err(|_| PacketError::new(PacketErrorKind::PayloadTooLarge))?;
        let total_len = HEADER_LEN
            .checked_add(usize::from(payload_len))
            .ok_or(PacketError::new(PacketErrorKind::DatagramTooLarge))?;
        if total_len > max_datagram_size.get() {
            return Err(PacketError::new(PacketErrorKind::DatagramTooLarge));
        }
        let meta = PacketMeta {
            frame_type: fields.frame_type,
            has_ack: fields.has_ack,
            connection_id: fields.connection_id,
            frame_sequence: fields.frame_sequence,
            ack_largest: fields.ack.largest.map_or(0, FrameSequence::get),
            ack_bitmap: fields.ack.bitmap,
            receive_window: fields.receive_window,
            payload_range: HEADER_LEN..total_len,
            stream_id: fields.stream_id,
            stream_sequence: fields.stream_sequence,
            message_id: fields.message_id,
            fragment_index: fields.fragment_index,
            fragment_count: fields.fragment_count,
            message_len: fields.message_len,
        };
        validate_meta(&meta, fields.max_fragment_payload)?;
        let mut datagram = allocator.alloc_packet_buf(max_datagram_size, total_len)?;
        if datagram.capacity() < total_len {
            return Err(PacketError::new(PacketErrorKind::DatagramTooLarge));
        }
        let bytes = datagram.spare_capacity_mut();
        bytes[..2].copy_from_slice(&MAGIC);
        bytes[2] = VERSION;
        bytes[3] = fields.frame_type as u8;
        bytes[4] = if fields.has_ack { 1 } else { 0 };
        bytes[5] = 0;
        bytes[6..8].copy_from_slice(&(HEADER_LEN as u16).to_le_bytes());
        bytes[8..16].copy_from_slice(&fields.connection_id.get().to_le_bytes());
        bytes[FRAME_SEQUENCE_OFFSET..24].copy_from_slice(&fields.frame_sequence.to_le_bytes());
        bytes[ACK_LARGEST_OFFSET..32].copy_from_slice(&meta.ack_largest.to_le_bytes());
        bytes[ACK_BITMAP_OFFSET..40].copy_from_slice(&meta.ack_bitmap.to_le_bytes());
        bytes[RECEIVE_WINDOW_OFFSET..42].copy_from_slice(&fields.receive_window.to_le_bytes());
        bytes[PAYLOAD_LEN_OFFSET..44].copy_from_slice(&payload_len.to_le_bytes());
        bytes[STREAM_ID_OFFSET..48].copy_from_slice(&fields.stream_id.to_le_bytes());
        bytes[STREAM_SEQUENCE_OFFSET..56].copy_from_slice(&fields.stream_sequence.to_le_bytes());
        bytes[MESSAGE_ID_OFFSET..64].copy_from_slice(&fields.message_id.to_le_bytes());
        bytes[FRAGMENT_INDEX_OFFSET..68].copy_from_slice(&fields.fragment_index.to_le_bytes());
        bytes[FRAGMENT_COUNT_OFFSET..72].copy_from_slice(&fields.fragment_count.to_le_bytes());
        bytes[MESSAGE_LEN_OFFSET..80].copy_from_slice(&fields.message_len.to_le_bytes());
        bytes[HEADER_LEN..total_len].copy_from_slice(fields.payload);
        datagram.set_len(total_len);
        Ok(Self { meta, datagram })
    }

    pub fn from_fixed_buf(datagram: FixedBuf) -> Result<Self, PacketError> {
        let meta = PacketRef::decode(datagram.as_slice())?.meta();
        Ok(Self { meta, datagram })
    }

    pub fn as_ref(&self) -> PacketRef<'_> {
        PacketRef {
            frame_type: self.meta.frame_type,
            has_ack: self.meta.has_ack,
            connection_id: self.meta.connection_id,
            frame_sequence: self.meta.frame_sequence,
            ack_largest: self.meta.ack_largest,
            ack_bitmap: self.meta.ack_bitmap,
            receive_window: self.meta.receive_window,
            stream_id: self.meta.stream_id,
            stream_sequence: self.meta.stream_sequence,
            message_id: self.meta.message_id,
            fragment_index: self.meta.fragment_index,
            fragment_count: self.meta.fragment_count,
            message_len: self.meta.message_len,
            payload: &self.datagram.as_slice()[self.meta.payload_range.clone()],
        }
    }

    pub fn as_slice(&self) -> &[u8] {
        self.datagram.as_slice()
    }

    pub fn into_fixed_buf(self) -> FixedBuf {
        self.datagram
    }

    pub fn meta(&self) -> &PacketMeta {
        &self.meta
    }

    pub fn ack(&self) -> Result<Ack, PacketError> {
        self.meta.ack()
    }
}

impl<'a> PacketRef<'a> {
    pub fn decode(datagram: &'a [u8]) -> Result<Self, PacketError> {
        Self::decode_with_constraints(datagram, 0, u64::MAX, u32::MAX)
    }

    pub fn decode_with_constraints(
        datagram: &'a [u8],
        max_fragment_payload: usize,
        max_message_size: u64,
        max_fragments: u32,
    ) -> Result<Self, PacketError> {
        if datagram.len() < 2 {
            return Err(PacketError::new(PacketErrorKind::Truncated));
        }
        if datagram[0..2] != MAGIC {
            return Err(PacketError::new(PacketErrorKind::BadMagic));
        }
        if datagram.len() < 3 {
            return Err(PacketError::new(PacketErrorKind::Truncated));
        }
        if datagram[2] != VERSION {
            return Err(PacketError::new(PacketErrorKind::UnsupportedVersion));
        }
        if datagram.len() < HEADER_LEN {
            return Err(PacketError::new(PacketErrorKind::Truncated));
        }
        let frame_type = FrameType::from_wire(datagram[3])
            .ok_or(PacketError::new(PacketErrorKind::InvalidFrameType))?;
        let has_ack = match datagram[4] {
            0 => false,
            1 => true,
            _ => return Err(PacketError::new(PacketErrorKind::InvalidAckFlag)),
        };
        if datagram[5] != 0 {
            return Err(PacketError::new(PacketErrorKind::NonZeroReserved));
        }
        let header_len = usize::from(read_u16(datagram, 6));
        if header_len != HEADER_LEN {
            return Err(PacketError::new(PacketErrorKind::InvalidHeaderLength));
        }
        let connection_id = ConnectionId::new(read_u64(datagram, 8))
            .ok_or(PacketError::new(PacketErrorKind::InvalidConnectionId))?;
        let frame_sequence = read_u64(datagram, FRAME_SEQUENCE_OFFSET);
        let ack_largest = read_u64(datagram, ACK_LARGEST_OFFSET);
        let ack_bitmap = read_u64(datagram, ACK_BITMAP_OFFSET);
        ack_from_wire(ack_largest, ack_bitmap)?;
        if !has_ack && (ack_largest != 0 || ack_bitmap != 0) {
            return Err(PacketError::new(PacketErrorKind::InvalidAck));
        }
        let payload_len = usize::from(read_u16(datagram, PAYLOAD_LEN_OFFSET));
        let expected_len = header_len
            .checked_add(payload_len)
            .ok_or(PacketError::new(PacketErrorKind::InvalidPayloadLength))?;
        if expected_len != datagram.len() {
            return Err(PacketError::new(PacketErrorKind::InvalidPayloadLength));
        }
        let stream_id = read_u32(datagram, STREAM_ID_OFFSET);
        let meta = PacketMeta {
            frame_type,
            has_ack,
            connection_id,
            frame_sequence,
            ack_largest,
            ack_bitmap,
            receive_window: read_u16(datagram, RECEIVE_WINDOW_OFFSET),
            payload_range: header_len..expected_len,
            stream_id,
            stream_sequence: read_u64(datagram, STREAM_SEQUENCE_OFFSET),
            message_id: read_u64(datagram, MESSAGE_ID_OFFSET),
            fragment_index: read_u32(datagram, FRAGMENT_INDEX_OFFSET),
            fragment_count: read_u32(datagram, FRAGMENT_COUNT_OFFSET),
            message_len: read_u64(datagram, MESSAGE_LEN_OFFSET),
        };
        if meta.message_len > max_message_size {
            return Err(PacketError::new(PacketErrorKind::MessageTooLarge));
        }
        if meta.fragment_count > max_fragments {
            return Err(PacketError::new(PacketErrorKind::FragmentCountExceeded));
        }
        let layout_limit = (max_fragment_payload != 0).then_some(max_fragment_payload);
        validate_meta(&meta, layout_limit)?;
        Ok(Self {
            frame_type,
            has_ack,
            connection_id,
            frame_sequence,
            ack_largest,
            ack_bitmap,
            receive_window: meta.receive_window,
            stream_id,
            stream_sequence: meta.stream_sequence,
            message_id: meta.message_id,
            fragment_index: meta.fragment_index,
            fragment_count: meta.fragment_count,
            message_len: meta.message_len,
            payload: &datagram[header_len..expected_len],
        })
    }

    pub fn ack(&self) -> Result<Ack, PacketError> {
        ack_from_wire(self.ack_largest, self.ack_bitmap)
    }

    pub fn handshake_cookie(&self) -> Result<&'a [u8; COOKIE_LEN], PacketError> {
        if !(self.frame_type == FrameType::SynAck || self.frame_type == FrameType::HandshakeAck)
            || self.has_ack
            || self.payload.len() != COOKIE_LEN
            || self.ack_largest != 0
            || self.ack_bitmap != 0
        {
            return Err(PacketError::new(PacketErrorKind::InvalidCookiePayload));
        }
        self.payload
            .try_into()
            .map_err(|_| PacketError::new(PacketErrorKind::InvalidCookiePayload))
    }

    pub fn meta(&self) -> PacketMeta {
        PacketMeta {
            frame_type: self.frame_type,
            has_ack: self.has_ack,
            connection_id: self.connection_id,
            frame_sequence: self.frame_sequence,
            ack_largest: self.ack_largest,
            ack_bitmap: self.ack_bitmap,
            receive_window: self.receive_window,
            payload_range: HEADER_LEN..HEADER_LEN + self.payload.len(),
            stream_id: self.stream_id,
            stream_sequence: self.stream_sequence,
            message_id: self.message_id,
            fragment_index: self.fragment_index,
            fragment_count: self.fragment_count,
            message_len: self.message_len,
        }
    }
}

fn validate_meta(
    meta: &PacketMeta,
    max_fragment_payload: Option<usize>,
) -> Result<(), PacketError> {
    if meta.connection_id.get() == 0 {
        return Err(PacketError::new(PacketErrorKind::InvalidConnectionId));
    }
    ack_from_wire(meta.ack_largest, meta.ack_bitmap)?;
    if !meta.has_ack && (meta.ack_largest != 0 || meta.ack_bitmap != 0) {
        return Err(PacketError::new(PacketErrorKind::InvalidAck));
    }
    if meta.frame_type == FrameType::Ack && !meta.has_ack {
        return Err(PacketError::new(PacketErrorKind::InvalidAckFlag));
    }
    if meta.payload_range.len() > usize::from(u16::MAX) {
        return Err(PacketError::new(PacketErrorKind::PayloadTooLarge));
    }
    if meta.frame_type.is_stream() {
        if StreamId::new(meta.stream_id).is_none() {
            return Err(PacketError::new(PacketErrorKind::InvalidStreamId));
        }
    } else if meta.stream_id != 0 {
        return Err(PacketError::new(PacketErrorKind::InvalidStreamId));
    }

    match meta.frame_type {
        FrameType::Data => {
            if meta.frame_sequence == 0 || meta.stream_sequence == 0 || meta.message_id == 0 {
                return Err(PacketError::new(PacketErrorKind::InvalidSequence));
            }
            if meta.fragment_count == 0 || meta.fragment_index >= meta.fragment_count {
                return Err(PacketError::new(PacketErrorKind::InvalidFragment));
            }
            validate_fragment_layout(meta, max_fragment_payload)?;
        }
        FrameType::MessageAck => {
            if meta.frame_sequence != 0
                || meta.stream_sequence != 0
                || meta.message_id == 0
                || meta.fragment_index != 0
                || meta.fragment_count != 0
                || meta.message_len != 0
                || !meta.payload_range.is_empty()
            {
                return Err(PacketError::new(PacketErrorKind::InvalidFragment));
            }
        }
        FrameType::SynAck | FrameType::HandshakeAck => {
            if meta.has_ack
                || meta.frame_sequence != 0
                || meta.stream_sequence != 0
                || meta.message_id != 0
                || meta.fragment_index != 0
                || meta.fragment_count != 0
                || meta.message_len != 0
                || meta.payload_range.len() != COOKIE_LEN
            {
                return Err(PacketError::new(PacketErrorKind::InvalidCookiePayload));
            }
        }
        FrameType::Syn | FrameType::Ack | FrameType::Rst => {
            if meta.frame_sequence != 0
                || meta.stream_sequence != 0
                || meta.message_id != 0
                || meta.fragment_index != 0
                || meta.fragment_count != 0
                || meta.message_len != 0
                || !meta.payload_range.is_empty()
            {
                return Err(PacketError::new(PacketErrorKind::InvalidFragment));
            }
        }
        FrameType::StreamOpen | FrameType::StreamOpenAck => {
            if meta.frame_sequence == 0
                || meta.stream_sequence != 0
                || meta.message_id != 0
                || meta.fragment_index != 0
                || meta.fragment_count != 0
                || meta.message_len != 0
                || meta.payload_range.len() != StreamOpenPayload::WIRE_LEN
            {
                return Err(PacketError::new(PacketErrorKind::InvalidControlPayload));
            }
        }
        FrameType::StreamFin | FrameType::StreamReset | FrameType::MaxStreamData => {
            if meta.frame_sequence == 0
                || meta.stream_sequence != 0
                || meta.message_id != 0
                || meta.fragment_index != 0
                || meta.fragment_count != 0
                || meta.message_len != 0
            {
                return Err(PacketError::new(PacketErrorKind::InvalidSequence));
            }
            if (meta.frame_type == FrameType::StreamFin && !meta.payload_range.is_empty())
                || (meta.frame_type == FrameType::MaxStreamData && meta.payload_range.len() != 8)
                || (meta.frame_type == FrameType::StreamReset
                    && meta.payload_range.len() > STREAM_RESET_MAX_PAYLOAD)
            {
                return Err(PacketError::new(PacketErrorKind::InvalidControlPayload));
            }
        }
        FrameType::MaxData | FrameType::MaxStreams => {
            if meta.frame_sequence == 0
                || meta.stream_sequence != 0
                || meta.message_id != 0
                || meta.fragment_index != 0
                || meta.fragment_count != 0
                || meta.message_len != 0
            {
                return Err(PacketError::new(PacketErrorKind::InvalidSequence));
            }
            let expected_len = match meta.frame_type {
                FrameType::MaxData => 8,
                FrameType::MaxStreams => 4,
                _ => unreachable!(),
            };
            if meta.payload_range.len() != expected_len {
                return Err(PacketError::new(PacketErrorKind::InvalidControlPayload));
            }
        }
        FrameType::Fin | FrameType::FinAck | FrameType::Ping => {
            if meta.frame_type.is_reliable() && meta.frame_sequence == 0 {
                return Err(PacketError::new(PacketErrorKind::InvalidSequence));
            }
            if meta.stream_sequence != 0
                || meta.message_id != 0
                || meta.fragment_index != 0
                || meta.fragment_count != 0
                || meta.message_len != 0
            {
                return Err(PacketError::new(PacketErrorKind::InvalidFragment));
            }
        }
    }
    Ok(())
}

fn validate_fragment_layout(
    meta: &PacketMeta,
    max_fragment_payload: Option<usize>,
) -> Result<(), PacketError> {
    let payload_len = meta.payload_range.len();
    if meta.message_len == 0 {
        if meta.fragment_count != 1 || meta.fragment_index != 0 || payload_len != 0 {
            return Err(PacketError::new(PacketErrorKind::InvalidFragment));
        }
        return Ok(());
    }
    if payload_len == 0 || u64::try_from(payload_len).unwrap_or(u64::MAX) > meta.message_len {
        return Err(PacketError::new(PacketErrorKind::InvalidFragment));
    }
    let Some(max_fragment_payload) = max_fragment_payload else {
        return Ok(());
    };
    if max_fragment_payload == 0 {
        return Err(PacketError::new(PacketErrorKind::InvalidFragment));
    }
    let message_len = usize::try_from(meta.message_len)
        .map_err(|_| PacketError::new(PacketErrorKind::MessageTooLarge))?;
    let expected_count = message_len
        .checked_add(max_fragment_payload - 1)
        .ok_or(PacketError::new(PacketErrorKind::InvalidFragment))?
        / max_fragment_payload;
    if u32::try_from(expected_count).ok() != Some(meta.fragment_count) {
        return Err(PacketError::new(PacketErrorKind::InvalidFragment));
    }
    let offset = usize::try_from(meta.fragment_index)
        .ok()
        .and_then(|index| index.checked_mul(max_fragment_payload))
        .ok_or(PacketError::new(PacketErrorKind::InvalidFragment))?;
    let expected_len = message_len
        .checked_sub(offset)
        .ok_or(PacketError::new(PacketErrorKind::InvalidFragment))?
        .min(max_fragment_payload);
    if payload_len != expected_len {
        return Err(PacketError::new(PacketErrorKind::InvalidFragment));
    }
    Ok(())
}

fn ack_from_wire(largest: u64, bitmap: u64) -> Result<Ack, PacketError> {
    let largest = FrameSequence::new(largest);
    if largest.is_none() && bitmap != 0 {
        return Err(PacketError::new(PacketErrorKind::InvalidAck));
    }
    Ok(Ack { largest, bitmap })
}

fn read_u16(data: &[u8], offset: usize) -> u16 {
    u16::from_le_bytes([data[offset], data[offset + 1]])
}

fn read_u32(data: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes([
        data[offset],
        data[offset + 1],
        data[offset + 2],
        data[offset + 3],
    ])
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

pub(crate) fn non_zero_sequence(value: u64) -> Result<FrameSequence, PacketError> {
    FrameSequence::new(value).ok_or(PacketError::new(PacketErrorKind::InvalidSequence))
}
