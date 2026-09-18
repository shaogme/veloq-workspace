use veloq::{
    buf::FixedBuf,
    runtime::context::Ctx,
    std::{
        error::Error as StdError,
        fmt,
        num::{NonZeroU64, NonZeroUsize},
        ops::{BitOr, BitOrAssign, Range},
    },
};

pub const MAGIC: [u8; 2] = *b"VQ";
pub const VERSION: u8 = 3;
pub const HEADER_LEN: usize = 66;
pub const COOKIE_LEN: usize = 32;

const HALF_SEQUENCE_SPACE: u64 = 1 << 63;
const FRAME_SEQUENCE_OFFSET: usize = 14;
const ACK_LARGEST_OFFSET: usize = 22;
const ACK_BITMAP_OFFSET: usize = 30;
const RECEIVE_WINDOW_OFFSET: usize = 38;
const PAYLOAD_LEN_OFFSET: usize = 40;
const MESSAGE_ID_OFFSET: usize = 42;
const FRAGMENT_INDEX_OFFSET: usize = 50;
const FRAGMENT_COUNT_OFFSET: usize = 54;
const MESSAGE_LEN_OFFSET: usize = 58;

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
    InvalidCookiePayload,
    CookiePayloadTooLarge,
    PayloadTooLarge,
    DatagramTooLarge,
    InvalidFragment,
    FragmentCountExceeded,
    MessageTooLarge,
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
            PacketErrorKind::InvalidHeaderLength => "packet header length is invalid",
            PacketErrorKind::InvalidFlags => "packet flags contain an invalid combination",
            PacketErrorKind::InvalidConnectionId => "connection ID must be non-zero",
            PacketErrorKind::InvalidSequence => "data frame sequence must be non-zero",
            PacketErrorKind::InvalidAck => "acknowledgement fields are invalid",
            PacketErrorKind::InvalidPayloadLength => "payload length does not match datagram size",
            PacketErrorKind::InvalidCookiePayload => "handshake cookie payload is invalid",
            PacketErrorKind::CookiePayloadTooLarge => "handshake cookie payload is too large",
            PacketErrorKind::PayloadTooLarge => "payload cannot be represented by the wire format",
            PacketErrorKind::DatagramTooLarge => "encoded datagram exceeds the configured limit",
            PacketErrorKind::InvalidFragment => "fragment metadata or payload length is invalid",
            PacketErrorKind::FragmentCountExceeded => "fragment count exceeds the configured limit",
            PacketErrorKind::MessageTooLarge => "message cannot be represented by the wire format",
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub struct FrameSequence(NonZeroU64);

impl FrameSequence {
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
                None => Self::new(1).expect("one is a valid frame sequence"),
            },
            None => Self::new(1).expect("one is a valid frame sequence"),
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

impl fmt::Display for FrameSequence {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.get())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub struct MessageId(NonZeroU64);

impl MessageId {
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
                Some(id) => id,
                None => Self::new(1).expect("one is a valid message ID"),
            },
            None => Self::new(1).expect("one is a valid message ID"),
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
}

impl fmt::Display for MessageId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.get())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
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
    pub const MESSAGE_ACK: Self = Self(1 << 7);

    pub const fn bits(self) -> u8 {
        self.0
    }

    pub const fn from_bits(bits: u8) -> Option<Self> {
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
            || self.0 == Self::MESSAGE_ACK.0
            || self.0 == (Self::MESSAGE_ACK.0 | Self::ACK.0)
            || self.0 == Self::FIN.0
            || self.0 == Self::FIN_ACK.0
            || self.0 == (Self::FIN.0 | Self::ACK.0)
            || self.0 == Self::RST.0
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
    pub flags: Flags,
    pub connection_id: ConnectionId,
    pub frame_sequence: u64,
    pub ack_largest: u64,
    pub ack_bitmap: u64,
    pub receive_window: u16,
    pub message_id: u64,
    pub fragment_index: u32,
    pub fragment_count: u32,
    pub message_len: u64,
    pub payload_range: Range<usize>,
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
    pub flags: Flags,
    pub connection_id: ConnectionId,
    pub frame_sequence: u64,
    pub ack_largest: u64,
    pub ack_bitmap: u64,
    pub receive_window: u16,
    pub message_id: u64,
    pub fragment_index: u32,
    pub fragment_count: u32,
    pub message_len: u64,
    pub payload: &'a [u8],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DataPacket<'a> {
    pub flags: Flags,
    pub connection_id: ConnectionId,
    pub frame_sequence: FrameSequence,
    pub ack: Ack,
    pub receive_window: u16,
    pub message_id: MessageId,
    pub fragment_index: u32,
    pub fragment_count: u32,
    pub message_len: u64,
    pub payload: &'a [u8],
    pub max_fragment_payload: usize,
}

struct PacketFields<'a> {
    flags: Flags,
    connection_id: ConnectionId,
    frame_sequence: u64,
    ack: Ack,
    receive_window: u16,
    message_id: u64,
    fragment_index: u32,
    fragment_count: u32,
    message_len: u64,
    payload: &'a [u8],
    max_fragment_payload: Option<usize>,
}

impl Packet {
    pub fn encode_control_into_with_limit<A: PacketBufAllocator + ?Sized>(
        allocator: &A,
        max_datagram_size: NonZeroUsize,
        flags: Flags,
        connection_id: ConnectionId,
        ack: Ack,
        receive_window: u16,
    ) -> Result<Self, PacketError> {
        Self::encode_fields(
            allocator,
            max_datagram_size,
            PacketFields {
                flags,
                connection_id,
                frame_sequence: 0,
                ack,
                receive_window,
                message_id: 0,
                fragment_index: 0,
                fragment_count: 0,
                message_len: 0,
                payload: &[],
                max_fragment_payload: None,
            },
        )
    }

    pub fn encode_handshake_cookie_into_with_limit<A: PacketBufAllocator + ?Sized>(
        allocator: &A,
        max_datagram_size: NonZeroUsize,
        flags: Flags,
        connection_id: ConnectionId,
        receive_window: u16,
        cookie: &[u8; COOKIE_LEN],
    ) -> Result<Self, PacketError> {
        Self::encode_fields(
            allocator,
            max_datagram_size,
            PacketFields {
                flags,
                connection_id,
                frame_sequence: 0,
                ack: Ack::empty(),
                receive_window,
                message_id: 0,
                fragment_index: 0,
                fragment_count: 0,
                message_len: 0,
                payload: cookie,
                max_fragment_payload: None,
            },
        )
    }

    pub fn encode_data_into_with_limit<A: PacketBufAllocator + ?Sized>(
        allocator: &A,
        max_datagram_size: NonZeroUsize,
        data: DataPacket<'_>,
    ) -> Result<Self, PacketError> {
        Self::encode_fields(
            allocator,
            max_datagram_size,
            PacketFields {
                flags: data.flags,
                connection_id: data.connection_id,
                frame_sequence: data.frame_sequence.get(),
                ack: data.ack,
                receive_window: data.receive_window,
                message_id: data.message_id.get(),
                fragment_index: data.fragment_index,
                fragment_count: data.fragment_count,
                message_len: data.message_len,
                payload: data.payload,
                max_fragment_payload: Some(data.max_fragment_payload),
            },
        )
    }

    pub fn encode_message_ack_into_with_limit<A: PacketBufAllocator + ?Sized>(
        allocator: &A,
        max_datagram_size: NonZeroUsize,
        connection_id: ConnectionId,
        ack: Ack,
        receive_window: u16,
        message_id: MessageId,
    ) -> Result<Self, PacketError> {
        let flags = if ack.largest().is_some() {
            Flags::MESSAGE_ACK | Flags::ACK
        } else {
            Flags::MESSAGE_ACK
        };
        Self::encode_fields(
            allocator,
            max_datagram_size,
            PacketFields {
                flags,
                connection_id,
                frame_sequence: 0,
                ack,
                receive_window,
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
            flags: fields.flags,
            connection_id: fields.connection_id,
            frame_sequence: fields.frame_sequence,
            ack_largest: fields.ack.largest.map_or(0, FrameSequence::get),
            ack_bitmap: fields.ack.bitmap,
            receive_window: fields.receive_window,
            message_id: fields.message_id,
            fragment_index: fields.fragment_index,
            fragment_count: fields.fragment_count,
            message_len: fields.message_len,
            payload_range: HEADER_LEN..total_len,
        };
        validate_meta(&meta, fields.max_fragment_payload)?;

        let mut datagram = allocator.alloc_packet_buf(max_datagram_size, total_len)?;
        if datagram.capacity() < total_len {
            return Err(PacketError::new(PacketErrorKind::DatagramTooLarge));
        }
        let bytes = datagram.spare_capacity_mut();
        bytes[..2].copy_from_slice(&MAGIC);
        bytes[2] = VERSION;
        bytes[3] = fields.flags.bits();
        bytes[4..6].copy_from_slice(&(HEADER_LEN as u16).to_le_bytes());
        bytes[6..14].copy_from_slice(&fields.connection_id.get().to_le_bytes());
        bytes[FRAME_SEQUENCE_OFFSET..22].copy_from_slice(&fields.frame_sequence.to_le_bytes());
        bytes[ACK_LARGEST_OFFSET..30].copy_from_slice(&meta.ack_largest.to_le_bytes());
        bytes[ACK_BITMAP_OFFSET..38].copy_from_slice(&meta.ack_bitmap.to_le_bytes());
        bytes[RECEIVE_WINDOW_OFFSET..40].copy_from_slice(&fields.receive_window.to_le_bytes());
        bytes[PAYLOAD_LEN_OFFSET..42].copy_from_slice(&payload_len.to_le_bytes());
        bytes[MESSAGE_ID_OFFSET..50].copy_from_slice(&fields.message_id.to_le_bytes());
        bytes[FRAGMENT_INDEX_OFFSET..54].copy_from_slice(&fields.fragment_index.to_le_bytes());
        bytes[FRAGMENT_COUNT_OFFSET..58].copy_from_slice(&fields.fragment_count.to_le_bytes());
        bytes[MESSAGE_LEN_OFFSET..66].copy_from_slice(&fields.message_len.to_le_bytes());
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
            flags: self.meta.flags,
            connection_id: self.meta.connection_id,
            frame_sequence: self.meta.frame_sequence,
            ack_largest: self.meta.ack_largest,
            ack_bitmap: self.meta.ack_bitmap,
            receive_window: self.meta.receive_window,
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
        let header_len = usize::from(read_u16(datagram, 4));
        if header_len != HEADER_LEN {
            return Err(PacketError::new(PacketErrorKind::InvalidHeaderLength));
        }
        let flags = Flags::from_bits(datagram[3])
            .filter(|flags| flags.is_valid())
            .ok_or(PacketError::new(PacketErrorKind::InvalidFlags))?;
        let connection_id = ConnectionId::new(read_u64(datagram, 6))
            .ok_or(PacketError::new(PacketErrorKind::InvalidConnectionId))?;
        let frame_sequence = read_u64(datagram, FRAME_SEQUENCE_OFFSET);
        let ack_largest = read_u64(datagram, ACK_LARGEST_OFFSET);
        let ack_bitmap = read_u64(datagram, ACK_BITMAP_OFFSET);
        ack_from_wire(ack_largest, ack_bitmap)?;
        if ack_largest != 0 && !flags.contains(Flags::ACK) {
            return Err(PacketError::new(PacketErrorKind::InvalidAck));
        }
        let payload_len = usize::from(read_u16(datagram, PAYLOAD_LEN_OFFSET));
        let expected_len = header_len
            .checked_add(payload_len)
            .ok_or(PacketError::new(PacketErrorKind::InvalidPayloadLength))?;
        if expected_len != datagram.len() {
            return Err(PacketError::new(PacketErrorKind::InvalidPayloadLength));
        }
        let meta = PacketMeta {
            flags,
            connection_id,
            frame_sequence,
            ack_largest,
            ack_bitmap,
            receive_window: read_u16(datagram, RECEIVE_WINDOW_OFFSET),
            message_id: read_u64(datagram, MESSAGE_ID_OFFSET),
            fragment_index: read_u32(datagram, FRAGMENT_INDEX_OFFSET),
            fragment_count: read_u32(datagram, FRAGMENT_COUNT_OFFSET),
            message_len: read_u64(datagram, MESSAGE_LEN_OFFSET),
            payload_range: header_len..expected_len,
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
            flags,
            connection_id,
            frame_sequence,
            ack_largest,
            ack_bitmap,
            receive_window: meta.receive_window,
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
        if !(self.flags == Flags::SYN_ACK || self.flags == Flags::ACK)
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
            flags: self.flags,
            connection_id: self.connection_id,
            frame_sequence: self.frame_sequence,
            ack_largest: self.ack_largest,
            ack_bitmap: self.ack_bitmap,
            receive_window: self.receive_window,
            message_id: self.message_id,
            fragment_index: self.fragment_index,
            fragment_count: self.fragment_count,
            message_len: self.message_len,
            payload_range: HEADER_LEN..HEADER_LEN + self.payload.len(),
        }
    }
}

fn validate_meta(
    meta: &PacketMeta,
    max_fragment_payload: Option<usize>,
) -> Result<(), PacketError> {
    if !meta.flags.is_valid() {
        return Err(PacketError::new(PacketErrorKind::InvalidFlags));
    }
    if meta.connection_id.get() == 0 {
        return Err(PacketError::new(PacketErrorKind::InvalidConnectionId));
    }
    ack_from_wire(meta.ack_largest, meta.ack_bitmap)?;
    if meta.ack_largest != 0 && !meta.flags.contains(Flags::ACK) {
        return Err(PacketError::new(PacketErrorKind::InvalidAck));
    }
    if meta.payload_range.len() > usize::from(u16::MAX) {
        return Err(PacketError::new(PacketErrorKind::PayloadTooLarge));
    }

    if meta.flags.contains(Flags::DATA) {
        if meta.frame_sequence == 0 || meta.message_id == 0 {
            return Err(PacketError::new(PacketErrorKind::InvalidSequence));
        }
        if meta.fragment_count == 0 || meta.fragment_index >= meta.fragment_count {
            return Err(PacketError::new(PacketErrorKind::InvalidFragment));
        }
        validate_fragment_layout(meta, max_fragment_payload)?;
    } else if meta.flags.contains(Flags::MESSAGE_ACK) {
        if meta.frame_sequence != 0
            || meta.message_id == 0
            || meta.fragment_index != 0
            || meta.fragment_count != 0
            || meta.message_len != 0
            || !meta.payload_range.is_empty()
        {
            return Err(PacketError::new(PacketErrorKind::InvalidFragment));
        }
    } else if meta.flags == Flags::SYN_ACK {
        if meta.frame_sequence != 0
            || meta.message_id != 0
            || meta.fragment_index != 0
            || meta.fragment_count != 0
            || meta.message_len != 0
            || meta.payload_range.len() != COOKIE_LEN
        {
            return Err(PacketError::new(PacketErrorKind::InvalidCookiePayload));
        }
    } else if meta.flags == Flags::ACK && !meta.payload_range.is_empty() {
        if meta.payload_range.len() != COOKIE_LEN {
            return Err(PacketError::new(PacketErrorKind::CookiePayloadTooLarge));
        }
        if meta.frame_sequence != 0
            || meta.message_id != 0
            || meta.fragment_index != 0
            || meta.fragment_count != 0
            || meta.message_len != 0
        {
            return Err(PacketError::new(PacketErrorKind::InvalidCookiePayload));
        }
    } else if meta.frame_sequence != 0
        || meta.message_id != 0
        || meta.fragment_index != 0
        || meta.fragment_count != 0
        || meta.message_len != 0
        || !meta.payload_range.is_empty()
    {
        return Err(PacketError::new(PacketErrorKind::InvalidFragment));
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
