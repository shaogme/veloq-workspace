use crate::slot::Generation;
use veloq_std::{error::Error, fmt, num::NonZeroU64};

/// `CompletionToken` 的 user 布局（bit 63 为 0）：
///
/// ```text
///  bit 63       : control flag，user token 恒为 0
///  bits 31..=62 : generation（完整 32 位，与 `Generation` 等宽）
///  bits 0..=30  : slot index（31 位）
/// ```
///
/// generation 位域必须与 [`Generation`] **等宽**：内核只回传这个 u64，解码出的
/// generation 一旦比 slot 侧窄，跨过窄宽度边界后完成就会被 `record_completion` 判成
/// Stale 而静默丢弃——slot 永久停留在 `InFlightWaiting`，其持有的 buffer 永不归还。
/// index 侧留 31 位：它的实际上界是 ring 深度（默认 1024），31 位有五个数量级的余量。
const INDEX_BITS: u32 = 31;
const INDEX_MASK: u64 = (1 << INDEX_BITS) - 1;
const INDEX_LIMIT: u64 = 1 << INDEX_BITS;
const GENERATION_SHIFT: u32 = INDEX_BITS;
const CONTROL_TOKEN_FLAG: u64 = 1 << 63;
const CONTROL_TOKEN_KIND_SHIFT: u32 = 61;
const CONTROL_TOKEN_KIND_MASK: u64 = 0b11;
const CONTROL_TOKEN_PAYLOAD_MASK: u64 = (1 << CONTROL_TOKEN_KIND_SHIFT) - 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub enum CompletionControlKind {
    Waker = 1,
    Cancel = 2,
}

impl CompletionControlKind {
    pub(super) fn from_raw(raw: u16) -> Option<Self> {
        match raw {
            1 => Some(Self::Waker),
            2 => Some(Self::Cancel),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompletionTokenClass {
    User(OpToken),
    Control {
        kind: CompletionControlKind,
        payload: u64,
    },
    UnknownControl {
        kind: u16,
        payload: u64,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SubmitTokenContext {
    pub op_token: OpToken,
    pub completion_token: CompletionToken,
}

impl SubmitTokenContext {
    pub fn user(op_token: OpToken) -> Self {
        Self {
            op_token,
            completion_token: CompletionToken::user(op_token),
        }
    }

    pub const fn new(op_token: OpToken, completion_token: CompletionToken) -> Self {
        Self {
            op_token,
            completion_token,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct OpToken {
    index: usize,
    generation: Generation,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpTokenError {
    /// slot index 超出 `CompletionToken` 的 index 位宽，编码后会侵占 generation 位。
    IndexOverflow { index: usize },
}

impl OpToken {
    /// index 的上界（不含），即 `CompletionToken` 中 index 位域能表达的槽位数量。
    pub const INDEX_LIMIT: usize = INDEX_LIMIT as usize;

    pub const fn try_new(index: usize, generation: Generation) -> Result<Self, OpTokenError> {
        if index as u64 >= INDEX_LIMIT {
            return Err(OpTokenError::IndexOverflow { index });
        }
        Ok(Self { index, generation })
    }

    pub const fn from_registry_parts(
        index: usize,
        generation: Generation,
    ) -> Result<Self, OpTokenError> {
        Self::try_new(index, generation)
    }

    pub const fn index(self) -> usize {
        self.index
    }

    pub const fn generation(self) -> Generation {
        self.generation
    }

    pub const fn parts(self) -> (usize, Generation) {
        (self.index, self.generation)
    }
}

/// Correlation ticket for an asynchronous cancellation completion.
///
/// Tickets are non-zero values in the 61-bit control-token payload range. The checked
/// constructor is intentionally the only public constructor so raw completion data cannot
/// manufacture a valid ticket with a reserved value.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct CancelTicket(NonZeroU64);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CancelTicketError {
    Zero,
    Overflow { raw: u64 },
    Exhausted,
}

impl fmt::Display for CancelTicketError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Zero => write!(f, "cancel ticket must be non-zero"),
            Self::Overflow { raw } => {
                write!(f, "cancel ticket {} exceeds the 61-bit limit", raw)
            }
            Self::Exhausted => write!(f, "cancel ticket space is exhausted"),
        }
    }
}

impl Error for CancelTicketError {}

impl CancelTicket {
    pub const MAX_RAW: u64 = CONTROL_TOKEN_PAYLOAD_MASK;

    pub const fn try_new(raw: u64) -> Result<Self, CancelTicketError> {
        if raw == 0 {
            return Err(CancelTicketError::Zero);
        }
        if raw > Self::MAX_RAW {
            return Err(CancelTicketError::Overflow { raw });
        }
        match NonZeroU64::new(raw) {
            Some(raw) => Ok(Self(raw)),
            None => Err(CancelTicketError::Zero),
        }
    }

    pub const fn raw(self) -> u64 {
        self.0.get()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CompletionToken(u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompletionTokenError {
    ReservedControlKind { kind: u16 },
    ControlKindOverflow { kind: u16 },
    ControlPayloadOverflow { payload: u64 },
}

impl fmt::Display for CompletionTokenError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ReservedControlKind { kind } => {
                write!(f, "Control kind {} is reserved by the driver", kind)
            }
            Self::ControlKindOverflow { kind } => {
                write!(f, "Control kind {} overflows 2-bit limit", kind)
            }
            Self::ControlPayloadOverflow { payload } => {
                write!(f, "Control payload {} overflows 61-bit limit", payload)
            }
        }
    }
}

impl Error for CompletionTokenError {}

impl CompletionToken {
    pub const fn user(token: OpToken) -> Self {
        let (index, generation) = token.parts();
        Self(((generation.get() as u64) << GENERATION_SHIFT) | (index as u64 & INDEX_MASK))
    }

    pub(super) const fn from_raw(raw: u64) -> Self {
        Self(raw)
    }

    pub const fn raw(self) -> u64 {
        self.0
    }

    pub const fn encode_control(kind: u16, payload: u64) -> Result<Self, CompletionTokenError> {
        if kind > CONTROL_TOKEN_KIND_MASK as u16 {
            return Err(CompletionTokenError::ControlKindOverflow { kind });
        }
        if kind == CompletionControlKind::Waker as u16
            || kind == CompletionControlKind::Cancel as u16
        {
            return Err(CompletionTokenError::ReservedControlKind { kind });
        }
        if payload > CONTROL_TOKEN_PAYLOAD_MASK {
            return Err(CompletionTokenError::ControlPayloadOverflow { payload });
        }
        Ok(Self(
            CONTROL_TOKEN_FLAG | ((kind as u64) << CONTROL_TOKEN_KIND_SHIFT) | payload,
        ))
    }

    const fn internal(kind: CompletionControlKind, payload: u64) -> Self {
        Self(
            CONTROL_TOKEN_FLAG
                | ((kind as u64 & CONTROL_TOKEN_KIND_MASK) << CONTROL_TOKEN_KIND_SHIFT)
                | (payload & CONTROL_TOKEN_PAYLOAD_MASK),
        )
    }

    pub const fn waker(id: u16) -> Self {
        Self::internal(CompletionControlKind::Waker, id as u64)
    }

    pub const fn cancel(ticket: CancelTicket) -> Self {
        Self::internal(CompletionControlKind::Cancel, ticket.raw())
    }

    pub fn classify(self) -> CompletionTokenClass {
        if (self.0 & CONTROL_TOKEN_FLAG) == 0 {
            // control flag 已确认为 0，故 `self.0 >> GENERATION_SHIFT` 至多 32 位，
            // `as u32` 不会丢位——generation 是无损往返的。
            let generation = Generation::new((self.0 >> GENERATION_SHIFT) as u32);
            if let Ok(index) = usize::try_from(self.0 & INDEX_MASK)
                && let Ok(token) = OpToken::try_new(index, generation)
            {
                return CompletionTokenClass::User(token);
            }
        }

        let kind = ((self.0 >> CONTROL_TOKEN_KIND_SHIFT) & CONTROL_TOKEN_KIND_MASK) as u16;
        let payload = self.0 & CONTROL_TOKEN_PAYLOAD_MASK;
        match CompletionControlKind::from_raw(kind) {
            Some(kind) => CompletionTokenClass::Control { kind, payload },
            None => CompletionTokenClass::UnknownControl { kind, payload },
        }
    }

    pub fn op_token(self) -> Option<OpToken> {
        match self.classify() {
            CompletionTokenClass::User(token) => Some(token),
            CompletionTokenClass::Control { .. } | CompletionTokenClass::UnknownControl { .. } => {
                None
            }
        }
    }
}

impl From<CompletionToken> for u64 {
    fn from(value: CompletionToken) -> Self {
        value.raw()
    }
}

#[cfg(test)]
#[cfg(not(feature = "loom"))]
mod tests {
    use super::*;

    /// 覆盖旧 15 位布局的边界：`0x8000` 及以上的 generation 曾在编码时被截断，
    /// 使完成回来后被判成 Stale 而静默丢弃。
    const GENERATION_CASES: [u32; 10] = [
        0,
        1,
        0x7ffe,
        0x7fff,
        0x8000,
        0xffff,
        0x1_0000,
        0x7fff_ffff,
        0x8000_0000,
        u32::MAX,
    ];

    #[test]
    fn user_token_round_trips_at_generation_boundaries() {
        for raw_generation in GENERATION_CASES {
            let generation = Generation::new(raw_generation);
            for index in [0usize, 1, 7, 1023, 1024, OpToken::INDEX_LIMIT - 1] {
                let token = OpToken::try_new(index, generation).expect("token should be encodable");
                let round_tripped = CompletionToken::user(token)
                    .op_token()
                    .expect("user token should decode back to an OpToken");

                assert_eq!(
                    round_tripped, token,
                    "index {index:#x} / generation {raw_generation:#x} did not survive the round-trip"
                );
            }
        }
    }

    #[test]
    fn user_token_never_collides_with_the_control_flag() {
        for raw_generation in GENERATION_CASES {
            let token = OpToken::try_new(OpToken::INDEX_LIMIT - 1, Generation::new(raw_generation))
                .expect("token");
            let raw = CompletionToken::user(token).raw();

            assert_eq!(
                raw & CONTROL_TOKEN_FLAG,
                0,
                "generation {raw_generation:#x} leaked into the control flag"
            );
        }
    }

    #[test]
    fn index_beyond_the_token_width_is_rejected() {
        assert_eq!(
            OpToken::try_new(OpToken::INDEX_LIMIT, Generation::ZERO),
            Err(OpTokenError::IndexOverflow {
                index: OpToken::INDEX_LIMIT
            })
        );
        assert!(OpToken::try_new(OpToken::INDEX_LIMIT - 1, Generation::new(u32::MAX)).is_ok());
    }

    #[test]
    fn control_tokens_do_not_decode_as_user_tokens() {
        assert!(CompletionToken::waker(0).op_token().is_none());
        assert!(CompletionToken::waker(u16::MAX).op_token().is_none());
        assert!(
            CompletionToken::cancel(
                CancelTicket::try_new(CancelTicket::MAX_RAW).expect("maximum ticket")
            )
            .op_token()
            .is_none()
        );
    }

    #[test]
    fn cancel_ticket_rejects_reserved_raw_values() {
        assert_eq!(CancelTicket::try_new(0), Err(CancelTicketError::Zero));
        assert_eq!(
            CancelTicket::try_new(CancelTicket::MAX_RAW + 1),
            Err(CancelTicketError::Overflow {
                raw: CancelTicket::MAX_RAW + 1
            })
        );
    }

    #[test]
    fn cancel_ticket_round_trips_at_payload_boundaries() {
        for raw in [1, 1 << 31, 1 << 60, CancelTicket::MAX_RAW] {
            let ticket = CancelTicket::try_new(raw).expect("ticket should be valid");
            let decoded = CompletionToken::cancel(ticket).classify();
            assert_eq!(
                decoded,
                CompletionTokenClass::Control {
                    kind: CompletionControlKind::Cancel,
                    payload: raw,
                }
            );
            assert_eq!(
                super::super::event::CompletionEnvelope::from_raw_parts(
                    super::super::types::CompletionBackend::Core,
                    CompletionToken::cancel(ticket).raw(),
                    0,
                    0,
                )
                .identity,
                super::super::event::CompletionIdentity::Cancel(ticket)
            );
        }
    }

    #[test]
    fn control_payload_does_not_truncate_unknown_tokens() {
        let payload = CancelTicket::MAX_RAW;
        let token = CompletionToken::encode_control(3, payload).expect("control token");
        assert_eq!(
            token.classify(),
            CompletionTokenClass::UnknownControl { kind: 3, payload }
        );
    }

    #[test]
    fn invalid_cancel_payload_is_not_routed_as_a_cancel() {
        let raw =
            CONTROL_TOKEN_FLAG | (CompletionControlKind::Cancel as u64) << CONTROL_TOKEN_KIND_SHIFT;
        let envelope = super::super::event::CompletionEnvelope::from_raw_parts(
            super::super::types::CompletionBackend::Core,
            raw,
            0,
            0,
        );
        assert_eq!(
            envelope.identity,
            super::super::event::CompletionIdentity::UnknownControl {
                kind: CompletionControlKind::Cancel as u16,
                payload: 0,
            }
        );
    }

    #[test]
    fn control_encoder_rejects_kind_and_payload_overflow() {
        assert_eq!(
            CompletionToken::encode_control(4, 0),
            Err(CompletionTokenError::ControlKindOverflow { kind: 4 })
        );
        assert_eq!(
            CompletionToken::encode_control(0, CancelTicket::MAX_RAW + 1),
            Err(CompletionTokenError::ControlPayloadOverflow {
                payload: CancelTicket::MAX_RAW + 1,
            })
        );
    }
}
