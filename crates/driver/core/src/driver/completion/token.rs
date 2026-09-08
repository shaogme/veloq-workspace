use crate::slot::Generation;
use veloq_std::{error::Error, fmt};

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
const CONTROL_TOKEN_KIND_SHIFT: u32 = 48;
const CONTROL_TOKEN_ID_SHIFT: u32 = 32;

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
        id: u16,
    },
    UnknownControl {
        kind: u16,
        id: u16,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CancelCompletionId(u16);

impl CancelCompletionId {
    pub const fn new(raw: u16) -> Self {
        Self(raw)
    }

    pub const fn raw(self) -> u16 {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CompletionToken(u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompletionTokenError {
    ReservedControlKind { kind: u16 },
    ControlKindOverflow { kind: u16 },
}

impl fmt::Display for CompletionTokenError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ReservedControlKind { kind } => {
                write!(f, "Control kind {} is reserved by the driver", kind)
            }
            Self::ControlKindOverflow { kind } => {
                write!(f, "Control kind {} overflows 15-bit limit", kind)
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

    pub const fn encode_control(kind: u16, id: u16) -> Result<Self, CompletionTokenError> {
        if kind > 0x7fff {
            return Err(CompletionTokenError::ControlKindOverflow { kind });
        }
        if kind == CompletionControlKind::Waker as u16
            || kind == CompletionControlKind::Cancel as u16
        {
            return Err(CompletionTokenError::ReservedControlKind { kind });
        }
        Ok(Self(
            CONTROL_TOKEN_FLAG
                | ((kind as u64) << CONTROL_TOKEN_KIND_SHIFT)
                | ((id as u64) << CONTROL_TOKEN_ID_SHIFT),
        ))
    }

    const fn internal(kind: CompletionControlKind, id: u16) -> Self {
        Self(
            CONTROL_TOKEN_FLAG
                | ((kind as u64 & 0x7fff) << CONTROL_TOKEN_KIND_SHIFT)
                | ((id as u64) << CONTROL_TOKEN_ID_SHIFT),
        )
    }

    pub const fn waker(id: u16) -> Self {
        Self::internal(CompletionControlKind::Waker, id)
    }

    pub const fn cancel(id: CancelCompletionId) -> Self {
        Self::internal(CompletionControlKind::Cancel, id.raw())
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

        let kind = ((self.0 >> CONTROL_TOKEN_KIND_SHIFT) & 0x7fff) as u16;
        let id = ((self.0 >> CONTROL_TOKEN_ID_SHIFT) & 0xffff) as u16;
        match CompletionControlKind::from_raw(kind) {
            Some(kind) => CompletionTokenClass::Control { kind, id },
            None => CompletionTokenClass::UnknownControl { kind, id },
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
            CompletionToken::cancel(CancelCompletionId::new(u16::MAX))
                .op_token()
                .is_none()
        );
    }
}
