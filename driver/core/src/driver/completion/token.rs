const INDEX_LIMIT: u64 = 1 << 48;
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
    #[inline]
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
    #[inline]
    pub fn user(op_token: OpToken) -> Self {
        Self {
            op_token,
            completion_token: CompletionToken::user(op_token),
        }
    }

    #[inline]
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
    generation: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpTokenError {
    ReservedControlIndex { index: usize },
}

impl OpToken {
    #[inline]
    pub const fn try_new(index: usize, generation: u32) -> Result<Self, OpTokenError> {
        if index as u64 >= INDEX_LIMIT {
            return Err(OpTokenError::ReservedControlIndex { index });
        }
        Ok(Self { index, generation })
    }

    #[inline]
    pub const fn from_registry_parts(index: usize, generation: u32) -> Result<Self, OpTokenError> {
        Self::try_new(index, generation)
    }

    #[inline]
    pub const fn index(self) -> usize {
        self.index
    }

    #[inline]
    pub const fn generation(self) -> u32 {
        self.generation
    }

    #[inline]
    pub const fn parts(self) -> (usize, u32) {
        (self.index, self.generation)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CancelCompletionId(u16);

impl CancelCompletionId {
    #[inline]
    pub const fn new(raw: u16) -> Self {
        Self(raw)
    }

    #[inline]
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

impl std::fmt::Display for CompletionTokenError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
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

impl std::error::Error for CompletionTokenError {}

impl CompletionToken {
    #[inline]
    pub const fn user(token: OpToken) -> Self {
        let (index, generation) = token.parts();
        Self(((generation as u64 & 0x7fff) << 48) | (index as u64 & 0x0000_ffff_ffff_ffff))
    }

    #[inline]
    pub(super) const fn from_raw(raw: u64) -> Self {
        Self(raw)
    }

    #[inline]
    pub const fn raw(self) -> u64 {
        self.0
    }

    #[inline]
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

    #[inline]
    const fn internal(kind: CompletionControlKind, id: u16) -> Self {
        Self(
            CONTROL_TOKEN_FLAG
                | ((kind as u64 & 0x7fff) << CONTROL_TOKEN_KIND_SHIFT)
                | ((id as u64) << CONTROL_TOKEN_ID_SHIFT),
        )
    }

    #[inline]
    pub const fn waker(id: u16) -> Self {
        Self::internal(CompletionControlKind::Waker, id)
    }

    #[inline]
    pub const fn cancel(id: CancelCompletionId) -> Self {
        Self::internal(CompletionControlKind::Cancel, id.raw())
    }

    #[inline]
    pub fn classify(self) -> CompletionTokenClass {
        if (self.0 & CONTROL_TOKEN_FLAG) == 0 {
            let raw_index = self.0 & 0x0000_ffff_ffff_ffff;
            if raw_index <= usize::MAX as u64 {
                let index = raw_index as usize;
                let generation = ((self.0 >> 48) & 0x7fff) as u32;
                if let Ok(token) = OpToken::try_new(index, generation) {
                    return CompletionTokenClass::User(token);
                }
            }
        }

        let kind = ((self.0 >> CONTROL_TOKEN_KIND_SHIFT) & 0x7fff) as u16;
        let id = ((self.0 >> CONTROL_TOKEN_ID_SHIFT) & 0xffff) as u16;
        match CompletionControlKind::from_raw(kind) {
            Some(kind) => CompletionTokenClass::Control { kind, id },
            None => CompletionTokenClass::UnknownControl { kind, id },
        }
    }

    #[inline]
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
    #[inline]
    fn from(value: CompletionToken) -> Self {
        value.raw()
    }
}
