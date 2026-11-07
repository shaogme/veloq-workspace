use super::{
    CancelTicket, CompletionAnomaly, CompletionBackend, CompletionControlKind, CompletionToken,
    CompletionTokenClass, OpToken,
};

/// Unified completion event produced by platform drivers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CompletionEvent {
    token: CompletionToken,
    res: i32,
    flags: u32,
}

impl CompletionEvent {
    pub const fn token(self) -> CompletionToken {
        self.token
    }

    pub const fn res(self) -> i32 {
        self.res
    }

    pub const fn flags(self) -> u32 {
        self.flags
    }

    pub const fn raw_token(self) -> u64 {
        self.token.raw()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RawCompletion {
    backend: CompletionBackend,
    token: CompletionToken,
    res: i32,
    flags: u32,
}

impl RawCompletion {
    pub const fn new(
        backend: CompletionBackend,
        token: CompletionToken,
        res: i32,
        flags: u32,
    ) -> Self {
        Self {
            backend,
            token,
            res,
            flags,
        }
    }

    pub const fn event(self) -> CompletionEvent {
        CompletionEvent {
            token: self.token,
            res: self.res,
            flags: self.flags,
        }
    }

    pub const fn backend(self) -> CompletionBackend {
        self.backend
    }

    pub const fn token(self) -> CompletionToken {
        self.token
    }

    pub const fn res(self) -> i32 {
        self.res
    }

    pub const fn flags(self) -> u32 {
        self.flags
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UserCompletionEvent {
    token: OpToken,
    raw: RawCompletion,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UserCompletionEventMismatch {
    pub token: OpToken,
    pub expected: CompletionToken,
    pub actual: CompletionToken,
}

impl UserCompletionEvent {
    pub fn try_new(
        token: OpToken,
        raw: RawCompletion,
    ) -> Result<Self, UserCompletionEventMismatch> {
        let expected = CompletionToken::user(token);
        if raw.token != expected {
            return Err(UserCompletionEventMismatch {
                token,
                expected,
                actual: raw.token,
            });
        }
        Ok(Self { token, raw })
    }

    pub fn from_parts(backend: CompletionBackend, token: OpToken, res: i32, flags: u32) -> Self {
        Self {
            token,
            raw: RawCompletion::new(backend, CompletionToken::user(token), res, flags),
        }
    }

    pub(super) fn from_classified(token: OpToken, raw: RawCompletion) -> Self {
        debug_assert_eq!(raw.token, CompletionToken::user(token));
        Self { token, raw }
    }

    pub const fn token(self) -> OpToken {
        self.token
    }

    pub const fn raw(self) -> RawCompletion {
        self.raw
    }

    pub const fn completion_token(self) -> CompletionToken {
        self.raw.token
    }

    pub const fn res(self) -> i32 {
        self.raw.res
    }

    pub const fn flags(self) -> u32 {
        self.raw.flags
    }

    pub const fn event(self) -> CompletionEvent {
        self.raw.event()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompletionIdentity {
    User(OpToken),
    Waker(u64),
    Cancel(CancelTicket),
    UnknownControl { kind: u16, payload: u64 },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompletionIdentitySource {
    UserToken,
    ControlToken,
    SidecarTokenWithQueueKey { queue_key: u64 },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CompletionEnvelope {
    raw: RawCompletion,
    identity: CompletionIdentity,
    source: CompletionIdentitySource,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompletionDispatch {
    User {
        event: UserCompletionEvent,
    },
    Waker {
        id: u64,
        raw: RawCompletion,
    },
    Cancel {
        ticket: CancelTicket,
        raw: RawCompletion,
    },
    Unknown {
        envelope: CompletionEnvelope,
    },
}

impl CompletionEnvelope {
    pub fn from_raw_parts(
        backend: CompletionBackend,
        raw_token: u64,
        res: i32,
        flags: u32,
    ) -> Self {
        Self::from_raw(RawCompletion::new(
            backend,
            CompletionToken::from_raw(raw_token),
            res,
            flags,
        ))
    }

    pub fn from_raw(raw: RawCompletion) -> Self {
        let (identity, source) = match raw.token.classify() {
            CompletionTokenClass::User(token) => (
                CompletionIdentity::User(token),
                CompletionIdentitySource::UserToken,
            ),
            CompletionTokenClass::Control {
                kind: CompletionControlKind::Waker,
                payload,
            } => (
                CompletionIdentity::Waker(payload),
                CompletionIdentitySource::ControlToken,
            ),
            CompletionTokenClass::Control {
                kind: CompletionControlKind::Cancel,
                payload,
            } => match CancelTicket::try_new(payload) {
                Ok(ticket) => (
                    CompletionIdentity::Cancel(ticket),
                    CompletionIdentitySource::ControlToken,
                ),
                Err(_) => (
                    CompletionIdentity::UnknownControl {
                        kind: CompletionControlKind::Cancel as u16,
                        payload,
                    },
                    CompletionIdentitySource::ControlToken,
                ),
            },
            CompletionTokenClass::UnknownControl { kind, payload } => (
                CompletionIdentity::UnknownControl { kind, payload },
                CompletionIdentitySource::ControlToken,
            ),
        };
        Self {
            raw,
            identity,
            source,
        }
    }

    pub fn from_sidecar_user_token(
        backend: CompletionBackend,
        token: OpToken,
        queue_key: u64,
        res: i32,
        flags: u32,
    ) -> Self {
        Self {
            raw: RawCompletion::new(backend, CompletionToken::user(token), res, flags),
            identity: CompletionIdentity::User(token),
            source: CompletionIdentitySource::SidecarTokenWithQueueKey { queue_key },
        }
    }

    pub const fn raw(self) -> RawCompletion {
        self.raw
    }

    pub const fn identity(self) -> CompletionIdentity {
        self.identity
    }

    pub const fn source(self) -> CompletionIdentitySource {
        self.source
    }

    pub(super) fn dispatch(self) -> CompletionDispatch {
        let raw = self.raw;
        match self.identity {
            CompletionIdentity::User(token) => CompletionDispatch::User {
                event: UserCompletionEvent::from_classified(token, raw),
            },
            CompletionIdentity::Waker(id) => CompletionDispatch::Waker { id, raw },
            CompletionIdentity::Cancel(ticket) => CompletionDispatch::Cancel { ticket, raw },
            CompletionIdentity::UnknownControl { .. } => {
                CompletionDispatch::Unknown { envelope: self }
            }
        }
    }
}

impl CompletionAnomaly {
    pub fn with_raw_completion(self, raw: RawCompletion) -> Self {
        use super::CompletionRaw;
        self.with_token(raw.token()).with_raw(CompletionRaw {
            backend: raw.backend(),
            res: raw.res(),
            flags: raw.flags(),
        })
    }
}
