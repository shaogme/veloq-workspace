use super::{
    CancelCompletionId, CompletionAnomaly, CompletionBackend, CompletionControlKind,
    CompletionToken, CompletionTokenClass, OpToken,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RawCompletion {
    pub backend: CompletionBackend,
    pub token: CompletionToken,
    pub res: i32,
    pub flags: u32,
}

impl RawCompletion {
    #[inline]
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

    #[inline]
    pub const fn event(self) -> CompletionEvent {
        CompletionEvent {
            token: self.token,
            res: self.res,
            flags: self.flags,
        }
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
    #[inline]
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

    #[inline]
    pub fn from_parts(backend: CompletionBackend, token: OpToken, res: i32, flags: u32) -> Self {
        Self {
            token,
            raw: RawCompletion::new(backend, CompletionToken::user(token), res, flags),
        }
    }

    #[inline]
    pub(super) fn from_classified(token: OpToken, raw: RawCompletion) -> Self {
        debug_assert_eq!(raw.token, CompletionToken::user(token));
        Self { token, raw }
    }

    #[inline]
    pub const fn token(self) -> OpToken {
        self.token
    }

    #[inline]
    pub const fn raw(self) -> RawCompletion {
        self.raw
    }

    #[inline]
    pub const fn completion_token(self) -> CompletionToken {
        self.raw.token
    }

    #[inline]
    pub const fn res(self) -> i32 {
        self.raw.res
    }

    #[inline]
    pub const fn flags(self) -> u32 {
        self.raw.flags
    }

    #[inline]
    pub const fn event(self) -> CompletionEvent {
        self.raw.event()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompletionDispatch {
    User {
        event: UserCompletionEvent,
    },
    Waker {
        id: u16,
        raw: RawCompletion,
    },
    Cancel {
        id: CancelCompletionId,
        raw: RawCompletion,
    },
    RioWake {
        id: u16,
        raw: RawCompletion,
    },
    Unknown {
        envelope: CompletionEnvelope,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompletionIdentity {
    User(OpToken),
    Waker(u16),
    Cancel(CancelCompletionId),
    RioWake(u16),
    UnknownControl {
        kind: u16,
        id: u16,
    },
    BackendContext {
        backend: CompletionBackend,
        raw_context: u64,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompletionIdentitySource {
    UserToken,
    ControlToken,
    SidecarTokenWithQueueKey { queue_key: u64 },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CompletionEnvelope {
    pub raw: RawCompletion,
    pub identity: CompletionIdentity,
    pub source: CompletionIdentitySource,
}

impl CompletionEnvelope {
    #[inline]
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

    #[inline]
    pub fn from_raw(raw: RawCompletion) -> Self {
        let (identity, source) = match raw.token.classify() {
            CompletionTokenClass::User(token) => (
                CompletionIdentity::User(token),
                CompletionIdentitySource::UserToken,
            ),
            CompletionTokenClass::Control {
                kind: CompletionControlKind::Waker,
                id,
            } => (
                CompletionIdentity::Waker(id),
                CompletionIdentitySource::ControlToken,
            ),
            CompletionTokenClass::Control {
                kind: CompletionControlKind::Cancel,
                id,
            } => (
                CompletionIdentity::Cancel(CancelCompletionId::new(id)),
                CompletionIdentitySource::ControlToken,
            ),
            CompletionTokenClass::Control {
                kind: CompletionControlKind::RioWake,
                id,
            } => (
                CompletionIdentity::RioWake(id),
                CompletionIdentitySource::ControlToken,
            ),
            CompletionTokenClass::UnknownControl { kind, id } => (
                CompletionIdentity::UnknownControl { kind, id },
                CompletionIdentitySource::ControlToken,
            ),
        };
        Self {
            raw,
            identity,
            source,
        }
    }

    #[inline]
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
}

/// Unified completion event produced by platform drivers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CompletionEvent {
    /// Completion token (generation + slot index, or backend control token).
    pub token: CompletionToken,
    /// Completion result code. Non-negative for success, negative for error.
    pub res: i32,
    /// Platform-specific completion flags.
    pub flags: u32,
}

impl CompletionEvent {
    #[inline]
    pub const fn raw_token(self) -> u64 {
        self.token.raw()
    }
}

#[inline]
pub(super) fn dispatch_envelope(envelope: CompletionEnvelope) -> CompletionDispatch {
    let raw = envelope.raw;
    match envelope.identity {
        CompletionIdentity::User(token) => CompletionDispatch::User {
            event: UserCompletionEvent::from_classified(token, raw),
        },
        CompletionIdentity::Waker(id) => CompletionDispatch::Waker { id, raw },
        CompletionIdentity::Cancel(id) => CompletionDispatch::Cancel { id, raw },
        CompletionIdentity::RioWake(id) => CompletionDispatch::RioWake { id, raw },
        CompletionIdentity::UnknownControl { .. } | CompletionIdentity::BackendContext { .. } => {
            CompletionDispatch::Unknown { envelope }
        }
    }
}

impl CompletionAnomaly {
    #[inline]
    pub fn with_raw_completion(self, raw: RawCompletion) -> Self {
        self.with_backend(raw.backend).with_event(raw.event())
    }
}

#[inline]
pub(super) fn unknown_completion_anomaly(envelope: CompletionEnvelope) -> CompletionAnomaly {
    match envelope.identity {
        CompletionIdentity::BackendContext {
            backend,
            raw_context,
        } => CompletionAnomaly::backend_context_unknown(envelope.raw.token)
            .with_raw_completion(envelope.raw)
            .with_backend(backend)
            .with_backend_context(raw_context),
        CompletionIdentity::UnknownControl { .. }
        | CompletionIdentity::User(_)
        | CompletionIdentity::Waker(_)
        | CompletionIdentity::Cancel(_)
        | CompletionIdentity::RioWake(_) => {
            CompletionAnomaly::unknown_control(envelope.raw.token).with_raw_completion(envelope.raw)
        }
    }
}
