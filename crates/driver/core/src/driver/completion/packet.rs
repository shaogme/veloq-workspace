use crate::{DriverResult, slot::SlotSpec};

use super::{
    AnomalyAttach, CompletionAnomalyKind, CompletionCleanupGuard, CompletionEvent,
    DriverCompletionDiagnostics, OpToken, UserCompletionEvent,
};

pub struct CompletionPacket<Spec: SlotSpec> {
    pub event: UserCompletionEvent,
    pub input: CompletionInput<Spec>,
}

pub struct UserCompletion<Spec: SlotSpec> {
    pub payload: Spec::UserPayload,
    pub detail: Option<DriverResult<Spec::Completion, Spec::Error>>,
    pub cleanup: CompletionCleanupGuard,
}

pub enum CompletionInput<Spec: SlotSpec> {
    User(UserCompletion<Spec>),
}

impl<Spec: SlotSpec> CompletionInput<Spec> {
    pub fn cleanup_mut(&mut self) -> &mut CompletionCleanupGuard {
        match self {
            Self::User(completion) => &mut completion.cleanup,
        }
    }

    pub fn lost_kind(&self) -> Option<(CompletionAnomalyKind, AnomalyAttach)> {
        None
    }
}

impl<Spec: SlotSpec> CompletionPacket<Spec> {
    pub fn user_event(
        event: UserCompletionEvent,
        payload: Spec::UserPayload,
        detail: Option<DriverResult<Spec::Completion, Spec::Error>>,
        cleanup: CompletionCleanupGuard,
    ) -> Self {
        Self {
            event,
            input: CompletionInput::User(UserCompletion {
                payload,
                detail,
                cleanup,
            }),
        }
    }

    pub fn user(
        event: UserCompletionEvent,
        payload: Spec::UserPayload,
        detail: Option<DriverResult<Spec::Completion, Spec::Error>>,
    ) -> Self {
        Self::user_event(event, payload, detail, CompletionCleanupGuard::default())
    }

    pub fn user_with_cleanup(
        event: UserCompletionEvent,
        payload: Spec::UserPayload,
        detail: Option<DriverResult<Spec::Completion, Spec::Error>>,
        cleanup: CompletionCleanupGuard,
    ) -> Self {
        Self::user_event(event, payload, detail, cleanup)
    }

    pub const fn token(&self) -> OpToken {
        self.event.token()
    }

    pub const fn completion_event(&self) -> CompletionEvent {
        self.event.event()
    }
}

pub struct CompletionRecord<Spec: SlotSpec> {
    pub event: UserCompletionEvent,
    pub payload: Spec::UserPayload,
    pub detail: Option<DriverResult<Spec::Completion, Spec::Error>>,
    pub cleanup: CompletionCleanupGuard,
}

impl<Spec: SlotSpec> CompletionRecord<Spec> {
    pub fn disarm_cleanup(&mut self) -> bool {
        self.cleanup.disarm()
    }
}

#[inline]
pub(super) fn run_rejected_cleanup<Spec: SlotSpec>(
    diagnostics: &DriverCompletionDiagnostics<Spec::CompletionDiagnostics>,
    mut packet: CompletionPacket<Spec>,
) {
    run_completion_cleanup(diagnostics, packet.input.cleanup_mut());
    drop(packet);
}

#[inline]
pub(super) fn run_completion_cleanup<B>(
    diagnostics: &DriverCompletionDiagnostics<B>,
    cleanup: &mut CompletionCleanupGuard,
) -> bool {
    match cleanup.run() {
        Ok(ran) => ran,
        Err(_) => {
            diagnostics.inc_orphan_cleanup_error();
            false
        }
    }
}
