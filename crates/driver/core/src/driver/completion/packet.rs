use crate::{DriverResult, slot::SlotSpec};

use super::{
    AnomalyAttach, CompletionAnomalyKind, CompletionCleanupGuard, CompletionEvent,
    DriverCompletionDiagnostics, OpToken, UserCompletionEvent,
};

/// 一条完成之后，该操作是否还会再投递完成。
///
/// 后端把自己的表示（io_uring 的 `IORING_CQE_F_MORE`、IOCP 的「没有这回事」）翻译成
/// 这个枚举交给 core——core 不解读后端 flags。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CompletionContinuation {
    /// 该操作到此终止：slot 可以归还，token 随这条完成被消费而失效。
    #[default]
    Final,
    /// 该操作仍在内核里，还会继续投递完成。slot 与 token 都必须保持有效。
    More,
}

impl CompletionContinuation {
    #[inline]
    pub const fn is_final(self) -> bool {
        matches!(self, Self::Final)
    }

    #[inline]
    pub const fn is_more(self) -> bool {
        matches!(self, Self::More)
    }
}

pub struct UserCompletion<Spec: SlotSpec> {
    pub payload: Spec::UserPayload,
    pub detail: Option<DriverResult<Spec::Completion, Spec::Error>>,
    pub cleanup: CompletionCleanupGuard,
}

/// 一条不携带可拥有 record payload 的终态完成。
///
/// multishot 的 replacement 可能在当前 socket/buffer 已经成功交付后失败。此时终态
/// 错误必须排在成功 record 后面，但不能伪造一个 socket 或 buffer 作为错误 record 的
/// payload；否则消费方可能把同一个资源误认为第二次所有权转移。
pub struct TerminalCompletion<Spec: SlotSpec> {
    pub detail: DriverResult<Spec::Completion, Spec::Error>,
    pub cleanup: CompletionCleanupGuard,
}

pub enum CompletionInput<Spec: SlotSpec> {
    User(UserCompletion<Spec>),
    Terminal(TerminalCompletion<Spec>),
}

pub struct CompletionPacket<Spec: SlotSpec> {
    event: UserCompletionEvent,
    input: CompletionInput<Spec>,
    continuation: CompletionContinuation,
}

pub struct CompletionPacketParts<Spec: SlotSpec> {
    pub event: UserCompletionEvent,
    pub input: CompletionInput<Spec>,
    pub continuation: CompletionContinuation,
}

pub struct CompletionRecordParts<Spec: SlotSpec> {
    pub event: UserCompletionEvent,
    pub payload: Option<Spec::UserPayload>,
    pub detail: Option<DriverResult<Spec::Completion, Spec::Error>>,
    pub cleanup: CompletionCleanupGuard,
    pub continuation: CompletionContinuation,
}

impl<Spec: SlotSpec> CompletionInput<Spec> {
    pub fn cleanup_mut(&mut self) -> &mut CompletionCleanupGuard {
        match self {
            Self::User(completion) => &mut completion.cleanup,
            Self::Terminal(completion) => &mut completion.cleanup,
        }
    }

    pub fn lost_kind(&self) -> Option<(CompletionAnomalyKind, AnomalyAttach)> {
        None
    }
}

impl<Spec: SlotSpec> CompletionPacket<Spec> {
    pub fn new(
        event: UserCompletionEvent,
        input: CompletionInput<Spec>,
        continuation: CompletionContinuation,
    ) -> Self {
        Self {
            event,
            input,
            continuation,
        }
    }

    pub const fn event(&self) -> UserCompletionEvent {
        self.event
    }

    pub const fn input(&self) -> &CompletionInput<Spec> {
        &self.input
    }

    pub fn input_mut(&mut self) -> &mut CompletionInput<Spec> {
        &mut self.input
    }

    pub const fn continuation(&self) -> CompletionContinuation {
        self.continuation
    }

    pub fn into_parts(self) -> CompletionPacketParts<Spec> {
        CompletionPacketParts {
            event: self.event,
            input: self.input,
            continuation: self.continuation,
        }
    }

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
            continuation: CompletionContinuation::Final,
        }
    }

    /// 标记这条完成之后还会有更多（multishot）。默认是 [`CompletionContinuation::Final`]，
    /// 所以所有单发路径的构造点都不必改。
    pub fn with_continuation(mut self, continuation: CompletionContinuation) -> Self {
        self.continuation = continuation;
        self
    }

    pub fn user(
        event: UserCompletionEvent,
        payload: Spec::UserPayload,
        detail: Option<DriverResult<Spec::Completion, Spec::Error>>,
    ) -> Self {
        Self::user_event(event, payload, detail, CompletionCleanupGuard::default())
    }

    /// Build a terminal completion that carries no record payload.
    pub fn terminal(
        event: UserCompletionEvent,
        detail: DriverResult<Spec::Completion, Spec::Error>,
    ) -> Self {
        Self::terminal_with_cleanup(event, detail, CompletionCleanupGuard::default())
    }

    /// Build a terminal completion with backend-owned cleanup.
    pub fn terminal_with_cleanup(
        event: UserCompletionEvent,
        detail: DriverResult<Spec::Completion, Spec::Error>,
        cleanup: CompletionCleanupGuard,
    ) -> Self {
        Self {
            event,
            input: CompletionInput::Terminal(TerminalCompletion { detail, cleanup }),
            continuation: CompletionContinuation::Final,
        }
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

    pub(super) fn run_rejected_cleanup(
        diagnostics: &DriverCompletionDiagnostics<Spec::CompletionDiagnostics>,
        mut packet: Self,
    ) {
        Self::run_completion_cleanup(diagnostics, packet.input.cleanup_mut());
        drop(packet);
    }

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
}

pub struct CompletionRecord<Spec: SlotSpec> {
    event: UserCompletionEvent,
    /// `None` 只用于没有可交付资源的终态错误 record。
    payload: Option<Spec::UserPayload>,
    detail: Option<DriverResult<Spec::Completion, Spec::Error>>,
    cleanup: CompletionCleanupGuard,
    /// 取走这条记录之后，该操作是否还会再产出完成。
    ///
    /// 单发操作恒为 [`CompletionContinuation::Final`]，于是 `LocalOp` / `DetachedOp` 的流
    /// 在第一项之后就结束；multishot 靠它判断流到此为止。
    continuation: CompletionContinuation,
}

impl<Spec: SlotSpec> CompletionRecord<Spec> {
    pub fn new(
        event: UserCompletionEvent,
        payload: Option<Spec::UserPayload>,
        detail: Option<DriverResult<Spec::Completion, Spec::Error>>,
        cleanup: CompletionCleanupGuard,
        continuation: CompletionContinuation,
    ) -> Self {
        Self {
            event,
            payload,
            detail,
            cleanup,
            continuation,
        }
    }

    pub const fn event(&self) -> UserCompletionEvent {
        self.event
    }

    pub fn payload(&self) -> Option<&Spec::UserPayload> {
        self.payload.as_ref()
    }

    pub fn detail(&self) -> Option<&DriverResult<Spec::Completion, Spec::Error>> {
        self.detail.as_ref()
    }

    pub const fn cleanup(&self) -> &CompletionCleanupGuard {
        &self.cleanup
    }

    pub fn cleanup_mut(&mut self) -> &mut CompletionCleanupGuard {
        &mut self.cleanup
    }

    pub const fn continuation(&self) -> CompletionContinuation {
        self.continuation
    }

    pub fn into_parts(self) -> CompletionRecordParts<Spec> {
        CompletionRecordParts {
            event: self.event,
            payload: self.payload,
            detail: self.detail,
            cleanup: self.cleanup,
            continuation: self.continuation,
        }
    }

    pub fn disarm_cleanup(&mut self) -> bool {
        self.cleanup.disarm()
    }
}
