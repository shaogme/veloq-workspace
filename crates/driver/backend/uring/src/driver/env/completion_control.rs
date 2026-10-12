use veloq_std::collections::HashMap;

use veloq_io_uring::cqueue;

use crate::{
    config::IoFd,
    driver::{
        control::{
            ControlPlaneEvent, ControlPlaneObserver, PendingCancel, UringControlEffectKind,
            UringPostCompletionEffects,
        },
        env::CompletionMetadata,
        lifecycle::CancellationPhase,
        registration::OwnedFdOwnershipTicket,
    },
    op::CompletionCleanupHintFn,
};
use veloq_driver_core::{
    driver::{CancelTicket, CompletionToken, OpToken, RawCompletion},
    slot::Generation,
};

/// The completion-side control projection.
///
/// It contains only bookkeeping needed to acknowledge control completions, settle cleanup
/// hints, identify the current waker generation, and append deferred control effects. Provided
/// buffers remain in the separate [`CqeEnv`] projection so completion hooks cannot reach
/// registration state.
/// 此视图只含确认控制完成、结算清理提示、读取当前 waker generation 和追加延迟 effect 所需账目；provided buffer 保持在独立的 `CqeEnv` 中。
pub(crate) struct CompletionControlView<'d> {
    pending_cancel_cqes: &'d mut HashMap<CancelTicket, PendingCancel>,
    completion_cleanup_hints: &'d mut HashMap<CompletionToken, Option<CompletionCleanupHintFn>>,
    waker_buf_len: usize,
    waker_generation: u64,
    observer: &'d mut ControlPlaneObserver,
    post: &'d mut UringPostCompletionEffects,
}

pub(crate) struct CompletionLedgerParts<'d> {
    pending_cancel_cqes: &'d mut HashMap<CancelTicket, PendingCancel>,
    completion_cleanup_hints: &'d mut HashMap<CompletionToken, Option<CompletionCleanupHintFn>>,
    observer: &'d mut ControlPlaneObserver,
    post: &'d mut UringPostCompletionEffects,
}

impl<'d> CompletionLedgerParts<'d> {
    pub(crate) fn new(
        pending_cancel_cqes: &'d mut HashMap<CancelTicket, PendingCancel>,
        completion_cleanup_hints: &'d mut HashMap<CompletionToken, Option<CompletionCleanupHintFn>>,
        observer: &'d mut ControlPlaneObserver,
        post: &'d mut UringPostCompletionEffects,
    ) -> Self {
        Self {
            pending_cancel_cqes,
            completion_cleanup_hints,
            observer,
            post,
        }
    }
}

impl<'d> CompletionControlView<'d> {
    pub(crate) fn new(ledger: CompletionLedgerParts<'d>, metadata: CompletionMetadata) -> Self {
        let CompletionLedgerParts {
            pending_cancel_cqes,
            completion_cleanup_hints,
            observer,
            post,
        } = ledger;
        Self {
            pending_cancel_cqes,
            completion_cleanup_hints,
            waker_buf_len: metadata.waker_buf_len(),
            waker_generation: metadata.waker_generation(),
            observer,
            post,
        }
    }

    #[inline]
    pub(crate) fn peek_completion_cleanup_hint(
        &mut self,
        token: CompletionToken,
    ) -> Option<Option<CompletionCleanupHintFn>> {
        self.completion_cleanup_hints.get(&token).copied()
    }

    #[inline]
    pub(crate) fn remove_completion_cleanup_hint(
        &mut self,
        token: CompletionToken,
        flags: u32,
    ) -> Option<Option<CompletionCleanupHintFn>> {
        let entry = if cqueue::more(flags) {
            self.completion_cleanup_hints.get(&token).copied()
        } else {
            self.completion_cleanup_hints.remove(&token)
        };
        if entry.is_some() {
            self.observer
                .record(ControlPlaneEvent::CleanupHintRemove(token));
        }
        entry
    }

    #[inline]
    pub(crate) fn take_pending_cancel(&mut self, ticket: CancelTicket) -> Option<PendingCancel> {
        let request = self.pending_cancel_cqes.remove(&ticket)?;
        self.observer
            .record(ControlPlaneEvent::CancelInFlightRemove {
                ticket,
                target: request.target(),
            });
        Some(request)
    }

    #[inline]
    pub(crate) fn waker_buf_len(&self) -> usize {
        self.waker_buf_len
    }

    #[inline]
    pub(crate) fn waker_generation(&self) -> u64 {
        self.waker_generation
    }

    #[inline]
    pub(crate) fn append_waker_effect(&mut self, generation: u64, rebuild: bool) {
        if rebuild {
            self.append_effect(
                None,
                None,
                UringControlEffectKind::WakerRebuild { generation },
            );
        }
        self.append_effect(
            None,
            None,
            UringControlEffectKind::WakerRearm { generation },
        );
        self.append_effect(None, None, UringControlEffectKind::BacklogKick);
    }

    #[inline]
    pub(crate) fn append_cancel_enoent(
        &mut self,
        cancel_ticket: CancelTicket,
        request: PendingCancel,
        raw: RawCompletion,
    ) {
        self.append_effect(
            Some(request.target()),
            Some(request.target().generation()),
            UringControlEffectKind::CancelReconcile {
                cancel_ticket,
                request,
                raw,
            },
        );
    }

    #[inline]
    pub(crate) fn append_cancel_phase_update(
        &mut self,
        cancel_ticket: CancelTicket,
        target: OpToken,
        phase: CancellationPhase,
    ) {
        self.append_effect(
            Some(target),
            Some(target.generation()),
            UringControlEffectKind::CancelAck {
                cancel_ticket,
                phase,
            },
        );
    }

    #[inline]
    pub(crate) fn append_close_unregister(&mut self, token: OpToken, fd: IoFd) {
        self.append_effect(
            Some(token),
            Some(token.generation()),
            UringControlEffectKind::CloseUnregister {
                ticket: OwnedFdOwnershipTicket::new(token, fd),
            },
        );
    }

    #[inline]
    pub(crate) fn append_udp_rearm(&mut self, token: OpToken, generation: u32) {
        self.append_effect(
            Some(token),
            Some(token.generation()),
            UringControlEffectKind::UdpRearm {
                logical_receiver_generation: generation,
            },
        );
    }

    #[inline]
    fn append_effect(
        &mut self,
        token: Option<OpToken>,
        generation: Option<Generation>,
        kind: UringControlEffectKind,
    ) {
        self.post.append(token, generation, kind);
    }
}
