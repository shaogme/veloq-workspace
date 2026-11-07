mod hooks;
mod protocol;
mod settlement;

use veloq_std::vec::Vec;

use veloq_driver_core::{
    driver::{
        AnomalyAttach, CompletionAnomalyKind, CompletionEnvelope, CompletionFlowExt,
        CompletionFlowOutcome, CompletionIngress, SyntheticCompletionSource, UserCompletionEvent,
    },
    slot::{CheckedSlotView, SlotRegistryExt, SlotView},
};

use crate::error::IocpResult;

use super::{
    registration::RegistrationService,
    state::{CompletionStateView, IocpDriverState},
};
use hooks::{IocpCompletionHooks, IocpCompletionHooksArgs};

pub(crate) use protocol::COMP_BACKEND_RIO;
pub(super) use protocol::{
    COMP_BACKEND_IOCP, IocpBackendIngress, IocpSubmissionFailurePhase, IocpSyntheticCompletion,
};

pub(super) struct CompletionService<'view, 'state> {
    state: &'view mut CompletionStateView<'state>,
}

impl<'view, 'state> CompletionService<'view, 'state> {
    pub(super) fn new(state: &'view mut CompletionStateView<'state>) -> Self {
        Self { state }
    }

    fn state_mut(&mut self) -> &mut CompletionStateView<'state> {
        self.state
    }

    pub(super) fn process_timers(&mut self) -> IocpResult<usize> {
        let timer_buffer = self.state_mut().timer.take_buffer();
        let mut expired = Vec::new();
        for entry in &timer_buffer {
            let token = entry.item;
            match self.state_mut().ops.checked_slot_view(token) {
                Ok(CheckedSlotView::Valid(SlotView::InFlightWaiting(mut slot))) => {
                    slot.platform_mut().timer_id = None;
                    expired.push(token);
                }
                Ok(CheckedSlotView::Valid(SlotView::InFlightOrphaned(mut slot))) => {
                    slot.platform_mut().timer_id = None;
                    expired.push(token);
                }
                _ => expired.push(token),
            }
        }

        let expired_count = expired.len();
        for token in expired {
            let event = UserCompletionEvent::from_parts(COMP_BACKEND_IOCP, token, 0, 0);
            self.accept_synthetic_completion(
                event,
                SyntheticCompletionSource::Timer,
                IocpSyntheticCompletion::None,
            )?;
        }
        self.state_mut().timer.restore_cleared_buffer(timer_buffer);
        Ok(expired_count)
    }

    pub(super) fn process_completion_envelope(
        &mut self,
        envelope: CompletionEnvelope,
    ) -> IocpResult<usize> {
        self.accept_completion_ingress(
            CompletionIngress::Kernel(envelope),
            IocpSyntheticCompletion::None,
        )?;
        Ok(1)
    }

    pub(super) fn process_backend_ingress(
        &mut self,
        ingress: IocpBackendIngress,
    ) -> IocpResult<usize> {
        self.accept_completion_ingress(
            CompletionIngress::Backend(ingress),
            IocpSyntheticCompletion::None,
        )?;
        Ok(1)
    }

    pub(super) fn accept_synthetic_completion(
        &mut self,
        event: UserCompletionEvent,
        source: SyntheticCompletionSource,
        synthetic: IocpSyntheticCompletion,
    ) -> IocpResult<CompletionFlowOutcome> {
        self.accept_completion_ingress(CompletionIngress::Synthetic { event, source }, synthetic)
    }

    pub(super) fn accept_completion_anomaly(
        &mut self,
        kind: CompletionAnomalyKind,
        attach: AnomalyAttach,
    ) -> IocpResult<CompletionFlowOutcome> {
        self.accept_completion_ingress(
            CompletionIngress::Anomaly { kind, attach },
            IocpSyntheticCompletion::None,
        )
    }

    pub(super) fn accept_raw_completion(
        &mut self,
        raw_token: u64,
        res: i32,
        flags: u32,
    ) -> IocpResult<CompletionFlowOutcome> {
        self.accept_completion_ingress(
            CompletionIngress::Kernel(CompletionEnvelope::from_raw_parts(
                COMP_BACKEND_IOCP,
                raw_token,
                res,
                flags,
            )),
            IocpSyntheticCompletion::None,
        )
    }

    fn accept_completion_ingress(
        &mut self,
        ingress: CompletionIngress<IocpBackendIngress>,
        synthetic: IocpSyntheticCompletion,
    ) -> IocpResult<CompletionFlowOutcome> {
        let state = self.state_mut();
        let port = state.completion.port_arc();
        let rio = state.rio.state_mut();
        let registered_slots = state.handles.submission_slots();
        let mut hooks = IocpCompletionHooks::new(IocpCompletionHooksArgs {
            ext: state.extensions,
            diagnostics: state.completion_diagnostics,
            rio,
            completion: state.completion,
            port,
            registered_slots,
            overlapped_registry: state.overlapped_registry,
            shutting_down: state.shutting_down,
            synthetic,
        });
        let outcome = state.ops.accept_completion(
            state.completion.table(),
            state.completion_diagnostics,
            &mut hooks,
            ingress,
        )?;
        let post = hooks.into_post_effects();
        if post.requires_socket_cleanup() {
            let mut view = IocpDriverState::registration_view_from_completion(self.state_mut());
            RegistrationService::new(&mut view).drain_deferred_socket_cleanup();
        }
        Ok(outcome)
    }
}
