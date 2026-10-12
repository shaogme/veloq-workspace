use veloq_std::{format, time::Instant, vec::Vec};

use diagweave::prelude::*;
use tracing::trace;
use veloq_io_uring::{IoUring, cqueue};

#[cfg(any(test, feature = "test-hooks"))]
use crate::driver::control::ControlPlaneEvent;
use crate::{
    config::UringDriveLimits,
    diagnostics::UringCompletionDiagnostics,
    driver::{
        completion::{
            COMP_BACKEND_URING, CompletionProgress, UringBackendIngress, UringCompletionHooks,
            UringSyntheticCompletion,
        },
        context::{
            CompletionContext, CompletionOperationBorrowParts, ControlPlaneInvariantAuditor,
        },
        control::{ExpiredBatch, UringControlPlane, UringPostCompletionEffects},
        drive::RoundBudget,
        operation::OperationLedger,
        registration::ProvidedBufPort,
    },
    error::{UringError, UringResult},
    op::{CheckedSlotView, RecordPolicy, SlotView, UringOpRegistryExt, UringSlotSpec},
};

use veloq_driver_core::{
    driver::{
        AnomalyAttach, CompletionAnomalyKind, CompletionEnvelope, CompletionFlowOutcome,
        CompletionFlowExt, CompletionIngress, CompletionToken, DriverCompletionDiagnostics,
        OpToken, SharedCompletionTable,
        SyntheticCompletionSource, UserCompletionEvent,
    },
};

#[derive(Clone, Copy)]
struct CqeRecord {
    raw_token: u64,
    result: i32,
    flags: u32,
}

pub(crate) struct CompletionScratch {
    cqe_buffer: Vec<CqeRecord>,
    effect_accumulator: Option<UringPostCompletionEffects>,
    pending_effects: Option<UringPostCompletionEffects>,
    pending_collector_exhausted: bool,
    limits: UringDriveLimits,
}

impl CompletionScratch {
    pub(super) fn with_capacity(
        cqe_capacity: usize,
        effect_capacity: usize,
        limits: UringDriveLimits,
    ) -> Self {
        Self {
            cqe_buffer: Vec::with_capacity(cqe_capacity),
            effect_accumulator: Some(UringPostCompletionEffects::with_capacity(effect_capacity)),
            pending_effects: None,
            pending_collector_exhausted: true,
            limits,
        }
    }

    pub(super) fn has_pending_cqes(&self) -> bool {
        !self.cqe_buffer.is_empty()
    }

    pub(super) const fn drive_limits(&self) -> UringDriveLimits {
        self.limits
    }

    pub(super) fn take_effects(&mut self) -> (UringPostCompletionEffects, bool) {
        (
            self.pending_effects
                .take()
                .expect("completion effect batch must be available"),
            self.pending_collector_exhausted,
        )
    }

    pub(super) fn recycle_effects(&mut self, mut effects: UringPostCompletionEffects) {
        effects.clear();
        debug_assert!(self.effect_accumulator.is_none());
        self.effect_accumulator = Some(effects);
    }

    pub(super) fn pending_effects_len(&self) -> usize {
        self.pending_effects
            .as_ref()
            .map_or(0, UringPostCompletionEffects::len)
    }

    pub(super) fn process_completion_batch(
        &mut self,
        context: &mut CompletionContext<'_, '_, '_, '_>,
        budget: &mut RoundBudget,
    ) -> UringResult<CompletionBatchProgress> {
        self.ports(context).process_completion_batch(budget)
    }

    pub(super) fn accept_synthetic_completion(
        &mut self,
        context: &mut CompletionContext<'_, '_, '_, '_>,
        event: UserCompletionEvent,
        source: SyntheticCompletionSource,
        synthetic: UringSyntheticCompletion,
    ) -> UringResult<CompletionFlowOutcome> {
        self.ports(context)
            .accept_completion_ingress(CompletionIngress::Synthetic { event, source }, synthetic)
    }

    pub(super) fn accept_completion_anomaly_kind(
        &mut self,
        context: &mut CompletionContext<'_, '_, '_, '_>,
        kind: CompletionAnomalyKind,
        attach: AnomalyAttach,
    ) -> UringResult<CompletionFlowOutcome> {
        self.ports(context).accept_completion_ingress(
            CompletionIngress::Anomaly { kind, attach },
            UringSyntheticCompletion::None,
        )
    }

    fn ports<'scratch, 'context>(
        &'scratch mut self,
        context: &'context mut CompletionContext<'_, '_, '_, '_>,
    ) -> CompletionPorts<'scratch, 'context> {
        let (operation, resources) = context.parts().split();
        let operation = operation.into_parts();
        let CompletionOperationBorrowParts {
            ops,
            control,
            completion_table,
            diagnostics: completion_diagnostics,
        } = operation;
        let (ring, provided_buffers) = resources.into_parts();
        CompletionPorts {
            scratch: self,
            ops,
            completion_diagnostics,
            completion_table,
            control,
            ring,
            provided_buffers,
        }
    }

    #[cfg(test)]
    pub(super) fn prepare_recycle_test(&mut self, capacity: usize) {
        let _ = self.effect_accumulator.take();
        self.pending_effects = Some(UringPostCompletionEffects::with_capacity(capacity));
    }
}

struct CompletionPorts<'scratch, 'context> {
    scratch: &'scratch mut CompletionScratch,
    ops: &'context mut OperationLedger,
    completion_diagnostics: &'context DriverCompletionDiagnostics<UringCompletionDiagnostics>,
    completion_table: &'context SharedCompletionTable<UringSlotSpec>,
    control: &'context mut UringControlPlane,
    ring: &'context mut IoUring,
    provided_buffers: Option<ProvidedBufPort<'context>>,
}

#[derive(Debug, Default)]
struct CqeCollection {
    count: usize,
    remaining: usize,
    collector_exhausted: bool,
    cqe_budget_hit: bool,
    emergency: bool,
    overflow: u32,
}

#[derive(Debug, Default)]
pub(crate) struct CompletionBatchProgress {
    flow: CompletionProgress,
    timer_count: usize,
    cqe_count: usize,
    pending_completion: bool,
    cqe_budget_hit: bool,
    emergency_drain: bool,
    cqe_overflow: bool,
}

impl CompletionBatchProgress {
    pub(crate) const fn user_completed(&self) -> usize {
        self.flow.user_completed
    }
    pub(crate) const fn timer_count(&self) -> usize {
        self.timer_count
    }
    pub(crate) const fn cqe_count(&self) -> usize {
        self.cqe_count
    }
    pub(crate) const fn cqe_budget_hit(&self) -> bool {
        self.cqe_budget_hit
    }
    pub(crate) const fn emergency_drain(&self) -> bool {
        self.emergency_drain
    }
    pub(crate) const fn cqe_overflow(&self) -> bool {
        self.cqe_overflow
    }
    pub(crate) const fn pending_work(&self) -> bool {
        self.pending_completion
    }
}

impl<'scratch, 'context> CompletionPorts<'scratch, 'context> {
    /// 推进自上次轮询以来完整经过的 timer tick。
    /// Advances the timer wheel by each whole tick elapsed since the previous poll.
    fn advance_timer_clock(&mut self) -> UringResult<ExpiredBatch> {
        let now = Instant::now();
        let expired = self
            .control
            .timers_mut()
            .advance_timer_wheel(now)
            .map_err(|error| {
                UringError::InvalidState
                    .report(
                        "uring.completion.advance_timer",
                        "timer wheel could not advance",
                    )
                    .with_ctx("timer_error", format!("{error:?}"))
            })?;

        #[cfg(any(test, feature = "test-hooks"))]
        for entry in expired.newly_expired_iter() {
            self.control.record(ControlPlaneEvent::TimerExpire {
                task_id: Some(entry.id),
                token: entry.item,
            });
        }
        Ok(expired)
    }

    fn quarantine_expired_timer(&mut self, token: OpToken) {
        self.control.quarantine_timer(token);
        let _ = self.ops.mark_terminal(
            token,
            self.control.observer_mut(),
            "expired timer was quarantined",
        );
    }

    fn collect_cqes(&mut self, max_cqes: usize) -> CqeCollection {
        let mut collection = CqeCollection::default();
        let mut cq = self.ring.completion();
        cq.sync();
        let visible = cq.len();
        collection.overflow = cq.overflow();
        let normal_limit = visible.min(max_cqes).min(self.scratch.limits.max_cqe_batch);
        let high_water = cq.capacity().saturating_mul(3) / 4;
        let emergency =
            visible > normal_limit && (visible >= high_water.max(1) || collection.overflow != 0);
        let limit = if emergency {
            visible
                .min(self.scratch.limits.emergency_drain_limit)
                .min(self.scratch.cqe_buffer.capacity())
        } else {
            normal_limit
        };

        trace!(visible, limit, emergency, "collecting uring completions");
        for _ in 0..limit {
            let Some(cqe) = cq.next() else {
                break;
            };
            let raw_token = cqe.user_data();
            if raw_token == CompletionToken::waker(0).raw() {
                self.control.waker().begin_processing();
            }
            self.scratch.cqe_buffer.push(CqeRecord {
                raw_token,
                result: cqe.result(),
                flags: cqe.flags(),
            });
            collection.count += 1;
        }
        collection.remaining = cq.len();
        collection.collector_exhausted = collection.remaining == 0;
        collection.cqe_budget_hit = visible > normal_limit;
        collection.emergency = emergency;
        collection
    }

    fn collect_udp_resume_tokens(&mut self, limit: usize) -> Vec<(OpToken, u32)> {
        let tokens: Vec<_> = self.ops.active_tokens().collect();
        tokens
            .into_iter()
            .filter_map(|token| {
                let view = self.ops.checked_slot_view(token).ok()?;
                let CheckedSlotView::Valid(SlotView::InFlightWaiting(mut slot)) = view else {
                    return None;
                };
                slot.with_access_mut(|access| {
                    let descriptor = access.operation().get_ref().descriptor();
                    if descriptor.record_policy != RecordPolicy::UdpMultishot {
                        return None;
                    }
                    unsafe { (descriptor.receive_generation)(access) }
                        .map(|generation| (token, generation))
                })
                .ok()
                .flatten()
            })
            .take(limit)
            .collect()
    }

    fn process_completion_batch(
        &mut self,
        budget: &mut RoundBudget,
    ) -> UringResult<CompletionBatchProgress> {
        let missing_cleanup_hints_before = self
            .completion_diagnostics
            .backend()
            .corrupt_cleanup_hint_missing_count();
        self.scratch.cqe_buffer.clear();
        let collection = self.collect_cqes(budget.cqes());
        budget.consume_cqes(collection.count);

        let mut batch_effects = self
            .scratch
            .effect_accumulator
            .take()
            .expect("completion effect accumulator must be available");
        batch_effects.clear();
        let mut progress = CompletionProgress::default();
        let mut first_error = None;

        for index in 0..self.scratch.cqe_buffer.len() {
            let cqe = self.scratch.cqe_buffer[index];
            let envelope = CompletionEnvelope::from_raw_parts(
                COMP_BACKEND_URING,
                cqe.raw_token,
                cqe.result,
                cqe.flags,
            );
            if let Err(error) = self
                .control
                .settle_staged_completion(envelope.raw.token, !cqueue::more(cqe.flags))
            {
                self.control.quarantine_unpublished_staged();
                remember_first_error(
                    &mut first_error,
                    UringError::InvalidState
                        .report(
                            "uring.completion.settle_staged",
                            format!("staged completion settlement failed: {error:?}"),
                        )
                        .attach_note(
                            "a CQE could not be correlated with its staged ledger entry; routing continues for cleanup",
                        ),
                );
            }
            let (_observation, outcome) = self.accept_completion_transaction_into(
                CompletionIngress::Kernel(envelope),
                UringSyntheticCompletion::None,
                &mut batch_effects,
            );
            #[cfg(any(test, feature = "test-hooks"))]
            observe_completion_result(self.control, _observation, &outcome);
            match outcome {
                Ok(outcome) => progress.merge(outcome),
                Err(report) => remember_first_error(&mut first_error, report),
            }
        }

        let missing_cleanup_hints = self
            .completion_diagnostics
            .backend()
            .corrupt_cleanup_hint_missing_count()
            .saturating_sub(missing_cleanup_hints_before);
        if missing_cleanup_hints != 0 {
            self.control.quarantine_unpublished_staged();
            remember_first_error(
                &mut first_error,
                UringError::InvalidState
                    .report(
                        "uring.completion.cleanup_hint",
                        "kernel completion cleanup sidecar is missing",
                    )
                    .with_ctx("missing_hints", missing_cleanup_hints),
            );
        }

        let expired = self.advance_timer_clock()?;
        let timer_count = expired.len().min(budget.timers());
        budget.consume_timers(timer_count);
        if expired.len() > timer_count {
            self.completion_diagnostics.backend().inc_timer_budget_hit();
        }
        for entry in expired.iter().take(timer_count) {
            let token = entry.item;
            let event = UserCompletionEvent::from_parts(COMP_BACKEND_URING, token, 0, 0);
            let (_observation, outcome) = self.accept_completion_transaction_into(
                CompletionIngress::Synthetic {
                    event,
                    source: SyntheticCompletionSource::Timer,
                },
                UringSyntheticCompletion::None,
                &mut batch_effects,
            );
            #[cfg(any(test, feature = "test-hooks"))]
            observe_completion_result(self.control, _observation, &outcome);
            match outcome {
                Ok(outcome) => progress.merge(outcome),
                Err(report) => {
                    self.quarantine_expired_timer(token);
                    remember_first_error(&mut first_error, report);
                }
            }
        }
        self.control
            .timers_mut()
            .recycle_expired(expired, timer_count);

        for (token, logical_receiver_generation) in self.collect_udp_resume_tokens(32) {
            for _ in 0..16 {
                let ingress = CompletionIngress::Backend(UringBackendIngress::ResumeUdpRecvMulti {
                    token,
                    logical_receiver_generation,
                });
                let (_observation, outcome) = self.accept_completion_transaction_into(
                    ingress,
                    UringSyntheticCompletion::None,
                    &mut batch_effects,
                );
                match outcome {
                    Ok(outcome) => {
                        let user_completed = outcome.user_completed;
                        progress.merge(outcome);
                        if user_completed == 0 {
                            break;
                        }
                    }
                    Err(report) => {
                        remember_first_error(&mut first_error, report);
                        break;
                    }
                }
            }
        }

        self.scratch.pending_collector_exhausted = collection.collector_exhausted;
        self.scratch.pending_effects = Some(batch_effects);
        self.scratch.cqe_buffer.clear();

        let invariant_result = ControlPlaneInvariantAuditor::check(self.ops, self.control);
        if let Err(invariant_error) = invariant_result {
            remember_first_error(&mut first_error, invariant_error);
        }
        if let Some(error) = first_error {
            return Err(error);
        }
        Ok(CompletionBatchProgress {
            flow: progress,
            timer_count,
            cqe_count: collection.count,
            pending_completion: collection.remaining > 0,
            cqe_budget_hit: collection.cqe_budget_hit,
            emergency_drain: collection.emergency,
            cqe_overflow: collection.overflow != 0,
        })
    }

    fn accept_completion_ingress(
        &mut self,
        ingress: CompletionIngress<UringBackendIngress>,
        synthetic: UringSyntheticCompletion,
    ) -> UringResult<CompletionFlowOutcome> {
        let mut effects = self
            .scratch
            .effect_accumulator
            .take()
            .expect("completion effect accumulator must be available");
        effects.clear();
        let (_observation, flow_result) =
            self.accept_completion_transaction_into(ingress, synthetic, &mut effects);
        #[cfg(any(test, feature = "test-hooks"))]
        observe_completion_result(self.control, _observation, &flow_result);
        self.scratch.pending_collector_exhausted = true;
        self.scratch.pending_effects = Some(effects);
        flow_result
    }

    fn accept_completion_transaction_into(
        &mut self,
        ingress: CompletionIngress<UringBackendIngress>,
        synthetic: UringSyntheticCompletion,
        effects: &mut UringPostCompletionEffects,
    ) -> (Option<(OpToken, bool)>, UringResult<CompletionFlowOutcome>) {
        let observation = completion_observation(&ingress);
        let control = self.control.with_completion_view();
        let mut hooks = UringCompletionHooks::new(
            self.completion_diagnostics,
            control,
            self.provided_buffers.take(),
            synthetic,
        );
        let flow_result = self.ops.accept_completion(
            self.completion_table,
            self.completion_diagnostics,
            &mut hooks,
            ingress,
        );
        self.provided_buffers = hooks.take_provided_buffers();
        drop(hooks);
        self.control.drain_post_effects_into(effects);
        if flow_result.is_err()
            && let Some((token, _)) = observation
        {
            self.control.quarantine_token(token);
        }
        if let Some((token, true)) = observation
            && let Ok(progress) = &flow_result
            && (progress.user_completed > 0 || progress.orphan_cleaned > 0)
        {
            self.control.release_quarantined_token(token);
        }
        (observation, flow_result)
    }
}

fn completion_observation(
    ingress: &CompletionIngress<UringBackendIngress>,
) -> Option<(OpToken, bool)> {
    let (token, flags) = match ingress {
        CompletionIngress::Kernel(envelope) => (envelope.raw.token.op_token()?, envelope.raw.flags),
        CompletionIngress::User(event) | CompletionIngress::Synthetic { event, .. } => {
            (event.token(), event.flags())
        }
        CompletionIngress::Backend(_) | CompletionIngress::Anomaly { .. } => return None,
    };
    Some((token, !cqueue::more(flags)))
}

#[cfg(any(test, feature = "test-hooks"))]
fn observe_completion_result(
    control: &mut UringControlPlane,
    observation: Option<(OpToken, bool)>,
    result: &UringResult<CompletionFlowOutcome>,
) {
    let Some((token, final_completion)) = observation else {
        return;
    };
    let Ok(progress) = result else {
        return;
    };
    if progress.user_completed == 0 && progress.orphan_cleaned == 0 {
        return;
    }
    control.record_completion_observation(token, final_completion);
}

fn remember_first_error(first_error: &mut Option<Report<UringError>>, report: Report<UringError>) {
    if first_error.is_none() {
        *first_error = Some(report);
    }
}
