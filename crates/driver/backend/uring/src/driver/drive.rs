use diagweave::prelude::*;

use crate::{
    config::UringDriveLimits,
    driver::{
        completion::{CompletionBatchProgress, CompletionEngine, effects::CompletionEffectBatch},
        context::DriveContext,
        lifecycle::LifecycleEngine,
        submission::{KernelEnterPlan, SubmissionEngine, SubmitProgress, WaitBudgetSource},
    },
    error::{UringError, UringResult},
};
use veloq_driver_core::driver::{DriveMode, DrivePendingWork};
use veloq_std::time::Duration;

/// A source that consumes a bounded portion of one drive round.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RoundBudgetKind {
    RemoteCancel,
    LifecycleAction,
    BacklogAction,
    SqeEnter,
    Cqe,
    Timer,
    Effect,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct BudgetCounter {
    remaining: usize,
    exhausted: bool,
}

impl BudgetCounter {
    const fn new(limit: usize) -> Self {
        Self {
            remaining: limit,
            exhausted: false,
        }
    }

    fn consume(&mut self, count: usize) {
        if count > self.remaining {
            self.exhausted = true;
        }
        self.remaining = self.remaining.saturating_sub(count);
    }

    fn try_consume_one(&mut self) -> bool {
        if self.remaining == 0 {
            self.exhausted = true;
            return false;
        }
        self.remaining -= 1;
        true
    }
}

/// Unified per-round budget.
///
/// Every bounded source in a drive round goes through this type. A zero remaining counter is
/// not by itself reported as exhaustion: the coordinator only reports exhaustion when the same
/// source still has pending work. This prevents a normally completed round from looking like it
/// needs another round merely because it used its entire budget.
pub(crate) struct RoundBudget {
    remote_cancels: BudgetCounter,
    lifecycle_actions: BudgetCounter,
    backlog_actions: BudgetCounter,
    sqe_enters: BudgetCounter,
    cqes: BudgetCounter,
    timers: BudgetCounter,
    effects: BudgetCounter,
}

impl RoundBudget {
    fn new(limits: UringDriveLimits) -> Self {
        let effect_limit = limits
            .max_cqe_batch
            .max(limits.emergency_drain_limit)
            .saturating_mul(4)
            .saturating_add(8);
        Self {
            remote_cancels: BudgetCounter::new(limits.max_control_events),
            lifecycle_actions: BudgetCounter::new(limits.max_cancel_actions),
            backlog_actions: BudgetCounter::new(limits.max_backlog_actions),
            sqe_enters: BudgetCounter::new(limits.max_submit_rounds),
            cqes: BudgetCounter::new(limits.max_cqes_per_drive),
            timers: BudgetCounter::new(limits.max_timer_expirations),
            effects: BudgetCounter::new(effect_limit),
        }
    }

    fn counter_mut(&mut self, kind: RoundBudgetKind) -> &mut BudgetCounter {
        match kind {
            RoundBudgetKind::RemoteCancel => &mut self.remote_cancels,
            RoundBudgetKind::LifecycleAction => &mut self.lifecycle_actions,
            RoundBudgetKind::BacklogAction => &mut self.backlog_actions,
            RoundBudgetKind::SqeEnter => &mut self.sqe_enters,
            RoundBudgetKind::Cqe => &mut self.cqes,
            RoundBudgetKind::Timer => &mut self.timers,
            RoundBudgetKind::Effect => &mut self.effects,
        }
    }

    fn counter(&self, kind: RoundBudgetKind) -> BudgetCounter {
        match kind {
            RoundBudgetKind::RemoteCancel => self.remote_cancels,
            RoundBudgetKind::LifecycleAction => self.lifecycle_actions,
            RoundBudgetKind::BacklogAction => self.backlog_actions,
            RoundBudgetKind::SqeEnter => self.sqe_enters,
            RoundBudgetKind::Cqe => self.cqes,
            RoundBudgetKind::Timer => self.timers,
            RoundBudgetKind::Effect => self.effects,
        }
    }

    fn consume(&mut self, kind: RoundBudgetKind, count: usize) {
        self.counter_mut(kind).consume(count);
    }

    fn try_sqe_enter(&mut self) -> bool {
        self.counter_mut(RoundBudgetKind::SqeEnter)
            .try_consume_one()
    }

    pub(crate) const fn cqes(&self) -> usize {
        self.cqes.remaining
    }

    pub(crate) fn consume_cqes(&mut self, count: usize) {
        self.consume(RoundBudgetKind::Cqe, count);
    }

    pub(crate) const fn timers(&self) -> usize {
        self.timers.remaining
    }

    pub(crate) fn consume_timers(&mut self, count: usize) {
        self.consume(RoundBudgetKind::Timer, count);
    }

    pub(crate) fn consume_effects(&mut self, count: usize) {
        self.consume(RoundBudgetKind::Effect, count);
    }

    pub(crate) const fn remote_cancels(&self) -> usize {
        self.remote_cancels.remaining
    }

    pub(crate) fn consume_remote_cancels(&mut self, count: usize) {
        self.consume(RoundBudgetKind::RemoteCancel, count);
    }

    pub(crate) const fn lifecycle_actions(&self) -> usize {
        self.lifecycle_actions.remaining
    }

    pub(crate) fn consume_lifecycle_actions(&mut self, count: usize) {
        self.consume(RoundBudgetKind::LifecycleAction, count);
    }

    pub(crate) const fn backlog_actions(&self) -> usize {
        self.backlog_actions.remaining
    }

    pub(crate) fn consume_backlog_actions(&mut self, count: usize) {
        self.consume(RoundBudgetKind::BacklogAction, count);
    }

    fn is_exhausted_for(&self, kind: RoundBudgetKind, pending: usize) -> bool {
        pending != 0 && (self.counter(kind).exhausted || self.counter(kind).remaining == 0)
    }

    fn exhausted_for(&self, pending: DrivePendingWork) -> bool {
        self.is_exhausted_for(RoundBudgetKind::RemoteCancel, pending.remote_cancels)
            || self.is_exhausted_for(RoundBudgetKind::LifecycleAction, pending.lifecycle_actions)
            || self.is_exhausted_for(RoundBudgetKind::BacklogAction, pending.backlog_actions)
            || self.is_exhausted_for(RoundBudgetKind::SqeEnter, pending.sqe_enters)
            || self.is_exhausted_for(RoundBudgetKind::Cqe, pending.cqes)
            || self.is_exhausted_for(RoundBudgetKind::Timer, pending.timers)
            || self.is_exhausted_for(RoundBudgetKind::Effect, pending.effects)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct DriveRoundReport {
    pub(crate) pending_work: DrivePendingWork,
    pub(crate) budget_exhausted: bool,
    pub(crate) needs_next_round: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WaitPriority {
    UserCompletion,
    WakerCompletion,
    TimerDeadline,
    ZeroTimeout,
    ExternalWait,
    ProbeWait,
}

/// Resolves wait inputs in one place so timeout and completion precedence cannot drift between
/// the real driver and table-driven tests.
pub(crate) const fn wait_priority(
    user_completion: bool,
    waker_completion: bool,
    timer_deadline: bool,
    zero_timeout: bool,
    external_timeout: bool,
) -> WaitPriority {
    if user_completion {
        WaitPriority::UserCompletion
    } else if waker_completion {
        WaitPriority::WakerCompletion
    } else if timer_deadline {
        WaitPriority::TimerDeadline
    } else if zero_timeout {
        WaitPriority::ZeroTimeout
    } else if external_timeout {
        WaitPriority::ExternalWait
    } else {
        WaitPriority::ProbeWait
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct WaitBudget {
    duration: Duration,
    source: WaitBudgetSource,
}

impl WaitBudget {
    pub(crate) const fn new(duration: Duration, source: WaitBudgetSource) -> Self {
        Self { duration, source }
    }

    pub(crate) const fn duration(self) -> Duration {
        self.duration
    }

    pub(crate) const fn source(self) -> WaitBudgetSource {
        self.source
    }
}

pub(crate) fn wait_budget(
    external_timeout: Option<Duration>,
    internal_timeout: Option<Duration>,
    probe: Duration,
) -> WaitBudget {
    let mut budget = WaitBudget::new(probe, WaitBudgetSource::Probe);

    if let Some(internal) = internal_timeout
        && internal <= budget.duration
    {
        budget = WaitBudget::new(internal, WaitBudgetSource::Timer);
    }
    if let Some(external) = external_timeout
        && external <= budget.duration
    {
        budget = WaitBudget::new(external, WaitBudgetSource::External);
    }
    budget
}

/// Drives one bounded round and coordinates lifecycle, submission and completion in a fixed order.
pub(crate) struct DriveCoordinator {
    limits: UringDriveLimits,
}

/// Executes completion effects after ingress settlement has released its slot/CQE borrows.
///
/// The executor is intentionally stateless. Its position in the drive layer, rather than in
/// `CompletionEngine`, is the ownership boundary that prevents completion routing from closing
/// descriptors, rebuilding the waker, or re-entering submission.
pub(crate) struct EffectExecutor;

impl EffectExecutor {
    pub(crate) fn execute(
        &mut self,
        context: &mut DriveContext<'_, '_, '_, '_>,
        lifecycle: &mut LifecycleEngine,
        completion: &mut CompletionEngine,
        effects: &CompletionEffectBatch,
        collector_exhausted: bool,
    ) -> UringResult<()> {
        context.execute_completion_effects(lifecycle, completion, effects, collector_exhausted)
    }
}

impl DriveCoordinator {
    pub(crate) const fn new(limits: UringDriveLimits) -> Self {
        Self { limits }
    }

    pub(crate) fn run(
        &mut self,
        context: &mut DriveContext<'_, '_, '_, '_>,
        submission: &mut SubmissionEngine,
        lifecycle: &mut LifecycleEngine,
        completion: &mut CompletionEngine,
        mode: DriveMode,
    ) -> UringResult<DriveRoundReport> {
        let mut budget = RoundBudget::new(self.limits);
        if matches!(mode, DriveMode::Wait { .. }) {
            context.diagnostics().backend().inc_wait_enter();
        }

        let (remote_cancel_pending, mut first_error) =
            self.prepare_round(context, lifecycle, submission, completion, &mut budget);
        if first_error.is_none()
            && let Some(error) = self.run_kernel_rounds(
                context,
                submission,
                lifecycle,
                completion,
                mode,
                &mut budget,
            )
        {
            remember_first_error(&mut first_error, error);
        }

        if first_error.is_some() {
            context.enter_submission_fail_stop(submission);
        }
        let pending_work = context.pending_work(remote_cancel_pending, lifecycle, completion);
        let budget_exhausted = budget.exhausted_for(pending_work);
        let needs_next_round = !pending_work.is_empty();
        let report = DriveRoundReport {
            pending_work,
            budget_exhausted,
            needs_next_round,
        };
        match first_error {
            Some(error) => Err(error),
            None => Ok(report),
        }
    }

    fn prepare_round(
        &self,
        context: &mut DriveContext<'_, '_, '_, '_>,
        lifecycle: &mut LifecycleEngine,
        submission: &mut SubmissionEngine,
        completion: &mut CompletionEngine,
        budget: &mut RoundBudget,
    ) -> (usize, Option<Report<UringError>>) {
        let mut first_error = None;
        let remote_cancel_limit = budget.remote_cancels();
        let mut remote_cancel_pending = 0;
        match context.drain_cancel_requests_bounded(lifecycle, completion, remote_cancel_limit) {
            Ok(drained) => {
                budget.consume_remote_cancels(drained);
                if drained == remote_cancel_limit && drained != 0 {
                    // mpsc does not expose a non-destructive length query. Equal-to-limit is a
                    // conservative lower bound: the next round is guaranteed to re-check it.
                    remote_cancel_pending = 1;
                }
            }
            Err(error) => remember_first_error(&mut first_error, error),
        }
        if first_error.is_none() {
            match context.stage_pending_cancellations(
                lifecycle,
                completion,
                budget.lifecycle_actions(),
            ) {
                Ok(count) => budget.consume_lifecycle_actions(count),
                Err(error) => remember_first_error(&mut first_error, error),
            }
        }
        if first_error.is_none() {
            match context.stage_backlog_entries(
                lifecycle,
                submission,
                completion,
                budget.backlog_actions(),
            ) {
                Ok(progress) => budget.consume_backlog_actions(progress.actions()),
                Err(error) => remember_first_error(&mut first_error, error),
            }
        }
        if first_error.is_none()
            && let Err(error) = context.submit_waker(submission)
        {
            remember_first_error(&mut first_error, error);
        }
        (remote_cancel_pending, first_error)
    }

    fn run_kernel_rounds(
        &self,
        context: &mut DriveContext<'_, '_, '_, '_>,
        submission: &mut SubmissionEngine,
        lifecycle: &mut LifecycleEngine,
        completion: &mut CompletionEngine,
        mode: DriveMode,
        budget: &mut RoundBudget,
    ) -> Option<Report<UringError>> {
        let mut first_error = None;
        let mut next_enter_mode = mode;
        while first_error.is_none() && budget.try_sqe_enter() {
            if let Err(error) = context.submit_waker(submission) {
                remember_first_error(&mut first_error, error);
                break;
            }
            let plan = match context.build_kernel_enter_plan(next_enter_mode) {
                Ok(plan) => plan,
                Err(error) => {
                    remember_first_error(&mut first_error, error);
                    break;
                }
            };
            let submit_progress = match context.submit_to_kernel(submission, plan) {
                Ok(progress) => progress,
                Err(error) => {
                    remember_first_error(&mut first_error, error);
                    break;
                }
            };
            tracing::trace!(
                staged = submit_progress.staged(),
                kernel_outstanding = submit_progress.kernel_outstanding(),
                pending_submit = submit_progress.pending_submit(),
                receipt = ?submit_progress.receipt(),
                "completed kernel enter round"
            );
            let completion_progress =
                match context.process_completion_batch(lifecycle, completion, budget) {
                    Ok(progress) => progress,
                    Err(error) => {
                        remember_first_error(&mut first_error, error);
                        break;
                    }
                };
            self.record_wait_diagnostics(
                context,
                mode,
                plan,
                &submit_progress,
                &completion_progress,
            );
            if let Err(error) =
                self.stage_follow_up(context, submission, lifecycle, completion, budget)
            {
                remember_first_error(&mut first_error, error);
                break;
            }
            let pending_submit = submit_progress.pending_submit()
                || context.control_has_pending_work()
                || completion_progress.pending_work();
            if !pending_submit {
                break;
            }
            next_enter_mode = DriveMode::Poll;
        }
        first_error
    }

    fn stage_follow_up(
        &self,
        context: &mut DriveContext<'_, '_, '_, '_>,
        submission: &mut SubmissionEngine,
        lifecycle: &mut LifecycleEngine,
        completion: &mut CompletionEngine,
        budget: &mut RoundBudget,
    ) -> UringResult<()> {
        let count = context.stage_pending_cancellations(
            lifecycle,
            completion,
            budget.lifecycle_actions(),
        )?;
        budget.consume_lifecycle_actions(count);
        let progress = context.stage_backlog_entries(
            lifecycle,
            submission,
            completion,
            budget.backlog_actions(),
        )?;
        budget.consume_backlog_actions(progress.actions());
        Ok(())
    }

    fn record_wait_diagnostics(
        &self,
        context: &DriveContext<'_, '_, '_, '_>,
        mode: DriveMode,
        plan: KernelEnterPlan,
        submit_progress: &SubmitProgress,
        completion: &CompletionBatchProgress,
    ) {
        record_completion_diagnostics(context, completion);
        if matches!(mode, DriveMode::Wait { .. }) && completion.user_completed() > 0 {
            context.diagnostics().backend().inc_wait_completion_return();
        }
        if matches!(mode, DriveMode::Wait { .. })
            && completion.timer_count() > 0
            && !submit_progress.timed_out()
        {
            context.diagnostics().backend().inc_wait_timer_return();
        }
        if submit_progress.timed_out() {
            context.diagnostics().backend().inc_wait_timeout();
            if let Some(source) = plan.wait_source() {
                match source {
                    WaitBudgetSource::External => {
                        context.diagnostics().backend().inc_wait_external_timeout()
                    }
                    WaitBudgetSource::Timer => {
                        context.diagnostics().backend().inc_wait_timer_return();
                    }
                    WaitBudgetSource::Probe => {
                        context.diagnostics().backend().inc_wait_probe_return();
                    }
                }
            }
        }
    }
}

fn remember_first_error(first_error: &mut Option<Report<UringError>>, error: Report<UringError>) {
    if let Some(previous) = first_error.take() {
        *first_error = Some(previous.with_diag_src_err(error));
    } else {
        *first_error = Some(error);
    }
}

fn record_completion_diagnostics(
    context: &DriveContext<'_, '_, '_, '_>,
    completion: &CompletionBatchProgress,
) {
    let diagnostics = context.diagnostics().backend();
    diagnostics.inc_cqe_batch();
    diagnostics.add_cqes_collected(completion.cqe_count());
    diagnostics.add_timer_synthetic(completion.timer_count());
    if completion.cqe_budget_hit() {
        diagnostics.inc_cqe_budget_hit();
    }
    if completion.emergency_drain() {
        diagnostics.inc_cqe_emergency_drain();
    }
    if completion.cqe_overflow() {
        diagnostics.inc_cqe_overflow();
    }
}

#[cfg(test)]
mod tests {
    use super::{RoundBudget, WaitPriority, wait_priority};
    use crate::config::UringDriveLimits;
    use veloq_driver_core::driver::DrivePendingWork;

    #[test]
    fn completion_priority_is_deterministic() {
        let cases = [
            (true, true, true, true, true, WaitPriority::UserCompletion),
            (false, true, true, true, true, WaitPriority::WakerCompletion),
            (false, false, true, true, true, WaitPriority::TimerDeadline),
            (false, false, false, true, true, WaitPriority::ZeroTimeout),
            (false, false, false, false, true, WaitPriority::ExternalWait),
            (false, false, false, false, false, WaitPriority::ProbeWait),
        ];

        for (user, waker, timer, zero, external, expected) in cases {
            assert_eq!(wait_priority(user, waker, timer, zero, external), expected);
        }
    }

    #[test]
    fn user_completion_wins_over_every_wait_source() {
        assert_eq!(
            wait_priority(true, false, false, false, false),
            WaitPriority::UserCompletion
        );
    }

    #[test]
    fn priority_table_covers_all_wait_input_combinations() {
        for user in [false, true] {
            for waker in [false, true] {
                for timer in [false, true] {
                    for zero in [false, true] {
                        for external in [false, true] {
                            let expected = if user {
                                WaitPriority::UserCompletion
                            } else if waker {
                                WaitPriority::WakerCompletion
                            } else if timer {
                                WaitPriority::TimerDeadline
                            } else if zero {
                                WaitPriority::ZeroTimeout
                            } else if external {
                                WaitPriority::ExternalWait
                            } else {
                                WaitPriority::ProbeWait
                            };
                            assert_eq!(wait_priority(user, waker, timer, zero, external), expected);
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn budget_exhaustion_requires_pending_work() {
        let mut budget = RoundBudget::new(UringDriveLimits::for_entries(1));
        budget.consume_remote_cancels(1);
        assert!(!budget.exhausted_for(DrivePendingWork::default()));
        assert!(budget.exhausted_for(DrivePendingWork {
            remote_cancels: 1,
            ..DrivePendingWork::default()
        }));
    }
}
