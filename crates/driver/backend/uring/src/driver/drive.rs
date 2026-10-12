use diagweave::prelude::*;

use crate::{
    config::UringDriveLimits,
    driver::{
        completion::{CompletionBatchProgress, CompletionEngine},
        context::DriveContext,
        lifecycle::LifecycleEngine,
        submission::{KernelEnterPlan, SubmissionEngine, SubmitProgress, WaitBudgetSource},
    },
    error::{UringError, UringResult},
};
use veloq_driver_core::driver::{DriveMode, DrivePendingWork};
use veloq_std::time::Duration;

/// A source that consumes a bounded portion of one drive round.
/// 表示一次 drive 轮次中受预算限制的工作来源。
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
/// 所有有界工作共享本轮预算；只有同一来源仍有待处理工作时，耗尽预算才表示需要下一轮。
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

pub(crate) struct DriveRoundPorts<'borrow, 'ops, 'registration, 'ring, 'driver> {
    context: &'borrow mut DriveContext<'ops, 'registration, 'ring, 'driver>,
    submission: &'borrow mut SubmissionEngine,
    lifecycle: &'borrow mut LifecycleEngine,
    completion: &'borrow mut CompletionEngine,
}

impl<'borrow, 'ops, 'registration, 'ring, 'driver>
    DriveRoundPorts<'borrow, 'ops, 'registration, 'ring, 'driver>
{
    pub(crate) fn new(
        context: &'borrow mut DriveContext<'ops, 'registration, 'ring, 'driver>,
        submission: &'borrow mut SubmissionEngine,
        lifecycle: &'borrow mut LifecycleEngine,
        completion: &'borrow mut CompletionEngine,
    ) -> Self {
        Self {
            context,
            submission,
            lifecycle,
            completion,
        }
    }
}

struct DriveRoundProgress<'round> {
    plan: KernelEnterPlan,
    submission: &'round SubmitProgress,
    completion: &'round CompletionBatchProgress,
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
/// 统一解析等待输入，避免真实驱动和表驱动测试对超时与完成优先级采用不同规则。
impl WaitPriority {
    pub(crate) const fn resolve(
        user_completion: bool,
        waker_completion: bool,
        timer_deadline: bool,
        zero_timeout: bool,
        external_timeout: bool,
    ) -> Self {
        if user_completion {
            Self::UserCompletion
        } else if waker_completion {
            Self::WakerCompletion
        } else if timer_deadline {
            Self::TimerDeadline
        } else if zero_timeout {
            Self::ZeroTimeout
        } else if external_timeout {
            Self::ExternalWait
        } else {
            Self::ProbeWait
        }
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

    pub(crate) fn resolve(
        external_timeout: Option<Duration>,
        internal_timeout: Option<Duration>,
        probe: Duration,
    ) -> Self {
        let mut budget = Self::new(probe, WaitBudgetSource::Probe);

        if let Some(internal) = internal_timeout
            && internal <= budget.duration
        {
            budget = Self::new(internal, WaitBudgetSource::Timer);
        }
        if let Some(external) = external_timeout
            && external <= budget.duration
        {
            budget = Self::new(external, WaitBudgetSource::External);
        }
        budget
    }
}

/// Drives one bounded round and coordinates lifecycle, submission and completion in a fixed order.
/// 按固定顺序协调生命周期、提交和完成处理，驱动一轮有界工作。
pub(crate) struct DriveCoordinator {
    limits: UringDriveLimits,
}

impl DriveCoordinator {
    pub(crate) const fn new(limits: UringDriveLimits) -> Self {
        Self { limits }
    }

    pub(crate) fn run(
        &mut self,
        mut ports: DriveRoundPorts<'_, '_, '_, '_, '_>,
        mode: DriveMode,
    ) -> UringResult<DriveRoundReport> {
        let mut budget = RoundBudget::new(self.limits);
        if matches!(mode, DriveMode::Wait { .. }) {
            ports.context.diagnostics().backend().inc_wait_enter();
        }

        let (remote_cancel_pending, mut first_error) = self.prepare_round(&mut ports, &mut budget);
        if first_error.is_none()
            && let Some(error) = self.run_kernel_rounds(&mut ports, mode, &mut budget)
        {
            remember_first_error(&mut first_error, error);
        }

        if first_error.is_some() {
            ports.context.enter_submission_fail_stop(ports.submission);
        }
        let pending_work =
            ports
                .context
                .pending_work(remote_cancel_pending, ports.lifecycle, ports.completion);
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
        ports: &mut DriveRoundPorts<'_, '_, '_, '_, '_>,
        budget: &mut RoundBudget,
    ) -> (usize, Option<Report<UringError>>) {
        let mut first_error = None;
        let remote_cancel_limit = budget.remote_cancels();
        let mut remote_cancel_pending = 0;
        match ports.context.drain_cancel_requests_bounded(
            ports.lifecycle,
            ports.completion,
            remote_cancel_limit,
        ) {
            Ok(drained) => {
                budget.consume_remote_cancels(drained);
                if drained == remote_cancel_limit && drained != 0 {
                    // mpsc does not expose a non-destructive length query. Equal-to-limit is a
                    // conservative lower bound: the next round is guaranteed to re-check it.
                    // mpsc 没有无副作用的长度查询；达到上限时按保守下界处理，确保下一轮重新检查。
                    remote_cancel_pending = 1;
                }
            }
            Err(error) => remember_first_error(&mut first_error, error),
        }
        if first_error.is_none() {
            match ports.context.stage_pending_cancellations(
                ports.lifecycle,
                ports.completion,
                budget.lifecycle_actions(),
            ) {
                Ok(count) => budget.consume_lifecycle_actions(count),
                Err(error) => remember_first_error(&mut first_error, error),
            }
        }
        if first_error.is_none() {
            match ports.context.stage_backlog_entries(
                ports.lifecycle,
                ports.submission,
                ports.completion,
                budget.backlog_actions(),
            ) {
                Ok(progress) => budget.consume_backlog_actions(progress.actions()),
                Err(error) => remember_first_error(&mut first_error, error),
            }
        }
        if first_error.is_none()
            && let Err(error) = ports.context.submit_waker(ports.submission)
        {
            remember_first_error(&mut first_error, error);
        }
        (remote_cancel_pending, first_error)
    }

    fn run_kernel_rounds(
        &self,
        ports: &mut DriveRoundPorts<'_, '_, '_, '_, '_>,
        mode: DriveMode,
        budget: &mut RoundBudget,
    ) -> Option<Report<UringError>> {
        let mut first_error = None;
        let mut next_enter_mode = mode;
        while first_error.is_none() && budget.try_sqe_enter() {
            if let Err(error) = ports.context.submit_waker(ports.submission) {
                remember_first_error(&mut first_error, error);
                break;
            }
            let plan = match ports.context.build_kernel_enter_plan(next_enter_mode) {
                Ok(plan) => plan,
                Err(error) => {
                    remember_first_error(&mut first_error, error);
                    break;
                }
            };
            let submit_progress = match ports.context.submit_to_kernel(ports.submission, plan) {
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
            let completion_progress = match ports.context.process_completion_batch(
                ports.lifecycle,
                ports.completion,
                budget,
            ) {
                Ok(progress) => progress,
                Err(error) => {
                    remember_first_error(&mut first_error, error);
                    break;
                }
            };
            self.record_wait_diagnostics(
                ports,
                mode,
                DriveRoundProgress {
                    plan,
                    submission: &submit_progress,
                    completion: &completion_progress,
                },
            );
            if let Err(error) = self.stage_follow_up(ports, budget) {
                remember_first_error(&mut first_error, error);
                break;
            }
            let pending_submit = submit_progress.pending_submit()
                || ports.context.control_has_pending_work()
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
        ports: &mut DriveRoundPorts<'_, '_, '_, '_, '_>,
        budget: &mut RoundBudget,
    ) -> UringResult<()> {
        let count = ports.context.stage_pending_cancellations(
            ports.lifecycle,
            ports.completion,
            budget.lifecycle_actions(),
        )?;
        budget.consume_lifecycle_actions(count);
        let progress = ports.context.stage_backlog_entries(
            ports.lifecycle,
            ports.submission,
            ports.completion,
            budget.backlog_actions(),
        )?;
        budget.consume_backlog_actions(progress.actions());
        Ok(())
    }

    fn record_wait_diagnostics(
        &self,
        ports: &DriveRoundPorts<'_, '_, '_, '_, '_>,
        mode: DriveMode,
        progress: DriveRoundProgress<'_>,
    ) {
        record_completion_diagnostics(ports.context, progress.completion);
        if matches!(mode, DriveMode::Wait { .. }) && progress.completion.user_completed() > 0 {
            ports
                .context
                .diagnostics()
                .backend()
                .inc_wait_completion_return();
        }
        if matches!(mode, DriveMode::Wait { .. })
            && progress.completion.timer_count() > 0
            && !progress.submission.timed_out()
        {
            ports
                .context
                .diagnostics()
                .backend()
                .inc_wait_timer_return();
        }
        if progress.submission.timed_out() {
            ports.context.diagnostics().backend().inc_wait_timeout();
            if let Some(source) = progress.plan.wait_source() {
                match source {
                    WaitBudgetSource::External => ports
                        .context
                        .diagnostics()
                        .backend()
                        .inc_wait_external_timeout(),
                    WaitBudgetSource::Timer => {
                        ports
                            .context
                            .diagnostics()
                            .backend()
                            .inc_wait_timer_return();
                    }
                    WaitBudgetSource::Probe => {
                        ports
                            .context
                            .diagnostics()
                            .backend()
                            .inc_wait_probe_return();
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
    use super::{RoundBudget, WaitPriority};
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
            assert_eq!(
                WaitPriority::resolve(user, waker, timer, zero, external),
                expected
            );
        }
    }

    #[test]
    fn user_completion_wins_over_every_wait_source() {
        assert_eq!(
            WaitPriority::resolve(true, false, false, false, false),
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
                            assert_eq!(
                                WaitPriority::resolve(user, waker, timer, zero, external),
                                expected
                            );
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
