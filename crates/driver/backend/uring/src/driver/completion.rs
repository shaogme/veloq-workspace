use veloq_std::{
    collections::HashMap,
    format, mem,
    num::NonZeroU8,
    sync::atomic::{AtomicU8, Ordering},
    time::{Duration, Instant},
    vec::Vec,
};

use diagweave::prelude::*;
use tracing::{debug, error, trace, warn};

use crate::{
    config::IoFd,
    diagnostics::UringCompletionDiagnostics,
    driver::control::waker::{WAKER_PROCESSING, WAKER_REARM},
    driver::{CqeEnv, PendingCancel, ProvidedBufGroup, UringDriver},
    error::{UringError, UringResult, uring_report_to_event_res},
    op::{CompletionCleanupHintFn, Slot, UringSlotSpec, UringUserPayload},
};

#[cfg(test)]
use crate::driver::control::waker::WAKER_NOTIFIED;
use veloq_driver_core::{
    driver::{
        AnomalyAttach, CancelCompletionId, CancelMode, CompletionAnomalyKind, CompletionBackend,
        CompletionBackendHooks, CompletionCleanupGuard, CompletionContinuation, CompletionControl,
        CompletionEnvelope, CompletionFlowExt, CompletionFlowOutcome, CompletionHookOutcome,
        CompletionIngress, CompletionSource, CompletionToken, Driver, DriverCompletionDiagnostics,
        OpToken, PlatformOp, RawCompletion, SyntheticCompletionSource, UserCompletionEvent,
        run_completion_cleanup,
    },
    slot::{CheckedSlotView, InFlightOrphaned, InFlightWaiting, SlotRegistryExt, SlotView},
};

pub(crate) type CompletionProgress = CompletionFlowOutcome;
pub(crate) const COMP_BACKEND_URING: CompletionBackend =
    CompletionBackend::Backend(match NonZeroU8::new(2) {
        Some(val) => val,
        None => unreachable!(),
    });

enum CompletionCleanupHintState {
    Missing,
    Known(Option<CompletionCleanupHintFn>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WaitBudgetSource {
    External,
    Timer,
    Probe,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct WaitBudget {
    duration: Duration,
    source: WaitBudgetSource,
}

fn wait_budget(
    external_timeout: Option<Duration>,
    internal_timeout: Option<Duration>,
    probe: Duration,
) -> WaitBudget {
    let mut budget = WaitBudget {
        duration: probe,
        source: WaitBudgetSource::Probe,
    };

    if let Some(internal) = internal_timeout
        && internal <= budget.duration
    {
        budget = WaitBudget {
            duration: internal,
            source: WaitBudgetSource::Timer,
        };
    }
    if let Some(external) = external_timeout
        && external <= budget.duration
    {
        budget = WaitBudget {
            duration: external,
            source: WaitBudgetSource::External,
        };
    }
    budget
}

pub(crate) enum UringSyntheticCompletion {
    None,
    Cancel { mode: CancelMode },
    SubmissionFailure { report: Option<Report<UringError>> },
}

impl UringSyntheticCompletion {
    #[inline]
    fn cancel_mode(&self) -> CancelMode {
        match self {
            Self::Cancel { mode } => *mode,
            Self::None | Self::SubmissionFailure { .. } => CancelMode::UserVisible,
        }
    }

    #[inline]
    fn take_submission_failure(&mut self) -> Option<Report<UringError>> {
        match self {
            Self::SubmissionFailure { report } => report.take(),
            Self::None | Self::Cancel { .. } => None,
        }
    }
}

#[derive(Default)]
struct UringPostCompletionEffects {
    rebuild_waker: bool,
    resubmit_waker: bool,
    flush_backlog: bool,
    cancel_enoent: Vec<(CancelCompletionId, PendingCancel, RawCompletion)>,
    close_unregister: Vec<IoFd>,
}

enum UringBackendEffect {
    None,
    Waker {
        recovery: WakerRecovery,
    },
    CancelEnoent {
        cancel_id: CancelCompletionId,
        request: PendingCancel,
        raw: RawCompletion,
    },
    CloseCompleted {
        fd: IoFd,
    },
}

#[derive(Clone, Copy)]
enum WakerRecovery {
    Rearm,
    RebuildAndRearm,
}

impl Default for UringBackendEffect {
    #[inline]
    fn default() -> Self {
        Self::None
    }
}

struct UringCompletionSidecar<'a> {
    pending_cancel_cqes: &'a mut HashMap<CancelCompletionId, PendingCancel>,
    completion_cleanup_hints: &'a mut HashMap<CompletionToken, Option<CompletionCleanupHintFn>>,
}

struct UringCompletionHooks<'a> {
    diagnostics: &'a DriverCompletionDiagnostics<UringCompletionDiagnostics>,
    sidecar: UringCompletionSidecar<'a>,
    waker_buf_len: usize,
    waker_armed: &'a mut bool,
    notification_state: &'a AtomicU8,
    provided_buffers: Option<&'a mut ProvidedBufGroup>,
    synthetic: UringSyntheticCompletion,
    post: UringPostCompletionEffects,
}

impl<'a> UringCompletionHooks<'a> {
    fn new(
        diagnostics: &'a DriverCompletionDiagnostics<UringCompletionDiagnostics>,
        sidecar: UringCompletionSidecar<'a>,
        waker_buf_len: usize,
        waker_armed: &'a mut bool,
        notification_state: &'a AtomicU8,
        provided_buffers: Option<&'a mut ProvidedBufGroup>,
        synthetic: UringSyntheticCompletion,
    ) -> Self {
        Self {
            diagnostics,
            sidecar,
            waker_buf_len,
            waker_armed,
            notification_state,
            provided_buffers,
            synthetic,
            post: UringPostCompletionEffects::default(),
        }
    }

    fn into_post_effects(self) -> UringPostCompletionEffects {
        self.post
    }

    #[inline]
    fn cqe_env(&mut self) -> CqeEnv<'_> {
        CqeEnv::new(self.provided_buffers.as_deref_mut())
    }

    fn completion_cleanup_hint_for(
        &mut self,
        token: CompletionToken,
        flags: u32,
    ) -> CompletionCleanupHintState {
        let entry = if io_uring::cqueue::more(flags) {
            self.sidecar.completion_cleanup_hints.get(&token).copied()
        } else {
            self.sidecar.completion_cleanup_hints.remove(&token)
        };
        match entry {
            Some(hint) => CompletionCleanupHintState::Known(hint),
            None => CompletionCleanupHintState::Missing,
        }
    }

    fn cleanup_corrupt_completion(
        &mut self,
        event: UserCompletionEvent,
        kind: CompletionAnomalyKind,
        source: CompletionSource<'_, ()>,
    ) -> CompletionCleanupGuard {
        let raw = event.raw();
        self.cqe_env().return_provided_buf(raw.flags);
        let hint = if matches!(source, CompletionSource::Kernel) {
            match self.completion_cleanup_hint_for(raw.token, raw.flags) {
                CompletionCleanupHintState::Known(hint) => {
                    if hint.is_some() {
                        self.diagnostics.backend().inc_corrupt_cleanup_attempt();
                        if raw.res >= 0 {
                            self.diagnostics.backend().inc_corrupt_raw_fd_cleanup();
                        }
                    }
                    hint
                }
                CompletionCleanupHintState::Missing => {
                    self.diagnostics
                        .backend()
                        .inc_corrupt_cleanup_hint_missing();
                    None
                }
            }
        } else {
            None
        };

        debug!(
            completion_token = raw.token.raw(),
            cqe_result = raw.res,
            cqe_flags = raw.flags,
            anomaly = ?kind,
            "corrupt uring completion"
        );
        hint.map_or_else(CompletionCleanupGuard::default, |hint| hint(raw.res))
    }

    fn handle_waker_control(
        &mut self,
        raw: RawCompletion,
    ) -> CompletionHookOutcome<UringSlotSpec, UringBackendEffect> {
        if raw.res == self.waker_buf_len as i32 {
            self.diagnostics.backend().inc_waker_ok();
            self.diagnostics.backend().inc_wait_waker_return();
            CompletionHookOutcome::ControlHandled {
                effect: UringBackendEffect::Waker {
                    recovery: WakerRecovery::Rearm,
                },
            }
        } else if raw.res >= 0 {
            self.diagnostics.backend().inc_waker_error();
            warn!(
                res = raw.res,
                expected = self.waker_buf_len,
                "eventfd waker read returned unexpected byte count"
            );
            CompletionHookOutcome::Failed {
                error: UringError::CompletionWait
                    .report(
                        "uring.completion.handle_waker_control",
                        format!(
                            "eventfd waker read returned {} bytes, expected {}",
                            raw.res, self.waker_buf_len
                        ),
                    )
                    .with_ctx("completion_result", raw.res),
                effect: UringBackendEffect::Waker {
                    recovery: WakerRecovery::RebuildAndRearm,
                },
            }
        } else {
            self.diagnostics.backend().inc_waker_error();
            match -raw.res {
                libc::EAGAIN | libc::EINTR => {
                    debug!(res = raw.res, "recoverable eventfd waker read completion");
                    CompletionHookOutcome::ControlHandled {
                        effect: UringBackendEffect::Waker {
                            recovery: WakerRecovery::Rearm,
                        },
                    }
                }
                errno => {
                    warn!(res = raw.res, errno, "eventfd waker read failed");
                    CompletionHookOutcome::Failed {
                        error: UringError::CompletionWait
                            .report(
                                "uring.completion.handle_waker_control",
                                "eventfd waker read failed",
                            )
                            .set_error_code(errno),
                        effect: UringBackendEffect::Waker {
                            recovery: WakerRecovery::RebuildAndRearm,
                        },
                    }
                }
            }
        }
    }

    fn handle_cancel_control(
        &mut self,
        cancel_id: CancelCompletionId,
        raw: RawCompletion,
    ) -> CompletionHookOutcome<UringSlotSpec, UringBackendEffect> {
        let request = self.sidecar.pending_cancel_cqes.remove(&cancel_id);
        let Some(request) = request else {
            return CompletionHookOutcome::Failed {
                error: UringError::InvalidState.report(
                    "uring.completion.handle_cancel_control",
                    format!(
                        "async cancel completion had no pending request for cancel_id: {}",
                        cancel_id.raw()
                    ),
                ),
                effect: UringBackendEffect::None,
            };
        };

        match raw.res {
            value if value >= 0 => {
                self.diagnostics.backend().inc_cancel_ack_ok();
                trace!(
                    cancel_id = cancel_id.raw(),
                    request = ?request,
                    result = value,
                    "async cancel completed"
                );
                CompletionHookOutcome::ControlHandled {
                    effect: UringBackendEffect::None,
                }
            }
            value if value == -libc::ENOENT => {
                self.diagnostics.backend().inc_cancel_ack_not_found();
                debug!(
                    cancel_id = cancel_id.raw(),
                    request = ?request,
                    "async cancel target was already complete or absent"
                );
                CompletionHookOutcome::ControlHandled {
                    effect: UringBackendEffect::CancelEnoent {
                        cancel_id,
                        request,
                        raw,
                    },
                }
            }
            value => {
                self.diagnostics.backend().inc_cancel_ack_error();
                warn!(
                    cancel_id = cancel_id.raw(),
                    request = ?request,
                    result = value,
                    errno = -value,
                    "async cancel request failed"
                );
                CompletionHookOutcome::ControlHandled {
                    effect: UringBackendEffect::None,
                }
            }
        }
    }
}

impl CompletionBackendHooks<UringSlotSpec> for UringCompletionHooks<'_> {
    type BackendIngress = ();
    type BackendEffect = UringBackendEffect;

    fn handle_control(
        &mut self,
        control: CompletionControl,
    ) -> CompletionHookOutcome<UringSlotSpec, Self::BackendEffect> {
        match control {
            CompletionControl::Waker { raw, .. } => self.handle_waker_control(raw),
            CompletionControl::Cancel { id, raw } => self.handle_cancel_control(id, raw),
        }
    }

    fn complete_waiting(
        &mut self,
        event: UserCompletionEvent,
        slot: Slot<'_, InFlightWaiting>,
        source: CompletionSource<'_, Self::BackendIngress>,
    ) -> UringResult<CompletionHookOutcome<UringSlotSpec, Self::BackendEffect>> {
        match source {
            CompletionSource::Synthetic(SyntheticCompletionSource::Timer) => {
                complete_timer_waiting_slot(slot, event)
            }
            CompletionSource::Synthetic(SyntheticCompletionSource::Cancel) => {
                complete_cancel_waiting_slot(slot, event, self.synthetic.cancel_mode())
            }
            CompletionSource::Synthetic(SyntheticCompletionSource::SubmissionFailure) => {
                complete_submission_failure_slot(
                    slot,
                    event,
                    self.synthetic.take_submission_failure(),
                )
            }
            CompletionSource::Kernel | CompletionSource::User | CompletionSource::Backend(_) => {
                let raw = event.raw();
                if raw.res == -libc::ENOBUFS {
                    self.cqe_env().note_exhausted();
                }
                let hint = if matches!(source, CompletionSource::Kernel) {
                    Some(self.completion_cleanup_hint_for(event.completion_token(), raw.flags))
                } else {
                    None
                };
                match complete_kernel_waiting_slot(slot, event.token(), raw, &mut self.cqe_env()) {
                    Ok(outcome) => Ok(outcome),
                    Err(error) => {
                        if error.fallback_cleanup
                            && let Some(CompletionCleanupHintState::Known(Some(hint))) = hint
                        {
                            let mut cleanup = hint(raw.res);
                            let _ = run_completion_cleanup(self.diagnostics, &mut cleanup);
                        }
                        Err(error.report)
                    }
                }
            }
        }
    }

    /// 一条完成落到了不存在 / 已陈旧的 slot 上。
    ///
    /// 记录本身没什么可救的，但它可能带着一个 provided buffer——那个 bid 已经被内核从环里
    /// 取走，不还就是永久少一个。core 的默认实现只记异常，所以这里必须覆盖。
    fn complete_corrupt(
        &mut self,
        event: UserCompletionEvent,
        kind: CompletionAnomalyKind,
        _source: CompletionSource<'_, Self::BackendIngress>,
    ) -> UringResult<CompletionHookOutcome<UringSlotSpec, Self::BackendEffect>> {
        let cleanup = self.cleanup_corrupt_completion(event, kind, _source);
        Ok(CompletionHookOutcome::Anomaly {
            kind,
            attach: AnomalyAttach::from_raw_completion(event.raw()),
            cleanup,
            effect: UringBackendEffect::None,
        })
    }

    fn complete_orphaned(
        &mut self,
        event: UserCompletionEvent,
        slot: Slot<'_, InFlightOrphaned>,
        source: CompletionSource<'_, Self::BackendIngress>,
    ) -> UringResult<CompletionHookOutcome<UringSlotSpec, Self::BackendEffect>> {
        let res = match source {
            CompletionSource::Synthetic(SyntheticCompletionSource::Timer) => 0,
            CompletionSource::Synthetic(SyntheticCompletionSource::Cancel) => event.res(),
            CompletionSource::Synthetic(SyntheticCompletionSource::SubmissionFailure)
            | CompletionSource::Kernel
            | CompletionSource::User
            | CompletionSource::Backend(_) => event.raw().res,
        };
        // 这条完成要被丢弃，但内核已经从环里取走了它选中的 buffer——不还回去就是每次取消
        // 泄漏一个 bid。「取消不等于结束」在 provided buffer 上的具体形态。
        self.cqe_env().return_provided_buf(event.raw().flags);
        // 一个已放弃的 multishot 会继续投递完成，每一条都要跑 cleanup（accept 的话就是
        // 关掉那个内核已经建好的连接），但只有终态那条才能归还 slot。
        let continuation = if io_uring::cqueue::more(event.raw().flags) {
            CompletionContinuation::More
        } else {
            CompletionContinuation::Final
        };
        let hint = if matches!(source, CompletionSource::Kernel) {
            Some(self.completion_cleanup_hint_for(event.completion_token(), event.raw().flags))
        } else {
            None
        };
        let (slot_cleanup, slot_accessed) = if continuation.is_more() {
            cleanup_orphaned_streaming_slot(slot, res)
        } else {
            cleanup_orphaned_slot(slot, res)
        };
        let cleanup = if slot_accessed {
            slot_cleanup
        } else {
            match hint {
                Some(CompletionCleanupHintState::Known(Some(hint))) => hint(res),
                _ => CompletionCleanupGuard::default(),
            }
        };
        Ok(CompletionHookOutcome::Cleanup {
            cleanup,
            continuation,
            effect: UringBackendEffect::None,
        })
    }

    fn finish_backend_effect(&mut self, effect: Self::BackendEffect) -> UringResult<()> {
        match effect {
            UringBackendEffect::None => Ok(()),
            UringBackendEffect::Waker { recovery } => {
                *self.waker_armed = false;
                self.notification_state
                    .compare_exchange(
                        WAKER_PROCESSING,
                        WAKER_REARM,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    )
                    .ok();
                self.post.rebuild_waker |= matches!(recovery, WakerRecovery::RebuildAndRearm);
                self.post.resubmit_waker = true;
                self.post.flush_backlog = true;
                Ok(())
            }
            UringBackendEffect::CancelEnoent {
                cancel_id,
                request,
                raw,
            } => {
                self.post.cancel_enoent.push((cancel_id, request, raw));
                Ok(())
            }
            UringBackendEffect::CloseCompleted { fd } => {
                self.post.close_unregister.push(fd);
                Ok(())
            }
        }
    }
}

impl<'a> UringDriver<'a> {
    const WAKE_FAILURE_PROBE_INTERVAL: Duration = Duration::from_secs(1);

    pub(crate) fn wait_internal(&mut self, external_timeout: Option<Duration>) -> UringResult<()> {
        let _ = self.drain_cancel_requests()?;
        self.flush_cancellations()?;
        self.flush_backlog()?;
        self.submit_waker()?;
        self.submit_to_kernel()?;

        // Waiting has a non-blocking preflight. It only decides whether the CQ already contains
        // an event; all events are processed once below so a waker completion cannot cause this
        // call to enter a second wait before the runtime gets control back.
        let cq_ready = {
            let mut completion = self.ring.completion();
            completion.sync();
            !completion.is_empty()
        };
        self.advance_timer_clock()?;
        self.flush_cancellations()?;
        self.flush_backlog()?;
        let ready_completion = self.ops.shared.has_ready_completion();
        if cq_ready || ready_completion {
            self.completion_diagnostics
                .backend()
                .inc_wait_ready_preflight();
        }

        let budget = wait_budget(
            external_timeout,
            self.timers.next_timeout(),
            Self::WAKE_FAILURE_PROBE_INTERVAL,
        );
        let zero_timeout = budget.duration.is_zero();
        if zero_timeout {
            self.completion_diagnostics.backend().inc_wait_zero();
        }

        let did_block = !cq_ready && !ready_completion && !zero_timeout;
        let mut timed_out_source = None;
        if did_block {
            self.completion_diagnostics.backend().inc_wait_block();
            let duration = budget.duration;
            let ts = io_uring::types::Timespec::new()
                .sec(duration.as_secs())
                .nsec(duration.subsec_nanos());

            let args = io_uring::types::SubmitArgs::new().timespec(&ts);
            match self.ring.submitter().submit_with_args(1, &args) {
                Ok(_) => {}
                Err(ref e) if e.raw_os_error() == Some(libc::ETIME) => {
                    self.completion_diagnostics.backend().inc_wait_timeout();
                    timed_out_source = Some(budget.source);
                    match budget.source {
                        WaitBudgetSource::External => self
                            .completion_diagnostics
                            .backend()
                            .inc_wait_external_timeout(),
                        WaitBudgetSource::Timer => self
                            .completion_diagnostics
                            .backend()
                            .inc_wait_timer_return(),
                        WaitBudgetSource::Probe => self
                            .completion_diagnostics
                            .backend()
                            .inc_wait_probe_return(),
                    }
                }
                Err(e) => {
                    return Err(UringError::CompletionWait
                        .io_report("driver.wait_internal.submit_with_args", e));
                }
            }
        }

        let progress = self.process_completions_internal()?;
        if progress.user_completed > 0 {
            self.completion_diagnostics
                .backend()
                .inc_wait_completion_return();
        }
        let timer_count = self.advance_timer_clock()?;
        if did_block && timer_count > 0 && timed_out_source != Some(WaitBudgetSource::Timer) {
            self.completion_diagnostics
                .backend()
                .inc_wait_timer_return();
        }
        self.flush_cancellations()?;
        self.flush_backlog()?;
        Ok(())
    }

    /// Advances the timer wheel by however many whole ticks elapsed since the last poll.
    fn advance_timer_clock(&mut self) -> UringResult<usize> {
        let now = Instant::now();
        let expired = self.timers.advance_timer_wheel(now).to_vec();
        let expired_count = expired.len();
        for &token in &expired {
            let event = UserCompletionEvent::from_parts(COMP_BACKEND_URING, token, 0, 0);
            self.accept_synthetic_completion(
                event,
                SyntheticCompletionSource::Timer,
                UringSyntheticCompletion::None,
            )?;
        }
        Ok(expired_count)
    }

    pub(crate) fn poll_nonblocking_internal(&mut self) -> UringResult<()> {
        let _ = self.drain_cancel_requests()?;
        self.flush_cancellations()?;
        self.flush_backlog()?;
        self.submit_to_kernel()?;
        let progress = self.process_completions_internal()?;
        let _ = progress.semantic_count();

        self.advance_timer_clock()?;

        self.flush_cancellations()?;
        self.flush_backlog()?;
        Ok(())
    }

    pub(crate) fn process_completions_internal(&mut self) -> UringResult<CompletionProgress> {
        unsafe {
            self.ring
                .submitter()
                .enter::<()>(0, 0, 1 /* IORING_ENTER_GETEVENTS */, None)
                .map_err(|e| {
                    UringError::CompletionWait
                        .io_report("driver.process_completions_internal.enter", e)
                })?;
        }

        // The CQEs are copied out of the ring first so that the borrow of `self.ring` ends
        // before the routing loop needs `&mut self`. The buffer lives on the driver to keep
        // its allocation across polls, and is moved out for the same reason.
        let mut cqes = mem::take(&mut self.cqe_buffer);
        cqes.clear();
        {
            let mut cqe_kicker = self.ring.completion();
            cqe_kicker.sync();

            trace!("Processing completions, count={}", cqe_kicker.len());
            for cqe in cqe_kicker {
                let raw_token = cqe.user_data();
                if raw_token == CompletionToken::waker(0).raw() {
                    self.waker.begin_processing();
                }
                cqes.push((raw_token, cqe.result(), cqe.flags()));
            }
        }

        let mut progress = CompletionProgress::default();
        let mut first_error = None;
        for &(raw_token, cqe_res, cqe_flags) in &cqes {
            let outcome = self.accept_completion_ingress(
                CompletionIngress::Kernel(CompletionEnvelope::from_raw_parts(
                    COMP_BACKEND_URING,
                    raw_token,
                    cqe_res,
                    cqe_flags,
                )),
                UringSyntheticCompletion::None,
            );
            match outcome {
                Ok(outcome) => progress.merge(outcome),
                Err(report) => {
                    if first_error.is_none() {
                        first_error = Some(report);
                    }
                }
            }
        }

        cqes.clear();
        self.cqe_buffer = cqes;
        first_error.map_or(Ok(progress), Err)
    }

    pub(crate) fn accept_synthetic_completion(
        &mut self,
        event: UserCompletionEvent,
        source: SyntheticCompletionSource,
        synthetic: UringSyntheticCompletion,
    ) -> UringResult<CompletionFlowOutcome> {
        self.accept_completion_ingress(CompletionIngress::Synthetic { event, source }, synthetic)
    }

    pub(crate) fn accept_completion_anomaly_kind(
        &mut self,
        kind: CompletionAnomalyKind,
        attach: AnomalyAttach,
    ) -> UringResult<CompletionFlowOutcome> {
        self.accept_completion_ingress(
            CompletionIngress::Anomaly { kind, attach },
            UringSyntheticCompletion::None,
        )
    }

    fn accept_completion_ingress(
        &mut self,
        ingress: CompletionIngress<()>,
        synthetic: UringSyntheticCompletion,
    ) -> UringResult<CompletionFlowOutcome> {
        let waker_view = self.waker.hooks_view();
        let sidecar = UringCompletionSidecar {
            pending_cancel_cqes: self.cancellations.in_flight_mut(),
            completion_cleanup_hints: &mut self.completion_cleanup_hints,
        };
        let mut hooks = UringCompletionHooks::new(
            &self.completion_diagnostics,
            sidecar,
            waker_view.buf_len,
            waker_view.armed,
            waker_view.notification_state,
            self.buffer_registry.provided_buffers_mut(),
            synthetic,
        );
        let flow_result = self.ops.accept_completion(
            &self.completion_table,
            &self.completion_diagnostics,
            &mut hooks,
            ingress,
        );
        let post = hooks.into_post_effects();
        let post_result = self.apply_post_completion_effects(post);
        match (flow_result, post_result) {
            (Ok(outcome), Ok(())) => Ok(outcome),
            (Err(flow_error), Ok(())) => Err(flow_error),
            (Ok(_), Err(post_error)) => Err(post_error),
            (Err(flow_error), Err(post_error)) => Err(post_error.with_diag_src_err(flow_error)),
        }
    }

    fn apply_post_completion_effects(
        &mut self,
        post: UringPostCompletionEffects,
    ) -> UringResult<()> {
        let mut first_error = None;
        for (cancel_id, request, raw) in post.cancel_enoent {
            if let Err(report) = self.record_cancel_enoent_if_target_active(cancel_id, request, raw)
                && first_error.is_none()
            {
                first_error = Some(report);
            }
        }

        // The user completion has already been published when this post-effect runs. A cleanup
        // failure must be reported without stopping the remaining Close effects: every successful
        // kernel Close still has to consume its owned handle exactly once.
        for fd in post.close_unregister {
            if let Err(report) = self.unregister_close_owned_fd(fd)
                && first_error.is_none()
            {
                first_error = Some(report);
            }
        }

        if post.rebuild_waker {
            self.completion_diagnostics.backend().inc_waker_rebuild();
            if let Err(report) = self
                .rebuild_waker_fd()
                .attach_note("failed to rebuild eventfd waker")
                && first_error.is_none()
            {
                first_error = Some(report);
            }
        }
        if post.resubmit_waker
            && let Err(e) = self.submit_waker()
        {
            self.completion_diagnostics.backend().inc_waker_rebuild();
            error!(report = ?e, "failed to resubmit waker");
            if first_error.is_none() {
                first_error = Some(e);
            }
        }
        if post.resubmit_waker {
            self.completion_diagnostics.backend().inc_waker_rearm();
        }
        if post.flush_backlog
            && let Err(report) = self.flush_backlog()
            && first_error.is_none()
        {
            first_error = Some(report);
        }
        first_error.map_or(Ok(()), Err)
    }

    fn record_cancel_enoent_if_target_active(
        &mut self,
        cancel_id: CancelCompletionId,
        request: PendingCancel,
        raw: RawCompletion,
    ) -> UringResult<()> {
        let active_target = match self.ops.checked_slot_view(request.target)? {
            CheckedSlotView::Valid(SlotView::InFlightWaiting(slot)) => Some((
                slot.snapshot(),
                "async cancel returned ENOENT while target is still waiting",
            )),
            CheckedSlotView::Valid(SlotView::InFlightOrphaned(slot)) => Some((
                slot.snapshot(),
                "async cancel returned ENOENT while target is still orphaned",
            )),
            _ => None,
        };

        let Some((snapshot, _message)) = active_target else {
            return Ok(());
        };

        self.completion_diagnostics
            .backend()
            .inc_cancel_ack_enoent_active();
        Err(UringError::InvalidState
            .report(
                "record_cancel_enoent_if_target_active",
                "io_uring cancel returned ENOENT but target slot is still active",
            )
            .with_ctx("cancel_id", cancel_id.raw())
            .with_ctx("expected_index", request.target.index())
            .with_ctx("expected_generation", request.target.generation())
            .with_ctx("actual_index", snapshot.index)
            .with_ctx("actual_generation", snapshot.generation)
            .with_ctx("slot_status", format!("{:?}", snapshot.status))
            .with_ctx("raw_cqe_res", raw.res)
            .with_ctx("raw_cqe_flags", raw.flags)
            .attach_note(
                "The io_uring asynchronous cancel operation completed with -ENOENT (indicating \
                 the operation was not found in kernel's pending queue), but the corresponding \
                 user-space I/O slot remains active (InFlightWaiting or InFlightOrphaned). \
                 This state mismatch indicates a potential memory leak or race condition.",
            ))
    }
}

struct KernelCompletionError {
    report: Report<UringError>,
    fallback_cleanup: bool,
}

fn complete_kernel_waiting_slot(
    mut slot: Slot<'_, InFlightWaiting>,
    token: OpToken,
    raw: RawCompletion,
    cqe_env: &mut CqeEnv<'_>,
) -> Result<CompletionHookOutcome<UringSlotSpec, UringBackendEffect>, KernelCompletionError> {
    // `IORING_CQE_F_MORE`：内核声明这个操作还会继续投递完成。flags 的解读到此为止，
    // core 只见 `CompletionContinuation`。
    let continuation = if io_uring::cqueue::more(raw.flags) {
        CompletionContinuation::More
    } else {
        CompletionContinuation::Final
    };

    let (final_res, cleanup, item) = match slot.with_op_and_payload_mut(|op, payload| {
        let final_res = unsafe { (op.vtable.on_complete)(op, payload, raw.res) };
        let cleanup = op.completion_cleanup(raw.res);
        let item = unsafe { (op.vtable.record_item)(op, payload, raw.res, raw.flags, cqe_env) };
        (final_res, cleanup, item)
    }) {
        Ok(result) => result,
        Err(err) => {
            return Err(KernelCompletionError {
                report: UringError::InvalidState.report(
                    "uring.complete_kernel_waiting_slot",
                    format!("slot corruption detected on completion: {:?}", err),
                ),
                fallback_cleanup: true,
            });
        }
    };
    let item = item.map_err(|report| KernelCompletionError {
        report,
        fallback_cleanup: false,
    })?;
    let res_code = driver_result_to_event_res(&final_res);
    let event = UserCompletionEvent::from_parts(COMP_BACKEND_URING, token, res_code, raw.flags);
    let res_is_ok = final_res.is_ok();
    // 完成本身的错误在没有更具体的 `detail` 时充当 detail，与队列化之前逐条等价。
    let mut res_error = final_res.err();

    if continuation.is_more() {
        // slot 原地不动：op 与提交 payload 还要给内核后续的完成用，cell 也必须停在
        // `InFlightWaiting` 才能继续路由。
        let Some(item) = item else {
            return Err(KernelCompletionError {
                report: UringError::InvalidState.report(
                    "uring.complete_kernel_waiting_slot",
                    "kernel reported IORING_CQE_F_MORE for an operation that produces no item",
                ),
                fallback_cleanup: false,
            });
        };
        return Ok(CompletionHookOutcome::User {
            event,
            payload: item,
            detail: res_error.take().map(Err),
            cleanup,
            continuation,
            effect: UringBackendEffect::None,
        });
    }

    let mut completed = slot.complete();
    let (submit_payload, detail) = completed.take_completion_data();
    // multishot 的终态完成同样产出一条 item，提交 payload（监听 socket 之类）到此为止。
    let payload = match item {
        Some(item) => {
            drop(submit_payload);
            item
        }
        None => {
            let Some(payload) = submit_payload else {
                drop(detail);
                return Err(KernelCompletionError {
                    report: UringError::InvalidState.report(
                        "uring.complete_kernel_waiting_slot",
                        "slot payload missing on completion",
                    ),
                    fallback_cleanup: false,
                });
            };
            payload
        }
    };

    let effect = if res_is_ok {
        match &payload {
            UringUserPayload::Close(close) => UringBackendEffect::CloseCompleted { fd: close.fd },
            _ => UringBackendEffect::None,
        }
    } else {
        UringBackendEffect::None
    };

    let detail = detail.or_else(|| res_error.take().map(Err));
    let _ = completed.take_op();

    Ok(CompletionHookOutcome::User {
        event,
        payload,
        detail,
        cleanup,
        continuation,
        effect,
    })
}

fn complete_timer_waiting_slot(
    mut slot: Slot<'_, InFlightWaiting>,
    event: UserCompletionEvent,
) -> UringResult<CompletionHookOutcome<UringSlotSpec, UringBackendEffect>> {
    slot.platform_mut().timer_id = None;
    let mut completed = slot.complete();
    let _ = completed.take_op();
    let (payload, detail) = completed.take_completion_data();
    let Some(payload) = payload else {
        drop(detail);
        return Err(UringError::InvalidState.report(
            "uring.complete_timer_waiting_slot",
            "slot payload missing on timer completion",
        ));
    };

    Ok(CompletionHookOutcome::User {
        event,
        payload,
        detail,
        cleanup: CompletionCleanupGuard::default(),
        // 软件定时器只会触发一次。
        continuation: CompletionContinuation::Final,
        effect: UringBackendEffect::None,
    })
}

fn complete_cancel_waiting_slot(
    slot: Slot<'_, InFlightWaiting>,
    event: UserCompletionEvent,
    mode: CancelMode,
) -> UringResult<CompletionHookOutcome<UringSlotSpec, UringBackendEffect>> {
    complete_local_cancel_slot(slot, event, mode, false)
}

fn complete_submission_failure_slot(
    slot: Slot<'_, InFlightWaiting>,
    event: UserCompletionEvent,
    report: Option<Report<UringError>>,
) -> UringResult<CompletionHookOutcome<UringSlotSpec, UringBackendEffect>> {
    let event_res = event.res();
    let mut completed = slot.complete();
    let cleanup = completed
        .with_op_mut(|op| op.completion_cleanup(event_res))
        .unwrap_or_default();
    let _ = completed.take_op();
    let (payload, detail) = completed.take_completion_data();
    let Some(payload) = payload else {
        drop(detail);
        return Err(UringError::InvalidState.report(
            "uring.complete_submission_failure_slot",
            "slot payload missing on submission failure",
        ));
    };

    Ok(CompletionHookOutcome::User {
        event,
        payload,
        detail: detail.or(report.map(Err)),
        cleanup,
        // 提交失败的操作从未进入内核，不会再有完成。
        continuation: CompletionContinuation::Final,
        effect: UringBackendEffect::None,
    })
}

fn complete_local_cancel_slot(
    slot: Slot<'_, InFlightWaiting>,
    event: UserCompletionEvent,
    mode: CancelMode,
    orphaned: bool,
) -> UringResult<CompletionHookOutcome<UringSlotSpec, UringBackendEffect>> {
    let mut completed = slot.complete();
    let cleanup = completed
        .with_op_mut(|op| {
            if mode == CancelMode::Abandon || orphaned {
                op.orphan_cleanup(event.res())
            } else {
                op.completion_cleanup(event.res())
            }
        })
        .unwrap_or_default();
    let (payload, detail) = completed.take_completion_data();
    let _ = completed.take_op();

    match (mode, payload) {
        (CancelMode::UserVisible, Some(payload)) => Ok(CompletionHookOutcome::User {
            event,
            payload,
            detail,
            cleanup,
            // 本地取消是这个操作的终点，不管它原本是不是 multishot。
            continuation: CompletionContinuation::Final,
            effect: UringBackendEffect::None,
        }),
        (CancelMode::UserVisible, None) => {
            drop(detail);
            Err(UringError::InvalidState.report(
                "uring.complete_local_cancel_slot",
                "slot payload missing on cancel",
            ))
        }
        (CancelMode::Abandon, payload) => {
            drop(payload);
            drop(detail);
            Ok(CompletionHookOutcome::Cleanup {
                cleanup,
                continuation: CompletionContinuation::Final,
                effect: UringBackendEffect::None,
            })
        }
    }
}

/// 一个仍在途的、已被放弃的 multishot 收到中间完成：只取 cleanup，**不掏空 slot**。
///
/// 与 [`cleanup_orphaned_slot`] 的差别就在这里——那个会 `take_op` / `take_completion_data`，
/// 而内核还要用它们继续投递完成。
fn cleanup_orphaned_streaming_slot(
    mut slot: Slot<'_, InFlightOrphaned>,
    cqe_res: i32,
) -> (CompletionCleanupGuard, bool) {
    match slot.with_op_mut(|op| op.orphan_cleanup(cqe_res)) {
        Ok(cleanup) => (cleanup, true),
        Err(_) => (CompletionCleanupGuard::default(), false),
    }
}

fn cleanup_orphaned_slot(
    slot: Slot<'_, InFlightOrphaned>,
    cqe_res: i32,
) -> (CompletionCleanupGuard, bool) {
    let mut completed = slot.complete();
    let (cleanup, slot_accessed) = match completed.with_op_mut(|op| op.orphan_cleanup(cqe_res)) {
        Ok(cleanup) => (cleanup, true),
        Err(_) => (CompletionCleanupGuard::default(), false),
    };
    let (payload, detail) = completed.take_completion_data();
    let _ = completed.take_op();
    drop(payload);
    drop(detail);
    (cleanup, slot_accessed)
}

#[inline]
pub(crate) fn driver_result_to_event_res(res: &UringResult<usize>) -> i32 {
    match res {
        Ok(v) => (*v).min(i32::MAX as usize) as i32,
        Err(e) => uring_report_to_event_res(e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::driver::{ProvidedBufGroup, registration::test_group};
    use crate::op::{
        Accept, AcceptMulti, Open, ReadRaw, Recv, UringOpErasure, UringOpRegistry, WriteRaw,
    };
    use veloq_driver_core::driver::{CompletionToken, SharedCompletionTable};
    use veloq_driver_core::slot::Generation;

    fn test_hooks<'a>(
        diagnostics: &'a DriverCompletionDiagnostics<UringCompletionDiagnostics>,
        pending_cancel_cqes: &'a mut HashMap<CancelCompletionId, PendingCancel>,
        completion_cleanup_hints: &'a mut HashMap<CompletionToken, Option<CompletionCleanupHintFn>>,
        waker_armed: &'a mut bool,
        notification_state: &'a AtomicU8,
    ) -> UringCompletionHooks<'a> {
        let sidecar = UringCompletionSidecar {
            pending_cancel_cqes,
            completion_cleanup_hints,
        };
        UringCompletionHooks::new(
            diagnostics,
            sidecar,
            8,
            waker_armed,
            notification_state,
            None,
            UringSyntheticCompletion::None,
        )
    }

    fn test_hooks_with_buffers<'a>(
        diagnostics: &'a DriverCompletionDiagnostics<UringCompletionDiagnostics>,
        pending_cancel_cqes: &'a mut HashMap<CancelCompletionId, PendingCancel>,
        completion_cleanup_hints: &'a mut HashMap<CompletionToken, Option<CompletionCleanupHintFn>>,
        waker_armed: &'a mut bool,
        notification_state: &'a AtomicU8,
        provided_buffers: Option<&'a mut ProvidedBufGroup>,
    ) -> UringCompletionHooks<'a> {
        let sidecar = UringCompletionSidecar {
            pending_cancel_cqes,
            completion_cleanup_hints,
        };
        UringCompletionHooks::new(
            diagnostics,
            sidecar,
            8,
            waker_armed,
            notification_state,
            provided_buffers,
            UringSyntheticCompletion::None,
        )
    }

    fn accept_corrupt(
        registry: &mut UringOpRegistry,
        sidecar: &mut HashMap<CompletionToken, Option<CompletionCleanupHintFn>>,
        token: OpToken,
        result: i32,
        flags: u32,
    ) -> CompletionFlowOutcome {
        let diagnostics = registry.shared.completion_diagnostics();
        let table: SharedCompletionTable<UringSlotSpec> = registry.shared.clone();
        let mut pending_cancel_cqes = HashMap::default();
        let mut waker_armed = true;
        let notification_state = AtomicU8::new(WAKER_NOTIFIED);
        let mut hooks = test_hooks(
            &diagnostics,
            &mut pending_cancel_cqes,
            sidecar,
            &mut waker_armed,
            &notification_state,
        );
        let envelope = CompletionEnvelope::from_raw_parts(
            COMP_BACKEND_URING,
            CompletionToken::user(token).raw(),
            result,
            flags,
        );
        registry
            .accept_completion(
                &table,
                &diagnostics,
                &mut hooks,
                CompletionIngress::Kernel(envelope),
            )
            .expect("corrupt completion should be handled")
    }

    fn open_test_fds() -> [i32; 2] {
        let mut fds = [-1; 2];
        assert_eq!(unsafe { libc::pipe(fds.as_mut_ptr()) }, 0);
        assert!(fds[0] > 2 && fds[1] > 2);
        fds
    }

    fn assert_closed(fd: i32) {
        assert_eq!(unsafe { libc::fcntl(fd, libc::F_GETFD) }, -1);
    }

    #[test]
    fn waker_control_records_unexpected_byte_count_as_error() {
        let diagnostics = DriverCompletionDiagnostics::<UringCompletionDiagnostics>::default();
        let mut pending_cancel_cqes = HashMap::default();
        let mut completion_cleanup_hints = HashMap::default();
        let mut waker_armed = true;
        let notification_state = AtomicU8::new(WAKER_PROCESSING);
        let mut hooks = test_hooks(
            &diagnostics,
            &mut pending_cancel_cqes,
            &mut completion_cleanup_hints,
            &mut waker_armed,
            &notification_state,
        );
        let raw = RawCompletion::new(COMP_BACKEND_URING, CompletionToken::waker(0), 4, 0);

        let outcome = hooks.handle_waker_control(raw);
        let (error, effect) = match outcome {
            CompletionHookOutcome::Failed { error, effect } => (error, effect),
            _ => panic!("unexpected byte count must produce a failed outcome"),
        };
        hooks
            .finish_backend_effect(effect)
            .expect("waker recovery effect should be recorded");

        assert_eq!(*error.inner(), UringError::CompletionWait);
        let rebuild_waker = hooks.post.rebuild_waker;
        let resubmit_waker = hooks.post.resubmit_waker;
        drop(hooks);
        assert!(!waker_armed);
        assert_eq!(notification_state.load(Ordering::Acquire), WAKER_REARM);
        assert!(rebuild_waker);
        assert!(resubmit_waker);
        assert_eq!(diagnostics.snapshot().backend.waker_error, 1);
    }

    #[test]
    fn waker_control_rearms_successful_completion() {
        let diagnostics = DriverCompletionDiagnostics::<UringCompletionDiagnostics>::default();
        let mut pending_cancel_cqes = HashMap::default();
        let mut completion_cleanup_hints = HashMap::default();
        let mut waker_armed = true;
        let notification_state = AtomicU8::new(WAKER_PROCESSING);
        let mut hooks = test_hooks(
            &diagnostics,
            &mut pending_cancel_cqes,
            &mut completion_cleanup_hints,
            &mut waker_armed,
            &notification_state,
        );
        let raw = RawCompletion::new(COMP_BACKEND_URING, CompletionToken::waker(0), 8, 0);

        let outcome = hooks.handle_waker_control(raw);
        let effect = match outcome {
            CompletionHookOutcome::ControlHandled { effect } => effect,
            _ => panic!("a valid eventfd read must be handled successfully"),
        };
        hooks
            .finish_backend_effect(effect)
            .expect("waker rearm effect should be recorded");

        let resubmit_waker = hooks.post.resubmit_waker;
        let rebuild_waker = hooks.post.rebuild_waker;
        drop(hooks);
        assert!(!waker_armed);
        assert_eq!(notification_state.load(Ordering::Acquire), WAKER_REARM);
        assert!(resubmit_waker);
        assert!(!rebuild_waker);
        assert_eq!(diagnostics.snapshot().backend.waker_ok, 1);
        assert_eq!(diagnostics.snapshot().backend.waker_error, 0);
    }

    #[test]
    fn recoverable_waker_errors_still_rearm() {
        let diagnostics = DriverCompletionDiagnostics::<UringCompletionDiagnostics>::default();
        for res in [-libc::EAGAIN, -libc::EINTR] {
            let mut pending_cancel_cqes = HashMap::default();
            let mut completion_cleanup_hints = HashMap::default();
            let mut waker_armed = true;
            let notification_state = AtomicU8::new(WAKER_PROCESSING);
            let mut hooks = test_hooks(
                &diagnostics,
                &mut pending_cancel_cqes,
                &mut completion_cleanup_hints,
                &mut waker_armed,
                &notification_state,
            );
            let raw = RawCompletion::new(COMP_BACKEND_URING, CompletionToken::waker(0), res, 0);

            let outcome = hooks.handle_waker_control(raw);
            let effect = match outcome {
                CompletionHookOutcome::ControlHandled { effect } => effect,
                _ => panic!("recoverable eventfd errors must be rearmed"),
            };
            hooks
                .finish_backend_effect(effect)
                .expect("waker rearm effect should be recorded");
            drop(hooks);

            assert!(!waker_armed);
            assert_eq!(notification_state.load(Ordering::Acquire), WAKER_REARM);
        }

        let snapshot = diagnostics.snapshot().backend;
        assert_eq!(snapshot.waker_error, 2);
        assert_eq!(snapshot.waker_rearm, 0);
    }

    #[test]
    fn waker_finish_does_not_overwrite_a_concurrent_notification() {
        let diagnostics = DriverCompletionDiagnostics::<UringCompletionDiagnostics>::default();
        let mut pending_cancel_cqes = HashMap::default();
        let mut completion_cleanup_hints = HashMap::default();
        let mut waker_armed = true;
        let notification_state = AtomicU8::new(WAKER_PROCESSING);
        let mut hooks = test_hooks(
            &diagnostics,
            &mut pending_cancel_cqes,
            &mut completion_cleanup_hints,
            &mut waker_armed,
            &notification_state,
        );

        notification_state.store(WAKER_NOTIFIED, Ordering::Release);
        hooks
            .finish_backend_effect(UringBackendEffect::Waker {
                recovery: WakerRecovery::Rearm,
            })
            .expect("waker rearm effect should be recorded");
        drop(hooks);

        assert!(!waker_armed);
        assert_eq!(notification_state.load(Ordering::Acquire), WAKER_NOTIFIED);
    }

    #[test]
    fn wait_budget_keeps_the_explicit_deadline_source() {
        assert_eq!(
            wait_budget(
                Some(Duration::from_secs(2)),
                Some(Duration::from_secs(3)),
                Duration::from_secs(1),
            ),
            WaitBudget {
                duration: Duration::from_secs(1),
                source: WaitBudgetSource::Probe,
            }
        );
        assert_eq!(
            wait_budget(
                Some(Duration::from_millis(3)),
                Some(Duration::from_millis(7)),
                Duration::from_secs(1),
            ),
            WaitBudget {
                duration: Duration::from_millis(3),
                source: WaitBudgetSource::External,
            }
        );
        assert_eq!(
            wait_budget(None, Some(Duration::from_millis(5)), Duration::from_secs(1),),
            WaitBudget {
                duration: Duration::from_millis(5),
                source: WaitBudgetSource::Timer,
            }
        );
    }

    #[test]
    fn untracked_cancel_cqe_is_anomaly_not_user_completion() {
        let diagnostics = DriverCompletionDiagnostics::<UringCompletionDiagnostics>::default();
        let mut pending_cancel_cqes = HashMap::default();
        let mut completion_cleanup_hints = HashMap::default();
        let mut waker_armed = true;
        let notification_state = AtomicU8::new(WAKER_NOTIFIED);
        let mut hooks = test_hooks(
            &diagnostics,
            &mut pending_cancel_cqes,
            &mut completion_cleanup_hints,
            &mut waker_armed,
            &notification_state,
        );
        let cancel_id = CancelCompletionId::new(7);
        let raw = RawCompletion::new(COMP_BACKEND_URING, CompletionToken::cancel(cancel_id), 0, 0);

        let outcome = hooks.handle_cancel_control(cancel_id, raw);

        let err = match outcome {
            CompletionHookOutcome::Failed { error, effect } => {
                assert!(matches!(effect, UringBackendEffect::None));
                error
            }
            _ => panic!("untracked cancel must produce a failed outcome"),
        };
        assert_eq!(*err.inner(), UringError::InvalidState);
    }

    #[test]
    fn raw_fd_cleanup_hint_is_exposed_only_for_fd_producing_ops() {
        assert!(
            <Open as UringOpErasure>::vtable()
                .completion_cleanup_hint
                .is_some()
        );
        assert!(
            <Accept as UringOpErasure>::vtable()
                .completion_cleanup_hint
                .is_some()
        );
        assert!(
            <AcceptMulti as UringOpErasure>::vtable()
                .completion_cleanup_hint
                .is_some()
        );
        assert!(
            <ReadRaw as UringOpErasure>::vtable()
                .completion_cleanup_hint
                .is_none()
        );
        assert!(
            <WriteRaw as UringOpErasure>::vtable()
                .completion_cleanup_hint
                .is_none()
        );
        assert!(
            <Recv as UringOpErasure>::vtable()
                .completion_cleanup_hint
                .is_none()
        );
    }

    #[test]
    fn stale_fd_completion_is_closed_and_final_hint_is_removed() {
        let mut registry = UringOpRegistry::new(1);
        let token = OpToken::from_registry_parts(0, Generation::new(1)).expect("test token");
        let fd_pair = open_test_fds();
        let fd = fd_pair[0];
        let hint = <Open as UringOpErasure>::vtable()
            .completion_cleanup_hint
            .expect("open must expose a cleanup hint");
        let mut sidecar = HashMap::default();
        sidecar.insert(CompletionToken::user(token), Some(hint));

        let outcome = accept_corrupt(&mut registry, &mut sidecar, token, fd, 0);

        assert_eq!(outcome.anomaly, 1);
        assert!(sidecar.is_empty());
        assert_closed(fd);
        unsafe { libc::close(fd_pair[1]) };
        let snapshot = registry.shared.completion_diagnostics().snapshot().backend;
        assert_eq!(snapshot.corrupt_cleanup_attempts, 1);
        assert_eq!(snapshot.corrupt_raw_fd_cleanups, 1);
        assert_eq!(snapshot.corrupt_cleanup_hint_missing, 0);
    }

    #[test]
    fn negative_fd_completion_does_not_close_or_retain_hint() {
        let mut registry = UringOpRegistry::new(1);
        let token = OpToken::from_registry_parts(0, Generation::new(1)).expect("test token");
        let fd_pair = open_test_fds();
        let hint = <Accept as UringOpErasure>::vtable()
            .completion_cleanup_hint
            .expect("accept must expose a cleanup hint");
        let mut sidecar = HashMap::default();
        sidecar.insert(CompletionToken::user(token), Some(hint));

        accept_corrupt(&mut registry, &mut sidecar, token, -libc::ECANCELED, 0);

        assert!(sidecar.is_empty());
        assert_ne!(unsafe { libc::fcntl(fd_pair[0], libc::F_GETFD) }, -1);
        unsafe {
            libc::close(fd_pair[0]);
            libc::close(fd_pair[1]);
        }
        let snapshot = registry.shared.completion_diagnostics().snapshot().backend;
        assert_eq!(snapshot.corrupt_cleanup_attempts, 1);
        assert_eq!(snapshot.corrupt_raw_fd_cleanups, 0);
        assert_eq!(snapshot.corrupt_cleanup_hint_missing, 0);
    }

    #[test]
    fn corrupt_completion_returns_selected_provided_buffer() {
        let mut registry = UringOpRegistry::new(1);
        let token = OpToken::from_registry_parts(0, Generation::new(1)).expect("test token");
        let mut sidecar = HashMap::default();
        let diagnostics = registry.shared.completion_diagnostics();
        let mut pending_cancel_cqes = HashMap::default();
        let mut waker_armed = true;
        let notification_state = AtomicU8::new(WAKER_NOTIFIED);
        let mut provided_buffers = test_group(2);
        let before = provided_buffers.stats();
        let table: SharedCompletionTable<UringSlotSpec> = registry.shared.clone();
        let mut hooks = test_hooks_with_buffers(
            &diagnostics,
            &mut pending_cancel_cqes,
            &mut sidecar,
            &mut waker_armed,
            &notification_state,
            Some(&mut provided_buffers),
        );
        let flags = 1 | (1 << 16);
        let envelope = CompletionEnvelope::from_raw_parts(
            COMP_BACKEND_URING,
            CompletionToken::user(token).raw(),
            7,
            flags,
        );

        let outcome = registry
            .accept_completion(
                &table,
                &diagnostics,
                &mut hooks,
                CompletionIngress::Kernel(envelope),
            )
            .expect("corrupt completion should be handled");

        assert_eq!(outcome.anomaly, 1);
        assert_eq!(provided_buffers.stats().returned, before.returned + 1);
        assert_eq!(provided_buffers.stats().available, before.available);
    }

    #[test]
    fn stale_non_fd_completion_does_not_close_a_positive_result() {
        let mut registry = UringOpRegistry::new(1);
        let token = OpToken::from_registry_parts(0, Generation::new(1)).expect("test token");
        let fd_pair = open_test_fds();
        let fd = fd_pair[0];
        let mut sidecar = HashMap::default();
        sidecar.insert(CompletionToken::user(token), None);

        let outcome = accept_corrupt(&mut registry, &mut sidecar, token, fd, 0);

        assert_eq!(outcome.anomaly, 1);
        assert!(sidecar.is_empty());
        assert_ne!(unsafe { libc::fcntl(fd, libc::F_GETFD) }, -1);
        unsafe {
            libc::close(fd);
            libc::close(fd_pair[1]);
        }
        let snapshot = registry.shared.completion_diagnostics().snapshot().backend;
        assert_eq!(snapshot.corrupt_cleanup_attempts, 0);
        assert_eq!(snapshot.corrupt_cleanup_hint_missing, 0);
    }

    #[test]
    fn missing_fd_cleanup_hint_does_not_guess_close() {
        let mut registry = UringOpRegistry::new(1);
        let token = OpToken::from_registry_parts(0, Generation::new(1)).expect("test token");
        let fd_pair = open_test_fds();

        accept_corrupt(&mut registry, &mut HashMap::default(), token, fd_pair[0], 0);

        assert_ne!(unsafe { libc::fcntl(fd_pair[0], libc::F_GETFD) }, -1);
        unsafe {
            libc::close(fd_pair[0]);
            libc::close(fd_pair[1]);
        }
        let snapshot = registry.shared.completion_diagnostics().snapshot().backend;
        assert_eq!(snapshot.corrupt_cleanup_attempts, 0);
        assert_eq!(snapshot.corrupt_raw_fd_cleanups, 0);
        assert_eq!(snapshot.corrupt_cleanup_hint_missing, 1);
    }

    #[test]
    fn multishot_corrupt_completions_close_each_fd_and_retain_hint_until_final() {
        const CQE_MORE: u32 = 2;

        let mut registry = UringOpRegistry::new(1);
        let token = OpToken::from_registry_parts(0, Generation::new(1)).expect("test token");
        let first_pair = open_test_fds();
        let second_pair = open_test_fds();
        let hint = <AcceptMulti as UringOpErasure>::vtable()
            .completion_cleanup_hint
            .expect("accept multi must expose a cleanup hint");
        let mut sidecar = HashMap::default();
        sidecar.insert(CompletionToken::user(token), Some(hint));

        let first = accept_corrupt(&mut registry, &mut sidecar, token, first_pair[0], CQE_MORE);
        assert_eq!(first.anomaly, 1);
        assert_eq!(sidecar.len(), 1);
        assert_closed(first_pair[0]);

        let final_outcome = accept_corrupt(&mut registry, &mut sidecar, token, second_pair[0], 0);
        assert_eq!(final_outcome.anomaly, 1);
        assert!(sidecar.is_empty());
        assert_closed(second_pair[0]);
        unsafe {
            libc::close(first_pair[1]);
            libc::close(second_pair[1]);
        }
        let snapshot = registry.shared.completion_diagnostics().snapshot().backend;
        assert_eq!(snapshot.corrupt_cleanup_attempts, 2);
        assert_eq!(snapshot.corrupt_raw_fd_cleanups, 2);
    }

    #[test]
    fn generation_reuse_keeps_old_and_new_cleanup_hints_separate() {
        let mut registry = UringOpRegistry::new(1);
        let old_token = OpToken::from_registry_parts(0, Generation::new(1)).expect("old token");
        let new_token = OpToken::from_registry_parts(0, Generation::new(2)).expect("new token");
        let old_pair = open_test_fds();
        let new_pair = open_test_fds();
        let hint = <Accept as UringOpErasure>::vtable()
            .completion_cleanup_hint
            .expect("accept must expose a cleanup hint");
        let mut sidecar = HashMap::default();
        sidecar.insert(CompletionToken::user(old_token), Some(hint));
        sidecar.insert(CompletionToken::user(new_token), Some(hint));

        accept_corrupt(&mut registry, &mut sidecar, old_token, old_pair[0], 0);
        assert_closed(old_pair[0]);
        assert_eq!(sidecar.len(), 1);
        assert!(sidecar.contains_key(&CompletionToken::user(new_token)));

        accept_corrupt(&mut registry, &mut sidecar, new_token, new_pair[0], 0);
        assert_closed(new_pair[0]);
        assert!(sidecar.is_empty());
        unsafe {
            libc::close(old_pair[1]);
            libc::close(new_pair[1]);
        }
    }
}
