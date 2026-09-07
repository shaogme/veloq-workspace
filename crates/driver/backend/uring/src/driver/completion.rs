use std::{
    collections::HashMap,
    mem,
    num::NonZeroU8,
    sync::atomic::{AtomicBool, Ordering},
    time::Instant,
};

use diagweave::prelude::*;
use tracing::{debug, error, trace, warn};

use crate::{
    config::IoFd,
    diagnostics::UringCompletionDiagnostics,
    driver::{CqeEnv, PendingCancel, ProvidedBufGroup, UringDriver},
    error::{UringError, UringResult, uring_report_to_event_res},
    op::{Slot, UringSlotSpec, UringUserPayload},
};
use veloq_driver_core::{
    driver::{
        AnomalyAttach, CancelCompletionId, CancelMode, CompletionAnomalyKind, CompletionBackend,
        CompletionBackendHooks, CompletionCleanupGuard, CompletionContinuation, CompletionControl,
        CompletionEnvelope, CompletionFlowExt, CompletionFlowOutcome, CompletionHookOutcome,
        CompletionIngress, CompletionSource, Driver, DriverCompletionDiagnostics, OpToken,
        PlatformOp, RawCompletion, SyntheticCompletionSource, UserCompletionEvent,
    },
    slot::{CheckedSlotView, InFlightOrphaned, InFlightWaiting, SlotRegistryExt, SlotView},
};

pub(crate) type CompletionProgress = CompletionFlowOutcome;
pub(crate) const COMP_BACKEND_URING: CompletionBackend =
    CompletionBackend::Backend(match NonZeroU8::new(2) {
        Some(val) => val,
        None => unreachable!(),
    });

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
        should_rebuild: bool,
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

impl Default for UringBackendEffect {
    #[inline]
    fn default() -> Self {
        Self::None
    }
}

struct UringCompletionHooks<'a> {
    diagnostics: &'a DriverCompletionDiagnostics<UringCompletionDiagnostics>,
    pending_cancel_cqes: &'a mut HashMap<CancelCompletionId, PendingCancel>,
    waker_buf_len: usize,
    waker_armed: &'a mut bool,
    is_waked: &'a AtomicBool,
    provided_buffers: Option<&'a mut ProvidedBufGroup>,
    synthetic: UringSyntheticCompletion,
    post: UringPostCompletionEffects,
}

impl<'a> UringCompletionHooks<'a> {
    fn new(
        diagnostics: &'a DriverCompletionDiagnostics<UringCompletionDiagnostics>,
        pending_cancel_cqes: &'a mut HashMap<CancelCompletionId, PendingCancel>,
        waker_buf_len: usize,
        waker_armed: &'a mut bool,
        is_waked: &'a AtomicBool,
        provided_buffers: Option<&'a mut ProvidedBufGroup>,
        synthetic: UringSyntheticCompletion,
    ) -> Self {
        Self {
            diagnostics,
            pending_cancel_cqes,
            waker_buf_len,
            waker_armed,
            is_waked,
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

    fn handle_waker_control(&mut self, raw: RawCompletion) -> UringBackendEffect {
        let mut should_rebuild = false;
        if raw.res == self.waker_buf_len as i32 {
            self.diagnostics.backend().inc_waker_ok();
        } else if raw.res >= 0 {
            self.diagnostics.backend().inc_waker_error();
            warn!(
                res = raw.res,
                expected = self.waker_buf_len,
                "eventfd waker read returned unexpected byte count"
            );
            should_rebuild = true;
        } else {
            self.diagnostics.backend().inc_waker_error();
            match -raw.res {
                libc::EAGAIN | libc::EINTR => {
                    debug!(res = raw.res, "recoverable eventfd waker read completion");
                }
                errno => {
                    warn!(res = raw.res, errno, "eventfd waker read failed");
                    should_rebuild = true;
                }
            }
        }

        UringBackendEffect::Waker { should_rebuild }
    }

    fn handle_cancel_control(
        &mut self,
        cancel_id: CancelCompletionId,
        raw: RawCompletion,
    ) -> UringResult<CompletionHookOutcome<UringSlotSpec, UringBackendEffect>> {
        let request = self.pending_cancel_cqes.remove(&cancel_id);
        let Some(request) = request else {
            return Err(UringError::InvalidState.report(
                "uring.completion.handle_cancel_control",
                format!(
                    "async cancel completion had no pending request for cancel_id: {}",
                    cancel_id.raw()
                ),
            ));
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
                Ok(CompletionHookOutcome::ControlHandled {
                    effect: UringBackendEffect::None,
                })
            }
            value if value == -libc::ENOENT => {
                self.diagnostics.backend().inc_cancel_ack_not_found();
                debug!(
                    cancel_id = cancel_id.raw(),
                    request = ?request,
                    "async cancel target was already complete or absent"
                );
                Ok(CompletionHookOutcome::ControlHandled {
                    effect: UringBackendEffect::CancelEnoent {
                        cancel_id,
                        request,
                        raw,
                    },
                })
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
                Ok(CompletionHookOutcome::ControlHandled {
                    effect: UringBackendEffect::None,
                })
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
    ) -> UringResult<CompletionHookOutcome<UringSlotSpec, Self::BackendEffect>> {
        match control {
            CompletionControl::Waker { raw, .. } => Ok(CompletionHookOutcome::ControlHandled {
                effect: self.handle_waker_control(raw),
            }),
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
                complete_kernel_waiting_slot(slot, event.token(), raw, &mut self.cqe_env())
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
        let flags = event.raw().flags;
        self.cqe_env().return_provided_buf(flags);
        Ok(CompletionHookOutcome::Anomaly {
            kind,
            attach: AnomalyAttach::from_raw_completion(event.raw()),
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
        let cleanup = if continuation.is_more() {
            cleanup_orphaned_streaming_slot(slot, res)
        } else {
            cleanup_orphaned_slot(slot, res)
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
            UringBackendEffect::Waker { should_rebuild } => {
                *self.waker_armed = false;
                self.is_waked.store(false, Ordering::Release);
                self.post.rebuild_waker |= should_rebuild;
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
    pub(crate) fn wait_internal(&mut self) -> UringResult<()> {
        let _ = self.drain_cancel_requests()?;
        self.flush_cancellations()?;
        self.flush_backlog()?;

        if !self.has_active_ops_internal() {
            return Ok(());
        }

        if self.ring.completion().is_empty() {
            let next_timeout = self.timers.next_timeout();

            if let Some(duration) = next_timeout {
                let ts = io_uring::types::Timespec::new()
                    .sec(duration.as_secs())
                    .nsec(duration.subsec_nanos());

                let args = io_uring::types::SubmitArgs::new().timespec(&ts);
                match self.ring.submitter().submit_with_args(1, &args) {
                    Ok(_) => {}
                    Err(ref e) if e.raw_os_error() == Some(libc::ETIME) => {}
                    Err(e) => {
                        return Err(UringError::CompletionWait
                            .io_report("driver.wait_internal.submit_with_args", e));
                    }
                }
            } else {
                self.ring.submit_and_wait(1).map_err(|e| {
                    UringError::CompletionWait.io_report("driver.wait_internal.submit_and_wait", e)
                })?;
            }
        }

        self.advance_timer_clock()?;

        let progress = self.process_completions_internal()?;
        let _ = progress.semantic_count();
        self.flush_cancellations()?;
        self.flush_backlog()?;
        Ok(())
    }

    /// Advances the timer wheel by however many whole ticks elapsed since the last poll.
    fn advance_timer_clock(&mut self) -> UringResult<()> {
        let now = Instant::now();
        let expired = self.timers.advance_timer_wheel(now).to_vec();
        for &token in &expired {
            let event = UserCompletionEvent::from_parts(COMP_BACKEND_URING, token, 0, 0);
            let _ = self.accept_synthetic_completion(
                event,
                SyntheticCompletionSource::Timer,
                UringSyntheticCompletion::None,
            );
        }
        Ok(())
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
        let _ = unsafe {
            self.ring
                .submitter()
                .enter::<()>(0, 0, 1 /* IORING_ENTER_GETEVENTS */, None)
        };

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
                cqes.push((cqe.user_data(), cqe.result(), cqe.flags()));
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
        let mut hooks = UringCompletionHooks::new(
            &self.completion_diagnostics,
            self.cancellations.in_flight_mut(),
            waker_view.buf_len,
            waker_view.armed,
            waker_view.is_waked,
            self.buffer_registry.provided_buffers_mut(),
            synthetic,
        );
        let outcome = self.ops.accept_completion(
            &self.completion_table,
            &self.completion_diagnostics,
            &mut hooks,
            ingress,
        )?;
        let post = hooks.into_post_effects();
        self.apply_post_completion_effects(post)?;
        Ok(outcome)
    }

    fn apply_post_completion_effects(
        &mut self,
        post: UringPostCompletionEffects,
    ) -> UringResult<()> {
        for (cancel_id, request, raw) in post.cancel_enoent {
            self.record_cancel_enoent_if_target_active(cancel_id, request, raw)?;
        }

        for fd in post.close_unregister {
            self.unregister_close_owned_fd(fd)?;
        }

        if post.rebuild_waker {
            self.completion_diagnostics.backend().inc_waker_rebuild();
            self.rebuild_waker_fd()
                .attach_note("failed to rebuild eventfd waker")?;
        }
        if post.resubmit_waker
            && let Err(e) = self.submit_waker()
        {
            self.completion_diagnostics.backend().inc_waker_rebuild();
            error!(report = ?e, "failed to resubmit waker");
            return Err(e);
        }
        if post.flush_backlog {
            self.flush_backlog()?;
        }
        Ok(())
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

fn complete_kernel_waiting_slot(
    mut slot: Slot<'_, InFlightWaiting>,
    token: OpToken,
    raw: RawCompletion,
    cqe_env: &mut CqeEnv<'_>,
) -> UringResult<CompletionHookOutcome<UringSlotSpec, UringBackendEffect>> {
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
            return Err(UringError::InvalidState.report(
                "uring.complete_kernel_waiting_slot",
                format!("slot corruption detected on completion: {:?}", err),
            ));
        }
    };
    let item = item?;
    let res_code = driver_result_to_event_res(&final_res);
    let event = UserCompletionEvent::from_parts(COMP_BACKEND_URING, token, res_code, raw.flags);
    let res_is_ok = final_res.is_ok();
    // 完成本身的错误在没有更具体的 `detail` 时充当 detail，与队列化之前逐条等价。
    let mut res_error = final_res.err();

    if continuation.is_more() {
        // slot 原地不动：op 与提交 payload 还要给内核后续的完成用，cell 也必须停在
        // `InFlightWaiting` 才能继续路由。
        let Some(item) = item else {
            return Err(UringError::InvalidState.report(
                "uring.complete_kernel_waiting_slot",
                "kernel reported IORING_CQE_F_MORE for an operation that produces no item",
            ));
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
                return Err(UringError::InvalidState.report(
                    "uring.complete_kernel_waiting_slot",
                    "slot payload missing on completion",
                ));
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
) -> CompletionCleanupGuard {
    slot.with_op_mut(|op| op.orphan_cleanup(cqe_res))
        .unwrap_or_default()
}

fn cleanup_orphaned_slot(slot: Slot<'_, InFlightOrphaned>, cqe_res: i32) -> CompletionCleanupGuard {
    let mut completed = slot.complete();
    let cleanup = completed
        .with_op_mut(|op| op.orphan_cleanup(cqe_res))
        .unwrap_or_default();
    let (payload, detail) = completed.take_completion_data();
    let _ = completed.take_op();
    drop(payload);
    drop(detail);
    cleanup
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
    use veloq_driver_core::driver::CompletionToken;

    fn test_hooks<'a>(
        diagnostics: &'a DriverCompletionDiagnostics<UringCompletionDiagnostics>,
        pending_cancel_cqes: &'a mut HashMap<CancelCompletionId, PendingCancel>,
        waker_armed: &'a mut bool,
        is_waked: &'a AtomicBool,
    ) -> UringCompletionHooks<'a> {
        UringCompletionHooks::new(
            diagnostics,
            pending_cancel_cqes,
            8,
            waker_armed,
            is_waked,
            None,
            UringSyntheticCompletion::None,
        )
    }

    #[test]
    fn waker_control_records_unexpected_byte_count_as_error() {
        let diagnostics = DriverCompletionDiagnostics::<UringCompletionDiagnostics>::default();
        let mut pending_cancel_cqes = HashMap::new();
        let mut waker_armed = true;
        let is_waked = AtomicBool::new(true);
        let mut hooks = test_hooks(
            &diagnostics,
            &mut pending_cancel_cqes,
            &mut waker_armed,
            &is_waked,
        );
        let raw = RawCompletion::new(COMP_BACKEND_URING, CompletionToken::waker(0), 4, 0);

        let effect = hooks.handle_waker_control(raw);

        assert!(matches!(
            effect,
            UringBackendEffect::Waker {
                should_rebuild: true
            }
        ));
        assert_eq!(diagnostics.snapshot().backend.waker_error, 1);
    }

    #[test]
    fn untracked_cancel_cqe_is_anomaly_not_user_completion() {
        let diagnostics = DriverCompletionDiagnostics::<UringCompletionDiagnostics>::default();
        let mut pending_cancel_cqes = HashMap::new();
        let mut waker_armed = true;
        let is_waked = AtomicBool::new(true);
        let mut hooks = test_hooks(
            &diagnostics,
            &mut pending_cancel_cqes,
            &mut waker_armed,
            &is_waked,
        );
        let cancel_id = CancelCompletionId::new(7);
        let raw = RawCompletion::new(COMP_BACKEND_URING, CompletionToken::cancel(cancel_id), 0, 0);

        let outcome = hooks.handle_cancel_control(cancel_id, raw);

        let err = outcome.err().expect("outcome should be Err");
        assert_eq!(*err.inner(), UringError::InvalidState);
    }
}
