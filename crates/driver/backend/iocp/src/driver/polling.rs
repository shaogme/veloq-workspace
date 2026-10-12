use veloq_std::{
    format, mem,
    sync::Arc,
    sync::atomic::{AtomicU8, Ordering},
    time::{Duration, Instant},
    vec::Vec,
};

use diagweave::prelude::*;
use veloq_buf::BufferRegistrar;
use veloq_driver_core::driver::{
    AnomalyAttach, CompletionAnomalyKind, CompletionEnvelope, CompletionIdentity, CompletionToken,
    OpToken, RawCompletion, RemoteWaker, SharedCompletionTable,
};
use veloq_wheel::{Expired, TimerError, TimerId, Wheel, WheelConfig};

use crate::{
    common::{
        IocpErrorContext, IocpWaker, WAKER_IDLE, WAKER_NOTIFIED, WAKER_PROCESSING, WAKER_REARM,
        iocp_msg,
    },
    driver::{
        cancellation::CancellationService,
        completion::{COMP_BACKEND_IOCP, CompletionService},
        registration::RegistrationService,
        state::{IocpDriverState, PollingStateView},
    },
    error::{IocpError, IocpResult},
    op::{IocpSlotSpec, OverlappedEntry},
    rio::CompletionPollArgs,
    win32::{CompletionBatch, CompletionStatus, IoCompletionPort, Overlapped},
};

use super::{RIO_EVENT_KEY, RIO_EVENT_TOKEN};

/// 每次 `GetQueuedCompletionStatusEx` 出队的完成数，与 `MAX_RIO_RESULTS` 一致。
/// Number of completions dequeued per `GetQueuedCompletionStatusEx`, matching `MAX_RIO_RESULTS`.
const MAX_IOCP_BATCH: usize = 128;
const WAKE_FAILURE_PROBE_INTERVAL: Duration = Duration::from_secs(1);

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

pub(super) struct CompletionPump {
    port: Arc<IoCompletionPort>,
    notification_state: Arc<AtomicU8>,
    table: SharedCompletionTable<IocpSlotSpec>,
    batch: CompletionBatch,
}

impl CompletionPump {
    pub(super) fn new(port: IoCompletionPort, table: SharedCompletionTable<IocpSlotSpec>) -> Self {
        Self {
            port: Arc::new(port),
            notification_state: Arc::new(AtomicU8::new(WAKER_IDLE)),
            table,
            batch: CompletionBatch::with_capacity(MAX_IOCP_BATCH),
        }
    }

    /// 批量取出完成项，并返回可通过 [`Self::status`] 读取的数量。
    /// Dequeues completions in a batch and returns the count available through [`Self::status`].
    ///
    /// 批次缓冲区由 pump 持有，排空时无需分配；读取完当前批次后才能请求下一批。
    /// The pump owns the batch buffer to avoid allocations while draining; callers must finish reading before requesting another batch.
    pub(super) fn fill_batch(&mut self, wait_ms: u32) -> IocpResult<usize> {
        self.port.get_status_batch(&mut self.batch, wait_ms)
    }

    pub(super) fn status(&self, index: usize) -> Option<CompletionStatus> {
        self.batch.status(index)
    }

    pub(super) fn port_arc(&self) -> Arc<IoCompletionPort> {
        self.port.clone()
    }

    pub(super) fn table(&self) -> &SharedCompletionTable<IocpSlotSpec> {
        &self.table
    }

    pub(super) fn completion_table(&self) -> SharedCompletionTable<IocpSlotSpec> {
        self.table.clone()
    }

    pub(super) fn begin_notification(&self) {
        loop {
            match self.notification_state.load(Ordering::Acquire) {
                WAKER_NOTIFIED => {
                    if self
                        .notification_state
                        .compare_exchange(
                            WAKER_NOTIFIED,
                            WAKER_PROCESSING,
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        )
                        .is_ok()
                    {
                        return;
                    }
                }
                WAKER_IDLE => {
                    if self
                        .notification_state
                        .compare_exchange(
                            WAKER_IDLE,
                            WAKER_PROCESSING,
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        )
                        .is_ok()
                    {
                        return;
                    }
                }
                WAKER_PROCESSING | WAKER_REARM => return,
                _ => unreachable!("invalid IOCP waker state"),
            }
        }
    }

    pub(super) fn clear_notification(&self) -> IocpResult<bool> {
        loop {
            match self.notification_state.load(Ordering::Acquire) {
                WAKER_PROCESSING => {
                    if self
                        .notification_state
                        .compare_exchange(
                            WAKER_PROCESSING,
                            WAKER_IDLE,
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        )
                        .is_ok()
                    {
                        return Ok(false);
                    }
                }
                WAKER_REARM => {
                    if self
                        .notification_state
                        .compare_exchange(
                            WAKER_REARM,
                            WAKER_NOTIFIED,
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        )
                        .is_ok()
                    {
                        if let Err(error) = self.port.notify(CompletionToken::waker(0)) {
                            self.notification_state
                                .compare_exchange(
                                    WAKER_NOTIFIED,
                                    WAKER_IDLE,
                                    Ordering::AcqRel,
                                    Ordering::Acquire,
                                )
                                .ok();
                            return Err(error
                                .push_ctx("scope", "iocp/driver.polling")
                                .attach_note("failed to rearm remote waker"));
                        }
                        return Ok(true);
                    }
                }
                WAKER_NOTIFIED => {
                    if self
                        .notification_state
                        .compare_exchange(
                            WAKER_NOTIFIED,
                            WAKER_IDLE,
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        )
                        .is_ok()
                    {
                        return Ok(false);
                    }
                }
                WAKER_IDLE => return Ok(false),
                _ => unreachable!("invalid IOCP waker state"),
            }
        }
    }

    pub(super) fn create_waker(&self) -> Arc<dyn RemoteWaker<IocpError>> {
        unsafe {
            Arc::new_unsized(
                IocpWaker {
                    port: self.port.clone(),
                    notification_state: self.notification_state.clone(),
                },
                |p| p as *const dyn RemoteWaker<IocpError>,
            )
        }
    }

    fn mark_waker_notifications(&self, count: usize) {
        let waker = CompletionToken::waker(0).raw() as usize;
        for index in 0..count {
            if let Some(status) = self.batch.status(index)
                && status.overlapped.is_null()
                && status.key == waker
            {
                self.begin_notification();
            }
        }
    }
}

pub(crate) struct TimerEngine {
    wheel: Wheel<OpToken>,
    buffer: Vec<Expired<OpToken>>,
    last_poll: Instant,
}

impl TimerEngine {
    pub(super) fn new() -> Self {
        Self {
            wheel: Wheel::new(WheelConfig::default()),
            buffer: Vec::new(),
            last_poll: Instant::now(),
        }
    }

    pub(super) fn next_deadline(&self) -> Result<Option<Duration>, TimerError> {
        self.wheel.next_deadline()
    }

    pub(super) fn insert(
        &mut self,
        token: OpToken,
        duration: Duration,
    ) -> Result<TimerId, TimerError> {
        self.wheel.insert(token, duration)
    }

    pub(super) fn cancel(&mut self, id: TimerId) {
        let _ = self.wheel.cancel(id);
    }

    pub(super) fn advance_to(&mut self, now: Instant) -> Result<(), TimerError> {
        let elapsed = now.saturating_duration_since(self.last_poll);
        self.wheel.advance_by(elapsed, &mut self.buffer)?;
        self.last_poll = now;
        Ok(())
    }

    pub(super) fn take_buffer(&mut self) -> Vec<Expired<OpToken>> {
        mem::take(&mut self.buffer)
    }

    pub(super) fn restore_cleared_buffer(&mut self, mut buffer: Vec<Expired<OpToken>>) {
        buffer.clear();
        self.buffer = buffer;
    }
}

pub(super) struct PollingService<'view, 'state, 'registrar> {
    state: &'view mut PollingStateView<'state>,
    registrar: &'registrar dyn BufferRegistrar,
}

impl<'view, 'state, 'registrar> PollingService<'view, 'state, 'registrar> {
    pub(super) fn new(
        state: &'view mut PollingStateView<'state>,
        registrar: &'registrar dyn BufferRegistrar,
    ) -> Self {
        Self { state, registrar }
    }

    fn state(&self) -> &PollingStateView<'state> {
        self.state
    }

    fn state_mut(&mut self) -> &mut PollingStateView<'state> {
        self.state
    }

    /// 排空 fast close 已孤儿化的状态，不发布用户完成事件。RIO slot 和 payload 在 boxed IOCP 状态交给此 reaper 前已转交 RIO 延迟所有者。
    /// Drains state orphaned by fast close without publishing user completions. RIO slots and payloads move to the RIO deferred owner before this reaper receives the boxed IOCP state.
    pub(super) fn drain_orphaned(&mut self, timeout: Duration) -> IocpResult<()> {
        let deadline = Instant::now().checked_add(timeout).ok_or_else(|| {
            IocpError::CompletionWait
                .to_report()
                .push_ctx("scope", "iocp/reaper")
                .attach_note("deferred IOCP drain timeout is too large")
        })?;
        let mut first_error = None;

        loop {
            self.drain_deferred_socket_cleanup();
            if !self.state().ops.has_active_ops()
                && self.state().rio.state().rio_outstanding_count() == 0
                && !self.state().rio.state().has_socket_inflight()
                && self.state().handles.deferred_cleanup_len() == 0
            {
                return first_error.map_or(Ok(()), Err);
            }

            let now = Instant::now();
            if now >= deadline {
                return Err(first_error.unwrap_or_else(|| {
                    IocpError::CompletionWait.report("iocp/reaper", "deferred IOCP drain timed out")
                }));
            }

            let count = self
                .state_mut()
                .completion
                .fill_batch(duration_to_wait_ms(deadline.saturating_duration_since(now)))
                .push_ctx("scope", "iocp/reaper")
                .attach_note("failed to poll deferred IOCP completion")?;
            for index in 0..count {
                if let Err(error) = self.handle_orphaned_batch_entry(index)
                    && first_error.is_none()
                {
                    first_error = Some(error);
                }
            }

            if self.state().rio.state().rio_outstanding_count() > 0 {
                let remaining = deadline.saturating_duration_since(Instant::now());
                if let Err(error) = self
                    .state_mut()
                    .rio
                    .state_mut()
                    .drain_outstanding(remaining)
                    .trans()
                    && first_error.is_none()
                {
                    first_error = Some(error);
                }
            }
        }
    }

    fn handle_orphaned_batch_entry(&mut self, index: usize) -> IocpResult<usize> {
        let Some(CompletionStatus {
            bytes,
            key,
            overlapped,
            success,
            error_code,
        }) = self.state().completion.status(index)
        else {
            return Ok(0);
        };
        let res = iocp_status_res(success, error_code, bytes);
        let flags = iocp_status_flags(success, error_code);
        match classify_completion_status(key, overlapped, success) {
            IocpCompletionStatusKind::RioWake => Ok(0),
            IocpCompletionStatusKind::OverlappedUser { queue_key } => {
                let envelope =
                    self.resolve_overlapped_user_envelope(queue_key, overlapped, res, flags)?;
                self.process_completion_envelope(envelope)
            }
            IocpCompletionStatusKind::ControlKey | IocpCompletionStatusKind::PostedToken => Ok(0),
            IocpCompletionStatusKind::NullFailure => Err(iocp_msg(
                IocpErrorContext::CompletionWait,
                "deferred IOCP completion reported a failure with null overlapped",
            )
            .with_ctx("os_error_code", error_code.unwrap_or(0))
            .with_ctx("completion_key", key)),
            IocpCompletionStatusKind::Unknown => Ok(0),
        }
    }

    pub(super) fn poll_completion(&mut self, timeout: Duration) -> IocpResult<usize> {
        let count = self
            .state_mut()
            .completion
            .fill_batch(duration_to_wait_ms(timeout))
            .push_ctx("scope", "iocp/driver")
            .attach_note("failed to poll IOCP status")?;
        self.state().completion.mark_waker_notifications(count);

        let mut drained = 0usize;
        let mut first_error = None;
        for index in 0..count {
            match self.handle_batch_entry(index) {
                Ok(handled) => drained += handled,
                Err(report) => {
                    if first_error.is_none() {
                        first_error = Some(report);
                    }
                }
            }
        }

        first_error.map_or(Ok(drained), Err)
    }

    /// 从 I/O completion port 获取完成事件。
    /// Retrieves completion events from the I/O completion port.
    pub(super) fn get_completion(&mut self, timeout: Option<Duration>) -> IocpResult<()> {
        self.drain_cancel_requests()?;
        self.state_mut()
            .timer
            .advance_to(Instant::now())
            .map_err(|error| {
                IocpError::InvalidState
                    .report("iocp.timer.advance", "timer wheel could not advance")
                    .with_ctx("timer_error", format!("{error:?}"))
            })?;
        self.process_timers()?;
        let ready_completion = self.state().ops.shared.has_ready_completion();
        let timer_deadline = self.state().timer.next_deadline().map_err(|error| {
            IocpError::InvalidState
                .report(
                    "iocp.timer.deadline",
                    "timer wheel deadline could not be queried",
                )
                .with_ctx("timer_error", format!("{error:?}"))
        })?;
        let budget = wait_budget(timeout, timer_deadline, WAKE_FAILURE_PROBE_INTERVAL);
        let wait_ms = if ready_completion {
            self.state()
                .completion_diagnostics
                .backend()
                .inc_wait_ready_preflight();
            0
        } else {
            duration_to_wait_ms(budget.duration)
        };
        if wait_ms == 0 {
            self.state()
                .completion_diagnostics
                .backend()
                .inc_wait_zero();
        } else {
            self.state()
                .completion_diagnostics
                .backend()
                .inc_wait_block();
        }

        let batched = self.state_mut().completion.fill_batch(wait_ms);
        let count = batched
            .attach_note("failed to get IOCP completion status")
            .trans()?;
        let mut timed_out_source = None;
        if count == 0 && wait_ms != 0 {
            self.state()
                .completion_diagnostics
                .backend()
                .inc_wait_timeout();
            timed_out_source = Some(budget.source);
            match budget.source {
                WaitBudgetSource::External => self
                    .state()
                    .completion_diagnostics
                    .backend()
                    .inc_wait_external_timeout(),
                WaitBudgetSource::Timer => self
                    .state()
                    .completion_diagnostics
                    .backend()
                    .inc_wait_timer_return(),
                WaitBudgetSource::Probe => self
                    .state()
                    .completion_diagnostics
                    .backend()
                    .inc_wait_probe_return(),
            }
        }
        self.state().completion.mark_waker_notifications(count);

        // 即使某个条目处理失败也继续路由整批，避免丢弃其余完成项；最后报告首个错误。
        // Route the whole batch despite individual failures so other completions are not dropped, then report the first error.
        let mut first_error = None;
        let mut handled = 0;
        for index in 0..count {
            match self.handle_batch_entry(index) {
                Ok(value) => handled += value,
                Err(report) => {
                    if first_error.is_none() {
                        first_error = Some(report);
                    }
                }
            }
        }

        self.state_mut()
            .timer
            .advance_to(Instant::now())
            .map_err(|error| {
                IocpError::InvalidState
                    .report("iocp.timer.advance", "timer wheel could not advance")
                    .with_ctx("timer_error", format!("{error:?}"))
            })?;
        let timer_count = self.process_timers()?;
        if handled > 0 {
            self.state()
                .completion_diagnostics
                .backend()
                .inc_wait_completion_return();
        }
        if timer_count > 0 && timed_out_source != Some(WaitBudgetSource::Timer) {
            self.state()
                .completion_diagnostics
                .backend()
                .inc_wait_timer_return();
        }
        first_error.map_or(Ok(()), Err)
    }

    /// 路由 completion pump 当前批次中的第 `index` 个条目。
    /// Routes entry `index` in the batch currently held by the completion pump.
    fn handle_batch_entry(&mut self, index: usize) -> IocpResult<usize> {
        let Some(CompletionStatus {
            bytes,
            key,
            overlapped,
            success,
            error_code,
        }) = self.state().completion.status(index)
        else {
            return Ok(0);
        };
        self.handle_completion_status(bytes, key, overlapped, success, error_code)
    }

    fn handle_completion_status(
        &mut self,
        bytes: u32,
        key: usize,
        overlapped: *mut Overlapped,
        success: bool,
        error_code: Option<u32>,
    ) -> IocpResult<usize> {
        let res = iocp_status_res(success, error_code, bytes);
        let flags = iocp_status_flags(success, error_code);
        match classify_completion_status(key, overlapped, success) {
            IocpCompletionStatusKind::RioWake => {
                let registrar = self.registrar;
                let state = self.state_mut();
                {
                    let (rio_state, registrar) = state.rio.state_and_registrar_mut(registrar);
                    rio_state.process_completions(CompletionPollArgs {
                        ops: state.ops,
                        ext: state.extensions,
                        registrar,
                        completion_table: state.completion.table(),
                        diagnostics: state.completion_diagnostics,
                    })
                }
                .inspect(|_| {
                    self.drain_deferred_socket_cleanup();
                })
                .push_ctx("scope", "iocp/driver")
                .attach_note("failed to process rio completions")?;
                Ok(0)
            }
            IocpCompletionStatusKind::OverlappedUser { queue_key } => {
                let envelope =
                    self.resolve_overlapped_user_envelope(queue_key, overlapped, res, flags)?;
                self.process_completion_envelope(envelope)
            }
            IocpCompletionStatusKind::ControlKey | IocpCompletionStatusKind::PostedToken => {
                self.accept_raw_completion(key as u64, res, flags)?;
                Ok(1)
            }
            IocpCompletionStatusKind::NullFailure => Err(iocp_msg(
                IocpErrorContext::CompletionWait,
                "GetQueuedCompletionStatusEx reported a failure with null overlapped",
            )
            .with_ctx("os_error_code", error_code.unwrap_or(0))
            .with_ctx("completion_key", key)
            .with_ctx("overlapped_is_null", true)),
            IocpCompletionStatusKind::Unknown => {
                let attach = AnomalyAttach::from_raw_completion(RawCompletion::new(
                    COMP_BACKEND_IOCP,
                    RIO_EVENT_TOKEN,
                    res,
                    flags,
                ));
                self.accept_completion_anomaly(
                    CompletionAnomalyKind::backend_context(COMP_BACKEND_IOCP, key as u64),
                    attach,
                )?;
                Ok(1)
            }
        }
    }

    fn resolve_overlapped_user_envelope(
        &mut self,
        completion_key: usize,
        overlapped: *mut Overlapped,
        res: i32,
        flags: u32,
    ) -> IocpResult<CompletionEnvelope> {
        let entry = unsafe { &*(overlapped as *const OverlappedEntry) };
        let idx = entry.token.index();
        if idx >= self.state().ops.capacity() {
            return Err(IocpError::InvalidState.report(
                "resolve_overlapped_user_envelope",
                format!(
                    "completed index out of bounds: index {}, capacity {}",
                    idx,
                    self.state().ops.capacity()
                ),
            ));
        }

        let envelope = CompletionEnvelope::from_sidecar_user_token(
            COMP_BACKEND_IOCP,
            entry.token,
            completion_key as u64,
            res,
            flags,
        );
        let raw = envelope.raw;
        let expected_key = raw.token.raw() as usize;
        if completion_key != 0 && completion_key != expected_key {
            return Err(IocpError::InvalidState
                .report(
                    "resolve_overlapped_user_envelope",
                    "Completion Key Mismatch occurred during IOCP status polling",
                )
                .with_ctx("slot_index", entry.token.index())
                .with_ctx("expected_generation", entry.token.generation())
                .with_ctx("expected_key", expected_key as u64)
                .with_ctx("received_completion_key", completion_key as u64)
                .with_ctx("overlapped_ptr", overlapped as usize)
                .with_ctx("raw_res", res)
                .with_ctx("raw_flags", flags)
                .attach_note(
                    "The Completion Key received from GetQueuedCompletionStatusEx does not match \
                     the expected key mapped to the submitted token. This indicates a programming \
                     bug or memory corruption during Socket/File registration to the completion port."
                ));
        }
        Ok(envelope)
    }

    fn drain_deferred_socket_cleanup(&mut self) {
        let mut view = IocpDriverState::registration_view_from_polling(self.state_mut());
        RegistrationService::new(&mut view).drain_deferred_socket_cleanup();
    }

    fn drain_cancel_requests(&mut self) -> IocpResult<()> {
        loop {
            let request = self.state_mut().remote_cancel_receiver.try_recv().ok();
            let Some(request) = request else {
                return Ok(());
            };
            let registrar = self.registrar;
            let mut view = IocpDriverState::cancellation_view_from_polling(self.state_mut());
            let _ = CancellationService::new(&mut view, registrar).cancel_op_internal(request)?;
        }
    }

    fn process_timers(&mut self) -> IocpResult<usize> {
        let registrar = self.registrar;
        let mut view = IocpDriverState::completion_view_from_polling(self.state_mut());
        CompletionService::new(&mut view, registrar).process_timers()
    }

    fn process_completion_envelope(&mut self, envelope: CompletionEnvelope) -> IocpResult<usize> {
        let registrar = self.registrar;
        let mut view = IocpDriverState::completion_view_from_polling(self.state_mut());
        CompletionService::new(&mut view, registrar).process_completion_envelope(envelope)
    }

    fn accept_raw_completion(&mut self, raw_token: u64, res: i32, flags: u32) -> IocpResult<()> {
        let registrar = self.registrar;
        let mut view = IocpDriverState::completion_view_from_polling(self.state_mut());
        CompletionService::new(&mut view, registrar)
            .accept_raw_completion(raw_token, res, flags)?;
        Ok(())
    }

    fn accept_completion_anomaly(
        &mut self,
        kind: CompletionAnomalyKind,
        attach: AnomalyAttach,
    ) -> IocpResult<()> {
        let registrar = self.registrar;
        let mut view = IocpDriverState::completion_view_from_polling(self.state_mut());
        CompletionService::new(&mut view, registrar).accept_completion_anomaly(kind, attach)?;
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum IocpCompletionStatusKind {
    RioWake,
    ControlKey,
    OverlappedUser { queue_key: usize },
    PostedToken,
    NullFailure,
    Unknown,
}

#[inline]
fn classify_completion_status(
    key: usize,
    overlapped: *mut Overlapped,
    success: bool,
) -> IocpCompletionStatusKind {
    if key == RIO_EVENT_KEY {
        IocpCompletionStatusKind::RioWake
    } else if !overlapped.is_null() {
        IocpCompletionStatusKind::OverlappedUser { queue_key: key }
    } else if !success && key == 0 {
        IocpCompletionStatusKind::NullFailure
    } else if matches!(
        CompletionEnvelope::from_raw_parts(COMP_BACKEND_IOCP, key as u64, 0, 0).identity,
        CompletionIdentity::Waker(_)
            | CompletionIdentity::Cancel(_)
            | CompletionIdentity::UnknownControl { .. }
    ) {
        IocpCompletionStatusKind::ControlKey
    } else if key != 0 || success {
        IocpCompletionStatusKind::PostedToken
    } else {
        IocpCompletionStatusKind::Unknown
    }
}

fn duration_to_wait_ms(duration: Duration) -> u32 {
    if duration.is_zero() {
        0
    } else {
        let millis = duration.as_millis();
        let rounded = millis.saturating_add(u128::from(
            !duration.subsec_nanos().is_multiple_of(1_000_000),
        ));
        rounded.clamp(1, u32::MAX as u128) as u32
    }
}

#[inline]
fn iocp_status_res(success: bool, error_code: Option<u32>, bytes: u32) -> i32 {
    if success {
        bytes.min(i32::MAX as u32) as i32
    } else {
        -(error_code.unwrap_or(0).min(i32::MAX as u32) as i32)
    }
}

#[inline]
fn iocp_status_flags(success: bool, error_code: Option<u32>) -> u32 {
    (u32::from(success)) | (error_code.unwrap_or(0).min(u32::MAX >> 1) << 1)
}

#[cfg(test)]
mod tests {
    use super::{
        IocpCompletionStatusKind, RIO_EVENT_KEY, WaitBudget, WaitBudgetSource,
        classify_completion_status, duration_to_wait_ms, wait_budget,
    };
    use veloq_driver_core::driver::CompletionToken;
    use veloq_std::{ptr, time::Duration};

    #[test]
    fn null_overlapped_key_zero_failure_is_not_posted_token() {
        assert_eq!(
            classify_completion_status(0, ptr::null_mut(), false),
            IocpCompletionStatusKind::NullFailure
        );
    }

    #[test]
    fn waker_key_is_classified_before_posted_token() {
        assert_eq!(
            classify_completion_status(
                CompletionToken::waker(0).raw() as usize,
                ptr::null_mut(),
                true,
            ),
            IocpCompletionStatusKind::ControlKey
        );
    }

    #[test]
    fn rio_key_is_wake_even_with_notification_overlapped() {
        assert_eq!(
            classify_completion_status(RIO_EVENT_KEY, ptr::dangling_mut(), true),
            IocpCompletionStatusKind::RioWake
        );
    }

    #[test]
    fn non_null_overlapped_keeps_queue_key_as_sidecar_context() {
        assert_eq!(
            classify_completion_status(0, ptr::dangling_mut(), true),
            IocpCompletionStatusKind::OverlappedUser { queue_key: 0 }
        );
        assert_eq!(
            classify_completion_status(123, ptr::dangling_mut(), true),
            IocpCompletionStatusKind::OverlappedUser { queue_key: 123 }
        );
    }

    #[test]
    fn external_timeout_rounds_up_to_avoid_early_expiry() {
        assert_eq!(duration_to_wait_ms(Duration::ZERO), 0);
        assert_eq!(duration_to_wait_ms(Duration::from_millis(1)), 1);
        assert_eq!(duration_to_wait_ms(Duration::from_micros(1_001)), 2);
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
}
