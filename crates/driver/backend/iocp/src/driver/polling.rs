use veloq_std::{
    format, mem,
    sync::Arc,
    sync::atomic::{AtomicU8, Ordering},
    time::{Duration, Instant},
    vec::Vec,
};

use diagweave::prelude::*;
use veloq_driver_core::driver::{
    AnomalyAttach, CompletionAnomalyKind, CompletionEnvelope, CompletionIdentity, CompletionToken,
    Driver, OpToken, RawCompletion, RemoteWaker, SharedCompletionTable,
};
use veloq_wheel::{TaskId, Wheel, WheelConfig};

use crate::{
    common::{
        IocpErrorContext, IocpWaker, WAKER_IDLE, WAKER_NOTIFIED, WAKER_PROCESSING, WAKER_REARM,
        iocp_msg,
    },
    error::{IocpError, IocpResult},
    op::{IocpSlotSpec, OverlappedEntry},
    win32::{CompletionBatch, CompletionStatus, IoCompletionPort, Overlapped},
};

use super::{IocpDriver, RIO_EVENT_KEY, RIO_EVENT_TOKEN, completion::COMP_BACKEND_IOCP};

/// Completions dequeued per `GetQueuedCompletionStatusEx` call, matching `MAX_RIO_RESULTS`.
const MAX_IOCP_BATCH: usize = 128;

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

    /// Dequeues a batch of completions, returning how many are available via [`Self::status`].
    ///
    /// The batch buffer is owned by the pump so that draining never allocates. Callers must
    /// finish reading the batch before requesting the next one.
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

    pub(super) fn clear_notification(&self) -> IocpResult<()> {
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
                        return Ok(());
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
                        return Ok(());
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
                        return Ok(());
                    }
                }
                WAKER_IDLE => return Ok(()),
                _ => unreachable!("invalid IOCP waker state"),
            }
        }
    }

    pub(super) fn create_waker(&self) -> Arc<dyn RemoteWaker<IocpError>> {
        Arc::new(IocpWaker {
            port: self.port.clone(),
            notification_state: self.notification_state.clone(),
        })
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

pub(super) struct TimerEngine {
    wheel: Wheel<OpToken>,
    buffer: Vec<OpToken>,
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

    pub(super) fn wheel_mut(&mut self) -> &mut Wheel<OpToken> {
        &mut self.wheel
    }

    pub(super) fn next_timeout(&self) -> Option<Duration> {
        self.wheel.next_timeout()
    }

    pub(super) fn insert(&mut self, token: OpToken, duration: Duration) -> TaskId {
        self.wheel.insert(token, duration)
    }

    pub(super) fn cancel(&mut self, id: TaskId) {
        self.wheel.cancel(id);
    }

    pub(super) fn advance_to(&mut self, now: Instant) {
        let elapsed = now.saturating_duration_since(self.last_poll);
        let tick_ms = self.wheel.tick_duration().as_millis() as u64;
        let ticks = elapsed.as_millis() as u64 / tick_ms;
        if ticks > 0 {
            self.wheel.advance(elapsed, &mut self.buffer);
            self.last_poll += Duration::from_millis(ticks * tick_ms);
        }
    }

    pub(super) fn take_buffer(&mut self) -> Vec<OpToken> {
        mem::take(&mut self.buffer)
    }

    pub(super) fn restore_cleared_buffer(&mut self, mut buffer: Vec<OpToken>) {
        buffer.clear();
        self.buffer = buffer;
    }
}

impl<'a> IocpDriver<'a> {
    pub(super) fn poll_completion(&mut self, timeout: Duration) -> IocpResult<usize> {
        let count = self
            .completion
            .fill_batch(duration_to_wait_ms(timeout))
            .push_ctx("scope", "iocp/driver")
            .attach_note("failed to poll IOCP status")?;
        self.completion.mark_waker_notifications(count);

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

    /// Retrieves completion events from the I/O completion port.
    pub(crate) fn get_completion(&mut self, timeout: Option<Duration>) -> IocpResult<()> {
        let _ = self.drain_cancel_requests()?;
        let wait_ms = if self.ops.shared.has_ready_completion() {
            0
        } else {
            self.calculate_wait_ms(timeout)
        };

        let batched = self.completion.fill_batch(wait_ms);
        let now = Instant::now();
        self.timer.advance_to(now);
        self.process_timers()?;

        let count = batched
            .attach_note("failed to get IOCP completion status")
            .trans()?;
        self.completion.mark_waker_notifications(count);

        // Every entry is routed even if one of them fails, so a single corrupt completion
        // cannot drop the rest of the batch on the floor; the first error is reported.
        let mut first_error = None;
        for index in 0..count {
            if let Err(report) = self.handle_batch_entry(index)
                && first_error.is_none()
            {
                first_error = Some(report);
            }
        }

        first_error.map_or(Ok(()), Err)
    }

    /// Routes the `index`-th entry of the batch currently held by the completion pump.
    fn handle_batch_entry(&mut self, index: usize) -> IocpResult<usize> {
        let Some(CompletionStatus {
            bytes,
            key,
            overlapped,
            success,
            error_code,
        }) = self.completion.status(index)
        else {
            return Ok(0);
        };
        self.handle_completion_status(bytes, key, overlapped, success, error_code)
    }

    pub(super) fn calculate_wait_ms(&self, timeout: Option<Duration>) -> u32 {
        let internal_timeout = self.timer.next_timeout().map(duration_to_wait_ms);
        let external_timeout = timeout.map(duration_to_wait_ms);

        match (external_timeout, internal_timeout) {
            (Some(external), Some(internal)) => external.min(internal),
            (Some(external), None) => external,
            (None, Some(internal)) => internal,
            (None, None) => u32::MAX,
        }
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
                {
                    let (rio_state, registrar) = self.rio.state_and_registrar_mut();
                    rio_state.process_completions(
                        &mut self.ops,
                        &self.extensions,
                        registrar,
                        self.completion.table(),
                        &mut self.completion_diagnostics,
                    )
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
        if idx >= self.ops.capacity() {
            return Err(IocpError::InvalidState.report(
                "resolve_overlapped_user_envelope",
                format!(
                    "completed index out of bounds: index {}, capacity {}",
                    idx,
                    self.ops.capacity()
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
    use super::*;
    use veloq_driver_core::driver::CompletionToken;
    use veloq_std::ptr;

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
}
