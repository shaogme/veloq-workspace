use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use diagweave::prelude::*;
use tracing::{debug, error};
use veloq_driver_core::driver::{
    CompletionAnomaly, CompletionBackend, CompletionToken, OpToken, RawCompletion, RemoteWaker,
    SharedCompletionTable, UserCompletionEvent, drain_cancel_requests,
};
use veloq_driver_core::slot::{CheckedSlotView, SlotRegistryExt, SlotView};
use veloq_wheel::{TaskId, Wheel, WheelConfig};

use crate::common::{IocpErrorContext, IocpWaker, iocp_msg};
use crate::error::{IocpError, IocpResult};
use crate::op::IocpUserPayload;

use super::{IocpDriver, IocpDriverResult, RIO_EVENT_KEY};

#[derive(Default)]
pub(super) struct CompletionProgress {
    pub(super) iocp: usize,
    pub(super) rio: usize,
    pub(super) user_completed: usize,
    pub(super) user_lost: usize,
    pub(super) orphan_cleaned: usize,
    pub(super) internal: usize,
    pub(super) anomaly: usize,
}

impl CompletionProgress {
    #[inline]
    pub(super) fn semantic_count(&self) -> usize {
        self.user_completed + self.user_lost + self.orphan_cleaned + self.internal + self.anomaly
    }
}

pub(super) struct CompletionPump {
    port: Arc<crate::win32::IoCompletionPort>,
    is_notified: Arc<AtomicBool>,
    table: SharedCompletionTable<IocpUserPayload, IocpError>,
}

impl CompletionPump {
    pub(super) fn new(
        port: crate::win32::IoCompletionPort,
        table: SharedCompletionTable<IocpUserPayload, IocpError>,
    ) -> Self {
        Self {
            port: Arc::new(port),
            is_notified: Arc::new(AtomicBool::new(false)),
            table,
        }
    }

    pub(super) fn port(&self) -> &crate::win32::IoCompletionPort {
        self.port.as_ref()
    }

    pub(super) fn port_arc(&self) -> Arc<crate::win32::IoCompletionPort> {
        self.port.clone()
    }

    pub(super) fn table(&self) -> &SharedCompletionTable<IocpUserPayload, IocpError> {
        &self.table
    }

    pub(super) fn completion_table(&self) -> SharedCompletionTable<IocpUserPayload, IocpError> {
        self.table.clone()
    }

    pub(super) fn clear_notification(&self) {
        self.is_notified.store(false, Ordering::Release);
    }

    pub(super) fn create_waker(&self) -> Arc<dyn RemoteWaker<IocpError>> {
        Arc::new(IocpWaker {
            port: self.port.clone(),
            is_notified: self.is_notified.clone(),
        })
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
        self.wheel.advance(elapsed, &mut self.buffer);
        self.last_poll = now;
    }

    pub(super) fn take_buffer(&mut self) -> Vec<OpToken> {
        std::mem::take(&mut self.buffer)
    }

    pub(super) fn restore_cleared_buffer(&mut self, mut buffer: Vec<OpToken>) {
        buffer.clear();
        self.buffer = buffer;
    }
}

impl<'a> IocpDriver<'a> {
    pub(super) fn poll_completion(
        &mut self,
        timeout: Duration,
    ) -> IocpDriverResult<CompletionProgress> {
        let status = self
            .completion
            .port()
            .get_status(duration_to_wait_ms(timeout))
            .push_ctx("scope", "iocp/driver")
            .attach_note("failed to poll IOCP status")?;

        match status {
            crate::win32::CompletionStatus::Completed {
                bytes,
                key,
                overlapped,
                success,
                error_code,
            } => self.handle_completion_status(bytes, key, overlapped, success, error_code),
            crate::win32::CompletionStatus::Timeout => Ok(CompletionProgress::default()),
        }
    }

    /// Retrieves completion events from the I/O completion port.
    pub(crate) fn get_completion(&mut self, timeout_ms: u32) -> IocpResult<()> {
        let _ = drain_cancel_requests(self)?;
        let wait_ms = self.calculate_wait_ms(timeout_ms);

        let status = self.completion.port().get_status(wait_ms);
        let now = Instant::now();
        self.timer.advance_to(now);
        self.process_timers();

        let status = status
            .attach_note("failed to get IOCP completion status")
            .trans()?;

        match status {
            crate::win32::CompletionStatus::Completed {
                bytes,
                key,
                overlapped,
                success,
                error_code,
            } => {
                let _ =
                    self.handle_completion_status(bytes, key, overlapped, success, error_code)?;
            }
            crate::win32::CompletionStatus::Timeout => {}
        }
        Ok(())
    }

    pub(super) fn calculate_wait_ms(&self, timeout_ms: u32) -> u32 {
        if let Some(delay) = self.timer.next_timeout() {
            let millis = delay.as_millis().min(u32::MAX as u128) as u32;
            std::cmp::min(timeout_ms, millis)
        } else {
            timeout_ms
        }
    }

    fn handle_completion_status(
        &mut self,
        bytes: u32,
        key: usize,
        overlapped: *mut crate::win32::Overlapped,
        success: bool,
        error_code: Option<u32>,
    ) -> IocpDriverResult<CompletionProgress> {
        if key == RIO_EVENT_KEY {
            let processed = {
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
            .attach_note("failed to process rio completions")
            .trans()?;
            return Ok(CompletionProgress {
                iocp: 0,
                rio: processed,
                internal: 1,
                ..CompletionProgress::default()
            });
        }

        let res = iocp_status_res(success, error_code, bytes);
        if !overlapped.is_null() {
            let event = self.resolve_overlapped_user_event(bytes, key, overlapped, success, res)?;
            return Ok(self.process_completion(event, success, error_code, bytes));
        }

        if !success && key == 0 {
            let _ = iocp_msg(
                IocpErrorContext::CompletionWait,
                "GetQueuedCompletionStatus failed with null overlapped",
            )
            .with_ctx("os_error_code", error_code.unwrap_or(0))
            .with_ctx("completion_key", key)
            .with_ctx("overlapped_is_null", true);
        }

        let flow = self.accept_raw_completion(key as u64, res, 0)?;
        Ok(CompletionProgress::from_flow(flow, 1, 0))
    }

    fn resolve_overlapped_user_event(
        &mut self,
        bytes: u32,
        completion_key: usize,
        overlapped: *mut crate::win32::Overlapped,
        success: bool,
        res: i32,
    ) -> IocpResult<UserCompletionEvent> {
        let entry = unsafe { &*(overlapped as *const crate::op::OverlappedEntry) };
        let idx = entry.token.index();
        if idx >= self.ops.capacity() {
            error!(
                idx,
                slots = self.ops.capacity(),
                "completed index out of bounds"
            );
        }

        let raw = RawCompletion::new(
            CompletionBackend::Iocp,
            CompletionToken::user(entry.token),
            res,
            0,
        );
        let expected_key = raw.token.raw() as usize;
        if completion_key != 0 && completion_key != expected_key {
            let mismatch_raw = RawCompletion::new(
                CompletionBackend::Iocp,
                CompletionToken::from_raw(completion_key as u64),
                raw.res,
                raw.flags,
            );
            let anomaly = completion_key_mismatch_anomaly(
                entry.token,
                raw,
                mismatch_raw,
                self.ops.checked_slot_view(entry.token),
            );
            let reason = anomaly.reason;
            let _ = self.accept_completion_anomaly(anomaly)?;
            debug!(
                expected_key,
                completion_key,
                sidecar_token = raw.token.raw(),
                reason = ?reason,
                "IOCP completion key does not match overlapped sidecar token"
            );
        }

        debug!(
            completion_key,
            success, bytes, "resolved overlapped IOCP completion"
        );
        Ok(UserCompletionEvent::from_parts(
            CompletionBackend::Iocp,
            entry.token,
            raw.res,
            raw.flags,
        ))
    }
}

fn duration_to_wait_ms(duration: Duration) -> u32 {
    if duration.is_zero() {
        0
    } else {
        duration.as_millis().clamp(1, u32::MAX as u128) as u32
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

fn completion_key_mismatch_anomaly(
    token: OpToken,
    raw: RawCompletion,
    mismatch_raw: RawCompletion,
    view: CheckedSlotView<'_, crate::op::IocpSlotSpec>,
) -> CompletionAnomaly {
    match view {
        CheckedSlotView::Valid(slot) => {
            let snapshot = match slot {
                SlotView::Reserved(slot) => slot.snapshot(),
                SlotView::InFlightWaiting(slot) => slot.snapshot(),
                SlotView::InFlightOrphaned(slot) => slot.snapshot(),
            };
            CompletionAnomaly::completion_key_mismatch(
                raw.token,
                snapshot.index,
                snapshot.generation,
                snapshot.state,
            )
            .with_slot_snapshot(snapshot)
            .with_raw_completion(mismatch_raw)
        }
        CheckedSlotView::Missing {
            index,
            expected_generation,
        } => CompletionAnomaly::unknown_slot(mismatch_raw.token, index, expected_generation)
            .with_raw_completion(mismatch_raw),
        CheckedSlotView::Empty(snapshot) => CompletionAnomaly::non_active(
            mismatch_raw.token,
            snapshot.index,
            token.generation(),
            snapshot.state,
        )
        .with_slot_snapshot(snapshot)
        .with_raw_completion(mismatch_raw),
        CheckedSlotView::Stale(snapshot) => CompletionAnomaly::stale(
            mismatch_raw.token,
            snapshot.index,
            token.generation(),
            snapshot.generation,
            snapshot.state,
        )
        .with_slot_snapshot(snapshot)
        .with_raw_completion(mismatch_raw),
        CheckedSlotView::Corrupt(snapshot) => {
            CompletionAnomaly::corrupt_slot_snapshot(mismatch_raw.token, snapshot)
                .with_raw_completion(mismatch_raw)
        }
    }
}
