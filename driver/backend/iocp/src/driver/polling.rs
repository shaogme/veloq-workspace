use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use diagweave::prelude::*;
use tracing::{debug, error, warn};
use veloq_driver_core::driver::{
    CompletionAnomaly, CompletionBackend, CompletionDispatch, CompletionEnvelope, CompletionToken,
    OpToken, RawCompletion, RemoteWaker, SharedCompletionTable, UserCompletionEvent,
    dispatch_raw_completion, drain_cancel_requests, record_completion_anomaly, slot_view_anomaly,
    unknown_completion_anomaly,
};
use veloq_driver_core::slot::SlotRegistryExt;
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
        match self.resolve_completion_kind(bytes, overlapped, success, key, error_code)? {
            CompletionDispatch::RioWake { .. } => {
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
                Ok(CompletionProgress {
                    iocp: 0,
                    rio: processed,
                    internal: 1,
                    ..CompletionProgress::default()
                })
            }
            CompletionDispatch::Waker { .. } => {
                self.handle_waker_completion(success, error_code);
                Ok(CompletionProgress {
                    internal: 1,
                    ..CompletionProgress::default()
                })
            }
            CompletionDispatch::User { event } => {
                Ok(self.process_completion(event, success, error_code, bytes))
            }
            CompletionDispatch::Cancel { raw, .. } => {
                let anomaly = CompletionAnomaly::control_completion_untracked(raw.token)
                    .with_raw_completion(raw);
                record_completion_anomaly(&self.completion_diagnostics, &anomaly);
                debug!(
                    token = raw.token.raw(),
                    key,
                    success,
                    ?error_code,
                    "untracked IOCP control completion token"
                );
                Ok(CompletionProgress {
                    anomaly: 1,
                    ..CompletionProgress::default()
                })
            }
            CompletionDispatch::Unknown { envelope } => {
                let raw = envelope.raw;
                let anomaly = if matches!(
                    envelope.identity,
                    veloq_driver_core::driver::CompletionIdentity::BackendContext { .. }
                ) {
                    unknown_completion_anomaly(envelope)
                } else if let Some(token) = raw.token.op_token() {
                    CompletionAnomaly::unknown_slot(raw.token, token.index(), token.generation())
                        .with_raw_completion(raw)
                } else {
                    unknown_completion_anomaly(envelope)
                };
                record_completion_anomaly(&self.completion_diagnostics, &anomaly);
                debug!(
                    token = raw.token.raw(),
                    key,
                    success,
                    ?error_code,
                    "unknown IOCP completion token"
                );
                Ok(CompletionProgress {
                    anomaly: 1,
                    ..CompletionProgress::default()
                })
            }
        }
    }

    fn handle_waker_completion(&mut self, success: bool, error_code: Option<u32>) {
        if success {
            self.completion_diagnostics.backend().inc_waker_ok();
            self.completion.clear_notification();
        } else {
            self.completion_diagnostics.backend().inc_waker_error();
            warn!(?error_code, "IOCP waker completion reported an error");
            self.completion.clear_notification();
        }
    }

    fn resolve_completion_kind(
        &mut self,
        bytes: u32,
        overlapped: *mut crate::win32::Overlapped,
        success: bool,
        completion_key: usize,
        error_code: Option<u32>,
    ) -> IocpResult<CompletionDispatch> {
        if completion_key == RIO_EVENT_KEY {
            let raw = RawCompletion::new(
                CompletionBackend::Iocp,
                CompletionToken::rio_wake(0),
                iocp_status_res(success, error_code, bytes),
                0,
            );
            return Ok(CompletionDispatch::RioWake { id: 0, raw });
        }

        if !overlapped.is_null() {
            // SAFETY: overlapped is non-null and corresponds to a valid OverlappedEntry.
            let entry = unsafe { &*(overlapped as *const crate::op::OverlappedEntry) };
            let idx = entry.token.index();
            if idx >= self.ops.capacity() {
                error!(
                    idx,
                    slots = self.ops.capacity(),
                    "completed index out of bounds"
                );
                return Ok(CompletionDispatch::Unknown {
                    envelope: CompletionEnvelope::from_raw(RawCompletion::new(
                        CompletionBackend::Iocp,
                        CompletionToken::user(entry.token),
                        iocp_status_res(success, error_code, bytes),
                        0,
                    )),
                });
            }
            let raw = RawCompletion::new(
                CompletionBackend::Iocp,
                CompletionToken::user(entry.token),
                iocp_status_res(success, error_code, bytes),
                0,
            );
            let expected_key = raw.token.raw() as usize;
            // Handles are associated with this IOCP using completion key 0. The overlapped
            // sidecar is the authoritative user token; a non-zero mismatched key is diagnostic
            // only and must not override the sidecar token.
            if completion_key != 0 && completion_key != expected_key {
                let mismatch_raw = RawCompletion::new(
                    CompletionBackend::Iocp,
                    CompletionToken::from_raw(completion_key as u64),
                    raw.res,
                    raw.flags,
                );
                let anomaly = match slot_view_anomaly(
                    CompletionBackend::Iocp,
                    entry.token,
                    mismatch_raw,
                    self.ops.checked_slot_view(entry.token),
                ) {
                    Ok(view) => {
                        let snapshot = match view {
                            veloq_driver_core::slot::SlotView::Reserved(slot) => slot.snapshot(),
                            veloq_driver_core::slot::SlotView::InFlightWaiting(slot) => {
                                slot.snapshot()
                            }
                            veloq_driver_core::slot::SlotView::InFlightOrphaned(slot) => {
                                slot.snapshot()
                            }
                        };
                        CompletionAnomaly::backend_invariant_broken(
                            raw.token,
                            snapshot.index,
                            snapshot.generation,
                            snapshot.state,
                        )
                        .with_slot_snapshot(snapshot)
                        .with_raw_completion(mismatch_raw)
                    }
                    Err(anomaly) => anomaly,
                };
                record_completion_anomaly(&self.completion_diagnostics, &anomaly);
                debug!(
                    expected_key,
                    completion_key,
                    sidecar_token = raw.token.raw(),
                    reason = ?anomaly.reason,
                    "IOCP completion key does not match overlapped sidecar token"
                );
            }
            return Ok(CompletionDispatch::User {
                event: UserCompletionEvent::from_parts(
                    CompletionBackend::Iocp,
                    entry.token,
                    raw.res,
                    raw.flags,
                ),
            });
        }

        if !success {
            let err = error_code.unwrap_or(0);
            if completion_key == 0 {
                let _ = iocp_msg(
                    IocpErrorContext::CompletionWait,
                    "GetQueuedCompletionStatus failed with null overlapped",
                )
                .with_ctx("os_error_code", err)
                .with_ctx("completion_key", completion_key)
                .with_ctx("overlapped_is_null", true);
                return Ok(CompletionDispatch::Unknown {
                    envelope: CompletionEnvelope::backend_context(
                        CompletionBackend::Iocp,
                        completion_key as u64,
                        iocp_status_res(success, error_code, bytes),
                        0,
                    ),
                });
            }
        }
        debug!(
            completion_key,
            success,
            ?error_code,
            "resolved null-overlapped completion from key"
        );

        let dispatch = dispatch_raw_completion(
            CompletionBackend::Iocp,
            completion_key as u64,
            iocp_status_res(success, error_code, bytes),
            0,
        );
        Ok(dispatch)
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
