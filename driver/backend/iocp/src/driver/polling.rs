use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use crossbeam_queue::SegQueue;
use diagweave::prelude::*;
use tracing::{debug, error};
use veloq_driver_core::driver::{
    RemoteWaker, SharedCompletionQueue, SharedCompletionTable, drain_cancel_requests,
};
use veloq_wheel::{TaskId, Wheel, WheelConfig};
use windows_sys::Win32::Foundation::WAIT_TIMEOUT;

use crate::common::{IocpErrorContext, IocpWaker, WAKEUP_USER_DATA, iocp_msg};
use crate::error::{IocpError, IocpResult};
use crate::op::IocpUserPayload;

use super::{IocpDriver, IocpDriverResult, RIO_EVENT_KEY};

pub(super) struct CompletionPump {
    port: Arc<crate::win32::IoCompletionPort>,
    is_notified: Arc<AtomicBool>,
    events: SharedCompletionQueue,
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
            events: Arc::new(SegQueue::new()),
            table,
        }
    }

    pub(super) fn port(&self) -> &crate::win32::IoCompletionPort {
        self.port.as_ref()
    }

    pub(super) fn port_arc(&self) -> Arc<crate::win32::IoCompletionPort> {
        self.port.clone()
    }

    pub(super) fn events(&self) -> &SharedCompletionQueue {
        &self.events
    }

    pub(super) fn completion_queue(&self) -> SharedCompletionQueue {
        self.events.clone()
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
    wheel: Wheel<usize>,
    buffer: Vec<usize>,
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

    pub(super) fn wheel_mut(&mut self) -> &mut Wheel<usize> {
        &mut self.wheel
    }

    pub(super) fn next_timeout(&self) -> Option<Duration> {
        self.wheel.next_timeout()
    }

    pub(super) fn insert(&mut self, user_data: usize, duration: Duration) -> TaskId {
        self.wheel.insert(user_data, duration)
    }

    pub(super) fn cancel(&mut self, id: TaskId) {
        self.wheel.cancel(id);
    }

    pub(super) fn advance_to(&mut self, now: Instant) {
        let elapsed = now.saturating_duration_since(self.last_poll);
        self.wheel.advance(elapsed, &mut self.buffer);
        self.last_poll = now;
    }

    pub(super) fn take_buffer(&mut self) -> Vec<usize> {
        std::mem::take(&mut self.buffer)
    }

    pub(super) fn restore_cleared_buffer(&mut self, mut buffer: Vec<usize>) {
        buffer.clear();
        self.buffer = buffer;
    }
}

impl<'a> IocpDriver<'a> {
    pub(super) fn poll_completion(&mut self, timeout: Duration) -> IocpDriverResult<usize> {
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
            crate::win32::CompletionStatus::Timeout => Ok(0),
        }
    }

    /// Retrieves completion events from the I/O completion port.
    pub(crate) fn get_completion(&mut self, timeout_ms: u32) -> IocpResult<()> {
        drain_cancel_requests(self);
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
    ) -> IocpDriverResult<usize> {
        if key == RIO_EVENT_KEY {
            let processed = {
                let (rio_state, registrar) = self.rio.state_and_registrar_mut();
                rio_state.process_completions(
                    &mut self.ops,
                    &self.extensions,
                    registrar,
                    self.completion.events(),
                    self.completion.table(),
                )
            }
            .inspect(|_| {
                self.drain_deferred_socket_cleanup();
            })
            .push_ctx("scope", "iocp/driver")
            .attach_note("failed to process rio completions")
            .trans()?;
            return Ok(processed);
        }

        let user_data = self.resolve_user_data(overlapped, success, key, error_code)?;
        if user_data == WAKEUP_USER_DATA {
            self.completion.clear_notification();
            return Ok(0);
        }

        self.process_completion(user_data, success, error_code, bytes);
        Ok(1)
    }

    fn resolve_user_data(
        &self,
        overlapped: *mut crate::win32::Overlapped,
        success: bool,
        completion_key: usize,
        error_code: Option<u32>,
    ) -> IocpResult<usize> {
        if !overlapped.is_null() {
            // SAFETY: overlapped is non-null and corresponds to a valid OverlappedEntry.
            let id = unsafe { crate::win32::OverlappedId::from_ptr(overlapped) };
            let idx = id.as_usize();
            if idx >= self.ops.local.len() {
                error!(
                    idx,
                    slots = self.ops.local.len(),
                    "completed index out of bounds"
                );
                return Err(IocpError::CompletionWait
                    .to_report()
                    .push_ctx("scope", "iocp/driver")
                    .with_ctx("completed_index", idx)
                    .with_ctx("slot_count", self.ops.local.len())
                    .attach_note("completed index out of bounds"));
            }
            Ok(idx)
        } else {
            if !success {
                let err = error_code.unwrap_or(0);
                if err == WAIT_TIMEOUT {
                    return Ok(WAKEUP_USER_DATA);
                }
                if completion_key == 0 {
                    return Err(iocp_msg(
                        IocpErrorContext::CompletionWait,
                        "GetQueuedCompletionStatus failed with null overlapped",
                    )
                    .with_ctx("os_error_code", err)
                    .with_ctx("completion_key", completion_key)
                    .with_ctx("overlapped_is_null", true));
                }
            }
            debug!(
                completion_key,
                success,
                ?error_code,
                "resolved completion from key"
            );
            Ok(completion_key)
        }
    }
}

fn duration_to_wait_ms(duration: Duration) -> u32 {
    if duration.is_zero() {
        0
    } else {
        duration.as_millis().clamp(1, u32::MAX as u128) as u32
    }
}
