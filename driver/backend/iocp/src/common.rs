use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use diagweave::prelude::*;
use tracing::error;

use crate::error::{IocpError, IocpResult, iocp_report_to_event_res};
use crate::op::IocpUserPayload;
use crate::win32::IoCompletionPort;
use veloq_driver_core::driver::{
    CompletionPacket, CompletionSidecar, CompletionToken, RecordCompletionOutcome, RemoteWaker,
    SharedCompletionTable, record_user_completion,
};

// ============================================================================
// Error Context & Logic
// ============================================================================

#[derive(Debug, Clone, Copy)]
pub(crate) enum IocpErrorContext {
    CompletionWait,
}

impl fmt::Display for IocpErrorContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CompletionWait => f.write_str("IOCP completion wait failed"),
        }
    }
}

impl std::error::Error for IocpErrorContext {}

impl From<IocpErrorContext> for IocpError {
    fn from(value: IocpErrorContext) -> Self {
        match value {
            IocpErrorContext::CompletionWait => IocpError::CompletionWait,
        }
    }
}

fn sanitize_field(s: &str) -> String {
    s.replace('\n', "\\n").replace('\r', "\\r")
}

pub(crate) fn iocp_msg(ctx: IocpErrorContext, detail: impl Into<String>) -> Report<IocpError> {
    let detail = detail.into();
    let report = IocpError::from(ctx)
        .to_report()
        .push_ctx("scope", "iocp/common")
        .with_ctx("detail", sanitize_field(&detail))
        .attach_note(detail);
    error!(
        context = %ctx,
        report = %report,
        "IOCP error report"
    );
    report
}

// ============================================================================
// Utilities
// ============================================================================

#[inline]
pub(crate) fn io_result_to_event_res(res: &IocpResult<usize>) -> i32 {
    match res {
        Ok(v) => (*v).min(i32::MAX as usize) as i32,
        Err(e) => iocp_report_to_event_res(e),
    }
}

#[inline]
pub(crate) fn completion_record(
    sidecar: CompletionSidecar<IocpUserPayload, IocpError>,
) -> CompletionPacket<IocpUserPayload, IocpError> {
    CompletionPacket::from(sidecar)
}

#[inline]
pub(crate) fn push_completion_shared(
    table: &SharedCompletionTable<IocpUserPayload, IocpError>,
    diagnostics: &mut veloq_driver_core::driver::DriverCompletionDiagnostics,
    packet: CompletionPacket<IocpUserPayload, IocpError>,
) -> RecordCompletionOutcome {
    let event = packet.event;
    let outcome = record_user_completion(table, diagnostics, packet);
    match &outcome {
        RecordCompletionOutcome::RecordedUser
        | RecordCompletionOutcome::RecordedLost
        | RecordCompletionOutcome::OrphanedDropped => {}
        anomaly => {
            tracing::debug!(
                token = event.token.raw(),
                res = event.res,
                flags = event.flags,
                outcome = ?anomaly,
                "IOCP completion table did not record completion normally"
            );
        }
    }
    outcome
}

// ============================================================================
// Waker
// ============================================================================

/// A waker that posts a completion status to the port to wake up the event loop.
pub(crate) struct IocpWaker {
    pub(crate) port: Arc<IoCompletionPort>,
    pub(crate) is_notified: Arc<AtomicBool>,
}

impl RemoteWaker<IocpError> for IocpWaker {
    fn wake(&self) -> crate::error::IocpDriverResult<()> {
        if self.is_notified.load(Ordering::Relaxed) {
            return Ok(());
        }
        if !self.is_notified.swap(true, Ordering::AcqRel) {
            self.port
                .notify(CompletionToken::waker(0))
                .push_ctx("scope", "iocp/common")
                .attach_note("failed to notify remote waker")?;
        }
        Ok(())
    }
}
