use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use diagweave::report::Report;
use tracing::error;

use crate::error::{IocpError, IocpResult, IocpResultExt};
use crate::win32::IoCompletionPort;
use veloq_driver_core::driver::{
    CompletionEvent, CompletionRecord, CompletionSidecar, RemoteWaker, SharedCompletionQueue,
    SharedCompletionTable, encode_completion_token,
};
use veloq_driver_core::{DriverErrorKind, DriverResult};

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
    let report = Report::new(IocpError::from(ctx))
        .with_ctx("scope", "iocp/common")
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
fn neg_code(code: i32) -> Option<i32> {
    (code != 0).then_some(-code.abs())
}

#[inline]
fn fallback_errno_by_iocp_error(kind: IocpError) -> i32 {
    match kind {
        IocpError::DriverInit => 5,       // EIO
        IocpError::CompletionWait => 110, // ETIMEDOUT
        IocpError::Submission => 11,      // EAGAIN
        IocpError::Rio => 5,              // EIO
        IocpError::ResolveFd => 9,        // EBADF
        IocpError::Socket => 5,           // EIO
        IocpError::Win32 => 5,            // EIO
        IocpError::InvalidInput => 22,    // EINVAL
        IocpError::InvalidState => 5,     // EIO
        IocpError::Internal => 5,         // EIO
    }
}

#[inline]
pub(crate) fn iocp_fallback_event_res(kind: IocpError) -> i32 {
    -fallback_errno_by_iocp_error(kind)
}

#[inline]
fn iocp_report_to_event_res(report: &Report<IocpError>) -> i32 {
    if let Some(code) = report
        .error_code()
        .and_then(|code| i32::try_from(code).ok())
        && let Some(res) = neg_code(code)
    {
        return res;
    }
    iocp_fallback_event_res(*report.inner())
}

#[inline]
pub(crate) fn completion_record(sidecar: CompletionSidecar) -> CompletionRecord {
    CompletionRecord {
        event: CompletionEvent {
            user_data: encode_completion_token(sidecar.user_data, sidecar.generation),
            res: sidecar.res,
            flags: sidecar.flags,
        },
        payload: sidecar.payload,
        detail: sidecar.detail,
    }
}

#[inline]
pub(crate) fn push_completion_shared(
    queue: &SharedCompletionQueue,
    table: &SharedCompletionTable,
    record: CompletionRecord,
) {
    table.record_completion_with_data(record.event, record.payload, record.detail);
    queue.push(record.event);
}

// ============================================================================
// Waker
// ============================================================================

pub(crate) const WAKEUP_USER_DATA: usize = usize::MAX;

/// A waker that posts a completion status to the port to wake up the event loop.
pub(crate) struct IocpWaker {
    pub(crate) port: Arc<IoCompletionPort>,
    pub(crate) is_notified: Arc<AtomicBool>,
}

impl RemoteWaker for IocpWaker {
    fn wake(&self) -> DriverResult<()> {
        if self.is_notified.load(Ordering::Relaxed) {
            return Ok(());
        }
        if !self.is_notified.swap(true, Ordering::AcqRel) {
            self.port.notify(WAKEUP_USER_DATA).to_driver_result(
                DriverErrorKind::Submission,
                "iocp/common",
                "failed to notify remote waker",
            )?;
        }
        Ok(())
    }
}
