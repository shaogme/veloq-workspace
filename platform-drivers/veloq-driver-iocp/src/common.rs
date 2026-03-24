use std::fmt;
use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use tracing::error;

use crate::error::{IocpError, IocpReportExt, IocpResultExt};
use crate::win32::IoCompletionPort;
use veloq_driver_core::driver::{
    CompletionEvent, CompletionRecord, CompletionSidecar, RemoteWaker, SharedCompletionQueue,
    SharedCompletionTable, encode_completion_token,
};

// ============================================================================
// Error Context & Logic
// ============================================================================

#[derive(Debug, Clone, Copy)]
pub(crate) enum IocpErrorContext {
    CompletionWait,
    Rio,
}

impl fmt::Display for IocpErrorContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CompletionWait => f.write_str("IOCP completion wait failed"),
            Self::Rio => f.write_str("RIO operation failed"),
        }
    }
}

impl std::error::Error for IocpErrorContext {}

impl From<IocpErrorContext> for IocpError {
    fn from(value: IocpErrorContext) -> Self {
        match value {
            IocpErrorContext::CompletionWait => IocpError::CompletionWait,
            IocpErrorContext::Rio => IocpError::Rio,
        }
    }
}

fn sanitize_field(s: &str) -> String {
    s.replace('\n', "\\n").replace('\r', "\\r")
}

fn structured_line(
    ctx: IocpErrorContext,
    detail: &str,
    source: Option<&str>,
    os_code: Option<i32>,
) -> String {
    let source = source
        .map(sanitize_field)
        .unwrap_or_else(|| "none".to_string());
    let os_code = os_code
        .map(|v| v.to_string())
        .unwrap_or_else(|| "none".to_string());
    format!(
        "context={ctx}; detail={}; source={source}; os_error={os_code}",
        sanitize_field(detail)
    )
}

pub(crate) fn io_msg(ctx: IocpErrorContext, detail: impl Into<String>) -> io::Error {
    let detail = detail.into();
    let report = error_stack::Report::new(IocpError::from(ctx)).attach(detail.clone());
    let msg = structured_line(ctx, &detail, None, None);
    error!(
        context = %ctx,
        detail = %detail,
        report = ?report,
        "IOCP error report"
    );
    report.to_io_error(msg)
}

// ============================================================================
// Utilities
// ============================================================================

#[inline]
pub(crate) fn io_result_to_event_res(res: &io::Result<usize>) -> i32 {
    match res {
        Ok(v) => (*v).min(i32::MAX as usize) as i32,
        Err(e) => -e.raw_os_error().unwrap_or(1),
    }
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
    fn wake(&self) -> io::Result<()> {
        if self.is_notified.load(Ordering::Relaxed) {
            return Ok(());
        }
        if !self.is_notified.swap(true, Ordering::AcqRel) {
            self.port
                .notify(WAKEUP_USER_DATA)
                .to_io_result("failed to notify remote waker")?;
        }
        Ok(())
    }
}
