use std::fmt;
use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use error_stack::Report;
use tracing::error;

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
    DriverInit,
    CompletionWait,
    Submission,
    Rio,
    ResolveFd,
}

impl fmt::Display for IocpErrorContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DriverInit => f.write_str("IOCP driver initialization failed"),
            Self::CompletionWait => f.write_str("IOCP completion wait failed"),
            Self::Submission => f.write_str("IOCP operation submission failed"),
            Self::Rio => f.write_str("RIO operation failed"),
            Self::ResolveFd => f.write_str("failed to resolve IO handle"),
        }
    }
}

impl std::error::Error for IocpErrorContext {}

fn sanitize_field(s: &str) -> String {
    s.replace('\n', "\\n").replace('\r', "\\r")
}

fn extract_structured_field<'a>(s: &'a str, key: &str) -> Option<&'a str> {
    s.split("; ").find_map(|part| part.strip_prefix(key))
}

fn parse_nested_source(source: &str) -> (String, Option<i32>) {
    if !source.starts_with("context=") {
        return (source.to_string(), None);
    }

    let nested_ctx = extract_structured_field(source, "context=").unwrap_or("unknown");
    let nested_detail = extract_structured_field(source, "detail=").unwrap_or("none");
    let nested_source = extract_structured_field(source, "source=");
    let nested_os = extract_structured_field(source, "os_error=").and_then(|v| {
        if v == "none" {
            None
        } else {
            v.parse::<i32>().ok()
        }
    });

    let nested_source = match nested_source {
        Some("none") | None => "none".to_string(),
        Some(val) => val.to_string(),
    };
    (
        format!(
            "nested_context={}; nested_detail={}; nested_source={}",
            sanitize_field(nested_ctx),
            sanitize_field(nested_detail),
            sanitize_field(&nested_source)
        ),
        nested_os,
    )
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

pub(crate) fn io_error(
    ctx: IocpErrorContext,
    err: io::Error,
    detail: impl Into<String>,
) -> io::Error {
    let detail = detail.into();
    let raw_source = err.to_string();
    let (source, nested_os) = parse_nested_source(&raw_source);
    let os_code = err.raw_os_error().or(nested_os);
    let report = Report::new(err).change_context(ctx).attach(detail.clone());
    let msg = structured_line(ctx, &detail, Some(&source), os_code);
    error!(
        context = %ctx,
        detail = %detail,
        source = %raw_source,
        os_error = ?os_code,
        report = ?report,
        "IOCP error report"
    );
    io::Error::other(msg)
}

pub(crate) fn io_msg(ctx: IocpErrorContext, detail: impl Into<String>) -> io::Error {
    let detail = detail.into();
    let report = Report::new(ctx).attach(detail.clone());
    let msg = structured_line(ctx, &detail, None, None);
    error!(
        context = %ctx,
        detail = %detail,
        report = ?report,
        "IOCP error report"
    );
    io::Error::other(msg)
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
            self.port.notify(WAKEUP_USER_DATA)?;
        }
        Ok(())
    }
}
