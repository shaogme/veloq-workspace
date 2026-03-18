use std::fmt;
use std::io;
use std::ptr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use error_stack::Report;
use tracing::error;
use windows_sys::Win32::Foundation::{
    CloseHandle, GetLastError, HANDLE, INVALID_HANDLE_VALUE, WAIT_TIMEOUT,
};
use windows_sys::Win32::System::IO::{
    CreateIoCompletionPort, GetQueuedCompletionStatus, OVERLAPPED, PostQueuedCompletionStatus,
};

use veloq_driver_core::driver::{
    CompletionEvent, CompletionRecord, CompletionSidecar, RemoteWaker, SharedCompletionQueue,
    SharedCompletionTable, encode_completion_token,
};

// ============================================================================
// Win32 Wrappers
// ============================================================================

/// A safe wrapper around a Win32 HANDLE that ensures it is closed when dropped.
#[derive(Debug)]
pub struct OwnedHandle(HANDLE);

impl OwnedHandle {
    /// Creates a new `OwnedHandle` from a raw HANDLE.
    ///
    /// # Safety
    ///
    /// The handle must be valid and owned by the caller.
    pub unsafe fn from_raw(handle: HANDLE) -> Self {
        Self(handle)
    }

    /// Returns the raw HANDLE.
    pub fn as_raw(&self) -> HANDLE {
        self.0
    }

    /// Checks if the handle is valid.
    pub fn is_valid(&self) -> bool {
        !self.0.is_null() && self.0 != INVALID_HANDLE_VALUE
    }
}

impl Drop for OwnedHandle {
    fn drop(&mut self) {
        if self.is_valid() {
            unsafe {
                CloseHandle(self.0);
            }
        }
    }
}

unsafe impl Send for OwnedHandle {}
unsafe impl Sync for OwnedHandle {}

/// A safe wrapper for an I/O Completion Port.
pub struct IoCompletionPort(OwnedHandle);

impl IoCompletionPort {
    /// Creates a new, unconnected I/O Completion Port.
    pub fn new(threads: u32) -> io::Result<Self> {
        let handle =
            unsafe { CreateIoCompletionPort(INVALID_HANDLE_VALUE, ptr::null_mut(), 0, threads) };
        if handle.is_null() {
            return Err(io::Error::last_os_error());
        }
        Ok(Self(OwnedHandle(handle)))
    }

    /// Associates a handle with this I/O Completion Port.
    ///
    /// # Safety
    ///
    /// The caller must ensure that `handle` is a valid file/socket handle.
    pub unsafe fn associate(&self, handle: HANDLE, completion_key: usize) -> io::Result<()> {
        let res = unsafe { CreateIoCompletionPort(handle, self.0.as_raw(), completion_key, 0) };
        if res.is_null() {
            let err = unsafe { GetLastError() };
            // Windows returns ERROR_INVALID_PARAMETER when trying to re-associate
            // a handle that is already bound to an IOCP.
            if err == windows_sys::Win32::Foundation::ERROR_INVALID_PARAMETER {
                return Ok(());
            }
            return Err(io::Error::from_raw_os_error(err as i32));
        }
        Ok(())
    }

    /// Posts a completion status to the port.
    ///
    /// # Safety
    ///
    /// If `overlapped` is not null, it must point to a valid `OVERLAPPED` structure
    /// that remains valid until the completion is retrieved.
    pub unsafe fn post(
        &self,
        bytes: u32,
        key: usize,
        overlapped: *mut OVERLAPPED,
    ) -> io::Result<()> {
        let res = unsafe { PostQueuedCompletionStatus(self.0.as_raw(), bytes, key, overlapped) };
        if res == 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    /// Notifies the completion port with a user-defined completion key.
    /// This is a safe wrapper around `PostQueuedCompletionStatus` for notifications.
    pub fn notify(&self, user_data: usize) -> io::Result<()> {
        // SAFETY: Posting a null overlapped pointer is safe.
        unsafe { self.post(0, user_data, ptr::null_mut()) }
    }

    /// Cancels a pending I/O request for the specified handle and overlapped structure.
    ///
    /// # Safety
    ///
    /// `overlapped` must point to the same `OVERLAPPED` structure used to start the I/O.
    pub unsafe fn cancel_request(handle: HANDLE, overlapped: *mut OVERLAPPED) -> io::Result<()> {
        let res = unsafe { windows_sys::Win32::System::IO::CancelIoEx(handle, overlapped) };
        if res == 0 {
            let err = unsafe { GetLastError() };
            if err == windows_sys::Win32::Foundation::ERROR_NOT_FOUND {
                return Ok(());
            }
            return Err(io::Error::from_raw_os_error(err as i32));
        }
        Ok(())
    }

    /// Retrieves a completion status from the port.
    pub fn get_status(&self, timeout_ms: u32) -> io::Result<CompletionStatus> {
        let mut bytes = 0;
        let mut key = 0;
        let mut overlapped = ptr::null_mut();

        let res = unsafe {
            GetQueuedCompletionStatus(
                self.0.as_raw(),
                &mut bytes,
                &mut key,
                &mut overlapped,
                timeout_ms,
            )
        };

        if res == 0 {
            let err = unsafe { GetLastError() };
            if overlapped.is_null() {
                if err == WAIT_TIMEOUT {
                    return Ok(CompletionStatus::Timeout);
                }
                return Err(io::Error::from_raw_os_error(err as i32));
            } else {
                // Operation failed but we got an overlapped pointer
                return Ok(CompletionStatus::Completed {
                    bytes,
                    key,
                    overlapped,
                    success: false,
                    error_code: Some(err),
                });
            }
        }

        Ok(CompletionStatus::Completed {
            bytes,
            key,
            overlapped,
            success: true,
            error_code: None,
        })
    }

    pub fn as_raw(&self) -> HANDLE {
        self.0.as_raw()
    }
}

pub enum CompletionStatus {
    Completed {
        bytes: u32,
        key: usize,
        overlapped: *mut OVERLAPPED,
        success: bool,
        error_code: Option<u32>,
    },
    Timeout,
}

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
