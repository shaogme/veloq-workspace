use crate::IocpHandle;
use crate::error::{IocpError, IocpResult};
use crate::rio::SocketInflightToken;
use crate::win32::{IoCompletionPort, Overlapped};
use std::io;
use std::sync::{Arc, Mutex};

pub(crate) type BlockingSuccessCleanup = fn(usize);

pub(crate) struct BlockingCompletion {
    port: Arc<IoCompletionPort>,
    completion_key: usize,
    result: Mutex<Option<IocpResult<usize>>>,
    cleanup_success: Option<BlockingSuccessCleanup>,
}

impl BlockingCompletion {
    pub(crate) fn new(
        port: Arc<IoCompletionPort>,
        completion_key: usize,
        cleanup_success: Option<BlockingSuccessCleanup>,
    ) -> Arc<Self> {
        Arc::new(Self {
            port,
            completion_key,
            result: Mutex::new(None),
            cleanup_success,
        })
    }

    pub(crate) fn store_result(&self, result: io::Result<usize>) {
        let result = result.map_err(|e| {
            IocpError::Win32.io_report("iocp.driver.inner.blocking_completion.store", e)
        });
        *self.result.lock().unwrap_or_else(|e| e.into_inner()) = Some(result);
    }

    pub(crate) fn complete(&self, result: io::Result<usize>) {
        self.store_result(result);
        if let Err(report) = self.port.notify(self.completion_key) {
            tracing::error!(
                completion_key = self.completion_key,
                report = ?report,
                "failed to post blocking completion"
            );
        }
    }

    pub(crate) fn take_result(&self) -> Option<IocpResult<usize>> {
        self.result.lock().unwrap_or_else(|e| e.into_inner()).take()
    }
}

impl Drop for BlockingCompletion {
    fn drop(&mut self) {
        let Some(cleanup_success) = self.cleanup_success else {
            return;
        };
        let result = self.result.lock().unwrap_or_else(|e| e.into_inner()).take();
        if let Some(Ok(value)) = result {
            cleanup_success(value);
        }
    }
}

/// A wrapper for the Windows OVERLAPPED structure with additional metadata.
#[repr(C)]
pub struct OverlappedEntry {
    /// The underlying Windows Overlapped structure.
    pub(crate) inner: Overlapped,
    /// User-defined data associated with the operation.
    pub(crate) user_data: usize,
    /// Generation count for slot validation.
    pub(crate) generation: u32,
    /// Whether the operation is currently in-flight in the kernel.
    pub(crate) in_flight: bool,
    /// Result of an offloaded blocking operation.
    pub(crate) blocking_completion: Option<Arc<BlockingCompletion>>,
    /// Resolved handle captured during submission to avoid re-resolving Fixed fd on hot paths.
    pub(crate) resolved_handle: Option<IocpHandle>,
    /// Socket inflight ownership acquired before a kernel-pending socket submit.
    pub(crate) socket_inflight: Option<SocketInflightToken>,
}

impl OverlappedEntry {
    /// Creates a new `OverlappedEntry` with the given user data.
    pub(crate) fn new(user_data: usize) -> Self {
        Self {
            inner: Overlapped::zeroed(),
            user_data,
            generation: 0,
            in_flight: false,
            blocking_completion: None,
            resolved_handle: None,
            socket_inflight: None,
        }
    }
}

impl Default for OverlappedEntry {
    fn default() -> Self {
        Self::new(0)
    }
}

// SAFETY: OverlappedEntry is safe to send between threads.
unsafe impl Send for OverlappedEntry {}

#[cfg(all(test, windows))]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    static CLEANED_VALUE: AtomicUsize = AtomicUsize::new(0);

    fn record_cleanup(value: usize) {
        CLEANED_VALUE.store(value, Ordering::SeqCst);
    }

    fn test_completion() -> Arc<BlockingCompletion> {
        let port = Arc::new(IoCompletionPort::new(0).expect("create test iocp"));
        BlockingCompletion::new(port, 7, Some(record_cleanup))
    }

    #[test]
    fn blocking_completion_cleans_unconsumed_success() {
        CLEANED_VALUE.store(0, Ordering::SeqCst);
        let completion = test_completion();

        completion.store_result(Ok(123));
        drop(completion);

        assert_eq!(CLEANED_VALUE.load(Ordering::SeqCst), 123);
    }

    #[test]
    fn blocking_completion_does_not_clean_consumed_success() {
        CLEANED_VALUE.store(0, Ordering::SeqCst);
        let completion = test_completion();

        completion.store_result(Ok(456));
        assert!(completion.take_result().expect("stored result").is_ok());
        drop(completion);

        assert_eq!(CLEANED_VALUE.load(Ordering::SeqCst), 0);
    }
}
