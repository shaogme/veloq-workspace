use crate::IocpHandle;
use crate::error::{IocpError, IocpResult};
use crate::rio::SocketInflightToken;
use crate::win32::{IoCompletionPort, Overlapped};
use std::io;
use std::sync::{Arc, Mutex};
use std::time::Instant;
use veloq_driver_core::driver::registry::OpRegistry as CoreOpRegistry;
use veloq_driver_core::driver::{CompletionToken, OpToken};
use veloq_driver_core::slot::{Slot as CoreSlot, SlotSpec as CoreSlotSpec};

use crate::op::{IocpOp, IocpUserPayload};

pub(crate) type BlockingSuccessCleanup = fn(usize);

pub(crate) struct BlockingCompletion {
    port: Arc<IoCompletionPort>,
    completion_token: CompletionToken,
    result: Mutex<Option<IocpResult<usize>>>,
    cleanup_success: Option<BlockingSuccessCleanup>,
}

impl BlockingCompletion {
    pub(crate) fn new(
        port: Arc<IoCompletionPort>,
        completion_token: CompletionToken,
        cleanup_success: Option<BlockingSuccessCleanup>,
    ) -> Arc<Self> {
        Arc::new(Self {
            port,
            completion_token,
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
        if let Err(report) = self.port.notify(self.completion_token) {
            tracing::error!(
                completion_token = self.completion_token.raw(),
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
    /// Token associated with the operation.
    pub(crate) token: OpToken,
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
    /// Creates a new `OverlappedEntry` with the given operation token.
    pub(crate) fn new(token: OpToken) -> Self {
        let mut entry = Self {
            inner: Overlapped::zeroed(),
            token,
            in_flight: false,
            blocking_completion: None,
            resolved_handle: None,
            socket_inflight: None,
        };
        entry.reset_for_token(token);
        entry
    }

    pub(crate) fn reset_for_token(&mut self, token: OpToken) {
        self.inner = Overlapped::zeroed();
        self.token = token;
        self.in_flight = false;
        self.blocking_completion = None;
        self.resolved_handle = None;
        self.socket_inflight = None;
    }
}

impl Default for OverlappedEntry {
    fn default() -> Self {
        Self::new(OpToken::new(0, 0))
    }
}

// SAFETY: OverlappedEntry is safe to send between threads.
unsafe impl Send for OverlappedEntry {}

/// State associated with an IOCP operation.
#[derive(Default)]
pub struct IocpOpState {
    pub(crate) generation: u32,
    pub(crate) timer_id: Option<veloq_wheel::TaskId>,
    pub(crate) timer_deadline: Option<Instant>,
    pub(crate) is_background: bool,
    pub(crate) rio_cancel_requested: bool,
}

pub enum IocpSlotSpec {}

impl CoreSlotSpec for IocpSlotSpec {
    type Op = IocpOp;
    type UserPayload = IocpUserPayload;
    type PlatformData = IocpOpState;
    type Sidecar = OverlappedEntry;
    type Error = IocpError;
    type Completion = usize;
}

pub(crate) type IocpOpRegistry = CoreOpRegistry<IocpSlotSpec>;
pub(crate) type Slot<'a, State> = CoreSlot<'a, State, IocpSlotSpec>;
