use crate::{
    IocpHandle,
    diagnostics::IocpCompletionDiagnostics,
    error::{IocpError, IocpResult},
    op::{IocpOp, IocpUserPayload},
    rio::SocketInflightToken,
    win32::{IoCompletionPort, Overlapped},
};
use std::{
    ptr::NonNull,
    sync::{Arc, atomic::Ordering},
    time::Instant,
};
use veloq_driver_core::{
    driver::{CompletionToken, OpToken, registry::OpRegistry as CoreOpRegistry},
    slot::{Slot as CoreSlot, SlotSpec as CoreSlotSpec},
};
use veloq_storage::{AtomicOptionPtr, StateOptionPtr};

pub(crate) type BlockingSuccessCleanup = fn(usize);

pub(crate) struct BlockingCompletion {
    port: Arc<IoCompletionPort>,
    completion_token: CompletionToken,
    result: AtomicOptionPtr<IocpResult<usize>>,
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
            result: AtomicOptionPtr::new(None),
            cleanup_success,
        })
    }

    pub(crate) fn store_result(&self, result: IocpResult<usize>) {
        let raw = Box::into_raw(Box::new(result));
        let non_null = NonNull::new(raw);
        let old = self.result.swap(non_null, Ordering::Release);
        if let Some(old_ptr) = old {
            unsafe {
                let _ = Box::from_raw(old_ptr.as_ptr());
            }
        }
    }

    pub(crate) fn complete(&self, result: IocpResult<usize>) {
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
        let ptr = self.result.swap(None, Ordering::Acquire);
        ptr.map(|p| unsafe { *Box::from_raw(p.as_ptr()) })
    }
}

impl Drop for BlockingCompletion {
    fn drop(&mut self) {
        let ptr = self.result.load(Ordering::Acquire);
        if let Some(p) = ptr {
            let result = unsafe { *Box::from_raw(p.as_ptr()) };
            if let Some(cleanup_success) = self.cleanup_success
                && let Ok(value) = result
            {
                cleanup_success(value);
            }
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
        Self::new(OpToken::from_registry_parts(0, 0).expect("zero token should be encodable"))
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
    type CompletionDiagnostics = IocpCompletionDiagnostics;
}

pub(crate) type IocpOpRegistry = CoreOpRegistry<IocpSlotSpec>;
pub(crate) type Slot<'a, State> = CoreSlot<'a, State, IocpSlotSpec>;
