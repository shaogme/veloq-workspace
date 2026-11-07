use crate::{
    IocpHandle,
    config::RawHandle,
    error::IocpResult,
    op::IocpSlotSpec,
    rio::SocketInflightToken,
    win32::{IoCompletionPort, Overlapped},
};
use tracing::error;
use veloq_driver_core::{
    CompletionToken, Generation, OpRegistry as CoreOpRegistry, OpToken, ReceiveRequest,
    Slot as CoreSlot,
};
use veloq_std::{
    boxed::Box,
    ptr::NonNull,
    sync::{Arc, atomic::Ordering},
    time::Instant,
};
use veloq_storage::{AtomicOptionPtr, StateOptionPtr};
use veloq_wheel::TimerId;

pub(crate) type BlockingSuccessCleanup = fn(IocpHandle);

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
            error!(
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
                cleanup_success(IocpHandle::for_file(value as _));
            }
        }
    }
}

/// A wrapper for the Windows OVERLAPPED structure with additional metadata.
#[repr(C)]
pub struct OverlappedEntry {
    /// The underlying Windows Overlapped structure.
    inner: Overlapped,
    /// Token associated with the operation.
    token: OpToken,
    /// Generation of the request currently represented by this overlapped entry.
    ///
    /// A multishot operation reuses the same `OVERLAPPED` allocation for each replacement
    /// request.  Keeping the request generation beside the slot token makes the replacement
    /// boundary explicit and gives completion hooks one more ownership invariant to validate.
    request_generation: Generation,
    /// Whether the operation is currently in-flight in the kernel.
    in_flight: bool,
    /// Result of an offloaded blocking operation.
    blocking_completion: Option<Arc<BlockingCompletion>>,
    /// Resolved handle captured during submission to avoid re-resolving Fixed fd on hot paths.
    resolved_handle: Option<RawHandle>,
    /// Socket inflight ownership acquired before a kernel-pending socket submit.
    socket_inflight: Option<SocketInflightToken>,
    receive_request: Option<ReceiveRequest>,
}

impl OverlappedEntry {
    /// Creates a new `OverlappedEntry` with the given operation token.
    pub(crate) fn new(token: OpToken) -> Self {
        let mut entry = Self {
            inner: Overlapped::zeroed(),
            token,
            request_generation: Generation::ZERO,
            in_flight: false,
            blocking_completion: None,
            resolved_handle: None,
            socket_inflight: None,
            receive_request: None,
        };
        entry.reset_for_token(token);
        entry
    }

    pub(crate) fn reset_for_token(&mut self, token: OpToken) {
        self.inner = Overlapped::zeroed();
        self.token = token;
        self.request_generation = Generation::ZERO;
        self.in_flight = false;
        self.blocking_completion = None;
        self.resolved_handle = None;
        self.socket_inflight = None;
        self.receive_request = None;
    }

    pub(crate) fn inner(&self) -> &Overlapped {
        &self.inner
    }

    pub(crate) fn inner_mut(&mut self) -> &mut Overlapped {
        &mut self.inner
    }

    pub(crate) const fn token(&self) -> OpToken {
        self.token
    }

    pub(crate) fn request_generation(&self) -> Generation {
        self.request_generation
    }

    pub(crate) fn request_generation_mut(&mut self) -> &mut Generation {
        &mut self.request_generation
    }

    pub(crate) const fn in_flight(&self) -> bool {
        self.in_flight
    }

    pub(crate) fn in_flight_mut(&mut self) -> &mut bool {
        &mut self.in_flight
    }

    pub(crate) fn blocking_completion_mut(&mut self) -> &mut Option<Arc<BlockingCompletion>> {
        &mut self.blocking_completion
    }

    pub(crate) const fn resolved_handle(&self) -> Option<RawHandle> {
        self.resolved_handle
    }

    pub(crate) fn resolved_handle_mut(&mut self) -> &mut Option<RawHandle> {
        &mut self.resolved_handle
    }

    pub(crate) fn socket_inflight(&self) -> Option<&SocketInflightToken> {
        self.socket_inflight.as_ref()
    }

    pub(crate) fn socket_inflight_mut(&mut self) -> &mut Option<SocketInflightToken> {
        &mut self.socket_inflight
    }

    pub(crate) const fn receive_request(&self) -> Option<ReceiveRequest> {
        self.receive_request
    }

    pub(crate) fn receive_request_mut(&mut self) -> &mut Option<ReceiveRequest> {
        &mut self.receive_request
    }
}

impl Default for OverlappedEntry {
    fn default() -> Self {
        Self::new(
            OpToken::from_registry_parts(0, Generation::ZERO)
                .expect("zero token should be encodable"),
        )
    }
}

/// # Safety
///
/// `OverlappedEntry` contains raw pointers inside Windows OS native `OVERLAPPED` structure (`inner`)
/// and `AtomicOptionPtr` (`blocking_completion`), preventing compiler auto-derivation of `Send`:
///
/// 1. `inner`: Raw pointer fields (`Internal`, `Pointer`) are internal OS control block state.
///    When `OverlappedEntry` is moved between threads, exclusive ownership is transferred, so no
///    data races occur.
/// 2. `blocking_completion`: Worker threads in the blocking pool write results via `Box::into_raw` + `Release`,
///    and the driver thread consumes results via `Acquire`, forming a safe release-acquire synchronization.
/// 3. `Drop for BlockingCompletion`: If unconsumed when dropped, `Box::from_raw` reclaims the allocation,
///    and `cleanup_success` safely frees the handle, preventing resource leaks.
unsafe impl Send for OverlappedEntry {}

/// State associated with an IOCP operation.
#[derive(Default)]
pub struct IocpOpState {
    pub(crate) generation: Generation,
    pub(crate) timer_id: Option<TimerId>,
    pub(crate) timer_deadline: Option<Instant>,
    pub(crate) is_background: bool,
    /// Whether user-visible cancellation was accepted for this IOCP operation.
    ///
    /// This is a logical operation state only. It does not mean that `CancelIoEx` was accepted,
    /// and it must not be used for orphan cleanup.
    pub(crate) iocp_user_cancel_requested: bool,
    /// Whether user-visible cancellation was accepted for this RIO operation.
    ///
    /// This is a logical operation state only. It does not mean that a cancellation request was
    /// submitted to the RIO kernel, and it must not be used for orphan cleanup.
    pub(crate) rio_user_cancel_requested: bool,
}

pub(crate) type IocpOpRegistry = CoreOpRegistry<IocpSlotSpec>;
pub(crate) type Slot<'a, State> = CoreSlot<'a, State, IocpSlotSpec>;
