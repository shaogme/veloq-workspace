use veloq_blocking::ThreadPool;
use veloq_std::{
    boxed::Box,
    mem,
    sync::{OnceLock, mpsc},
    thread::Builder,
    time::{Duration, Instant},
    vec::Vec,
};

use diagweave::prelude::*;
use tracing::debug;
use veloq_buf::{BufferRegistrar, NoopRegistrar};
use veloq_driver_core::{
    driver::{CancelRequest, CompletionAccess, SharedCompletionTable},
    slot::{CheckedSlotView, SlotRegistryExt, SlotView},
};
use windows_sys::Win32::Networking::WinSock::{WSACleanup, WSADATA, WSAGetLastError, WSAStartup};

use crate::{
    config::{BorrowedRawHandle, BufferRegistrationMode, IocpConfig, IocpHandle, RawHandle},
    error::{IocpError, IocpResult},
    ext::Extensions,
    op::IocpSlotSpec,
    rio::RioState,
    win32::IoCompletionPort,
};

use super::{
    CloseMode, IocpDriver, IocpDriverCompletionDiagnostics, IocpDriverState, IocpOpRegistry,
    PreInit,
    polling::{CompletionPump, TimerEngine},
    registration::HandleRegistry,
};

#[derive(Clone, Copy, Default)]
pub(super) struct ShutdownPending {
    pub(super) iocp_pending: usize,
    pub(super) rio_pending: usize,
}

#[derive(Clone, Copy)]
enum ShutdownOpKind {
    Iocp,
    Rio,
    Immediate,
}

pub(super) struct IocpRioRuntime {
    state: RioState,
}

/// Owns one successful Winsock startup for an IOCP driver.
///
/// Winsock keeps a process-wide reference count. Each driver acquires one
/// reference so initialization failures and driver drops release exactly the
/// startup performed for that driver.
pub(super) struct WinsockGuard;

const IOCP_REAPER_DRAIN_TIMEOUT: Duration = Duration::from_secs(5);

struct DeferredIocpCleanup {
    state: Box<IocpDriverState>,
}

// SAFETY: the boxed state is transferred to exactly one reaper thread, which owns all polling
// and destruction after the handoff. No caller-borrowed registrar is included in the state.
unsafe impl Send for DeferredIocpCleanup {}

impl DeferredIocpCleanup {
    fn run(self) {
        static NOOP_REGISTRAR: NoopRegistrar = NoopRegistrar;

        let mut driver = IocpDriver {
            registrar: &NOOP_REGISTRAR,
            state: Some(self.state),
        };
        driver.state_mut().shutting_down = true;

        if let Err(error) = driver.drain_orphaned(IOCP_REAPER_DRAIN_TIMEOUT) {
            tracing::warn!(error = ?error, "IocpReaper: deferred drain timed out or failed");
            // The kernel may still own an OVERLAPPED or a socket buffer. Deliberately leak the
            // entire state instead of allowing its fields to be dropped out of order.
            mem::forget(driver);
            return;
        }

        driver.state_mut().closed = true;
        drop(driver);
    }
}

fn iocp_reaper_sender() -> Option<&'static mpsc::Sender<DeferredIocpCleanup>> {
    static SENDER: OnceLock<Option<mpsc::Sender<DeferredIocpCleanup>>> = OnceLock::new();
    let sender = SENDER.get_or_init(|| {
        let (tx, rx) = mpsc::channel::<DeferredIocpCleanup>();
        match Builder::new()
            .name("veloq-iocp-reaper".into())
            .spawn(move || {
                while let Ok(task) = rx.recv() {
                    task.run();
                }
            }) {
            Ok(_) => Some(tx),
            Err(error) => {
                tracing::error!(error = ?error, "failed to spawn veloq-iocp-reaper");
                None
            }
        }
    });
    sender.as_ref()
}

impl IocpRioRuntime {
    pub(super) fn new(
        port: BorrowedRawHandle<'_>,
        entries: u32,
        ext: &Extensions,
        registration_mode: BufferRegistrationMode,
        diagnostics: IocpDriverCompletionDiagnostics,
    ) -> IocpResult<Self> {
        let state = RioState::new(port, entries, ext, registration_mode, diagnostics)
            .with_ctx("entries", entries)
            .with_ctx("port_raw", port.raw().as_handle() as usize)
            .attach_note("failed to initialize RIO state")
            .trans()?;
        Ok(Self { state })
    }

    pub(super) fn state(&self) -> &RioState {
        &self.state
    }

    pub(super) fn state_mut(&mut self) -> &mut RioState {
        &mut self.state
    }

    pub(super) fn state_and_registrar_mut<'state, 'registrar>(
        &'state mut self,
        registrar: &'registrar (dyn BufferRegistrar + 'registrar),
    ) -> (
        &'state mut RioState,
        &'registrar (dyn BufferRegistrar + 'registrar),
    ) {
        (&mut self.state, registrar)
    }
}

impl<'a> IocpDriver<'a> {
    /// Creates a pre-initialization completion port handle.
    pub(crate) fn create_pre_init() -> IocpResult<PreInit> {
        IoCompletionPort::new(0).attach_note("failed to create pre-init IOCP")
    }

    /// Creates a new IOCP driver instance.
    pub fn new(
        config: impl AsRef<IocpConfig>,
        registrar: &'a (dyn BufferRegistrar + 'a),
    ) -> IocpResult<Self> {
        let cfg = config.as_ref();
        let pre = Self::create_pre_init()?;
        let blocking_pool = ThreadPool::new(
            cfg.blocking_pool.core_threads,
            cfg.blocking_pool.max_threads,
            cfg.blocking_pool.queue_capacity,
            cfg.blocking_pool.keep_alive,
        );
        Self::new_from_pre_init(
            cfg.entries.get(),
            pre,
            cfg.registration_mode,
            registrar,
            blocking_pool,
        )
    }

    /// Creates a new IOCP driver from a pre-initialized handle.
    pub(crate) fn new_from_pre_init(
        entries: u32,
        port_val: PreInit,
        registration_mode: BufferRegistrationMode,
        registrar: &'a (dyn BufferRegistrar + 'a),
        blocking_pool: ThreadPool,
    ) -> IocpResult<Self> {
        let winsock = Self::start_winsock()?;

        let port_handle = port_val.as_raw();
        debug!(port = ?port_handle, "Initializing IocpDriver");
        let extensions = Extensions::new()
            .with_ctx("port_raw", port_handle as usize)
            .attach_note("failed to load IOCP extensions")?;
        let ops = IocpOpRegistry::new(entries as usize);
        let completion_table: SharedCompletionTable<IocpSlotSpec> = ops.shared_table();
        let completion_diagnostics = ops.shared.completion_diagnostics();
        let rio = IocpRioRuntime::new(
            RawHandle::new(IocpHandle::for_file(port_handle)).borrow(),
            entries,
            &extensions,
            registration_mode,
            completion_diagnostics.clone(),
        )
        .attach_note("failed to initialize RIO runtime")?;
        let (remote_cancel_sender, remote_cancel_receiver) = mpsc::channel();
        Ok(Self {
            registrar,
            state: Some(Box::new(IocpDriverState {
                completion: CompletionPump::new(port_val, completion_table),
                ops,
                extensions,
                timer: TimerEngine::new(),
                remote_cancel_sender,
                remote_cancel_receiver,
                completion_diagnostics,
                rio,
                handles: HandleRegistry::new(),
                shutting_down: false,
                closed: false,
                blocking_pool,
                _winsock: winsock,
            })),
        })
    }

    fn start_winsock() -> IocpResult<WinsockGuard> {
        // SAFETY: WSAStartup is required before Windows socket APIs are used.
        let ret = unsafe {
            let mut data: WSADATA = mem::zeroed();
            WSAStartup(0x0202, &mut data)
        };
        if ret != 0 {
            return IocpError::DriverInit
                .push_ctx("scope", "iocp/driver")
                .set_error_code(ret)
                .attach_note("WSAStartup failed");
        }
        Ok(WinsockGuard)
    }

    pub(super) fn shutdown_ops(&mut self) -> IocpResult<ShutdownPending> {
        if self.state().shutting_down {
            return Ok(ShutdownPending::default());
        }
        self.state_mut().shutting_down = true;
        self.state_mut()
            .rio
            .state_mut()
            .stop_accepting_new_submissions();

        let mut in_flight = Vec::new();
        let mut pending = ShutdownPending::default();
        for token in self.state().ops.active_tokens().collect::<Vec<_>>() {
            let kind = match self.state_mut().ops.checked_slot_view(token)? {
                CheckedSlotView::Valid(SlotView::InFlightWaiting(mut slot)) => {
                    if slot.platform().timer_id.is_some() {
                        Some(ShutdownOpKind::Immediate)
                    } else if slot
                        .with_access_mut(|access| Self::is_rio_op(access.operation().get_ref()))
                        .unwrap_or(false)
                    {
                        Some(ShutdownOpKind::Rio)
                    } else {
                        Some(ShutdownOpKind::Iocp)
                    }
                }
                CheckedSlotView::Valid(SlotView::InFlightOrphaned(mut slot)) => {
                    if slot.platform().timer_id.is_some() {
                        Some(ShutdownOpKind::Immediate)
                    } else if slot
                        .with_access_mut(|access| Self::is_rio_op(access.operation().get_ref()))
                        .unwrap_or(false)
                    {
                        Some(ShutdownOpKind::Rio)
                    } else {
                        Some(ShutdownOpKind::Iocp)
                    }
                }
                _ => None,
            };

            let Some(kind) = kind else {
                continue;
            };
            match kind {
                ShutdownOpKind::Iocp => pending.iocp_pending += 1,
                ShutdownOpKind::Rio => pending.rio_pending += 1,
                ShutdownOpKind::Immediate => {}
            }
            // Driver shutdown abandons the user-visible operation before requesting backend
            // cleanup. Keep this lifecycle marker separate from the RIO user-cancel flag so an
            // eventual completion cannot be reported as a user cancellation.
            self.state().ops.shared.mark_orphaned(token);
            in_flight.push(token);
        }
        for token in in_flight {
            if let Err(error) = self.cancel_op_internal(CancelRequest::abandon(token)) {
                tracing::warn!(
                    slot_index = token.index(),
                    slot_generation = token.generation().get(),
                    report = ?error,
                    "IocpDriver: abandon cancel failed; deferred drain will retain the slot"
                );
            }
        }
        Ok(pending)
    }

    pub(super) fn drain_pending_all(
        &mut self,
        pending_iocp_count: usize,
        timeout: Duration,
    ) -> IocpResult<()> {
        let mut drained_iocp = 0usize;
        let deadline = Instant::now().checked_add(timeout).ok_or_else(|| {
            IocpError::CompletionWait
                .to_report()
                .push_ctx("scope", "iocp/driver")
                .attach_note("strict close timeout is too large")
        })?;

        while drained_iocp < pending_iocp_count
            || self.state().rio.state().rio_outstanding_count > 0
        {
            let now = Instant::now();
            if now >= deadline {
                return Err(
                    IocpError::CompletionWait.report("iocp/driver", "strict close drain timed out")
                );
            }
            let count = self.poll_completion(deadline.saturating_duration_since(now))?;
            drained_iocp += count;
        }
        Ok(())
    }

    /// Moves all slot-owned RIO state out of the driver before its registry is dropped.
    ///
    /// A fast close cannot wait for the RIO CQ, but the kernel may still dereference both the
    /// operation payload and the user payload. The reaper therefore owns both until every valid
    /// request context has released its backend effect. This path is deliberately independent
    /// of `rio_user_cancel_requested`: fast close is orphan cleanup, not user cancellation.
    fn preserve_rio_payloads_for_fast_close(&mut self) {
        let mut rio_slots = Vec::new();
        for token in self.state().ops.active_tokens().collect::<Vec<_>>() {
            let is_rio = match self.state_mut().ops.checked_slot_view(token) {
                Ok(CheckedSlotView::Valid(SlotView::InFlightWaiting(mut slot))) => slot
                    .with_access_mut(|access| Self::is_rio_op(access.operation().get_ref()))
                    .unwrap_or(false),
                Ok(CheckedSlotView::Valid(SlotView::InFlightOrphaned(mut slot))) => slot
                    .with_access_mut(|access| Self::is_rio_op(access.operation().get_ref()))
                    .unwrap_or(false),
                _ => false,
            };
            if is_rio {
                rio_slots.push(token);
            }
        }
        let mut kernel_ops = Vec::new();
        let mut payloads = Vec::new();
        for token in rio_slots {
            match self.state_mut().ops.checked_slot_view(token) {
                Ok(CheckedSlotView::Valid(SlotView::InFlightWaiting(slot))) => {
                    let mut draining = slot.complete();
                    let mut op = draining
                        .take_op()
                        .expect("RIO in-flight slot must retain its kernel operation");
                    op.unbind_user_payload();
                    kernel_ops.push(op);
                    let (payload, _result) = draining.take_completion_data();
                    if let Some(payload) = payload {
                        payloads.push(payload);
                    }
                }
                Ok(CheckedSlotView::Valid(SlotView::InFlightOrphaned(slot))) => {
                    let mut draining = slot.complete();
                    let mut op = draining
                        .take_op()
                        .expect("RIO orphaned slot must retain its kernel operation");
                    op.unbind_user_payload();
                    kernel_ops.push(op);
                    let (payload, _result) = draining.take_completion_data();
                    if let Some(payload) = payload {
                        payloads.push(payload);
                    }
                }
                _ => {}
            }
        }

        self.state_mut()
            .rio
            .state_mut()
            .defer_kernel_ops(kernel_ops);
        self.state_mut().rio.state_mut().defer_payloads(payloads);
    }

    pub(super) fn close_impl(&mut self, mode: CloseMode) -> IocpResult<()> {
        if self.state().closed {
            return Ok(());
        }
        let pending = self.shutdown_ops()?;
        if let CloseMode::Strict { timeout } = mode {
            self.drain_pending_all(pending.iocp_pending, timeout)
                .push_ctx("scope", "iocp/driver")
                .attach_note("strict close drain pending requests timed out")?;
            self.drain_deferred_socket_cleanup();
            self.state_mut()
                .rio
                .state_mut()
                .forget_runtime_after_drain()
                .trans()?;
            self.state_mut().rio.state_mut().kernel.close();
            self.state_mut().closed = true;
            return Ok(());
        }

        self.preserve_rio_payloads_for_fast_close();
        self.drain_deferred_socket_cleanup();
        let needs_deferred_cleanup = pending.iocp_pending > 0
            || pending.rio_pending > 0
            || self.state().ops.has_active_ops()
            || self.state().rio.state().rio_outstanding_count > 0
            || self.state().rio.state().has_socket_inflight()
            || self.state().handles.deferred_cleanup_len() > 0;
        self.state_mut().closed = true;
        if needs_deferred_cleanup {
            let task = DeferredIocpCleanup {
                state: self.take_state(),
            };
            if let Some(sender) = iocp_reaper_sender() {
                if let Err(error) = sender.send(task) {
                    tracing::warn!(
                        "IocpReaper unavailable, falling back to inline deferred cleanup"
                    );
                    error.0.run();
                }
            } else {
                tracing::warn!(
                    "IocpReaper thread failed to start, falling back to inline deferred cleanup"
                );
                task.run();
            }
        }
        Ok(())
    }
}

impl Drop for IocpDriver<'_> {
    fn drop(&mut self) {
        if self.state.is_none() {
            return;
        }
        debug!("Dropping IocpDriver");
        if let Err(e) = self.close_impl(CloseMode::Fast) {
            tracing::error!(report = ?e, "iocp close_impl fast failed during drop");
        }
    }
}

impl Drop for WinsockGuard {
    fn drop(&mut self) {
        // SAFETY: This guard is only constructed after a successful WSAStartup.
        let ret = unsafe { WSACleanup() };
        if ret != 0 {
            // SAFETY: WSAGetLastError reads the calling thread's Winsock error code.
            let code = unsafe { WSAGetLastError() };
            tracing::error!(error_code = code, "WSACleanup failed");
        }
    }
}
