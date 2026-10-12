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
    driver::{CancelRequest, CompletionAccess, OpToken, SharedCompletionTable},
    slot::{CheckedSlotView, SlotRegistryExt, SlotState, SlotView},
};
use windows_sys::Win32::Networking::WinSock::{WSACleanup, WSADATA, WSAGetLastError, WSAStartup};

use crate::{
    config::{BorrowedRawHandle, BufferRegistrationMode, IocpConfig, IocpHandle, RawHandle},
    error::{IocpError, IocpResult},
    ext::Extensions,
    op::{IocpOpRegistry, IocpSlotSpec},
    rio::RioState,
    win32::IoCompletionPort,
};

use super::{
    CloseMode, IocpDriverCompletionDiagnostics, IocpDriverState, PreInit,
    cancellation::CancellationService,
    polling::{CompletionPump, PollingService, TimerEngine},
    registration::{HandleRegistry, RegistrationService},
    state::{IocpDriverStateParts, LifecycleStateView},
};

#[derive(Default)]
pub(super) struct ShutdownPending {
    pub(super) iocp_pending: usize,
    pub(super) rio_pending: usize,
    pub(super) iocp_targets: Vec<OpToken>,
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

/// 为一个 IOCP driver 持有一次成功的 Winsock startup。
/// Owns one successful Winsock startup for an IOCP driver.
///
/// Winsock 使用进程级引用计数；每个 driver 获取一个引用，确保初始化失败或 driver 销毁时只释放自己的 startup。
/// Winsock uses a process-wide reference count; each driver acquires one reference so failures and drops release only its own startup.
pub(super) struct WinsockGuard;

pub(super) struct LifecycleService<'view, 'state, 'registrar> {
    state: &'view mut LifecycleStateView<'state>,
    registrar: &'registrar dyn BufferRegistrar,
}

impl<'view, 'state, 'registrar> LifecycleService<'view, 'state, 'registrar> {
    pub(super) fn new(
        state: &'view mut LifecycleStateView<'state>,
        registrar: &'registrar dyn BufferRegistrar,
    ) -> Self {
        Self { state, registrar }
    }

    fn state(&self) -> &LifecycleStateView<'state> {
        self.state
    }

    fn state_mut(&mut self) -> &mut LifecycleStateView<'state> {
        self.state
    }
}

const IOCP_REAPER_DRAIN_TIMEOUT: Duration = Duration::from_secs(5);

struct DeferredIocpCleanup {
    state: Box<IocpDriverState>,
}

// SAFETY: boxed state 只移交给一个 reaper 线程，之后由它独占轮询和销毁；状态不包含调用方借用的 registrar。
// SAFETY: the boxed state moves to one reaper thread for exclusive polling and destruction; it contains no caller-borrowed registrar.
unsafe impl Send for DeferredIocpCleanup {}

impl DeferredIocpCleanup {
    fn run(self) {
        static NOOP_REGISTRAR: NoopRegistrar = NoopRegistrar;

        let mut state = self.state;
        let result = {
            let mut view = state.lifecycle_view();
            *view.shutting_down = true;
            let result = LifecycleService::new(&mut view, &NOOP_REGISTRAR)
                .drain_orphaned(IOCP_REAPER_DRAIN_TIMEOUT);
            if result.is_ok() {
                *view.closed = true;
            }
            result
        };
        if let Err(error) = result {
            tracing::warn!(error = ?error, "IocpReaper: deferred drain timed out or failed");
            // 内核可能仍持有 OVERLAPPED 或 socket 缓冲区，因此泄漏完整状态以保留其生命周期。
            // The kernel may still own an OVERLAPPED or socket buffer, so leak the full state.
            mem::forget(state);
            return;
        }
        drop(state);
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

    #[cfg(feature = "test-hooks")]
    pub(super) fn debug_chunk_register_attempts(&self) -> u64 {
        self.state.debug_chunk_register_attempts()
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

impl LifecycleService<'_, '_, '_> {
    /// 创建初始化前使用的 completion port 句柄。
    /// Creates a completion-port handle for pre-initialization use.
    pub(super) fn create_pre_init() -> IocpResult<PreInit> {
        IoCompletionPort::new(0).attach_note("failed to create pre-init IOCP")
    }

    /// 创建新的 IOCP driver 实例。
    /// Creates a new IOCP driver instance.
    pub(super) fn build(
        config: impl AsRef<IocpConfig>,
        registrar: &dyn BufferRegistrar,
    ) -> IocpResult<IocpDriverState> {
        let cfg = config.as_ref();
        let pre = Self::create_pre_init()?;
        let blocking_config = cfg.blocking_pool();
        let blocking_pool = ThreadPool::new(
            blocking_config.core_threads,
            blocking_config.max_threads,
            blocking_config.queue_capacity,
            blocking_config.keep_alive,
        );
        Self::build_from_pre_init(
            cfg.entries().get(),
            pre,
            cfg.get_registration_mode(),
            registrar,
            blocking_pool,
        )
    }

    /// 从预初始化句柄创建新的 IOCP driver。
    /// Creates a new IOCP driver from a pre-initialized handle.
    pub(super) fn build_from_pre_init(
        entries: u32,
        port_val: PreInit,
        registration_mode: BufferRegistrationMode,
        _registrar: &dyn BufferRegistrar,
        blocking_pool: ThreadPool,
    ) -> IocpResult<IocpDriverState> {
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
        Ok(IocpDriverState::new(IocpDriverStateParts {
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
            winsock,
        }))
    }

    fn start_winsock() -> IocpResult<WinsockGuard> {
        // SAFETY: 调用 Windows socket API 前必须成功执行 WSAStartup。
        // SAFETY: WSAStartup must succeed before Windows socket APIs are used.
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
        if *self.state().shutting_down {
            return Ok(ShutdownPending::default());
        }
        *self.state_mut().shutting_down = true;
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
                        .with_access_mut(|access| access.operation().get_ref().uses_rio_backend())
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
                        .with_access_mut(|access| access.operation().get_ref().uses_rio_backend())
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
                ShutdownOpKind::Iocp => {
                    pending.iocp_pending += 1;
                    pending.iocp_targets.push(token);
                }
                ShutdownOpKind::Rio => pending.rio_pending += 1,
                ShutdownOpKind::Immediate => {}
            }
            // shutdown 先放弃用户可见操作再请求后端清理；生命周期标记与 RIO 用户取消标志分开，避免把最终完成误报为用户取消。
            // Shutdown abandons the user-visible operation before backend cleanup; keep this marker separate from RIO user cancellation.
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
        pending: &ShutdownPending,
        timeout: Duration,
    ) -> IocpResult<()> {
        let deadline = Instant::now().checked_add(timeout).ok_or_else(|| {
            IocpError::CompletionWait
                .to_report()
                .push_ctx("scope", "iocp/driver")
                .attach_note("strict close timeout is too large")
        })?;

        while Self::strict_drain_pending(
            self.has_pending_iocp_targets(&pending.iocp_targets)?,
            self.state().rio.state().rio_outstanding_count(),
        ) {
            let now = Instant::now();
            if now >= deadline {
                return Err(
                    IocpError::CompletionWait.report("iocp/driver", "strict close drain timed out")
                );
            }
            let _ = self.poll_completion(deadline.saturating_duration_since(now))?;
        }
        Ok(())
    }

    fn has_pending_iocp_targets(&mut self, targets: &[OpToken]) -> IocpResult<bool> {
        for token in targets {
            match self.state_mut().ops.checked_slot_view(*token)? {
                CheckedSlotView::Valid(SlotView::InFlightWaiting(slot))
                    if Self::is_pending_iocp_target_state(slot.snapshot().status.state) =>
                {
                    return Ok(true);
                }
                CheckedSlotView::Valid(SlotView::InFlightOrphaned(slot))
                    if Self::is_pending_iocp_target_state(slot.snapshot().status.state) =>
                {
                    return Ok(true);
                }
                _ => {}
            }
        }
        Ok(false)
    }

    fn is_pending_iocp_target_state(state: SlotState) -> bool {
        matches!(
            state,
            SlotState::InFlightWaiting | SlotState::InFlightOrphaned
        )
    }

    fn strict_drain_pending(target_slot_active: bool, rio_outstanding: usize) -> bool {
        target_slot_active || rio_outstanding > 0
    }

    /// 在 registry 销毁前，将 slot 所有的 RIO 状态移出 driver。
    /// Moves all slot-owned RIO state out of the driver before its registry is dropped.
    ///
    /// fast close 不等待 RIO CQ，但内核仍可能解引用 operation 和 user payload；reaper 持有二者，直到所有有效请求上下文释放后端效果。此路径独立于 `rio_user_cancel_requested`，表示孤儿清理而非用户取消。
    /// Fast close cannot wait for the RIO CQ while the kernel may still dereference either payload; the reaper retains both until valid request contexts release their backend effects. This is orphan cleanup, not user cancellation.
    fn preserve_rio_payloads_for_fast_close(&mut self) {
        let mut rio_slots = Vec::new();
        for token in self.state().ops.active_tokens().collect::<Vec<_>>() {
            let is_rio = match self.state_mut().ops.checked_slot_view(token) {
                Ok(CheckedSlotView::Valid(SlotView::InFlightWaiting(mut slot))) => slot
                    .with_access_mut(|access| access.operation().get_ref().uses_rio_backend())
                    .unwrap_or(false),
                Ok(CheckedSlotView::Valid(SlotView::InFlightOrphaned(mut slot))) => slot
                    .with_access_mut(|access| access.operation().get_ref().uses_rio_backend())
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

    pub(super) fn close_impl(&mut self, mode: CloseMode) -> IocpResult<bool> {
        if *self.state().closed {
            return Ok(false);
        }
        let pending = self.shutdown_ops()?;
        if let CloseMode::Strict { timeout } = mode {
            self.drain_pending_all(&pending, timeout)
                .push_ctx("scope", "iocp/driver")
                .attach_note("strict close drain pending requests timed out")?;
            self.drain_deferred_socket_cleanup();
            self.state_mut()
                .rio
                .state_mut()
                .forget_runtime_after_drain()
                .trans()?;
            self.state_mut().rio.state_mut().close_kernel();
            *self.state_mut().closed = true;
            return Ok(false);
        }

        self.preserve_rio_payloads_for_fast_close();
        self.drain_deferred_socket_cleanup();
        let needs_deferred_cleanup = pending.iocp_pending > 0
            || pending.rio_pending > 0
            || self.state().ops.has_active_ops()
            || self.state().rio.state().rio_outstanding_count() > 0
            || self.state().rio.state().has_socket_inflight()
            || self.state().handles.deferred_cleanup_len() > 0;
        *self.state_mut().closed = true;
        Ok(needs_deferred_cleanup)
    }

    fn cancel_op_internal(&mut self, request: CancelRequest) -> IocpResult<()> {
        let registrar = self.registrar;
        let mut view = IocpDriverState::cancellation_view_from_lifecycle(self.state_mut());
        let _ = CancellationService::new(&mut view, registrar).cancel_op_internal(request)?;
        Ok(())
    }

    fn poll_completion(&mut self, timeout: Duration) -> IocpResult<usize> {
        let registrar = self.registrar;
        let mut view = IocpDriverState::polling_view_from_lifecycle(self.state_mut());
        PollingService::new(&mut view, registrar).poll_completion(timeout)
    }

    fn drain_deferred_socket_cleanup(&mut self) {
        let mut view = IocpDriverState::registration_view_from_lifecycle(self.state_mut());
        RegistrationService::new(&mut view).drain_deferred_socket_cleanup();
    }

    pub(super) fn drain_orphaned(&mut self, timeout: Duration) -> IocpResult<()> {
        let registrar = self.registrar;
        let mut view = IocpDriverState::polling_view_from_lifecycle(self.state_mut());
        PollingService::new(&mut view, registrar).drain_orphaned(timeout)
    }

    pub(super) fn schedule_deferred_cleanup(state: Box<IocpDriverState>) {
        let task = DeferredIocpCleanup { state };
        if let Some(sender) = iocp_reaper_sender() {
            if let Err(error) = sender.send(task) {
                tracing::warn!("IocpReaper unavailable; running deferred cleanup inline");
                error.0.run();
            }
        } else {
            tracing::warn!("IocpReaper thread failed to start; running deferred cleanup inline");
            task.run();
        }
    }
}

impl Drop for WinsockGuard {
    fn drop(&mut self) {
        // SAFETY: 仅在 WSAStartup 成功后才会构造此 guard。
        // SAFETY: this guard is constructed only after WSAStartup succeeds.
        let ret = unsafe { WSACleanup() };
        if ret != 0 {
            // SAFETY: WSAGetLastError 读取当前线程的 Winsock 错误码。
            // SAFETY: WSAGetLastError reads the calling thread's Winsock error code.
            let code = unsafe { WSAGetLastError() };
            tracing::error!(error_code = code, "WSACleanup failed");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::LifecycleService;
    use veloq_driver_core::slot::SlotState;

    #[test]
    fn unrelated_control_completion_cannot_finish_strict_iocp_drain() {
        let control_completion_count = 1;
        let target_state_after_control = SlotState::InFlightOrphaned;
        assert!(control_completion_count > 0);
        assert!(LifecycleService::strict_drain_pending(
            LifecycleService::is_pending_iocp_target_state(target_state_after_control),
            0,
        ));
        assert!(!LifecycleService::strict_drain_pending(
            LifecycleService::is_pending_iocp_target_state(SlotState::Idle),
            0,
        ));
    }
}
