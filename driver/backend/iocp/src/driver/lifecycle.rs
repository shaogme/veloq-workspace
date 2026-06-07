use std::sync::Arc;
use std::time::{Duration, Instant};

use diagweave::prelude::*;
use tracing::debug;
use veloq_buf::BufferRegistrar;
use veloq_driver_core::driver::SharedCompletionTable;
use veloq_driver_core::slot::{DetachedCancelTable, SlotRegistryExt, SlotView};

use crate::config::{BorrowedRawHandle, BufferRegistrationMode, IocpConfig, IocpHandle};
use crate::error::{IocpError, IocpResult};
use crate::ext::Extensions;
use crate::op::IocpUserPayload;
use crate::rio::RioState;

use super::polling::{CompletionPump, TimerEngine};
use super::registration::HandleRegistry;
use super::{CloseMode, IocpDriver, IocpDriverResult, IocpOpRegistry, PreInit};

pub(super) struct IocpRioRuntime<'a> {
    state: RioState,
    registrar: Box<dyn BufferRegistrar + 'a>,
}

/// Owns one successful Winsock startup for an IOCP driver.
///
/// Winsock keeps a process-wide reference count. Each driver acquires one
/// reference so initialization failures and driver drops release exactly the
/// startup performed for that driver.
pub(super) struct WinsockGuard;

impl<'a> IocpRioRuntime<'a> {
    pub(super) fn new(
        port: BorrowedRawHandle<'_>,
        entries: u32,
        ext: &Extensions,
        registration_mode: BufferRegistrationMode,
        registrar: Box<dyn BufferRegistrar + 'a>,
    ) -> IocpResult<Self> {
        let state = RioState::new(port, entries, ext, registration_mode)
            .with_ctx("entries", entries)
            .with_ctx("port_raw", port.raw().as_handle() as usize)
            .attach_note("failed to initialize RIO state")
            .trans()?;
        Ok(Self { state, registrar })
    }

    pub(super) fn state(&self) -> &RioState {
        &self.state
    }

    pub(super) fn state_mut(&mut self) -> &mut RioState {
        &mut self.state
    }

    pub(super) fn state_and_registrar_mut(
        &mut self,
    ) -> (&mut RioState, &(dyn BufferRegistrar + 'a)) {
        (&mut self.state, self.registrar.as_ref())
    }
}

impl<'a> IocpDriver<'a> {
    /// Creates a pre-initialization completion port handle.
    pub(crate) fn create_pre_init() -> IocpResult<PreInit> {
        crate::win32::IoCompletionPort::new(0).attach_note("failed to create pre-init IOCP")
    }

    /// Creates a new IOCP driver instance.
    pub fn new(
        config: impl AsRef<IocpConfig>,
        registrar: Box<dyn BufferRegistrar + 'a>,
    ) -> IocpResult<Self> {
        let cfg = config.as_ref();
        let pre = Self::create_pre_init()?;
        Self::new_from_pre_init(cfg.entries.get(), pre, cfg.registration_mode, registrar)
    }

    /// Creates a new IOCP driver from a pre-initialized handle.
    pub(crate) fn new_from_pre_init(
        entries: u32,
        port_val: PreInit,
        registration_mode: BufferRegistrationMode,
        registrar: Box<dyn BufferRegistrar + 'a>,
    ) -> IocpResult<Self> {
        let winsock = Self::start_winsock()?;

        let port_handle = port_val.as_raw();
        debug!(port = ?port_handle, "Initializing IocpDriver");
        let extensions = crate::ext::Extensions::new()
            .with_ctx("port_raw", port_handle as usize)
            .attach_note("failed to load IOCP extensions")?;
        let rio = IocpRioRuntime::new(
            crate::config::RawHandle::new(IocpHandle::for_file(port_handle)).borrow(),
            entries,
            &extensions,
            registration_mode,
            registrar,
        )
        .attach_note("failed to initialize RIO runtime")?;
        let ops = IocpOpRegistry::new(entries as usize);
        let completion_table: SharedCompletionTable<IocpUserPayload, IocpError> =
            ops.shared.clone();
        Ok(Self {
            completion: CompletionPump::new(port_val, completion_table),
            ops,
            extensions,
            timer: TimerEngine::new(),
            handles: HandleRegistry::new(),
            detached_cancel_table: Arc::new(DetachedCancelTable::new(entries as usize)),
            rio,
            shutting_down: false,
            closed: false,
            _winsock: winsock,
        })
    }

    fn start_winsock() -> IocpResult<WinsockGuard> {
        use windows_sys::Win32::Networking::WinSock::{WSADATA, WSAStartup};

        // SAFETY: WSAStartup is required before Windows socket APIs are used.
        let ret = unsafe {
            let mut data: WSADATA = std::mem::zeroed();
            WSAStartup(0x0202, &mut data)
        };
        if ret != 0 {
            return Err(IocpError::DriverInit
                .to_report()
                .push_ctx("scope", "iocp/driver")
                .set_error_code(ret)
                .attach_note("WSAStartup failed"));
        }
        Ok(WinsockGuard)
    }

    pub(super) fn shutdown_ops(&mut self) -> usize {
        if self.shutting_down {
            return 0;
        }
        self.shutting_down = true;
        self.rio.state_mut().begin_shutdown();

        let mut in_flight = Vec::new();
        for user_data in 0..self.ops.local.len() {
            if matches!(
                self.ops.slot_view(user_data),
                Some(SlotView::InFlightWaiting(_)) | Some(SlotView::InFlightOrphaned(_))
            ) {
                in_flight.push(user_data);
            }
        }
        let count = in_flight.len();
        for user_data in in_flight {
            self.cancel_op_internal(user_data);
        }
        count
    }

    pub(super) fn drain_pending_iocp(
        &mut self,
        pending_count: usize,
        timeout: Duration,
    ) -> IocpDriverResult<()> {
        if pending_count == 0 {
            return Ok(());
        }
        let mut drained = 0usize;
        let deadline = Instant::now().checked_add(timeout).ok_or_else(|| {
            IocpError::CompletionWait
                .to_report()
                .push_ctx("scope", "iocp/driver")
                .attach_note("strict close timeout is too large")
        })?;

        while drained < pending_count {
            let now = Instant::now();
            if now >= deadline {
                return Err(IocpError::CompletionWait.report("iocp/driver", "drain timed out"));
            }
            drained += self.poll_completion(deadline.saturating_duration_since(now))?;
        }
        Ok(())
    }

    pub(super) fn close_impl(&mut self, mode: CloseMode) -> IocpDriverResult<()> {
        if self.closed {
            return Ok(());
        }
        let pending = self.shutdown_ops();
        if let CloseMode::Strict { timeout } = mode {
            self.drain_pending_iocp(pending, timeout)
                .push_ctx("scope", "iocp/driver")
                .attach_note("drain pending iocp timed out")?;
            self.rio
                .state_mut()
                .drain_outstanding(timeout)
                .push_ctx("scope", "iocp/driver")
                .attach_note("failed to drain RIO outstanding requests")
                .trans()?;
        }
        self.rio.state_mut().kernel.close();
        self.closed = true;
        Ok(())
    }
}

impl Drop for IocpDriver<'_> {
    fn drop(&mut self) {
        debug!("Dropping IocpDriver");
        if let Err(e) = self.close_impl(CloseMode::Fast) {
            tracing::error!(report = ?e, "iocp close_impl fast failed during drop");
        }
    }
}

impl Drop for WinsockGuard {
    fn drop(&mut self) {
        use windows_sys::Win32::Networking::WinSock::{WSACleanup, WSAGetLastError};

        // SAFETY: This guard is only constructed after a successful WSAStartup.
        let ret = unsafe { WSACleanup() };
        if ret != 0 {
            // SAFETY: WSAGetLastError reads the calling thread's Winsock error code.
            let code = unsafe { WSAGetLastError() };
            tracing::error!(error_code = code, "WSACleanup failed");
        }
    }
}
