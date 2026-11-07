use diagweave::DiagnosticResult;
use veloq_blocking::ThreadPool;
use veloq_buf::AnyBufPool;
use veloq_std::sync::{Arc, mpsc};

use crate::{
    diagnostics::IocpCompletionDiagnostics,
    error::{IocpError, IocpResult},
    ext::Extensions,
    op::IocpOpRegistry,
};
use veloq_driver_core::{
    CancelRequest, DriverCompletionDiagnostics, RemoteCancelSender, RemoteWaker,
};

#[cfg(test)]
use veloq_driver_core::{CheckedSlotView, OpToken, SlotRegistryExt};

use super::{
    lifecycle::{IocpRioRuntime, WinsockGuard},
    overlapped::OverlappedRegistry,
    polling::{CompletionPump, TimerEngine},
    registration::HandleRegistry,
};

#[cfg(test)]
use crate::config::RegisteredHandle;
#[cfg(test)]
use veloq_std::sync::atomic::Ordering;

pub(crate) type IocpDriverCompletionDiagnostics =
    DriverCompletionDiagnostics<IocpCompletionDiagnostics>;

/// 地址稳定的状态，拥有内核可能引用的资源。 / Address-stable state owns resources referenced by the kernel.
pub(super) struct IocpDriverState {
    completion: CompletionPump,
    ops: IocpOpRegistry,
    overlapped_registry: OverlappedRegistry,
    extensions: Extensions,
    timer: TimerEngine,
    remote_cancel_sender: RemoteCancelSender,
    remote_cancel_receiver: mpsc::Receiver<CancelRequest>,
    completion_diagnostics: IocpDriverCompletionDiagnostics,
    rio: IocpRioRuntime,
    buffer_pool_owner: Option<AnyBufPool>,
    handles: HandleRegistry,
    shutting_down: bool,
    closed: bool,
    blocking_pool: ThreadPool,
    // 保持此字段最后，使 WSACleanup 在 socket/RIO 状态销毁后执行。
    // Keep this last so WSACleanup runs after socket/RIO state is destroyed.
    _winsock: WinsockGuard,
}

// SAFETY: 唯一的 reaper 在线程间接管整个 Box 后独占轮询和销毁状态，OVERLAPPED 保持原 slot 地址。
// SAFETY: The sole reaper exclusively polls and drops the whole Box; OVERLAPPED addresses stay in their slots.
unsafe impl Send for IocpDriverState {}

pub(super) struct IocpDriverStateParts {
    pub(super) completion: CompletionPump,
    pub(super) ops: IocpOpRegistry,
    pub(super) extensions: Extensions,
    pub(super) timer: TimerEngine,
    pub(super) remote_cancel_sender: RemoteCancelSender,
    pub(super) remote_cancel_receiver: mpsc::Receiver<CancelRequest>,
    pub(super) completion_diagnostics: IocpDriverCompletionDiagnostics,
    pub(super) rio: IocpRioRuntime,
    pub(super) buffer_pool_owner: Option<AnyBufPool>,
    pub(super) handles: HandleRegistry,
    pub(super) shutting_down: bool,
    pub(super) closed: bool,
    pub(super) blocking_pool: ThreadPool,
    pub(super) winsock: WinsockGuard,
}

pub(super) struct CancellationStateView<'a> {
    pub(super) completion: &'a mut CompletionPump,
    pub(super) ops: &'a mut IocpOpRegistry,
    pub(super) overlapped_registry: &'a mut OverlappedRegistry,
    pub(super) timer: &'a mut TimerEngine,
    pub(super) completion_diagnostics: &'a mut IocpDriverCompletionDiagnostics,
    pub(super) rio: &'a mut IocpRioRuntime,
    pub(super) handles: &'a mut HandleRegistry,
    pub(super) extensions: &'a Extensions,
    pub(super) shutting_down: bool,
}

pub(super) struct CompletionStateView<'a> {
    pub(super) completion: &'a mut CompletionPump,
    pub(super) ops: &'a mut IocpOpRegistry,
    pub(super) overlapped_registry: &'a mut OverlappedRegistry,
    pub(super) timer: &'a mut TimerEngine,
    pub(super) completion_diagnostics: &'a mut IocpDriverCompletionDiagnostics,
    pub(super) rio: &'a mut IocpRioRuntime,
    pub(super) handles: &'a mut HandleRegistry,
    pub(super) extensions: &'a Extensions,
    pub(super) shutting_down: bool,
}

pub(super) struct PollingStateView<'a> {
    pub(super) completion: &'a mut CompletionPump,
    pub(super) ops: &'a mut IocpOpRegistry,
    pub(super) overlapped_registry: &'a mut OverlappedRegistry,
    pub(super) timer: &'a mut TimerEngine,
    pub(super) completion_diagnostics: &'a mut IocpDriverCompletionDiagnostics,
    pub(super) rio: &'a mut IocpRioRuntime,
    pub(super) handles: &'a mut HandleRegistry,
    pub(super) extensions: &'a Extensions,
    pub(super) remote_cancel_receiver: &'a mut mpsc::Receiver<CancelRequest>,
    pub(super) shutting_down: bool,
}

pub(super) struct RegistrationStateView<'a> {
    pub(super) rio: &'a mut IocpRioRuntime,
    pub(super) handles: &'a mut HandleRegistry,
}

pub(super) struct SubmissionStateView<'a> {
    pub(super) completion: &'a mut CompletionPump,
    pub(super) ops: &'a mut IocpOpRegistry,
    pub(super) overlapped_registry: &'a mut OverlappedRegistry,
    pub(super) timer: &'a mut TimerEngine,
    pub(super) completion_diagnostics: &'a mut IocpDriverCompletionDiagnostics,
    pub(super) rio: &'a mut IocpRioRuntime,
    pub(super) buffer_pool: Option<&'a AnyBufPool>,
    pub(super) handles: &'a mut HandleRegistry,
    pub(super) extensions: &'a Extensions,
    pub(super) blocking_pool: &'a ThreadPool,
}

pub(super) struct LifecycleStateView<'a> {
    pub(super) completion: &'a mut CompletionPump,
    pub(super) ops: &'a mut IocpOpRegistry,
    pub(super) overlapped_registry: &'a mut OverlappedRegistry,
    pub(super) timer: &'a mut TimerEngine,
    pub(super) completion_diagnostics: &'a mut IocpDriverCompletionDiagnostics,
    pub(super) rio: &'a mut IocpRioRuntime,
    pub(super) handles: &'a mut HandleRegistry,
    pub(super) extensions: &'a Extensions,
    pub(super) shutting_down: &'a mut bool,
    pub(super) closed: &'a mut bool,
    pub(super) remote_cancel_receiver: &'a mut mpsc::Receiver<CancelRequest>,
}

impl IocpDriverState {
    pub(super) fn new(mut parts: IocpDriverStateParts) -> IocpResult<Self> {
        let overlapped_registry = OverlappedRegistry::new(&mut parts.ops)?;
        Ok(Self {
            completion: parts.completion,
            ops: parts.ops,
            overlapped_registry,
            extensions: parts.extensions,
            timer: parts.timer,
            remote_cancel_sender: parts.remote_cancel_sender,
            remote_cancel_receiver: parts.remote_cancel_receiver,
            completion_diagnostics: parts.completion_diagnostics,
            rio: parts.rio,
            buffer_pool_owner: parts.buffer_pool_owner,
            handles: parts.handles,
            shutting_down: parts.shutting_down,
            closed: parts.closed,
            blocking_pool: parts.blocking_pool,
            _winsock: parts.winsock,
        })
    }

    pub(super) fn retain_buffer_pool(&mut self, pool: AnyBufPool) -> IocpResult<()> {
        if self.buffer_pool_owner.is_some() {
            return IocpError::InvalidState.attach_note("IOCP driver already owns a buffer pool");
        }
        self.buffer_pool_owner = Some(pool);
        Ok(())
    }

    pub(super) fn cancellation_view(&mut self) -> CancellationStateView<'_> {
        CancellationStateView {
            completion: &mut self.completion,
            ops: &mut self.ops,
            overlapped_registry: &mut self.overlapped_registry,
            timer: &mut self.timer,
            completion_diagnostics: &mut self.completion_diagnostics,
            rio: &mut self.rio,
            handles: &mut self.handles,
            extensions: &self.extensions,
            shutting_down: self.shutting_down,
        }
    }

    pub(super) fn polling_view(&mut self) -> PollingStateView<'_> {
        PollingStateView {
            completion: &mut self.completion,
            ops: &mut self.ops,
            overlapped_registry: &mut self.overlapped_registry,
            timer: &mut self.timer,
            completion_diagnostics: &mut self.completion_diagnostics,
            rio: &mut self.rio,
            handles: &mut self.handles,
            extensions: &self.extensions,
            remote_cancel_receiver: &mut self.remote_cancel_receiver,
            shutting_down: self.shutting_down,
        }
    }

    pub(super) fn registration_view(&mut self) -> RegistrationStateView<'_> {
        RegistrationStateView {
            rio: &mut self.rio,
            handles: &mut self.handles,
        }
    }

    pub(super) fn submission_view(&mut self) -> SubmissionStateView<'_> {
        SubmissionStateView {
            completion: &mut self.completion,
            ops: &mut self.ops,
            overlapped_registry: &mut self.overlapped_registry,
            timer: &mut self.timer,
            completion_diagnostics: &mut self.completion_diagnostics,
            rio: &mut self.rio,
            buffer_pool: self.buffer_pool_owner.as_ref(),
            handles: &mut self.handles,
            extensions: &self.extensions,
            blocking_pool: &self.blocking_pool,
        }
    }

    pub(super) fn lifecycle_view(&mut self) -> LifecycleStateView<'_> {
        LifecycleStateView {
            completion: &mut self.completion,
            ops: &mut self.ops,
            overlapped_registry: &mut self.overlapped_registry,
            timer: &mut self.timer,
            completion_diagnostics: &mut self.completion_diagnostics,
            rio: &mut self.rio,
            handles: &mut self.handles,
            extensions: &self.extensions,
            shutting_down: &mut self.shutting_down,
            closed: &mut self.closed,
            remote_cancel_receiver: &mut self.remote_cancel_receiver,
        }
    }

    pub(super) fn ops(&self) -> &IocpOpRegistry {
        &self.ops
    }

    pub(super) fn ops_mut(&mut self) -> &mut IocpOpRegistry {
        &mut self.ops
    }

    pub(super) fn completion(&self) -> &CompletionPump {
        &self.completion
    }

    pub(super) fn timer(&self) -> &TimerEngine {
        &self.timer
    }

    pub(super) fn completion_diagnostics(&self) -> &IocpDriverCompletionDiagnostics {
        &self.completion_diagnostics
    }

    pub(super) fn remote_cancel_sender(&self) -> &RemoteCancelSender {
        &self.remote_cancel_sender
    }

    pub(super) fn remote_cancel_receiver_mut(&mut self) -> &mut mpsc::Receiver<CancelRequest> {
        &mut self.remote_cancel_receiver
    }

    pub(super) fn is_shutting_down(&self) -> bool {
        self.shutting_down
    }

    pub(super) fn has_active_ops(&self) -> bool {
        self.ops.has_active_ops()
    }

    pub(super) fn buffer_pool(&self) -> Option<&AnyBufPool> {
        self.buffer_pool_owner.as_ref()
    }

    pub(super) fn create_waker(&self) -> Arc<dyn RemoteWaker<IocpError>> {
        self.completion.create_waker()
    }

    #[cfg(test)]
    pub(super) fn debug_registered_file(&self, idx: usize) -> Option<&RegisteredHandle> {
        self.handles.registered_file(idx)
    }

    #[cfg(test)]
    pub(super) fn debug_slot_is_idle(&mut self, index: usize) -> bool {
        let Some(slot) = self.ops.shared_slots().slots().get(index) else {
            return false;
        };
        let Ok(token) = OpToken::from_registry_parts(index, slot.generation(Ordering::Acquire))
        else {
            return false;
        };
        matches!(
            self.ops.checked_slot_view(token),
            Ok(CheckedSlotView::Empty(_))
        )
    }

    #[cfg(feature = "test-hooks")]
    pub(super) fn debug_chunk_register_attempts(&self) -> u64 {
        self.rio.debug_chunk_register_attempts()
    }
}

impl IocpDriverState {
    pub(super) fn completion_view_from_cancellation<'view, 'state>(
        view: &'view mut CancellationStateView<'state>,
    ) -> CompletionStateView<'view> {
        CompletionStateView {
            completion: view.completion,
            ops: view.ops,
            overlapped_registry: view.overlapped_registry,
            timer: view.timer,
            completion_diagnostics: view.completion_diagnostics,
            rio: view.rio,
            handles: view.handles,
            extensions: view.extensions,
            shutting_down: view.shutting_down,
        }
    }

    pub(super) fn registration_view_from_completion<'view, 'state>(
        view: &'view mut CompletionStateView<'state>,
    ) -> RegistrationStateView<'view> {
        RegistrationStateView {
            rio: &mut *view.rio,
            handles: &mut *view.handles,
        }
    }

    pub(super) fn completion_view_from_polling<'view, 'state>(
        view: &'view mut PollingStateView<'state>,
    ) -> CompletionStateView<'view> {
        CompletionStateView {
            completion: view.completion,
            ops: view.ops,
            overlapped_registry: view.overlapped_registry,
            timer: view.timer,
            completion_diagnostics: view.completion_diagnostics,
            rio: view.rio,
            handles: view.handles,
            extensions: view.extensions,
            shutting_down: view.shutting_down,
        }
    }

    pub(super) fn cancellation_view_from_polling<'view, 'state>(
        view: &'view mut PollingStateView<'state>,
    ) -> CancellationStateView<'view> {
        CancellationStateView {
            completion: view.completion,
            ops: view.ops,
            overlapped_registry: view.overlapped_registry,
            timer: view.timer,
            completion_diagnostics: view.completion_diagnostics,
            rio: view.rio,
            handles: view.handles,
            extensions: view.extensions,
            shutting_down: view.shutting_down,
        }
    }

    pub(super) fn registration_view_from_polling<'view, 'state>(
        view: &'view mut PollingStateView<'state>,
    ) -> RegistrationStateView<'view> {
        RegistrationStateView {
            rio: view.rio,
            handles: view.handles,
        }
    }

    pub(super) fn cancellation_view_from_lifecycle<'view, 'state>(
        view: &'view mut LifecycleStateView<'state>,
    ) -> CancellationStateView<'view> {
        CancellationStateView {
            completion: view.completion,
            ops: view.ops,
            overlapped_registry: view.overlapped_registry,
            timer: view.timer,
            completion_diagnostics: view.completion_diagnostics,
            rio: view.rio,
            handles: view.handles,
            extensions: view.extensions,
            shutting_down: *view.shutting_down,
        }
    }

    pub(super) fn polling_view_from_lifecycle<'view, 'state>(
        view: &'view mut LifecycleStateView<'state>,
    ) -> PollingStateView<'view> {
        PollingStateView {
            completion: view.completion,
            ops: view.ops,
            overlapped_registry: view.overlapped_registry,
            timer: view.timer,
            completion_diagnostics: view.completion_diagnostics,
            rio: view.rio,
            handles: view.handles,
            extensions: view.extensions,
            remote_cancel_receiver: view.remote_cancel_receiver,
            shutting_down: *view.shutting_down,
        }
    }

    pub(super) fn registration_view_from_lifecycle<'view, 'state>(
        view: &'view mut LifecycleStateView<'state>,
    ) -> RegistrationStateView<'view> {
        RegistrationStateView {
            rio: view.rio,
            handles: view.handles,
        }
    }
}
