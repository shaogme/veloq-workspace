//! RIO backend orchestration for IOCP. / IOCP 的 RIO 后端编排入口。
//!
//! `RioState` keeps shared state while core, runtime, and lifecycle own their operations.
//! `RioState` 聚合共享状态，具体操作分别由 core、runtime 和 lifecycle 管理。

mod core;
mod lifecycle;
mod runtime;

mod error {
    pub(crate) use super::core::{RioError, RioResult};
}

use crate::{
    IocpResult,
    config::{BorrowedRawHandle, SocketKey},
    driver::IocpDriverCompletionDiagnostics,
    ext::Extensions,
    op::{IocpKernelOp, IocpUserPayload, SubmissionResult},
};
use diagweave::prelude::*;
use veloq_buf::{BufferRegistrationRequest, FixedBuf, LockedMemoryBudget, LockedMemoryPermit};
use veloq_driver_core::{BufferRegistrationStatus, ReceivePumpState};
use veloq_std::{time::Duration, vec::Vec};

use self::core::{RioKernel, RioRegistry, RioSubmitOperations};
use self::lifecycle::{DeferredRioCleanup, RioLifecycle, RioLifecycleResources};

pub(crate) use self::{
    core::{RioCore, RioEnv, RioError, RioOpKind, RioResult, RioSendToArgs, RioTarget},
    runtime::{
        CompletionPollArgs, RioCompletionAnomaly, RioCompletionService, RioRuntime, RioSocketActor,
        SocketInflightGuard, SocketInflightIdentity, SocketInflightToken,
    },
};

pub(crate) struct RioState {
    core: RioCore,
    runtime: RioRuntime,
    lifecycle: RioLifecycleResources,
    diagnostics: IocpDriverCompletionDiagnostics,
}

impl RioState {
    pub(crate) fn shutdown_actor(&mut self, socket_key: SocketKey) {
        RioCompletionService::shutdown_actor(&mut self.runtime, socket_key);
    }

    pub(crate) fn stop_accepting_new_submissions(&mut self) {
        self.runtime.stop_accepting_new_submissions();
    }

    pub(crate) fn forget_runtime_after_drain(&mut self) -> RioResult<()> {
        self.runtime
            .forget_after_drain(self.core.outstanding_count())
    }

    pub(crate) fn process_completions(
        &mut self,
        args: CompletionPollArgs<'_>,
    ) -> IocpResult<usize> {
        RioCompletionService::process_completions(&mut self.core, &mut self.runtime, args)
    }

    pub(crate) fn new(
        port: BorrowedRawHandle<'_>,
        entries: u32,
        ext: &Extensions,
        diagnostics: IocpDriverCompletionDiagnostics,
        budget: LockedMemoryBudget,
        address_permit: Option<LockedMemoryPermit>,
    ) -> RioResult<Self> {
        let kernel = RioKernel::from_extensions(port, entries, ext)?;
        let rq_depth = entries.clamp(32, 64);

        Ok(Self {
            core: RioCore::new(
                kernel,
                RioRegistry::with_budget(rq_depth, entries as usize, budget, address_permit),
            ),
            runtime: RioRuntime::new(),
            lifecycle: RioLifecycleResources {
                deferred_kernel_ops: Vec::new(),
                deferred_payloads: Vec::new(),
            },
            diagnostics,
        })
    }

    pub(crate) fn rio_outstanding_count(&self) -> usize {
        self.core.outstanding_count()
    }

    pub(crate) fn close_kernel(&mut self) {
        self.core.kernel_mut().close();
    }

    #[cfg(feature = "test-hooks")]
    pub(crate) fn debug_chunk_register_attempts(&self) -> u64 {
        self.core.chunk_register_attempts()
    }
}

impl RioState {
    pub(crate) fn mark_socket_registered(&mut self, actor_key: SocketKey) {
        self.runtime.mark_socket_registered(actor_key);
    }

    pub(crate) fn begin_socket_cleanup(&mut self, actor_key: SocketKey) -> RioResult<bool> {
        self.runtime.begin_socket_cleanup(actor_key)
    }

    pub(crate) fn try_acquire_socket_inflight_guard(
        &mut self,
        actor_key: SocketKey,
    ) -> RioResult<SocketInflightGuard<'_>> {
        self.runtime
            .try_acquire_socket_inflight_guard(actor_key, self.core.outstanding_count())
    }

    pub(crate) fn release_socket_inflight_token(
        &mut self,
        token: SocketInflightToken,
    ) -> RioResult<()> {
        self.runtime.release_socket_inflight_token(token)
    }

    pub(crate) fn socket_ready_for_cleanup(&self, actor_key: SocketKey) -> bool {
        self.runtime.socket_ready_for_cleanup(actor_key)
    }

    pub(crate) fn forget_socket_runtime(&mut self, actor_key: SocketKey) -> RioResult<()> {
        self.runtime.forget_socket_runtime(actor_key)
    }

    pub(crate) fn has_socket_inflight(&self) -> bool {
        self.runtime.has_socket_inflight()
    }

    pub(crate) fn runtime_ready_for_forget(&self) -> bool {
        self.runtime
            .runtime_ready_for_forget(self.core.outstanding_count())
    }
}

impl RioState {
    pub(crate) fn drain_outstanding(&mut self, timeout: Duration) -> RioResult<()> {
        RioLifecycle::drain(
            &mut self.core,
            &mut self.runtime,
            &mut self.diagnostics,
            timeout,
        )
    }

    fn finalize_cleanup(&mut self) -> RioResult<()> {
        RioLifecycle::finalize(&mut self.core, &mut self.runtime)
    }

    pub(crate) fn take_deferred(&mut self) -> Option<DeferredRioCleanup> {
        RioLifecycle::take_deferred(
            &mut self.core,
            &mut self.runtime,
            &mut self.lifecycle,
            &self.diagnostics,
        )
    }

    pub(crate) fn defer_payloads(&mut self, payloads: Vec<IocpUserPayload>) {
        self.lifecycle.deferred_payloads.extend(payloads);
    }

    pub(crate) fn defer_kernel_ops(&mut self, ops: Vec<IocpKernelOp>) {
        self.lifecycle.deferred_kernel_ops.extend(ops);
    }
}

impl RioState {
    pub(crate) fn register_buffer_backend(
        &mut self,
        request: BufferRegistrationRequest,
    ) -> RioResult<BufferRegistrationStatus> {
        let (registry, env) = self.core.registry_with_env().into_parts();
        let Some(env) = env else {
            return Err(RioError::NotSupported
                .to_report()
                .attach_note("RIO buffer registration is unavailable without a dispatch table"));
        };
        registry
            .register_buffer_backend(request, env)
            .map(|registered| {
                if registered {
                    BufferRegistrationStatus::Registered
                } else {
                    BufferRegistrationStatus::BudgetExceeded
                }
            })
    }

    pub(crate) fn try_submit_send_to(
        &mut self,
        args: RioSendToArgs<'_>,
    ) -> RioResult<SubmissionResult> {
        RioSubmitOperations::new(&mut self.core, &mut self.runtime).try_submit_send_to(args)
    }

    pub(crate) fn try_submit_recv(
        &mut self,
        target: RioTarget<'_>,
        buf: &mut FixedBuf,
    ) -> RioResult<SubmissionResult> {
        RioSubmitOperations::new(&mut self.core, &mut self.runtime).try_submit_recv(target, buf)
    }

    pub(crate) fn try_submit_send(
        &mut self,
        target: RioTarget<'_>,
        buf: &FixedBuf,
    ) -> RioResult<SubmissionResult> {
        RioSubmitOperations::new(&mut self.core, &mut self.runtime).try_submit_send(target, buf)
    }

    pub(crate) fn try_submit_recv_multi_initial(
        &mut self,
        target: RioTarget<'_>,
        op_kind: RioOpKind,
        pump: &mut ReceivePumpState,
    ) -> RioResult<()> {
        RioSubmitOperations::new(&mut self.core, &mut self.runtime)
            .try_submit_recv_multi_initial(target, op_kind, pump)
    }
}

impl Drop for RioState {
    fn drop(&mut self) {
        self.stop_accepting_new_submissions();
        if self.runtime_ready_for_forget() {
            if let Err(error) = self.finalize_cleanup() {
                tracing::error!(report = ?error, "failed to finalize drained RIO state");
            }
            return;
        }

        if let Some(task) = self.take_deferred() {
            RioLifecycle::submit_deferred(task);
            return;
        }

        if let Err(error) = self.finalize_cleanup() {
            tracing::error!(report = ?error, "failed to finalize RIO state during drop");
        }
    }
}
