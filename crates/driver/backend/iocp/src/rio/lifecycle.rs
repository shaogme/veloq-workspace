//! Shutdown and deferred cleanup orchestration. / 关闭与延迟清理编排。

use crate::{
    driver::{COMP_BACKEND_RIO, IocpDriverCompletionDiagnostics, RIO_EVENT_TOKEN},
    op::{IocpKernelOp, IocpUserPayload},
    rio::{
        RioCompletionAnomaly, RioCore, RioRuntime,
        core::{RioCompletionKind, RioOpRequestInit, RioRequestContextDecode},
        error::{RioError, RioResult},
    },
};
use diagweave::prelude::*;
use veloq_buf::NoopRegistrar;
use veloq_driver_core::driver::{AnomalyAttach, RawCompletion};
use veloq_std::{
    mem::{self, zeroed},
    string::ToString,
    sync::{OnceLock, mpsc},
    thread::{Builder, sleep, yield_now},
    time::{Duration, Instant},
    vec::Vec,
};
use windows_sys::Win32::Networking::WinSock::{RIO_CORRUPT_CQ, RIORESULT};

const RIO_REAPER_DRAIN_TIMEOUT: Duration = Duration::from_secs(5);

pub(crate) struct DeferredRioCleanup {
    core: RioCore,
    runtime: RioRuntime,
    resources: RioLifecycleResources,
    diagnostics: IocpDriverCompletionDiagnostics,
}

pub(crate) struct RioLifecycleResources {
    pub(crate) deferred_kernel_ops: Vec<IocpKernelOp>,
    pub(crate) deferred_payloads: Vec<IocpUserPayload>,
}

pub(super) struct RioLifecycle;

// SAFETY: Ownership transfers to one reaper thread. / 安全性：资源所有权转移给唯一的 reaper 线程。
unsafe impl Send for DeferredRioCleanup {}

fn rio_drain_attach(result: &RIORESULT) -> AnomalyAttach {
    AnomalyAttach::from_raw_completion(RawCompletion::new(
        COMP_BACKEND_RIO,
        RIO_EVENT_TOKEN,
        rio_drain_raw_result(result),
        0,
    ))
}

fn rio_drain_raw_result(result: &RIORESULT) -> i32 {
    if result.Status == 0 {
        result.BytesTransferred.min(i32::MAX as u32) as i32
    } else if result.Status > 0 {
        -result.Status
    } else {
        result.Status
    }
}

impl DeferredRioCleanup {
    fn run(mut self) {
        self.runtime.stop_accepting_new_submissions();
        if let Err(e) = RioLifecycle::drain(
            &mut self.core,
            &mut self.runtime,
            &mut self.diagnostics,
            RIO_REAPER_DRAIN_TIMEOUT,
        ) {
            tracing::warn!(error = ?e, "RioReaper: background drain timed out");
        }
        if !self
            .runtime
            .runtime_ready_for_forget(self.core.outstanding_count())
        {
            tracing::warn!(
                rio_outstanding_count = self.core.outstanding_count(),
                socket_inflight = self.runtime.socket_inflight_count(),
                "RioReaper: leaking deferred RIO state to keep in-flight resources alive"
            );
            mem::forget(self);
            return;
        }
        if let Err(error) = RioLifecycle::finalize(&mut self.core, &mut self.runtime) {
            tracing::error!(report = ?error, "RioReaper: failed to finalize drained RIO state");
            mem::forget(self);
            return;
        }
        drop(self.resources);
    }
}

fn reaper_sender() -> Option<&'static mpsc::Sender<DeferredRioCleanup>> {
    static SENDER: OnceLock<Option<mpsc::Sender<DeferredRioCleanup>>> = OnceLock::new();
    let opt = SENDER.get_or_init(|| {
        let (tx, rx) = mpsc::channel::<DeferredRioCleanup>();
        match Builder::new()
            .name("veloq-rio-reaper".to_string())
            .spawn(move || {
                while let Ok(task) = rx.recv() {
                    task.run();
                }
            }) {
            Ok(_) => Some(tx),
            Err(e) => {
                tracing::error!("failed to spawn veloq-rio-reaper: {e}");
                None
            }
        }
    });
    opt.as_ref()
}

impl RioLifecycle {
    pub(super) fn take_deferred(
        core: &mut RioCore,
        runtime: &mut RioRuntime,
        resources: &mut RioLifecycleResources,
        diagnostics: &IocpDriverCompletionDiagnostics,
    ) -> Option<DeferredRioCleanup> {
        if core.kernel().cq().is_invalid() {
            return None;
        }
        let registration_mode = core.registration_mode();
        let core = mem::replace(core, RioCore::empty(registration_mode));
        let runtime = mem::replace(runtime, RioRuntime::closed_empty());
        let resources = mem::replace(
            resources,
            RioLifecycleResources {
                deferred_kernel_ops: Vec::new(),
                deferred_payloads: Vec::new(),
            },
        );
        Some(DeferredRioCleanup {
            core,
            runtime,
            resources,
            diagnostics: diagnostics.clone(),
        })
    }

    pub(super) fn drain(
        core: &mut RioCore,
        runtime: &mut RioRuntime,
        diagnostics: &mut IocpDriverCompletionDiagnostics,
        timeout: Duration,
    ) -> RioResult<()> {
        struct Backoff {
            yields: u32,
        }

        impl Backoff {
            fn new() -> Self {
                Self { yields: 0 }
            }

            fn snooze(&mut self) {
                if self.yields < 10 {
                    self.yields += 1;
                    let _ = yield_now();
                } else {
                    let _ = sleep(Duration::from_millis(1));
                }
            }
        }

        let start = Instant::now();
        let mut backoff = Backoff::new();
        while core.outstanding_count() > 0 {
            if start.elapsed() >= timeout {
                return RioError::Internal
                    .with_ctx("rio_outstanding_count", core.outstanding_count())
                    .with_ctx("timeout_ms", timeout.as_millis() as u64)
                    .attach_note("strict close timed out while draining RIO outstanding requests");
            }

            const MAX_RESULTS: usize = 128;
            // SAFETY: RIORESULT is a POD completion record and can be initialized to zero. / 安全性：RIORESULT 是 POD 完成记录，可安全地零初始化。
            let mut results: [RIORESULT; MAX_RESULTS] = unsafe { zeroed() };
            let count = core.kernel_mut().dequeue(&mut results);
            if count == RIO_CORRUPT_CQ {
                return RioError::Internal
                    .attach_note("RIO completion queue is corrupt (RIO_CORRUPT_CQ)");
            }
            if count == 0 {
                backoff.snooze();
                continue;
            }

            backoff = Backoff::new();
            for result in results.iter().take(count as usize) {
                Self::handle_drain_result(core, runtime, diagnostics, result)?;
            }
        }
        Ok(())
    }

    fn handle_drain_result(
        core: &mut RioCore,
        runtime: &mut RioRuntime,
        diagnostics: &mut IocpDriverCompletionDiagnostics,
        result: &RIORESULT,
    ) -> RioResult<()> {
        let mut release_result = Ok(());
        let resources_released = match core
            .registry_mut()
            .decode_request_context_checked(result.RequestContext)
        {
            RioRequestContextDecode::Valid(kind) => {
                let RioCompletionKind::Op {
                    init:
                        RioOpRequestInit {
                            op_kind,
                            socket_inflight,
                            addr,
                            buffer_lease,
                            ..
                        },
                    context: _completed_context,
                } = *kind;
                if let Err(error) = core.registry_mut().free_addr_reservation(addr) {
                    tracing::error!(report = ?error, "failed to release drained RIO address lease");
                }
                release_result = core
                    .registry_mut()
                    .release_buffer_lease_deferred(buffer_lease);
                let _ = runtime.release_request_inflight(op_kind, socket_inflight);
                true
            }
            RioRequestContextDecode::Malformed { raw } => {
                diagnostics.record_anomaly_kind(
                    RioCompletionAnomaly::malformed(raw),
                    rio_drain_attach(result),
                );
                false
            }
            RioRequestContextDecode::Missing { id } => {
                diagnostics.record_anomaly_kind(
                    RioCompletionAnomaly::missing(
                        result.RequestContext,
                        id.index(),
                        id.generation(),
                    ),
                    rio_drain_attach(result),
                );
                false
            }
            RioRequestContextDecode::Stale {
                id,
                actual_generation,
            } => {
                diagnostics.record_anomaly_kind(
                    RioCompletionAnomaly::stale(
                        result.RequestContext,
                        id.index(),
                        id.generation(),
                        actual_generation,
                    ),
                    rio_drain_attach(result),
                );
                false
            }
        };
        if resources_released {
            core.decrement_outstanding();
        } else {
            // A malformed identity cannot name its leases, so outstanding ownership is retained.
            // 无法识别的请求上下文不能定位租约，因此必须保留 outstanding 所有权。
            tracing::warn!(
                request_context = result.RequestContext,
                rio_outstanding_count = core.outstanding_count(),
                "keeping RIO state alive after an unidentifiable drained completion"
            );
        }
        release_result
    }

    pub(super) fn finalize(core: &mut RioCore, runtime: &mut RioRuntime) -> RioResult<()> {
        runtime.stop_accepting_new_submissions();
        runtime.forget_after_drain(core.outstanding_count())?;
        let registrar = NoopRegistrar;
        let (registry, env) = core.registry_with_env(&registrar).into_parts();
        if let Some(env) = env {
            registry.cleanup_deregister(env);
        }
        core.kernel_mut().close();
        Ok(())
    }

    pub(super) fn submit_deferred(task: DeferredRioCleanup) {
        if let Some(tx) = reaper_sender() {
            if let Err(err) = tx.send(task) {
                tracing::warn!("RioReaper unavailable, falling back to inline cleanup");
                err.0.run();
            }
        } else {
            tracing::warn!("RioReaper thread failed to start, falling back to inline cleanup");
            task.run();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{RIO_REAPER_DRAIN_TIMEOUT, RioLifecycle};
    use crate::{
        BufferRegistrationMode,
        config::IocpHandle,
        driver::IocpDriverCompletionDiagnostics,
        rio::{
            RioRuntime, SocketInflightToken,
            core::{
                RioCore, RioKernel, RioOpKind, RioOpRequestInit, RioRegistry,
                RioRequestContextDecode, RioRequestDiagnostics,
            },
        },
    };
    use veloq_driver_core::{driver::OpToken, slot::Generation};
    use veloq_std::{mem::zeroed, ptr::null_mut, time::Duration};
    use windows_sys::Win32::Networking::WinSock::RIORESULT;

    struct RioLifecycleTestContext {
        core: RioCore,
        runtime: RioRuntime,
        diagnostics: IocpDriverCompletionDiagnostics,
    }

    impl RioLifecycleTestContext {
        fn new() -> Self {
            Self {
                core: RioCore::new(
                    RioKernel::noop(),
                    RioRegistry::new(32, 1),
                    BufferRegistrationMode::default(),
                ),
                runtime: RioRuntime::new(),
                diagnostics: IocpDriverCompletionDiagnostics::default(),
            }
        }
    }

    fn test_request_init(request_id: u64) -> RioOpRequestInit {
        let socket = IocpHandle::for_socket(null_mut());
        RioOpRequestInit {
            token: OpToken::from_registry_parts(0, Generation::new(1)).unwrap(),
            socket_inflight: SocketInflightToken::new(socket, request_id),
            op_kind: RioOpKind::Recv,
            request_id,
            addr_slot: None,
            addr_generation: None,
            addr: None,
            buffer_lease: None,
            receive_slot_id: None,
            receive_slot_generation: None,
            diagnostics: RioRequestDiagnostics::default(),
        }
    }

    #[test]
    fn timeout_constant_is_positive() {
        assert!(RIO_REAPER_DRAIN_TIMEOUT > Duration::from_secs(0));
    }

    #[test]
    fn malformed_drain_context_keeps_request_outstanding() {
        let mut context = RioLifecycleTestContext::new();
        context.core.set_outstanding_count(1);
        // SAFETY: RIORESULT is POD; omitted fields do not affect this assertion. / 安全性：RIORESULT 是 POD，未赋值字段不影响此断言。
        let mut result: RIORESULT = unsafe { zeroed() };
        result.RequestContext = 0;

        RioLifecycle::handle_drain_result(
            &mut context.core,
            &mut context.runtime,
            &mut context.diagnostics,
            &result,
        )
        .expect("malformed context is recorded without a release error");
        assert_eq!(context.core.outstanding_count(), 1);
    }

    #[test]
    fn stale_drain_context_does_not_release_current_request() {
        let mut context = RioLifecycleTestContext::new();
        let old_context = context
            .core
            .registry_mut()
            .alloc_request_context(test_request_init(1));
        let old_raw = old_context.as_request_context() as usize as u64;
        assert!(
            context
                .core
                .registry_mut()
                .take_prepared_request_init(old_context)
                .is_some()
        );

        let current_context = context
            .core
            .registry_mut()
            .alloc_request_context(test_request_init(2));
        context.core.set_outstanding_count(1);
        // SAFETY: RIORESULT is POD; omitted fields do not affect this assertion. / 安全性：RIORESULT 是 POD，未赋值字段不影响此断言。
        let mut result: RIORESULT = unsafe { zeroed() };
        result.BytesTransferred = 1;
        result.RequestContext = old_raw;

        RioLifecycle::handle_drain_result(
            &mut context.core,
            &mut context.runtime,
            &mut context.diagnostics,
            &result,
        )
        .expect("stale context is recorded without a release error");
        assert_eq!(context.core.outstanding_count(), 1);
        assert!(matches!(
            context.core.registry_mut().decode_request_context_checked(
                current_context.as_request_context() as usize as u64
            ),
            RioRequestContextDecode::Valid(_)
        ));
    }
}
