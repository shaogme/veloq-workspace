//! Shutdown and deferred cleanup orchestration for `RioState`.

use crate::{
    BufferRegistrationMode,
    config::SocketKey,
    driver::{IocpDriverCompletionDiagnostics, RIO_EVENT_TOKEN, completion::COMP_BACKEND_RIO},
    op::{IocpKernelOp, IocpUserPayload},
    rio::{
        ActorKey, RioState, SocketRuntimeState,
        core::{
            RioCompletionKind, RioKernel, RioOpRequestInit, RioRegistry, RioRequestContextDecode,
        },
        error::{RioError, RioResult},
        runtime::{
            RioSocketActor,
            control_flow::{
                rio_malformed_context_kind, rio_missing_context_kind, rio_stale_context_kind,
            },
        },
    },
};
use diagweave::prelude::*;
use slotmap::SlotMap;
use veloq_buf::NoopRegistrar;
use veloq_driver_core::driver::AnomalyAttach;
use veloq_std::{
    collections::FastHashMap,
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
    kernel: RioKernel,
    registry: RioRegistry,
    registration_mode: BufferRegistrationMode,
    submissions_closed: bool,
    actors: SlotMap<ActorKey, RioSocketActor>,
    actor_by_handle: FastHashMap<SocketKey, ActorKey>,
    socket_runtime: FastHashMap<SocketKey, SocketRuntimeState>,
    rio_outstanding_count: usize,
    next_request_id: u64,
    deferred_kernel_ops: Vec<IocpKernelOp>,
    deferred_payloads: Vec<IocpUserPayload>,
    diagnostics: IocpDriverCompletionDiagnostics,
}

// SAFETY: DeferredRioCleanup is transferred by ownership to a single reaper thread.
unsafe impl Send for DeferredRioCleanup {}

impl DeferredRioCleanup {
    fn run(self) {
        let mut state = RioState {
            kernel: self.kernel,
            registry: self.registry,
            registration_mode: self.registration_mode,
            submissions_closed: self.submissions_closed,
            actors: self.actors,
            actor_by_handle: self.actor_by_handle,
            socket_runtime: self.socket_runtime,
            rio_outstanding_count: self.rio_outstanding_count,
            next_request_id: self.next_request_id,
            deferred_kernel_ops: self.deferred_kernel_ops,
            deferred_payloads: self.deferred_payloads,
            diagnostics: self.diagnostics,
            cq_armed: true,
        };
        state.stop_accepting_new_submissions();
        if let Err(e) = state.drain_outstanding(RIO_REAPER_DRAIN_TIMEOUT) {
            tracing::warn!(error = ?e, "RioReaper: background drain timed out");
        }
        if !state.runtime_ready_for_forget() {
            tracing::warn!(
                rio_outstanding_count = state.rio_outstanding_count,
                socket_inflight = state.socket_inflight_count(),
                "RioReaper: leaking deferred RIO state to keep in-flight resources alive"
            );
            mem::forget(state);
            return;
        }
        if let Err(error) = state.finalize_cleanup() {
            tracing::error!(report = ?error, "RioReaper: failed to finalize drained RIO state");
            mem::forget(state);
        }
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

impl RioState {
    fn handle_drain_result(&mut self, res: &RIORESULT) -> RioResult<()> {
        let mut release_result = Ok(());
        let resources_released = match self.decode_req_ctx_checked(res.RequestContext) {
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
                if let Err(error) = self.registry.free_addr_reservation(addr) {
                    tracing::error!(report = ?error, "failed to release drained RIO address lease");
                }
                release_result = self.registry.release_buffer_lease_deferred(buffer_lease);
                let _ = self.release_request_inflight(op_kind, socket_inflight);
                true
            }
            RioRequestContextDecode::Malformed { raw } => {
                self.diagnostics
                    .record_anomaly_kind(rio_malformed_context_kind(raw), rio_drain_attach(res));
                false
            }
            RioRequestContextDecode::Missing { id } => {
                self.diagnostics.record_anomaly_kind(
                    rio_missing_context_kind(res.RequestContext, id.index(), id.generation()),
                    rio_drain_attach(res),
                );
                false
            }
            RioRequestContextDecode::Stale {
                id,
                actual_generation,
            } => {
                self.diagnostics.record_anomaly_kind(
                    rio_stale_context_kind(
                        res.RequestContext,
                        id.index(),
                        id.generation(),
                        actual_generation,
                    ),
                    rio_drain_attach(res),
                );
                false
            }
        };
        if resources_released && self.rio_outstanding_count > 0 {
            self.rio_outstanding_count -= 1;
        } else if !resources_released {
            // Without a valid request context we cannot identify the buffer lease, address
            // reservation, or socket inflight token. Keep the request outstanding so cleanup
            // cannot deregister resources while an unknown RIO request may still reference them.
            // The reaper will retain the whole state on timeout instead of guessing a release.
            tracing::warn!(
                request_context = res.RequestContext,
                rio_outstanding_count = self.rio_outstanding_count,
                "keeping RIO state alive after an unidentifiable drained completion"
            );
        }
        release_result
    }

    fn drain_batch(&mut self, results: &[RIORESULT], count: usize) -> RioResult<()> {
        for res in results.iter().take(count) {
            self.handle_drain_result(res)?;
        }
        Ok(())
    }

    pub(crate) fn drain_outstanding(&mut self, timeout: Duration) -> RioResult<()> {
        struct Backoff {
            yields: u32,
        }

        impl Backoff {
            #[inline]
            fn new() -> Self {
                Self { yields: 0 }
            }

            #[inline]
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
        while self.rio_outstanding_count > 0 {
            if start.elapsed() >= timeout {
                return RioError::Internal
                    .with_ctx("rio_outstanding_count", self.rio_outstanding_count)
                    .with_ctx("timeout_ms", timeout.as_millis() as u64)
                    .attach_note("strict close timed out while draining RIO outstanding requests");
            }

            const MAX_RESULTS: usize = 128;
            // SAFETY: RIORESULT is a POD struct and safe to zero-initialize.
            let mut results: [RIORESULT; MAX_RESULTS] = unsafe { zeroed() };
            let count = self.kernel.dequeue(&mut results);

            if count == RIO_CORRUPT_CQ {
                return RioError::Internal
                    .attach_note("RIO completion queue is corrupt (RIO_CORRUPT_CQ)");
            }

            if count == 0 {
                backoff.snooze();
                continue;
            }

            backoff = Backoff::new();
            self.drain_batch(&results, count as usize)?;
        }

        Ok(())
    }

    fn finalize_cleanup(&mut self) -> RioResult<()> {
        self.stop_accepting_new_submissions();
        self.forget_runtime_after_drain()?;
        if let Some(env) = self.kernel.env(&NoopRegistrar, self.registration_mode) {
            self.registry.cleanup_deregister(env);
        }
        self.kernel.close();
        Ok(())
    }

    pub(crate) fn take_deferred(&mut self) -> Option<DeferredRioCleanup> {
        if self.kernel.cq.is_invalid() {
            return None;
        }
        let kernel = mem::replace(&mut self.kernel, RioKernel::noop());
        let registry = mem::replace(&mut self.registry, RioRegistry::new(32, 1));
        Some(DeferredRioCleanup {
            kernel,
            registry,
            registration_mode: self.registration_mode,
            submissions_closed: self.submissions_closed,
            actors: mem::take(&mut self.actors),
            actor_by_handle: mem::take(&mut self.actor_by_handle),
            socket_runtime: mem::take(&mut self.socket_runtime),
            rio_outstanding_count: mem::take(&mut self.rio_outstanding_count),
            next_request_id: self.next_request_id,
            deferred_kernel_ops: mem::take(&mut self.deferred_kernel_ops),
            deferred_payloads: mem::take(&mut self.deferred_payloads),
            diagnostics: self.diagnostics.clone(),
        })
    }

    pub(crate) fn defer_payloads(&mut self, payloads: Vec<IocpUserPayload>) {
        self.deferred_payloads.extend(payloads);
    }

    pub(crate) fn defer_kernel_ops(&mut self, ops: Vec<IocpKernelOp>) {
        self.deferred_kernel_ops.extend(ops);
    }
}

#[inline]
fn rio_drain_attach(res: &RIORESULT) -> AnomalyAttach {
    AnomalyAttach::from_raw_completion(veloq_driver_core::driver::RawCompletion::new(
        COMP_BACKEND_RIO,
        RIO_EVENT_TOKEN,
        rio_drain_raw_res(res),
        0,
    ))
}

#[inline]
fn rio_drain_raw_res(res: &RIORESULT) -> i32 {
    if res.Status == 0 {
        res.BytesTransferred.min(i32::MAX as u32) as i32
    } else if res.Status > 0 {
        -res.Status
    } else {
        res.Status
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
            if let Some(tx) = reaper_sender() {
                if let Err(err) = tx.send(task) {
                    tracing::warn!("RioReaper unavailable, falling back to inline cleanup");
                    err.0.run();
                }
            } else {
                tracing::warn!("RioReaper thread failed to start, falling back to inline cleanup");
                task.run();
            }
            return;
        }

        if let Err(error) = self.finalize_cleanup() {
            tracing::error!(report = ?error, "failed to finalize RIO state during drop");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        config::IocpHandle,
        rio::{
            SocketInflightToken,
            core::{RioOpKind, RioRequestDiagnostics},
        },
    };
    use slotmap::SlotMap;
    use veloq_driver_core::{driver::OpToken, slot::Generation};
    use veloq_std::{
        collections::FastHashMap, mem::zeroed, ptr::null_mut, time::Duration, vec::Vec,
    };

    fn test_state() -> RioState {
        RioState {
            kernel: RioKernel::noop(),
            registry: RioRegistry::new(4, 1),
            registration_mode: BufferRegistrationMode::default(),
            submissions_closed: false,
            actors: SlotMap::with_key(),
            actor_by_handle: FastHashMap::default(),
            socket_runtime: FastHashMap::default(),
            rio_outstanding_count: 0,
            next_request_id: 0,
            deferred_kernel_ops: Vec::new(),
            deferred_payloads: Vec::new(),
            diagnostics: IocpDriverCompletionDiagnostics::default(),
            cq_armed: true,
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
        assert!(super::RIO_REAPER_DRAIN_TIMEOUT > Duration::from_secs(0));
    }

    #[test]
    fn malformed_drain_context_keeps_request_outstanding() {
        let mut state = test_state();
        state.rio_outstanding_count = 1;
        // SAFETY: RIORESULT is a POD completion record and all omitted fields are irrelevant to
        // the malformed-context assertion.
        let mut result: RIORESULT = unsafe { zeroed() };
        result.RequestContext = 0;

        state
            .handle_drain_result(&result)
            .expect("malformed context is recorded without a release error");
        assert_eq!(state.rio_outstanding_count, 1);
    }

    #[test]
    fn stale_drain_context_does_not_release_current_request() {
        let mut state = test_state();
        let old_context = state.registry.alloc_request_context(test_request_init(1));
        let old_raw = old_context.as_request_context() as usize as u64;
        assert!(
            state
                .registry
                .take_prepared_request_init(old_context)
                .is_some()
        );

        let current_context = state.registry.alloc_request_context(test_request_init(2));
        state.rio_outstanding_count = 1;
        // SAFETY: RIORESULT is a POD completion record and all omitted fields are irrelevant to
        // the stale-context assertion.
        let mut result: RIORESULT = unsafe { zeroed() };
        result.BytesTransferred = 1;
        result.RequestContext = old_raw;

        state
            .handle_drain_result(&result)
            .expect("stale context is recorded without a release error");
        assert_eq!(state.rio_outstanding_count, 1);
        assert!(matches!(
            state.registry.decode_request_context_checked(
                current_context.as_request_context() as usize as u64
            ),
            RioRequestContextDecode::Valid(_)
        ));
    }
}
