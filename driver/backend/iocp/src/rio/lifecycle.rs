//! Shutdown and deferred cleanup orchestration for `RioState`.

use crate::config::SocketKey;
use crate::driver::IocpDriverCompletionDiagnostics;
use crate::rio::ActorKey;
use crate::rio::RioState;
use crate::rio::core::{
    RioCompletionKind, RioKernel, RioOpRequestInit, RioRegistry, RioRequestContextDecode,
};
use crate::rio::error::{RioError, RioResult};
use crate::rio::runtime::RioSocketActor;
use crate::rio::runtime::control_flow::{
    rio_malformed_context_anomaly, rio_missing_context_anomaly, rio_stale_context_anomaly,
};
use crate::rio::runtime::release_socket_inflight_token_from;
use diagweave::prelude::*;
use rustc_hash::FxHashMap;
use slotmap::SlotMap;
use std::sync::OnceLock;
use windows_sys::Win32::Networking::WinSock::{RIO_CORRUPT_CQ, RIORESULT};

const RIO_REAPER_DRAIN_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

pub(crate) struct DeferredRioCleanup {
    kernel: RioKernel,
    registry: RioRegistry,
    registration_mode: crate::BufferRegistrationMode,
    submissions_closed: bool,
    actors: SlotMap<ActorKey, RioSocketActor>,
    actor_by_handle: FxHashMap<SocketKey, ActorKey>,
    socket_runtime: FxHashMap<SocketKey, crate::rio::SocketRuntimeState>,
    outstanding_count: usize,
    next_request_id: u64,
    deferred_payloads: Vec<crate::op::IocpUserPayload>,
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
            outstanding_count: self.outstanding_count,
            next_request_id: self.next_request_id,
            deferred_payloads: self.deferred_payloads,
            diagnostics: self.diagnostics,
        };
        state.stop_accepting_new_submissions();
        if let Err(e) = state.drain_outstanding(RIO_REAPER_DRAIN_TIMEOUT) {
            tracing::warn!(error = ?e, "RioReaper: background drain timed out");
            if state.outstanding_count > 0 {
                tracing::warn!(
                    outstanding_count = state.outstanding_count,
                    "RioReaper: leaking deferred RIO state to keep in-flight buffers alive"
                );
                std::mem::forget(state);
                return;
            }
        }
        state.finalize_cleanup();
    }
}

fn reaper_sender() -> Option<&'static std::sync::mpsc::Sender<DeferredRioCleanup>> {
    static SENDER: OnceLock<Option<std::sync::mpsc::Sender<DeferredRioCleanup>>> = OnceLock::new();
    let opt = SENDER.get_or_init(|| {
        let (tx, rx) = std::sync::mpsc::channel::<DeferredRioCleanup>();
        match std::thread::Builder::new()
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
        match self.decode_req_ctx_checked(res.RequestContext) {
            RioRequestContextDecode::Valid(RioCompletionKind::Op {
                init:
                    RioOpRequestInit {
                        socket_inflight,
                        addr_slot,
                        buffer_lease,
                        ..
                    },
                context: _completed_context,
            }) => {
                self.registry.free_addr_slot(addr_slot);
                release_result = self.registry.release_buffer_lease_deferred(buffer_lease);
                let _ =
                    release_socket_inflight_token_from(&mut self.socket_runtime, socket_inflight);
            }
            RioRequestContextDecode::Malformed { raw } => {
                let anomaly = rio_malformed_context_anomaly(raw)
                    .with_raw_result(rio_drain_raw_res(res))
                    .with_flags(0);
                self.diagnostics.record_anomaly(&anomaly);
            }
            RioRequestContextDecode::Missing { id } => {
                let anomaly =
                    rio_missing_context_anomaly(res.RequestContext, id.index(), id.generation())
                        .with_raw_result(rio_drain_raw_res(res))
                        .with_flags(0);
                self.diagnostics.record_anomaly(&anomaly);
            }
            RioRequestContextDecode::Stale {
                id,
                actual_generation,
            } => {
                let anomaly = rio_stale_context_anomaly(
                    res.RequestContext,
                    id.index(),
                    id.generation(),
                    actual_generation,
                )
                .with_raw_result(rio_drain_raw_res(res))
                .with_flags(0);
                self.diagnostics.record_anomaly(&anomaly);
            }
        }
        if self.outstanding_count > 0 {
            self.outstanding_count -= 1;
        }
        release_result
    }

    fn drain_batch(&mut self, results: &[RIORESULT], count: usize) -> RioResult<()> {
        for res in results.iter().take(count) {
            self.handle_drain_result(res)?;
        }
        Ok(())
    }

    pub(crate) fn drain_outstanding(&mut self, timeout: std::time::Duration) -> RioResult<()> {

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
                std::thread::yield_now();
            } else {
                std::thread::sleep(std::time::Duration::from_millis(1));
            }
        }
        }

        let start = std::time::Instant::now();
        let mut backoff = Backoff::new();
        while self.outstanding_count > 0 {
            if start.elapsed() >= timeout {
                return RioError::Internal
                    .with_ctx("outstanding_count", self.outstanding_count)
                    .with_ctx("timeout_ms", timeout.as_millis() as u64)
                    .attach_note("strict close timed out while draining RIO outstanding requests");
            }

            const MAX_RESULTS: usize = 128;
            // SAFETY: RIORESULT is a POD struct and safe to zero-initialize.
            let mut results: [RIORESULT; MAX_RESULTS] = unsafe { std::mem::zeroed() };
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

    fn finalize_cleanup(&mut self) {
        self.stop_accepting_new_submissions();
        if self.outstanding_count == 0 {
            self.forget_runtime_after_drain();
        } else {
            tracing::warn!(
                outstanding_count = self.outstanding_count,
                "finalizing RIO state before outstanding requests drained"
            );
        }
        if let Some(env) = self
            .kernel
            .env(&veloq_buf::NoopRegistrar, self.registration_mode)
        {
            self.registry.cleanup_deregister(env);
        }
        self.kernel.close();
    }

    pub(crate) fn take_deferred(&mut self) -> Option<DeferredRioCleanup> {
        if self.kernel.cq.is_invalid() {
            return None;
        }
        let kernel = std::mem::replace(&mut self.kernel, RioKernel::noop());
        let registry = std::mem::replace(&mut self.registry, RioRegistry::new(32, 1));
        Some(DeferredRioCleanup {
            kernel,
            registry,
            registration_mode: self.registration_mode,
            submissions_closed: self.submissions_closed,
            actors: std::mem::take(&mut self.actors),
            actor_by_handle: std::mem::take(&mut self.actor_by_handle),
            socket_runtime: std::mem::take(&mut self.socket_runtime),
            outstanding_count: std::mem::take(&mut self.outstanding_count),
            next_request_id: self.next_request_id,
            deferred_payloads: std::mem::take(&mut self.deferred_payloads),
            diagnostics: self.diagnostics.clone(),
        })
    }

    pub(crate) fn defer_payloads(&mut self, payloads: Vec<crate::op::IocpUserPayload>) {
        self.deferred_payloads.extend(payloads);
    }
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
        if self.outstanding_count == 0 {
            self.finalize_cleanup();
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

        self.finalize_cleanup();
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn timeout_constant_is_positive() {
        assert!(super::RIO_REAPER_DRAIN_TIMEOUT > std::time::Duration::from_secs(0));
    }
}
