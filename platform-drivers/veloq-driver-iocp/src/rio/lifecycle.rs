//! Shutdown and deferred cleanup orchestration for `RioState`.

use crate::rio::ActorKey;
use crate::rio::RioState;
use crate::rio::core::RioCompletionKind;
use crate::rio::core::registry::RioRegistry;
use crate::rio::core::submit_ops::RioKernel;
use crate::rio::core::{RioOpCtxGuard, RioPoolCtxGuard};
use crate::rio::error::{RioError, RioResult};
use crate::rio::runtime::control_flow::RioSocketActor;
use error_stack::ResultExt;
use rustc_hash::FxHashMap;
use slotmap::SlotMap;
use std::sync::OnceLock;
use windows_sys::Win32::Foundation::HANDLE;
use windows_sys::Win32::Networking::WinSock::{RIO_CORRUPT_CQ, RIORESULT};

const RIO_REAPER_DRAIN_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

pub(crate) struct DeferredRioCleanup {
    kernel: RioKernel,
    registry: RioRegistry,
    registration_mode: crate::BufferRegistrationMode,
    actors: SlotMap<ActorKey, RioSocketActor>,
    actor_by_handle: FxHashMap<HANDLE, ActorKey>,
    outstanding_count: usize,
}

// SAFETY: DeferredRioCleanup is transferred by ownership to a single reaper thread.
unsafe impl Send for DeferredRioCleanup {}

impl DeferredRioCleanup {
    fn run(self) {
        let mut state = RioState {
            kernel: self.kernel,
            registry: self.registry,
            registration_mode: self.registration_mode,
            actors: self.actors,
            actor_by_handle: self.actor_by_handle,
            outstanding_count: self.outstanding_count,
        };
        state.begin_shutdown();
        if let Err(e) = state.drain_outstanding(RIO_REAPER_DRAIN_TIMEOUT) {
            tracing::warn!(error = ?e, "RioReaper: background drain timed out");
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
    fn handle_drain_result(&mut self, res: &RIORESULT) {
        match Self::decode_req_ctx(res.RequestContext) {
            Some(RioCompletionKind::Pool {
                actor_key,
                generation,
                ctx_ptr,
            }) => {
                let _ctx_guard = RioPoolCtxGuard(ctx_ptr);
                let _ = self.mark_pool_done(actor_key, generation);
            }
            Some(RioCompletionKind::Op { ctx_ptr, .. }) => {
                let _ctx_guard = RioOpCtxGuard(ctx_ptr);
            }
            None => {}
        }
        if self.outstanding_count > 0 {
            self.outstanding_count -= 1;
        }
    }

    fn drain_batch(&mut self, results: &[RIORESULT], count: usize) {
        for res in results.iter().take(count) {
            self.handle_drain_result(res);
        }
    }

    pub(crate) fn drain_outstanding(&mut self, timeout: std::time::Duration) -> RioResult<()> {
        let start = std::time::Instant::now();
        while self.outstanding_count > 0 {
            if start.elapsed() >= timeout {
                return Err(error_stack::Report::new(RioError::Internal)).attach(format!(
                    "strict close timed out while draining RIO outstanding requests: {}",
                    self.outstanding_count
                ));
            }

            const MAX_RESULTS: usize = 128;
            // SAFETY: RIORESULT is a POD struct and safe to zero-initialize.
            let mut results: [RIORESULT; MAX_RESULTS] = unsafe { std::mem::zeroed() };
            let count = self.kernel.dequeue(&mut results);

            if count == RIO_CORRUPT_CQ {
                return Err(error_stack::Report::new(RioError::Internal))
                    .attach("RIO completion queue is corrupt (RIO_CORRUPT_CQ)");
            }

            if count == 0 {
                std::thread::yield_now();
                continue;
            }

            self.drain_batch(&results, count as usize);
        }

        Ok(())
    }

    fn finalize_cleanup(&mut self) {
        self.forget_udp_contexts();
        self.shutdown_rio_actors(&veloq_buf::NoopRegistrar);
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
        let registry = std::mem::replace(&mut self.registry, RioRegistry::new(32));
        Some(DeferredRioCleanup {
            kernel,
            registry,
            registration_mode: self.registration_mode,
            actors: std::mem::take(&mut self.actors),
            actor_by_handle: std::mem::take(&mut self.actor_by_handle),
            outstanding_count: std::mem::take(&mut self.outstanding_count),
        })
    }
}

impl Drop for RioState {
    fn drop(&mut self) {
        self.begin_shutdown();
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
