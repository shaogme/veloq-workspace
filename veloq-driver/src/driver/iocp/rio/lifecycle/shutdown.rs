//! Shutdown and deferred cleanup orchestration for `RioState`.
//!
//! This module defines strict close behavior:
//! - mark actors draining,
//! - synchronously or asynchronously drain outstanding CQ completions,
//! - release registrations and close kernel resources in deterministic order.
//!
//! When immediate teardown cannot complete in `Drop`, ownership is moved to a
//! background reaper thread to avoid blocking critical threads indefinitely.

use crate::driver::iocp::error::{IocpErrorContext, io_msg};
use crate::driver::iocp::rio::RioState;
use crate::driver::iocp::rio::core::registry::RioRegistry;
use crate::driver::iocp::rio::core::submit_ops::RioKernel;
use crate::driver::iocp::rio::runtime::control_flow::actor::RioSocketActor;
use rustc_hash::FxHashMap;
use std::io;
use std::sync::OnceLock;
use windows_sys::Win32::Foundation::HANDLE;
use windows_sys::Win32::Networking::WinSock::{RIO_CORRUPT_CQ, RIORESULT};

const RIO_REAPER_DRAIN_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

struct DeferredRioCleanup {
    kernel: RioKernel,
    registry: RioRegistry,
    actors: FxHashMap<HANDLE, RioSocketActor>,
    actor_routes: FxHashMap<u32, HANDLE>,
    outstanding_count: usize,
}

// Safety: deferred cleanup task is transferred by ownership to a single reaper thread.
unsafe impl Send for DeferredRioCleanup {}

impl DeferredRioCleanup {
    fn run(self) {
        let mut state = RioState {
            kernel: self.kernel,
            registry: self.registry,
            actors: self.actors,
            actor_routes: self.actor_routes,
            next_actor_id: 1,
            outstanding_count: self.outstanding_count,
        };
        state.begin_shutdown();
        if let Err(e) = state.drain_outstanding_for(RIO_REAPER_DRAIN_TIMEOUT) {
            tracing::warn!(error = ?e, "RioReaper: background drain timed out");
        }
        state.finalize_shutdown_cleanup();
    }
}

fn reaper_sender() -> &'static std::sync::mpsc::Sender<DeferredRioCleanup> {
    static SENDER: OnceLock<std::sync::mpsc::Sender<DeferredRioCleanup>> = OnceLock::new();
    SENDER.get_or_init(|| {
        let (tx, rx) = std::sync::mpsc::channel::<DeferredRioCleanup>();
        std::thread::Builder::new()
            .name("veloq-rio-reaper".to_string())
            .spawn(move || {
                while let Ok(task) = rx.recv() {
                    task.run();
                }
            })
            .expect("failed to spawn veloq-rio-reaper");
        tx
    })
}

impl RioState {
    fn handle_drain_result(&mut self, res: &RIORESULT) {
        if let Some((actor_id, completion_generation)) =
            Self::decode_pool_context(res.RequestContext)
        {
            let _ = self.try_mark_pool_completion(actor_id, completion_generation);
        } else {
            Self::free_op_request_context(res.RequestContext);
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

    pub(crate) fn drain_outstanding_for(&mut self, timeout: std::time::Duration) -> io::Result<()> {
        let start = std::time::Instant::now();
        while self.outstanding_count > 0 {
            if start.elapsed() >= timeout {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    format!(
                        "strict close timed out while draining RIO outstanding requests: {}",
                        self.outstanding_count
                    ),
                ));
            }

            const MAX_RESULTS: usize = 128;
            let mut results: [RIORESULT; MAX_RESULTS] = unsafe { std::mem::zeroed() };
            let count = self
                .kernel
                .dequeue(results.as_mut_ptr(), MAX_RESULTS as u32);

            if count == RIO_CORRUPT_CQ {
                return Err(io_msg(
                    IocpErrorContext::Rio,
                    "RIO completion queue is corrupt (RIO_CORRUPT_CQ)",
                ));
            }

            if count == 0 {
                std::thread::yield_now();
                continue;
            }

            self.drain_batch(&results, count as usize);
        }

        Ok(())
    }

    fn finalize_shutdown_cleanup(&mut self) {
        self.forget_all_udp_pool_contexts();
        self.shutdown_all_actors_with_registry_cleanup(&veloq_buf::NoopRegistrar);
        let env = self.kernel.env(&veloq_buf::NoopRegistrar);
        self.registry.cleanup_deregister(env);
        self.kernel.close();
    }

    fn take_deferred_cleanup(&mut self) -> Option<DeferredRioCleanup> {
        if self.kernel.cq == 0 {
            return None;
        }
        let kernel = std::mem::replace(&mut self.kernel, RioKernel::noop());
        let registry = std::mem::replace(&mut self.registry, RioRegistry::new(32));
        Some(DeferredRioCleanup {
            kernel,
            registry,
            actors: std::mem::take(&mut self.actors),
            actor_routes: std::mem::take(&mut self.actor_routes),
            outstanding_count: std::mem::take(&mut self.outstanding_count),
        })
    }
}

impl Drop for RioState {
    fn drop(&mut self) {
        self.begin_shutdown();
        if self.outstanding_count == 0 {
            self.finalize_shutdown_cleanup();
            return;
        }

        if let Some(task) = self.take_deferred_cleanup() {
            let tx = reaper_sender();
            if let Err(err) = tx.send(task) {
                tracing::warn!("RioReaper unavailable, falling back to inline cleanup");
                err.0.run();
            }
            return;
        }

        self.finalize_shutdown_cleanup();
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn timeout_constant_is_positive() {
        assert!(super::RIO_REAPER_DRAIN_TIMEOUT > std::time::Duration::from_secs(0));
    }
}
