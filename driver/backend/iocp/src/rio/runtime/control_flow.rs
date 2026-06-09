//! Actor coordination and completion routing for the RIO runtime.

use crate::IoFd;
use crate::common::push_completion_shared;
use crate::config::{BorrowedRawHandle, SocketKey};
use crate::error::IocpError;
use crate::op::IocpOpRegistry;
use crate::rio::core::registry::RioRegistry;
use crate::rio::core::rio_result_to_event_res;
use crate::rio::core::submit_ops::RioRq;
use crate::rio::core::{RioCompletionKind, RioOpRequestInit, RioRequestContextDecode};
use crate::rio::error::{RioError, RioResult};
use crate::rio::runtime::release_socket_inflight_token_from;
use crate::rio::{
    RioCompletionContext, RioEnv, RioState, SocketInflightToken, SocketLifecycleState,
    SocketRuntimeState,
};
use diagweave::prelude::*;
use rustc_hash::FxHashMap;
use tracing::debug;
use veloq_driver_core::driver::{
    CompletionAnomaly, CompletionBackend, CompletionEvent, CompletionPacket, CompletionToken,
    DriverCompletionDiagnostics, RawCompletion, RoutedSlotCompletion, SharedCompletionTable,
    record_completion_anomaly, record_lost_completion, route_user_completion,
    run_completion_cleanup,
};
use veloq_driver_core::slot::SlotRegistryExt;
use windows_sys::Win32::Foundation::ERROR_OPERATION_ABORTED;
use windows_sys::Win32::Networking::WinSock::{RIO_CORRUPT_CQ, RIORESULT};

pub(crate) struct RioSocketActor {
    pub(crate) rq: RioRq,
}

impl RioSocketActor {
    pub(crate) fn new(rq: RioRq) -> Self {
        Self { rq }
    }
}

struct RioCompletionRouter<'a> {
    outstanding_count: &'a mut usize,
    socket_runtime: &'a mut FxHashMap<SocketKey, SocketRuntimeState>,
    comp: RioCompletionContext<'a>,
    registry: &'a mut RioRegistry,
    env: RioEnv<'a>,
    completed_count: usize,
}

impl<'a> RioCompletionRouter<'a> {
    fn new(
        outstanding_count: &'a mut usize,
        socket_runtime: &'a mut FxHashMap<SocketKey, SocketRuntimeState>,
        comp: RioCompletionContext<'a>,
        env: (&'a mut RioRegistry, RioEnv<'a>),
    ) -> Self {
        let (registry, env) = env;
        Self {
            outstanding_count,
            socket_runtime,
            comp,
            registry,
            env,
            completed_count: 0,
        }
    }

    fn release_socket_inflight(&mut self, socket_inflight: SocketInflightToken) {
        let _ = release_socket_inflight_token_from(self.socket_runtime, socket_inflight);
    }

    fn handle_op_completion(&mut self, init: RioOpRequestInit, res: &RIORESULT) -> RioResult<()> {
        let RioOpRequestInit {
            token: op_token,
            socket_inflight,
            op_kind,
            request_id,
            addr_slot,
            buffer_lease,
            diagnostics,
        } = init;
        let (user_data, generation) = op_token.parts();
        let socket_key = socket_inflight.socket_key();
        let ops = &mut self.comp.ops;
        let completion_token = CompletionToken::user(op_token);
        let raw = RawCompletion::new(
            CompletionBackend::Rio,
            completion_token,
            rio_raw_res(res),
            0,
        );
        match route_user_completion(op_token, raw, ops.checked_slot_view(op_token)) {
            RoutedSlotCompletion::Waiting(mut slot) => {
                if slot.platform().generation != generation {
                    let snapshot = slot.snapshot();
                    let anomaly = CompletionAnomaly::corrupt(
                        completion_token,
                        snapshot.index,
                        snapshot.generation,
                        snapshot.state,
                    )
                    .with_slot_snapshot(snapshot)
                    .with_raw_completion(raw);
                    record_completion_anomaly(self.comp.diagnostics, &anomaly);
                    let report = IocpError::Internal
                        .to_report()
                        .push_ctx("scope", "rio.runtime.control_flow.handle_op_completion")
                        .with_ctx("user_data", user_data)
                        .with_ctx("generation", generation)
                        .with_ctx("platform_generation", slot.platform().generation)
                        .with_ctx("rio_op_kind", op_kind.as_str())
                        .with_ctx("rio_request_id", request_id)
                        .attach_note("RIO slot platform generation mismatch");
                    let mut guard = slot.complete();
                    let completion = Err(report);
                    let cleanup = guard
                        .op
                        .as_mut()
                        .map(|op| op.completion_cleanup(&completion))
                        .unwrap_or_default();
                    let _ = guard.take_op();
                    let _ = guard.take_completion_data();
                    drop(guard);
                    let _ = record_lost_completion(
                        self.comp.table,
                        self.comp.diagnostics,
                        raw.event(),
                        anomaly,
                        cleanup,
                    );
                    let _ = ops.finalize_corrupt_slot(snapshot);
                } else {
                    let cancelled = slot.platform().rio_cancel_requested;
                    let mut completion = if cancelled {
                        Err(IocpError::CompletionWait
                            .to_report()
                            .push_ctx("scope", "rio.runtime.control_flow.handle_op_completion")
                            .with_ctx("socket_raw", socket_key.as_handle() as usize)
                            .with_ctx("rio_op_kind", op_kind.as_str())
                            .with_ctx("rio_request_id", request_id)
                            .set_error_code(ERROR_OPERATION_ABORTED as i32)
                            .attach_note("RIO operation was cancelled before kernel completion"))
                    } else if res.Status == 0 {
                        Ok(res.BytesTransferred as usize)
                    } else {
                        IocpError::CompletionWait
                            .push_ctx("scope", "rio.runtime.control_flow.handle_op_completion")
                            .with_ctx("socket_raw", socket_key.as_handle() as usize)
                            .with_ctx("rio_op_kind", op_kind.as_str())
                            .with_ctx("rio_request_id", request_id)
                            .with_ctx("rq_raw", diagnostics.rq_raw)
                            .with_ctx("data_buffer_id", diagnostics.data_buffer_id)
                            .with_ctx("data_buffer_offset", diagnostics.data_buffer_offset)
                            .with_ctx("data_buffer_length", diagnostics.data_buffer_length)
                            .with_ctx("addr_slot", addr_slot.unwrap_or(usize::MAX))
                            .set_error_code(res.Status)
                            .attach_note("rio completion returned os error")
                    };
                    let _ = slot.with_op_mut(|iocp_op| {
                        if let Some(addr_slot) = addr_slot
                            && let crate::op::IocpOpPayload::UdpRecvFrom(payload) =
                                &mut iocp_op.payload
                            && !cancelled
                            && completion.is_ok()
                            && let Err(e) = self
                                .registry
                                .copy_addr_slot_to(addr_slot, &mut payload.addr)
                                .trans()
                        {
                            completion = Err(e
                                .with_ctx("socket_raw", socket_key.as_handle() as usize)
                                .with_ctx("rio_op_kind", op_kind.as_str())
                                .with_ctx("rio_request_id", request_id)
                                .with_ctx("addr_slot", addr_slot)
                                .attach_note("failed to copy RIO recv_from address"));
                        }
                        if iocp_op.header.in_flight {
                            iocp_op.header.in_flight = false;
                        }
                        if !cancelled && let Ok(bytes) = completion.as_ref().copied() {
                            completion = iocp_op
                                .on_complete(bytes, self.comp.ext)
                                .with_ctx("scope", "rio.runtime.control_flow.handle_op_completion")
                                .attach_note("rio op completion hook failed");
                        }
                    });
                    let res_code = rio_result_to_event_res(&completion);
                    {
                        let mut guard = slot.complete();
                        let _ = guard.take_op();
                        let (payload, detail) = guard.take_completion_data();
                        let event = CompletionEvent {
                            token: completion_token,
                            res: res_code,
                            flags: 0,
                        };

                        let outcome = push_completion_shared(
                            self.comp.table,
                            self.comp.diagnostics,
                            CompletionPacket::new(event, payload, detail.or(Some(completion))),
                        );
                        let _ = outcome;
                    }
                    let _ = ops.finalize_waiting_completion(op_token);
                }
            }
            RoutedSlotCompletion::Orphaned(mut slot) => {
                if slot.platform_mut().generation != generation {
                    let snapshot = slot.snapshot();
                    let anomaly = CompletionAnomaly::corrupt(
                        completion_token,
                        snapshot.index,
                        snapshot.generation,
                        snapshot.state,
                    )
                    .with_slot_snapshot(snapshot)
                    .with_raw_completion(raw);
                    record_completion_anomaly(self.comp.diagnostics, &anomaly);
                    debug!(
                        user_data,
                        generation,
                        actual_generation = slot.platform().generation,
                        "RIO orphaned completion found platform generation mismatch"
                    );
                    let orphan_result = IocpError::CompletionWait
                        .push_ctx("scope", "rio.runtime.control_flow.orphan_cleanup")
                        .with_ctx("socket_raw", socket_key.as_handle() as usize)
                        .with_ctx("rio_op_kind", op_kind.as_str())
                        .with_ctx("rio_request_id", request_id)
                        .attach_note("orphaned RIO completion had platform generation mismatch");
                    let mut guard = slot.complete();
                    let cleanup = guard
                        .op
                        .as_mut()
                        .map(|op| op.completion_cleanup(&orphan_result))
                        .unwrap_or_default();
                    let _ = guard.take_op();
                    let _ = guard.take_completion_data();
                    let _ = std::mem::take(guard.platform_mut());
                    drop(guard);
                    let mut cleanup = cleanup;
                    let _ = run_completion_cleanup(self.comp.diagnostics, &mut cleanup);
                    let _ = ops.finalize_orphaned_completion(op_token);
                } else {
                    let mut guard = slot.complete();
                    let orphan_result = if res.Status == 0 {
                        Ok(res.BytesTransferred as usize)
                    } else {
                        IocpError::CompletionWait
                            .push_ctx("scope", "rio.runtime.control_flow.orphan_cleanup")
                            .with_ctx("socket_raw", socket_key.as_handle() as usize)
                            .with_ctx("rio_op_kind", op_kind.as_str())
                            .with_ctx("rio_request_id", request_id)
                            .set_error_code(res.Status)
                            .attach_note("orphaned RIO completion returned os error")
                    };
                    let cleanup = guard
                        .op
                        .as_mut()
                        .map(|op| op.completion_cleanup(&orphan_result))
                        .unwrap_or_default();
                    let _ = guard.take_op();
                    let _ = guard.take_completion_data();
                    let _ = std::mem::take(guard.platform_mut());
                    drop(guard);
                    let mut cleanup = cleanup;
                    let _ = run_completion_cleanup(self.comp.diagnostics, &mut cleanup);
                    let _ = ops.finalize_orphaned_completion(op_token);
                }
            }
            RoutedSlotCompletion::Corrupt(anomaly) => {
                record_completion_anomaly(self.comp.diagnostics, &anomaly);
                debug!(
                    user_data,
                    generation, "RIO completion found corrupt or reserved slot"
                );
                if let Some(snapshot) = anomaly.slot_snapshot {
                    let lost_result = Err(IocpError::InvalidState
                        .to_report()
                        .push_ctx("scope", "rio.runtime.control_flow.corrupt_completion")
                        .with_ctx("user_data", snapshot.index)
                        .with_ctx("generation", snapshot.generation)
                        .with_ctx("slot_state", format!("{:?}", snapshot.state))
                        .with_ctx("has_op", snapshot.has_op)
                        .with_ctx("has_payload", snapshot.has_payload)
                        .with_ctx("rio_op_kind", op_kind.as_str())
                        .with_ctx("rio_request_id", request_id)
                        .set_error_code(5)
                        .attach_note("RIO completion found corrupt slot"));
                    let cleanup = ops
                        .get_slot_entry_op_storage_and_entry_mut_token(op_token)
                        .and_then(|(_, _, op, _)| {
                            let cleanup = op
                                .as_mut()
                                .map(|op| op.completion_cleanup(&lost_result))
                                .unwrap_or_default();
                            let _ = op.take();
                            Some(cleanup)
                        })
                        .unwrap_or_default();
                    let _ =
                        ops.with_slot_storage_mut_token(op_token, |result, payload, _sidecar| {
                            let _ = result.take();
                            let _ = payload.take();
                        });
                    let event = CompletionEvent {
                        token: raw.token,
                        res: -5,
                        flags: raw.flags,
                    };
                    let _ = record_lost_completion(
                        self.comp.table,
                        self.comp.diagnostics,
                        event,
                        anomaly,
                        cleanup,
                    );
                    let _ = ops.finalize_corrupt_slot(snapshot);
                }
            }
            RoutedSlotCompletion::Missing(anomaly) => {
                record_completion_anomaly(self.comp.diagnostics, &anomaly);
                debug!(
                    user_data,
                    generation,
                    slots = ops.capacity(),
                    "RIO completion for missing slot"
                );
            }
            RoutedSlotCompletion::Empty(anomaly) => {
                record_completion_anomaly(self.comp.diagnostics, &anomaly);
                debug!(
                    user_data,
                    generation,
                    state = ?anomaly.state,
                    "RIO completion for non-active slot"
                );
            }
            RoutedSlotCompletion::Stale(anomaly) => {
                record_completion_anomaly(self.comp.diagnostics, &anomaly);
                debug!(
                    user_data,
                    generation,
                    actual_generation = anomaly.actual_generation,
                    state = ?anomaly.state,
                    "RIO completion for stale slot"
                );
            }
        }

        self.registry.free_addr_slot(addr_slot);
        let release_result = self.registry.release_buffer_lease(buffer_lease, self.env);
        self.release_socket_inflight(socket_inflight);
        if *self.outstanding_count > 0 {
            *self.outstanding_count -= 1;
        }
        self.completed_count += 1;
        release_result
    }

    fn handle_one(&mut self, res: &RIORESULT) -> RioResult<()> {
        let kind = match self
            .registry
            .decode_request_context_checked(res.RequestContext)
        {
            RioRequestContextDecode::Valid(kind) => kind,
            RioRequestContextDecode::Malformed { raw } => {
                self.comp.diagnostics.inc_rio_malformed_context();
                self.record_unknown_request_context(raw, res, "malformed RIO request context");
                return Ok(());
            }
            RioRequestContextDecode::Missing { id } => {
                self.comp.diagnostics.inc_rio_missing_context();
                debug!(
                    request_context = res.RequestContext,
                    context_index = id.index(),
                    context_generation = id.generation(),
                    status = res.Status,
                    bytes = res.BytesTransferred,
                    "ignoring missing RIO request context"
                );
                self.record_unknown_request_context(
                    res.RequestContext,
                    res,
                    "missing RIO request context",
                );
                return Ok(());
            }
            RioRequestContextDecode::Stale {
                id,
                actual_generation,
            } => {
                self.comp.diagnostics.inc_rio_stale_context();
                debug!(
                    request_context = res.RequestContext,
                    context_index = id.index(),
                    expected_generation = id.generation(),
                    actual_generation,
                    status = res.Status,
                    bytes = res.BytesTransferred,
                    "ignoring stale RIO request context"
                );
                self.record_unknown_request_context(
                    res.RequestContext,
                    res,
                    "stale RIO request context",
                );
                return Ok(());
            }
        };
        match kind {
            RioCompletionKind::Op {
                init,
                context: _completed_context,
            } => self.handle_op_completion(init, res),
        }
    }

    fn record_unknown_request_context(
        &mut self,
        request_context: u64,
        res: &RIORESULT,
        note: &'static str,
    ) {
        let raw = RawCompletion::new(
            CompletionBackend::Rio,
            CompletionToken::from_raw(request_context),
            rio_raw_res(res),
            0,
        );
        let anomaly = CompletionAnomaly::unknown_control(raw.token).with_raw_completion(raw);
        record_completion_anomaly(self.comp.diagnostics, &anomaly);
        debug!(
            request_context,
            status = res.Status,
            bytes = res.BytesTransferred,
            note,
            "ignoring RIO request context"
        );
    }
}

#[inline]
fn rio_raw_res(res: &RIORESULT) -> i32 {
    if res.Status == 0 {
        res.BytesTransferred.min(i32::MAX as u32) as i32
    } else if res.Status > 0 {
        -res.Status
    } else {
        res.Status
    }
}

impl RioState {
    pub(crate) fn ensure_actor(
        &mut self,
        target: (IoFd, BorrowedRawHandle<'_>),
        env: RioEnv<'_>,
    ) -> RioResult<&mut RioSocketActor> {
        let (fd, handle) = target;
        let socket_key = handle.raw().actor_key();
        if self.submissions_closed {
            return RioError::InvalidInput
                .with_ctx("socket_raw", socket_key.as_handle() as usize)
                .with_ctx("outstanding_count", self.outstanding_count)
                .attach_note("RIO runtime is shutting down; rejecting actor creation");
        }

        if let Some(key) = self.actor_by_handle.get(&socket_key).copied() {
            return self
                .actors
                .get_mut(key)
                .ok_or(RioError::Internal)
                .attach_note("failed to retrieve indexed actor");
        }

        let rq = self
            .registry
            .create_rq((handle, fd), env)
            .push_ctx("scope", "rio.runtime.control_flow.ensure_actor")
            .with_ctx("fd_fixed_index", fd.fixed_index())
            .with_ctx("fd_generation", fd.generation())
            .with_ctx("handle_raw", handle.raw().as_handle() as usize)
            .with_ctx("socket_raw", handle.raw().as_handle() as usize)
            .with_ctx("rq_depth", self.registry.rq_depth)
            .with_ctx("max_outstanding_recvs", self.registry.rq_depth)
            .with_ctx("max_outstanding_sends", self.registry.rq_depth)
            .with_ctx("max_receive_data_buffers", 1_u32)
            .with_ctx("max_send_data_buffers", 1_u32)
            .with_ctx("outstanding_count", self.outstanding_count)
            .with_ctx("actors_len", self.actors.len())
            .with_ctx(
                "actor_index_hit",
                self.actor_by_handle.contains_key(&socket_key),
            )
            .attach_note("RIOCreateRequestQueue failed")?;

        let actor = RioSocketActor::new(rq);
        let key = self.actors.insert(actor);
        self.actor_by_handle.insert(socket_key, key);
        self.socket_runtime.entry(socket_key).or_default();
        self.actors
            .get_mut(key)
            .ok_or(RioError::Internal)
            .trans()
            .attach_note("failed to retrieve inserted actor")
    }

    pub(crate) fn shutdown_actor(&mut self, socket_key: SocketKey) {
        let Some(key) = self.actor_by_handle.remove(&socket_key) else {
            return;
        };
        let _ = self.actors.remove(key);
    }

    pub(crate) fn stop_accepting_new_submissions(&mut self) {
        self.submissions_closed = true;
        self.actor_by_handle.clear();
        for state in self.socket_runtime.values_mut() {
            state.lifecycle = SocketLifecycleState::Closing;
        }
    }

    pub(crate) fn forget_runtime_after_drain(&mut self) {
        debug_assert_eq!(
            self.outstanding_count, 0,
            "forgetting RIO runtime before outstanding completions drained"
        );
        debug_assert!(
            self.socket_runtime
                .values()
                .all(|state| state.inflight == 0),
            "forgetting socket runtime before socket inflight counters drained"
        );
        self.actors.clear();
        self.actor_by_handle.clear();
        self.socket_runtime.clear();
    }

    pub(crate) fn process_completions(
        &mut self,
        ops: &mut IocpOpRegistry,
        ext: &crate::ext::Extensions,
        registrar: &dyn veloq_buf::BufferRegistrar,
        completion_table: &SharedCompletionTable<crate::op::IocpUserPayload, IocpError>,
        diagnostics: &mut DriverCompletionDiagnostics,
    ) -> RioResult<usize> {
        self.process_completions_internal(ops, ext, registrar, completion_table, diagnostics)
    }

    fn process_completions_internal(
        &mut self,
        ops: &mut IocpOpRegistry,
        ext: &crate::ext::Extensions,
        registrar: &dyn veloq_buf::BufferRegistrar,
        completion_table: &SharedCompletionTable<crate::op::IocpUserPayload, IocpError>,
        diagnostics: &mut DriverCompletionDiagnostics,
    ) -> RioResult<usize> {
        const MAX_RIO_RESULTS: usize = 128;
        let mut results: [RIORESULT; MAX_RIO_RESULTS] = unsafe { std::mem::zeroed() };
        let Some(env) = self.kernel.env(registrar, self.registration_mode) else {
            return Ok(0);
        };
        let mut router = RioCompletionRouter::new(
            &mut self.outstanding_count,
            &mut self.socket_runtime,
            RioCompletionContext {
                ops,
                ext,
                table: completion_table,
                diagnostics,
            },
            (&mut self.registry, env),
        );
        loop {
            let count = self.kernel.dequeue(&mut results);
            if count == RIO_CORRUPT_CQ {
                return RioError::Internal
                    .attach_note("RIO completion queue is corrupt (RIO_CORRUPT_CQ)");
            }
            if count == 0 {
                break;
            }

            for res in results.iter().take(count as usize) {
                router.handle_one(res)?;
            }

            if count < MAX_RIO_RESULTS as u32 {
                break;
            }
        }

        self.kernel.rearm_notify()?;

        if *router.outstanding_count == 0 {
            router.registry.flush_deregs(router.env);
        }
        Ok(router.completed_count)
    }

    pub(crate) fn drain_outstanding_with_ops(
        &mut self,
        timeout: std::time::Duration,
        ops: &mut IocpOpRegistry,
        ext: &crate::ext::Extensions,
        registrar: &dyn veloq_buf::BufferRegistrar,
        completion_table: &SharedCompletionTable<crate::op::IocpUserPayload, IocpError>,
        diagnostics: &mut DriverCompletionDiagnostics,
    ) -> RioResult<()> {
        let deadline = std::time::Instant::now()
            .checked_add(timeout)
            .ok_or_else(|| {
                RioError::Internal
                    .to_report()
                    .with_ctx("timeout_ms", timeout.as_millis() as u64)
                    .attach_note("strict close RIO drain timeout is too large")
            })?;

        while self.outstanding_count > 0 {
            let now = std::time::Instant::now();
            if now >= deadline {
                return RioError::Internal
                    .with_ctx("outstanding_count", self.outstanding_count)
                    .with_ctx("timeout_ms", timeout.as_millis() as u64)
                    .attach_note("strict close timed out while draining RIO outstanding requests");
            }

            let processed =
                self.process_completions(ops, ext, registrar, completion_table, diagnostics)?;
            if processed == 0 {
                std::thread::yield_now();
            }
        }

        Ok(())
    }
}
