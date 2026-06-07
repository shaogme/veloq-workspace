//! Actor coordination and completion routing for the RIO runtime.

use crate::IoFd;
use crate::config::{BorrowedRawHandle, SocketKey};
use crate::driver::IocpOpRegistry;
use crate::error::IocpError;
use crate::rio::core::registry::RioRegistry;
use crate::rio::core::rio_result_to_event_res;
use crate::rio::core::submit_ops::RioRq;
use crate::rio::core::{RioCompletionKind, RioOpRequestInit};
use crate::rio::error::{RioError, RioResult};
use crate::rio::{RioCompletionContext, RioEnv, RioState, SocketRuntimeState};
use diagweave::prelude::*;
use rustc_hash::FxHashMap;
use veloq_driver_core::driver::{
    CompletionEvent, SharedCompletionQueue, SharedCompletionTable, encode_completion_token,
};
use veloq_driver_core::slot::{SlotRegistryExt, SlotView};
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

    fn release_socket_inflight(&mut self, socket_key: SocketKey) {
        if let Some(state) = self.socket_runtime.get_mut(&socket_key)
            && state.inflight > 0
        {
            state.inflight -= 1;
        }
    }

    fn handle_op_completion(&mut self, init: RioOpRequestInit, res: &RIORESULT) -> RioResult<()> {
        let RioOpRequestInit {
            user_data,
            generation,
            socket_key,
            op_kind,
            request_id,
            addr_slot,
            heap_lease,
            diagnostics,
        } = init;
        let ops = &mut self.comp.ops;
        if user_data < ops.local.len() {
            match ops.slot_view(user_data) {
                Some(SlotView::InFlightWaiting(mut slot))
                    if slot.platform().generation == generation =>
                {
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
                            user_data: encode_completion_token(user_data, generation),
                            res: res_code,
                            flags: 0,
                        };

                        self.comp.table.record_completion_with_data(
                            event,
                            payload,
                            detail.or(Some(completion)),
                        );
                        self.comp.events.push(event);
                    }
                    let _ = self.comp.ops.remove(user_data);
                }
                Some(SlotView::InFlightOrphaned(mut slot)) => {
                    if slot.platform_mut().generation != generation {
                    } else {
                        let mut guard = slot.complete();
                        let _ = guard.take_op();
                        let _ = guard.take_completion_data();
                        let _ = std::mem::take(guard.platform_mut());
                        self.comp.ops.recycle(user_data, generation.wrapping_add(1));
                    }
                }
                _ => {}
            }
        }

        self.registry.free_addr_slot(addr_slot);
        self.registry.release_heap_lease(heap_lease);
        self.release_socket_inflight(socket_key);
        if *self.outstanding_count > 0 {
            *self.outstanding_count -= 1;
        }
        self.completed_count += 1;
        Ok(())
    }

    fn handle_one(&mut self, res: &RIORESULT) -> RioResult<()> {
        let Some(kind) = RioState::decode_req_ctx(res.RequestContext) else {
            return Ok(());
        };
        match kind {
            RioCompletionKind::Op {
                init,
                context: _completed_context,
            } => self.handle_op_completion(init, res),
        }
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

    pub(crate) fn begin_shutdown(&mut self) {
        self.actor_by_handle.clear();
        self.socket_runtime.clear();
        self.actors.clear();
    }

    pub(crate) fn shutdown_rio_actors(&mut self) {
        self.actors.clear();
        self.actor_by_handle.clear();
        self.socket_runtime.clear();
    }

    pub(crate) fn process_completions(
        &mut self,
        ops: &mut IocpOpRegistry,
        ext: &crate::ext::Extensions,
        registrar: &dyn veloq_buf::BufferRegistrar,
        completion_events: &SharedCompletionQueue,
        completion_table: &SharedCompletionTable<crate::op::IocpUserPayload, IocpError>,
    ) -> RioResult<usize> {
        self.process_completions_internal(ops, ext, registrar, completion_events, completion_table)
    }

    fn process_completions_internal(
        &mut self,
        ops: &mut IocpOpRegistry,
        ext: &crate::ext::Extensions,
        registrar: &dyn veloq_buf::BufferRegistrar,
        completion_events: &SharedCompletionQueue,
        completion_table: &SharedCompletionTable<crate::op::IocpUserPayload, IocpError>,
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
                events: completion_events,
                table: completion_table,
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
        completion_events: &SharedCompletionQueue,
        completion_table: &SharedCompletionTable<crate::op::IocpUserPayload, IocpError>,
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
                self.process_completions(ops, ext, registrar, completion_events, completion_table)?;
            if processed == 0 {
                std::thread::yield_now();
            }
        }

        Ok(())
    }
}
