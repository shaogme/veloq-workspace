//! Actor coordination and completion routing for the RIO runtime.

use crate::IoFd;
use crate::config::{BorrowedRawHandle, SocketKey};
use crate::driver::IocpOpRegistry;
use crate::error::IocpError;
use crate::rio::core::RioCompletionKind;
use crate::rio::core::RioOpCtxGuard;
use crate::rio::core::registry::RioRegistry;
use crate::rio::core::rio_result_to_event_res;
use crate::rio::core::submit_ops::RioRq;
use crate::rio::error::{RioError, RioResult};
use crate::rio::{RioCompletionContext, RioEnv, RioState, SocketRuntimeMode, SocketRuntimeState};
use diagweave::prelude::*;
use rustc_hash::FxHashMap;
use veloq_driver_core::driver::{
    CompletionEvent, SharedCompletionQueue, SharedCompletionTable, encode_completion_token,
};
use veloq_driver_core::slot::{SlotRegistryExt, SlotView};
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

    fn handle_op_completion(
        &mut self,
        user_data: usize,
        generation: u32,
        res: &RIORESULT,
    ) -> RioResult<()> {
        let ops = &mut self.comp.ops;

        if user_data < ops.local.len() {
            match ops.slot_view(user_data) {
                Some(SlotView::InFlightWaiting(mut slot)) => {
                    if slot.platform_mut().generation != generation {
                        return Ok(());
                    }

                    let mut completion = if res.Status == 0 {
                        Ok(res.BytesTransferred as usize)
                    } else {
                        Err(IocpError::CompletionWait
                            .to_report()
                            .with_ctx("scope", "rio.runtime.control_flow.handle_op_completion")
                            .set_error_code(res.Status)
                            .attach_note("rio completion returned os error"))
                    };
                    let socket_key = slot
                        .with_op_mut(|iocp_op| {
                            let socket_key = if iocp_op.header.in_flight {
                                iocp_op.header.in_flight = false;
                                iocp_op
                                    .header
                                    .resolved_handle
                                    .filter(|handle| handle.is_socket())
                                    .map(|handle| handle.actor_key())
                            } else {
                                None
                            };
                            if let Ok(bytes) = completion {
                                completion = iocp_op
                                    .on_complete(bytes, self.comp.ext)
                                    .with_ctx(
                                        "scope",
                                        "rio.runtime.control_flow.handle_op_completion",
                                    )
                                    .attach_note("rio op completion hook failed");
                            }
                            socket_key
                        })
                        .flatten();
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
                    if let Some(socket_key) = socket_key {
                        self.release_socket_inflight(socket_key);
                    }
                }
                Some(SlotView::InFlightOrphaned(mut slot)) => {
                    if slot.platform_mut().generation != generation {
                        return Ok(());
                    }

                    let mut guard = slot.complete();
                    let _ = guard.take_op();
                    let _ = guard.take_completion_data();
                    let _ = std::mem::take(guard.platform_mut());
                    self.comp.ops.recycle(user_data, generation.wrapping_add(1));
                }
                _ => {}
            }
        }

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
                user_data,
                generation,
                ctx_ptr,
            } => {
                let _ctx_guard = RioOpCtxGuard(ctx_ptr);
                self.handle_op_completion(user_data, generation, res)
            }
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

        let rq = self.registry.create_rq((handle, fd), env)
            .with_ctx("scope", "rio.runtime.control_flow.ensure_actor")
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
        let state = self.socket_runtime.entry(socket_key).or_default();
        state.mode = SocketRuntimeMode::RioPreferred;
        state.iocp_associated = false;
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
}
