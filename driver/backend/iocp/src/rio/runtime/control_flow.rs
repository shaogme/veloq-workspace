//! Actor coordination and completion routing for the RIO runtime.

use crate::IoFd;
use crate::config::{BorrowedRawHandle, SocketKey};
use crate::driver::IocpOpState;
use crate::ops::IocpOp;
use crate::rio::core::RioCompletionKind;
use crate::rio::core::registry::RioRegistry;
use crate::rio::core::rio_result_to_event_res;
use crate::rio::core::submit_ops::RioRq;
use crate::rio::core::{RioOpCtxGuard, RioPoolCtxGuard};
use crate::rio::error::{RioError, RioResult};
#[cfg(test)]
use crate::rio::runtime::pool::UdpRecvPoolDebugStats;
use crate::rio::runtime::pool::{UdpMailbox, UdpPoolManager, UdpPoolState};
use crate::rio::{ActorKey, RioCompletionContext, RioContext, RioEnv, RioState, SocketRuntimeMode};
use diagweave::report::ResultReportExt;
use slotmap::SlotMap;
use tracing::error;
use veloq_driver_core::driver::registry::OpRegistry;
use veloq_driver_core::driver::{
    CompletionEvent, SharedCompletionQueue, SharedCompletionTable, encode_completion_token,
};
use veloq_driver_core::slot::{SlotRegistryExt, SlotView};
use veloq_driver_core::{DriverErrorKind, driver_os_error};
use windows_sys::Win32::Networking::WinSock::{RIO_CORRUPT_CQ, RIORESULT};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RioActorState {
    Active,
    Draining,
}

pub(crate) struct RioSocketActor {
    pub(crate) socket_key: SocketKey,
    pub(crate) rq: RioRq,
    pub(crate) pool_manager: UdpPoolManager,
    pub(crate) udp_mailbox: UdpMailbox,
    pub(crate) is_explicit_shutdown: bool,
    pub(crate) state: RioActorState,
}

impl RioSocketActor {
    pub(crate) fn new(socket_key: SocketKey, rq: RioRq) -> Self {
        Self {
            socket_key,
            rq,
            pool_manager: UdpPoolManager::new(),
            udp_mailbox: UdpMailbox::new(),
            is_explicit_shutdown: false,
            state: RioActorState::Active,
        }
    }

    #[inline]
    pub(crate) const fn socket_key(&self) -> SocketKey {
        self.socket_key
    }
}

struct RioCompletionRouter<'a> {
    actors: &'a mut SlotMap<ActorKey, RioSocketActor>,
    actor_by_handle: &'a mut rustc_hash::FxHashMap<SocketKey, ActorKey>,
    outstanding_count: &'a mut usize,
    comp: RioCompletionContext<'a>,
    registry: &'a mut RioRegistry,
    env: RioEnv<'a>,
    completed_count: usize,
}

struct ActorStoreRefs<'a> {
    actors: &'a mut SlotMap<ActorKey, RioSocketActor>,
    actor_by_handle: &'a mut rustc_hash::FxHashMap<SocketKey, ActorKey>,
}

impl<'a> RioCompletionRouter<'a> {
    fn new(
        actor_store: ActorStoreRefs<'a>,
        outstanding_count: &'a mut usize,
        comp: RioCompletionContext<'a>,
        env: (&'a mut RioRegistry, RioEnv<'a>),
    ) -> Self {
        let (registry, env) = env;
        Self {
            actors: actor_store.actors,
            actor_by_handle: actor_store.actor_by_handle,
            outstanding_count,
            comp,
            registry,
            env,
            completed_count: 0,
        }
    }

    fn remove_actor_by_key(&mut self, key: ActorKey) {
        let Some(actor) = self.actors.remove(key) else {
            return;
        };
        let socket_key = actor.socket_key();
        if self.actor_by_handle.get(&socket_key).copied() == Some(key) {
            self.actor_by_handle.remove(&socket_key);
        }
    }

    fn on_pool_completion(
        &mut self,
        actor_key: ActorKey,
        generation: u32,
        res: &RIORESULT,
    ) -> RioResult<()> {
        let (pool_submissions, remove_actor) = {
            let Some(actor) = self.actors.get_mut(actor_key) else {
                return Ok(());
            };
            let Some(slot_key) = actor.pool_manager.ack_pool_done(generation) else {
                return Ok(());
            };
            let mut ctx = RioContext {
                env: self.env,
                actor_key,
                rq: actor.rq,
            };
            let (pool_manager, udp_mailbox) = (&mut actor.pool_manager, &mut actor.udp_mailbox);
            let submissions = pool_manager
                .handle_completion(udp_mailbox, (slot_key, res), &mut self.comp, &mut ctx)
                .attach_note("failed to handle pool completion")?;
            let remove = pool_manager.cleanup_drained_pool(&mut ctx);
            (submissions, remove)
        };
        if remove_actor {
            self.remove_actor_by_key(actor_key);
        }
        if *self.outstanding_count > 0 {
            *self.outstanding_count -= 1;
        }
        *self.outstanding_count += pool_submissions;
        self.completed_count += 1;
        Ok(())
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

                    let mut guard = slot.complete();
                    let result_for_slot = if res.Status == 0 {
                        Ok(res.BytesTransferred as usize)
                    } else {
                        Err(driver_os_error(
                            DriverErrorKind::Completion,
                            "rio.runtime.control_flow.handle_op_completion",
                            res.Status,
                            "rio completion returned os error",
                        ))
                    };
                    let res_code = rio_result_to_event_res(&result_for_slot);
                    let event = CompletionEvent {
                        user_data: encode_completion_token(user_data, generation),
                        res: res_code,
                        flags: 0,
                    };
                    let (payload, detail) = guard.take_completion_data();

                    self.comp
                        .table
                        .record_completion_with_data(event, payload, detail);
                    self.comp.events.push(event);
                    let _ = guard.take_op();
                    let _ = std::mem::take(guard.platform_mut());
                    self.comp.ops.shared.push_free(user_data);
                }
                Some(SlotView::InFlightOrphaned(mut slot)) => {
                    if slot.platform_mut().generation != generation {
                        return Ok(());
                    }

                    let mut guard = slot.complete();
                    let _ = guard.take_completion_data();
                    let _ = guard.take_op();
                    let _ = std::mem::take(guard.platform_mut());
                    self.comp.ops.shared.push_free(user_data);
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
            RioCompletionKind::Pool {
                actor_key,
                generation,
                ctx_ptr,
            } => {
                let _ctx_guard = RioPoolCtxGuard(ctx_ptr);
                self.on_pool_completion(actor_key, generation, res)
            }
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
    #[inline]
    pub(crate) fn build_ctx<'a>(
        _registry: &'a mut RioRegistry,
        env: RioEnv<'a>,
        actor: (ActorKey, RioRq),
    ) -> RioContext<'a> {
        let (actor_key, rq) = actor;
        RioContext { env, actor_key, rq }
    }

    fn remove_actor_by_key(&mut self, key: ActorKey) -> Option<RioSocketActor> {
        let actor = self.actors.remove(key)?;
        let socket_key = actor.socket_key();
        if self.actor_by_handle.get(&socket_key).copied() == Some(key) {
            self.actor_by_handle.remove(&socket_key);
        }
        Some(actor)
    }

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
                .ok_or_else(|| diagweave::report::Report::new(RioError::Internal))
                .attach_note("failed to retrieve indexed actor");
        }

        let rq = match self.registry.create_rq((handle, fd), env) {
            Ok(rq) => rq,
            Err(e) => {
                let diag = format!(
                    "ensure_actor_create_rq: fd={fd:?}, handle={:?}, socket_raw=0x{:x}, rq_depth={}, max_outstanding_recvs={}, max_outstanding_sends={}, max_receive_data_buffers=1, max_send_data_buffers=1, outstanding_count={}, actors_len={}, actor_index_hit={}",
                    handle.raw().as_handle(),
                    handle.raw().as_handle() as usize,
                    self.registry.rq_depth,
                    self.registry.rq_depth,
                    self.registry.rq_depth,
                    self.outstanding_count,
                    self.actors.len(),
                    self.actor_by_handle.contains_key(&socket_key),
                );
                error!(
                    fd = ?fd,
                    handle = ?handle.raw().as_handle(),
                    socket_raw = handle.raw().as_handle() as usize,
                    rq_depth = self.registry.rq_depth,
                    max_outstanding_recvs = self.registry.rq_depth,
                    max_outstanding_sends = self.registry.rq_depth,
                    max_receive_data_buffers = 1_u32,
                    max_send_data_buffers = 1_u32,
                    outstanding_count = self.outstanding_count,
                    actors_len = self.actors.len(),
                    actor_index_hit = self.actor_by_handle.contains_key(&socket_key),
                    rio_error = %e,
                    "RIOCreateRequestQueue failed diagnostics"
                );
                return Err(e.attach_note(diag));
            }
        };

        let actor = RioSocketActor::new(socket_key, rq);
        let key = self.actors.insert(actor);
        self.actor_by_handle.insert(socket_key, key);
        let state = self.socket_runtime.entry(socket_key).or_default();
        state.mode = SocketRuntimeMode::RioPreferred;
        state.iocp_associated = false;
        self.actors
            .get_mut(key)
            .ok_or_else(|| diagweave::report::Report::new(RioError::Internal))
            .attach_note("failed to retrieve inserted actor")
    }

    pub(crate) fn warmup_udp_socket(
        &mut self,
        target: (IoFd, BorrowedRawHandle<'_>),
        requested_chunk_size: usize,
        credits: usize,
        registrar: &dyn veloq_buf::BufferRegistrar,
    ) -> RioResult<usize> {
        let (fd, handle) = target;
        let socket_key = handle.raw().actor_key();
        let Some(dispatch) = self.kernel.dispatch else {
            return Err(diagweave::report::Report::new(RioError::NotSupported))
                .attach_note("RIO dispatch not available for UDP warmup");
        };
        let Some(cq) = (!self.kernel.cq.is_invalid()).then_some(self.kernel.cq) else {
            return Err(diagweave::report::Report::new(RioError::NotSupported))
                .attach_note("RIO dispatch not available for UDP warmup");
        };
        let env = RioEnv {
            registrar,
            dispatch: &dispatch,
            cq,
            registration_mode: self.registration_mode,
        };
        let _ = self.ensure_actor((fd, handle), env)?;

        if self.is_iocp_fallback(socket_key) {
            return Err(diagweave::report::Report::new(RioError::NotSupported))
                .attach_note("Socket is marked for IOCP fallback");
        }

        let key = self
            .actor_by_handle
            .get(&socket_key)
            .copied()
            .ok_or_else(|| diagweave::report::Report::new(RioError::Internal))
            .attach_note("actor not found")?;

        let actor = self
            .actors
            .get_mut(key)
            .ok_or_else(|| diagweave::report::Report::new(RioError::Internal))
            .attach_note("actor not found")?;
        let mut ctx = Self::build_ctx(&mut self.registry, env, (key, actor.rq));
        let (pool_manager, udp_mailbox) = (&mut actor.pool_manager, &mut actor.udp_mailbox);
        pool_manager.warmup_pool(udp_mailbox, requested_chunk_size, credits, &mut ctx)
    }

    pub(crate) fn shutdown_actor(&mut self, socket_key: SocketKey) {
        let Some(key) = self.actor_by_handle.remove(&socket_key) else {
            return;
        };

        let should_remove = {
            let Some(actor) = self.actors.get_mut(key) else {
                return;
            };

            actor.is_explicit_shutdown = true;
            actor.state = RioActorState::Draining;

            if actor.pool_manager.pool.state == UdpPoolState::Uninitialized {
                true
            } else {
                let Some(env) = self
                    .kernel
                    .env(&veloq_buf::NoopRegistrar, self.registration_mode)
                else {
                    // If we can't get an environment, we can't cleanly shutdown the pool.
                    // This is rare and usually means the driver is already destroyed.
                    return;
                };

                let mut ctx = Self::build_ctx(&mut self.registry, env, (key, actor.rq));
                let (pool_manager, udp_mailbox) = (&mut actor.pool_manager, &mut actor.udp_mailbox);
                pool_manager.shutdown_pool(udp_mailbox);
                pool_manager.cleanup_drained_pool(&mut ctx)
            }
        };

        if should_remove {
            let _ = self.actors.remove(key);
        }
    }

    pub(crate) fn begin_shutdown(&mut self) {
        self.actor_by_handle.clear();
        self.socket_runtime.clear();
        let Some(env) = self
            .kernel
            .env(&veloq_buf::NoopRegistrar, self.registration_mode)
        else {
            self.actors.clear();
            return;
        };

        self.actors.retain(|key, actor| {
            actor.is_explicit_shutdown = true;
            actor.state = RioActorState::Draining;
            let (pool_manager, udp_mailbox) = (&mut actor.pool_manager, &mut actor.udp_mailbox);
            pool_manager.shutdown_pool(udp_mailbox);

            let mut ctx = Self::build_ctx(&mut self.registry, env, (key, actor.rq));
            !pool_manager.cleanup_drained_pool(&mut ctx)
        });
    }

    pub(crate) fn cancel_udp_waiter(
        &mut self,
        socket_key: SocketKey,
        uid: (usize, u32),
        registrar: &dyn veloq_buf::BufferRegistrar,
    ) {
        let Some(env) = self.kernel.env(registrar, self.registration_mode) else {
            return;
        };
        if let Some(key) = self.actor_by_handle.get(&socket_key).copied()
            && let Some(actor) = self.actors.get_mut(key)
        {
            let mut ctx = Self::build_ctx(&mut self.registry, env, (key, actor.rq));
            let (pool_manager, udp_mailbox) = (&mut actor.pool_manager, &mut actor.udp_mailbox);
            pool_manager.cancel_waiter(udp_mailbox, uid, &mut ctx);
        }

        for (key, actor) in self.actors.iter_mut().filter(|(_, actor)| {
            actor.state == RioActorState::Draining && actor.socket_key() == socket_key
        }) {
            let mut ctx = Self::build_ctx(&mut self.registry, env, (key, actor.rq));
            let (pool_manager, udp_mailbox) = (&mut actor.pool_manager, &mut actor.udp_mailbox);
            pool_manager.cancel_waiter(udp_mailbox, uid, &mut ctx);
        }
    }

    #[cfg(test)]
    pub(crate) fn udp_pool_debug_stats(
        &self,
        socket_key: SocketKey,
    ) -> Option<UdpRecvPoolDebugStats> {
        self.actor_by_handle
            .get(&socket_key)
            .and_then(|&key| self.actors.get(key))
            .and_then(|actor| actor.pool_manager.udp_pool_debug_stats(&actor.udp_mailbox))
            .or_else(|| {
                self.actors
                    .values()
                    .find(|actor| {
                        actor.state == RioActorState::Draining && actor.socket_key() == socket_key
                    })
                    .and_then(|actor| actor.pool_manager.udp_pool_debug_stats(&actor.udp_mailbox))
            })
    }

    #[cfg(test)]
    pub(crate) fn debug_tick_udp_pool_idle(
        &mut self,
        socket_key: SocketKey,
        ticks: usize,
        registrar: &dyn veloq_buf::BufferRegistrar,
    ) -> RioResult<()> {
        let Some(env) = self.kernel.env(registrar, self.registration_mode) else {
            return Ok(());
        };
        if let Some(key) = self.actor_by_handle.get(&socket_key).copied()
            && let Some(actor) = self.actors.get_mut(key)
        {
            let mut ctx = Self::build_ctx(&mut self.registry, env, (key, actor.rq));
            let (pool_manager, udp_mailbox) = (&mut actor.pool_manager, &actor.udp_mailbox);
            for _ in 0..ticks {
                pool_manager.rebalance_udp_pool(udp_mailbox, &mut ctx)?;
            }
        }
        Ok(())
    }

    pub(crate) fn forget_udp_contexts(&mut self) {
        for actor in self.actors.values_mut() {
            actor.pool_manager.registry.map.clear();
        }
    }

    pub(crate) fn shutdown_rio_actors(&mut self, registrar: &dyn veloq_buf::BufferRegistrar) {
        let env_opt = self.kernel.env(registrar, self.registration_mode);

        // Use drain to take ownership of all actors and clear the map.
        for (key, mut actor) in self.actors.drain() {
            if let Some(env) = &env_opt {
                let mut ctx = Self::build_ctx(&mut self.registry, *env, (key, actor.rq));
                let (pool_manager, udp_mailbox) = (&mut actor.pool_manager, &mut actor.udp_mailbox);
                pool_manager.forget_and_cleanup(udp_mailbox, &mut ctx);
            }
            // Even if env is missing, the actor is now dropped and its RQ will be closed
            // when the socket is closed (which happens in veloq-runtime).
        }
        self.actor_by_handle.clear();
        self.socket_runtime.clear();
    }

    pub(crate) fn mark_pool_done(
        &mut self,
        actor_key: ActorKey,
        completion_generation: u32,
    ) -> bool {
        {
            let Some(actor) = self.actors.get_mut(actor_key) else {
                return false;
            };
            let _ = actor.pool_manager.ack_pool_done(completion_generation);
            actor.pool_manager.handle_drain_comp();
        }

        let Some(env) = self
            .kernel
            .env(&veloq_buf::NoopRegistrar, self.registration_mode)
        else {
            let _ = self.remove_actor_by_key(actor_key);
            return true;
        };

        let should_remove = {
            let Some(actor) = self.actors.get_mut(actor_key) else {
                return false;
            };
            let mut ctx = Self::build_ctx(&mut self.registry, env, (actor_key, actor.rq));
            if actor.pool_manager.cleanup_drained_pool(&mut ctx) {
                // We ONLY remove the actor from the store if it's explicitly shutting down.
                // If it's just a pool drain due to fallback, we keep the actor state until the
                // socket itself is dropped.
                actor.is_explicit_shutdown
            } else {
                false
            }
        };

        if should_remove {
            let _ = self.remove_actor_by_key(actor_key);
        }
        true
    }

    pub(crate) fn process_completions(
        &mut self,
        ops: &mut OpRegistry<IocpOp, IocpOpState, crate::ops::OverlappedEntry>,
        registrar: &dyn veloq_buf::BufferRegistrar,
        completion_events: &SharedCompletionQueue,
        completion_table: &SharedCompletionTable,
    ) -> RioResult<usize> {
        self.process_completions_internal(ops, registrar, completion_events, completion_table)
    }

    fn process_completions_internal(
        &mut self,
        ops: &mut OpRegistry<IocpOp, IocpOpState, crate::ops::OverlappedEntry>,
        registrar: &dyn veloq_buf::BufferRegistrar,
        completion_events: &SharedCompletionQueue,
        completion_table: &SharedCompletionTable,
    ) -> RioResult<usize> {
        const MAX_RIO_RESULTS: usize = 128;
        // SAFETY: RIORESULT is a POD struct and safe to zero-initialize.
        let mut results: [RIORESULT; MAX_RIO_RESULTS] = unsafe { std::mem::zeroed() };
        let Some(env) = self.kernel.env(registrar, self.registration_mode) else {
            return Ok(0);
        };
        let mut router = RioCompletionRouter::new(
            ActorStoreRefs {
                actors: &mut self.actors,
                actor_by_handle: &mut self.actor_by_handle,
            },
            &mut self.outstanding_count,
            RioCompletionContext {
                ops,
                events: completion_events,
                table: completion_table,
            },
            (&mut self.registry, env),
        );
        loop {
            let count = self.kernel.dequeue(&mut results);
            if count == RIO_CORRUPT_CQ {
                return Err(diagweave::report::Report::new(RioError::Internal))
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
