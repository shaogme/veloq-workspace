//! Actor coordination and completion routing for the RIO runtime.

use crate::IoFd;
use crate::driver::IocpOpState;
use crate::ops::IocpOp;
use crate::rio::core::RioCompletionKind;
use crate::rio::core::registry::RioRegistry;
use crate::rio::core::rio_result_to_event_res;
use crate::rio::core::submit_ops::RioRq;
use crate::rio::core::{RioOpCtxGuard, RioPoolCtxGuard};
use crate::rio::error::{RioDiag, RioError, RioReportExt, RioResult};
#[cfg(test)]
use crate::rio::runtime::pool::UdpRecvPoolDebugStats;
use crate::rio::runtime::pool::{UdpMailbox, UdpPoolManager, UdpPoolState};
use crate::rio::{ActorKey, RioCompletionContext, RioContext, RioEnv, RioState};
use error_stack::ResultExt;
use slotmap::SlotMap;
use std::io;
use tracing::error;
use veloq_driver_core::driver::{
    CompletionEvent, SharedCompletionQueue, SharedCompletionTable, encode_completion_token,
};
use veloq_driver_core::op_registry::OpRegistry;
use veloq_driver_core::slot::{SlotRegistryExt, SlotView};
use windows_sys::Win32::Foundation::HANDLE;
use windows_sys::Win32::Networking::WinSock::{RIO_CORRUPT_CQ, RIORESULT};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RioActorState {
    Active,
    Draining,
}

pub(crate) struct RioSocketActor {
    pub(crate) handle: HANDLE,
    pub(crate) rq: RioRq,
    pub(crate) pool_manager: UdpPoolManager,
    pub(crate) udp_mailbox: UdpMailbox,
    pub(crate) state: RioActorState,
}

impl RioSocketActor {
    pub(crate) fn new(handle: HANDLE, rq: RioRq) -> Self {
        Self {
            handle,
            rq,
            pool_manager: UdpPoolManager::new(),
            udp_mailbox: UdpMailbox::new(),
            state: RioActorState::Active,
        }
    }
}

struct RioCompletionRouter<'a> {
    actors: &'a mut SlotMap<ActorKey, RioSocketActor>,
    actor_by_handle: &'a mut rustc_hash::FxHashMap<HANDLE, ActorKey>,
    outstanding_count: &'a mut usize,
    comp: RioCompletionContext<'a>,
    registry: &'a mut RioRegistry,
    env: RioEnv<'a>,
    completed_count: usize,
}

struct ActorStoreRefs<'a> {
    actors: &'a mut SlotMap<ActorKey, RioSocketActor>,
    actor_by_handle: &'a mut rustc_hash::FxHashMap<HANDLE, ActorKey>,
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
        if self.actor_by_handle.get(&actor.handle).copied() == Some(key) {
            self.actor_by_handle.remove(&actor.handle);
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
                .attach("failed to handle pool completion")?;
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
                        Err(io::Error::from_raw_os_error(res.Status))
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
        if self.actor_by_handle.get(&actor.handle).copied() == Some(key) {
            self.actor_by_handle.remove(&actor.handle);
        }
        Some(actor)
    }

    pub(crate) fn ensure_actor(
        &mut self,
        target: (IoFd, HANDLE),
        env: RioEnv<'_>,
    ) -> RioResult<&mut RioSocketActor> {
        let (fd, handle) = target;
        if let Some(key) = self.actor_by_handle.get(&handle).copied() {
            return self
                .actors
                .get_mut(key)
                .ok_or_else(|| error_stack::Report::new(RioError::Internal))
                .attach("failed to retrieve indexed actor");
        }

        let rq = self.registry.create_rq((handle, fd), env).map_err(|e| {
            let source = e.to_string();
            let wsa_class = RioDiag::wsa_class_from_text(&source);
            let diag = RioDiag::new("ensure_actor_create_rq")
                .field("fd", format!("{fd:?}"))
                .field("handle", format!("{handle:?}"))
                .field("socket_raw", format!("0x{:x}", handle as usize))
                .field("rq_depth", self.registry.rq_depth)
                .field("max_outstanding_recvs", self.registry.rq_depth)
                .field("max_outstanding_sends", self.registry.rq_depth)
                .field("max_receive_data_buffers", 1_u32)
                .field("max_send_data_buffers", 1_u32)
                .field("outstanding_count", self.outstanding_count)
                .field("actors_len", self.actors.len())
                .field("actor_index_hit", self.actor_by_handle.contains_key(&handle))
                .field("wsa_class", wsa_class);
            error!(
                fd = ?fd,
                handle = ?handle,
                socket_raw = handle as usize,
                rq_depth = self.registry.rq_depth,
                max_outstanding_recvs = self.registry.rq_depth,
                max_outstanding_sends = self.registry.rq_depth,
                max_receive_data_buffers = 1_u32,
                max_send_data_buffers = 1_u32,
                outstanding_count = self.outstanding_count,
                actors_len = self.actors.len(),
                actor_index_hit = self.actor_by_handle.contains_key(&handle),
                wsa_class = wsa_class,
                rio_error = %e,
                "RIOCreateRequestQueue failed diagnostics"
            );
            e.attach(diag.to_string())
        })?;
        let key = self.actors.insert(RioSocketActor::new(handle, rq));
        self.actor_by_handle.insert(handle, key);
        self.actors
            .get_mut(key)
            .ok_or_else(|| error_stack::Report::new(RioError::Internal))
            .attach("failed to retrieve inserted actor")
    }

    pub(crate) fn shutdown_actor(&mut self, handle: HANDLE) {
        self.udp_iocp_fallback_handles.remove(&handle);
        let Some(key) = self.actor_by_handle.get(&handle).copied() else {
            return;
        };
        let Some(env) = self
            .kernel
            .env(&veloq_buf::NoopRegistrar, self.registration_mode)
        else {
            let _ = self.remove_actor_by_key(key);
            return;
        };

        let should_remove = {
            let Some(actor) = self.actors.get_mut(key) else {
                return;
            };
            actor.state = RioActorState::Draining;
            if actor.pool_manager.pool.state == UdpPoolState::Uninitialized {
                true
            } else {
                let mut ctx = Self::build_ctx(&mut self.registry, env, (key, actor.rq));
                let (pool_manager, udp_mailbox) = (&mut actor.pool_manager, &mut actor.udp_mailbox);
                pool_manager.shutdown_pool(udp_mailbox);
                pool_manager.cleanup_drained_pool(&mut ctx)
            }
        };

        self.actor_by_handle.remove(&handle);
        if should_remove {
            let _ = self.remove_actor_by_key(key);
        }
    }

    pub(crate) fn begin_shutdown(&mut self) {
        for actor in self.actors.values_mut() {
            let (pool_manager, udp_mailbox) = (&mut actor.pool_manager, &mut actor.udp_mailbox);
            pool_manager.shutdown_pool(udp_mailbox);
            actor.state = RioActorState::Draining;
        }
        self.actor_by_handle.clear();
    }

    pub(crate) fn cancel_udp_waiter(
        &mut self,
        handle: HANDLE,
        uid: (usize, u32),
        registrar: &dyn veloq_buf::BufferRegistrar,
    ) {
        let Some(env) = self.kernel.env(registrar, self.registration_mode) else {
            return;
        };
        if let Some(key) = self.actor_by_handle.get(&handle).copied()
            && let Some(actor) = self.actors.get_mut(key)
        {
            let mut ctx = Self::build_ctx(&mut self.registry, env, (key, actor.rq));
            let (pool_manager, udp_mailbox) = (&mut actor.pool_manager, &mut actor.udp_mailbox);
            pool_manager.cancel_waiter(udp_mailbox, uid, &mut ctx);
        }

        for (key, actor) in self
            .actors
            .iter_mut()
            .filter(|(_, actor)| actor.state == RioActorState::Draining && actor.handle == handle)
        {
            let mut ctx = Self::build_ctx(&mut self.registry, env, (key, actor.rq));
            let (pool_manager, udp_mailbox) = (&mut actor.pool_manager, &mut actor.udp_mailbox);
            pool_manager.cancel_waiter(udp_mailbox, uid, &mut ctx);
        }
    }

    #[cfg(test)]
    pub(crate) fn udp_pool_debug_stats(&self, handle: HANDLE) -> Option<UdpRecvPoolDebugStats> {
        self.actor_by_handle
            .get(&handle)
            .and_then(|&key| self.actors.get(key))
            .and_then(|actor| actor.pool_manager.udp_pool_debug_stats(&actor.udp_mailbox))
            .or_else(|| {
                self.actors
                    .values()
                    .find(|actor| actor.state == RioActorState::Draining && actor.handle == handle)
                    .and_then(|actor| actor.pool_manager.udp_pool_debug_stats(&actor.udp_mailbox))
            })
    }

    #[cfg(test)]
    pub(crate) fn debug_tick_udp_pool_idle(
        &mut self,
        handle: HANDLE,
        ticks: usize,
        registrar: &dyn veloq_buf::BufferRegistrar,
    ) -> RioResult<()> {
        let Some(env) = self.kernel.env(registrar, self.registration_mode) else {
            return Ok(());
        };
        if let Some(key) = self.actor_by_handle.get(&handle).copied()
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
        let Some(env) = self.kernel.env(registrar, self.registration_mode) else {
            self.actors.clear();
            self.actor_by_handle.clear();
            self.udp_iocp_fallback_handles.clear();
            return;
        };
        for (key, actor) in self.actors.iter_mut() {
            let mut ctx = Self::build_ctx(&mut self.registry, env, (key, actor.rq));
            let (pool_manager, udp_mailbox) = (&mut actor.pool_manager, &mut actor.udp_mailbox);
            pool_manager.forget_and_cleanup(udp_mailbox, &mut ctx);
        }
        self.actors.clear();
        self.actor_by_handle.clear();
        self.udp_iocp_fallback_handles.clear();
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
            actor.pool_manager.cleanup_drained_pool(&mut ctx)
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
    ) -> io::Result<usize> {
        self.process_completions_internal(ops, registrar, completion_events, completion_table)
            .map_err(|e| e.to_io_error("RIO completion processing failed"))
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
                return Err(error_stack::Report::new(RioError::Internal))
                    .attach("RIO completion queue is corrupt (RIO_CORRUPT_CQ)");
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
