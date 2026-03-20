//! Actor coordination and completion routing for the RIO runtime.

use crate::IoFd;
use crate::driver::IocpOpState;
use crate::ops::IocpOp;
use crate::rio::core::RioCompletionKind;
use crate::rio::core::RioOpCtxGuard;
use crate::rio::core::registry::RioRegistry;
use crate::rio::core::rio_result_to_event_res;
use crate::rio::core::submit_ops::RioRq;
use crate::rio::runtime::pool::UdpPoolManager;
#[cfg(test)]
use crate::rio::runtime::pool::UdpRecvPoolDebugStats;
use crate::rio::{RioCompletionContext, RioContext, RioEnv, RioState};
use rustc_hash::FxHashMap;
use std::io;
use veloq_driver_core::driver::{
    CompletionEvent, SharedCompletionQueue, SharedCompletionTable, encode_completion_token,
};
use veloq_driver_core::op_registry::OpRegistry;
use veloq_driver_core::slot::{SlotRegistryExt, SlotView};
use windows_sys::Win32::Foundation::HANDLE;
use windows_sys::Win32::Networking::WinSock::{RIO_CORRUPT_CQ, RIORESULT};

pub(crate) struct RioSocketActor {
    pub(crate) actor_id: u32,
    pub(crate) rq: RioRq,
    pub(crate) pool_manager: UdpPoolManager,
}

impl RioSocketActor {
    pub(crate) fn new(actor_id: u32, rq: RioRq) -> Self {
        Self {
            actor_id,
            rq,
            pool_manager: UdpPoolManager::new(),
        }
    }
}

struct RioCompletionRouter<'a> {
    actors: &'a mut FxHashMap<HANDLE, RioSocketActor>,
    actor_routes: &'a mut FxHashMap<u32, HANDLE>,
    outstanding_count: &'a mut usize,
    comp: RioCompletionContext<'a>,
    registry: &'a mut RioRegistry,
    env: RioEnv<'a>,
    completed_count: usize,
}

impl<'a> RioCompletionRouter<'a> {
    fn new(
        actors: &'a mut FxHashMap<HANDLE, RioSocketActor>,
        router_ctx: (&'a mut FxHashMap<u32, HANDLE>, &'a mut usize),
        comp: RioCompletionContext<'a>,
        env: (&'a mut RioRegistry, RioEnv<'a>),
    ) -> Self {
        let (actor_routes, outstanding_count) = router_ctx;
        let (registry, env) = env;
        Self {
            actors,
            actor_routes,
            outstanding_count,
            comp,
            registry,
            env,
            completed_count: 0,
        }
    }

    fn on_pool_completion(&mut self, actor_id: u32, generation: u32, res: &RIORESULT) {
        let Some(&handle) = self.actor_routes.get(&actor_id) else {
            return;
        };
        let (pool_submissions, remove_actor) = {
            let Some(actor) = self.actors.get_mut(&handle) else {
                return;
            };
            let Some(slot_idx) = actor.pool_manager.ack_pool_done(generation) else {
                return;
            };
            let mut ctx = RioContext {
                registry: self.registry,
                env: self.env,
                actor_id: actor.actor_id,
                rq: actor.rq,
            };
            let submissions =
                actor
                    .pool_manager
                    .handle_completion((slot_idx, res), &mut self.comp, &mut ctx);
            let remove = actor.pool_manager.cleanup_drained_pool(&mut ctx);
            (submissions, remove)
        };
        if remove_actor {
            self.actors.remove(&handle);
            self.actor_routes.remove(&actor_id);
        }
        *self.outstanding_count -= 1;
        *self.outstanding_count += pool_submissions;
        self.completed_count += 1;
    }

    fn handle_op_completion(&mut self, user_data: usize, generation: u32, res: &RIORESULT) {
        let ops = &mut self.comp.ops;

        if user_data < ops.local.len() {
            match ops.slot_view(user_data) {
                Some(SlotView::InFlightWaiting(mut slot)) => {
                    if slot.platform_mut().generation != generation {
                        return;
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
                        return;
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
    }

    fn handle_one(&mut self, res: &RIORESULT) {
        let Some(kind) = RioState::decode_req_ctx(res.RequestContext) else {
            return;
        };

        match kind {
            RioCompletionKind::Pool {
                actor_id,
                generation,
            } => self.on_pool_completion(actor_id, generation, res),
            RioCompletionKind::Op {
                user_data,
                generation,
                ctx_ptr,
            } => {
                let _ctx_guard = RioOpCtxGuard(ctx_ptr);
                self.handle_op_completion(user_data, generation, res);
            }
        }
    }
}

impl RioState {
    #[inline]
    pub(crate) fn build_ctx<'a>(
        registry: &'a mut RioRegistry,
        env: RioEnv<'a>,
        actor: (u32, RioRq),
    ) -> RioContext<'a> {
        let (actor_id, rq) = actor;
        RioContext {
            registry,
            env,
            actor_id,
            rq,
        }
    }

    pub(crate) fn alloc_actor_id(&mut self) -> u32 {
        loop {
            let id = self.next_actor_id;
            self.next_actor_id = self.next_actor_id.wrapping_add(1);
            if id == 0 {
                continue;
            }
            if !self.actor_routes.contains_key(&id) {
                return id;
            }
        }
    }

    pub(crate) fn ensure_actor(
        &mut self,
        target: (IoFd, HANDLE),
        env: RioEnv<'_>,
    ) -> io::Result<&mut RioSocketActor> {
        let (fd, handle) = target;
        if !self.actors.contains_key(&handle) {
            let rq = self.registry.create_rq((handle, fd), env)?;
            let actor_id = self.alloc_actor_id();
            self.actor_routes.insert(actor_id, handle);
            self.actors
                .insert(handle, RioSocketActor::new(actor_id, rq));
        }
        self.actors
            .get_mut(&handle)
            .ok_or_else(|| io::Error::other("failed to retrieve inserted actor"))
    }

    pub(crate) fn shutdown_udp_pool(&mut self, handle: HANDLE) {
        let Some(env) = self
            .kernel
            .env(&veloq_buf::NoopRegistrar, self.registration_mode)
        else {
            if let Some(actor) = self.actors.remove(&handle) {
                self.actor_routes.remove(&actor.actor_id);
            }
            return;
        };
        let mut remove_actor = None;
        if let Some(actor) = self.actors.get_mut(&handle) {
            let mut ctx = Self::build_ctx(&mut self.registry, env, (actor.actor_id, actor.rq));
            actor.pool_manager.shutdown_pool();
            if actor.pool_manager.cleanup_drained_pool(&mut ctx) {
                remove_actor = Some(actor.actor_id);
            }
        }
        if let Some(actor_id) = remove_actor {
            self.actors.remove(&handle);
            self.actor_routes.remove(&actor_id);
        }
    }

    pub(crate) fn begin_shutdown(&mut self) {
        for actor in self.actors.values_mut() {
            actor.pool_manager.shutdown_pool();
        }
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
        if let Some(actor) = self.actors.get_mut(&handle) {
            let mut ctx = Self::build_ctx(&mut self.registry, env, (actor.actor_id, actor.rq));
            actor.pool_manager.cancel_waiter(uid, &mut ctx);
        }
    }

    #[cfg(test)]
    pub(crate) fn udp_pool_debug_stats(&self, handle: HANDLE) -> Option<UdpRecvPoolDebugStats> {
        self.actors
            .get(&handle)
            .and_then(|actor| actor.pool_manager.udp_pool_debug_stats())
    }

    #[cfg(test)]
    pub(crate) fn debug_tick_udp_pool_idle(
        &mut self,
        handle: HANDLE,
        ticks: usize,
        registrar: &dyn veloq_buf::BufferRegistrar,
    ) -> io::Result<()> {
        let Some(env) = self.kernel.env(registrar, self.registration_mode) else {
            return Ok(());
        };
        if let Some(actor) = self.actors.get_mut(&handle) {
            let mut ctx = Self::build_ctx(&mut self.registry, env, (actor.actor_id, actor.rq));
            for _ in 0..ticks {
                actor.pool_manager.rebalance_udp_pool(&mut ctx)?;
            }
        }
        Ok(())
    }

    pub(crate) fn forget_udp_contexts(&mut self) {
        for actor in self.actors.values_mut() {
            actor.pool_manager.udp_ctx_map.clear();
        }
    }

    pub(crate) fn shutdown_rio_actors(&mut self, registrar: &dyn veloq_buf::BufferRegistrar) {
        let Some(env) = self.kernel.env(registrar, self.registration_mode) else {
            self.actors.clear();
            self.actor_routes.clear();
            return;
        };
        for actor in self.actors.values_mut() {
            let mut ctx = Self::build_ctx(&mut self.registry, env, (actor.actor_id, actor.rq));
            actor.pool_manager.forget_and_cleanup(&mut ctx);
        }
        self.actors.clear();
        self.actor_routes.clear();
    }

    pub(crate) fn mark_pool_done(&mut self, actor_id: u32, completion_generation: u32) -> bool {
        let Some(handle) = self.actor_routes.get(&actor_id).copied() else {
            return false;
        };
        let Some(actor) = self.actors.get_mut(&handle) else {
            return false;
        };
        let _ = actor.pool_manager.ack_pool_done(completion_generation);
        actor.pool_manager.handle_drain_comp();
        let Some(env) = self
            .kernel
            .env(&veloq_buf::NoopRegistrar, self.registration_mode)
        else {
            self.actor_routes.remove(&actor_id);
            self.actors.remove(&handle);
            return true;
        };
        let mut ctx = Self::build_ctx(&mut self.registry, env, (actor_id, actor.rq));
        if actor.pool_manager.cleanup_drained_pool(&mut ctx) {
            self.actor_routes.remove(&actor_id);
            self.actors.remove(&handle);
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
        const MAX_RIO_RESULTS: usize = 128;
        // SAFETY: RIORESULT is a POD struct and safe to zero-initialize.
        let mut results: [RIORESULT; MAX_RIO_RESULTS] = unsafe { std::mem::zeroed() };
        let Some(env) = self.kernel.env(registrar, self.registration_mode) else {
            return Ok(0);
        };
        let mut router = RioCompletionRouter::new(
            &mut self.actors,
            (&mut self.actor_routes, &mut self.outstanding_count),
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
                return Err(io::Error::other(
                    "RIO completion queue is corrupt (RIO_CORRUPT_CQ)",
                ));
            }
            if count == 0 {
                break;
            }

            for res in results.iter().take(count as usize) {
                router.handle_one(res);
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
