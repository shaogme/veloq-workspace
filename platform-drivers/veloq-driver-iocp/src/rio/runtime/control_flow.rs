//! Actor coordination and completion routing for the RIO runtime.

use crate::IoFd;
use crate::driver::{IocpOpState, OpLifecycle};
use crate::ops::slot_ext::IocpSlotExt;
use crate::ops::IocpOp;
use crate::rio::core::RioCompletionKind;
use crate::rio::core::RioOpCtxGuard;
use crate::rio::core::registry::RioRegistry;
use crate::rio::core::rio_result_to_event_res;
use crate::rio::runtime::pool::UdpPoolManager;
use crate::rio::{RioCompletionContext, RioContext, RioEnv, RioState};
use rustc_hash::FxHashMap;
use std::io;
use veloq_driver_core::driver::{
    CompletionEvent, SharedCompletionQueue, SharedCompletionTable, encode_completion_token,
};
use veloq_driver_core::op_registry::OpRegistry;
use windows_sys::Win32::Foundation::HANDLE;
use windows_sys::Win32::Networking::WinSock::{RIO_CORRUPT_CQ, RIO_RQ, RIORESULT};

pub(crate) struct RioSocketActor {
    pub(crate) actor_id: u32,
    pub(crate) rq: RIO_RQ,
    pub(crate) pool_manager: UdpPoolManager,
}

impl RioSocketActor {
    pub(crate) fn new(actor_id: u32, rq: RIO_RQ) -> Self {
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

    fn handle_one(&mut self, res: &RIORESULT) {
        let Some(kind) = RioState::decode_request_context(res.RequestContext) else {
            return;
        };

        match kind {
            RioCompletionKind::Pool {
                actor_id,
                generation,
            } => {
                let Some(&handle) = self.actor_routes.get(&actor_id) else {
                    return;
                };
                let (pool_submissions, remove_actor) = {
                    let Some(actor) = self.actors.get_mut(&handle) else {
                        return;
                    };
                    let Some(slot_idx) = actor.pool_manager.ack_udp_pool_completion(generation)
                    else {
                        return;
                    };
                    let mut ctx = RioContext {
                        registry: self.registry,
                        env: self.env,
                        actor_id: actor.actor_id,
                        rq: actor.rq,
                    };
                    let submissions = actor.pool_manager.handle_completion(
                        (slot_idx, res),
                        &mut self.comp,
                        &mut ctx,
                    );
                    let remove = actor
                        .pool_manager
                        .cleanup_shutdown_udp_pool_if_drained(&mut ctx);
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
            RioCompletionKind::Op {
                user_data,
                generation,
                ctx_ptr,
            } => {
                let _ctx_guard = RioOpCtxGuard(ctx_ptr);
                let ops = &mut self.comp.ops;

                if user_data < ops.local.len() {
                    let op = &mut ops.local[user_data];
                    let slot = &ops.shared.slots[user_data];

                    if op.platform_data.generation == generation {
                        if matches!(op.platform_data.lifecycle, OpLifecycle::InFlight) {
                            op.platform_data.lifecycle = OpLifecycle::Completed;

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
                            // SAFETY: IO completed; safe to take data using IocpSlotExt.
                            let (payload, detail) = unsafe { slot.take_completion_data() };
                            self.comp
                                .table
                                .record_completion_with_data(event, payload, detail);
                            self.comp.events.push(event);
                            unsafe { slot.take_op() };
                            let _ = std::mem::take(&mut op.platform_data);
                            self.comp.ops.shared.push_free(user_data);
                        } else if matches!(op.platform_data.lifecycle, OpLifecycle::Cancelled) {
                            // SAFETY: IO completed after cancellation; safe to cleanup.
                            unsafe {
                                slot.take_op();
                                slot.take_completion_data();
                            }
                            let _ = std::mem::take(&mut op.platform_data);
                            self.comp.ops.shared.push_free(user_data);
                        }
                    }
                }

                if *self.outstanding_count > 0 {
                    *self.outstanding_count -= 1;
                }
                self.completed_count += 1;
            }
        }
    }
}

impl RioState {
    #[inline]
    pub(crate) fn build_ctx<'a>(
        registry: &'a mut RioRegistry,
        env: RioEnv<'a>,
        actor: (u32, RIO_RQ),
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
        Ok(self.actors.get_mut(&handle).expect("actor inserted"))
    }

    pub(crate) fn begin_udp_pool_shutdown_for_handle(&mut self, handle: HANDLE) {
        let env = self
            .kernel
            .env(&veloq_buf::NoopRegistrar, self.registration_mode);
        let mut remove_actor = None;
        if let Some(actor) = self.actors.get_mut(&handle) {
            let mut ctx = Self::build_ctx(&mut self.registry, env, (actor.actor_id, actor.rq));
            actor.pool_manager.begin_udp_pool_shutdown();
            if actor
                .pool_manager
                .cleanup_shutdown_udp_pool_if_drained(&mut ctx)
            {
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
            actor.pool_manager.begin_udp_pool_shutdown();
        }
    }

    pub(crate) fn cancel_udp_recv_waiter(
        &mut self,
        handle: HANDLE,
        uid: (usize, u32),
        registrar: &dyn veloq_buf::BufferRegistrar,
    ) {
        let env = self.kernel.env(registrar, self.registration_mode);
        if let Some(actor) = self.actors.get_mut(&handle) {
            let mut ctx = Self::build_ctx(&mut self.registry, env, (actor.actor_id, actor.rq));
            actor.pool_manager.cancel_udp_recv_waiter(uid, &mut ctx);
        }
    }

    #[cfg(test)]
    pub(crate) fn udp_pool_debug_stats(
        &self,
        handle: HANDLE,
    ) -> Option<crate::rio::runtime::pool::UdpRecvPoolDebugStats> {
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
        let env = self.kernel.env(registrar, self.registration_mode);
        if let Some(actor) = self.actors.get_mut(&handle) {
            let mut ctx = Self::build_ctx(&mut self.registry, env, (actor.actor_id, actor.rq));
            for _ in 0..ticks {
                actor.pool_manager.rebalance_udp_pool(&mut ctx)?;
            }
        }
        Ok(())
    }

    pub(crate) fn forget_all_udp_pool_contexts(&mut self) {
        for actor in self.actors.values_mut() {
            actor.pool_manager.udp_ctx_map.clear();
        }
    }

    pub(crate) fn shutdown_all_actors_with_registry_cleanup(
        &mut self,
        registrar: &dyn veloq_buf::BufferRegistrar,
    ) {
        let env = self.kernel.env(registrar, self.registration_mode);
        for actor in self.actors.values_mut() {
            let mut ctx = Self::build_ctx(&mut self.registry, env, (actor.actor_id, actor.rq));
            actor
                .pool_manager
                .forget_in_flight_and_deregister_rest(&mut ctx);
        }
        self.actors.clear();
        self.actor_routes.clear();
    }

    pub(crate) fn try_mark_pool_completion(
        &mut self,
        actor_id: u32,
        completion_generation: u32,
    ) -> bool {
        let Some(handle) = self.actor_routes.get(&actor_id).copied() else {
            return false;
        };
        let Some(actor) = self.actors.get_mut(&handle) else {
            return false;
        };
        let _ = actor
            .pool_manager
            .ack_udp_pool_completion(completion_generation);
        actor.pool_manager.handle_completion_drain_only();
        let env = self
            .kernel
            .env(&veloq_buf::NoopRegistrar, self.registration_mode);
        let mut ctx = Self::build_ctx(&mut self.registry, env, (actor_id, actor.rq));
        if actor
            .pool_manager
            .cleanup_shutdown_udp_pool_if_drained(&mut ctx)
        {
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
        let mut results: [RIORESULT; MAX_RIO_RESULTS] = unsafe { std::mem::zeroed() };
        let env = self.kernel.env(registrar, self.registration_mode);
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
            let count = self
                .kernel
                .dequeue(results.as_mut_ptr(), MAX_RIO_RESULTS as u32);

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
        Ok(router.completed_count)
    }
}
