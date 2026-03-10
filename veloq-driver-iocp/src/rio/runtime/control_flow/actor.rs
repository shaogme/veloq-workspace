//! Actor-level control flow for per-socket RIO runtime state.
//!
//! Actors bind a socket handle to an RQ and an optional UDP pool manager. This
//! module manages actor allocation, route lookup tables, and control operations
//! such as pool shutdown/cancellation hooks and debug inspection entry points.
//!
//! It intentionally avoids direct CQ decoding logic, which lives in the
//! completion module of the same control-flow layer.

use crate::IoFd;
use crate::rio::RioContext;
use crate::rio::RioEnv;
use crate::rio::RioState;
use crate::rio::core::registry::RioRegistry;
use crate::rio::runtime::data_plane::pool::UdpPoolManager;
use std::io;
use windows_sys::Win32::Foundation::HANDLE;
use windows_sys::Win32::Networking::WinSock::RIO_RQ;

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
    ) -> Option<crate::rio::runtime::data_plane::pool::UdpRecvPoolDebugStats> {
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
}
