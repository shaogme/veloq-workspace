//! Completion-queue routing and publication into driver completion structures.
//!
//! This module consumes `RIORESULT` batches and dispatches them by request kind:
//! - pooled completions are forwarded to actor-local UDP pool handlers,
//! - operation completions are reconciled against op generation/lifecycle state.
//!
//! It is the control-flow bridge between kernel CQ events and user-visible
//! completion queue/table records.

use super::actor::RioSocketActor;
use crate::driver::iocp::error::{IocpErrorContext, io_msg};
use crate::driver::iocp::rio::core::op_ctx::{
    RioCompletionKind, RioOpCtxGuard, rio_result_to_event_res,
};
use crate::driver::iocp::rio::core::registry::RioRegistry;
use crate::driver::iocp::rio::{RioCompletionContext, RioContext, RioEnv, RioState};
use crate::driver::iocp::{IocpOp, IocpOpState, OpLifecycle};
use crate::driver::op_registry::OpRegistry;
use crate::driver::{
    CompletionEvent, SharedCompletionQueue, SharedCompletionTable, encode_completion_token,
};
use rustc_hash::FxHashMap;
use std::io;
use windows_sys::Win32::Foundation::HANDLE;
use windows_sys::Win32::Networking::WinSock::{RIO_CORRUPT_CQ, RIORESULT};

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
                            let payload = unsafe { (*slot.payload.get()).take() };
                            let detail = unsafe { (*slot.result.get()).take() };
                            self.comp
                                .table
                                .record_completion_with_data(event, payload, detail);
                            self.comp.events.push(event);
                            let _ = unsafe { (*slot.op.get()).take() };
                            let _ = std::mem::take(&mut op.platform_data);
                            self.comp.ops.shared.push_free(user_data);
                        } else if matches!(op.platform_data.lifecycle, OpLifecycle::Cancelled) {
                            let _ = unsafe { (*slot.op.get()).take() };
                            let _ = unsafe { (*slot.payload.get()).take() };
                            let _ = unsafe { (*slot.result.get()).take() };
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
    pub(crate) fn process_completions(
        &mut self,
        ops: &mut OpRegistry<IocpOp, IocpOpState>,
        registrar: &dyn veloq_buf::BufferRegistrar,
        completion_events: &SharedCompletionQueue,
        completion_table: &SharedCompletionTable,
    ) -> io::Result<usize> {
        const MAX_RIO_RESULTS: usize = 128;
        let mut results: [RIORESULT; MAX_RIO_RESULTS] = unsafe { std::mem::zeroed() };
        let env = self.kernel.env(registrar);
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
                return Err(io_msg(
                    IocpErrorContext::Rio,
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
