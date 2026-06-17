//! Actor coordination and completion routing for the RIO runtime.

use crate::{
    IoFd,
    config::{BorrowedRawHandle, SocketKey},
    driver::{IocpDriverCompletionDiagnostics, RIO_EVENT_TOKEN, completion::COMP_BACKEND_RIO},
    error::{IocpError, IocpResult},
    op::{IocpOpPayload, IocpOpRegistry, IocpSlotSpec, Slot},
    rio::{
        RioEnv, RioState, SocketInflightToken, SocketLifecycleState, SocketRuntimeState,
        core::{
            RioBufferLeaseToken, RioCompletionKind, RioOpRequestInit, RioRegistry,
            RioRequestContextDecode, RioRq, rio_result_to_event_res,
        },
        error::{RioError, RioResult},
        runtime::release_socket_inflight_token_from,
    },
};
use diagweave::prelude::*;
use rustc_hash::FxHashMap;
use std::mem::{take, zeroed};
use veloq_buf::BufferRegistrar;
use veloq_driver_core::{
    driver::{
        AnomalyAttach, CompletionAnomalyKind, CompletionBackendHooks,
        CompletionBackendIngressAction, CompletionControl, CompletionFlowExt,
        CompletionHookOutcome, CompletionIngress, CompletionSource, RawCompletion,
        SharedCompletionTable, UserCompletionEvent,
    },
    slot::{InFlightOrphaned, InFlightWaiting, SlotState},
};
use windows_sys::Win32::{
    Foundation::ERROR_OPERATION_ABORTED,
    Networking::WinSock::{RIO_CORRUPT_CQ, RIORESULT},
};

pub(crate) struct RioSocketActor {
    pub(crate) rq: RioRq,
}

impl RioSocketActor {
    pub(crate) fn new(rq: RioRq) -> Self {
        Self { rq }
    }
}

#[derive(Clone, Copy)]
struct RioResultData {
    request_context: u64,
    status: i32,
    bytes: u32,
}

impl RioResultData {
    #[inline]
    fn from_result(res: &RIORESULT) -> Self {
        Self {
            request_context: res.RequestContext,
            status: res.Status,
            bytes: res.BytesTransferred,
        }
    }

    #[inline]
    fn raw_res(self) -> i32 {
        if self.status == 0 {
            self.bytes.min(i32::MAX as u32) as i32
        } else if self.status > 0 {
            -self.status
        } else {
            self.status
        }
    }
}

struct RioIngress {
    init: RioOpRequestInit,
    result: RioResultData,
}

#[derive(Default)]
struct RioBackendEffect {
    release: Option<RioReleaseEffect>,
}

#[derive(Clone, Copy)]
struct RioReleaseEffect {
    addr_slot: Option<usize>,
    buffer_lease: Option<RioBufferLeaseToken>,
    socket_inflight: SocketInflightToken,
}

impl RioBackendEffect {
    #[inline]
    fn from_init(init: &RioOpRequestInit) -> Self {
        Self {
            release: Some(RioReleaseEffect {
                addr_slot: init.addr_slot,
                buffer_lease: init.buffer_lease,
                socket_inflight: init.socket_inflight,
            }),
        }
    }
}

struct RioCompletionHooks<'a> {
    outstanding_count: &'a mut usize,
    socket_runtime: &'a mut FxHashMap<SocketKey, SocketRuntimeState>,
    registry: &'a mut RioRegistry,
    env: RioEnv<'a>,
    ext: &'a crate::ext::Extensions,
    completed_count: usize,
}

impl<'a> RioCompletionHooks<'a> {
    fn new(
        outstanding_count: &'a mut usize,
        socket_runtime: &'a mut FxHashMap<SocketKey, SocketRuntimeState>,
        registry: &'a mut RioRegistry,
        env: RioEnv<'a>,
        ext: &'a crate::ext::Extensions,
    ) -> Self {
        Self {
            outstanding_count,
            socket_runtime,
            registry,
            env,
            ext,
            completed_count: 0,
        }
    }
}

impl CompletionBackendHooks<IocpSlotSpec> for RioCompletionHooks<'_> {
    type BackendIngress = RioIngress;
    type BackendEffect = RioBackendEffect;

    fn handle_control(
        &mut self,
        _control: CompletionControl,
    ) -> IocpResult<CompletionHookOutcome<IocpSlotSpec, Self::BackendEffect>> {
        Ok(CompletionHookOutcome::Ignore {
            effect: RioBackendEffect::default(),
        })
    }

    fn complete_waiting(
        &mut self,
        event: UserCompletionEvent,
        slot: Slot<'_, InFlightWaiting>,
        source: CompletionSource<'_, Self::BackendIngress>,
    ) -> IocpResult<CompletionHookOutcome<IocpSlotSpec, Self::BackendEffect>> {
        let CompletionSource::Backend(ingress) = source else {
            let source_name = match source {
                CompletionSource::Kernel => "Kernel",
                CompletionSource::User => "User",
                CompletionSource::Synthetic(_) => "Synthetic",
                CompletionSource::Backend(_) => "Backend",
            };
            return IocpError::InvalidState
                .push_ctx("scope", "rio.runtime.control_flow.complete_waiting")
                .with_ctx("token_index", event.token().index())
                .with_ctx("token_generation", event.token().generation())
                .with_ctx("slot_state", format!("{:?}", SlotState::InFlightWaiting))
                .with_ctx("completion_source", source_name)
                .attach_note(
                    "Backend invariant broken: RIO complete_waiting received non-Backend source",
                );
        };
        complete_rio_waiting_slot(self.registry, self.ext, slot, ingress)
    }

    fn complete_orphaned(
        &mut self,
        _event: UserCompletionEvent,
        slot: Slot<'_, InFlightOrphaned>,
        source: CompletionSource<'_, Self::BackendIngress>,
    ) -> IocpResult<CompletionHookOutcome<IocpSlotSpec, Self::BackendEffect>> {
        let CompletionSource::Backend(ingress) = source else {
            return Ok(CompletionHookOutcome::Ignore {
                effect: RioBackendEffect::default(),
            });
        };
        Ok(complete_rio_orphaned_slot(slot, ingress))
    }

    fn complete_corrupt(
        &mut self,
        event: UserCompletionEvent,
        kind: CompletionAnomalyKind,
        source: CompletionSource<'_, Self::BackendIngress>,
    ) -> IocpResult<CompletionHookOutcome<IocpSlotSpec, Self::BackendEffect>> {
        let effect = match source {
            CompletionSource::Backend(ingress) => RioBackendEffect::from_init(&ingress.init),
            CompletionSource::Kernel | CompletionSource::User | CompletionSource::Synthetic(_) => {
                RioBackendEffect::default()
            }
        };
        Ok(CompletionHookOutcome::Anomaly {
            kind,
            attach: AnomalyAttach::from_raw_completion(event.raw()),
            effect,
        })
    }

    fn complete_backend_ingress(
        &mut self,
        ingress: &Self::BackendIngress,
    ) -> IocpResult<CompletionBackendIngressAction<IocpSlotSpec, Self::BackendEffect>> {
        Ok(CompletionBackendIngressAction::RouteUser(
            UserCompletionEvent::from_parts(
                COMP_BACKEND_RIO,
                ingress.init.token,
                ingress.result.raw_res(),
                0,
            ),
        ))
    }

    fn finish_backend_effect(&mut self, effect: Self::BackendEffect) -> IocpResult<()> {
        let Some(release) = effect.release else {
            return Ok(());
        };
        self.registry.free_addr_slot(release.addr_slot);
        self.registry
            .release_buffer_lease(release.buffer_lease, self.env)
            .trans()?;
        release_socket_inflight_token_from(self.socket_runtime, release.socket_inflight).trans()?;
        if *self.outstanding_count > 0 {
            *self.outstanding_count -= 1;
        }
        self.completed_count += 1;
        Ok(())
    }
}

fn complete_rio_waiting_slot(
    registry: &mut RioRegistry,
    ext: &crate::ext::Extensions,
    mut slot: Slot<'_, InFlightWaiting>,
    ingress: &RioIngress,
) -> IocpResult<CompletionHookOutcome<IocpSlotSpec, RioBackendEffect>> {
    let init = &ingress.init;
    let result = ingress.result;
    let token = init.token;
    let (user_data, generation) = token.parts();
    let socket_key = init.socket_inflight.socket_key();
    let effect = RioBackendEffect::from_init(init);
    if slot.platform().generation != generation {
        let report = IocpError::Internal
            .to_report()
            .push_ctx("scope", "rio.runtime.control_flow.handle_op_completion")
            .with_ctx("user_data", user_data)
            .with_ctx("generation", generation)
            .with_ctx("platform_generation", slot.platform().generation)
            .with_ctx("rio_op_kind", init.op_kind.as_str())
            .with_ctx("rio_request_id", init.request_id)
            .attach_note("RIO slot platform generation mismatch");
        return Err(report);
    }

    let cancelled = slot.platform().rio_cancel_requested;
    let mut completion = if cancelled {
        IocpError::CompletionWait
            .push_ctx("scope", "rio.runtime.control_flow.handle_op_completion")
            .with_ctx("socket_raw", socket_key.as_handle() as usize)
            .with_ctx("rio_op_kind", init.op_kind.as_str())
            .with_ctx("rio_request_id", init.request_id)
            .set_error_code(ERROR_OPERATION_ABORTED as i32)
            .attach_note("RIO operation was cancelled before kernel completion")
    } else if result.status == 0 {
        Ok(result.bytes as usize)
    } else {
        IocpError::CompletionWait
            .push_ctx("scope", "rio.runtime.control_flow.handle_op_completion")
            .with_ctx("socket_raw", socket_key.as_handle() as usize)
            .with_ctx("rio_op_kind", init.op_kind.as_str())
            .with_ctx("rio_request_id", init.request_id)
            .with_ctx("rq_raw", init.diagnostics.rq_raw)
            .with_ctx("data_buffer_id", init.diagnostics.data_buffer_id)
            .with_ctx("data_buffer_offset", init.diagnostics.data_buffer_offset)
            .with_ctx("data_buffer_length", init.diagnostics.data_buffer_length)
            .with_ctx("addr_slot", init.addr_slot.unwrap_or(usize::MAX))
            .set_error_code(result.status)
            .attach_note("rio completion returned os error")
    };

    let _ = slot.with_op_mut(|iocp_op| {
        if let Some(addr_slot) = init.addr_slot
            && let IocpOpPayload::UdpRecvFrom(payload) = &mut iocp_op.payload
            && !cancelled
            && completion.is_ok()
            && let Err(e) = registry
                .copy_addr_slot_to(addr_slot, &mut payload.addr)
                .trans()
        {
            completion = Err(e
                .with_ctx("socket_raw", socket_key.as_handle() as usize)
                .with_ctx("rio_op_kind", init.op_kind.as_str())
                .with_ctx("rio_request_id", init.request_id)
                .with_ctx("addr_slot", addr_slot)
                .attach_note("failed to copy RIO recv_from address"));
        }
        if iocp_op.header.in_flight {
            iocp_op.header.in_flight = false;
        }
        if !cancelled && let Ok(bytes) = completion.as_ref().copied() {
            completion = iocp_op
                .on_complete(bytes, ext)
                .with_ctx("scope", "rio.runtime.control_flow.handle_op_completion")
                .attach_note("rio op completion hook failed");
        }
    });

    let res_code = rio_result_to_event_res(&completion);
    let mut guard = slot.complete();
    let cleanup = guard
        .with_op_mut(|op| op.completion_cleanup(&completion))
        .unwrap_or_default();
    let _ = guard.take_op();
    let (payload, detail) = guard.take_completion_data();
    let event = UserCompletionEvent::from_parts(COMP_BACKEND_RIO, token, res_code, 0);
    if let Some(payload) = payload {
        Ok(CompletionHookOutcome::User {
            event,
            payload,
            detail: detail.or(Some(completion)),
            cleanup,
            effect,
        })
    } else {
        drop(detail);
        IocpError::InvalidState
            .push_ctx("scope", "rio.runtime.control_flow.handle_op_completion")
            .with_ctx("token_index", token.index())
            .with_ctx("token_generation", token.generation())
            .with_ctx("rio_op_kind", init.op_kind.as_str())
            .with_ctx("rio_request_id", init.request_id)
            .attach_note("Backend invariant broken: RIO slot completion payload is missing")
    }
}

fn complete_rio_orphaned_slot(
    mut slot: Slot<'_, InFlightOrphaned>,
    ingress: &RioIngress,
) -> CompletionHookOutcome<IocpSlotSpec, RioBackendEffect> {
    let init = &ingress.init;
    let result = ingress.result;
    let generation = init.token.generation();
    let socket_key = init.socket_inflight.socket_key();
    let effect = RioBackendEffect::from_init(init);
    let generation_mismatch = slot.platform_mut().generation != generation;

    let mut guard = slot.complete();
    let orphan_result = if generation_mismatch {
        IocpError::CompletionWait
            .push_ctx("scope", "rio.runtime.control_flow.orphan_cleanup")
            .with_ctx("socket_raw", socket_key.as_handle() as usize)
            .with_ctx("rio_op_kind", init.op_kind.as_str())
            .with_ctx("rio_request_id", init.request_id)
            .attach_note("orphaned RIO completion had platform generation mismatch")
    } else if result.status == 0 {
        Ok(result.bytes as usize)
    } else {
        IocpError::CompletionWait
            .push_ctx("scope", "rio.runtime.control_flow.orphan_cleanup")
            .with_ctx("socket_raw", socket_key.as_handle() as usize)
            .with_ctx("rio_op_kind", init.op_kind.as_str())
            .with_ctx("rio_request_id", init.request_id)
            .set_error_code(result.status)
            .attach_note("orphaned RIO completion returned os error")
    };
    let cleanup = guard
        .with_op_mut(|op| op.orphan_cleanup(&orphan_result))
        .unwrap_or_default();
    let _ = guard.take_op();
    let _ = guard.take_completion_data();
    let _ = take(guard.platform_mut());
    CompletionHookOutcome::Cleanup { cleanup, effect }
}

pub(crate) const RIO_ANOMALY_MALFORMED: u16 = 1;
pub(crate) const RIO_ANOMALY_MISSING: u16 = 2;
pub(crate) const RIO_ANOMALY_STALE: u16 = 3;

pub(crate) fn rio_malformed_context_kind(raw_context: u64) -> CompletionAnomalyKind {
    CompletionAnomalyKind::backend_specific(RIO_ANOMALY_MALFORMED, COMP_BACKEND_RIO, raw_context)
}

pub(crate) fn rio_missing_context_kind(
    raw_context: u64,
    index: usize,
    generation: u32,
) -> CompletionAnomalyKind {
    CompletionAnomalyKind::backend_specific_missing(
        RIO_ANOMALY_MISSING,
        COMP_BACKEND_RIO,
        raw_context,
        index,
        generation,
    )
}

pub(crate) fn rio_stale_context_kind(
    raw_context: u64,
    index: usize,
    expected_generation: u32,
    actual_generation: u32,
) -> CompletionAnomalyKind {
    CompletionAnomalyKind::backend_specific_stale(
        RIO_ANOMALY_STALE,
        COMP_BACKEND_RIO,
        raw_context,
        index,
        expected_generation,
        actual_generation,
    )
}

fn rio_result_attach(res: RioResultData) -> AnomalyAttach {
    AnomalyAttach::from_raw_completion(RawCompletion::new(
        COMP_BACKEND_RIO,
        RIO_EVENT_TOKEN,
        res.raw_res(),
        0,
    ))
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
        registrar: &dyn BufferRegistrar,
        completion_table: &SharedCompletionTable<IocpSlotSpec>,
        diagnostics: &mut IocpDriverCompletionDiagnostics,
    ) -> IocpResult<usize> {
        const MAX_RIO_RESULTS: usize = 128;
        let mut results: [RIORESULT; MAX_RIO_RESULTS] = unsafe { zeroed() };
        let Some(env) = self.kernel.env(registrar, self.registration_mode) else {
            return Ok(0);
        };
        let mut hooks = RioCompletionHooks::new(
            &mut self.outstanding_count,
            &mut self.socket_runtime,
            &mut self.registry,
            env,
            ext,
        );

        loop {
            let count = self.kernel.dequeue(&mut results);
            if count == RIO_CORRUPT_CQ {
                return RioError::Internal
                    .attach_note("RIO completion queue is corrupt (RIO_CORRUPT_CQ)")
                    .trans();
            }
            if count == 0 {
                break;
            }

            for res in results.iter().take(count as usize) {
                let result = RioResultData::from_result(res);
                match hooks
                    .registry
                    .decode_request_context_checked(result.request_context)
                {
                    RioRequestContextDecode::Valid(RioCompletionKind::Op {
                        init,
                        context: _completed_context,
                    }) => {
                        let _ = ops.accept_completion(
                            completion_table,
                            diagnostics,
                            &mut hooks,
                            CompletionIngress::Backend(RioIngress { init, result }),
                        )?;
                    }
                    RioRequestContextDecode::Malformed { raw } => {
                        let _ = ops.accept_completion(
                            completion_table,
                            diagnostics,
                            &mut hooks,
                            CompletionIngress::Anomaly {
                                kind: rio_malformed_context_kind(raw),
                                attach: rio_result_attach(result),
                            },
                        )?;
                    }
                    RioRequestContextDecode::Missing { id } => {
                        let _ = ops.accept_completion(
                            completion_table,
                            diagnostics,
                            &mut hooks,
                            CompletionIngress::Anomaly {
                                kind: rio_missing_context_kind(
                                    result.request_context,
                                    id.index(),
                                    id.generation(),
                                ),
                                attach: rio_result_attach(result),
                            },
                        )?;
                    }
                    RioRequestContextDecode::Stale {
                        id,
                        actual_generation,
                    } => {
                        let _ = ops.accept_completion(
                            completion_table,
                            diagnostics,
                            &mut hooks,
                            CompletionIngress::Anomaly {
                                kind: rio_stale_context_kind(
                                    result.request_context,
                                    id.index(),
                                    id.generation(),
                                    actual_generation,
                                ),
                                attach: rio_result_attach(result),
                            },
                        )?;
                    }
                }
            }

            if count < MAX_RIO_RESULTS as u32 {
                break;
            }
        }

        self.kernel.rearm_notify().trans()?;

        if *hooks.outstanding_count == 0 {
            hooks.registry.flush_deregs(hooks.env);
        }
        Ok(hooks.completed_count)
    }
}
