use super::single::RioSingleCompletion;
use crate::{
    driver::COMP_BACKEND_RIO,
    error::{IocpError, IocpResult, iocp_report_to_event_res},
    ext::Extensions,
    op::{IocpSlotSpec, Slot},
    rio::{
        RioRuntime, SocketInflightIdentity,
        core::{
            RioAddrReservation, RioBufferLeaseToken, RioCore, RioOpKind, RioOpRequestInit,
            RioReceiveReplacementArgs, RioSubmitOperations, RioSubmittedRequest, RioTarget,
        },
        error::RioResult,
    },
};
use diagweave::prelude::*;
use veloq_buf::BufferRegistrar;
use veloq_driver_core::platform::receive_pump::{ReceivePumpState, ReceiveReplacementReservation};
use veloq_driver_core::{
    driver::{
        AnomalyAttach, CompletionAnomalyKind, CompletionBackendHooks,
        CompletionBackendIngressAction, CompletionCleanupGuard, CompletionControl,
        CompletionFailure, CompletionSettlement, CompletionSource, UserCompletionEvent,
    },
    slot::{InFlightOrphaned, InFlightWaiting, SlotState, SlotStatus},
};
use veloq_std::format;
use windows_sys::Win32::Networking::WinSock::RIORESULT;

#[derive(Clone, Copy)]
pub(super) struct RioResultData {
    request_context: u64,
    status: i32,
    bytes: u32,
}

pub(super) struct RioCompletionResult;

impl RioCompletionResult {
    pub(super) fn event_code(result: &IocpResult<usize>) -> i32 {
        match result {
            Ok(bytes) => (*bytes).min(i32::MAX as usize) as i32,
            Err(error) => iocp_report_to_event_res(error),
        }
    }
}

/// Immutable observation of one real RIO completion. / 一条真实 RIO 完成记录的不可变快照。
///
/// A user cancel may change the logical result without erasing kernel status or request identity.
/// 用户取消可以改变逻辑结果，但不能抹去内核状态或请求身份等诊断信息。
#[derive(Clone, Copy)]
pub(super) struct RioCompletionObservation {
    request_id: u64,
    request_context: u64,
    op_kind: RioOpKind,
    status: i32,
    bytes: u32,
    receive_slot_id: Option<u32>,
    receive_slot_generation: Option<u32>,
}

impl RioCompletionObservation {
    #[inline]
    pub(super) fn new(init: &RioOpRequestInit, result: RioResultData) -> Self {
        Self {
            request_id: init.request_id,
            request_context: result.request_context,
            op_kind: init.op_kind,
            status: result.status,
            bytes: result.bytes,
            receive_slot_id: init.receive_slot_id,
            receive_slot_generation: init.receive_slot_generation,
        }
    }

    pub(super) const fn status(self) -> i32 {
        self.status
    }

    pub(super) const fn bytes(self) -> u32 {
        self.bytes
    }

    #[inline]
    pub(super) fn attach_to_report(
        self,
        report: Report<IocpError>,
        user_cancelled: bool,
    ) -> Report<IocpError> {
        report
            .with_ctx("rio_user_cancelled", user_cancelled)
            .with_ctx("rio_completion_status", self.status)
            .with_ctx("rio_completion_bytes", self.bytes)
            .with_ctx("rio_request_id", self.request_id)
            .with_ctx("rio_request_context", self.request_context)
            .with_ctx("rio_op_kind", self.op_kind.as_str())
            .with_ctx("receive_slot_id", self.receive_slot_id.unwrap_or(u32::MAX))
            .with_ctx(
                "receive_slot_generation",
                self.receive_slot_generation.unwrap_or(u32::MAX),
            )
    }
}

impl RioResultData {
    pub(super) fn new(request_context: u64, status: i32, bytes: u32) -> Self {
        Self {
            request_context,
            status,
            bytes,
        }
    }

    pub(super) const fn request_context(self) -> u64 {
        self.request_context
    }

    pub(super) const fn status(self) -> i32 {
        self.status
    }

    pub(super) const fn bytes(self) -> u32 {
        self.bytes
    }

    #[inline]
    pub(super) fn from_result(res: &RIORESULT) -> Self {
        Self::new(res.RequestContext, res.Status, res.BytesTransferred)
    }

    #[inline]
    pub(super) fn raw_res(self) -> i32 {
        if self.status == 0 {
            self.bytes.min(i32::MAX as u32) as i32
        } else if self.status > 0 {
            -self.status
        } else {
            self.status
        }
    }
}

pub(super) struct RioIngress {
    pub(super) init: RioOpRequestInit,
    pub(super) result: RioResultData,
}

#[derive(Default)]
pub(super) struct RioBackendEffect {
    release: Option<RioReleaseEffect>,
}

#[derive(Clone, Copy)]
struct RioReleaseEffect {
    addr: Option<RioAddrReservation>,
    buffer_lease: Option<RioBufferLeaseToken>,
    socket_inflight: SocketInflightIdentity,
    op_kind: RioOpKind,
}

impl RioBackendEffect {
    #[inline]
    pub(super) fn from_init(init: &RioOpRequestInit) -> Self {
        Self {
            release: Some(RioReleaseEffect {
                addr: init.addr,
                buffer_lease: init.buffer_lease,
                socket_inflight: init.socket_inflight.identity(),
                op_kind: init.op_kind,
            }),
        }
    }
}

pub(super) struct RioCompletionHooks<'a> {
    core: &'a mut RioCore,
    runtime: &'a mut RioRuntime,
    registrar: &'a dyn BufferRegistrar,
    ext: &'a Extensions,
    completed_count: usize,
}

impl<'a> RioCompletionHooks<'a> {
    pub(super) fn new(
        core: &'a mut RioCore,
        runtime: &'a mut RioRuntime,
        registrar: &'a dyn BufferRegistrar,
        ext: &'a Extensions,
    ) -> Self {
        Self {
            core,
            runtime,
            registrar,
            ext,
            completed_count: 0,
        }
    }

    pub(super) fn core_mut(&mut self) -> &mut RioCore {
        self.core
    }

    pub(super) fn runtime_mut(&mut self) -> &mut RioRuntime {
        self.runtime
    }

    pub(super) fn runtime(&self) -> &RioRuntime {
        self.runtime
    }

    pub(super) fn ext(&self) -> &Extensions {
        self.ext
    }

    pub(super) fn completed_count(&self) -> usize {
        self.completed_count
    }
}

impl CompletionBackendHooks<IocpSlotSpec> for RioCompletionHooks<'_> {
    type BackendIngress = RioIngress;
    type BackendEffect = RioBackendEffect;

    fn handle_control(
        &mut self,
        _control: CompletionControl,
    ) -> CompletionSettlement<IocpSlotSpec, Self::BackendEffect> {
        CompletionSettlement::Ignore {
            effect: RioBackendEffect::default(),
        }
    }

    fn complete_waiting(
        &mut self,
        event: UserCompletionEvent,
        slot: Slot<'_, InFlightWaiting>,
        source: CompletionSource<'_, Self::BackendIngress>,
    ) -> CompletionSettlement<IocpSlotSpec, Self::BackendEffect> {
        let CompletionSource::Backend(ingress) = source else {
            let source_name = match source {
                CompletionSource::Kernel => "Kernel",
                CompletionSource::User => "User",
                CompletionSource::Synthetic(_) => "Synthetic",
                CompletionSource::Backend(_) => "Backend",
            };
            return CompletionSettlement::TerminalFailure {
                failure: CompletionFailure::terminal(
                    IocpError::InvalidState
                        .to_report()
                        .push_ctx("scope", "rio.runtime.control_flow.complete_waiting")
                        .with_ctx("token_index", event.token().index())
                        .with_ctx("token_generation", event.token().generation())
                        .with_ctx(
                            "slot_status",
                            format!("{:?}", SlotStatus::of(SlotState::InFlightWaiting)),
                        )
                        .with_ctx("completion_source", source_name)
                        .attach_note(
                            "Backend invariant broken: RIO complete_waiting received non-Backend source",
                        ),
                    CompletionCleanupGuard::default(),
                    RioBackendEffect::default(),
                ),
            };
        };
        RioSingleCompletion::complete_waiting_slot(self, slot, ingress)
    }

    fn complete_orphaned(
        &mut self,
        _event: UserCompletionEvent,
        slot: Slot<'_, InFlightOrphaned>,
        source: CompletionSource<'_, Self::BackendIngress>,
    ) -> CompletionSettlement<IocpSlotSpec, Self::BackendEffect> {
        let CompletionSource::Backend(ingress) = source else {
            return CompletionSettlement::Ignore {
                effect: RioBackendEffect::default(),
            };
        };
        RioSingleCompletion::complete_orphaned_slot(self, slot, ingress)
    }

    fn complete_corrupt(
        &mut self,
        event: UserCompletionEvent,
        kind: CompletionAnomalyKind,
        source: CompletionSource<'_, Self::BackendIngress>,
    ) -> CompletionSettlement<IocpSlotSpec, Self::BackendEffect> {
        let effect = match source {
            CompletionSource::Backend(ingress) => RioBackendEffect::from_init(&ingress.init),
            CompletionSource::Kernel | CompletionSource::User | CompletionSource::Synthetic(_) => {
                RioBackendEffect::default()
            }
        };
        CompletionSettlement::Anomaly {
            kind,
            attach: AnomalyAttach::from_raw_completion(event.raw()),
            cleanup: CompletionCleanupGuard::default(),
            effect,
        }
    }

    fn complete_backend_ingress(
        &mut self,
        ingress: &Self::BackendIngress,
    ) -> CompletionBackendIngressAction<IocpSlotSpec, Self::BackendEffect> {
        CompletionBackendIngressAction::RouteUser(UserCompletionEvent::from_parts(
            COMP_BACKEND_RIO,
            ingress.init.token,
            ingress.result.raw_res(),
            0,
        ))
    }

    fn finish_backend_effect(&mut self, effect: Self::BackendEffect) -> IocpResult<()> {
        self.release_backend_effect(effect)
    }
}

impl RioCompletionHooks<'_> {
    pub(super) fn release_backend_effect(&mut self, effect: RioBackendEffect) -> IocpResult<()> {
        let Some(release) = effect.release else {
            return Ok(());
        };
        let (registry, env) = self.core.registry_with_env(self.registrar).into_parts();
        let env = env.ok_or_else(|| {
            IocpError::Unsupported
                .to_report()
                .attach_note("RIO completion release requires an active dispatch table")
        })?;
        registry.free_addr_reservation(release.addr).trans()?;
        registry
            .release_buffer_lease(release.buffer_lease, env)
            .trans()?;
        self.runtime
            .release_request_inflight_identity(release.op_kind, release.socket_inflight)
            .trans()?;
        if self.core.outstanding_count() > 0 {
            self.core.decrement_outstanding();
        }
        self.completed_count += 1;
        Ok(())
    }

    pub(super) fn try_submit_recv_multi_replacement(
        &mut self,
        target: RioTarget<'_>,
        op_kind: RioOpKind,
        pump: &mut ReceivePumpState,
        reservation: ReceiveReplacementReservation,
    ) -> RioResult<RioSubmittedRequest> {
        RioSubmitOperations::new(self.core, self.runtime).try_submit_recv_multi_replacement(
            RioReceiveReplacementArgs {
                target,
                op_kind,
                pump,
                reservation,
                registrar: self.registrar,
            },
        )
    }
}
