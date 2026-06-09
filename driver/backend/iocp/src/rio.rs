//! RIO backend orchestration for the IOCP driver.
//!
//! This module intentionally keeps only the cross-cutting state and type glue
//! (`RioState`, `RioEnv`, and shared context structs). Concrete behavior is
//! organized into layered submodules to keep high-level ownership boundaries
//! explicit:
//! - `core`: low-level primitives and kernel-facing helpers.
//! - `runtime`: steady-state operation split into datapath and control-flow.
//! - `lifecycle`: shutdown sequencing and deferred cleanup semantics.

pub(crate) mod core;
pub(crate) mod error;
pub(crate) mod lifecycle;
pub(crate) mod runtime;

use crate::BufferRegistrationMode;
use crate::config::SocketKey;
use crate::error::IocpError;
use crate::op::IocpOpRegistry;
use rustc_hash::FxHashMap;
use slotmap::{SlotMap, new_key_type};
use veloq_driver_core::driver::SharedCompletionTable;

use crate::driver::IocpDriverCompletionDiagnostics;
use self::core::registry::RioRegistry;
use self::core::submit_ops::{RioCq, RioDispatch, RioKernel};
use self::runtime::control_flow::RioSocketActor;

pub(crate) use self::runtime::RioSendToArgs;
pub(crate) use self::runtime::RioTarget;
pub(crate) use self::runtime::RioUdpRecvFromArgs;

new_key_type! {
    pub(crate) struct ActorKey;
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SocketLifecycleState {
    Open,
    Closing,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct SocketRuntimeState {
    pub(crate) lifecycle: SocketLifecycleState,
    pub(crate) inflight: u32,
}

impl Default for SocketRuntimeState {
    fn default() -> Self {
        Self {
            lifecycle: SocketLifecycleState::Open,
            inflight: 0,
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct SocketInflightToken {
    socket_key: SocketKey,
}

impl SocketInflightToken {
    #[inline]
    pub(crate) const fn new(socket_key: SocketKey) -> Self {
        Self { socket_key }
    }

    #[inline]
    pub(crate) const fn socket_key(&self) -> SocketKey {
        self.socket_key
    }
}

#[must_use = "dropping a SocketInflightGuard releases the acquired socket inflight slot"]
pub(crate) struct SocketInflightGuard<'a> {
    pub(crate) state: &'a mut RioState,
    pub(crate) token: Option<SocketInflightToken>,
}

#[derive(Clone, Copy)]
pub(crate) struct RioEnv<'a> {
    pub(crate) registrar: &'a dyn veloq_buf::BufferRegistrar,
    pub(crate) dispatch: &'a RioDispatch,
    pub(crate) cq: RioCq,
    pub(crate) registration_mode: BufferRegistrationMode,
}

pub(crate) struct RioCompletionContext<'a> {
    pub(crate) ops: &'a mut IocpOpRegistry,
    pub(crate) ext: &'a crate::ext::Extensions,
    pub(crate) table: &'a SharedCompletionTable<crate::op::IocpUserPayload, IocpError>,
    pub(crate) diagnostics: &'a mut IocpDriverCompletionDiagnostics,
}

pub(crate) struct RioState {
    pub(crate) kernel: RioKernel,
    pub(crate) registry: RioRegistry,
    pub(crate) registration_mode: BufferRegistrationMode,
    pub(crate) submissions_closed: bool,
    pub(crate) actors: SlotMap<ActorKey, RioSocketActor>,
    pub(crate) actor_by_handle: FxHashMap<SocketKey, ActorKey>,
    pub(crate) socket_runtime: FxHashMap<SocketKey, SocketRuntimeState>,
    pub(crate) outstanding_count: usize,
    pub(crate) next_request_id: u64,
    pub(crate) deferred_payloads: Vec<crate::op::IocpUserPayload>,
    pub(crate) diagnostics: IocpDriverCompletionDiagnostics,
}
