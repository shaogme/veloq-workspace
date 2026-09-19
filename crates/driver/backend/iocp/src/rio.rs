//! RIO backend orchestration for the IOCP driver.
//!
//! This module intentionally keeps only the cross-cutting state and type glue
//! (`RioState`, `RioEnv`, and shared context structs). Concrete behavior is
//! organized into layered submodules to keep high-level ownership boundaries
//! explicit:
//! - `core`: low-level primitives and kernel-facing helpers.
//! - `runtime`: steady-state operation split into datapath and control-flow.
//! - `lifecycle`: shutdown sequencing and deferred cleanup semantics.

mod core;
mod error;
mod lifecycle;
pub(crate) mod runtime;

use crate::{
    BufferRegistrationMode,
    config::SocketKey,
    driver::IocpDriverCompletionDiagnostics,
    op::{IocpKernelOp, IocpUserPayload},
};
use slotmap::{SlotMap, new_key_type};
use veloq_driver_core::driver::OpToken;
use veloq_std::{
    collections::{FastHashMap, FastHashSet},
    vec::Vec,
};

use self::{
    core::{RioCq, RioDispatch, RioKernel, RioRegistry},
    runtime::RioSocketActor,
};

pub(crate) use self::{
    core::RioOpKind,
    error::RioError,
    runtime::{RioSendToArgs, RioTarget},
};

new_key_type! {
    pub(crate) struct ActorKey;
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SocketLifecycleState {
    Open,
    Closing,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct SocketRuntimeState {
    pub(crate) lifecycle: SocketLifecycleState,
    pub(crate) inflight: u32,
    pub(crate) active_tokens: FastHashSet<u64>,
    pub(crate) send_inflight: u32,
    pub(crate) receive_inflight: u32,
    pub(crate) receive_pump_inflight: u32,
    pub(crate) receive_pump_token: Option<OpToken>,
    pub(crate) socket_receive_shutdown_requested: bool,
    pub(crate) socket_receive_shutdown_pending: bool,
}

impl Default for SocketRuntimeState {
    fn default() -> Self {
        Self {
            lifecycle: SocketLifecycleState::Open,
            inflight: 0,
            active_tokens: FastHashSet::default(),
            send_inflight: 0,
            receive_inflight: 0,
            receive_pump_inflight: 0,
            receive_pump_token: None,
            socket_receive_shutdown_requested: false,
            socket_receive_shutdown_pending: false,
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct SocketInflightToken {
    socket_key: SocketKey,
    request_id: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct SocketInflightIdentity {
    socket_key: SocketKey,
    request_id: u64,
}

impl SocketInflightToken {
    #[inline]
    pub(crate) const fn new(socket_key: SocketKey, request_id: u64) -> Self {
        Self {
            socket_key,
            request_id,
        }
    }

    #[inline]
    pub(crate) const fn socket_key(&self) -> SocketKey {
        self.socket_key
    }

    #[inline]
    pub(crate) const fn request_id(&self) -> u64 {
        self.request_id
    }

    #[inline]
    pub(crate) const fn identity(&self) -> SocketInflightIdentity {
        SocketInflightIdentity {
            socket_key: self.socket_key,
            request_id: self.request_id,
        }
    }
}

impl SocketInflightIdentity {
    #[inline]
    pub(crate) const fn socket_key(self) -> SocketKey {
        self.socket_key
    }

    #[inline]
    pub(crate) const fn request_id(self) -> u64 {
        self.request_id
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

pub(crate) struct RioState {
    pub(crate) kernel: RioKernel,
    pub(crate) registry: RioRegistry,
    pub(crate) registration_mode: BufferRegistrationMode,
    pub(crate) submissions_closed: bool,
    pub(crate) actors: SlotMap<ActorKey, RioSocketActor>,
    pub(crate) actor_by_handle: FastHashMap<SocketKey, ActorKey>,
    pub(crate) socket_runtime: FastHashMap<SocketKey, SocketRuntimeState>,
    pub(crate) rio_outstanding_count: usize,
    pub(crate) next_request_id: u64,
    /// Kernel operations retained by fast close until the RIO reaper observes every completion.
    ///
    /// These operations may own backend buffers or receive-pump storage that the kernel still
    /// references after the driver has been dropped. They must outlive the RIO request, just like
    /// the deferred user payloads below.
    pub(crate) deferred_kernel_ops: Vec<IocpKernelOp>,
    pub(crate) deferred_payloads: Vec<IocpUserPayload>,
    pub(crate) diagnostics: IocpDriverCompletionDiagnostics,
    pub(crate) cq_armed: bool,
}
