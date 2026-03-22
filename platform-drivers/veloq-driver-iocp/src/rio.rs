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
use crate::IocpOpState;
use crate::ops::IocpOp;
use rustc_hash::{FxHashMap, FxHashSet};
use slotmap::{SlotMap, new_key_type};
use veloq_driver_core::driver::{SharedCompletionQueue, SharedCompletionTable};
use veloq_driver_core::op_registry::OpRegistry;
use windows_sys::Win32::Foundation::HANDLE;

use self::core::registry::RioRegistry;
use self::core::submit_ops::{RioCq, RioDispatch, RioKernel, RioRq};
use self::runtime::control_flow::RioSocketActor;

pub(crate) use self::runtime::RioSendToArgs;
pub(crate) use self::runtime::RioTarget;
pub(crate) use self::runtime::RioUdpRecvArgs;
pub(crate) use self::runtime::RioUdpStreamArgs;

new_key_type! {
    pub(crate) struct ActorKey;
}

#[derive(Clone, Copy)]
pub(crate) struct RioEnv<'a> {
    pub(crate) registrar: &'a dyn veloq_buf::BufferRegistrar,
    pub(crate) dispatch: &'a RioDispatch,
    pub(crate) cq: RioCq,
    pub(crate) registration_mode: BufferRegistrationMode,
}

pub(crate) struct RioContext<'a> {
    pub(crate) registry: &'a mut RioRegistry,
    pub(crate) env: RioEnv<'a>,
    pub(crate) actor_key: ActorKey,
    pub(crate) rq: RioRq,
}

pub(crate) struct RioCompletionContext<'a> {
    pub(crate) ops: &'a mut OpRegistry<IocpOp, IocpOpState, crate::ops::OverlappedEntry>,
    pub(crate) events: &'a SharedCompletionQueue,
    pub(crate) table: &'a SharedCompletionTable,
}

pub(crate) struct RioState {
    pub(crate) kernel: RioKernel,
    pub(crate) registry: RioRegistry,
    pub(crate) registration_mode: BufferRegistrationMode,
    pub(crate) actors: SlotMap<ActorKey, RioSocketActor>,
    pub(crate) actor_by_handle: FxHashMap<HANDLE, ActorKey>,
    pub(crate) udp_iocp_fallback_handles: FxHashSet<HANDLE>,
    pub(crate) outstanding_count: usize,
}
