//! RIO backend orchestration for the IOCP driver.
//!
//! This module intentionally keeps only the cross-cutting state and type glue
//! (`RioState`, `RioEnv`, and shared context structs). Concrete behavior is
//! organized into layered submodules to keep high-level ownership boundaries
//! explicit:
//! - `core`: low-level primitives and kernel-facing helpers.
//! - `runtime`: steady-state operation split into datapath and control-flow.
//! - `lifecycle`: shutdown sequencing and deferred cleanup semantics.

mod core {
    /// Core context encoding, registry ownership, and kernel dispatch wrappers.
    pub(crate) mod op_ctx;
    /// Core registration table for chunks, slab pages, and heap buffers.
    pub(crate) mod registry;
    /// Core RIO kernel/CQ/RQ creation and submission helpers.
    pub(crate) mod submit_ops;
}

mod runtime {
    /// Runtime datapath: hot path buffer/pool state and UDP submissions.
    pub(crate) mod data_plane {
        pub(crate) mod pool;
        pub(crate) mod submit_udp;
    }

    /// Runtime control-flow: actor coordination and completion routing.
    pub(crate) mod control_flow {
        pub(crate) mod actor;
        pub(crate) mod completion;
    }
}

mod lifecycle {
    /// Drop-time shutdown, background reaper handoff, and strict drain logic.
    pub(crate) mod shutdown;
}

use crate::driver::iocp::IocpOp;
use crate::driver::iocp::IocpOpState;
use crate::driver::op_registry::OpRegistry;
use crate::driver::{SharedCompletionQueue, SharedCompletionTable};
use rustc_hash::FxHashMap;
use windows_sys::Win32::Foundation::HANDLE;
use windows_sys::Win32::Networking::WinSock::{RIO_CQ, RIO_RQ};

use self::core::registry::RioRegistry;
use self::core::submit_ops::{RioDispatch, RioKernel};
use self::runtime::control_flow::actor::RioSocketActor;

pub(crate) use self::runtime::data_plane::submit_udp::{RioSendToArgs, RioUdpStreamArgs};

#[derive(Clone, Copy)]
pub(crate) struct RioEnv<'a> {
    pub(crate) registrar: &'a dyn veloq_buf::BufferRegistrar,
    pub(crate) dispatch: &'a RioDispatch,
    pub(crate) cq: RIO_CQ,
}

pub(crate) struct RioContext<'a> {
    pub(crate) registry: &'a mut RioRegistry,
    pub(crate) env: RioEnv<'a>,
    pub(crate) actor_id: u32,
    pub(crate) rq: RIO_RQ,
}

pub(crate) struct RioCompletionContext<'a> {
    pub(crate) ops: &'a mut OpRegistry<IocpOp, IocpOpState>,
    pub(crate) events: &'a SharedCompletionQueue,
    pub(crate) table: &'a SharedCompletionTable,
}

pub(crate) struct RioState {
    pub(crate) kernel: RioKernel,
    pub(crate) registry: RioRegistry,
    actors: FxHashMap<HANDLE, RioSocketActor>,
    actor_routes: FxHashMap<u32, HANDLE>,
    next_actor_id: u32,
    pub(crate) outstanding_count: usize,
}
