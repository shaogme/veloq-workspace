//! Core context encoding, registry ownership, and kernel dispatch wrappers. / 核心上下文编码、注册表所有权和内核分发表封装。

mod registry;
mod request;
mod submit_ops;
mod submit_txn;

use diagweave::prelude::*;

pub(crate) use registry::{RioAddrReservation, RioBufferLeaseToken, RioRegistry};
pub(crate) use request::{
    RioAddressPolicy, RioCompletedRequestContext, RioCompletionKind, RioOpKind, RioOpRequestInit,
    RioPreparedRequest, RioPreparedRequestContext, RioRequestContextDecode, RioRequestContextId,
    RioSubmitPlan,
};

pub(crate) use submit_ops::{
    RioBufferId, RioCq, RioDispatch, RioKernel, RioProvider, RioReceiveReplacementArgs, RioRq,
    RioSendToArgs, RioSubmitOperations, RioTarget,
};
pub(crate) use submit_txn::{RioSubmitTxn, RioSubmittedRequest};

#[cfg(test)]
pub(crate) use request::RioRequestDiagnostics;

#[derive(Clone, Copy)]
pub(crate) struct RioEnv<'a> {
    pub(crate) dispatch: &'a RioDispatch,
    pub(crate) cq: RioCq,
}

set! {
    #[derive(Debug, Copy, Clone, PartialEq, Eq)]
    pub RioError = {
        #[display("RIO library or function pointers load failure")]
        LibraryLoad,
        #[display("failed to register memory buffer for RIO")]
        BufferRegistration,
        #[display("RIO requires a registered buffer")]
        BufferRegistrationRequired,
        #[display("failed to create RIO completion queue")]
        CqCreation,
        #[display("failed to create RIO request queue")]
        RqCreation,
        #[display("RIO datapath operation error")]
        Datapath,
        #[display("RIO invalid input")]
        InvalidInput,
        #[display("RIO resource limit reached")]
        ResourceExhaustion,
        #[display("RIO not supported or initialized")]
        NotSupported,
        #[display("RIO internal inconsistency")]
        Internal,
    }
}

pub(crate) type RioResult<T> = Result<T, Report<RioError>>;

pub(crate) struct RioCore {
    kernel: RioKernel,
    registry: RioRegistry,
    rio_outstanding_count: usize,
    cq_armed: bool,
}

pub(super) struct RioCoreRegistryEnv<'a> {
    registry: &'a mut RioRegistry,
    env: Option<RioEnv<'a>>,
}

impl RioCore {
    pub(super) fn new(kernel: RioKernel, registry: RioRegistry) -> Self {
        Self {
            kernel,
            registry,
            rio_outstanding_count: 0,
            cq_armed: true,
        }
    }

    pub(super) fn empty() -> Self {
        Self::new(RioKernel::noop(), RioRegistry::empty())
    }

    pub(super) fn kernel(&self) -> &RioKernel {
        &self.kernel
    }

    pub(super) fn kernel_mut(&mut self) -> &mut RioKernel {
        &mut self.kernel
    }

    pub(super) fn registry(&self) -> &RioRegistry {
        &self.registry
    }

    pub(super) fn registry_mut(&mut self) -> &mut RioRegistry {
        &mut self.registry
    }

    pub(super) fn registry_with_env<'a>(&'a mut self) -> RioCoreRegistryEnv<'a> {
        let env = self.kernel.env();
        RioCoreRegistryEnv {
            registry: &mut self.registry,
            env,
        }
    }

    pub(super) const fn outstanding_count(&self) -> usize {
        self.rio_outstanding_count
    }

    pub(super) fn set_outstanding_count(&mut self, count: usize) {
        self.rio_outstanding_count = count;
    }

    pub(super) fn next_outstanding_count(&self) -> Option<usize> {
        self.rio_outstanding_count.checked_add(1)
    }

    pub(super) fn decrement_outstanding(&mut self) {
        if self.rio_outstanding_count == 0 {
            return;
        }
        self.rio_outstanding_count -= 1;
    }

    pub(super) const fn cq_is_armed(&self) -> bool {
        self.cq_armed
    }

    pub(super) fn mark_cq_armed(&mut self) {
        self.cq_armed = true;
    }

    pub(super) fn mark_cq_disarmed(&mut self) {
        self.cq_armed = false;
    }

    #[cfg(feature = "test-hooks")]
    pub(super) fn chunk_register_attempts(&self) -> u64 {
        self.registry.chunk_register_attempts()
    }
}

impl<'a> RioCoreRegistryEnv<'a> {
    pub(super) fn into_parts(self) -> (&'a mut RioRegistry, Option<RioEnv<'a>>) {
        (self.registry, self.env)
    }
}
