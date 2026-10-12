//! Core context encoding, registry ownership, and kernel dispatch wrappers. / 核心上下文编码、注册表所有权和内核分发表封装。

use crate::BufferRegistrationMode;
use diagweave::prelude::*;
use veloq_buf::BufferRegistrar;

mod registry;
mod request;
mod submit_ops;
mod submit_txn;

pub(crate) use registry::{RioAddrReservation, RioBufferLeaseToken, RioRegistry};
pub(crate) use request::{
    RioAddressPolicy, RioCompletedRequestContext, RioCompletionKind, RioOpKind, RioOpRequestInit,
    RioPreparedRequest, RioPreparedRequestContext, RioRequestContextDecode, RioRequestContextId,
    RioSubmitPlan,
};

#[cfg(test)]
pub(crate) use request::RioRequestDiagnostics;

pub(crate) use submit_ops::{
    RioBufferId, RioCq, RioDispatch, RioKernel, RioProvider, RioReceiveReplacementArgs, RioRq,
    RioSendToArgs, RioSubmitOperations, RioTarget,
};
pub(crate) use submit_txn::{RioSubmitTxn, RioSubmittedRequest};

#[derive(Clone, Copy)]
pub(crate) struct RioEnv<'a> {
    pub(crate) registrar: &'a dyn BufferRegistrar,
    pub(crate) dispatch: &'a RioDispatch,
    pub(crate) cq: RioCq,
    pub(crate) registration_mode: BufferRegistrationMode,
}

set! {
    #[derive(Debug, Copy, Clone, PartialEq, Eq)]
    pub RioError = {
        #[display("RIO library or function pointers load failure")]
        LibraryLoad,
        #[display("failed to register memory buffer for RIO")]
        BufferRegistration,
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
    registration_mode: BufferRegistrationMode,
    rio_outstanding_count: usize,
    cq_armed: bool,
}

pub(super) struct RioCoreRegistryEnv<'a> {
    registry: &'a mut RioRegistry,
    env: Option<RioEnv<'a>>,
}

impl RioCore {
    pub(super) fn new(
        kernel: RioKernel,
        registry: RioRegistry,
        registration_mode: BufferRegistrationMode,
    ) -> Self {
        Self {
            kernel,
            registry,
            registration_mode,
            rio_outstanding_count: 0,
            cq_armed: true,
        }
    }

    pub(super) fn empty(registration_mode: BufferRegistrationMode) -> Self {
        Self::new(
            RioKernel::noop(),
            RioRegistry::new(32, 1),
            registration_mode,
        )
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

    pub(super) fn registry_with_env<'a>(
        &'a mut self,
        registrar: &'a dyn BufferRegistrar,
    ) -> RioCoreRegistryEnv<'a> {
        let env = self.kernel.env(registrar, self.registration_mode);
        RioCoreRegistryEnv {
            registry: &mut self.registry,
            env,
        }
    }

    pub(super) const fn registration_mode(&self) -> BufferRegistrationMode {
        self.registration_mode
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
