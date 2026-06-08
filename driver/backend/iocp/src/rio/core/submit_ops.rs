//! Kernel-facing RIO dispatch table and submission primitives.
//!
//! This module encapsulates:
//! - CQ creation/notification lifecycle,
//! - minimal wrappers for `RIOReceive`, `RIOSend`, and `RIOSendEx`,
//! - `RioState` constructors and basic registration entry points.
//!
//! It forms the low-level boundary between high-level runtime orchestration and
//! Windows RIO APIs, keeping unsafe calls and pointer setup in one place.

pub(crate) mod dispatch;
pub(crate) use dispatch::*;

use crate::BufferRegistrationMode;
use crate::config::BorrowedRawHandle;
use crate::ext::Extensions;
use crate::op::SubmissionResult;
use crate::rio::core::registry::{RioRegistry, RioSubmissionKind};
use crate::rio::core::{RioAddressPolicy, RioOpKind, RioSubmitPlan};
use crate::rio::error::{RioError, RioResult};
use crate::rio::{RioState, RioTarget};

impl RioState {
    pub(crate) fn new(
        port: BorrowedRawHandle<'_>,
        entries: u32,
        ext: &Extensions,
        registration_mode: BufferRegistrationMode,
    ) -> RioResult<Self> {
        let kernel = RioKernel::from_extensions(port, entries, ext)?;

        // Keep per-socket RQ depth conservative so that multi-socket warmup
        // does not exhaust RIO request-queue resources too early.
        let rq_depth = entries.clamp(32, 64);

        Ok(Self {
            kernel,
            registry: RioRegistry::new(rq_depth, entries as usize),
            registration_mode,
            submissions_closed: false,
            actors: slotmap::SlotMap::with_key(),
            actor_by_handle: rustc_hash::FxHashMap::default(),
            socket_runtime: rustc_hash::FxHashMap::default(),
            outstanding_count: 0,
            next_request_id: 0,
            deferred_payloads: Vec::new(),
        })
    }

    pub(crate) fn register_chunk(
        &mut self,
        id: veloq_buf::heap::ChunkId,
        ptr: *const u8,
        len: usize,
    ) -> RioResult<()> {
        let Some(env) = self
            .kernel
            .env(&veloq_buf::NoopRegistrar, self.registration_mode)
        else {
            return Ok(());
        };
        self.registry.register_chunk(id, (ptr, len), env)
    }

    pub(crate) fn try_submit_recv(
        &mut self,
        target: RioTarget<'_>,
        buf: &mut veloq_buf::FixedBuf,
        registrar: &dyn veloq_buf::BufferRegistrar,
    ) -> RioResult<SubmissionResult> {
        self.try_submit_recv_internal(target, buf, registrar)
    }

    fn try_submit_recv_internal(
        &mut self,
        target: RioTarget<'_>,
        buf: &mut veloq_buf::FixedBuf,
        registrar: &dyn veloq_buf::BufferRegistrar,
    ) -> RioResult<SubmissionResult> {
        let RioTarget {
            fd,
            handle,
            token,
            buf_offset,
            operation,
        } = target;
        self.submit_rio(
            RioSubmitPlan {
                fd,
                handle,
                token,
                op_kind: RioOpKind::Recv,
                buffer_kind: RioSubmissionKind::Recv,
                buffer: buf,
                buffer_offset: buf_offset,
                operation,
                address: RioAddressPolicy::None,
                dispatch_error: RioError::NotSupported,
                dispatch_note: "RIO not supported or dispatch table missing",
                submit_scope: "rio.core.submit_ops.try_submit_recv_internal",
                submit_note: "RIOReceive submit failed",
            },
            registrar,
            |kernel, request| {
                kernel.submit_receive(
                    request.rq,
                    &request.data_buf.rio_buf,
                    request.as_request_context(),
                )
            },
        )
    }

    pub(crate) fn try_submit_send(
        &mut self,
        target: RioTarget<'_>,
        buf: &veloq_buf::FixedBuf,
        registrar: &dyn veloq_buf::BufferRegistrar,
    ) -> RioResult<SubmissionResult> {
        let RioTarget {
            fd,
            handle,
            token,
            buf_offset,
            operation,
        } = target;
        self.submit_rio(
            RioSubmitPlan {
                fd,
                handle,
                token,
                op_kind: RioOpKind::Send,
                buffer_kind: RioSubmissionKind::Send,
                buffer: buf,
                buffer_offset: buf_offset,
                operation,
                address: RioAddressPolicy::None,
                dispatch_error: RioError::NotSupported,
                dispatch_note: "RIO not supported or dispatch table missing",
                submit_scope: "rio.core.submit_ops.try_submit_send",
                submit_note: "RIOSend submit failed",
            },
            registrar,
            |kernel, request| {
                kernel.submit_send(
                    request.rq,
                    &request.data_buf.rio_buf,
                    request.as_request_context(),
                )
            },
        )
    }
}
