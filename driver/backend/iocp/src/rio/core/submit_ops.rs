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
use crate::op::submit::SubmissionResult;
use crate::rio::core::registry::{RioRegistry, RioSubmissionKind};
use crate::rio::error::{RioError, RioResult};
use crate::rio::{RioEnv, RioState, RioTarget};
use diagweave::prelude::*;

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
            actors: slotmap::SlotMap::with_key(),
            actor_by_handle: rustc_hash::FxHashMap::default(),
            socket_runtime: rustc_hash::FxHashMap::default(),
            outstanding_count: 0,
        })
    }

    pub(crate) fn register_chunk(&mut self, id: u16, ptr: *const u8, len: usize) -> RioResult<()> {
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
            user_data,
            generation,
            buf_offset,
            operation,
        } = target;
        let buf_len = RioSubmissionKind::Recv.data_len(buf, buf_offset, operation)?;
        let dispatch = self
            .kernel
            .dispatch
            .ok_or(RioError::NotSupported)
            .attach_note("RIO not supported or dispatch table missing")?;
        let env = RioEnv {
            registrar,
            dispatch: &dispatch,
            cq: self.kernel.cq,
            registration_mode: self.registration_mode,
        };
        let rq = {
            let actor = self
                .ensure_actor((fd, handle), env)
                .attach_note("failed to ensure RIO actor")?;
            actor.rq
        };
        let data_buf = self
            .registry
            .prepare_submission(buf, buf_offset, buf_len, env)?;
        let request_context =
            Self::encode_req_ctx(user_data, generation, None, data_buf.heap_lease);
        if let Err(e) = self
            .kernel
            .submit_receive(rq, &data_buf.rio_buf, request_context)
        {
            Self::free_op_req_ctx(request_context as u64);
            return Err(e
                .push_ctx("scope", "rio.core.submit_ops.try_submit_recv_internal")
                .with_ctx("fd_fixed_index", fd.fixed_index())
                .with_ctx("fd_generation", fd.generation())
                .with_ctx("handle_raw", handle.raw().as_handle() as usize)
                .with_ctx("rq_raw", rq.0 as usize)
                .with_ctx("buffer_id", data_buf.rio_buf.BufferId as usize)
                .with_ctx("buffer_offset", data_buf.rio_buf.Offset)
                .with_ctx("buffer_length", data_buf.rio_buf.Length)
                .with_ctx("outstanding_count", self.outstanding_count)
                .attach_note("RIOReceive submit failed"));
        }
        self.registry.commit_heap_lease(data_buf.heap_lease);
        self.outstanding_count += 1;
        Ok(SubmissionResult::Pending)
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
            user_data,
            generation,
            buf_offset,
            operation,
        } = target;
        let buf_len = RioSubmissionKind::Send.data_len(buf, buf_offset, operation)?;
        let dispatch = self
            .kernel
            .dispatch
            .ok_or(RioError::NotSupported)
            .attach_note("RIO not supported or dispatch table missing")?;
        let env = RioEnv {
            registrar,
            dispatch: &dispatch,
            cq: self.kernel.cq,
            registration_mode: self.registration_mode,
        };
        let outstanding_snapshot = self.outstanding_count;
        let rq = {
            let actor = self
                .ensure_actor((fd, handle), env)
                .push_ctx("scope", "rio.core.submit_ops.try_submit_send.ensure_actor")
                .with_ctx("fd_fixed_index", fd.fixed_index())
                .with_ctx("fd_generation", fd.generation())
                .with_ctx("handle_raw", handle.raw().as_handle() as usize)
                .with_ctx("outstanding_count", outstanding_snapshot)
                .attach_note("failed to ensure RIO actor")?;

            actor.rq
        };
        let data_buf = self
            .registry
            .prepare_submission(buf, buf_offset, buf_len, env)?;
        let request_context =
            Self::encode_req_ctx(user_data, generation, None, data_buf.heap_lease);
        if let Err(e) = self
            .kernel
            .submit_send(rq, &data_buf.rio_buf, request_context)
        {
            Self::free_op_req_ctx(request_context as u64);
            return Err(e
                .push_ctx("scope", "rio.core.submit_ops.try_submit_send")
                .with_ctx("fd_fixed_index", fd.fixed_index())
                .with_ctx("fd_generation", fd.generation())
                .with_ctx("handle_raw", handle.raw().as_handle() as usize)
                .with_ctx("rq_raw", rq.0 as usize)
                .with_ctx("buffer_id", data_buf.rio_buf.BufferId as usize)
                .with_ctx("buffer_offset", data_buf.rio_buf.Offset)
                .with_ctx("buffer_length", data_buf.rio_buf.Length)
                .with_ctx("outstanding_count", self.outstanding_count)
                .attach_note("RIOSend submit failed"));
        }
        self.registry.commit_heap_lease(data_buf.heap_lease);
        self.outstanding_count += 1;
        Ok(SubmissionResult::Pending)
    }
}
