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
use crate::ext::Extensions;
use crate::ops::submit::SubmissionResult;
use crate::rio::core::registry::RioRegistry;
use crate::rio::error::{RioError, RioReportExt, RioResult};
use crate::rio::{RioEnv, RioState, RioTarget};
use error_stack::ResultExt;
use std::io;
use windows_sys::Win32::Foundation::HANDLE;

impl RioState {
    pub(crate) fn new(
        port: HANDLE,
        entries: u32,
        ext: &Extensions,
        registration_mode: BufferRegistrationMode,
    ) -> RioResult<Self> {
        let kernel = RioKernel::from_extensions(port, entries, ext)?;

        let rq_depth = entries.clamp(32, 256);

        Ok(Self {
            kernel,
            registry: RioRegistry::new(rq_depth),
            registration_mode,
            actors: slotmap::SlotMap::with_key(),
            actor_by_handle: rustc_hash::FxHashMap::default(),
            udp_iocp_fallback_handles: rustc_hash::FxHashSet::default(),
            outstanding_count: 0,
        })
    }

    pub(crate) fn resize_rqs(&mut self, size: usize) {
        self.registry.resize_rqs(size);
    }

    pub(crate) fn clear_registered_rq(&mut self, idx: usize) {
        self.registry.clear_registered_rq(idx);
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
        target: RioTarget,
        buf: &mut veloq_buf::FixedBuf,
        registrar: &dyn veloq_buf::BufferRegistrar,
    ) -> io::Result<SubmissionResult> {
        self.try_submit_recv_internal(target, buf, registrar)
            .map_err(|e| e.to_io_error("RIOReceive submission failed"))
    }

    fn try_submit_recv_internal(
        &mut self,
        target: RioTarget,
        buf: &mut veloq_buf::FixedBuf,
        registrar: &dyn veloq_buf::BufferRegistrar,
    ) -> RioResult<SubmissionResult> {
        let RioTarget {
            fd,
            handle,
            user_data,
            generation,
            buf_offset,
        } = target;
        let Some(dispatch) = self.kernel.dispatch else {
            return Ok(SubmissionResult::Pending);
        };
        let env = RioEnv {
            registrar,
            dispatch: &dispatch,
            cq: self.kernel.cq,
            registration_mode: self.registration_mode,
        };
        let rq = self
            .ensure_actor((fd, handle), env)
            .map_err(|e| io::Error::other(e.to_string()))
            .change_context(RioError::Internal)
            .attach("failed to ensure RIO actor")?
            .rq;
        let rio_buf = self.registry.prepare_submission(
            buf,
            buf_offset,
            (buf.capacity().saturating_sub(buf_offset)) as u32,
            env,
        )?;
        let request_context = Self::encode_req_ctx(user_data, generation);
        if let Err(e) = self.kernel.submit_receive(rq, &rio_buf, request_context) {
            Self::free_op_req_ctx(request_context as u64);
            return Err(e).attach(format!(
                "RIOReceive submission failed: fd={fd:?}, handle={handle:?}"
            ));
        }
        self.outstanding_count += 1;
        Ok(SubmissionResult::Pending)
    }

    pub(crate) fn try_submit_send(
        &mut self,
        target: RioTarget,
        buf: &veloq_buf::FixedBuf,
        registrar: &dyn veloq_buf::BufferRegistrar,
    ) -> io::Result<SubmissionResult> {
        self.try_submit_send_internal(target, buf, registrar)
            .map_err(|e| e.to_io_error("RIOSend submission failed"))
    }

    fn try_submit_send_internal(
        &mut self,
        target: RioTarget,
        buf: &veloq_buf::FixedBuf,
        registrar: &dyn veloq_buf::BufferRegistrar,
    ) -> RioResult<SubmissionResult> {
        let RioTarget {
            fd,
            handle,
            user_data,
            generation,
            buf_offset,
        } = target;
        let Some(dispatch) = self.kernel.dispatch else {
            return Ok(SubmissionResult::Pending);
        };
        let env = RioEnv {
            registrar,
            dispatch: &dispatch,
            cq: self.kernel.cq,
            registration_mode: self.registration_mode,
        };
        let rq = self
            .ensure_actor((fd, handle), env)
            .map_err(|e| io::Error::other(e.to_string()))
            .change_context(RioError::Internal)
            .attach("failed to ensure RIO actor")?
            .rq;
        let rio_buf = self.registry.prepare_submission(
            buf,
            buf_offset,
            (buf.len().saturating_sub(buf_offset)) as u32,
            env,
        )?;
        let request_context = Self::encode_req_ctx(user_data, generation);
        if let Err(e) = self.kernel.submit_send(rq, &rio_buf, request_context) {
            Self::free_op_req_ctx(request_context as u64);
            return Err(e).attach(format!(
                "RIOSend submission failed: fd={fd:?}, handle={handle:?}"
            ));
        }
        self.outstanding_count += 1;
        Ok(SubmissionResult::Pending)
    }
}
