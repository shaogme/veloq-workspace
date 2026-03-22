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
use crate::rio::error::{RioDiag, RioError, RioReportExt, RioResult};
use crate::rio::{RioEnv, RioState, RioTarget};
use error_stack::ResultExt;
use std::io;
use tracing::error;
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
        let dispatch = self.kernel.dispatch.ok_or_else(|| {
            error_stack::Report::new(RioError::NotSupported)
                .attach("RIO not supported or dispatch table missing")
        })?;
        let env = RioEnv {
            registrar,
            dispatch: &dispatch,
            cq: self.kernel.cq,
            registration_mode: self.registration_mode,
        };
        let rq = self
            .ensure_actor((fd, handle), env)
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
        let RioTarget {
            fd,
            handle,
            user_data,
            generation,
            buf_offset,
        } = target;
        let buf_len = buf.len();
        self.try_submit_send_internal(
            RioTarget {
                fd,
                handle,
                user_data,
                generation,
                buf_offset,
            },
            buf,
            registrar,
        )
            .map_err(|e| {
                let source = e.to_string();
                let wsa_class = RioDiag::wsa_class_from_text(&source);
                let diag_submit = RioDiag::new("submit_send")
                    .field("fd", format!("{fd:?}"))
                    .field("handle", format!("{handle:?}"))
                    .field("user_data", user_data)
                    .field("generation", generation)
                    .field("buf_offset", buf_offset)
                    .field("buf_len", buf_len)
                    .field("wsa_class", wsa_class);
                let diag_source = RioDiag::new("submit_send_source")
                    .field("source_kind", "rio_report")
                    .field("source", source);
                e.to_io_error(format!(
                    "RIOSend submission failed; {}; {}",
                    diag_submit, diag_source
                ))
            })
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
        let dispatch = self.kernel.dispatch.ok_or_else(|| {
            error_stack::Report::new(RioError::NotSupported)
                .attach("RIO not supported or dispatch table missing")
        })?;
        let env = RioEnv {
            registrar,
            dispatch: &dispatch,
            cq: self.kernel.cq,
            registration_mode: self.registration_mode,
        };
        let outstanding_snapshot = self.outstanding_count;
        let (rq, actor_state) = {
            let actor = self
                .ensure_actor((fd, handle), env)
                .map_err(|e| {
                    let source = e.to_string();
                    let wsa_class = RioDiag::wsa_class_from_text(&source);
                    let diag = RioDiag::new("submit_send_ensure_actor")
                        .field("fd", format!("{fd:?}"))
                        .field("handle", format!("{handle:?}"))
                        .field("outstanding_count", outstanding_snapshot)
                        .field("wsa_class", wsa_class);
                    e.attach(diag.to_string())
                })
                .attach("failed to ensure RIO actor")?;
            (actor.rq, format!("{:?}", actor.state))
        };
        let rio_buf = self.registry.prepare_submission(
            buf,
            buf_offset,
            (buf.len().saturating_sub(buf_offset)) as u32,
            env,
        )?;
        let request_context = Self::encode_req_ctx(user_data, generation);
        if let Err(e) = self.kernel.submit_send(rq, &rio_buf, request_context) {
            Self::free_op_req_ctx(request_context as u64);
            let source = e.to_string();
            let wsa_class = RioDiag::wsa_class_from_text(&source);
            let diag = RioDiag::new("submit_send_internal")
                .field("fd", format!("{fd:?}"))
                .field("handle", format!("{handle:?}"))
                .field("rq_raw", format!("0x{:x}", rq.0 as usize))
                .field("buffer_id", format!("0x{:x}", rio_buf.BufferId as usize))
                .field("buffer_offset", rio_buf.Offset)
                .field("buffer_length", rio_buf.Length)
                .field("outstanding_count", self.outstanding_count)
                .field("actor_state", actor_state.clone())
                .field("wsa_class", wsa_class);
            error!(
                fd = ?fd,
                handle = ?handle,
                rq_raw = rq.0 as usize,
                buffer_id = rio_buf.BufferId as usize,
                buffer_offset = rio_buf.Offset,
                buffer_length = rio_buf.Length,
                outstanding_count = self.outstanding_count,
                actor_state = %actor_state,
                wsa_class = wsa_class,
                rio_error = %source,
                "RIOSend submit failed diagnostics"
            );
            return Err(e).attach(diag.to_string());
        }
        self.outstanding_count += 1;
        Ok(SubmissionResult::Pending)
    }
}
