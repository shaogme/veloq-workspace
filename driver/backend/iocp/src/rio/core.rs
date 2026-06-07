//! Core context encoding, registry ownership, and kernel dispatch wrappers.

pub(crate) mod registry;
pub(crate) mod submit_ops;

use crate::config::{BorrowedRawHandle, IoFd, SocketKey};
use crate::error::{IocpError, iocp_report_to_event_res};
use crate::op::submit::SubmissionResult;
use crate::rio::RioState;
use crate::rio::core::registry::{RioAddrReservation, RioHeapLeaseToken, RioPreparedBuffer};
use crate::rio::core::submit_ops::{RioKernel, RioRq};
use crate::rio::error::{RioError, RioResult};
use diagweave::prelude::*;
use windows_sys::Win32::Networking::WinSock::RIO_BUF;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RioOpKind {
    Recv,
    Send,
    SendTo,
    RecvFrom,
}

impl RioOpKind {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Recv => "recv",
            Self::Send => "send",
            Self::SendTo => "send_to",
            Self::RecvFrom => "recv_from",
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct RioRequestDiagnostics {
    pub(crate) rq_raw: usize,
    pub(crate) data_buffer_id: usize,
    pub(crate) data_buffer_offset: u32,
    pub(crate) data_buffer_length: u32,
    pub(crate) addr_buffer_id: usize,
    pub(crate) addr_buffer_offset: u32,
    pub(crate) addr_buffer_length: u32,
}

impl RioRequestDiagnostics {
    fn new(rq: RioRq, data_buf: &RIO_BUF, addr: Option<&RioAddrReservation>) -> Self {
        let (addr_buffer_id, addr_buffer_offset, addr_buffer_length) = addr
            .map(|addr| {
                (
                    addr.rio_buf.BufferId as usize,
                    addr.rio_buf.Offset,
                    addr.rio_buf.Length,
                )
            })
            .unwrap_or((0, 0, 0));
        Self {
            rq_raw: rq.0 as usize,
            data_buffer_id: data_buf.BufferId as usize,
            data_buffer_offset: data_buf.Offset,
            data_buffer_length: data_buf.Length,
            addr_buffer_id,
            addr_buffer_offset,
            addr_buffer_length,
        }
    }
}

pub(crate) struct RioOpRequestInit {
    pub(crate) user_data: usize,
    pub(crate) generation: u32,
    pub(crate) socket_key: SocketKey,
    pub(crate) op_kind: RioOpKind,
    pub(crate) request_id: u64,
    pub(crate) addr_slot: Option<usize>,
    pub(crate) heap_lease: Option<RioHeapLeaseToken>,
    pub(crate) diagnostics: RioRequestDiagnostics,
}

#[derive(Clone, Copy)]
pub(crate) enum RioCompletionKind {
    Op {
        user_data: usize,
        generation: u32,
        socket_key: SocketKey,
        op_kind: RioOpKind,
        request_id: u64,
        addr_slot: Option<usize>,
        heap_lease: Option<RioHeapLeaseToken>,
        diagnostics: RioRequestDiagnostics,
        ctx_ptr: *mut RioOpRequestContext,
    },
}

#[repr(C)]
pub(crate) struct RioOpRequestContext {
    pub(crate) user_data: usize,
    pub(crate) generation: u32,
    pub(crate) socket_key: SocketKey,
    pub(crate) op_kind: RioOpKind,
    pub(crate) request_id: u64,
    pub(crate) addr_slot: usize,
    pub(crate) heap_lease: Option<RioHeapLeaseToken>,
    pub(crate) diagnostics: RioRequestDiagnostics,
}

pub(crate) struct RioOpCtxGuard(pub(crate) *mut RioOpRequestContext);

impl Drop for RioOpCtxGuard {
    fn drop(&mut self) {
        if !self.0.is_null() {
            // SAFETY: self.0 was created from Box::into_raw in encode_req_ctx.
            unsafe { drop(Box::from_raw(self.0)) };
            self.0 = std::ptr::null_mut();
        }
    }
}

pub(crate) struct RioSubmissionSpec {
    pub(crate) user_data: usize,
    pub(crate) generation: u32,
    pub(crate) socket_key: SocketKey,
    pub(crate) op_kind: RioOpKind,
    pub(crate) rq: RioRq,
    pub(crate) data_buf: RioPreparedBuffer,
    pub(crate) addr: Option<RioAddrReservation>,
}

pub(crate) struct RioPreparedRequest {
    pub(crate) socket_key: SocketKey,
    pub(crate) op_kind: RioOpKind,
    pub(crate) request_id: u64,
    pub(crate) rq: RioRq,
    pub(crate) context: *const std::ffi::c_void,
    pub(crate) data_buf: RioPreparedBuffer,
    pub(crate) addr: Option<RioAddrReservation>,
    pub(crate) diagnostics: RioRequestDiagnostics,
    pub(crate) outstanding_snapshot: usize,
}

pub(crate) struct RioSubmitErrorContext<'a> {
    pub(crate) scope: &'static str,
    pub(crate) fd: IoFd,
    pub(crate) handle: BorrowedRawHandle<'a>,
    pub(crate) user_data: usize,
    pub(crate) generation: u32,
    pub(crate) note: &'static str,
}

pub(crate) struct RioSubmissionLease<'a> {
    state: &'a mut RioState,
    request: RioPreparedRequest,
    submitted: bool,
}

impl RioPreparedRequest {
    pub(crate) fn attach_submit_error(
        &self,
        error: Report<RioError>,
        ctx: RioSubmitErrorContext<'_>,
    ) -> Report<RioError> {
        let diagnostics = self.diagnostics;
        error
            .push_ctx("scope", ctx.scope)
            .with_ctx("fd_fixed_index", ctx.fd.fixed_index())
            .with_ctx("fd_generation", ctx.fd.generation())
            .with_ctx("handle_raw", ctx.handle.raw().as_handle() as usize)
            .with_ctx("socket_raw", self.socket_key.as_handle() as usize)
            .with_ctx("user_data", ctx.user_data)
            .with_ctx("generation", ctx.generation)
            .with_ctx("rio_op_kind", self.op_kind.as_str())
            .with_ctx("rio_request_id", self.request_id)
            .with_ctx(
                "addr_slot",
                self.addr.map(|addr| addr.slot).unwrap_or(usize::MAX),
            )
            .with_ctx("rq_raw", diagnostics.rq_raw)
            .with_ctx("data_buffer_id", diagnostics.data_buffer_id)
            .with_ctx("data_buffer_offset", diagnostics.data_buffer_offset)
            .with_ctx("data_buffer_length", diagnostics.data_buffer_length)
            .with_ctx("addr_buffer_id", diagnostics.addr_buffer_id)
            .with_ctx("addr_buffer_offset", diagnostics.addr_buffer_offset)
            .with_ctx("addr_buffer_length", diagnostics.addr_buffer_length)
            .with_ctx("outstanding_count", self.outstanding_snapshot)
            .attach_note(ctx.note)
    }
}

impl<'a> RioSubmissionLease<'a> {
    pub(crate) fn submit_with(
        mut self,
        submit: impl FnOnce(&RioKernel, &RioPreparedRequest) -> RioResult<()>,
    ) -> RioResult<SubmissionResult> {
        submit(&self.state.kernel, &self.request)?;
        self.commit_submitted();
        Ok(SubmissionResult::Pending)
    }

    fn commit_submitted(&mut self) {
        if self.submitted {
            return;
        }
        self.state
            .registry
            .commit_heap_lease(self.request.data_buf.heap_lease);
        self.state.outstanding_count += 1;
        self.state
            .acquire_socket_kernel_inflight(self.request.socket_key);
        self.submitted = true;
    }
}

impl Drop for RioSubmissionLease<'_> {
    fn drop(&mut self) {
        if self.submitted {
            return;
        }
        RioState::free_op_req_ctx(self.request.context as u64);
        self.state
            .registry
            .free_addr_slot(self.request.addr.map(|addr| addr.slot));
    }
}

#[inline]
pub(crate) fn rio_result_to_event_res(res: &crate::error::IocpDriverResult<usize>) -> i32 {
    match res {
        Ok(v) => (*v).min(i32::MAX as usize) as i32,
        Err(e) => iocp_report_to_event_res(e),
    }
}

impl RioState {
    #[inline]
    pub(crate) fn encode_req_ctx(init: RioOpRequestInit) -> *const std::ffi::c_void {
        let ctx = Box::new(RioOpRequestContext {
            user_data: init.user_data,
            generation: init.generation,
            socket_key: init.socket_key,
            op_kind: init.op_kind,
            request_id: init.request_id,
            addr_slot: init.addr_slot.unwrap_or(usize::MAX),
            heap_lease: init.heap_lease,
            diagnostics: init.diagnostics,
        });
        Box::into_raw(ctx).cast::<std::ffi::c_void>()
    }

    #[inline]
    pub(crate) fn prepare_submission_lease(
        &mut self,
        spec: RioSubmissionSpec,
    ) -> RioSubmissionLease<'_> {
        let diagnostics =
            RioRequestDiagnostics::new(spec.rq, &spec.data_buf.rio_buf, spec.addr.as_ref());
        let request_id = self.next_request_id();
        let context = Self::encode_req_ctx(RioOpRequestInit {
            user_data: spec.user_data,
            generation: spec.generation,
            socket_key: spec.socket_key,
            op_kind: spec.op_kind,
            request_id,
            addr_slot: spec.addr.map(|addr| addr.slot),
            heap_lease: spec.data_buf.heap_lease,
            diagnostics,
        });
        let request = RioPreparedRequest {
            socket_key: spec.socket_key,
            op_kind: spec.op_kind,
            request_id,
            rq: spec.rq,
            context,
            data_buf: spec.data_buf,
            addr: spec.addr,
            diagnostics,
            outstanding_snapshot: self.outstanding_count,
        };
        RioSubmissionLease {
            state: self,
            request,
            submitted: false,
        }
    }

    #[inline]
    pub(crate) fn decode_req_ctx(ctx: u64) -> Option<RioCompletionKind> {
        if ctx == 0 {
            return None;
        }
        let ctx_ptr = ctx as usize as *mut RioOpRequestContext;
        if ctx_ptr.is_null() {
            return None;
        }
        // SAFETY: ctx_ptr is a valid pointer to RioOpRequestContext.
        let op_ctx = unsafe { &*ctx_ptr };
        Some(RioCompletionKind::Op {
            user_data: op_ctx.user_data,
            generation: op_ctx.generation,
            socket_key: op_ctx.socket_key,
            op_kind: op_ctx.op_kind,
            request_id: op_ctx.request_id,
            addr_slot: (op_ctx.addr_slot != usize::MAX).then_some(op_ctx.addr_slot),
            heap_lease: op_ctx.heap_lease,
            diagnostics: op_ctx.diagnostics,
            ctx_ptr,
        })
    }

    #[inline]
    pub(crate) fn free_op_req_ctx(ctx: u64) {
        if ctx == 0 {
            return;
        }
        let ptr = ctx as usize as *mut RioOpRequestContext;
        if !ptr.is_null() {
            // SAFETY: ptr was created from Box::into_raw in encode_req_ctx.
            unsafe { drop(Box::from_raw(ptr)) };
        }
    }

    #[inline]
    fn next_request_id(&mut self) -> u64 {
        self.next_request_id = self.next_request_id.wrapping_add(1);
        if self.next_request_id == 0 {
            self.next_request_id = 1;
        }
        self.next_request_id
    }

    #[inline]
    pub(crate) fn last_wsa_error_code() -> i32 {
        // SAFETY: WSAGetLastError is safe to call.
        unsafe { windows_sys::Win32::Networking::WinSock::WSAGetLastError() }
    }

    pub(crate) fn last_wsa_report(context: RioError, scope: &'static str) -> Report<RioError> {
        let code = Self::last_wsa_error_code() as u32;
        context
            .to_report()
            .push_ctx("scope", scope)
            .set_error_code(code)
            .attach_note(
                IocpError::Internal
                    .to_report()
                    .push_ctx("scope", scope)
                    .set_error_code(code as i32)
                    .attach_note("winsock error"),
            )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::IocpHandle;

    fn test_req_init(addr_slot: Option<usize>) -> RioOpRequestInit {
        RioOpRequestInit {
            user_data: 11,
            generation: 17,
            socket_key: IocpHandle::for_socket(std::ptr::null_mut()),
            op_kind: RioOpKind::Recv,
            request_id: 23,
            addr_slot,
            heap_lease: None,
            diagnostics: RioRequestDiagnostics::default(),
        }
    }

    #[test]
    fn op_ctx_roundtrip_decode_and_free() {
        let ptr = RioState::encode_req_ctx(test_req_init(None));
        let decoded = RioState::decode_req_ctx(ptr as u64);
        assert!(matches!(
            decoded,
            Some(RioCompletionKind::Op {
                user_data: 11,
                generation: 17,
                op_kind: RioOpKind::Recv,
                request_id: 23,
                addr_slot: None,
                ..
            })
        ));
        RioState::free_op_req_ctx(ptr as u64);
    }

    #[test]
    fn op_ctx_with_addr_roundtrip_decode_and_free() {
        let ptr = RioState::encode_req_ctx(test_req_init(Some(3)));
        let decoded = RioState::decode_req_ctx(ptr as u64);
        assert!(matches!(
            decoded,
            Some(RioCompletionKind::Op {
                user_data: 11,
                generation: 17,
                op_kind: RioOpKind::Recv,
                request_id: 23,
                addr_slot: Some(3),
                ..
            })
        ));
        RioState::free_op_req_ctx(ptr as u64);
    }

    #[test]
    fn rio_result_translation_behaviour() {
        assert_eq!(rio_result_to_event_res(&Ok(5)), 5);
        assert_eq!(
            rio_result_to_event_res(&Ok((i32::MAX as usize) + 10)),
            i32::MAX
        );
        let err = IocpError::Internal
            .to_report()
            .push_ctx("scope", "rio.core.tests")
            .set_error_code(10022)
            .attach_note("invalid argument");
        assert_eq!(rio_result_to_event_res(&Err(err)), -10022);
    }

    #[test]
    fn free_zero_context_is_noop() {
        RioState::free_op_req_ctx(0);
    }
}
