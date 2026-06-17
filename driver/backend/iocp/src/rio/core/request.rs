use super::{
    registry::{
        RioAddrReservation, RioBufferLeaseToken, RioPreparedBuffer, RioRegistry, RioSubmissionKind,
    },
    submit_ops::RioRq,
};
use crate::{
    config::{BorrowedRawHandle, IoFd, SocketKey},
    rio::{SocketInflightToken, error::RioError},
};
use diagweave::prelude::*;
use std::ffi::c_void;
use veloq_driver_core::driver::OpToken;
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
    pub(super) fn new(rq: RioRq, data_buf: &RIO_BUF, addr: Option<&RioAddrReservation>) -> Self {
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
    pub(crate) token: OpToken,
    pub(crate) socket_inflight: SocketInflightToken,
    pub(crate) op_kind: RioOpKind,
    pub(crate) request_id: u64,
    pub(crate) addr_slot: Option<usize>,
    pub(crate) buffer_lease: Option<RioBufferLeaseToken>,
    pub(crate) diagnostics: RioRequestDiagnostics,
}

pub(crate) enum RioCompletionKind {
    Op {
        init: RioOpRequestInit,
        context: RioCompletedRequestContext,
    },
}

pub(crate) enum RioRequestContextDecode {
    Valid(RioCompletionKind),
    Malformed {
        raw: u64,
    },
    Missing {
        id: RioRequestContextId,
    },
    Stale {
        id: RioRequestContextId,
        actual_generation: u32,
    },
}

const RIO_REQUEST_CONTEXT_MAGIC: u64 = 0xA7;
const RIO_REQUEST_CONTEXT_MAGIC_SHIFT: u32 = 56;
const RIO_REQUEST_CONTEXT_INDEX_SHIFT: u32 = 32;
const RIO_REQUEST_CONTEXT_INDEX_MASK: u64 = 0x00ff_ffff;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct RioRequestContextId {
    index: usize,
    generation: u32,
}

impl RioRequestContextId {
    #[inline]
    pub(crate) fn new(index: usize, generation: u32) -> Self {
        assert!(
            index <= RIO_REQUEST_CONTEXT_INDEX_MASK as usize,
            "RIO request context index exceeds encodable range"
        );
        Self { index, generation }
    }

    #[inline]
    pub(crate) const fn index(self) -> usize {
        self.index
    }

    #[inline]
    pub(crate) const fn generation(self) -> u32 {
        self.generation
    }

    #[inline]
    pub(crate) fn raw(self) -> u64 {
        (RIO_REQUEST_CONTEXT_MAGIC << RIO_REQUEST_CONTEXT_MAGIC_SHIFT)
            | ((self.index as u64) << RIO_REQUEST_CONTEXT_INDEX_SHIFT)
            | self.generation as u64
    }

    #[inline]
    pub(crate) fn from_raw(raw: u64) -> Option<Self> {
        let magic = raw >> RIO_REQUEST_CONTEXT_MAGIC_SHIFT;
        if magic != RIO_REQUEST_CONTEXT_MAGIC {
            return None;
        }
        let index =
            ((raw >> RIO_REQUEST_CONTEXT_INDEX_SHIFT) & RIO_REQUEST_CONTEXT_INDEX_MASK) as usize;
        let generation = raw as u32;
        Some(Self { index, generation })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct RioPreparedRequestContext {
    id: RioRequestContextId,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct RioSubmittedRequestContext {
    id: RioRequestContextId,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct RioCompletedRequestContext;

impl RioPreparedRequestContext {
    #[inline]
    pub(crate) fn new(id: RioRequestContextId) -> Self {
        Self { id }
    }

    #[inline]
    pub(crate) const fn id(self) -> RioRequestContextId {
        self.id
    }

    #[inline]
    pub(crate) fn as_request_context(&self) -> *const c_void {
        self.id.raw() as usize as *const c_void
    }

    #[inline]
    fn into_submitted(self) -> RioSubmittedRequestContext {
        RioSubmittedRequestContext { id: self.id }
    }
}

impl RioSubmittedRequestContext {
    #[inline]
    pub(super) fn as_request_context(&self) -> *const c_void {
        self.id.raw() as usize as *const c_void
    }
}

impl RioCompletedRequestContext {
    #[inline]
    pub(crate) fn new() -> Self {
        Self
    }
}

pub(crate) struct RioPreparedRequest {
    pub(crate) op_kind: RioOpKind,
    pub(crate) request_id: u64,
    pub(crate) rq: RioRq,
    pub(super) context: Option<RioPreparedRequestContext>,
    pub(crate) token: OpToken,
    pub(crate) socket_key: SocketKey,
    pub(crate) addr_slot: Option<usize>,
    pub(crate) data_buf: RioPreparedBuffer,
    pub(crate) addr: Option<RioAddrReservation>,
    pub(crate) diagnostics: RioRequestDiagnostics,
    pub(crate) outstanding_snapshot: usize,
}

pub(crate) struct RioSubmitErrorContext<'a> {
    pub(crate) scope: &'static str,
    pub(crate) fd: IoFd,
    pub(crate) handle: BorrowedRawHandle<'a>,
    pub(crate) note: &'static str,
}

#[derive(Clone, Copy)]
pub(crate) enum RioAddressPolicy {
    None,
    SendTo {
        addr_ptr: *const c_void,
        addr_len: i32,
    },
    RecvFrom {
        addr_ptr: *mut c_void,
    },
}

#[derive(Clone, Copy)]
pub(crate) struct RioSubmitPlan<'a> {
    pub(crate) fd: IoFd,
    pub(crate) handle: BorrowedRawHandle<'a>,
    pub(crate) token: OpToken,
    pub(crate) op_kind: RioOpKind,
    pub(crate) buffer_kind: RioSubmissionKind,
    pub(crate) buffer: &'a veloq_buf::FixedBuf,
    pub(crate) buffer_offset: usize,
    pub(crate) operation: &'static str,
    pub(crate) address: RioAddressPolicy,
    pub(crate) dispatch_error: RioError,
    pub(crate) dispatch_note: &'static str,
    pub(crate) submit_scope: &'static str,
    pub(crate) submit_note: &'static str,
}

impl RioPreparedRequest {
    #[inline]
    pub(super) fn take_init(&mut self, registry: &mut RioRegistry) -> Option<RioOpRequestInit> {
        let context = self.context.take()?;
        registry.take_prepared_request_init(context)
    }

    #[inline]
    pub(crate) fn socket_key(&self) -> SocketKey {
        self.socket_key
    }

    #[inline]
    pub(crate) fn as_request_context(&self) -> *const c_void {
        self.context
            .as_ref()
            .expect("RIO prepared request context missing")
            .as_request_context()
    }

    #[inline]
    pub(super) fn mark_submitted(&mut self) -> RioSubmittedRequestContext {
        self.context
            .take()
            .expect("RIO prepared request context already submitted")
            .into_submitted()
    }

    pub(crate) fn attach_submit_error(
        &self,
        error: Report<RioError>,
        ctx: RioSubmitErrorContext<'_>,
    ) -> Report<RioError> {
        let diagnostics = self.diagnostics;
        let socket_key = self.socket_key();
        error
            .push_ctx("scope", ctx.scope)
            .with_ctx("fd_fixed_index", ctx.fd.fixed_index())
            .with_ctx("fd_generation", ctx.fd.generation())
            .with_ctx("handle_raw", ctx.handle.raw().as_handle() as usize)
            .with_ctx("socket_raw", socket_key.as_handle() as usize)
            .with_ctx("user_data", self.token.index())
            .with_ctx("generation", self.token.generation())
            .with_ctx("rio_op_kind", self.op_kind.as_str())
            .with_ctx("rio_request_id", self.request_id)
            .with_ctx("addr_slot", self.addr_slot.unwrap_or(usize::MAX))
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

impl<'a> RioSubmitPlan<'a> {
    #[inline]
    pub(super) fn submit_error_context(&self) -> RioSubmitErrorContext<'a> {
        RioSubmitErrorContext {
            scope: self.submit_scope,
            fd: self.fd,
            handle: self.handle,
            note: self.submit_note,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::registry::RioRegistry;
    use super::*;
    use crate::config::IocpHandle;

    use std::ptr;

    fn test_req_init(addr_slot: Option<usize>) -> RioOpRequestInit {
        let socket_key = IocpHandle::for_socket(ptr::null_mut());
        RioOpRequestInit {
            token: OpToken::from_registry_parts(11, 17).expect("test token should be encodable"),
            socket_inflight: SocketInflightToken::new(socket_key),
            op_kind: RioOpKind::Recv,
            request_id: 23,
            addr_slot,
            buffer_lease: None,
            diagnostics: RioRequestDiagnostics::default(),
        }
    }

    #[test]
    fn op_ctx_roundtrip_decode_and_free() {
        let mut registry = RioRegistry::new(32, 1);
        let context = registry.alloc_request_context(test_req_init(None));
        let raw = context.as_request_context() as usize as u64;
        let _submitted = context.into_submitted();
        let decoded = registry.decode_request_context(raw);
        assert!(matches!(
            decoded,
            Some(RioCompletionKind::Op {
                init: RioOpRequestInit {
                    token,
                    op_kind: RioOpKind::Recv,
                    request_id: 23,
                    addr_slot: None,
                    ..
                },
                ..
            }) if token
                == OpToken::from_registry_parts(11, 17)
                    .expect("test token should be encodable")));
    }

    #[test]
    fn op_ctx_with_addr_roundtrip_decode_and_free() {
        let mut registry = RioRegistry::new(32, 1);
        let context = registry.alloc_request_context(test_req_init(Some(3)));
        let raw = context.as_request_context() as usize as u64;
        let _submitted = context.into_submitted();
        let decoded = registry.decode_request_context(raw);
        assert!(matches!(
            decoded,
            Some(RioCompletionKind::Op {
                init: RioOpRequestInit {
                    token,
                    op_kind: RioOpKind::Recv,
                    request_id: 23,
                    addr_slot: Some(3),
                    ..
                },
                ..
            }) if token
                == OpToken::from_registry_parts(11, 17)
                    .expect("test token should be encodable")));
    }

    #[test]
    fn decode_zero_context_is_noop() {
        let mut registry = RioRegistry::new(32, 1);
        assert!(registry.decode_request_context(0).is_none());
    }

    #[test]
    fn decode_unknown_context_does_not_deref_raw_pointer() {
        let mut registry = RioRegistry::new(32, 1);
        assert!(
            registry
                .decode_request_context(0xa700_0002_0000_0001)
                .is_none()
        );
    }
}
