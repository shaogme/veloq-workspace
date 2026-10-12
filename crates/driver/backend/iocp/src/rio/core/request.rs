use super::{
    registry::{RioAddrReservation, RioBufferLeaseToken, RioPreparedBuffer, RioRegistry},
    submit_ops::{RioRq, RioSubmissionKind},
};
use crate::{
    config::{BorrowedRawHandle, IoFd, SocketKey},
    net::addr::SockAddrStorage,
    rio::{SocketInflightToken, error::RioError},
};
use core::ffi::c_void;
use diagweave::prelude::*;
use veloq_buf::FixedBuf;
use veloq_driver_core::{driver::OpToken, slot::Generation};
use veloq_std::{boxed::Box, string::ToString};
use windows_sys::Win32::Networking::WinSock::RIO_BUF;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RioOpKind {
    Recv,
    RecvProvided,
    Send,
    SendTo,
    TcpRecvMulti,
    UdpRecvMulti,
}

impl RioOpKind {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Recv => "recv",
            Self::RecvProvided => "recv_provided",
            Self::Send => "send",
            Self::SendTo => "send_to",
            Self::TcpRecvMulti => "recv_multi",
            Self::UdpRecvMulti => "udp_recv_multi",
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct RioRequestDiagnostics {
    rq_raw: usize,
    data_buffer_id: usize,
    data_buffer_offset: u32,
    data_buffer_length: u32,
    addr_buffer_id: usize,
    addr_buffer_offset: u32,
    addr_buffer_length: u32,
    receive_slot_id: Option<u32>,
    receive_slot_generation: Option<u32>,
}

#[derive(Clone, Copy, Default)]
struct RioAddressBufferDiagnostic {
    id: usize,
    offset: u32,
    length: u32,
}

impl RioRequestDiagnostics {
    pub(super) fn new(rq: RioRq, data_buf: &RIO_BUF, addr: Option<&RioAddrReservation>) -> Self {
        Self::with_receive_slot(rq, data_buf, addr, None, None)
    }

    pub(super) fn with_receive_slot(
        rq: RioRq,
        data_buf: &RIO_BUF,
        addr: Option<&RioAddrReservation>,
        receive_slot_id: Option<u32>,
        receive_slot_generation: Option<u32>,
    ) -> Self {
        let addr_diagnostic = addr
            .map(|addr| RioAddressBufferDiagnostic {
                id: addr.rio_buf.BufferId as usize,
                offset: addr.rio_buf.Offset,
                length: addr.rio_buf.Length,
            })
            .unwrap_or_default();
        Self {
            rq_raw: rq.0 as usize,
            data_buffer_id: data_buf.BufferId as usize,
            data_buffer_offset: data_buf.Offset,
            data_buffer_length: data_buf.Length,
            addr_buffer_id: addr_diagnostic.id,
            addr_buffer_offset: addr_diagnostic.offset,
            addr_buffer_length: addr_diagnostic.length,
            receive_slot_id,
            receive_slot_generation,
        }
    }

    pub(crate) const fn rq_raw(self) -> usize {
        self.rq_raw
    }

    pub(crate) const fn data_buffer_id(self) -> usize {
        self.data_buffer_id
    }

    pub(crate) const fn data_buffer_offset(self) -> u32 {
        self.data_buffer_offset
    }

    pub(crate) const fn data_buffer_length(self) -> u32 {
        self.data_buffer_length
    }

    pub(crate) const fn addr_buffer_id(self) -> usize {
        self.addr_buffer_id
    }

    pub(crate) const fn addr_buffer_offset(self) -> u32 {
        self.addr_buffer_offset
    }

    pub(crate) const fn addr_buffer_length(self) -> u32 {
        self.addr_buffer_length
    }

    #[cfg(test)]
    pub(crate) const fn receive_slot_id(self) -> Option<u32> {
        self.receive_slot_id
    }

    #[cfg(test)]
    pub(crate) const fn receive_slot_generation(self) -> Option<u32> {
        self.receive_slot_generation
    }
}

pub(crate) struct RioOpRequestInit {
    pub(crate) token: OpToken,
    pub(crate) socket_inflight: SocketInflightToken,
    pub(crate) op_kind: RioOpKind,
    pub(crate) request_id: u64,
    pub(crate) addr_slot: Option<usize>,
    pub(crate) addr_generation: Option<u64>,
    pub(crate) addr: Option<RioAddrReservation>,
    pub(crate) buffer_lease: Option<RioBufferLeaseToken>,
    pub(crate) receive_slot_id: Option<u32>,
    pub(crate) receive_slot_generation: Option<u32>,
    pub(crate) diagnostics: RioRequestDiagnostics,
}

pub(crate) enum RioCompletionKind {
    Op {
        init: RioOpRequestInit,
        context: RioCompletedRequestContext,
    },
}

pub(crate) enum RioRequestContextDecode {
    Valid(Box<RioCompletionKind>),
    Malformed {
        raw: u64,
    },
    Missing {
        id: RioRequestContextId,
    },
    Stale {
        id: RioRequestContextId,
        actual_generation: Generation,
    },
}

const RIO_REQUEST_CONTEXT_MAGIC: u64 = 0xA7;
const RIO_REQUEST_CONTEXT_MAGIC_SHIFT: u32 = 56;
const RIO_REQUEST_CONTEXT_INDEX_SHIFT: u32 = 32;
const RIO_REQUEST_CONTEXT_INDEX_MASK: u64 = 0x00ff_ffff;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct RioRequestContextId {
    index: usize,
    generation: Generation,
}

impl RioRequestContextId {
    #[inline]
    pub(crate) fn new(index: usize, generation: Generation) -> Self {
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
    pub(crate) const fn generation(self) -> Generation {
        self.generation
    }

    #[inline]
    pub(crate) fn raw(self) -> u64 {
        (RIO_REQUEST_CONTEXT_MAGIC << RIO_REQUEST_CONTEXT_MAGIC_SHIFT)
            | ((self.index as u64) << RIO_REQUEST_CONTEXT_INDEX_SHIFT)
            | self.generation.get() as u64
    }

    #[inline]
    pub(crate) fn from_raw(raw: u64) -> Option<Self> {
        let magic = raw >> RIO_REQUEST_CONTEXT_MAGIC_SHIFT;
        if magic != RIO_REQUEST_CONTEXT_MAGIC {
            return None;
        }
        let index =
            ((raw >> RIO_REQUEST_CONTEXT_INDEX_SHIFT) & RIO_REQUEST_CONTEXT_INDEX_MASK) as usize;
        let generation = Generation::new(raw as u32);
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

impl RioCompletedRequestContext {
    #[inline]
    pub(crate) fn new() -> Self {
        Self
    }
}

pub(crate) struct RioPreparedRequest {
    op_kind: RioOpKind,
    request_id: u64,
    rq: RioRq,
    context: Option<RioPreparedRequestContext>,
    token: OpToken,
    socket_key: SocketKey,
    addr_slot: Option<usize>,
    addr_generation: Option<u64>,
    data_buf: RioPreparedBuffer,
    addr: Option<RioAddrReservation>,
    receive_slot_id: Option<u32>,
    receive_slot_generation: Option<u32>,
    diagnostics: RioRequestDiagnostics,
    outstanding_snapshot: usize,
}

pub(super) struct RioPreparedRequestParts {
    pub(super) op_kind: RioOpKind,
    pub(super) request_id: u64,
    pub(super) rq: RioRq,
    pub(super) context: Option<RioPreparedRequestContext>,
    pub(super) token: OpToken,
    pub(super) socket_key: SocketKey,
    pub(super) addr_slot: Option<usize>,
    pub(super) addr_generation: Option<u64>,
    pub(super) data_buf: RioPreparedBuffer,
    pub(super) addr: Option<RioAddrReservation>,
    pub(super) receive_slot_id: Option<u32>,
    pub(super) receive_slot_generation: Option<u32>,
    pub(super) diagnostics: RioRequestDiagnostics,
    pub(super) outstanding_snapshot: usize,
}

pub(crate) struct RioSubmitErrorContext<'a> {
    pub(crate) scope: &'static str,
    pub(crate) fd: IoFd,
    pub(crate) handle: BorrowedRawHandle<'a>,
    pub(crate) note: &'static str,
}

#[derive(Clone, Copy)]
pub(crate) enum RioAddressPolicy<'a> {
    None,
    SendTo {
        addr: &'a SockAddrStorage,
        addr_len: i32,
    },
    RecvMulti,
}

#[derive(Clone, Copy)]
pub(crate) struct RioSubmitPlan<'a> {
    pub(crate) fd: IoFd,
    pub(crate) handle: BorrowedRawHandle<'a>,
    pub(crate) token: OpToken,
    pub(crate) op_kind: RioOpKind,
    pub(crate) buffer_kind: RioSubmissionKind,
    pub(crate) buffer: &'a FixedBuf,
    pub(crate) buffer_offset: usize,
    pub(crate) operation: &'static str,
    pub(crate) address: RioAddressPolicy<'a>,
    pub(crate) dispatch_error: RioError,
    pub(crate) dispatch_note: &'static str,
    pub(crate) submit_scope: &'static str,
    pub(crate) submit_note: &'static str,
    pub(crate) receive_slot_id: Option<u32>,
    pub(crate) receive_slot_generation: Option<u32>,
}

impl RioPreparedRequest {
    pub(super) fn new(parts: RioPreparedRequestParts) -> Self {
        Self {
            op_kind: parts.op_kind,
            request_id: parts.request_id,
            rq: parts.rq,
            context: parts.context,
            token: parts.token,
            socket_key: parts.socket_key,
            addr_slot: parts.addr_slot,
            addr_generation: parts.addr_generation,
            data_buf: parts.data_buf,
            addr: parts.addr,
            receive_slot_id: parts.receive_slot_id,
            receive_slot_generation: parts.receive_slot_generation,
            diagnostics: parts.diagnostics,
            outstanding_snapshot: parts.outstanding_snapshot,
        }
    }

    #[inline]
    pub(super) fn take_init(&mut self, registry: &mut RioRegistry) -> Option<RioOpRequestInit> {
        let context = self.context.take()?;
        registry.take_prepared_request_init(context)
    }

    #[inline]
    pub(crate) fn socket_key(&self) -> SocketKey {
        self.socket_key
    }

    pub(crate) const fn op_kind(&self) -> RioOpKind {
        self.op_kind
    }

    pub(crate) const fn request_id(&self) -> u64 {
        self.request_id
    }

    pub(crate) const fn rq(&self) -> RioRq {
        self.rq
    }

    pub(crate) const fn data_buf(&self) -> &RioPreparedBuffer {
        &self.data_buf
    }

    pub(crate) const fn addr(&self) -> Option<&RioAddrReservation> {
        self.addr.as_ref()
    }

    pub(crate) const fn diagnostics(&self) -> RioRequestDiagnostics {
        self.diagnostics
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
            .with_ctx("fd", ctx.fd.to_string())
            .with_ctx("handle_raw", ctx.handle.raw().as_handle() as usize)
            .with_ctx("socket_raw", socket_key.as_handle() as usize)
            .with_ctx("user_data", self.token.index())
            .with_ctx("generation", self.token.generation())
            .with_ctx("rio_op_kind", self.op_kind.as_str())
            .with_ctx("rio_request_id", self.request_id)
            .with_ctx("addr_slot", self.addr_slot.unwrap_or(usize::MAX))
            .with_ctx("addr_generation", self.addr_generation.unwrap_or_default())
            .with_ctx("receive_slot_id", self.receive_slot_id.unwrap_or(u32::MAX))
            .with_ctx(
                "receive_slot_generation",
                self.receive_slot_generation.unwrap_or(u32::MAX),
            )
            .with_ctx("rq_raw", diagnostics.rq_raw)
            .with_ctx("data_buffer_id", diagnostics.data_buffer_id)
            .with_ctx("data_buffer_offset", diagnostics.data_buffer_offset)
            .with_ctx("data_buffer_length", diagnostics.data_buffer_length)
            .with_ctx("addr_buffer_id", diagnostics.addr_buffer_id)
            .with_ctx("addr_buffer_offset", diagnostics.addr_buffer_offset)
            .with_ctx("addr_buffer_length", diagnostics.addr_buffer_length)
            .with_ctx("rio_outstanding_count", self.outstanding_snapshot)
            .attach_note(ctx.note)
    }
}

#[cfg(test)]
mod tests {
    use super::super::registry::RioRegistry;
    use super::{RioCompletionKind, RioOpKind, RioOpRequestInit, RioRequestDiagnostics};
    use crate::{config::IocpHandle, rio::SocketInflightToken};
    use veloq_driver_core::{driver::OpToken, slot::Generation};
    use veloq_std::ptr;

    fn test_req_init(addr_slot: Option<usize>) -> RioOpRequestInit {
        let socket_key = IocpHandle::for_socket(ptr::null_mut());
        RioOpRequestInit {
            token: OpToken::from_registry_parts(11, Generation::new(17))
                .expect("test token should be encodable"),
            socket_inflight: SocketInflightToken::new(socket_key, 23),
            op_kind: RioOpKind::Recv,
            request_id: 23,
            addr_slot,
            addr_generation: None,
            addr: None,
            buffer_lease: None,
            receive_slot_id: None,
            receive_slot_generation: None,
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
                == OpToken::from_registry_parts(11, Generation::new(17))
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
                == OpToken::from_registry_parts(11, Generation::new(17))
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
