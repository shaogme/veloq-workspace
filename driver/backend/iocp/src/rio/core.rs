//! Core context encoding, registry ownership, and kernel dispatch wrappers.

pub(crate) mod registry;
pub(crate) mod submit_ops;

use crate::error::{IocpError, iocp_report_to_event_res};
use crate::rio::RioState;
use crate::rio::core::registry::RioHeapLeaseToken;
use crate::rio::error::RioError;
use diagweave::prelude::*;

#[derive(Clone, Copy)]
pub(crate) enum RioCompletionKind {
    Op {
        user_data: usize,
        generation: u32,
        addr_slot: Option<usize>,
        heap_lease: Option<RioHeapLeaseToken>,
        ctx_ptr: *mut RioOpRequestContext,
    },
}

#[repr(C)]
pub(crate) struct RioOpRequestContext {
    pub(crate) user_data: usize,
    pub(crate) generation: u32,
    pub(crate) addr_slot: usize,
    pub(crate) heap_lease: Option<RioHeapLeaseToken>,
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

#[inline]
pub(crate) fn rio_result_to_event_res(res: &crate::error::IocpDriverResult<usize>) -> i32 {
    match res {
        Ok(v) => (*v).min(i32::MAX as usize) as i32,
        Err(e) => iocp_report_to_event_res(e),
    }
}

impl RioState {
    #[inline]
    pub(crate) fn encode_req_ctx(
        user_data: usize,
        generation: u32,
        addr_slot: Option<usize>,
        heap_lease: Option<RioHeapLeaseToken>,
    ) -> *const std::ffi::c_void {
        let ctx = Box::new(RioOpRequestContext {
            user_data,
            generation,
            addr_slot: addr_slot.unwrap_or(usize::MAX),
            heap_lease,
        });
        Box::into_raw(ctx).cast::<std::ffi::c_void>()
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
            addr_slot: (op_ctx.addr_slot != usize::MAX).then_some(op_ctx.addr_slot),
            heap_lease: op_ctx.heap_lease,
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

    #[test]
    fn op_ctx_roundtrip_decode_and_free() {
        let ptr = RioState::encode_req_ctx(11, 17, None, None);
        let decoded = RioState::decode_req_ctx(ptr as u64);
        assert!(matches!(
            decoded,
            Some(RioCompletionKind::Op {
                user_data: 11,
                generation: 17,
                addr_slot: None,
                ..
            })
        ));
        RioState::free_op_req_ctx(ptr as u64);
    }

    #[test]
    fn op_ctx_with_addr_roundtrip_decode_and_free() {
        let ptr = RioState::encode_req_ctx(11, 17, Some(3), None);
        let decoded = RioState::decode_req_ctx(ptr as u64);
        assert!(matches!(
            decoded,
            Some(RioCompletionKind::Op {
                user_data: 11,
                generation: 17,
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
