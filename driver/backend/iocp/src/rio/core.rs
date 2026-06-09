//! Core context encoding, registry ownership, and kernel dispatch wrappers.

mod registry;
mod request;
mod submit_ops;
mod submit_txn;

pub(crate) use registry::{RioBufferLeaseToken, RioRegistry, RioSubmissionKind};
pub(crate) use request::{
    RioAddressPolicy, RioCompletedRequestContext, RioCompletionKind, RioOpKind, RioOpRequestInit,
    RioPreparedRequestContext, RioRequestContextDecode, RioRequestContextId, RioSubmitPlan,
};
pub(crate) use submit_ops::{RioBufferId, RioCq, RioDispatch, RioKernel, RioProvider, RioRq};

use crate::error::{IocpError, iocp_report_to_event_res};
use crate::rio::RioState;
use crate::rio::error::RioError;
use diagweave::prelude::*;

#[inline]
pub(crate) fn rio_result_to_event_res(res: &crate::error::IocpDriverResult<usize>) -> i32 {
    match res {
        Ok(v) => (*v).min(i32::MAX as usize) as i32,
        Err(e) => iocp_report_to_event_res(e),
    }
}

impl RioState {
    #[inline]
    pub(crate) fn encode_req_ctx(&mut self, init: RioOpRequestInit) -> RioPreparedRequestContext {
        self.registry.alloc_request_context(init)
    }

    #[inline]
    pub(crate) fn decode_req_ctx_checked(&mut self, ctx: u64) -> RioRequestContextDecode {
        self.registry.decode_request_context_checked(ctx)
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
}
