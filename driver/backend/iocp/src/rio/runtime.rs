//! Runtime datapath: hot path buffer/pool state and UDP submissions.

pub(crate) mod control_flow;

use crate::IoFd;
use crate::config::{BorrowedRawHandle, SocketKey};
use crate::op::SubmissionResult;
use crate::rio::core::registry::RioSubmissionKind;
use crate::rio::core::{RioAddressPolicy, RioOpKind, RioSubmitPlan};
use crate::rio::error::{RioError, RioResult};
use crate::rio::{
    RioState, SocketInflightGuard, SocketInflightToken, SocketLifecycleState, SocketRuntimeState,
};
use diagweave::prelude::*;
use rustc_hash::FxHashMap;
use veloq_driver_core::driver::OpToken;
use veloq_driver_core::op::UdpRecvFrom;

pub(crate) struct RioTarget<'a> {
    pub(crate) fd: IoFd,
    pub(crate) handle: BorrowedRawHandle<'a>,
    pub(crate) token: OpToken,
    pub(crate) buf_offset: usize,
    pub(crate) operation: &'static str,
}

pub(crate) struct RioSendToArgs<'a> {
    pub(crate) fd: IoFd,
    pub(crate) handle: BorrowedRawHandle<'a>,
    pub(crate) buf: &'a veloq_buf::FixedBuf,
    pub(crate) addr_ptr: *const std::ffi::c_void,
    pub(crate) addr_len: i32,
    pub(crate) token: OpToken,
    pub(crate) buf_offset: usize,
}

pub(crate) struct RioUdpRecvFromArgs<'a> {
    pub(crate) fd: IoFd,
    pub(crate) handle: BorrowedRawHandle<'a>,
    pub(crate) recv_from_op: &'a mut UdpRecvFrom,
    pub(crate) addr_ptr: *mut std::ffi::c_void,
    pub(crate) token: OpToken,
}

impl RioState {
    #[inline]
    fn socket_runtime_mut(&mut self, actor_key: SocketKey) -> &mut SocketRuntimeState {
        self.socket_runtime.entry(actor_key).or_default()
    }

    #[inline]
    pub(crate) fn mark_socket_registered(&mut self, actor_key: SocketKey) {
        let submissions_closed = self.submissions_closed;
        let state = self.socket_runtime_mut(actor_key);
        state.lifecycle = if submissions_closed {
            SocketLifecycleState::Closing
        } else {
            SocketLifecycleState::Open
        };
    }

    #[inline]
    pub(crate) fn begin_socket_cleanup(&mut self, actor_key: SocketKey) -> bool {
        let state = self.socket_runtime_mut(actor_key);
        state.lifecycle = SocketLifecycleState::Closing;
        state.inflight == 0
    }

    #[inline]
    pub(crate) fn try_acquire_socket_inflight_token(
        &mut self,
        actor_key: SocketKey,
    ) -> RioResult<SocketInflightToken> {
        if self.submissions_closed {
            return RioError::InvalidInput
                .with_ctx("socket_raw", actor_key.as_handle() as usize)
                .with_ctx("outstanding_count", self.outstanding_count)
                .attach_note("RIO runtime is shutting down; rejecting socket submission");
        }

        let state = self.socket_runtime.get_mut(&actor_key);
        debug_assert!(
            state.is_some(),
            "socket inflight acquire without registered runtime state"
        );
        let Some(state) = state else {
            return RioError::InvalidInput
                .with_ctx("socket_raw", actor_key.as_handle() as usize)
                .attach_note("socket runtime missing while acquiring inflight slot");
        };
        if state.lifecycle == SocketLifecycleState::Closing {
            return RioError::InvalidInput
                .with_ctx("socket_raw", actor_key.as_handle() as usize)
                .with_ctx("socket_lifecycle", "closing")
                .with_ctx("socket_inflight", state.inflight)
                .attach_note("socket is closing; rejecting new socket submission");
        }
        state.inflight = state.inflight.checked_add(1).ok_or_else(|| {
            RioError::ResourceExhaustion
                .to_report()
                .with_ctx("socket_raw", actor_key.as_handle() as usize)
                .with_ctx("socket_inflight", state.inflight)
                .attach_note("socket inflight counter overflow")
        })?;
        Ok(SocketInflightToken::new(actor_key))
    }

    #[inline]
    pub(crate) fn try_acquire_socket_inflight_guard(
        &mut self,
        actor_key: SocketKey,
    ) -> RioResult<SocketInflightGuard<'_>> {
        let token = self.try_acquire_socket_inflight_token(actor_key)?;
        Ok(SocketInflightGuard {
            state: self,
            token: Some(token),
        })
    }

    #[inline]
    pub(crate) fn release_socket_inflight_token(&mut self, token: SocketInflightToken) {
        let _ = release_socket_inflight_token_from(&mut self.socket_runtime, token);
    }

    #[inline]
    pub(crate) fn socket_ready_for_cleanup(&self, actor_key: SocketKey) -> bool {
        self.socket_runtime.get(&actor_key).is_some_and(|state| {
            state.lifecycle == SocketLifecycleState::Closing && state.inflight == 0
        })
    }

    #[inline]
    pub(crate) fn forget_socket_runtime(&mut self, actor_key: SocketKey) {
        if let Some(state) = self.socket_runtime.get(&actor_key) {
            debug_assert!(
                state.lifecycle == SocketLifecycleState::Closing && state.inflight == 0,
                "forgetting socket runtime before cleanup is ready"
            );
        }
        self.socket_runtime.remove(&actor_key);
    }

    pub(crate) fn try_submit_send_to(
        &mut self,
        args: RioSendToArgs<'_>,
        registrar: &dyn veloq_buf::BufferRegistrar,
    ) -> RioResult<SubmissionResult> {
        self.try_submit_send_to_internal(args, registrar)
    }

    fn try_submit_send_to_internal(
        &mut self,
        args: RioSendToArgs<'_>,
        registrar: &dyn veloq_buf::BufferRegistrar,
    ) -> RioResult<SubmissionResult> {
        let RioSendToArgs {
            fd,
            handle,
            buf,
            addr_ptr,
            addr_len,
            token,
            buf_offset,
            ..
        } = args;
        self.submit_rio(
            RioSubmitPlan {
                fd,
                handle,
                token,
                op_kind: RioOpKind::SendTo,
                buffer_kind: RioSubmissionKind::Send,
                buffer: buf,
                buffer_offset: buf_offset,
                operation: "send_to",
                address: RioAddressPolicy::SendTo { addr_ptr, addr_len },
                dispatch_error: RioError::Internal,
                dispatch_note: "lost RIO context",
                submit_scope: "rio.runtime.try_submit_send_to_internal",
                submit_note: "RIOSendEx submit failed",
            },
            registrar,
            |kernel, request| {
                let Some(addr) = request.addr.as_ref() else {
                    return RioError::Internal.attach_note("RIO send_to missing prepared address");
                };
                kernel.submit_send_ex(
                    request.rq,
                    &request.data_buf.rio_buf,
                    &addr.rio_buf,
                    request.as_request_context(),
                )
            },
        )
    }

    pub(crate) fn try_submit_recv_from(
        &mut self,
        args: RioUdpRecvFromArgs<'_>,
        registrar: &dyn veloq_buf::BufferRegistrar,
    ) -> RioResult<SubmissionResult> {
        self.try_submit_recv_from_internal(args, registrar)
    }

    fn try_submit_recv_from_internal(
        &mut self,
        args: RioUdpRecvFromArgs<'_>,
        registrar: &dyn veloq_buf::BufferRegistrar,
    ) -> RioResult<SubmissionResult> {
        let RioUdpRecvFromArgs {
            fd,
            handle,
            recv_from_op,
            addr_ptr,
            token,
        } = args;
        let buf_offset = recv_from_op.buf_offset;
        self.submit_rio(
            RioSubmitPlan {
                fd,
                handle,
                token,
                op_kind: RioOpKind::RecvFrom,
                buffer_kind: RioSubmissionKind::Recv,
                buffer: &recv_from_op.buf,
                buffer_offset: buf_offset,
                operation: "udp_recv_from",
                address: RioAddressPolicy::RecvFrom { addr_ptr },
                dispatch_error: RioError::Internal,
                dispatch_note: "lost RIO context",
                submit_scope: "rio.runtime.try_submit_recv_from_internal",
                submit_note: "RIOReceiveEx submit failed",
            },
            registrar,
            |kernel, request| {
                let Some(addr) = request.addr.as_ref() else {
                    return RioError::Internal
                        .attach_note("RIO recv_from missing prepared address");
                };
                kernel.submit_receive_ex(
                    request.rq,
                    &request.data_buf.rio_buf,
                    &addr.rio_buf,
                    request.as_request_context(),
                )
            },
        )
    }
}

impl SocketInflightGuard<'_> {
    #[inline]
    pub(crate) fn commit(mut self) -> SocketInflightToken {
        self.token
            .take()
            .expect("socket inflight guard already committed")
    }
}

impl Drop for SocketInflightGuard<'_> {
    fn drop(&mut self) {
        if let Some(token) = self.token.take() {
            self.state.release_socket_inflight_token(token);
        }
    }
}

pub(crate) fn release_socket_inflight_token_from(
    socket_runtime: &mut FxHashMap<SocketKey, SocketRuntimeState>,
    token: SocketInflightToken,
) -> bool {
    let socket_key = token.socket_key();
    let state = socket_runtime.get_mut(&socket_key);
    debug_assert!(
        state.is_some(),
        "socket inflight release without registered runtime state"
    );
    let Some(state) = state else {
        tracing::error!(
            socket_raw = socket_key.as_handle() as usize,
            "socket inflight release without registered runtime state"
        );
        return false;
    };
    debug_assert!(state.inflight > 0, "socket inflight counter underflow");
    if state.inflight == 0 {
        tracing::error!(
            socket_raw = socket_key.as_handle() as usize,
            "socket inflight counter underflow"
        );
        return false;
    }
    state.inflight -= 1;
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::BufferRegistrationMode;
    use crate::config::IocpHandle;
    use crate::rio::core::registry::RioRegistry;
    use crate::rio::core::submit_ops::{RioKernel, RioRq};
    use crate::rio::runtime::control_flow::RioSocketActor;

    fn test_state() -> RioState {
        RioState {
            kernel: RioKernel::noop(),
            registry: RioRegistry::new(32, 1),
            registration_mode: BufferRegistrationMode::default(),
            submissions_closed: false,
            actors: slotmap::SlotMap::with_key(),
            actor_by_handle: FxHashMap::default(),
            socket_runtime: FxHashMap::default(),
            outstanding_count: 0,
            next_request_id: 0,
            deferred_payloads: Vec::new(),
            diagnostics: crate::driver::IocpDriverCompletionDiagnostics::default(),
        }
    }

    fn test_socket_key() -> SocketKey {
        IocpHandle::for_socket(std::ptr::null_mut())
    }

    #[test]
    fn socket_inflight_token_acquire_and_release_balances_count() {
        let mut state = test_state();
        let key = test_socket_key();
        state.mark_socket_registered(key);

        let token = state
            .try_acquire_socket_inflight_token(key)
            .expect("registered open socket should acquire inflight token");
        assert_eq!(state.socket_runtime.get(&key).unwrap().inflight, 1);

        state.release_socket_inflight_token(token);
        assert_eq!(state.socket_runtime.get(&key).unwrap().inflight, 0);
    }

    #[test]
    fn socket_inflight_guard_drop_rolls_back_count() {
        let mut state = test_state();
        let key = test_socket_key();
        state.mark_socket_registered(key);

        {
            let _guard = state
                .try_acquire_socket_inflight_guard(key)
                .expect("registered open socket should acquire inflight guard");
        }

        assert_eq!(state.socket_runtime.get(&key).unwrap().inflight, 0);
    }

    #[test]
    fn closing_socket_rejects_new_inflight_acquire() {
        let mut state = test_state();
        let key = test_socket_key();
        state.mark_socket_registered(key);

        assert!(state.begin_socket_cleanup(key));
        assert!(state.try_acquire_socket_inflight_token(key).is_err());
    }

    #[test]
    fn stop_accepting_new_submissions_preserves_socket_runtime_until_drain() {
        let mut state = test_state();
        let key = test_socket_key();
        state.mark_socket_registered(key);
        let actor = state.actors.insert(RioSocketActor::new(RioRq(1 as _)));
        state.actor_by_handle.insert(key, actor);

        let token = state
            .try_acquire_socket_inflight_token(key)
            .expect("registered open socket should acquire inflight token");
        state.stop_accepting_new_submissions();

        let socket_state = state
            .socket_runtime
            .get(&key)
            .expect("socket runtime must survive shutdown gate");
        assert_eq!(socket_state.lifecycle, SocketLifecycleState::Closing);
        assert_eq!(socket_state.inflight, 1);
        assert!(state.actor_by_handle.is_empty());
        assert_eq!(state.actors.len(), 1);
        assert!(state.try_acquire_socket_inflight_token(key).is_err());

        state.release_socket_inflight_token(token);
        assert_eq!(state.socket_runtime.get(&key).unwrap().inflight, 0);
    }

    #[test]
    fn forget_runtime_after_drain_clears_runtime_state() {
        let mut state = test_state();
        let key = test_socket_key();
        state.mark_socket_registered(key);
        let actor = state.actors.insert(RioSocketActor::new(RioRq(1 as _)));
        state.actor_by_handle.insert(key, actor);
        state.stop_accepting_new_submissions();

        state.forget_runtime_after_drain();

        assert!(state.actors.is_empty());
        assert!(state.actor_by_handle.is_empty());
        assert!(state.socket_runtime.is_empty());
    }

    #[test]
    fn missing_socket_runtime_is_not_ready_for_cleanup() {
        let mut state = test_state();
        let key = test_socket_key();

        assert!(!state.socket_ready_for_cleanup(key));
        state.mark_socket_registered(key);
        assert!(!state.socket_ready_for_cleanup(key));
        assert!(state.begin_socket_cleanup(key));
        assert!(state.socket_ready_for_cleanup(key));
    }
}
