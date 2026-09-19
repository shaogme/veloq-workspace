pub(crate) mod control_flow;

use crate::{
    IoFd,
    config::{BorrowedRawHandle, SocketKey},
    op::SubmissionResult,
    rio::{
        RioState, SocketInflightGuard, SocketInflightIdentity, SocketInflightToken,
        SocketLifecycleState, SocketRuntimeState,
        core::{RioAddressPolicy, RioOpKind, RioSubmissionKind, RioSubmitPlan},
        error::{RioError, RioResult},
    },
};

#[cfg(test)]
use crate::driver::IocpDriverCompletionDiagnostics;

use diagweave::prelude::*;
use veloq_buf::{BufferRegistrar, FixedBuf};
use veloq_driver_core::driver::OpToken;
use veloq_std::{collections::FastHashMap, ffi::c_void};
use windows_sys::Win32::Networking::WinSock::{SD_RECEIVE, SOCKET_ERROR, shutdown};

pub(crate) use control_flow::RioSocketActor;

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
    pub(crate) buf: &'a FixedBuf,
    pub(crate) addr_ptr: *const c_void,
    pub(crate) addr_len: i32,
    pub(crate) token: OpToken,
    pub(crate) buf_offset: usize,
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
    pub(crate) fn begin_socket_cleanup(&mut self, actor_key: SocketKey) -> RioResult<bool> {
        self.socket_runtime_mut(actor_key).lifecycle = SocketLifecycleState::Closing;
        let has_receive_inflight = self
            .socket_runtime
            .get(&actor_key)
            .is_some_and(|state| state.receive_inflight != 0);
        if has_receive_inflight {
            self.request_socket_receive_shutdown_for_cleanup(actor_key)?;
        }
        Ok(self
            .socket_runtime
            .get(&actor_key)
            .is_some_and(|state| state.inflight == 0))
    }

    pub(crate) fn request_socket_receive_shutdown_for_cleanup(
        &mut self,
        actor_key: SocketKey,
    ) -> RioResult<()> {
        {
            let state = self.socket_runtime_mut(actor_key);
            if state.receive_inflight == 0 {
                state.socket_receive_shutdown_pending = false;
                return Ok(());
            }
            if state.socket_receive_shutdown_requested {
                state.socket_receive_shutdown_pending = false;
                return Ok(());
            }
            if state.send_inflight != 0 {
                state.socket_receive_shutdown_pending = true;
                return Ok(());
            }
        }

        // This is a socket-lifecycle cleanup action, not a user cancellation mechanism. User
        // cancellation only changes the logical slot or receive-pump state and never reaches
        // this helper; the request context, leases, and socket token remain until RIO dequeue.
        // SAFETY: the actor key is the live socket handle retained by the registered socket
        // runtime. Shutting down only the receive direction leaves the connected socket open for
        // its send side while the pending RIO receive is completed by Winsock.
        let result = unsafe { shutdown(actor_key.as_socket(), SD_RECEIVE) };
        if result == SOCKET_ERROR {
            return Err(RioState::last_wsa_report(
                RioError::InvalidInput,
                "rio.runtime.shutdown_receive_for_cleanup",
            ));
        }
        let state = self.socket_runtime_mut(actor_key);
        state.socket_receive_shutdown_requested = true;
        state.socket_receive_shutdown_pending = false;
        Ok(())
    }

    pub(crate) fn activate_receive_pump(&mut self, actor_key: SocketKey, token: OpToken) {
        let state = self.socket_runtime_mut(actor_key);
        state.receive_pump_token = Some(token);
    }

    pub(crate) fn finish_receive_pump(&mut self, actor_key: SocketKey, token: OpToken) {
        if let Some(state) = self.socket_runtime.get_mut(&actor_key)
            && state.receive_pump_token == Some(token)
        {
            state.receive_pump_token = None;
        }
    }

    pub(crate) fn record_request_submitted(
        &mut self,
        op_kind: RioOpKind,
        actor_key: SocketKey,
    ) -> RioResult<()> {
        let state = self.socket_runtime_mut(actor_key);
        let counter = match op_kind {
            RioOpKind::Recv
            | RioOpKind::RecvProvided
            | RioOpKind::TcpRecvMulti
            | RioOpKind::UdpRecvMulti => &mut state.receive_inflight,
            RioOpKind::Send | RioOpKind::SendTo => &mut state.send_inflight,
        };
        *counter = counter.checked_add(1).ok_or_else(|| {
            RioError::ResourceExhaustion
                .to_report()
                .with_ctx("socket_raw", actor_key.as_handle() as usize)
                .with_ctx("rio_op_kind", op_kind.as_str())
                .attach_note("RIO request class counter overflow")
        })?;
        if matches!(op_kind, RioOpKind::TcpRecvMulti | RioOpKind::UdpRecvMulti) {
            state.receive_pump_inflight =
                state.receive_pump_inflight.checked_add(1).ok_or_else(|| {
                    RioError::ResourceExhaustion
                        .to_report()
                        .with_ctx("socket_raw", actor_key.as_handle() as usize)
                        .attach_note("RIO receive pump counter overflow")
                })?;
        }
        Ok(())
    }

    pub(crate) fn release_request_inflight(
        &mut self,
        op_kind: RioOpKind,
        token: SocketInflightToken,
    ) -> RioResult<()> {
        self.release_request_inflight_identity(op_kind, token.identity())
    }

    pub(crate) fn release_request_inflight_identity(
        &mut self,
        op_kind: RioOpKind,
        identity: SocketInflightIdentity,
    ) -> RioResult<()> {
        let actor_key = identity.socket_key();
        let state = self.socket_runtime.get(&actor_key).ok_or_else(|| {
            RioError::InvalidInput
                .to_report()
                .with_ctx("socket_raw", actor_key.as_handle() as usize)
                .with_ctx("request_id", identity.request_id())
                .attach_note("RIO request release without socket runtime")
        })?;
        if !state.active_tokens.contains(&identity.request_id()) {
            return RioError::Internal
                .with_ctx("socket_raw", actor_key.as_handle() as usize)
                .with_ctx("request_id", identity.request_id())
                .attach_note("RIO request release received a stale socket inflight identity");
        }
        let retry_receive_shutdown = {
            let state = self.socket_runtime.get_mut(&actor_key).ok_or_else(|| {
                RioError::InvalidInput
                    .to_report()
                    .with_ctx("socket_raw", actor_key.as_handle() as usize)
                    .attach_note("RIO request release without socket runtime")
            })?;
            match op_kind {
                RioOpKind::Recv
                | RioOpKind::RecvProvided
                | RioOpKind::TcpRecvMulti
                | RioOpKind::UdpRecvMulti => {
                    if state.receive_inflight == 0 {
                        return RioError::Internal
                            .with_ctx("socket_raw", actor_key.as_handle() as usize)
                            .with_ctx("rio_op_kind", op_kind.as_str())
                            .attach_note("RIO request class counter underflow");
                    }
                    state.receive_inflight -= 1;
                    false
                }
                RioOpKind::Send | RioOpKind::SendTo => {
                    if state.send_inflight == 0 {
                        return RioError::Internal
                            .with_ctx("socket_raw", actor_key.as_handle() as usize)
                            .with_ctx("rio_op_kind", op_kind.as_str())
                            .attach_note("RIO request class counter underflow");
                    }
                    state.send_inflight -= 1;
                    state.send_inflight == 0 && state.socket_receive_shutdown_pending
                }
            }
        };
        if matches!(op_kind, RioOpKind::TcpRecvMulti | RioOpKind::UdpRecvMulti) {
            let state = self.socket_runtime.get_mut(&actor_key).ok_or_else(|| {
                RioError::InvalidInput
                    .to_report()
                    .with_ctx("socket_raw", actor_key.as_handle() as usize)
                    .attach_note("RIO receive release without socket runtime")
            })?;
            if state.receive_pump_inflight == 0 {
                return RioError::Internal
                    .with_ctx("socket_raw", actor_key.as_handle() as usize)
                    .attach_note("RIO receive pump counter underflow");
            }
            state.receive_pump_inflight -= 1;
        }
        release_socket_inflight_identity_from(&mut self.socket_runtime, identity)?;
        if retry_receive_shutdown {
            self.request_socket_receive_shutdown_for_cleanup(actor_key)?;
        }
        Ok(())
    }

    #[inline]
    pub(crate) fn try_acquire_socket_inflight_token(
        &mut self,
        actor_key: SocketKey,
    ) -> RioResult<SocketInflightToken> {
        if self.submissions_closed {
            return RioError::InvalidInput
                .with_ctx("socket_raw", actor_key.as_handle() as usize)
                .with_ctx("rio_outstanding_count", self.rio_outstanding_count)
                .attach_note("RIO runtime is shutting down; rejecting socket submission");
        }

        let Some(state) = self.socket_runtime.get(&actor_key) else {
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
        let next_inflight = state.inflight.checked_add(1).ok_or_else(|| {
            RioError::ResourceExhaustion
                .to_report()
                .with_ctx("socket_raw", actor_key.as_handle() as usize)
                .with_ctx("socket_inflight", state.inflight)
                .attach_note("socket inflight counter overflow")
        })?;
        let request_id = self.next_request_id_checked()?;
        let state = self
            .socket_runtime
            .get_mut(&actor_key)
            .expect("socket runtime disappeared during inflight acquisition");
        if !state.active_tokens.insert(request_id) {
            return RioError::Internal
                .with_ctx("socket_raw", actor_key.as_handle() as usize)
                .with_ctx("request_id", request_id)
                .attach_note("socket inflight request identity was allocated twice");
        }
        state.inflight = next_inflight;
        Ok(SocketInflightToken::new(actor_key, request_id))
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
    pub(crate) fn release_socket_inflight_token(
        &mut self,
        token: SocketInflightToken,
    ) -> RioResult<()> {
        release_socket_inflight_token_from(&mut self.socket_runtime, token)
    }

    #[inline]
    pub(crate) fn socket_ready_for_cleanup(&self, actor_key: SocketKey) -> bool {
        self.socket_runtime.get(&actor_key).is_some_and(|state| {
            state.lifecycle == SocketLifecycleState::Closing
                && state.inflight == 0
                && state.active_tokens.is_empty()
        })
    }

    #[inline]
    pub(crate) fn forget_socket_runtime(&mut self, actor_key: SocketKey) -> RioResult<()> {
        if !self.socket_ready_for_cleanup(actor_key) {
            return RioError::InvalidInput
                .with_ctx("socket_raw", actor_key.as_handle() as usize)
                .attach_note("forgetting socket runtime before cleanup is ready");
        }
        self.socket_runtime.remove(&actor_key);
        Ok(())
    }

    #[inline]
    pub(crate) fn socket_inflight_count(&self) -> u64 {
        self.socket_runtime
            .values()
            .map(|state| u64::from(state.inflight))
            .sum()
    }

    #[inline]
    pub(crate) fn has_socket_inflight(&self) -> bool {
        self.socket_runtime
            .values()
            .any(|state| state.inflight != 0 || !state.active_tokens.is_empty())
    }

    #[inline]
    pub(crate) fn runtime_ready_for_forget(&self) -> bool {
        self.rio_outstanding_count == 0 && !self.has_socket_inflight()
    }

    fn next_request_id_checked(&mut self) -> RioResult<u64> {
        let next = self.next_request_id.checked_add(1).ok_or_else(|| {
            RioError::ResourceExhaustion
                .to_report()
                .attach_note("socket inflight request identity exhausted")
        })?;
        self.next_request_id = next;
        Ok(next)
    }

    pub(crate) fn try_submit_send_to(
        &mut self,
        args: RioSendToArgs<'_>,
        registrar: &dyn BufferRegistrar,
    ) -> RioResult<SubmissionResult> {
        self.try_submit_send_to_internal(args, registrar)
    }

    fn try_submit_send_to_internal(
        &mut self,
        args: RioSendToArgs<'_>,
        registrar: &dyn BufferRegistrar,
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
                receive_slot_id: None,
                receive_slot_generation: None,
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
        if let Some(token) = self.token.take()
            && let Err(e) = self.state.release_socket_inflight_token(token)
        {
            tracing::error!(error = ?e, "failed to release socket inflight token in guard drop");
        }
    }
}

pub(crate) fn release_socket_inflight_token_from(
    socket_runtime: &mut FastHashMap<SocketKey, SocketRuntimeState>,
    token: SocketInflightToken,
) -> RioResult<()> {
    release_socket_inflight_identity_from(socket_runtime, token.identity())
}

pub(crate) fn release_socket_inflight_identity_from(
    socket_runtime: &mut FastHashMap<SocketKey, SocketRuntimeState>,
    identity: SocketInflightIdentity,
) -> RioResult<()> {
    let socket_key = identity.socket_key();
    let state = socket_runtime.get_mut(&socket_key).ok_or_else(|| {
        RioError::InvalidInput
            .to_report()
            .with_ctx("socket_raw", socket_key.as_handle() as usize)
            .attach_note("socket inflight release without registered runtime state")
    })?;

    if !state.active_tokens.contains(&identity.request_id()) {
        return RioError::Internal
            .with_ctx("socket_raw", socket_key.as_handle() as usize)
            .with_ctx("request_id", identity.request_id())
            .attach_note("socket inflight token is stale, duplicated, or already released");
    }
    if state.inflight == 0 {
        return RioError::Internal
            .with_ctx("socket_raw", socket_key.as_handle() as usize)
            .with_ctx("request_id", identity.request_id())
            .attach_note("socket inflight counter underflow");
    }
    state.active_tokens.remove(&identity.request_id());
    state.inflight -= 1;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        BufferRegistrationMode,
        config::IocpHandle,
        rio::core::{RioKernel, RioRegistry, RioRq},
    };
    use veloq_driver_core::slot::Generation;
    use veloq_std::{collections::FastHashMap, ptr::null_mut, vec::Vec};

    fn test_state() -> RioState {
        RioState {
            kernel: RioKernel::noop(),
            registry: RioRegistry::new(32, 1),
            registration_mode: BufferRegistrationMode::default(),
            submissions_closed: false,
            actors: slotmap::SlotMap::with_key(),
            actor_by_handle: FastHashMap::default(),
            socket_runtime: FastHashMap::default(),
            rio_outstanding_count: 0,
            next_request_id: 0,
            deferred_kernel_ops: Vec::new(),
            deferred_payloads: Vec::new(),
            diagnostics: IocpDriverCompletionDiagnostics::default(),
            cq_armed: true,
        }
    }

    fn test_socket_key() -> SocketKey {
        IocpHandle::for_socket(null_mut())
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

        state.release_socket_inflight_token(token).unwrap();
        let socket_state = state.socket_runtime.get(&key).unwrap();
        assert_eq!(socket_state.inflight, 0);
        assert!(socket_state.active_tokens.is_empty());
    }

    #[test]
    fn duplicate_token_release_does_not_decrement_new_request() {
        let mut state = test_state();
        let key = test_socket_key();
        state.mark_socket_registered(key);

        let first = state
            .try_acquire_socket_inflight_token(key)
            .expect("first request should acquire an inflight token");
        let first_id = first.request_id();
        state
            .release_socket_inflight_token(first)
            .expect("first request should release its token");
        let second = state
            .try_acquire_socket_inflight_token(key)
            .expect("second request should acquire an inflight token");

        let stale = SocketInflightToken::new(key, first_id);
        assert!(state.release_socket_inflight_token(stale).is_err());
        assert_eq!(state.socket_runtime.get(&key).unwrap().inflight, 1);
        assert!(
            state
                .socket_runtime
                .get(&key)
                .unwrap()
                .active_tokens
                .contains(&second.request_id())
        );

        state
            .release_socket_inflight_token(second)
            .expect("second request should release its token");
    }

    #[test]
    fn runtime_is_not_ready_while_socket_token_is_active() {
        let mut state = test_state();
        let key = test_socket_key();
        state.mark_socket_registered(key);
        let token = state
            .try_acquire_socket_inflight_token(key)
            .expect("request should acquire an inflight token");

        assert!(!state.runtime_ready_for_forget());
        assert!(state.forget_runtime_after_drain().is_err());
        assert!(state.socket_runtime.contains_key(&key));

        state
            .release_socket_inflight_token(token)
            .expect("request should release its token");
        assert!(state.runtime_ready_for_forget());
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

        assert!(state.begin_socket_cleanup(key).unwrap());
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

        state.release_socket_inflight_token(token).unwrap();
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

        state
            .forget_runtime_after_drain()
            .expect("empty test runtime should be ready for cleanup");

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
        assert!(state.begin_socket_cleanup(key).unwrap());
        assert!(state.socket_ready_for_cleanup(key));
    }

    #[test]
    fn socket_cleanup_retries_receive_shutdown_after_send_drains() {
        let mut state = test_state();
        let key = test_socket_key();
        state.mark_socket_registered(key);

        let receive_token = state
            .try_acquire_socket_inflight_token(key)
            .expect("receive request should acquire an inflight token");
        state
            .record_request_submitted(RioOpKind::Recv, key)
            .expect("receive request should be tracked");
        let send_token = state
            .try_acquire_socket_inflight_token(key)
            .expect("send request should acquire an inflight token");
        state
            .record_request_submitted(RioOpKind::Send, key)
            .expect("send request should be tracked");

        assert!(
            !state
                .begin_socket_cleanup(key)
                .expect("cleanup should defer while requests are in flight")
        );
        let socket_state = state.socket_runtime.get(&key).unwrap();
        assert!(socket_state.socket_receive_shutdown_pending);
        assert!(!socket_state.socket_receive_shutdown_requested);

        state
            .release_request_inflight(RioOpKind::Recv, receive_token)
            .expect("receive completion should release its request");
        state
            .release_request_inflight(RioOpKind::Send, send_token)
            .expect("send completion should retry receive cleanup");

        let socket_state = state.socket_runtime.get(&key).unwrap();
        assert_eq!(socket_state.inflight, 0);
        assert!(!socket_state.socket_receive_shutdown_pending);
    }

    #[test]
    fn finish_receive_pump_preserves_socket_shutdown_state() {
        let mut state = test_state();
        let key = test_socket_key();
        let token = OpToken::from_registry_parts(0, Generation::new(1)).unwrap();
        state.mark_socket_registered(key);
        state.activate_receive_pump(key, token);
        {
            let socket_state = state.socket_runtime.get_mut(&key).unwrap();
            socket_state.socket_receive_shutdown_requested = true;
            socket_state.socket_receive_shutdown_pending = true;
        }

        state.finish_receive_pump(key, token);

        let socket_state = state.socket_runtime.get(&key).unwrap();
        assert_eq!(socket_state.receive_pump_token, None);
        assert!(socket_state.socket_receive_shutdown_requested);
        assert!(socket_state.socket_receive_shutdown_pending);
    }
}
