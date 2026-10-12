mod completion;

use crate::{
    config::SocketKey,
    rio::{
        core::{RioKernel, RioOpKind, RioRq},
        error::{RioError, RioResult},
    },
};

use diagweave::prelude::*;
use slotmap::{SlotMap, new_key_type};
use veloq_driver_core::driver::OpToken;
use veloq_std::collections::{FastHashMap, FastHashSet};
use windows_sys::Win32::Networking::WinSock::{SD_RECEIVE, SOCKET_ERROR, shutdown};

pub(crate) use completion::{CompletionPollArgs, RioCompletionAnomaly, RioCompletionService};

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct SocketInflightToken {
    socket_key: SocketKey,
    request_id: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct SocketInflightIdentity {
    socket_key: SocketKey,
    request_id: u64,
}

#[must_use = "dropping a SocketInflightGuard releases the acquired socket inflight slot"]
pub(crate) struct SocketInflightGuard<'a> {
    runtime: &'a mut RioRuntime,
    token: Option<SocketInflightToken>,
}

new_key_type! {
    pub(crate) struct ActorKey;
}

pub(crate) struct RioSocketActor {
    rq: RioRq,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SocketLifecycleState {
    Open,
    Closing,
}

#[derive(Debug, PartialEq, Eq)]
struct SocketRuntimeState {
    lifecycle: SocketLifecycleState,
    inflight: u32,
    active_tokens: FastHashSet<u64>,
    send_inflight: u32,
    receive_inflight: u32,
    receive_pump_inflight: u32,
    receive_pump_token: Option<OpToken>,
    socket_receive_shutdown_requested: bool,
    socket_receive_shutdown_pending: bool,
}

impl Default for SocketRuntimeState {
    fn default() -> Self {
        Self {
            lifecycle: SocketLifecycleState::Open,
            inflight: 0,
            active_tokens: FastHashSet::default(),
            send_inflight: 0,
            receive_inflight: 0,
            receive_pump_inflight: 0,
            receive_pump_token: None,
            socket_receive_shutdown_requested: false,
            socket_receive_shutdown_pending: false,
        }
    }
}

pub(crate) struct RioRuntime {
    submissions_closed: bool,
    actors: SlotMap<ActorKey, RioSocketActor>,
    actor_by_handle: FastHashMap<SocketKey, ActorKey>,
    socket_runtime: FastHashMap<SocketKey, SocketRuntimeState>,
    next_request_id: u64,
}

impl RioRuntime {
    pub(super) fn new() -> Self {
        Self {
            submissions_closed: false,
            actors: SlotMap::with_key(),
            actor_by_handle: FastHashMap::default(),
            socket_runtime: FastHashMap::default(),
            next_request_id: 0,
        }
    }

    pub(super) fn closed_empty() -> Self {
        Self {
            submissions_closed: true,
            ..Self::new()
        }
    }

    fn socket_runtime_mut(&mut self, actor_key: SocketKey) -> &mut SocketRuntimeState {
        self.socket_runtime.entry(actor_key).or_default()
    }

    pub(super) fn mark_socket_registered(&mut self, actor_key: SocketKey) {
        let lifecycle = if self.submissions_closed {
            SocketLifecycleState::Closing
        } else {
            SocketLifecycleState::Open
        };
        self.socket_runtime_mut(actor_key).lifecycle = lifecycle;
    }

    pub(super) fn stop_accepting_new_submissions(&mut self) {
        self.submissions_closed = true;
        self.actor_by_handle.clear();
        for socket_state in self.socket_runtime.values_mut() {
            socket_state.lifecycle = SocketLifecycleState::Closing;
        }
    }

    pub(super) fn forget_after_drain(&mut self, rio_outstanding_count: usize) -> RioResult<()> {
        if !self.runtime_ready_for_forget(rio_outstanding_count) {
            return RioError::InvalidInput
                .with_ctx("rio_outstanding_count", rio_outstanding_count)
                .with_ctx("socket_inflight", self.socket_inflight_count())
                .attach_note("forgetting RIO runtime before all requests drained");
        }
        self.actors.clear();
        self.actor_by_handle.clear();
        self.socket_runtime.clear();
        Ok(())
    }

    pub(super) fn begin_socket_cleanup(&mut self, actor_key: SocketKey) -> RioResult<bool> {
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

    pub(super) fn request_socket_receive_shutdown_for_cleanup(
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

        // SAFETY: The registered runtime retains this handle; receive shutdown leaves sends open. / 安全性：已注册 runtime 持有此句柄；关闭接收方向不会关闭发送方向。
        let result = unsafe { shutdown(actor_key.as_socket(), SD_RECEIVE) };
        if result == SOCKET_ERROR {
            return Err(RioKernel::last_wsa_report(
                RioError::InvalidInput,
                "rio.runtime.shutdown_receive_for_cleanup",
            ));
        }
        let state = self.socket_runtime_mut(actor_key);
        state.socket_receive_shutdown_requested = true;
        state.socket_receive_shutdown_pending = false;
        Ok(())
    }

    pub(super) fn activate_receive_pump(&mut self, actor_key: SocketKey, token: OpToken) {
        self.socket_runtime_mut(actor_key).receive_pump_token = Some(token);
    }

    pub(super) fn finish_receive_pump(&mut self, actor_key: SocketKey, token: OpToken) {
        if let Some(state) = self.socket_runtime.get_mut(&actor_key)
            && state.receive_pump_token == Some(token)
        {
            state.receive_pump_token = None;
        }
    }

    pub(super) fn record_request_submitted(
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

    pub(super) fn release_request_inflight(
        &mut self,
        op_kind: RioOpKind,
        token: SocketInflightToken,
    ) -> RioResult<()> {
        self.release_request_inflight_identity(op_kind, token.identity())
    }

    pub(super) fn release_request_inflight_identity(
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
        identity.release_from(&mut self.socket_runtime)?;
        if retry_receive_shutdown {
            self.request_socket_receive_shutdown_for_cleanup(actor_key)?;
        }
        Ok(())
    }

    pub(super) fn try_acquire_socket_inflight_token(
        &mut self,
        actor_key: SocketKey,
        rio_outstanding_count: usize,
    ) -> RioResult<SocketInflightToken> {
        if self.submissions_closed {
            return RioError::InvalidInput
                .with_ctx("socket_raw", actor_key.as_handle() as usize)
                .with_ctx("rio_outstanding_count", rio_outstanding_count)
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

    pub(super) fn try_acquire_socket_inflight_guard(
        &mut self,
        actor_key: SocketKey,
        rio_outstanding_count: usize,
    ) -> RioResult<SocketInflightGuard<'_>> {
        let token = self.try_acquire_socket_inflight_token(actor_key, rio_outstanding_count)?;
        Ok(SocketInflightGuard {
            runtime: self,
            token: Some(token),
        })
    }

    pub(super) fn release_socket_inflight_token(
        &mut self,
        token: SocketInflightToken,
    ) -> RioResult<()> {
        token.release_from(&mut self.socket_runtime)
    }

    pub(super) fn socket_ready_for_cleanup(&self, actor_key: SocketKey) -> bool {
        self.socket_runtime.get(&actor_key).is_some_and(|state| {
            state.lifecycle == SocketLifecycleState::Closing
                && state.inflight == 0
                && state.active_tokens.is_empty()
        })
    }

    pub(super) fn forget_socket_runtime(&mut self, actor_key: SocketKey) -> RioResult<()> {
        if !self.socket_ready_for_cleanup(actor_key) {
            return RioError::InvalidInput
                .with_ctx("socket_raw", actor_key.as_handle() as usize)
                .attach_note("forgetting socket runtime before cleanup is ready");
        }
        self.socket_runtime.remove(&actor_key);
        Ok(())
    }

    pub(super) fn socket_inflight_count(&self) -> u64 {
        self.socket_runtime
            .values()
            .map(|state| u64::from(state.inflight))
            .sum()
    }

    pub(super) fn has_socket_inflight(&self) -> bool {
        self.socket_runtime
            .values()
            .any(|state| state.inflight != 0 || !state.active_tokens.is_empty())
    }

    pub(super) fn runtime_ready_for_forget(&self, rio_outstanding_count: usize) -> bool {
        rio_outstanding_count == 0 && !self.has_socket_inflight()
    }

    pub(super) fn is_submissions_closed(&self) -> bool {
        self.submissions_closed
    }

    pub(super) fn actor_mut_for_handle(
        &mut self,
        socket_key: SocketKey,
    ) -> Option<&mut RioSocketActor> {
        let actor_key = self.actor_by_handle.get(&socket_key).copied()?;
        self.actors.get_mut(actor_key)
    }

    pub(super) fn actor_index_contains(&self, socket_key: SocketKey) -> bool {
        self.actor_by_handle.contains_key(&socket_key)
    }

    pub(super) fn actor_count(&self) -> usize {
        self.actors.len()
    }

    pub(super) fn has_actors(&self) -> bool {
        !self.actor_by_handle.is_empty()
    }

    pub(super) fn insert_actor(&mut self, socket_key: SocketKey, actor: RioSocketActor) -> bool {
        let is_first = self.actor_by_handle.is_empty();
        let actor_key = self.actors.insert(actor);
        self.actor_by_handle.insert(socket_key, actor_key);
        self.socket_runtime.entry(socket_key).or_default();
        is_first
    }

    pub(super) fn remove_actor(&mut self, socket_key: SocketKey) {
        if let Some(actor_key) = self.actor_by_handle.remove(&socket_key) {
            let _ = self.actors.remove(actor_key);
        }
    }

    pub(super) fn socket_closing_inflight(&self, actor_key: SocketKey) -> Option<u32> {
        self.socket_runtime
            .get(&actor_key)
            .filter(|state| state.lifecycle == SocketLifecycleState::Closing)
            .map(|state| state.inflight)
    }

    #[cfg(test)]
    pub(super) fn actor_index_is_empty(&self) -> bool {
        self.actor_by_handle.is_empty()
    }

    #[cfg(test)]
    pub(super) fn socket_inflight_for(&self, actor_key: SocketKey) -> Option<u32> {
        self.socket_runtime
            .get(&actor_key)
            .map(|state| state.inflight)
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
}

impl RioSocketActor {
    pub(crate) fn new(rq: RioRq) -> Self {
        Self { rq }
    }

    pub(crate) const fn rq(&self) -> RioRq {
        self.rq
    }
}

impl SocketInflightToken {
    #[inline]
    pub(crate) const fn new(socket_key: SocketKey, request_id: u64) -> Self {
        Self {
            socket_key,
            request_id,
        }
    }

    #[inline]
    pub(crate) const fn socket_key(&self) -> SocketKey {
        self.socket_key
    }

    #[inline]
    pub(crate) const fn request_id(&self) -> u64 {
        self.request_id
    }

    #[inline]
    pub(crate) const fn identity(&self) -> SocketInflightIdentity {
        SocketInflightIdentity {
            socket_key: self.socket_key,
            request_id: self.request_id,
        }
    }

    fn release_from(
        self,
        socket_runtime: &mut FastHashMap<SocketKey, SocketRuntimeState>,
    ) -> RioResult<()> {
        self.identity().release_from(socket_runtime)
    }
}

impl SocketInflightIdentity {
    #[inline]
    pub(crate) const fn socket_key(self) -> SocketKey {
        self.socket_key
    }

    #[inline]
    pub(crate) const fn request_id(self) -> u64 {
        self.request_id
    }

    fn release_from(
        self,
        socket_runtime: &mut FastHashMap<SocketKey, SocketRuntimeState>,
    ) -> RioResult<()> {
        let socket_key = self.socket_key();
        let state = socket_runtime.get_mut(&socket_key).ok_or_else(|| {
            RioError::InvalidInput
                .to_report()
                .with_ctx("socket_raw", socket_key.as_handle() as usize)
                .attach_note("socket inflight release without registered runtime state")
        })?;

        if !state.active_tokens.contains(&self.request_id()) {
            return RioError::Internal
                .with_ctx("socket_raw", socket_key.as_handle() as usize)
                .with_ctx("request_id", self.request_id())
                .attach_note("socket inflight token is stale, duplicated, or already released");
        }
        if state.inflight == 0 {
            return RioError::Internal
                .with_ctx("socket_raw", socket_key.as_handle() as usize)
                .with_ctx("request_id", self.request_id())
                .attach_note("socket inflight counter underflow");
        }
        state.active_tokens.remove(&self.request_id());
        state.inflight -= 1;
        Ok(())
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
            && let Err(e) = self.runtime.release_socket_inflight_token(token)
        {
            tracing::error!(error = ?e, "failed to release socket inflight token in guard drop");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{RioRuntime, RioSocketActor, SocketInflightToken, SocketLifecycleState};
    use crate::{
        config::{IocpHandle, SocketKey},
        rio::core::{RioOpKind, RioRq},
    };
    use veloq_driver_core::driver::OpToken;
    use veloq_driver_core::slot::Generation;
    use veloq_std::ptr::null_mut;

    fn test_runtime() -> RioRuntime {
        RioRuntime::new()
    }

    fn test_socket_key() -> SocketKey {
        IocpHandle::for_socket(null_mut())
    }

    #[test]
    fn socket_inflight_token_acquire_and_release_balances_count() {
        let mut state = test_runtime();
        let key = test_socket_key();
        state.mark_socket_registered(key);

        let token = state
            .try_acquire_socket_inflight_token(key, 0)
            .expect("registered open socket should acquire inflight token");
        assert_eq!(state.socket_runtime.get(&key).unwrap().inflight, 1);

        state.release_socket_inflight_token(token).unwrap();
        let socket_state = state.socket_runtime.get(&key).unwrap();
        assert_eq!(socket_state.inflight, 0);
        assert!(socket_state.active_tokens.is_empty());
    }

    #[test]
    fn duplicate_token_release_does_not_decrement_new_request() {
        let mut state = test_runtime();
        let key = test_socket_key();
        state.mark_socket_registered(key);

        let first = state
            .try_acquire_socket_inflight_token(key, 0)
            .expect("first request should acquire an inflight token");
        let first_id = first.request_id();
        state
            .release_socket_inflight_token(first)
            .expect("first request should release its token");
        let second = state
            .try_acquire_socket_inflight_token(key, 0)
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
        let mut state = test_runtime();
        let key = test_socket_key();
        state.mark_socket_registered(key);
        let token = state
            .try_acquire_socket_inflight_token(key, 0)
            .expect("request should acquire an inflight token");

        assert!(!state.runtime_ready_for_forget(0));
        assert!(state.forget_after_drain(0).is_err());
        assert!(state.socket_runtime.contains_key(&key));

        state
            .release_socket_inflight_token(token)
            .expect("request should release its token");
        assert!(state.runtime_ready_for_forget(0));
    }

    #[test]
    fn socket_inflight_guard_drop_rolls_back_count() {
        let mut state = test_runtime();
        let key = test_socket_key();
        state.mark_socket_registered(key);

        {
            let _guard = state
                .try_acquire_socket_inflight_guard(key, 0)
                .expect("registered open socket should acquire inflight guard");
        }

        assert_eq!(state.socket_runtime.get(&key).unwrap().inflight, 0);
    }

    #[test]
    fn closing_socket_rejects_new_inflight_acquire() {
        let mut state = test_runtime();
        let key = test_socket_key();
        state.mark_socket_registered(key);

        assert!(state.begin_socket_cleanup(key).unwrap());
        assert!(state.try_acquire_socket_inflight_token(key, 0).is_err());
    }

    #[test]
    fn stop_accepting_new_submissions_preserves_socket_runtime_until_drain() {
        let mut state = test_runtime();
        let key = test_socket_key();
        state.mark_socket_registered(key);
        let actor = state.actors.insert(RioSocketActor::new(RioRq(1 as _)));
        state.actor_by_handle.insert(key, actor);

        let token = state
            .try_acquire_socket_inflight_token(key, 0)
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
        assert!(state.try_acquire_socket_inflight_token(key, 0).is_err());

        state.release_socket_inflight_token(token).unwrap();
        assert_eq!(state.socket_runtime.get(&key).unwrap().inflight, 0);
    }

    #[test]
    fn forget_runtime_after_drain_clears_runtime_state() {
        let mut state = test_runtime();
        let key = test_socket_key();
        state.mark_socket_registered(key);
        let actor = state.actors.insert(RioSocketActor::new(RioRq(1 as _)));
        state.actor_by_handle.insert(key, actor);
        state.stop_accepting_new_submissions();

        state
            .forget_after_drain(0)
            .expect("empty test runtime should be ready for cleanup");

        assert!(state.actors.is_empty());
        assert!(state.actor_by_handle.is_empty());
        assert!(state.socket_runtime.is_empty());
    }

    #[test]
    fn missing_socket_runtime_is_not_ready_for_cleanup() {
        let mut state = test_runtime();
        let key = test_socket_key();

        assert!(!state.socket_ready_for_cleanup(key));
        state.mark_socket_registered(key);
        assert!(!state.socket_ready_for_cleanup(key));
        assert!(state.begin_socket_cleanup(key).unwrap());
        assert!(state.socket_ready_for_cleanup(key));
    }

    #[test]
    fn socket_cleanup_retries_receive_shutdown_after_send_drains() {
        let mut state = test_runtime();
        let key = test_socket_key();
        state.mark_socket_registered(key);

        let receive_token = state
            .try_acquire_socket_inflight_token(key, 0)
            .expect("receive request should acquire an inflight token");
        state
            .record_request_submitted(RioOpKind::Recv, key)
            .expect("receive request should be tracked");
        let send_token = state
            .try_acquire_socket_inflight_token(key, 0)
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
        let mut state = test_runtime();
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
