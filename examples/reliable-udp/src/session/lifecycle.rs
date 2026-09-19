use veloq::std::{time::Duration, vec, vec::Vec};

use crate::{
    config::Config,
    error::{Error, Result},
    packet::{COOKIE_LEN, ConnectionId, FrameType},
    timer::TimerKind,
};

use super::{Role, SessionState};

pub(super) enum LifecycleAction {
    EmitControl(FrameType),
    EmitCookieProof([u8; COOKIE_LEN]),
    StateChanged(SessionState),
    ArmTimer {
        kind: TimerKind,
        delay: Duration,
    },
    CancelTimer(TimerKind),
    Terminate {
        error: Error,
        state: SessionState,
        generation: u64,
    },
}

pub(super) struct LifecycleState {
    role: Role,
    state: SessionState,
    connection_id: ConnectionId,
    generation: u64,
    handshake_started: Option<Duration>,
    handshake_rto: Duration,
    handshake_retries: u8,
    fin_retries: u8,
    cookie: Option<[u8; COOKIE_LEN]>,
}

impl LifecycleState {
    pub(super) fn new(
        role: Role,
        state: SessionState,
        connection_id: ConnectionId,
        config: &Config,
    ) -> Self {
        Self {
            role,
            state,
            connection_id,
            generation: 1,
            handshake_started: None,
            handshake_rto: config.handshake_initial_rto,
            handshake_retries: 0,
            fin_retries: 0,
            cookie: None,
        }
    }

    pub(super) fn role(&self) -> Role {
        self.role
    }

    pub(super) fn state(&self) -> SessionState {
        self.state
    }

    pub(super) fn connection_id(&self) -> ConnectionId {
        self.connection_id
    }

    pub(super) fn generation(&self) -> u64 {
        self.generation
    }

    pub(super) fn start(&mut self, now: Duration, config: &Config) -> Result<Vec<LifecycleAction>> {
        if self.role != Role::Client || self.state != SessionState::SynSent {
            return Err(Error::InvalidState);
        }
        self.begin_handshake(now, config);
        Ok(vec![
            LifecycleAction::EmitControl(FrameType::Syn),
            LifecycleAction::ArmTimer {
                kind: TimerKind::HandshakeRetry,
                delay: self.handshake_delay(now, config),
            },
        ])
    }

    pub(super) fn on_syn_ack(&mut self, cookie: &[u8; COOKIE_LEN]) -> Vec<LifecycleAction> {
        if self.role != Role::Client {
            return Vec::new();
        }
        match self.state {
            SessionState::SynSent | SessionState::CookieSent => {
                self.cookie = Some(*cookie);
                let mut actions = if self.state == SessionState::SynSent {
                    self.transition(SessionState::CookieSent)
                } else {
                    Vec::new()
                };
                actions.push(LifecycleAction::EmitCookieProof(*cookie));
                actions
            }
            _ => Vec::new(),
        }
    }

    pub(super) fn on_handshake_confirmation(&mut self) -> Vec<LifecycleAction> {
        if self.role == Role::Client && self.state == SessionState::CookieSent {
            self.establish()
        } else {
            Vec::new()
        }
    }

    pub(super) fn on_fin(&mut self) -> Vec<LifecycleAction> {
        if !matches!(self.state, SessionState::Established) {
            return Vec::new();
        }
        let mut actions = vec![LifecycleAction::CancelTimer(TimerKind::HandshakeRetry)];
        actions.extend(self.transition(SessionState::CloseWait));
        actions.push(LifecycleAction::EmitControl(FrameType::FinAck));
        actions.extend(self.transition(SessionState::Closed));
        actions
    }

    pub(super) fn on_fin_ack(&mut self) -> Vec<LifecycleAction> {
        if self.state != SessionState::FinWait {
            return Vec::new();
        }
        let mut actions = vec![LifecycleAction::CancelTimer(TimerKind::FinRetry)];
        actions.extend(self.transition(SessionState::Closed));
        actions
    }

    pub(super) fn establish(&mut self) -> Vec<LifecycleAction> {
        if self.state != SessionState::Established {
            let mut actions = vec![LifecycleAction::CancelTimer(TimerKind::HandshakeRetry)];
            self.handshake_started = None;
            actions.extend(self.transition(SessionState::Established));
            actions
        } else {
            Vec::new()
        }
    }

    pub(super) fn request_close(
        &mut self,
        now: Duration,
        config: &Config,
    ) -> Result<Vec<LifecycleAction>> {
        match self.state {
            SessionState::Established | SessionState::CloseWait => {
                self.fin_retries = 0;
                let mut actions = vec![LifecycleAction::EmitControl(FrameType::Fin)];
                actions.extend(self.transition(SessionState::FinWait));
                actions.push(LifecycleAction::ArmTimer {
                    kind: TimerKind::FinRetry,
                    delay: config.close_timeout,
                });
                Ok(actions)
            }
            SessionState::Closed => Ok(Vec::new()),
            SessionState::Failed | SessionState::Reset => Err(Error::ConnectionClosed),
            _ => {
                let actions = vec![
                    LifecycleAction::CancelTimer(TimerKind::HandshakeRetry),
                    LifecycleAction::CancelTimer(TimerKind::FinRetry),
                ];
                self.state = SessionState::Closed;
                let mut actions = actions;
                actions.push(LifecycleAction::StateChanged(SessionState::Closed));
                let _ = now;
                Ok(actions)
            }
        }
    }

    pub(super) fn request_abort(&mut self, error: Error) -> Result<Vec<LifecycleAction>> {
        if matches!(
            self.state,
            SessionState::Closed | SessionState::Failed | SessionState::Reset
        ) {
            return Ok(Vec::new());
        }
        let mut actions = vec![LifecycleAction::EmitControl(FrameType::Rst)];
        actions.extend(self.request_terminate(error, SessionState::Failed));
        Ok(actions)
    }

    pub(super) fn request_terminate(
        &mut self,
        error: Error,
        state: SessionState,
    ) -> Vec<LifecycleAction> {
        if matches!(
            self.state,
            SessionState::Closed | SessionState::Failed | SessionState::Reset
        ) {
            return Vec::new();
        }
        let generation = self.generation;
        self.handshake_started = None;
        self.state = state;
        self.generation = self.generation.wrapping_add(1);
        vec![LifecycleAction::Terminate {
            error,
            state,
            generation,
        }]
    }

    pub(super) fn on_handshake_timeout(
        &mut self,
        now: Duration,
        config: &Config,
    ) -> Result<Vec<LifecycleAction>> {
        if !matches!(self.state, SessionState::SynSent | SessionState::CookieSent) {
            return Ok(Vec::new());
        }
        let elapsed = self
            .handshake_started
            .map_or(config.handshake_deadline, |started| {
                now.saturating_sub(started)
            });
        if elapsed >= config.handshake_deadline
            || self.handshake_retries >= config.handshake_max_retries
        {
            return Ok(self.request_terminate(Error::HandshakeTimeout, SessionState::Failed));
        }
        self.handshake_retries = self.handshake_retries.saturating_add(1);
        self.handshake_rto = self
            .handshake_rto
            .saturating_mul(2)
            .min(config.handshake_max_rto);
        let mut actions = match self.state {
            SessionState::SynSent => vec![LifecycleAction::EmitControl(FrameType::Syn)],
            SessionState::CookieSent => vec![LifecycleAction::EmitCookieProof(
                self.cookie.expect("cookie sent state has a cookie"),
            )],
            _ => Vec::new(),
        };
        actions.push(LifecycleAction::ArmTimer {
            kind: TimerKind::HandshakeRetry,
            delay: self.handshake_delay(now, config),
        });
        Ok(actions)
    }

    pub(super) fn on_fin_timeout(&mut self, config: &Config) -> Result<Vec<LifecycleAction>> {
        if self.state != SessionState::FinWait {
            return Ok(Vec::new());
        }
        if self.fin_retries >= config.max_retries {
            let mut actions = vec![LifecycleAction::EmitControl(FrameType::Rst)];
            actions.extend(self.request_terminate(Error::CloseTimeout, SessionState::Failed));
            return Ok(actions);
        }
        self.fin_retries = self.fin_retries.saturating_add(1);
        Ok(vec![
            LifecycleAction::EmitControl(FrameType::Fin),
            LifecycleAction::ArmTimer {
                kind: TimerKind::FinRetry,
                delay: config.close_timeout,
            },
        ])
    }

    fn begin_handshake(&mut self, now: Duration, config: &Config) {
        self.handshake_started = Some(now);
        self.handshake_rto = config.handshake_initial_rto;
        self.handshake_retries = 0;
    }

    fn handshake_delay(&self, now: Duration, config: &Config) -> Duration {
        let elapsed = self
            .handshake_started
            .map_or(Duration::ZERO, |started| now.saturating_sub(started));
        let remaining = config.handshake_deadline.saturating_sub(elapsed);
        self.handshake_rto.min(remaining)
    }

    fn transition(&mut self, state: SessionState) -> Vec<LifecycleAction> {
        if self.state == state {
            Vec::new()
        } else {
            self.state = state;
            vec![LifecycleAction::StateChanged(state)]
        }
    }
}
