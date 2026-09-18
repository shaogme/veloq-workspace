use veloq::std::{time::Duration, vec, vec::Vec};

use crate::{
    config::Config,
    error::{Error, Result},
    packet::{ConnectionId, Flags},
    timer::TimerKind,
};

use super::{Role, SessionState};

pub(super) enum LifecycleAction {
    EmitControl(Flags),
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
            LifecycleAction::EmitControl(Flags::SYN),
            LifecycleAction::ArmTimer {
                kind: TimerKind::HandshakeRetry,
                delay: self.handshake_delay(now, config),
            },
        ])
    }

    pub(super) fn on_syn(
        &mut self,
        now: Duration,
        _receive_window: u16,
        config: &Config,
    ) -> Vec<LifecycleAction> {
        if self.role != Role::Server {
            return Vec::new();
        }
        match self.state {
            SessionState::Listen => {
                let mut actions = self.transition(SessionState::SynReceived);
                self.begin_handshake(now, config);
                actions.push(LifecycleAction::EmitControl(Flags::SYN_ACK));
                actions.push(LifecycleAction::ArmTimer {
                    kind: TimerKind::HandshakeRetry,
                    delay: self.handshake_delay(now, config),
                });
                actions
            }
            SessionState::SynReceived | SessionState::Established => {
                vec![LifecycleAction::EmitControl(Flags::SYN_ACK)]
            }
            _ => Vec::new(),
        }
    }

    pub(super) fn on_syn_ack(&mut self, _receive_window: u16) -> Vec<LifecycleAction> {
        if self.role != Role::Client {
            return Vec::new();
        }
        match self.state {
            SessionState::SynSent => {
                let mut actions = self.establish();
                actions.push(LifecycleAction::EmitControl(Flags::ACK));
                actions
            }
            SessionState::Established => vec![LifecycleAction::EmitControl(Flags::ACK)],
            _ => Vec::new(),
        }
    }

    pub(super) fn on_fin(&mut self) -> Vec<LifecycleAction> {
        if !matches!(
            self.state,
            SessionState::Established | SessionState::SynReceived
        ) {
            return Vec::new();
        }
        let mut actions = vec![LifecycleAction::CancelTimer(TimerKind::HandshakeRetry)];
        actions.extend(self.transition(SessionState::CloseWait));
        actions.push(LifecycleAction::EmitControl(Flags::FIN_ACK));
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
                let mut actions = vec![LifecycleAction::EmitControl(Flags::FIN)];
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
        let mut actions = vec![LifecycleAction::EmitControl(Flags::RST)];
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
        if !matches!(
            self.state,
            SessionState::SynSent | SessionState::SynReceived
        ) {
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
        let flags = match self.role {
            Role::Client => Flags::SYN,
            Role::Server => Flags::SYN_ACK,
        };
        Ok(vec![
            LifecycleAction::EmitControl(flags),
            LifecycleAction::ArmTimer {
                kind: TimerKind::HandshakeRetry,
                delay: self.handshake_delay(now, config),
            },
        ])
    }

    pub(super) fn on_fin_timeout(&mut self, config: &Config) -> Result<Vec<LifecycleAction>> {
        if self.state != SessionState::FinWait {
            return Ok(Vec::new());
        }
        if self.fin_retries >= config.max_retries {
            let mut actions = vec![LifecycleAction::EmitControl(Flags::RST)];
            actions.extend(self.request_terminate(Error::CloseTimeout, SessionState::Failed));
            return Ok(actions);
        }
        self.fin_retries = self.fin_retries.saturating_add(1);
        Ok(vec![
            LifecycleAction::EmitControl(Flags::FIN),
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
