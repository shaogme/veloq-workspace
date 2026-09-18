use tracing::{debug, trace, warn};
use veloq::{
    buf::FixedBuf,
    std::{collections::VecDeque, vec::Vec},
    sync::TrySendError,
};

use crate::{
    connection::Connection,
    error::{Error, Result},
    packet::MessageSequence,
    session::{Role, SessionEvent, SessionState},
    timer::TimerCommand,
};

use super::ConnectionKey;
use super::command::CommandSender;
use super::io::{OutboundDatagram, OutboundSender, PumpSender, SendTicket};
use super::state::ProtocolState;

struct QueuedDatagram {
    datagram: FixedBuf,
    retain_for_retransmit: Option<MessageSequence>,
}

#[derive(Default)]
pub(super) struct EventBatch {
    datagrams: VecDeque<QueuedDatagram>,
    timer_commands: Vec<TimerCommand>,
    terminal_error: Option<Error>,
    reject_accept: bool,
}

impl EventBatch {
    fn extend(&mut self, other: Self) {
        self.datagrams.extend(other.datagrams);
        self.timer_commands.extend(other.timer_commands);
        if other.terminal_error.is_some() {
            self.terminal_error = other.terminal_error;
        }
        self.reject_accept |= other.reject_accept;
    }
}

pub(super) struct EventRouter;

impl EventRouter {
    pub(super) fn record_events<'rt>(
        state: &mut ProtocolState<'rt>,
        key: ConnectionKey,
        events: Vec<SessionEvent>,
        command: &CommandSender,
        outbound: &OutboundSender,
        pump_events: &PumpSender,
    ) -> Result<()> {
        Self::update_session_stats(state, &key);
        let mut batch = Self::collect_events(state, key, events, command)?;
        if batch.reject_accept {
            let now = state.now();
            let ctx = state.ctx();
            let abort_events = state
                .entry_mut(&key)
                .ok_or(Error::ConnectionClosed)?
                .session_mut()
                .abort(now, Error::TooManyConnections, &ctx)?;
            let abort_batch = Self::collect_events(state, key, abort_events, command)?;
            batch.extend(abort_batch);
            batch.terminal_error = Some(Error::TooManyConnections);
        }

        let session_state = state.entry(&key).map(|entry| entry.session().state());
        let mut terminal = batch.terminal_error.or_else(|| {
            session_state
                .filter(|state| {
                    matches!(
                        state,
                        SessionState::Closed | SessionState::Failed | SessionState::Reset
                    )
                })
                .map(|state| match state {
                    SessionState::Reset => Error::ConnectionReset,
                    _ => Error::ConnectionClosed,
                })
        });
        if terminal.is_some() {
            state.clock_mut().cancel_connection(key);
        } else {
            Self::apply_timer_commands(state, key, batch.timer_commands)?;
        }

        if Self::enqueue_datagrams(state, key, batch.datagrams, outbound, pump_events) {
            terminal = Some(Error::OutboundQueueFull);
            let now = state.now();
            let ctx = state.ctx();
            let reset_events = state
                .entry_mut(&key)
                .ok_or(Error::ConnectionClosed)?
                .session_mut()
                .abort(now, Error::OutboundQueueFull, &ctx)?;
            Self::enqueue_reset(key, reset_events, outbound);
        }

        if let Some(error) = terminal {
            state.clock_mut().cancel_connection(key);
            Self::update_session_stats(state, &key);
            let stats = state.stats_arc();
            if let Some(entry) = state.entry_mut(&key) {
                stats.remove_session_gauges(entry.observed_stats());
                entry.finish(error);
            }
            let _ = state.remove(&key);
            state.stats().connection_finished();
        }
        Ok(())
    }

    pub(super) fn shutdown<'rt>(
        state: &mut ProtocolState<'rt>,
        error: Error,
        command: &CommandSender,
        outbound: &OutboundSender,
        pump_events: &PumpSender,
    ) -> Result<()> {
        let keys = state.keys();
        for key in keys {
            let now = state.now();
            let ctx = state.ctx();
            let events = match state.entry_mut(&key) {
                Some(entry) => entry
                    .session_mut()
                    .abort(now, error, &ctx)
                    .unwrap_or_default(),
                None => continue,
            };
            Self::record_events(state, key, events, command, outbound, pump_events)?;
        }
        state.clear_sessions();
        state.clock_mut().clear();
        state.drain_accept();
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn handle_send_completion<'rt>(
        state: &mut ProtocolState<'rt>,
        key: ConnectionKey,
        ticket: SendTicket,
        result: Result<()>,
        datagram: Option<FixedBuf>,
        command: &CommandSender,
        outbound: &OutboundSender,
        pump_events: &PumpSender,
    ) -> Result<()> {
        match ticket {
            SendTicket::CompleteConnect => {
                drop(datagram);
                let should_handle = state
                    .entry_mut(&key)
                    .is_some_and(|entry| entry.take_connect_completion_pending());
                if !should_handle {
                    return Ok(());
                }
                match result {
                    Ok(()) => {
                        if let Some(entry) = state.entry_mut(&key)
                            && let Some(reply) = entry.take_connect_reply()
                        {
                            let _ = reply.send(Ok(()));
                        }
                    }
                    Err(error) => {
                        let now = state.now();
                        let ctx = state.ctx();
                        let events = match state.entry_mut(&key) {
                            Some(entry) => entry.session_mut().abort(now, error, &ctx)?,
                            None => return Ok(()),
                        };
                        Self::record_events(state, key, events, command, outbound, pump_events)?;
                    }
                }
            }
            SendTicket::ReturnToSession(sequence) => match result {
                Ok(()) => {
                    let Some(datagram) = datagram else {
                        return Self::handle_send_error(
                            state,
                            key,
                            Error::Io,
                            command,
                            outbound,
                            pump_events,
                        );
                    };
                    let now = state.now();
                    let events = {
                        let Some(entry) = state.entry_mut(&key) else {
                            return Ok(());
                        };
                        entry
                            .session_mut()
                            .on_send_completed(now, sequence, datagram)?
                    };
                    Self::record_events(state, key, events, command, outbound, pump_events)?;
                }
                Err(error) => {
                    drop(datagram);
                    Self::handle_send_error(state, key, error, command, outbound, pump_events)?;
                }
            },
            SendTicket::DropAfterSend => drop(datagram),
        }
        Ok(())
    }

    pub(super) fn handle_send_error<'rt>(
        state: &mut ProtocolState<'rt>,
        key: ConnectionKey,
        error: Error,
        command: &CommandSender,
        outbound: &OutboundSender,
        pump_events: &PumpSender,
    ) -> Result<()> {
        let now = state.now();
        let ctx = state.ctx();
        let events = match state.entry_mut(&key) {
            Some(entry) => entry.session_mut().abort(now, error, &ctx)?,
            None => return Ok(()),
        };
        Self::record_events(state, key, events, command, outbound, pump_events)
    }

    fn update_session_stats(state: &mut ProtocolState<'_>, key: &ConnectionKey) {
        let stats = state.stats_arc();
        let Some(entry) = state.entry_mut(key) else {
            return;
        };
        let previous = entry.observed_stats();
        let current = entry.session().stats();
        stats.record_session_delta(previous, current);
        entry.set_observed_stats(current);
    }

    fn collect_events(
        state: &mut ProtocolState<'_>,
        key: ConnectionKey,
        events: Vec<SessionEvent>,
        command: &CommandSender,
    ) -> Result<EventBatch> {
        let max_payload = state.config().max_payload();
        let mut batch = EventBatch::default();
        for event in events {
            match event {
                SessionEvent::Outbound {
                    datagram,
                    retain_for_retransmit,
                } => batch.datagrams.push_back(QueuedDatagram {
                    datagram,
                    retain_for_retransmit,
                }),
                SessionEvent::ArmTimer(command) | SessionEvent::CancelTimer(command) => {
                    trace!(
                        target: "veloq_reliable_udp::endpoint",
                        connection_id = key.connection_id().get(),
                        command = ?command,
                        "protocol loop received timer command"
                    );
                    batch.timer_commands.push(command);
                }
                SessionEvent::SendAcked(receipt) => {
                    debug!(
                        target: "veloq_reliable_udp::endpoint",
                        peer = ?key.peer(),
                        connection_id = key.connection_id().get(),
                        token = receipt.token.get(),
                        sequence = receipt.sequence.get(),
                        retransmissions = receipt.retransmissions,
                        rtt = ?receipt.rtt,
                        "protocol loop received SendAcked"
                    );
                    if let Some(entry) = state.entry_mut(&key)
                        && let Some(reply) = entry.take_pending_send(receipt.token)
                    {
                        let _ = reply.send(Ok(receipt));
                    }
                }
                SessionEvent::SendFailed { token, error, .. } => {
                    if let Some(entry) = state.entry_mut(&key)
                        && let Some(reply) = entry.take_pending_send(token)
                    {
                        let _ = reply.send(Err(error));
                    }
                }
                SessionEvent::StateChanged(SessionState::Established) => {
                    let (role, connecting, accepted) = state
                        .entry(&key)
                        .map(|entry| {
                            (
                                entry.session().role(),
                                entry.has_connect_reply(),
                                entry.is_accepted(),
                            )
                        })
                        .ok_or(Error::ConnectionClosed)?;
                    if role == Role::Client && connecting {
                        if let Some(entry) = state.entry_mut(&key) {
                            entry.mark_connect_send_pending();
                        }
                    } else if !accepted {
                        let connection = Connection::new(command.clone(), key, max_payload);
                        match state.try_accept(connection) {
                            Ok(()) => {
                                if let Some(entry) = state.entry_mut(&key) {
                                    entry.mark_accepted();
                                }
                            }
                            Err(TrySendError::Full(mut returned))
                            | Err(TrySendError::Closed(mut returned)) => {
                                returned.suppress_drop();
                                drop(returned);
                                batch.reject_accept = true;
                            }
                        }
                    }
                }
                SessionEvent::StateChanged(SessionState::Closed)
                | SessionEvent::StateChanged(SessionState::Failed) => {
                    batch.terminal_error = Some(Error::ConnectionClosed);
                }
                SessionEvent::StateChanged(SessionState::Reset) => {
                    batch.terminal_error = Some(Error::ConnectionReset);
                }
                SessionEvent::Failed(error) => batch.terminal_error = Some(error),
                SessionEvent::MessageAvailable | SessionEvent::StateChanged(_) => {}
            }
        }
        Ok(batch)
    }

    fn apply_timer_commands(
        state: &mut ProtocolState<'_>,
        key: ConnectionKey,
        commands: Vec<TimerCommand>,
    ) -> Result<()> {
        for command in commands {
            state.clock_mut().apply(key, command)?;
        }
        Ok(())
    }

    fn enqueue_datagrams(
        state: &mut ProtocolState<'_>,
        key: ConnectionKey,
        mut datagrams: VecDeque<QueuedDatagram>,
        outbound: &OutboundSender,
        _pump_events: &PumpSender,
    ) -> bool {
        let mut queue_full = false;
        while let Some(queued) = datagrams.pop_front() {
            let ticket = if let Some(sequence) = queued.retain_for_retransmit {
                SendTicket::ReturnToSession(sequence)
            } else if state
                .entry(&key)
                .is_some_and(|entry| entry.connect_send_pending())
            {
                SendTicket::CompleteConnect
            } else {
                SendTicket::DropAfterSend
            };
            let item = OutboundDatagram::new(key, queued.datagram, ticket);
            match outbound.try_send(item) {
                Ok(()) => {
                    if matches!(ticket, SendTicket::CompleteConnect)
                        && let Some(entry) = state.entry_mut(&key)
                        && entry.take_connect_send_pending()
                    {
                        entry.mark_connect_completion_pending();
                    }
                }
                Err(TrySendError::Full(_)) | Err(TrySendError::Closed(_)) => {
                    state.stats().record_outbound_drop();
                    queue_full = true;
                    warn!(
                        target: "veloq_reliable_udp::endpoint",
                        peer = ?key.peer(),
                        connection_id = key.connection_id().get(),
                        "protocol loop dropped outbound datagram because queue is full or closed"
                    );
                }
            }
        }
        queue_full
    }

    fn enqueue_reset(key: ConnectionKey, events: Vec<SessionEvent>, outbound: &OutboundSender) {
        for event in events {
            if let SessionEvent::Outbound { datagram, .. } = event {
                let _ = outbound.try_send(OutboundDatagram::new(
                    key,
                    datagram,
                    SendTicket::DropAfterSend,
                ));
            }
        }
    }
}
