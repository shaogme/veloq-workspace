use tracing::trace;
use veloq::{
    net::UdpSocket,
    runtime::{Outcome, context::Ctx, scope, select},
    std::result::Result as StdResult,
    sync::{TryRecvError, mpmc::owned_bounded},
    time::sleep,
};

use crate::error::{Error, Result};

use super::event::EventRouter;
use super::io::{
    InboundDatagram, InboundReceiver, OutboundSender, PumpEvent, PumpReceiver, PumpSender,
    receive_pump, send_pump,
};
use super::state::{ProtocolState, ReadySender};
use super::{
    Reply,
    command::{CommandOutcome, CommandPorts, CommandReceiver, CommandService},
};

enum DriverEvent {
    Command(StdResult<super::Command, TryRecvError>),
    Packet(StdResult<InboundDatagram, TryRecvError>),
    Pump(StdResult<PumpEvent, TryRecvError>),
    Timer,
}

pub(super) struct Driver<'rt> {
    ctx: Ctx<'rt>,
    socket: Option<UdpSocket<'rt>>,
    commands: CommandReceiver,
    state: ProtocolState<'rt>,
    endpoint_close_reply: Option<Reply<()>>,
    ready: Option<ReadySender>,
}

impl<'rt> Driver<'rt> {
    pub(super) fn new(
        ctx: Ctx<'rt>,
        socket: UdpSocket<'rt>,
        commands: CommandReceiver,
        state: ProtocolState<'rt>,
        ready: ReadySender,
    ) -> Self {
        Self {
            ctx,
            socket: Some(socket),
            commands,
            state,
            endpoint_close_reply: None,
            ready: Some(ready),
        }
    }

    pub(super) async fn run(mut self) -> Result<()> {
        let socket = match self.socket.take() {
            Some(socket) => socket,
            None => {
                self.send_ready(Err(Error::EndpointClosed));
                return Err(Error::EndpointClosed);
            }
        };
        trace!(target: "veloq_reliable_udp::endpoint", "endpoint driver started");
        let config = self.state.config().clone();
        let (inbound_tx, inbound_rx) = owned_bounded(config.inbound_capacity.get());
        let (outbound_tx, outbound_rx) = owned_bounded(config.outbound_capacity.get());
        let event_capacity = config
            .outbound_capacity
            .get()
            .checked_add(config.max_connections.get())
            .ok_or(Error::Io)?;
        let (pump_tx, pump_rx) = owned_bounded(event_capacity);
        let ctx = self.ctx;
        let receive_socket = socket.clone();
        let send_socket = socket.clone();
        let stats = self.state.stats_arc();

        let scoped = scope!(ctx, async |scope| {
            let mut receive_task = scope.spawn_boxed(receive_pump(
                ctx,
                receive_socket,
                config.clone(),
                inbound_tx,
                pump_tx.clone(),
                stats,
            ));
            let mut send_task =
                scope.spawn_boxed(send_pump(send_socket, outbound_rx, pump_tx.clone()));
            let result = self
                .run_protocol(inbound_rx, outbound_tx, pump_tx, pump_rx)
                .await;
            let graceful = matches!(result, Ok(true));
            receive_task.cancel();
            if !graceful {
                send_task.cancel();
            }
            let _ = receive_task.await;
            let _ = send_task.await;
            result
        })
        .await
        .map_err(|_| Error::Io)?;

        let result = match scoped {
            Outcome::Ok(_) => Ok(()),
            Outcome::Err(error) => Err(error),
        };
        if let Err(error) = result {
            self.send_ready(Err(error));
        }
        let close_result = socket.close().await.map_err(|_| Error::Io);
        let result = result.and(close_result);
        if let Some(reply) = self.endpoint_close_reply.take() {
            let _ = reply.send(result);
        }
        result
    }

    async fn run_protocol(
        &mut self,
        inbound: InboundReceiver,
        outbound: OutboundSender,
        pump_sender: PumpSender,
        mut pump_events: PumpReceiver,
    ) -> Result<bool> {
        let result = match self.wait_for_pumps(&mut pump_events).await {
            Ok(()) => {
                self.send_ready(Ok(()));
                self.run_protocol_loop(inbound, &outbound, &pump_sender, pump_events)
                    .await
            }
            Err(error) => {
                self.send_ready(Err(error));
                Err(error)
            }
        };
        if let Err(error) = result {
            self.shutdown(error, &outbound, &pump_sender)?;
        }
        result
    }

    async fn wait_for_pumps(&self, pump_events: &mut PumpReceiver) -> Result<()> {
        let mut receive_ready = false;
        let mut send_ready = false;
        while !(receive_ready && send_ready) {
            match pump_events.recv().await {
                Ok(PumpEvent::ReceiveReady) => receive_ready = true,
                Ok(PumpEvent::SendReady) => send_ready = true,
                Ok(PumpEvent::ReceiveFailed(error)) => return Err(error),
                Ok(PumpEvent::SendFailed { error, .. }) => return Err(error),
                Ok(PumpEvent::SendCompleted {
                    result, datagram, ..
                }) => {
                    drop(datagram);
                    result?
                }
                Err(_) => return Err(Error::EndpointClosed),
            }
        }
        Ok(())
    }

    fn send_ready(&mut self, result: Result<()>) {
        if let Some(sender) = self.ready.take() {
            let _ = sender.send(result);
        }
    }

    async fn run_protocol_loop(
        &mut self,
        inbound: InboundReceiver,
        outbound: &OutboundSender,
        pump_sender: &PumpSender,
        pump_events: PumpReceiver,
    ) -> Result<bool> {
        loop {
            self.advance_endpoint_clock(outbound, pump_sender)?;
            let event = match self.state.clock().next_deadline()? {
                Some(delay) => select! {
                    self.ctx;
                    command = self.commands.recv() => DriverEvent::Command(command),
                    packet = inbound.recv() => DriverEvent::Packet(packet),
                    pump = pump_events.recv() => DriverEvent::Pump(pump),
                    _ = sleep(self.ctx, delay) => DriverEvent::Timer,
                },
                None => select! {
                    self.ctx;
                    command = self.commands.recv() => DriverEvent::Command(command),
                    packet = inbound.recv() => DriverEvent::Packet(packet),
                    pump = pump_events.recv() => DriverEvent::Pump(pump),
                },
            };

            match event {
                DriverEvent::Command(Ok(command)) => {
                    let command_sender = self.command_sender();
                    let ports = CommandPorts::new(self.ctx, &command_sender, outbound, pump_sender);
                    match CommandService::handle(command, &mut self.state, &ports)? {
                        CommandOutcome::Continue => {}
                        CommandOutcome::Close(reply) => {
                            self.endpoint_close_reply = Some(reply);
                            return Ok(true);
                        }
                    }
                }
                DriverEvent::Command(Err(_)) => {
                    self.shutdown(Error::EndpointClosed, outbound, pump_sender)?;
                    return Ok(true);
                }
                DriverEvent::Packet(Ok(packet)) => {
                    let command_sender = self.command_sender();
                    let ports = CommandPorts::new(self.ctx, &command_sender, outbound, pump_sender);
                    CommandService::handle_packet(packet, &mut self.state, &ports)?;
                }
                DriverEvent::Packet(Err(_)) => {
                    self.shutdown(Error::EndpointClosed, outbound, pump_sender)?;
                    return Ok(true);
                }
                DriverEvent::Pump(Ok(PumpEvent::ReceiveFailed(error))) => {
                    self.shutdown(error, outbound, pump_sender)?;
                    return Err(error);
                }
                DriverEvent::Pump(Ok(PumpEvent::ReceiveReady | PumpEvent::SendReady)) => {}
                DriverEvent::Pump(Ok(PumpEvent::SendCompleted {
                    key,
                    ticket,
                    result,
                    datagram,
                })) => {
                    let command = self.command_sender();
                    EventRouter::handle_send_completion(
                        &mut self.state,
                        key,
                        ticket,
                        result,
                        datagram,
                        &command,
                        outbound,
                        pump_sender,
                    )?;
                }
                DriverEvent::Pump(Ok(PumpEvent::SendFailed { key, error })) => {
                    let command = self.command_sender();
                    EventRouter::handle_send_error(
                        &mut self.state,
                        key,
                        error,
                        &command,
                        outbound,
                        pump_sender,
                    )?;
                }
                DriverEvent::Pump(Err(_)) => {
                    self.shutdown(Error::EndpointClosed, outbound, pump_sender)?;
                    return Err(Error::Io);
                }
                DriverEvent::Timer => {}
            }
        }
    }

    fn advance_endpoint_clock(
        &mut self,
        outbound: &OutboundSender,
        pump_events: &PumpSender,
    ) -> Result<()> {
        let expired = self.state.clock_mut().advance()?;
        for timer in expired {
            let slot = timer.slot();
            self.state.stats().record_timer_expiration(timer.delay());
            trace!(
                target: "veloq_reliable_udp::endpoint",
                peer = ?slot.connection().peer(),
                connection_id = slot.connection().connection_id().get(),
                kind = ?slot.kind(),
                generation = slot.generation(),
                delay = ?timer.delay(),
                "endpoint dispatching timer"
            );
            let now = self.state.now();
            let events = {
                let Some(entry) = self.state.entry_mut(&slot.connection()) else {
                    continue;
                };
                entry
                    .session_mut()
                    .on_timer(now, slot.kind(), slot.generation(), &self.ctx)?
            };
            if !events.is_empty() {
                let command = self.command_sender();
                EventRouter::record_events(
                    &mut self.state,
                    slot.connection(),
                    events,
                    &command,
                    outbound,
                    pump_events,
                )?;
            }
        }

        let keys = self.state.keys();
        for key in keys {
            let connect_closed = if let Some(entry) = self.state.entry_mut(&key) {
                entry.clear_closed_waiters();
                entry.connect_reply_closed()
            } else {
                false
            };
            if connect_closed {
                let now = self.state.now();
                let events = match self.state.entry_mut(&key) {
                    Some(entry) => {
                        entry
                            .session_mut()
                            .abort(now, Error::ConnectionClosed, &self.ctx)?
                    }
                    None => continue,
                };
                let command = self.command_sender();
                EventRouter::record_events(
                    &mut self.state,
                    key,
                    events,
                    &command,
                    outbound,
                    pump_events,
                )?;
            }
        }
        Ok(())
    }

    fn shutdown(
        &mut self,
        error: Error,
        outbound: &OutboundSender,
        pump_events: &PumpSender,
    ) -> Result<()> {
        let command = self.command_sender();
        EventRouter::shutdown(&mut self.state, error, &command, outbound, pump_events)
    }

    fn command_sender(&self) -> super::io::CommandSender {
        self.state.command_sender()
    }
}
