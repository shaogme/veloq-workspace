use veloq::std::time::Duration;

use crate::timer::{TimerCommand, TimerKind};

pub(super) struct TimerState {
    handshake_armed: bool,
    ack_armed: bool,
    fin_armed: bool,
}

impl TimerState {
    pub(super) fn new() -> Self {
        Self {
            handshake_armed: false,
            ack_armed: false,
            fin_armed: false,
        }
    }

    pub(super) fn arm(
        &mut self,
        kind: TimerKind,
        generation: u64,
        delay: Duration,
    ) -> TimerCommand {
        self.set_armed(kind, true);
        TimerCommand::Arm {
            kind,
            generation,
            delay,
        }
    }

    pub(super) fn cancel(&mut self, kind: TimerKind, generation: u64) -> Option<TimerCommand> {
        let was_armed = self.is_armed(kind);
        self.set_armed(kind, false);
        if was_armed
            || matches!(
                kind,
                TimerKind::Retransmit { .. }
                    | TimerKind::MessageAckRetry { .. }
                    | TimerKind::ReassemblyTimeout { .. }
                    | TimerKind::StreamOpenRetry { .. }
            )
        {
            Some(TimerCommand::Cancel { kind, generation })
        } else {
            None
        }
    }

    pub(super) fn mark_expired(&mut self, kind: TimerKind, _generation: u64) {
        self.set_armed(kind, false);
    }

    pub(super) fn cancel_all(&mut self, generation: u64) -> [Option<TimerCommand>; 3] {
        [
            self.cancel(TimerKind::HandshakeRetry, generation),
            self.cancel(TimerKind::AckDelay, generation),
            self.cancel(TimerKind::FinRetry, generation),
        ]
    }

    fn is_armed(&self, kind: TimerKind) -> bool {
        match kind {
            TimerKind::HandshakeRetry => self.handshake_armed,
            TimerKind::AckDelay => self.ack_armed,
            TimerKind::FinRetry => self.fin_armed,
            TimerKind::Retransmit { .. }
            | TimerKind::MessageAckRetry { .. }
            | TimerKind::ReassemblyTimeout { .. }
            | TimerKind::StreamOpenRetry { .. } => false,
        }
    }

    fn set_armed(&mut self, kind: TimerKind, armed: bool) {
        match kind {
            TimerKind::HandshakeRetry => self.handshake_armed = armed,
            TimerKind::AckDelay => self.ack_armed = armed,
            TimerKind::FinRetry => self.fin_armed = armed,
            TimerKind::Retransmit { .. }
            | TimerKind::MessageAckRetry { .. }
            | TimerKind::ReassemblyTimeout { .. }
            | TimerKind::StreamOpenRetry { .. } => {}
        }
    }
}
