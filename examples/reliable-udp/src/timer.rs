use veloq::std::time::Duration;

use crate::packet::{FrameSequence, MessageId, StreamId};

/// The logical timers understood by a reliable UDP session.
///
/// Timer IDs are deliberately absent here. They belong to the endpoint's
/// timing wheel and must never become part of the protocol state machine.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TimerKind {
    Retransmit {
        sequence: FrameSequence,
    },
    MessageAckRetry {
        stream_id: StreamId,
        message_id: MessageId,
    },
    ReassemblyTimeout {
        stream_id: StreamId,
        message_id: MessageId,
    },
    StreamOpenRetry {
        stream_id: StreamId,
    },
    HandshakeRetry,
    AckDelay,
    FinRetry,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimerCommand {
    Arm {
        kind: TimerKind,
        generation: u64,
        delay: Duration,
    },
    Cancel {
        kind: TimerKind,
        generation: u64,
    },
}
