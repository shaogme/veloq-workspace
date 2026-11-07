use crate::{driver::CompletionContinuation, op::UdpRecvPacket};
use veloq_std::net::SocketAddr;

use super::{config::ReceivePumpError, permit::ReceivePermit};

/// Logical pump lifecycle.  `RunningWithTerminalError` still permits already-published records
/// to be consumed; the terminal error is observed only after those records are drained.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReceivePumpPhase {
    Created,
    Starting,
    Running,
    PausedByBackpressure,
    /// The previous multishot arm ended, but the logical receiver still owns the operation and
    /// may submit one replacement after its selected leases have settled.
    RearmPending,
    RunningWithTerminalError,
    Closing,
    Drained,
}

/// Lifecycle of one fixed receive buffer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReceiveSlotState {
    Free,
    SubmitPending,
    InFlight,
    CompletedPending,
    /// The completion metadata is retained while the backend still owns its payload lease and
    /// waits for room in the bounded pending queue.
    LeaseHeld,
    Retiring,
}

/// The request identity carried by a backend request context.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReceiveRequest {
    pub request_id: u64,
    pub slot_id: u32,
    pub slot_generation: u32,
}

/// Backend-neutral identity for one completed datagram.
///
/// RIO uses a request slot/generation underneath this key while io_uring uses a CQE completion
/// sequence and a logical rearm generation. Keeping both parts here prevents either backend from
/// manufacturing a packet from metadata after its payload owner has disappeared.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReceivePendingKey {
    logical_receiver_generation: u32,
    completion_sequence: u64,
}

impl ReceivePendingKey {
    pub const fn new(logical_receiver_generation: u32, completion_sequence: u64) -> Self {
        Self {
            logical_receiver_generation,
            completion_sequence,
        }
    }

    pub const fn logical_receiver_generation(self) -> u32 {
        self.logical_receiver_generation
    }

    pub const fn completion_sequence(self) -> u64 {
        self.completion_sequence
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct PendingCompletion {
    pub(super) request: ReceiveRequest,
    pub(super) key: ReceivePendingKey,
    pub(super) len: usize,
    pub(super) remote_addr: SocketAddr,
}

/// A completion that has a permit and is ready for the backend to copy into an output buffer.
///
/// The slot remains unavailable while this value exists. The backend must call
/// [`ReceivePumpState::finish_delivery`] after the replacement submit decision, or
/// [`ReceivePumpState::abort_delivery`] if output-buffer preparation failed.
pub struct ReceiveDelivery {
    request: ReceiveRequest,
    key: ReceivePendingKey,
    len: usize,
    remote_addr: SocketAddr,
    replacement_required: bool,
    permit: Option<ReceivePermit>,
}

pub(super) struct ReceiveDeliveryParts {
    pub(super) request: ReceiveRequest,
    pub(super) key: ReceivePendingKey,
    pub(super) len: usize,
    pub(super) remote_addr: SocketAddr,
    pub(super) replacement_required: bool,
    pub(super) permit: ReceivePermit,
}

impl ReceiveDelivery {
    pub(super) fn new(parts: ReceiveDeliveryParts) -> Self {
        Self {
            request: parts.request,
            key: parts.key,
            len: parts.len,
            remote_addr: parts.remote_addr,
            replacement_required: parts.replacement_required,
            permit: Some(parts.permit),
        }
    }

    pub fn request(&self) -> ReceiveRequest {
        self.request
    }

    pub fn key(&self) -> ReceivePendingKey {
        self.key
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn remote_addr(&self) -> SocketAddr {
        self.remote_addr
    }

    pub fn replacement_required(&self) -> bool {
        self.replacement_required
    }

    pub(super) fn take_permit(&mut self) -> Option<ReceivePermit> {
        self.permit.take()
    }
}

/// A slot selected for a submit.  The backend assigns the request id only after its submit
/// transaction succeeds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReceiveSubmission {
    pub slot_id: u32,
    pub slot_generation: u32,
}

/// A generation reserved for a replacement request before the backend submits it.
///
/// The backend must use this value for the request context.  The reservation is also supplied
/// when the delivery transaction is committed so that the state machine can verify that the
/// submitted request and the slot state refer to the same replacement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReceiveReplacementReservation {
    slot_id: u32,
    slot_generation: u32,
}

impl ReceiveReplacementReservation {
    pub const fn new(slot_id: u32, slot_generation: u32) -> Self {
        Self {
            slot_id,
            slot_generation,
        }
    }

    pub const fn slot_id(self) -> u32 {
        self.slot_id
    }

    pub const fn slot_generation(self) -> u32 {
        self.slot_generation
    }
}

/// Result supplied by a backend for one request.
pub type ReceiveCompletionResult = Result<usize, ReceivePumpError>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReceiveCompletion {
    pub request: ReceiveRequest,
    pub result: ReceiveCompletionResult,
    pub remote_addr: SocketAddr,
}

/// Event returned when the backend hands a completion to the state machine.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReceivePumpEvent {
    Pending {
        request: ReceiveRequest,
        key: ReceivePendingKey,
    },
    /// The backend/pump lease ledger retains the datagram because the core pending queue is full.
    /// No user record exists yet; the owner must call [`ReceivePumpState::resume_pending`] after
    /// capacity becomes available.
    Held {
        request: ReceiveRequest,
        key: ReceivePendingKey,
    },
    /// The current kernel arm ended for an internal reason such as provided-buffer exhaustion.
    /// The logical receiver remains owned and must be rearmed by the backend.
    Retained {
        logical_receiver_generation: u32,
    },
    /// The logical receive has ended while actual requests are still in flight.  The backend must
    /// stop replacement and keep routing every remaining completion until they are drained.
    Drain,
    Final {
        error: ReceivePumpError,
    },
    Ignored,
}

/// Compatibility spelling for callers that describe the input as a completion event.
pub type ReceiveCompletionEvent = ReceivePumpEvent;

/// Result of a replacement submit attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplacementSubmit {
    /// No replacement is submitted because the logical receive pump is already draining.
    NotSubmitted,
    Submitted {
        request_id: u64,
        reservation: ReceiveReplacementReservation,
    },
    Failed {
        reservation: ReceiveReplacementReservation,
    },
}

/// A packet record ready for publication to the core completion mailbox.
pub struct ReceiveRecord {
    pub request: ReceiveRequest,
    pub key: ReceivePendingKey,
    pub packet: UdpRecvPacket,
    pub continuation: CompletionContinuation,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ReceivePumpStats {
    pub receive_requests_submitted: usize,
    pub receive_requests_completed: usize,
    pub receive_requests_rearmed: usize,
    pub receive_backpressure_events: usize,
    pub receive_permit_exhausted: usize,
    pub receive_context_anomaly: usize,
    pub receive_logical_rearms: usize,
}

/// Bounded diagnostics for completions observed by a receive pump.
///
/// Backends may receive several real kernel completions after a logical cancellation.  The pump
/// keeps only aggregate status information so those observations remain available to the final
/// cancellation report without retaining an unbounded per-completion payload.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ReceiveCompletionDiagnostics {
    pub completion_count: usize,
    pub cancelled_completion_count: usize,
    pub total_bytes: usize,
    pub first_error_status: Option<i32>,
    pub last_status: Option<i32>,
    pub error_count: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReceivePumpSnapshot {
    pub phase: ReceivePumpPhase,
    pub target_depth: usize,
    pub actual_in_flight: usize,
    pub pending_completions: usize,
    pub held_completions: usize,
    pub ready_packets: usize,
    pub held_packets: usize,
    pub permits_available: usize,
    pub final_emitted: bool,
    pub logical_receiver_generation: u32,
}
