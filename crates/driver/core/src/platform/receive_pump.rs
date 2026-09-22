//! Platform-independent state and ownership rules for a persistent UDP receive pump.
//!
//! This module deliberately does not know about IOCP, RIO, io_uring, or an operating-system
//! error type.  A backend drives it with three facts only: a receive request was submitted, a
//! request completed, and a replacement request was submitted.  Keeping those transitions here
//! makes the resource invariants testable without booting a driver or an operating system.

use crate::{
    driver::CompletionContinuation,
    op::types::{UdpRecvPacket, UdpRecvPacketBuf},
};
use veloq_buf::FixedBuf;
use veloq_std::{
    boxed::Box,
    collections::VecDeque,
    fmt,
    net::SocketAddr,
    num::NonZeroUsize,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

/// The largest datagram capacity accepted by the platform-neutral validation layer.
pub const MAX_UDP_DATAGRAM_CAPACITY: usize = u16::MAX as usize;

/// Configuration used internally by a backend receive pump.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReceivePumpConfig {
    pub kernel_capacity: NonZeroUsize,
    pub queue_capacity: NonZeroUsize,
    pub datagram_capacity: NonZeroUsize,
    pub close_timeout: Duration,
}

/// Platform-neutral configuration accepted by the UDP facade.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UdpReceiveConfig {
    pub kernel_capacity: NonZeroUsize,
    pub queue_capacity: NonZeroUsize,
    pub datagram_capacity: NonZeroUsize,
    pub close_timeout: Duration,
}

/// Errors returned while validating the platform-neutral UDP receive configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UdpReceiveBuildError {
    InvalidConfig,
}

impl UdpReceiveConfig {
    /// Validate the only configuration invariants shared by all receive backends.
    pub fn validate(self) -> Result<Self, UdpReceiveBuildError> {
        let memory_entries = self
            .queue_capacity
            .get()
            .checked_add(self.kernel_capacity.get());
        let memory_budget =
            memory_entries.and_then(|entries| entries.checked_mul(self.datagram_capacity.get()));
        if self.kernel_capacity.get() > u32::MAX as usize
            || self.datagram_capacity.get() > MAX_UDP_DATAGRAM_CAPACITY
            || self.close_timeout.is_zero()
            || memory_budget.is_none()
        {
            return Err(UdpReceiveBuildError::InvalidConfig);
        }
        Ok(self)
    }

    /// Convert the public contract into the backend state-machine configuration.
    pub const fn into_pump_config(self) -> ReceivePumpConfig {
        ReceivePumpConfig {
            kernel_capacity: self.kernel_capacity,
            queue_capacity: self.queue_capacity,
            datagram_capacity: self.datagram_capacity,
            close_timeout: self.close_timeout,
        }
    }
}

impl From<UdpReceiveConfig> for ReceivePumpConfig {
    fn from(config: UdpReceiveConfig) -> Self {
        config.into_pump_config()
    }
}

/// 非阻塞地通知 owner worker 有 packet permit 已释放。
///
/// 通知器只负责唤醒 owner worker，不得在 packet drop 路径中直接访问 driver、slot 或
/// provided-buffer group。backend 可以据此合并多个通知，并在自己的 worker 上扫描并恢复
/// pending datagram。
pub trait ReceivePermitNotifier: Send + Sync {
    fn notify(&self);
}

impl<F> ReceivePermitNotifier for F
where
    F: Fn() + Send + Sync,
{
    fn notify(&self) {
        self();
    }
}

impl ReceivePumpConfig {
    pub fn validate(self, max_outstanding_receive: usize) -> Result<Self, ReceivePumpError> {
        let public = UdpReceiveConfig {
            kernel_capacity: self.kernel_capacity,
            queue_capacity: self.queue_capacity,
            datagram_capacity: self.datagram_capacity,
            close_timeout: self.close_timeout,
        };
        if self.kernel_capacity.get() > max_outstanding_receive || public.validate().is_err() {
            return Err(ReceivePumpError::ReceiveConfigInvalid);
        }
        Ok(self)
    }

    /// Maximum payload memory for user-held output buffers plus backend-held pending payloads.
    pub fn memory_budget_bytes(self) -> Result<usize, ReceivePumpError> {
        self.queue_capacity
            .get()
            .checked_add(self.kernel_capacity.get())
            .and_then(|entries| entries.checked_mul(self.datagram_capacity.get()))
            .ok_or(ReceivePumpError::ReceiveConfigInvalid)
    }

    pub const fn pending_capacity(self) -> usize {
        self.kernel_capacity.get()
    }
}

/// Errors that affect pump transitions or become the terminal error returned after the final
/// already-completed datagram.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReceivePumpError {
    ReceiveConfigInvalid,
    ReceiveAlreadyOwned,
    ReceiverNotReady,
    InitialReceiveSubmitFailed,
    ReplacementSubmitFailed,
    ReceiveQueueStalled,
    ReceiveBufferExhausted,
    ReceiveEof,
    ReceiveOsError { code: i32 },
    ReceiveCancelled,
    ReceiveContextCorrupt,
    DriverShutdown,
    InvalidTransition,
}

impl fmt::Display for ReceivePumpError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ReceiveConfigInvalid => f.write_str("invalid receive configuration"),
            Self::ReceiveAlreadyOwned => f.write_str("socket receive is already owned"),
            Self::ReceiverNotReady => f.write_str("receiver is not ready"),
            Self::InitialReceiveSubmitFailed => f.write_str("initial receive submit failed"),
            Self::ReplacementSubmitFailed => f.write_str("replacement receive submit failed"),
            Self::ReceiveQueueStalled => f.write_str("receive queue is stalled"),
            Self::ReceiveBufferExhausted => f.write_str("receive output buffer is exhausted"),
            Self::ReceiveEof => f.write_str("receive reached EOF"),
            Self::ReceiveOsError { code } => write!(f, "receive OS error ({code})"),
            Self::ReceiveCancelled => f.write_str("receive was cancelled"),
            Self::ReceiveContextCorrupt => f.write_str("receive request context is corrupt"),
            Self::DriverShutdown => f.write_str("driver is shutting down"),
            Self::InvalidTransition => f.write_str("invalid receive pump transition"),
        }
    }
}

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
    pub const fn logical_receiver_generation(self) -> u32 {
        self.logical_receiver_generation
    }

    pub const fn completion_sequence(self) -> u64 {
        self.completion_sequence
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

/// A completion that has a permit and is ready for the backend to copy into an output buffer.
///
/// The slot remains unavailable while this value exists.  The backend must call
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

impl ReceiveDelivery {
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
}

/// A packet record ready for publication to the core completion mailbox.
pub struct ReceiveRecord {
    pub request: ReceiveRequest,
    pub key: ReceivePendingKey,
    pub packet: UdpRecvPacket,
    pub continuation: CompletionContinuation,
}

/// A fixed receive slot.  Its buffer never moves while the pump exists.
pub struct ReceiveSlot {
    slot_id: u32,
    generation: u32,
    data_buf: FixedBuf,
    state: ReceiveSlotState,
    completed_len: usize,
    completed_addr: Option<SocketAddr>,
    current_request: Option<ReceiveRequest>,
}

impl ReceiveSlot {
    pub fn new(slot_id: u32, data_buf: FixedBuf) -> Self {
        Self {
            slot_id,
            generation: 0,
            data_buf,
            state: ReceiveSlotState::Free,
            completed_len: 0,
            completed_addr: None,
            current_request: None,
        }
    }

    pub fn slot_id(&self) -> u32 {
        self.slot_id
    }

    pub fn generation(&self) -> u32 {
        self.generation
    }

    pub fn state(&self) -> ReceiveSlotState {
        self.state
    }

    pub fn data_buf(&self) -> &FixedBuf {
        &self.data_buf
    }

    pub fn data_buf_mut(&mut self) -> &mut FixedBuf {
        &mut self.data_buf
    }

    pub fn completed_len(&self) -> Option<usize> {
        matches!(
            self.state,
            ReceiveSlotState::CompletedPending
                | ReceiveSlotState::LeaseHeld
                | ReceiveSlotState::SubmitPending
        )
        .then_some(self.completed_len)
    }

    /// View bytes written by the kernel while the slot is no longer in flight.
    ///
    /// The RIO receive path writes directly into the fixed buffer and reports the byte count
    /// separately, so the buffer's logical length is intentionally not changed here.
    pub fn completed_data(&self) -> Option<&[u8]> {
        let len = self.completed_len()?;
        Some(unsafe { core::slice::from_raw_parts(self.data_buf.as_ptr(), len) })
    }

    pub fn current_request(&self) -> Option<ReceiveRequest> {
        self.current_request
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PendingCompletion {
    request: ReceiveRequest,
    key: ReceivePendingKey,
    len: usize,
    remote_addr: SocketAddr,
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

/// A bounded permit pool shared by the pump and every packet it publishes.
#[derive(Clone)]
pub struct ReceivePermitPool {
    inner: Arc<PermitState>,
}

struct PermitState {
    capacity: usize,
    available: AtomicUsize,
    notifier: Option<Arc<dyn ReceivePermitNotifier>>,
}

impl ReceivePermitPool {
    pub fn new(capacity: NonZeroUsize) -> Self {
        Self::new_with_notifier(capacity, None)
    }

    pub fn new_with_notifier(
        capacity: NonZeroUsize,
        notifier: Option<Arc<dyn ReceivePermitNotifier>>,
    ) -> Self {
        Self {
            inner: Arc::new(PermitState {
                capacity: capacity.get(),
                available: AtomicUsize::new(capacity.get()),
                notifier,
            }),
        }
    }

    pub fn capacity(&self) -> usize {
        self.inner.capacity
    }

    pub fn available(&self) -> usize {
        self.inner.available.load(Ordering::Acquire)
    }

    pub fn in_use(&self) -> usize {
        self.capacity() - self.available()
    }

    fn acquire(&self) -> Option<ReceivePermit> {
        let mut available = self.available();
        loop {
            if available == 0 {
                return None;
            }
            match self.inner.available.compare_exchange(
                available,
                available - 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    return Some(ReceivePermit {
                        pool: Some(self.inner.clone()),
                    });
                }
                Err(observed) => available = observed,
            }
        }
    }
}

/// A permit held by one packet or one in-progress delivery.
pub struct ReceivePermit {
    pool: Option<Arc<PermitState>>,
}

impl ReceivePermit {
    pub(crate) fn detached() -> Self {
        Self { pool: None }
    }

    pub fn release(self) {
        drop(self);
    }
}

impl Drop for ReceivePermit {
    fn drop(&mut self) {
        if let Some(pool) = self.pool.take() {
            pool.available.fetch_add(1, Ordering::Release);
            if let Some(notifier) = &pool.notifier {
                notifier.notify();
            }
        }
    }
}

/// 一个 io_uring multishot datagram admission 的身份。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DatagramAdmission {
    key: ReceivePendingKey,
}

impl DatagramAdmission {
    pub const fn key(self) -> ReceivePendingKey {
        self.key
    }
}

/// 已经按 FIFO 顺序取得 permit、等待 backend copy 的 datagram。
pub struct DatagramDelivery {
    key: ReceivePendingKey,
    len: usize,
    remote_addr: SocketAddr,
    permit: Option<ReceivePermit>,
}

impl DatagramDelivery {
    pub const fn key(&self) -> ReceivePendingKey {
        self.key
    }

    pub const fn len(&self) -> usize {
        self.len
    }

    pub const fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub const fn remote_addr(&self) -> SocketAddr {
        self.remote_addr
    }
}

/// The pure state machine for one logical UDP receive operation.
pub struct ReceivePumpState {
    config: ReceivePumpConfig,
    phase: ReceivePumpPhase,
    slots: Box<[ReceiveSlot]>,
    permits: ReceivePermitPool,
    pending: VecDeque<PendingCompletion>,
    held: VecDeque<PendingCompletion>,
    prepared: Option<ReceiveRequest>,
    actual_in_flight: usize,
    ready_packets: usize,
    terminal_error: Option<ReceivePumpError>,
    eof_on_empty: bool,
    logical_receiver_generation: u32,
    next_completion_sequence: u64,
    final_emitted: bool,
    stats: ReceivePumpStats,
    completion_diagnostics: ReceiveCompletionDiagnostics,
    multishot: bool,
    multishot_pending: VecDeque<PendingCompletion>,
}

impl ReceivePumpState {
    pub fn try_new(
        config: ReceivePumpConfig,
        slots: Box<[ReceiveSlot]>,
    ) -> Result<Self, ReceivePumpError> {
        Self::try_new_with_eof(config, slots, false)
    }

    /// Construct a pump for a stream receive operation where an empty successful completion is
    /// the terminal EOF record rather than a zero-length datagram.
    pub fn try_new_tcp(
        config: ReceivePumpConfig,
        slots: Box<[ReceiveSlot]>,
    ) -> Result<Self, ReceivePumpError> {
        Self::try_new_with_eof(config, slots, true)
    }

    /// Construct the platform-independent state for one io_uring multishot datagram operation.
    ///
    /// Unlike the RIO request model this mode has no fixed receive slots. The provided-buffer
    /// lease remains owned by the backend while this state machine owns only bounded datagram
    /// metadata and packet permits.
    pub fn try_new_multishot(
        config: ReceivePumpConfig,
        notifier: Option<Arc<dyn ReceivePermitNotifier>>,
    ) -> Result<Self, ReceivePumpError> {
        config.validate(config.kernel_capacity.get())?;
        Self::try_new_with_options(config, Box::new([]), false, true, notifier)
    }

    fn try_new_with_eof(
        config: ReceivePumpConfig,
        slots: Box<[ReceiveSlot]>,
        eof_on_empty: bool,
    ) -> Result<Self, ReceivePumpError> {
        Self::try_new_with_options(config, slots, eof_on_empty, false, None)
    }

    fn try_new_with_options(
        config: ReceivePumpConfig,
        slots: Box<[ReceiveSlot]>,
        eof_on_empty: bool,
        multishot: bool,
        notifier: Option<Arc<dyn ReceivePermitNotifier>>,
    ) -> Result<Self, ReceivePumpError> {
        config.validate(if multishot {
            config.kernel_capacity.get()
        } else {
            slots.len()
        })?;
        if !multishot && slots.len() != config.kernel_capacity.get() {
            return Err(ReceivePumpError::ReceiveConfigInvalid);
        }
        if multishot && !slots.is_empty() {
            return Err(ReceivePumpError::ReceiveConfigInvalid);
        }
        for (index, slot) in slots.iter().enumerate() {
            if slot.data_buf.capacity() < config.datagram_capacity.get() {
                return Err(ReceivePumpError::ReceiveConfigInvalid);
            }
            if slots[..index]
                .iter()
                .any(|previous| previous.slot_id == slot.slot_id)
            {
                return Err(ReceivePumpError::ReceiveConfigInvalid);
            }
        }

        let pending_capacity = config.kernel_capacity.get();
        Ok(Self {
            config,
            phase: ReceivePumpPhase::Created,
            slots,
            permits: ReceivePermitPool::new_with_notifier(config.queue_capacity, notifier),
            pending: VecDeque::with_capacity(pending_capacity),
            held: VecDeque::with_capacity(pending_capacity),
            prepared: None,
            actual_in_flight: 0,
            ready_packets: 0,
            terminal_error: None,
            eof_on_empty,
            logical_receiver_generation: 0,
            next_completion_sequence: 0,
            final_emitted: false,
            stats: ReceivePumpStats::default(),
            completion_diagnostics: ReceiveCompletionDiagnostics::default(),
            multishot,
            multishot_pending: VecDeque::with_capacity(pending_capacity),
        })
    }

    pub fn config(&self) -> ReceivePumpConfig {
        self.config
    }

    pub fn phase(&self) -> ReceivePumpPhase {
        self.phase
    }

    pub fn stats(&self) -> ReceivePumpStats {
        self.stats
    }

    pub fn completion_diagnostics(&self) -> ReceiveCompletionDiagnostics {
        self.completion_diagnostics
    }

    /// Record the raw status observed by a backend before logical result classification.
    pub fn record_completion_observation(&mut self, status: i32, bytes: u32, user_cancelled: bool) {
        let diagnostics = &mut self.completion_diagnostics;
        diagnostics.completion_count = diagnostics.completion_count.saturating_add(1);
        diagnostics.total_bytes = diagnostics.total_bytes.saturating_add(bytes as usize);
        diagnostics.last_status = Some(status);
        if user_cancelled {
            diagnostics.cancelled_completion_count =
                diagnostics.cancelled_completion_count.saturating_add(1);
        }
        if status != 0 {
            diagnostics.error_count = diagnostics.error_count.saturating_add(1);
            if diagnostics.first_error_status.is_none() {
                diagnostics.first_error_status = Some(status);
            }
        }
    }

    pub fn actual_in_flight(&self) -> usize {
        self.actual_in_flight
    }

    pub fn pending_completions(&self) -> usize {
        self.pending.len()
    }

    /// Number of completion metadata entries retained while their backend payload lease waits
    /// for room in the bounded pending queue.
    pub fn held_completions(&self) -> usize {
        self.held.len()
    }

    pub fn ready_packets(&self) -> usize {
        self.ready_packets
    }

    pub fn held_packets(&self) -> usize {
        self.permits.in_use().saturating_sub(self.ready_packets)
    }

    pub fn permits_available(&self) -> usize {
        self.permits.available()
    }

    pub fn is_multishot(&self) -> bool {
        self.multishot
    }

    /// Arm the logical multishot receiver after its SQE has been accepted by the backend.
    pub fn arm_multishot(&mut self) -> Result<u32, ReceivePumpError> {
        if !self.multishot || !matches!(self.phase, ReceivePumpPhase::Created) {
            return Err(ReceivePumpError::InvalidTransition);
        }
        let generation = self
            .logical_receiver_generation
            .checked_add(1)
            .ok_or(ReceivePumpError::ReceiveContextCorrupt)?;
        self.logical_receiver_generation = generation;
        self.phase = ReceivePumpPhase::Running;
        Ok(generation)
    }

    /// 将一个完整 datagram 的 metadata 按完成顺序提交到 multishot FIFO。
    pub fn admit_datagram(
        &mut self,
        len: usize,
        remote_addr: SocketAddr,
    ) -> Result<DatagramAdmission, ReceivePumpError> {
        if !self.multishot
            || !matches!(
                self.phase,
                ReceivePumpPhase::Running | ReceivePumpPhase::PausedByBackpressure
            )
        {
            return Err(ReceivePumpError::InvalidTransition);
        }
        if len > self.config.datagram_capacity.get()
            || self.multishot_pending.len() >= self.config.pending_capacity()
        {
            self.pause_for_backpressure();
            return Err(ReceivePumpError::ReceiveQueueStalled);
        }
        let completion_sequence = self
            .next_completion_sequence
            .checked_add(1)
            .ok_or(ReceivePumpError::ReceiveContextCorrupt)?;
        self.next_completion_sequence = completion_sequence;
        let key = ReceivePendingKey {
            logical_receiver_generation: self.logical_receiver_generation,
            completion_sequence,
        };
        self.multishot_pending.push_back(PendingCompletion {
            request: ReceiveRequest {
                request_id: completion_sequence,
                slot_id: u32::MAX,
                slot_generation: self.logical_receiver_generation,
            },
            key,
            len,
            remote_addr,
        });
        if self.permits.available() == 0 {
            self.pause_for_backpressure();
        }
        Ok(DatagramAdmission { key })
    }

    /// 取得最旧的 multishot metadata 和一个 packet permit。
    pub fn next_datagram_delivery(&mut self) -> Option<DatagramDelivery> {
        if !self.multishot
            || !matches!(
                self.phase,
                ReceivePumpPhase::Running
                    | ReceivePumpPhase::PausedByBackpressure
                    | ReceivePumpPhase::RearmPending
            )
        {
            return None;
        }
        if self.multishot_pending.is_empty() {
            return None;
        }
        let permit = match self.permits.acquire() {
            Some(permit) => permit,
            None => {
                self.pause_for_backpressure();
                return None;
            }
        };
        let pending = self.multishot_pending.pop_front()?;
        self.resume_if_possible();
        Some(DatagramDelivery {
            key: pending.key,
            len: pending.len,
            remote_addr: pending.remote_addr,
            permit: Some(permit),
        })
    }

    pub fn finish_datagram_delivery(
        &mut self,
        mut delivery: DatagramDelivery,
        mut output: FixedBuf,
    ) -> Result<UdpRecvPacket, ReceivePumpError> {
        if output.capacity() < delivery.len {
            self.multishot_pending.push_front(PendingCompletion {
                request: ReceiveRequest {
                    request_id: delivery.key.completion_sequence,
                    slot_id: u32::MAX,
                    slot_generation: delivery.key.logical_receiver_generation,
                },
                key: delivery.key,
                len: delivery.len,
                remote_addr: delivery.remote_addr,
            });
            drop(delivery);
            self.pause_for_backpressure();
            return Err(ReceivePumpError::ReceiveQueueStalled);
        }
        output.set_len(delivery.len);
        let permit = delivery
            .permit
            .take()
            .ok_or(ReceivePumpError::InvalidTransition)?;
        Ok(UdpRecvPacket {
            buf: UdpRecvPacketBuf::from_fixed_buf_with_permit(output, permit),
            addr: delivery.remote_addr,
        })
    }

    pub fn abort_datagram_delivery(
        &mut self,
        delivery: DatagramDelivery,
    ) -> Result<(), ReceivePumpError> {
        self.multishot_pending.push_front(PendingCompletion {
            request: ReceiveRequest {
                request_id: delivery.key.completion_sequence,
                slot_id: u32::MAX,
                slot_generation: delivery.key.logical_receiver_generation,
            },
            key: delivery.key,
            len: delivery.len,
            remote_addr: delivery.remote_addr,
        });
        drop(delivery);
        Ok(())
    }

    pub fn pending_datagrams(&self) -> usize {
        self.multishot_pending.len()
    }

    pub fn rearm_pending(&self) -> bool {
        self.multishot && self.phase == ReceivePumpPhase::RearmPending
    }

    pub fn clear_multishot_pending(&mut self) -> usize {
        let count = self.multishot_pending.len();
        self.multishot_pending.clear();
        self.maybe_drained();
        count
    }

    pub fn terminal_error(&self) -> Option<ReceivePumpError> {
        self.terminal_error
    }

    pub fn logical_receiver_generation(&self) -> u32 {
        self.logical_receiver_generation
    }

    /// Retain the logical receive operation after a backend arm ended without a user-visible
    /// terminal error. The backend must call [`Self::submit_rearm`] only after all eligible
    /// selected leases have settled.
    pub fn retain_for_rearm(&mut self) -> Result<ReceivePumpEvent, ReceivePumpError> {
        if !matches!(
            self.phase,
            ReceivePumpPhase::Running | ReceivePumpPhase::PausedByBackpressure
        ) {
            return Err(ReceivePumpError::InvalidTransition);
        }
        let Some(generation) = self.logical_receiver_generation.checked_add(1) else {
            self.set_terminal(ReceivePumpError::ReceiveContextCorrupt);
            return self.emit_final_or_drain();
        };
        self.logical_receiver_generation = generation;
        self.phase = ReceivePumpPhase::RearmPending;
        Ok(ReceivePumpEvent::Retained {
            logical_receiver_generation: generation,
        })
    }

    /// Commit a successful logical rearm without creating a second receiver claim.
    pub fn submit_rearm(&mut self) -> Result<(), ReceivePumpError> {
        if self.phase != ReceivePumpPhase::RearmPending {
            return Err(ReceivePumpError::InvalidTransition);
        }
        self.phase = ReceivePumpPhase::Running;
        self.stats.receive_logical_rearms = self.stats.receive_logical_rearms.saturating_add(1);
        Ok(())
    }

    pub fn snapshot(&self) -> ReceivePumpSnapshot {
        ReceivePumpSnapshot {
            phase: self.phase,
            target_depth: self.config.kernel_capacity.get(),
            actual_in_flight: self.actual_in_flight,
            pending_completions: self.pending.len(),
            held_completions: self.held.len(),
            ready_packets: self.ready_packets,
            held_packets: self.held_packets(),
            permits_available: self.permits.available(),
            final_emitted: self.final_emitted,
            logical_receiver_generation: self.logical_receiver_generation,
        }
    }

    pub fn slot(&self, slot_id: u32) -> Option<&ReceiveSlot> {
        self.slot_index(slot_id).map(|index| &self.slots[index])
    }

    pub fn is_ready(&self) -> bool {
        matches!(
            self.phase,
            ReceivePumpPhase::Running
                | ReceivePumpPhase::PausedByBackpressure
                | ReceivePumpPhase::RearmPending
        )
    }

    pub fn start(&mut self) -> Result<(), ReceivePumpError> {
        if self.phase != ReceivePumpPhase::Created {
            return Err(ReceivePumpError::InvalidTransition);
        }
        self.phase = ReceivePumpPhase::Starting;
        Ok(())
    }

    /// Select the next fixed slot for an initial submit.
    pub fn next_submission(&mut self) -> Option<ReceiveSubmission> {
        if self.phase != ReceivePumpPhase::Starting {
            return None;
        }
        let index = self
            .slots
            .iter()
            .position(|slot| slot.state == ReceiveSlotState::Free)?;
        let slot = &mut self.slots[index];
        slot.state = ReceiveSlotState::SubmitPending;
        Some(ReceiveSubmission {
            slot_id: slot.slot_id,
            slot_generation: slot.generation,
        })
    }

    /// Commit a successful initial submit.  Readiness is reached only after the target depth is
    /// fully in flight.
    pub fn submit_initial(
        &mut self,
        submission: ReceiveSubmission,
        request_id: u64,
    ) -> Result<ReceiveRequest, ReceivePumpError> {
        if self.phase != ReceivePumpPhase::Starting {
            return Err(ReceivePumpError::InvalidTransition);
        }
        let index = self.validate_submission(submission)?;
        let request = ReceiveRequest {
            request_id,
            slot_id: submission.slot_id,
            slot_generation: submission.slot_generation,
        };
        let slot = &mut self.slots[index];
        slot.state = ReceiveSlotState::InFlight;
        slot.current_request = Some(request);
        self.actual_in_flight += 1;
        self.stats.receive_requests_submitted += 1;
        self.try_enter_running();
        Ok(request)
    }

    /// Abort startup after a submit failed.  Already-submitted requests remain owned and must be
    /// drained through their completions before the pump can be reclaimed.
    pub fn fail_initial_submit(
        &mut self,
        submission: ReceiveSubmission,
    ) -> Result<ReceivePumpEvent, ReceivePumpError> {
        if self.phase != ReceivePumpPhase::Starting {
            return Err(ReceivePumpError::InvalidTransition);
        }
        let index = self.validate_submission(submission)?;
        self.slots[index].state = ReceiveSlotState::Retiring;
        self.terminal_error = Some(ReceivePumpError::InitialReceiveSubmitFailed);
        self.phase = ReceivePumpPhase::Closing;
        self.emit_final_if_possible()
    }

    /// Route one backend completion through the state machine.
    pub fn complete(
        &mut self,
        completion: ReceiveCompletion,
    ) -> Result<ReceivePumpEvent, ReceivePumpError> {
        let Some(index) = self.slot_index(completion.request.slot_id) else {
            self.stats.receive_context_anomaly += 1;
            return Err(ReceivePumpError::ReceiveContextCorrupt);
        };
        let slot = &mut self.slots[index];
        if slot.state != ReceiveSlotState::InFlight
            || slot.current_request != Some(completion.request)
        {
            self.stats.receive_context_anomaly += 1;
            return Err(ReceivePumpError::ReceiveContextCorrupt);
        }
        if !matches!(
            self.phase,
            ReceivePumpPhase::Closing | ReceivePumpPhase::RunningWithTerminalError
        ) && self.pending.len() >= self.config.pending_capacity()
            && self.held.len() >= self.config.pending_capacity()
        {
            self.pause_for_backpressure();
            return Err(ReceivePumpError::ReceiveQueueStalled);
        }
        slot.current_request = None;
        self.actual_in_flight -= 1;
        self.stats.receive_requests_completed += 1;

        if matches!(
            self.phase,
            ReceivePumpPhase::Closing | ReceivePumpPhase::RunningWithTerminalError
        ) {
            slot.state = ReceiveSlotState::Retiring;
            slot.completed_len = 0;
            slot.completed_addr = None;
            self.promote_held();
            return match self.oldest_pending_event() {
                Some(event) => Ok(event),
                None => self.emit_final_or_drain(),
            };
        }
        match completion.result {
            Ok(len) if len <= slot.data_buf.capacity() => {
                let Some(completion_sequence) = self.next_completion_sequence.checked_add(1) else {
                    slot.state = ReceiveSlotState::Retiring;
                    self.set_terminal(ReceivePumpError::ReceiveContextCorrupt);
                    return self.emit_final_or_drain();
                };
                self.next_completion_sequence = completion_sequence;
                let key = ReceivePendingKey {
                    logical_receiver_generation: self.logical_receiver_generation,
                    completion_sequence,
                };
                slot.completed_len = len;
                slot.completed_addr = Some(completion.remote_addr);
                let pending = PendingCompletion {
                    request: completion.request,
                    key,
                    len,
                    remote_addr: completion.remote_addr,
                };
                let pending_capacity = self.config.pending_capacity();
                if self.pending.len() >= pending_capacity {
                    slot.state = ReceiveSlotState::LeaseHeld;
                    self.held.push_back(pending);
                    self.pause_for_backpressure();
                    return Ok(ReceivePumpEvent::Held {
                        request: completion.request,
                        key,
                    });
                }
                slot.state = ReceiveSlotState::CompletedPending;
                self.pending.push_back(pending);
                if len == 0 && self.eof_on_empty {
                    self.set_terminal(ReceivePumpError::ReceiveEof);
                }
                if self.permits.available() == 0 {
                    self.pause_for_backpressure();
                }
                Ok(ReceivePumpEvent::Pending {
                    request: completion.request,
                    key,
                })
            }
            Ok(_) => {
                slot.state = ReceiveSlotState::Retiring;
                self.set_terminal(ReceivePumpError::ReceiveContextCorrupt);
                match self.oldest_pending_event() {
                    Some(event) => Ok(event),
                    None => self.emit_final_or_drain(),
                }
            }
            Err(error) => {
                slot.state = ReceiveSlotState::Retiring;
                self.set_terminal(error);
                match self.oldest_pending_event() {
                    Some(event) => Ok(event),
                    None => self.emit_final_or_drain(),
                }
            }
        }
    }

    /// Move the oldest pending datagram into the copy/replacement phase if a permit is available.
    pub fn next_delivery(&mut self) -> Option<ReceiveDelivery> {
        self.promote_held();
        if self.prepared.is_some()
            || !matches!(
                self.phase,
                ReceivePumpPhase::Running
                    | ReceivePumpPhase::PausedByBackpressure
                    | ReceivePumpPhase::RunningWithTerminalError
            )
        {
            return None;
        }
        let permit = match self.permits.acquire() {
            Some(permit) => permit,
            None => {
                self.pause_for_backpressure();
                return None;
            }
        };
        let pending = self.pending.pop_front()?;
        let Some(index) = self.slot_index(pending.request.slot_id) else {
            drop(permit);
            self.stats.receive_context_anomaly += 1;
            self.set_terminal(ReceivePumpError::ReceiveContextCorrupt);
            return None;
        };
        let slot = &mut self.slots[index];
        if slot.state != ReceiveSlotState::CompletedPending {
            drop(permit);
            self.stats.receive_context_anomaly += 1;
            self.set_terminal(ReceivePumpError::ReceiveContextCorrupt);
            return None;
        }
        slot.state = ReceiveSlotState::SubmitPending;
        self.prepared = Some(pending.request);
        self.resume_if_possible();
        Some(ReceiveDelivery {
            request: pending.request,
            key: pending.key,
            len: pending.len,
            remote_addr: pending.remote_addr,
            replacement_required: matches!(
                self.phase,
                ReceivePumpPhase::Running | ReceivePumpPhase::PausedByBackpressure
            ),
            permit: Some(permit),
        })
    }

    /// Promote retained completion metadata after a backend payload lease becomes eligible for
    /// delivery.  The operation stays owned and no empty user record is created.
    pub fn resume_pending(&mut self) -> Result<usize, ReceivePumpError> {
        if matches!(
            self.phase,
            ReceivePumpPhase::Closing | ReceivePumpPhase::Drained
        ) {
            return Err(ReceivePumpError::InvalidTransition);
        }
        let promoted = self.promote_held();
        self.resume_if_possible();
        Ok(promoted)
    }

    /// Reserve the next generation before submitting a replacement request to the backend.
    pub fn reserve_replacement(
        &mut self,
        delivery: &ReceiveDelivery,
    ) -> Result<ReceiveReplacementReservation, ReceivePumpError> {
        if self.prepared != Some(delivery.request) {
            return Err(ReceivePumpError::InvalidTransition);
        }
        let Some(index) = self.slot_index(delivery.request.slot_id) else {
            return Err(ReceivePumpError::ReceiveContextCorrupt);
        };
        let slot = &mut self.slots[index];
        if slot.state != ReceiveSlotState::SubmitPending
            || slot.generation != delivery.request.slot_generation
        {
            return Err(ReceivePumpError::ReceiveContextCorrupt);
        }
        let Some(slot_generation) = slot.generation.checked_add(1) else {
            return Err(ReceivePumpError::ReceiveContextCorrupt);
        };
        slot.generation = slot_generation;
        Ok(ReceiveReplacementReservation {
            slot_id: slot.slot_id,
            slot_generation,
        })
    }

    /// Complete the copy/replacement transaction.  A successful replacement is committed before
    /// the returned record is marked `More`; a failure publishes the current packet as `Final`.
    pub fn finish_delivery(
        &mut self,
        mut delivery: ReceiveDelivery,
        mut output: FixedBuf,
        replacement: ReplacementSubmit,
    ) -> Result<ReceiveRecord, ReceivePumpError> {
        if self.prepared != Some(delivery.request) {
            return Err(ReceivePumpError::InvalidTransition);
        }
        if !delivery.replacement_required && !matches!(replacement, ReplacementSubmit::NotSubmitted)
        {
            return Err(ReceivePumpError::InvalidTransition);
        }
        let Some(index) = self.slot_index(delivery.request.slot_id) else {
            return Err(ReceivePumpError::ReceiveContextCorrupt);
        };
        let slot = &mut self.slots[index];
        if slot.state != ReceiveSlotState::SubmitPending {
            return Err(ReceivePumpError::ReceiveContextCorrupt);
        }
        let reservation = match replacement {
            ReplacementSubmit::NotSubmitted => None,
            ReplacementSubmit::Submitted { reservation, .. }
            | ReplacementSubmit::Failed { reservation } => {
                if reservation.slot_id != slot.slot_id
                    || reservation.slot_generation != slot.generation
                    || reservation.slot_generation == delivery.request.slot_generation
                {
                    return Err(ReceivePumpError::ReceiveContextCorrupt);
                }
                Some(reservation)
            }
        };
        self.prepared = None;
        if output.capacity() < delivery.len {
            if reservation.is_some() {
                slot.generation = delivery.request.slot_generation;
            }
            slot.state = ReceiveSlotState::CompletedPending;
            self.pending.push_front(PendingCompletion {
                request: delivery.request,
                key: delivery.key,
                len: delivery.len,
                remote_addr: delivery.remote_addr,
            });
            drop(delivery);
            self.pause_for_backpressure();
            return Err(ReceivePumpError::ReceiveQueueStalled);
        }
        output.set_len(delivery.len);
        let permit = delivery
            .permit
            .take()
            .ok_or(ReceivePumpError::InvalidTransition)?;
        let packet = UdpRecvPacket {
            buf: UdpRecvPacketBuf::from_fixed_buf_with_permit(output, permit),
            addr: delivery.remote_addr,
        };

        match replacement {
            ReplacementSubmit::Submitted { request_id, .. } => {
                let reservation = reservation.expect("validated replacement reservation");
                let request = ReceiveRequest {
                    request_id,
                    slot_id: slot.slot_id,
                    slot_generation: reservation.slot_generation,
                };
                slot.current_request = Some(request);
                slot.state = ReceiveSlotState::InFlight;
                slot.completed_addr = None;
                slot.completed_len = 0;
                self.actual_in_flight += 1;
                self.stats.receive_requests_submitted += 1;
                self.stats.receive_requests_rearmed += 1;
                self.ready_packets += 1;
                self.resume_if_possible();
                Ok(ReceiveRecord {
                    request: delivery.request,
                    key: delivery.key,
                    packet,
                    continuation: CompletionContinuation::More,
                })
            }
            ReplacementSubmit::Failed { .. } => {
                slot.state = ReceiveSlotState::Retiring;
                self.set_terminal(ReceivePumpError::ReplacementSubmitFailed);
                let final_now =
                    self.actual_in_flight == 0 && self.pending.is_empty() && self.held.is_empty();
                self.final_emitted = final_now;
                self.ready_packets += 1;
                Ok(ReceiveRecord {
                    request: delivery.request,
                    key: delivery.key,
                    packet,
                    continuation: if final_now {
                        CompletionContinuation::Final
                    } else {
                        CompletionContinuation::More
                    },
                })
            }
            ReplacementSubmit::NotSubmitted => {
                slot.state = ReceiveSlotState::Retiring;
                let final_now =
                    self.actual_in_flight == 0 && self.pending.is_empty() && self.held.is_empty();
                self.final_emitted = final_now;
                self.ready_packets += 1;
                Ok(ReceiveRecord {
                    request: delivery.request,
                    key: delivery.key,
                    packet,
                    continuation: if final_now {
                        CompletionContinuation::Final
                    } else {
                        CompletionContinuation::More
                    },
                })
            }
        }
    }

    /// Return an output allocation to the pending queue after a copy/allocation failure.
    pub fn abort_delivery(
        &mut self,
        delivery: ReceiveDelivery,
        error: ReceivePumpError,
    ) -> Result<(), ReceivePumpError> {
        if self.prepared != Some(delivery.request) {
            return Err(ReceivePumpError::InvalidTransition);
        }
        self.prepared = None;
        let Some(index) = self.slot_index(delivery.request.slot_id) else {
            return Err(ReceivePumpError::ReceiveContextCorrupt);
        };
        self.slots[index].state = ReceiveSlotState::CompletedPending;
        self.pending.push_front(PendingCompletion {
            request: delivery.request,
            key: delivery.key,
            len: delivery.len,
            remote_addr: delivery.remote_addr,
        });
        drop(delivery);
        if matches!(error, ReceivePumpError::ReceiveQueueStalled) {
            self.pause_for_backpressure();
        } else {
            self.set_terminal(error);
        }
        Ok(())
    }

    pub fn record_consumed(&mut self) -> Result<(), ReceivePumpError> {
        if self.ready_packets == 0 {
            return Err(ReceivePumpError::InvalidTransition);
        }
        self.ready_packets -= 1;
        self.promote_held();
        self.resume_if_possible();
        self.maybe_drained();
        Ok(())
    }

    /// Request cancellation.  The pump remains alive until every actual request has completed.
    pub fn cancel(&mut self) -> Result<ReceivePumpEvent, ReceivePumpError> {
        if self.phase == ReceivePumpPhase::Drained {
            return Ok(ReceivePumpEvent::Ignored);
        }
        self.phase = ReceivePumpPhase::Closing;
        self.set_terminal(ReceivePumpError::ReceiveCancelled);
        self.retire_pending();
        self.prepared = None;
        self.emit_final_or_drain()
    }

    pub fn acknowledge_final(&mut self) -> Result<(), ReceivePumpError> {
        if !self.final_emitted {
            return Err(ReceivePumpError::InvalidTransition);
        }
        self.maybe_drained();
        Ok(())
    }

    /// Poll the terminal transition after previously admitted packets have been delivered.
    pub fn poll_final(&mut self) -> Result<ReceivePumpEvent, ReceivePumpError> {
        self.emit_final_or_drain()
    }

    fn slot_index(&self, slot_id: u32) -> Option<usize> {
        self.slots.iter().position(|slot| slot.slot_id == slot_id)
    }

    fn oldest_pending_event(&self) -> Option<ReceivePumpEvent> {
        self.pending
            .front()
            .map(|pending| ReceivePumpEvent::Pending {
                request: pending.request,
                key: pending.key,
            })
    }

    fn promote_held(&mut self) -> usize {
        let mut promoted = 0;
        while self.pending.len() < self.config.pending_capacity() {
            let Some(held) = self.held.pop_front() else {
                break;
            };
            let Some(index) = self.slot_index(held.request.slot_id) else {
                self.stats.receive_context_anomaly += 1;
                self.set_terminal(ReceivePumpError::ReceiveContextCorrupt);
                break;
            };
            let slot = &mut self.slots[index];
            if slot.state != ReceiveSlotState::LeaseHeld
                || slot.generation != held.request.slot_generation
            {
                self.stats.receive_context_anomaly += 1;
                self.set_terminal(ReceivePumpError::ReceiveContextCorrupt);
                break;
            }
            slot.state = ReceiveSlotState::CompletedPending;
            self.pending.push_back(held);
            promoted += 1;
        }
        promoted
    }

    fn validate_submission(
        &self,
        submission: ReceiveSubmission,
    ) -> Result<usize, ReceivePumpError> {
        let Some(index) = self.slot_index(submission.slot_id) else {
            return Err(ReceivePumpError::ReceiveContextCorrupt);
        };
        let slot = &self.slots[index];
        if slot.state != ReceiveSlotState::SubmitPending
            || slot.generation != submission.slot_generation
        {
            return Err(ReceivePumpError::InvalidTransition);
        }
        Ok(index)
    }

    fn try_enter_running(&mut self) {
        if self.phase == ReceivePumpPhase::Starting
            && self.actual_in_flight == self.config.kernel_capacity.get()
            && !self
                .slots
                .iter()
                .any(|slot| slot.state == ReceiveSlotState::SubmitPending)
        {
            self.phase = ReceivePumpPhase::Running;
        }
    }

    fn pause_for_backpressure(&mut self) {
        if self.phase != ReceivePumpPhase::PausedByBackpressure {
            self.stats.receive_backpressure_events += 1;
        }
        self.stats.receive_permit_exhausted += 1;
        self.phase = ReceivePumpPhase::PausedByBackpressure;
    }

    fn resume_if_possible(&mut self) {
        if self.phase == ReceivePumpPhase::PausedByBackpressure && self.permits.available() > 0 {
            self.phase = ReceivePumpPhase::Running;
        }
    }

    fn set_terminal(&mut self, error: ReceivePumpError) {
        if self.terminal_error.is_none() {
            self.terminal_error = Some(error);
        }
        if self.phase != ReceivePumpPhase::Closing {
            self.phase = ReceivePumpPhase::RunningWithTerminalError;
        }
    }

    fn retire_pending(&mut self) {
        while let Some(pending) = self.pending.pop_front() {
            if let Some(index) = self.slot_index(pending.request.slot_id) {
                self.slots[index].state = ReceiveSlotState::Retiring;
            }
        }
        while let Some(held) = self.held.pop_front() {
            if let Some(index) = self.slot_index(held.request.slot_id) {
                self.slots[index].state = ReceiveSlotState::Retiring;
            }
        }
    }

    fn emit_final_if_possible(&mut self) -> Result<ReceivePumpEvent, ReceivePumpError> {
        if self.final_emitted {
            return Ok(ReceivePumpEvent::Ignored);
        }
        if self.actual_in_flight != 0
            || !self.pending.is_empty()
            || !self.held.is_empty()
            || !self.multishot_pending.is_empty()
            || self.prepared.is_some()
        {
            return Ok(ReceivePumpEvent::Ignored);
        }
        self.final_emitted = true;
        let error = self
            .terminal_error
            .unwrap_or(ReceivePumpError::DriverShutdown);
        Ok(ReceivePumpEvent::Final { error })
    }

    fn emit_final_or_drain(&mut self) -> Result<ReceivePumpEvent, ReceivePumpError> {
        let event = self.emit_final_if_possible()?;
        if matches!(event, ReceivePumpEvent::Ignored)
            && self.actual_in_flight != 0
            && matches!(
                self.phase,
                ReceivePumpPhase::Closing | ReceivePumpPhase::RunningWithTerminalError
            )
        {
            Ok(ReceivePumpEvent::Drain)
        } else {
            Ok(event)
        }
    }

    fn maybe_drained(&mut self) {
        if matches!(
            self.phase,
            ReceivePumpPhase::Closing | ReceivePumpPhase::RunningWithTerminalError
        ) && self.actual_in_flight == 0
            && self.pending.is_empty()
            && self.held.is_empty()
            && self.multishot_pending.is_empty()
            && self.prepared.is_none()
            && self.final_emitted
        {
            self.phase = ReceivePumpPhase::Drained;
            for slot in &mut self.slots {
                if slot.state != ReceiveSlotState::InFlight {
                    slot.state = ReceiveSlotState::Retiring;
                }
            }
        }
    }
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

#[cfg(all(test, not(feature = "loom")))]
mod tests {
    use super::*;
    use crate::driver::CompletionContinuation;
    use veloq_std::{
        net::SocketAddr,
        sync::atomic::{AtomicUsize, Ordering},
        vec,
    };

    fn config(depth: usize, queue: usize) -> ReceivePumpConfig {
        ReceivePumpConfig {
            kernel_capacity: NonZeroUsize::new(depth).expect("depth must be non-zero"),
            queue_capacity: NonZeroUsize::new(queue).expect("queue must be non-zero"),
            datagram_capacity: NonZeroUsize::new(1024).expect("capacity must be non-zero"),
            close_timeout: Duration::from_secs(1),
        }
    }

    fn pump(depth: usize, queue: usize) -> ReceivePumpState {
        let slots = (0..depth)
            .map(|slot_id| {
                ReceiveSlot::new(
                    slot_id as u32,
                    FixedBuf::alloc_heap(
                        NonZeroUsize::new(1024).expect("capacity must be non-zero"),
                        1024,
                    )
                    .expect("test buffer allocation should succeed"),
                )
            })
            .collect::<vec::Vec<_>>()
            .into_boxed_slice();
        let mut pump = ReceivePumpState::try_new(config(depth, queue), slots)
            .expect("test pump should be valid");
        pump.start().expect("pump should start");
        for _ in 0..depth {
            let submission = pump.next_submission().expect("slot should be selected");
            pump.submit_initial(submission, submission.slot_id as u64 + 1)
                .expect("initial submit should succeed");
        }
        assert!(pump.is_ready());
        pump
    }

    fn completion(request: ReceiveRequest, len: usize) -> ReceiveCompletion {
        ReceiveCompletion {
            request,
            result: Ok(len),
            remote_addr: "127.0.0.1:9000"
                .parse::<SocketAddr>()
                .expect("valid address"),
        }
    }

    fn failed_completion(request: ReceiveRequest, error: ReceivePumpError) -> ReceiveCompletion {
        ReceiveCompletion {
            request,
            result: Err(error),
            remote_addr: "127.0.0.1:9000"
                .parse::<SocketAddr>()
                .expect("valid address"),
        }
    }

    fn output(len: usize) -> FixedBuf {
        FixedBuf::alloc_heap(
            NonZeroUsize::new(len.max(1)).expect("capacity must be non-zero"),
            0,
        )
        .expect("test output allocation should succeed")
    }

    #[test]
    fn multishot_admission_preserves_fifo_while_permit_is_held() {
        let mut pump = ReceivePumpState::try_new_multishot(config(2, 1), None)
            .expect("multishot pump should be valid");
        pump.arm_multishot().expect("multishot should arm");
        let first_addr = "127.0.0.1:9001".parse().expect("valid address");
        let second_addr = "127.0.0.1:9002".parse().expect("valid address");
        let first = pump
            .admit_datagram(3, first_addr)
            .expect("first datagram should be admitted");
        let first_delivery = pump.next_datagram_delivery().expect("first permit");
        assert_eq!(first_delivery.key(), first.key());
        let first_packet = pump
            .finish_datagram_delivery(first_delivery, output(3))
            .expect("first packet should finish");

        let second = pump
            .admit_datagram(4, second_addr)
            .expect("second datagram should be retained");
        assert_eq!(pump.phase(), ReceivePumpPhase::PausedByBackpressure);
        assert_eq!(pump.pending_datagrams(), 1);
        assert!(pump.next_datagram_delivery().is_none());

        drop(first_packet);
        let second_delivery = pump.next_datagram_delivery().expect("released permit");
        assert_eq!(second_delivery.key(), second.key());
        assert_eq!(second_delivery.remote_addr(), second_addr);
        let second_packet = pump
            .finish_datagram_delivery(second_delivery, output(4))
            .expect("second packet should finish");
        assert_eq!(second_packet.buf.len(), 4);
        drop(second_packet);
    }

    #[test]
    fn multishot_permit_release_notifies_without_touching_pump_state() {
        let notifications = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&notifications);
        let notifier: Arc<dyn ReceivePermitNotifier> = Arc::new(move || {
            observed.fetch_add(1, Ordering::Relaxed);
        });
        let mut pump = ReceivePumpState::try_new_multishot(config(1, 1), Some(notifier))
            .expect("multishot pump should be valid");
        pump.arm_multishot().expect("multishot should arm");
        pump.admit_datagram(2, "127.0.0.1:9001".parse().expect("valid address"))
            .expect("datagram should be admitted");
        let delivery = pump.next_datagram_delivery().expect("permit");
        let packet = pump
            .finish_datagram_delivery(delivery, output(2))
            .expect("packet should finish");
        assert_eq!(notifications.load(Ordering::Relaxed), 0);
        drop(packet);
        assert_eq!(notifications.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn depth_one_completion_always_rearms_before_more_is_published() {
        let mut pump = pump(1, 2);
        let request = pump
            .slot(0)
            .expect("slot exists")
            .current_request()
            .expect("in flight");
        assert!(matches!(
            pump.complete(completion(request, 4)),
            Ok(ReceivePumpEvent::Pending { .. })
        ));
        let delivery = pump.next_delivery().expect("permit should be available");
        let reservation = pump
            .reserve_replacement(&delivery)
            .expect("replacement generation should be reserved");
        let record = pump
            .finish_delivery(
                delivery,
                output(4),
                ReplacementSubmit::Submitted {
                    request_id: 2,
                    reservation,
                },
            )
            .expect("replacement should succeed");
        assert_eq!(record.continuation, CompletionContinuation::More);
        assert_eq!(pump.actual_in_flight(), 1);
        assert_eq!(pump.stats().receive_requests_rearmed, 1);
    }

    #[test]
    fn initial_ready_waits_for_every_successful_submit() {
        let mut pump = pump_without_arm(3, 3);
        pump.start().expect("pump should start");
        for id in 0..2 {
            let submission = pump.next_submission().expect("slot should be selected");
            pump.submit_initial(submission, id + 1)
                .expect("initial submit should succeed");
            assert!(!pump.is_ready(), "depth is not complete after {id} submits");
        }
        let submission = pump
            .next_submission()
            .expect("last slot should be selected");
        pump.submit_initial(submission, 3)
            .expect("initial submit should succeed");
        assert!(pump.is_ready());
        assert_eq!(pump.actual_in_flight(), 3);
    }

    #[test]
    fn permit_exhaustion_keeps_completion_pending_without_publish_or_rearm() {
        let mut pump = pump(1, 1);
        let request = pump
            .slot(0)
            .expect("slot exists")
            .current_request()
            .expect("in flight");
        let first = pump
            .complete(completion(request, 3))
            .expect("completion should route");
        let delivery = pump
            .next_delivery()
            .expect("first permit should be available");
        let reservation = pump
            .reserve_replacement(&delivery)
            .expect("replacement generation should be reserved");
        let record = pump
            .finish_delivery(
                delivery,
                output(3),
                ReplacementSubmit::Submitted {
                    request_id: 2,
                    reservation,
                },
            )
            .expect("replacement should succeed");
        assert_eq!(record.continuation, CompletionContinuation::More);
        let held_record = record.packet;
        assert_eq!(pump.permits_available(), 0);

        let request = pump
            .slot(0)
            .expect("slot exists")
            .current_request()
            .expect("rearmed");
        assert!(matches!(first, ReceivePumpEvent::Pending { .. }));
        assert!(matches!(
            pump.complete(completion(request, 5)),
            Ok(ReceivePumpEvent::Pending { .. })
        ));
        assert_eq!(pump.pending_completions(), 1);
        assert_eq!(pump.stats().receive_requests_rearmed, 1);
        assert!(pump.next_delivery().is_none());
        drop(held_record);
        let delivery = pump.next_delivery().expect("released permit should resume");
        assert_eq!(delivery.len(), 5);
    }

    #[test]
    fn replacement_failure_publishes_current_packet_then_exposes_terminal_error() {
        let mut pump = pump(1, 1);
        let request = pump
            .slot(0)
            .expect("slot exists")
            .current_request()
            .expect("in flight");
        pump.complete(completion(request, 4))
            .expect("completion should route");
        let delivery = pump.next_delivery().expect("permit should be available");
        let reservation = pump
            .reserve_replacement(&delivery)
            .expect("replacement generation should be reserved");
        let record = pump
            .finish_delivery(
                delivery,
                output(4),
                ReplacementSubmit::Failed { reservation },
            )
            .expect("current packet must be delivered");
        assert_eq!(record.continuation, CompletionContinuation::Final);
        assert_eq!(
            pump.terminal_error(),
            Some(ReceivePumpError::ReplacementSubmitFailed)
        );
        assert_eq!(pump.ready_packets(), 1);
        pump.record_consumed().expect("record should be consumed");
        assert_eq!(pump.phase(), ReceivePumpPhase::Drained);
    }

    #[test]
    fn one_request_error_keeps_the_logical_slot_until_all_requests_drain() {
        let mut pump = pump(2, 2);
        let first = pump
            .slot(0)
            .expect("first slot exists")
            .current_request()
            .expect("first request is in flight");
        let second = pump
            .slot(1)
            .expect("second slot exists")
            .current_request()
            .expect("second request is in flight");

        assert_eq!(
            pump.complete(failed_completion(
                first,
                ReceivePumpError::ReceiveOsError { code: 10035 },
            ))
            .expect("request error should enter drain"),
            ReceivePumpEvent::Drain
        );
        assert_eq!(pump.actual_in_flight(), 1);
        assert_eq!(pump.phase(), ReceivePumpPhase::RunningWithTerminalError);

        assert_eq!(
            pump.complete(completion(second, 8))
                .expect("remaining request should drain"),
            ReceivePumpEvent::Final {
                error: ReceivePumpError::ReceiveOsError { code: 10035 }
            }
        );
        assert_eq!(pump.actual_in_flight(), 0);
    }

    #[test]
    fn terminal_error_drains_admitted_packets_without_replacement() {
        let mut pump = pump(3, 3);
        let first = pump
            .slot(0)
            .expect("first slot exists")
            .current_request()
            .expect("first request is in flight");
        let second = pump
            .slot(1)
            .expect("second slot exists")
            .current_request()
            .expect("second request is in flight");
        let third = pump
            .slot(2)
            .expect("third slot exists")
            .current_request()
            .expect("third request is in flight");

        pump.complete(completion(first, 4))
            .expect("first packet should be admitted");
        pump.complete(completion(second, 5))
            .expect("second packet should be admitted");
        assert!(matches!(
            pump.complete(failed_completion(
                third,
                ReceivePumpError::ReceiveOsError { code: 10054 },
            ))
            .expect("terminal request error should preserve pending packets"),
            ReceivePumpEvent::Pending { .. }
        ));

        let first_delivery = pump.next_delivery().expect("first packet should drain");
        assert!(!first_delivery.replacement_required());
        let first_key = first_delivery.key();
        let first_record = pump
            .finish_delivery(first_delivery, output(4), ReplacementSubmit::NotSubmitted)
            .expect("first packet should finish without replacement");
        assert_eq!(first_record.key, first_key);
        assert_eq!(first_record.continuation, CompletionContinuation::More);
        drop(first_record.packet);
        pump.record_consumed()
            .expect("first record should be consumed");

        let second_delivery = pump.next_delivery().expect("second packet should drain");
        assert!(!second_delivery.replacement_required());
        let second_record = pump
            .finish_delivery(second_delivery, output(5), ReplacementSubmit::NotSubmitted)
            .expect("second packet should finish without replacement");
        assert_eq!(second_record.key.completion_sequence(), 2);
        assert_eq!(second_record.continuation, CompletionContinuation::Final);
        drop(second_record.packet);
        pump.record_consumed()
            .expect("second record should be consumed");
        assert_eq!(
            pump.terminal_error(),
            Some(ReceivePumpError::ReceiveOsError { code: 10054 })
        );
        assert_eq!(pump.phase(), ReceivePumpPhase::Drained);
    }

    #[test]
    fn replacement_failure_drains_when_other_requests_remain() {
        let mut pump = pump(2, 2);
        let first = pump
            .slot(0)
            .expect("first slot exists")
            .current_request()
            .expect("first request is in flight");
        pump.complete(completion(first, 4))
            .expect("completion should route");
        let delivery = pump.next_delivery().expect("permit should be available");
        let reservation = pump
            .reserve_replacement(&delivery)
            .expect("replacement generation should be reserved");
        let record = pump
            .finish_delivery(
                delivery,
                output(4),
                ReplacementSubmit::Failed { reservation },
            )
            .expect("completed packet should still be delivered");

        assert_eq!(record.continuation, CompletionContinuation::More);
        assert_eq!(pump.phase(), ReceivePumpPhase::RunningWithTerminalError);
        assert_eq!(pump.actual_in_flight(), 1);
    }

    #[test]
    fn replacement_reservation_advances_generation_before_submit_commit() {
        let mut pump = pump(1, 2);
        let old = pump
            .slot(0)
            .expect("slot exists")
            .current_request()
            .expect("in flight");
        pump.complete(completion(old, 4))
            .expect("completion should route");
        let delivery = pump.next_delivery().expect("permit should be available");
        let reservation = pump
            .reserve_replacement(&delivery)
            .expect("replacement generation should be reserved");

        assert_eq!(reservation.slot_id(), old.slot_id);
        assert_eq!(reservation.slot_generation(), old.slot_generation + 1);
        assert_eq!(
            pump.slot(0).expect("slot exists").generation(),
            reservation.slot_generation()
        );
        assert!(
            pump.slot(0)
                .expect("slot exists")
                .current_request()
                .is_none()
        );

        let record = pump
            .finish_delivery(
                delivery,
                output(4),
                ReplacementSubmit::Submitted {
                    request_id: 2,
                    reservation,
                },
            )
            .expect("replacement should commit");
        assert_eq!(record.continuation, CompletionContinuation::More);
        assert_eq!(
            pump.slot(0)
                .expect("slot exists")
                .current_request()
                .expect("replacement should be in flight")
                .slot_generation,
            reservation.slot_generation()
        );
    }

    #[test]
    fn pending_keys_follow_completion_order_and_logical_rearms() {
        let mut pump = pump(1, 2);
        let first_request = pump
            .slot(0)
            .expect("slot exists")
            .current_request()
            .expect("in flight");
        pump.complete(completion(first_request, 4))
            .expect("completion should route");
        let first_delivery = pump.next_delivery().expect("permit should be available");
        assert_eq!(first_delivery.key().logical_receiver_generation(), 0);
        assert_eq!(first_delivery.key().completion_sequence(), 1);
        let first_reservation = pump
            .reserve_replacement(&first_delivery)
            .expect("replacement generation should be reserved");
        let first_record = pump
            .finish_delivery(
                first_delivery,
                output(4),
                ReplacementSubmit::Submitted {
                    request_id: 2,
                    reservation: first_reservation,
                },
            )
            .expect("first delivery should finish");
        let first_key = first_record.key;
        drop(first_record.packet);
        pump.record_consumed()
            .expect("first record should be consumed");

        assert_eq!(
            pump.retain_for_rearm().expect("rearm should be retained"),
            ReceivePumpEvent::Retained {
                logical_receiver_generation: 1
            }
        );
        pump.submit_rearm().expect("logical rearm should commit");
        let second_request = pump
            .slot(0)
            .expect("slot exists")
            .current_request()
            .expect("request remains owned by the logical receiver");
        pump.complete(completion(second_request, 5))
            .expect("second completion should route");
        let second_delivery = pump
            .next_delivery()
            .expect("second permit should be available");
        assert_eq!(first_key.logical_receiver_generation(), 0);
        assert_eq!(second_delivery.key().logical_receiver_generation(), 1);
        assert_eq!(second_delivery.key().completion_sequence(), 2);
    }

    #[test]
    fn stale_generation_cannot_overwrite_a_rearmed_slot() {
        let mut pump = pump(1, 2);
        let old = pump
            .slot(0)
            .expect("slot exists")
            .current_request()
            .expect("in flight");
        pump.complete(completion(old, 4))
            .expect("completion should route");
        let delivery = pump.next_delivery().expect("permit should be available");
        let reservation = pump
            .reserve_replacement(&delivery)
            .expect("replacement generation should be reserved");
        pump.finish_delivery(
            delivery,
            output(4),
            ReplacementSubmit::Submitted {
                request_id: 2,
                reservation,
            },
        )
        .expect("replacement should succeed");
        let current = pump
            .slot(0)
            .expect("slot exists")
            .current_request()
            .expect("rearmed");
        assert_eq!(current.slot_generation, old.slot_generation + 1);
        assert_eq!(
            pump.complete(completion(old, 8)),
            Err(ReceivePumpError::ReceiveContextCorrupt)
        );
        assert_eq!(
            pump.slot(0).expect("slot exists").current_request(),
            Some(current)
        );
        assert_eq!(pump.actual_in_flight(), 1);
    }

    #[test]
    fn cancel_mid_stream_emits_final_only_after_the_actual_request_completes() {
        let mut pump = pump(1, 1);
        assert_eq!(
            pump.cancel().expect("cancel should be accepted"),
            ReceivePumpEvent::Drain
        );
        assert_eq!(pump.phase(), ReceivePumpPhase::Closing);
        let request = pump
            .slot(0)
            .expect("slot exists")
            .current_request()
            .expect("in flight");
        assert_eq!(
            pump.complete(completion(request, 4))
                .expect("completion should drain"),
            ReceivePumpEvent::Final {
                error: ReceivePumpError::ReceiveCancelled
            }
        );
        assert_eq!(pump.phase(), ReceivePumpPhase::Closing);
        pump.acknowledge_final()
            .expect("final should be acknowledged");
        assert_eq!(pump.phase(), ReceivePumpPhase::Drained);
    }

    #[test]
    fn cancel_discards_every_closing_completion_until_final() {
        let mut pump = pump(2, 2);
        let first = pump
            .slot(0)
            .expect("first slot exists")
            .current_request()
            .expect("first request is in flight");
        let second = pump
            .slot(1)
            .expect("second slot exists")
            .current_request()
            .expect("second request is in flight");

        assert_eq!(
            pump.cancel().expect("cancel should be accepted"),
            ReceivePumpEvent::Drain
        );
        assert_eq!(pump.actual_in_flight(), 2);
        assert_eq!(pump.pending_completions(), 0);
        assert_eq!(pump.held_completions(), 0);

        assert_eq!(
            pump.complete(completion(first, 4))
                .expect("first completion should drain"),
            ReceivePumpEvent::Drain
        );
        let first_slot = pump.slot(0).expect("first slot exists");
        assert_eq!(first_slot.state(), ReceiveSlotState::Retiring);
        assert_eq!(first_slot.completed_len(), None);
        assert_eq!(first_slot.completed_data(), None);
        assert_eq!(pump.actual_in_flight(), 1);
        assert_eq!(pump.pending_completions(), 0);

        assert_eq!(
            pump.complete(completion(second, 8))
                .expect("second completion should drain"),
            ReceivePumpEvent::Final {
                error: ReceivePumpError::ReceiveCancelled
            }
        );
        assert_eq!(pump.actual_in_flight(), 0);
        assert_eq!(pump.pending_completions(), 0);
        pump.acknowledge_final()
            .expect("final should be acknowledged");
        assert_eq!(pump.phase(), ReceivePumpPhase::Drained);
    }

    #[test]
    fn orphaned_more_does_not_take_the_persistent_submit_payload() {
        let mut pump = pump(1, 1);
        let request = pump
            .slot(0)
            .expect("slot exists")
            .current_request()
            .expect("in flight");
        pump.cancel().expect("cancel should be accepted");
        let event = pump
            .complete(completion(request, 4))
            .expect("completion should drain");
        assert!(matches!(event, ReceivePumpEvent::Final { .. }));
        assert!(
            pump.slot(0)
                .expect("slot exists")
                .current_request()
                .is_none()
        );
        assert_eq!(pump.actual_in_flight(), 0);
        assert_eq!(pump.stats().receive_requests_completed, 1);
    }

    #[test]
    fn failed_submit_and_completion_keep_request_counters_balanced() {
        let mut pump = pump_without_arm(2, 2);
        pump.start().expect("pump should start");
        let first = pump
            .next_submission()
            .expect("first slot should be selected");
        pump.submit_initial(first, 1)
            .expect("first submit should succeed");
        let second = pump
            .next_submission()
            .expect("second slot should be selected");
        assert_eq!(
            pump.fail_initial_submit(second)
                .expect("failure should enter closing"),
            ReceivePumpEvent::Ignored
        );
        let request = pump
            .slot(0)
            .expect("slot exists")
            .current_request()
            .expect("first request remains");
        assert_eq!(pump.actual_in_flight(), 1);
        assert_eq!(pump.stats().receive_requests_submitted, 1);
        assert_eq!(
            pump.complete(completion(request, 4))
                .expect("completion should drain"),
            ReceivePumpEvent::Final {
                error: ReceivePumpError::InitialReceiveSubmitFailed
            }
        );
        assert_eq!(pump.actual_in_flight(), 0);
        assert_eq!(pump.stats().receive_requests_completed, 1);
        pump.acknowledge_final()
            .expect("final should be acknowledged");
        assert_eq!(pump.phase(), ReceivePumpPhase::Drained);
    }

    fn pump_without_arm(depth: usize, queue: usize) -> ReceivePumpState {
        let slots = (0..depth)
            .map(|slot_id| {
                ReceiveSlot::new(
                    slot_id as u32,
                    FixedBuf::alloc_heap(
                        NonZeroUsize::new(1024).expect("capacity must be non-zero"),
                        1024,
                    )
                    .expect("test buffer allocation should succeed"),
                )
            })
            .collect::<vec::Vec<_>>()
            .into_boxed_slice();
        ReceivePumpState::try_new(config(depth, queue), slots).expect("test pump should be valid")
    }
}
