use veloq_buf::heap::ChunkId;
use veloq_std::time::Instant;

use crate::driver::registration::{KernelUpdateOutcome, UpdateEvidence};

#[derive(Debug, Clone, Copy)]
pub(super) struct BufferRegistrationQuarantine {
    chunk_id: ChunkId,
    register_errno: Option<i32>,
    cleanup_errno: Option<i32>,
    register_outcome: KernelUpdateOutcome,
    cleanup_outcome: Option<KernelUpdateOutcome>,
    scope: &'static str,
    anomaly_count: u64,
}

impl BufferRegistrationQuarantine {
    pub(super) fn new(
        chunk_id: ChunkId,
        register: UpdateEvidence,
        cleanup: Option<UpdateEvidence>,
        scope: &'static str,
    ) -> Self {
        Self {
            chunk_id,
            register_errno: update_errno(register.outcome()),
            cleanup_errno: cleanup.and_then(|evidence| update_errno(evidence.outcome())),
            register_outcome: register.outcome(),
            cleanup_outcome: cleanup.map(UpdateEvidence::outcome),
            scope,
            anomaly_count: 1,
        }
    }

    pub(super) fn note_anomaly(&mut self) {
        self.anomaly_count = self.anomaly_count.saturating_add(1);
    }

    pub(super) const fn chunk_id(self) -> ChunkId {
        self.chunk_id
    }

    pub(super) const fn register_errno(self) -> Option<i32> {
        self.register_errno
    }

    pub(super) const fn cleanup_errno(self) -> Option<i32> {
        self.cleanup_errno
    }

    pub(super) const fn register_outcome(self) -> KernelUpdateOutcome {
        self.register_outcome
    }

    pub(super) const fn cleanup_outcome(self) -> Option<KernelUpdateOutcome> {
        self.cleanup_outcome
    }

    pub(super) const fn scope(self) -> &'static str {
        self.scope
    }

    pub(super) const fn anomaly_count(self) -> u64 {
        self.anomaly_count
    }
}

fn update_errno(outcome: KernelUpdateOutcome) -> Option<i32> {
    match outcome {
        KernelUpdateOutcome::Applied(_) => None,
        #[cfg(feature = "test-hooks")]
        KernelUpdateOutcome::Rejected(errno) => Some(errno),
        KernelUpdateOutcome::Unknown(errno, _) => errno,
    }
}

/// The userspace resource state for one fixed-buffer chunk.
///
/// `None` in [`FixedBufferOwner::chunk_records`] is the `Absent` state. The remaining states
/// are recorded even when the fast bitset is clear, so a fallback or an in-progress update is
/// not confused with a kernel registration.
/// 此账本记录每个固定 buffer chunk 的用户态资源状态；即使快速位图未置位，也保留回退或更新中的实际状态。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ChunkRegistrationState {
    Registering,
    Registered,
    Fallback,
    Quarantined,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ChunkRegistrationIdentity {
    ptr: usize,
    len: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ChunkRegistrationRecord {
    id: ChunkId,
    ptr: usize,
    len: usize,
    identity: ChunkRegistrationIdentity,
    state: ChunkRegistrationState,
    last_failure: Option<Instant>,
}

impl ChunkRegistrationRecord {
    #[inline]
    pub(super) fn new(
        id: ChunkId,
        ptr: *const u8,
        len: usize,
        state: ChunkRegistrationState,
    ) -> Self {
        let ptr = ptr as usize;
        Self {
            id,
            ptr,
            len,
            identity: ChunkRegistrationIdentity { ptr, len },
            state,
            last_failure: None,
        }
    }

    #[inline]
    pub(crate) fn with_last_failure(mut self, last_failure: Option<Instant>) -> Self {
        self.last_failure = last_failure;
        self
    }

    #[inline]
    pub(crate) fn matches(&self, id: ChunkId, ptr: *const u8, len: usize) -> bool {
        self.id == id
            && self.ptr == ptr as usize
            && self.len == len
            && self.identity
                == ChunkRegistrationIdentity {
                    ptr: ptr as usize,
                    len,
                }
    }

    #[inline]
    pub(crate) const fn ptr(self) -> usize {
        self.ptr
    }

    #[inline]
    pub(crate) const fn len(self) -> usize {
        self.len
    }

    #[inline]
    pub(crate) const fn state(self) -> ChunkRegistrationState {
        self.state
    }

    #[inline]
    pub(crate) const fn last_failure(self) -> Option<Instant> {
        self.last_failure
    }

    #[inline]
    pub(crate) fn clear_last_failure(&mut self) {
        self.last_failure = None;
    }

    #[inline]
    #[cfg(feature = "test-hooks")]
    pub(crate) fn set_last_failure(&mut self, failure: Instant) {
        self.last_failure = Some(failure);
    }
}
