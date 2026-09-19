use super::{
    KernelUpdateOutcome, MAX_CHUNKS, REGISTER_FAILURE_RETRY_COOLDOWN, UpdateEvidence,
    UringRegistrationStats,
};
use crate::{
    config::BufferRegistrationMode,
    diagnostics::UringCompletionDiagnostics,
    driver::env::ChunkRegistrationDecision,
    driver::env::{ProvidedBufSqeInfo, SqeEnv},
    driver::registration::file_table::FileTable,
    error::{UringError, UringResult},
};
use diagweave::prelude::*;
use tracing::debug;
use veloq_buf::{BufferRegistrar, heap::ChunkId};
use veloq_driver_core::driver::BufferRegistrationStatus;
use veloq_io_uring::{ResourceKind, ResourceRegistration, ResourceRegistrationState, Submitter};
use veloq_std::{boxed::Box, collections::BitSet, format, ptr, time::Instant, vec};

#[cfg(feature = "test-hooks")]
use veloq_std::collections::VecDeque;

#[derive(Debug, Clone, Copy)]
pub(crate) struct BufferRegistrationQuarantine {
    chunk_id: ChunkId,
    register_errno: Option<i32>,
    cleanup_errno: Option<i32>,
    register_outcome: KernelUpdateOutcome,
    cleanup_outcome: Option<KernelUpdateOutcome>,
    scope: &'static str,
    anomaly_count: u64,
}

impl BufferRegistrationQuarantine {
    pub(crate) fn new(
        chunk_id: ChunkId,
        register: UpdateEvidence,
        cleanup: Option<UpdateEvidence>,
        scope: &'static str,
    ) -> Self {
        Self {
            chunk_id,
            register_errno: update_errno(register.outcome),
            cleanup_errno: cleanup.and_then(|evidence| update_errno(evidence.outcome)),
            register_outcome: register.outcome,
            cleanup_outcome: cleanup.map(|evidence| evidence.outcome),
            scope,
            anomaly_count: 1,
        }
    }

    pub(crate) fn note_anomaly(&mut self) {
        self.anomaly_count = self.anomaly_count.saturating_add(1);
    }

    pub(crate) const fn chunk_id(self) -> ChunkId {
        self.chunk_id
    }

    pub(crate) const fn register_errno(self) -> Option<i32> {
        self.register_errno
    }

    pub(crate) const fn cleanup_errno(self) -> Option<i32> {
        self.cleanup_errno
    }

    pub(crate) const fn register_outcome(self) -> KernelUpdateOutcome {
        self.register_outcome
    }

    pub(crate) const fn cleanup_outcome(self) -> Option<KernelUpdateOutcome> {
        self.cleanup_outcome
    }

    pub(crate) const fn scope(self) -> &'static str {
        self.scope
    }

    pub(crate) const fn anomaly_count(self) -> u64 {
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
    pub(crate) fn new(
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

pub(crate) struct FixedBufferSubmitPort<'r, 'a> {
    registered_chunks_cache: &'r mut BitSet,
    chunk_records: &'r mut [Option<ChunkRegistrationRecord>],
    registrar: &'a (dyn BufferRegistrar + 'a),
    registration_stats: &'r mut UringRegistrationStats,
    registration_mode: BufferRegistrationMode,
    fixed_buffers_available: bool,
    fixed_buffers_failure_errno: Option<i32>,
    fixed_buffer_registration: Option<&'r ResourceRegistration>,
    registration_quarantine: &'r mut Option<BufferRegistrationQuarantine>,
    #[cfg(feature = "test-hooks")]
    register_buffers_update_outcomes: &'r mut VecDeque<BufferUpdateInjection>,
    #[cfg(feature = "test-hooks")]
    bitset_set_failure: &'r mut bool,
    provided: Option<ProvidedBufSqeInfo>,
}

#[cfg(feature = "test-hooks")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BufferUpdateInjection {
    Applied,
    Rejected(i32),
    Unknown(i32),
}

pub(crate) struct FixedBufferOwner<'a> {
    /// A derived fast cache; `chunk_records` is the only registration source of truth.
    registered_chunks_cache: BitSet,
    chunk_records: Box<[Option<ChunkRegistrationRecord>]>,
    registrar: &'a (dyn BufferRegistrar + 'a),
    stats: UringRegistrationStats,
    mode: BufferRegistrationMode,
    fixed_buffers_available: bool,
    fixed_buffers_failure_errno: Option<i32>,
    fixed_buffer_registration: Option<ResourceRegistration>,
    registration_quarantine: Option<BufferRegistrationQuarantine>,
    #[cfg(feature = "test-hooks")]
    register_buffers_update_outcomes: VecDeque<BufferUpdateInjection>,
    #[cfg(feature = "test-hooks")]
    bitset_set_failure: bool,
}

impl<'a> FixedBufferOwner<'a> {
    pub(crate) fn new(
        mode: BufferRegistrationMode,
        registrar: &'a (dyn BufferRegistrar + 'a),
    ) -> Self {
        Self {
            registered_chunks_cache: BitSet::new(MAX_CHUNKS),
            chunk_records: vec![None; MAX_CHUNKS].into_boxed_slice(),
            registrar,
            stats: UringRegistrationStats::default(),
            mode,
            fixed_buffers_available: false,
            fixed_buffers_failure_errno: None,
            fixed_buffer_registration: None,
            registration_quarantine: None,
            #[cfg(feature = "test-hooks")]
            register_buffers_update_outcomes: VecDeque::new(),
            #[cfg(feature = "test-hooks")]
            bitset_set_failure: false,
        }
    }

    pub(crate) fn split_for_submit(
        &mut self,
        provided: Option<ProvidedBufSqeInfo>,
    ) -> FixedBufferSubmitPort<'_, 'a> {
        debug_assert!(self.chunk_ledger_is_consistent());
        FixedBufferSubmitPort {
            registered_chunks_cache: &mut self.registered_chunks_cache,
            chunk_records: &mut self.chunk_records,
            registrar: self.registrar,
            registration_stats: &mut self.stats,
            registration_mode: self.mode,
            fixed_buffers_available: self.fixed_buffers_available,
            fixed_buffers_failure_errno: self.fixed_buffers_failure_errno,
            fixed_buffer_registration: self.fixed_buffer_registration.as_ref(),
            registration_quarantine: &mut self.registration_quarantine,
            #[cfg(feature = "test-hooks")]
            register_buffers_update_outcomes: &mut self.register_buffers_update_outcomes,
            #[cfg(feature = "test-hooks")]
            bitset_set_failure: &mut self.bitset_set_failure,
            provided,
        }
    }

    pub(crate) fn set_fixed_buffers_available(&mut self, registration: ResourceRegistration) {
        debug_assert!(registration.kind() == ResourceKind::Buffers);
        debug_assert!(registration.state() == ResourceRegistrationState::Registered);
        self.fixed_buffers_available = true;
        self.fixed_buffers_failure_errno = None;
        self.fixed_buffer_registration = Some(registration);
    }

    #[cfg(feature = "test-hooks")]
    pub(crate) fn fixed_buffers_available(&self) -> bool {
        self.fixed_buffers_available
    }

    pub(crate) fn set_fixed_buffers_unavailable(&mut self, errno: Option<i32>) {
        self.fixed_buffers_available = false;
        self.fixed_buffers_failure_errno = errno;
        self.fixed_buffer_registration = None;
    }

    #[cfg(feature = "test-hooks")]
    pub(crate) fn inject_register_buffers_update_failure(&mut self, errno: i32) {
        self.register_buffers_update_outcomes
            .push_back(BufferUpdateInjection::Rejected(errno));
    }

    #[cfg(feature = "test-hooks")]
    pub(crate) fn inject_register_buffers_update_unknown(&mut self, errno: i32) {
        self.register_buffers_update_outcomes
            .push_back(BufferUpdateInjection::Unknown(errno));
    }

    #[cfg(feature = "test-hooks")]
    pub(crate) fn inject_register_buffers_update_sequence(&mut self, outcomes: &[Option<i32>]) {
        self.register_buffers_update_outcomes.clear();
        self.register_buffers_update_outcomes
            .extend(outcomes.iter().copied().map(|outcome| match outcome {
                Some(errno) => BufferUpdateInjection::Rejected(errno),
                None => BufferUpdateInjection::Applied,
            }));
    }

    #[cfg(feature = "test-hooks")]
    pub(crate) fn inject_bitset_set_failure(&mut self) {
        self.bitset_set_failure = true;
    }

    #[cfg(feature = "test-hooks")]
    #[inline]
    pub(crate) fn stats(&self) -> &UringRegistrationStats {
        &self.stats
    }

    #[inline]
    pub(crate) fn stats_mut(&mut self) -> &mut UringRegistrationStats {
        &mut self.stats
    }

    #[cfg(feature = "test-hooks")]
    pub(crate) fn is_chunk_registered(&self, chunk_id: ChunkId) -> bool {
        self.record_is_registered(chunk_id.as_usize())
    }

    fn chunk_ledger_is_consistent(&self) -> bool {
        (0..MAX_CHUNKS).all(|index| {
            let registered = self.registered_chunks_cache.get(index).unwrap_or(false);
            let ledger_registered = self.chunk_records[index]
                .is_some_and(|record| record.state == ChunkRegistrationState::Registered);
            registered == ledger_registered
        })
    }

    #[cfg(feature = "test-hooks")]
    #[inline]
    fn record_is_registered(&self, index: usize) -> bool {
        let Some(record) = self.chunk_records.get(index).copied().flatten() else {
            return false;
        };
        let registered = record.state == ChunkRegistrationState::Registered;
        debug_assert_eq!(
            self.registered_chunks_cache.get(index).unwrap_or(false),
            registered,
            "fixed-buffer cache diverged from the resource ledger"
        );
        registered
    }
}

impl FixedBufferSubmitPort<'_, '_> {
    /// Narrows the mutable registration projection to the immutable SQE view.
    pub(crate) fn sqe_env<'view>(&'view self, file_table: &'view FileTable) -> SqeEnv<'view> {
        SqeEnv::from_parts(
            file_table,
            self.chunk_records,
            self.registrar,
            self.provided,
        )
    }

    /// Registers `[ptr, ptr + len)` as the kernel's fixed buffer number `id`.
    pub(crate) fn register_buffer_backend(
        &mut self,
        submitter: &Submitter<'_>,
        diagnostics: &UringCompletionDiagnostics,
        id: ChunkId,
        ptr: *const u8,
        len: usize,
    ) -> UringResult<BufferRegistrationStatus> {
        let scope = "driver.register_buffer_internal";
        let index = self.prepare_chunk_registration(id, ptr, len, scope)?;
        self.ensure_registration_healthy(scope, id, None)?;

        if let Some(record) = self.chunk_records[index]
            && record.state() == ChunkRegistrationState::Registered
        {
            if record.matches(id, ptr, len) {
                debug_assert!(self.cache_is_registered(index));
                return Ok(BufferRegistrationStatus::Registered);
            }
            diagnostics.inc_fixed_chunk_identity_mismatch();
            return self.identity_mismatch_status(id, ptr, len, scope, None, record);
        }

        if !self.fixed_buffers_available {
            self.set_chunk_record(id, ptr, len, ChunkRegistrationState::Fallback);
            if self.registration_mode.is_strict() {
                return Err(self.fixed_buffers_unavailable_report(scope, id, None));
            }
            return self.registration_unavailable(
                id,
                self.fixed_buffers_failure_errno,
                "sparse fixed-buffer registration unavailable",
            );
        }

        if let Some(last_fail) = self.chunk_records[index].and_then(|record| record.last_failure())
        {
            if last_fail.elapsed() < REGISTER_FAILURE_RETRY_COOLDOWN {
                self.registration_stats
                    .inc_chunk_register_skipped_recent_failure();
                self.set_chunk_record(id, ptr, len, ChunkRegistrationState::Fallback);
                if self.registration_mode.is_strict() {
                    return UringError::Registration
                        .push_ctx("scope", scope)
                        .with_ctx("chunk_id", id.raw())
                        .attach_note("recent chunk registration failure cooldown");
                }
                return self.registration_unavailable(
                    id,
                    None,
                    "recent chunk registration failure cooldown",
                );
            }
            self.clear_chunk_failure(index);
        }

        let iovecs = [libc::iovec {
            iov_base: ptr as *mut _,
            iov_len: len,
        }];
        self.set_chunk_record(id, ptr, len, ChunkRegistrationState::Registering);
        self.registration_stats.inc_chunk_register_attempts();
        let register = self.issue_chunk_update(submitter, index as u32, &iovecs);

        match register.outcome() {
            KernelUpdateOutcome::Applied(updated) if updated == register.requested() => {
                if let Err(set_report) = self.commit_chunk_registration(index, id, ptr, len) {
                    return self.recover_failed_commit(submitter, id, ptr, len, set_report);
                }
                self.registration_stats.inc_chunk_register_success();
                debug_assert!(self.chunk_ledger_is_consistent());
                Ok(BufferRegistrationStatus::Registered)
            }
            #[cfg(feature = "test-hooks")]
            KernelUpdateOutcome::Rejected(errno) => {
                self.note_chunk_registration_failure(id, ptr, len);
                let report = self.chunk_update_report(
                    "driver.register_buffer_internal.register_buffers_update",
                    register,
                    "fixed-buffer registration was rejected before reaching the kernel",
                );
                if self.registration_mode.is_strict() {
                    Err(report)
                } else {
                    self.registration_unavailable(
                        id,
                        Some(errno),
                        "fixed-buffer registration was explicitly rejected",
                    )
                }
            }
            KernelUpdateOutcome::Unknown(_, _) => {
                self.registration_stats.inc_chunk_register_failures();
                self.quarantine_chunk(id, ptr, len, register, None);
                Err(self.chunk_update_report(
                    "driver.register_buffer_internal.register_buffers_update",
                    register,
                    "fixed-buffer registration result is unknown; ring is quarantined",
                ))
            }
            KernelUpdateOutcome::Applied(_) => {
                self.registration_stats.inc_chunk_register_failures();
                self.quarantine_chunk(id, ptr, len, register, None);
                Err(self.chunk_update_report(
                    "driver.register_buffer_internal.register_buffers_update",
                    register,
                    "fixed-buffer registration applied an unexpected update count",
                ))
            }
        }
    }

    fn prepare_chunk_registration(
        &self,
        id: ChunkId,
        ptr: *const u8,
        len: usize,
        scope: &'static str,
    ) -> UringResult<usize> {
        let index = id.as_usize();
        if index >= MAX_CHUNKS {
            return UringError::InvalidInput
                .push_ctx("scope", scope)
                .with_ctx("chunk_id", index)
                .with_ctx("max_chunks", MAX_CHUNKS)
                .attach_note("chunk id exceeds maximum registered chunk count");
        }

        if ptr.is_null() || len == 0 {
            return UringError::InvalidInput
                .push_ctx("scope", scope)
                .with_ctx("chunk_id", index)
                .with_ctx("buffer_len", len)
                .attach_note("fixed-buffer registration requires a non-null pointer and length");
        }
        Ok(index)
    }

    fn set_chunk_record(
        &mut self,
        id: ChunkId,
        ptr: *const u8,
        len: usize,
        state: ChunkRegistrationState,
    ) {
        let last_failure = (state == ChunkRegistrationState::Fallback)
            .then(|| self.chunk_records[id.as_usize()].and_then(|record| record.last_failure()))
            .flatten();
        self.chunk_records[id.as_usize()] =
            Some(ChunkRegistrationRecord::new(id, ptr, len, state).with_last_failure(last_failure));
    }

    fn clear_chunk_failure(&mut self, index: usize) {
        if let Some(record) = self.chunk_records[index].as_mut() {
            record.clear_last_failure();
        }
    }

    #[cfg(feature = "test-hooks")]
    fn note_chunk_registration_failure(&mut self, id: ChunkId, ptr: *const u8, len: usize) {
        self.registration_stats.inc_chunk_register_failures();
        self.set_chunk_record(id, ptr, len, ChunkRegistrationState::Fallback);
        if let Some(record) = self.chunk_records[id.as_usize()].as_mut() {
            record.set_last_failure(Instant::now());
        }
    }

    fn cache_is_registered(&self, index: usize) -> bool {
        self.registered_chunks_cache.get(index).unwrap_or(false)
    }

    #[inline]
    fn is_record_registered(&self, id: ChunkId) -> bool {
        let index = id.as_usize();
        self.chunk_records
            .get(index)
            .and_then(Option::as_ref)
            .is_some_and(|record| record.state() == ChunkRegistrationState::Registered)
    }

    fn commit_chunk_registration(
        &mut self,
        index: usize,
        id: ChunkId,
        ptr: *const u8,
        len: usize,
    ) -> UringResult<()> {
        // The record is authoritative. The bitset is updated afterwards and is only a derived
        // cache used to diagnose an incomplete commit.
        self.set_chunk_record(id, ptr, len, ChunkRegistrationState::Registered);
        self.mark_registered_cache(index)
    }

    fn recover_failed_commit(
        &mut self,
        submitter: &Submitter<'_>,
        id: ChunkId,
        ptr: *const u8,
        len: usize,
        set_report: Report<UringError>,
    ) -> UringResult<BufferRegistrationStatus> {
        let index = id.as_usize();
        let zero_iovec = [libc::iovec {
            iov_base: ptr::null_mut(),
            iov_len: 0,
        }];
        let cleanup = self.issue_chunk_update(submitter, index as u32, &zero_iovec);
        match cleanup.outcome() {
            KernelUpdateOutcome::Applied(updated) if updated == cleanup.requested() => {
                self.set_chunk_record(id, ptr, len, ChunkRegistrationState::Fallback);
                self.clear_registered_cache(index)?;
                if self.registration_mode.is_strict() {
                    Err(set_report.attach_note(
                        "fixed-buffer registration was cleared after ledger cache update failed",
                    ))
                } else {
                    self.registration_unavailable(
                        id,
                        None,
                        "fixed-buffer ledger commit failed; using raw buffer I/O",
                    )
                }
            }
            _ => {
                self.quarantine_chunk(
                    id,
                    ptr,
                    len,
                    UpdateEvidence::new(1, KernelUpdateOutcome::Applied(1)),
                    Some(cleanup),
                );
                let report = set_report
                    .with_ctx("cleanup_evidence", format!("{:?}", cleanup.outcome()))
                    .attach_note(
                        "fixed-buffer cleanup result is not fully applied; registry is quarantined",
                    );
                Err(report)
            }
        }
    }

    fn fixed_identity_mismatch_report(
        &self,
        id: ChunkId,
        ptr: *const u8,
        len: usize,
        scope: &'static str,
        user_data: Option<usize>,
        record: ChunkRegistrationRecord,
    ) -> Report<UringError> {
        let report = UringError::InvalidState
            .to_report()
            .push_ctx("scope", scope)
            .with_ctx("chunk_id", id.raw())
            .with_ctx("buffer_ptr", ptr as usize)
            .with_ctx("buffer_len", len)
            .with_ctx("registered_ptr", record.ptr())
            .with_ctx("registered_len", record.len())
            .attach_note("fixed-buffer ledger identity does not match the registered kernel entry");
        if let Some(user_data) = user_data {
            report.with_ctx("user_data", user_data)
        } else {
            report
        }
    }

    fn identity_mismatch_status(
        &mut self,
        id: ChunkId,
        ptr: *const u8,
        len: usize,
        scope: &'static str,
        user_data: Option<usize>,
        record: ChunkRegistrationRecord,
    ) -> UringResult<BufferRegistrationStatus> {
        let report = self.fixed_identity_mismatch_report(id, ptr, len, scope, user_data, record);
        if self.registration_mode.is_strict() {
            Err(report)
        } else {
            self.registration_unavailable(
                id,
                None,
                "fixed-buffer identity mismatch; using raw buffer I/O",
            )
        }
    }

    fn chunk_ledger_is_consistent(&self) -> bool {
        (0..MAX_CHUNKS).all(|index| {
            let registered = self.registered_chunks_cache.get(index).unwrap_or(false);
            let ledger_registered = self.chunk_records[index]
                .is_some_and(|record| record.state() == ChunkRegistrationState::Registered);
            registered == ledger_registered
        })
    }

    /// Registers `chunk_id` on demand so the kernel can reach the buffer this SQE points at.
    ///
    /// Runs after `make_sqe`, which means the very first submission touching a chunk uses the
    /// non-fixed opcode and only later ones get `ReadFixed`/`WriteFixed`.
    pub(crate) fn ensure_chunk_registered(
        &mut self,
        submitter: &Submitter<'_>,
        diagnostics: &UringCompletionDiagnostics,
        chunk_id: ChunkId,
        user_data: usize,
        scope: &'static str,
    ) -> UringResult<ChunkRegistrationDecision> {
        let index = chunk_id.as_usize();
        if index >= MAX_CHUNKS {
            return UringError::InvalidInput
                .push_ctx("scope", scope)
                .with_ctx("chunk_id", index)
                .with_ctx("max_chunks", MAX_CHUNKS)
                .with_ctx("user_data", user_data)
                .attach_note("chunk id exceeds maximum registered chunk count");
        }
        self.ensure_registration_healthy(scope, chunk_id, Some(user_data))?;
        let record = self.chunk_records[index];
        let Some(info) = self.registrar.resolve_chunk_info(chunk_id) else {
            self.registration_stats.inc_submission_missing_chunk_info();
            if record.is_some_and(|record| record.state() == ChunkRegistrationState::Registered)
                || self.registration_mode.is_strict()
            {
                return UringError::InvalidState
                    .push_ctx("scope", scope)
                    .with_ctx("chunk_id", chunk_id.raw())
                    .with_ctx("user_data", user_data)
                    .attach_note("fixed-buffer identity cannot be verified without chunk info");
            }
            return Ok(self.raw_fallback(
                chunk_id,
                user_data,
                None,
                "missing chunk info for lazy registration",
            ));
        };

        if info.id != chunk_id {
            return UringError::InvalidState
                .push_ctx("scope", scope)
                .with_ctx("chunk_id", chunk_id.raw())
                .with_ctx("resolved_chunk_id", info.id.raw())
                .with_ctx("user_data", user_data)
                .attach_note("chunk registrar returned mismatched chunk info");
        }

        if let Some(record) = record
            && record.state() == ChunkRegistrationState::Registered
        {
            if record.matches(info.id, info.ptr.as_ptr(), info.len.get()) {
                return Ok(ChunkRegistrationDecision::Fixed);
            }
            diagnostics.inc_fixed_chunk_identity_mismatch();
            return match self.identity_mismatch_status(
                chunk_id,
                info.ptr.as_ptr(),
                info.len.get(),
                scope,
                Some(user_data),
                record,
            )? {
                BufferRegistrationStatus::Registered => Ok(ChunkRegistrationDecision::Fixed),
                BufferRegistrationStatus::Unavailable => Ok(self.raw_fallback(
                    chunk_id,
                    user_data,
                    None,
                    "fixed-buffer identity mismatch",
                )),
            };
        }

        if !self.fixed_buffers_available {
            if self.registration_mode.is_strict() {
                return Err(self.fixed_buffers_unavailable_report(
                    scope,
                    chunk_id,
                    Some(user_data),
                ));
            }
            return Ok(self.raw_fallback(
                chunk_id,
                user_data,
                self.fixed_buffers_failure_errno,
                "sparse fixed-buffer registration unavailable",
            ));
        }

        // Resolving a missing local snapshot may have drained the worker's chunk message queue;
        // the backend may have registered the chunk through another path in the meantime.
        if self.is_record_registered(info.id) {
            return Ok(ChunkRegistrationDecision::Fixed);
        }

        match self.register_buffer_backend(
            submitter,
            diagnostics,
            info.id,
            info.ptr.as_ptr(),
            info.len.get(),
        )? {
            BufferRegistrationStatus::Registered => Ok(ChunkRegistrationDecision::Fixed),
            BufferRegistrationStatus::Unavailable => Ok(self.raw_fallback(
                chunk_id,
                user_data,
                self.fixed_buffers_failure_errno,
                "chunk fixed-buffer registration unavailable",
            )),
        }
    }

    fn registration_unavailable(
        &mut self,
        chunk_id: ChunkId,
        errno: Option<i32>,
        reason: &'static str,
    ) -> UringResult<BufferRegistrationStatus> {
        debug!(
            chunk_id = chunk_id.raw(),
            errno = ?errno,
            registration_mode = self.registration_mode.as_str(),
            fallback = true,
            reason,
            "fixed-buffer registration unavailable; using raw buffer I/O"
        );
        Ok(BufferRegistrationStatus::Unavailable)
    }

    fn fixed_buffers_unavailable_report(
        &self,
        scope: &'static str,
        chunk_id: ChunkId,
        user_data: Option<usize>,
    ) -> Report<UringError> {
        let report = UringError::Registration
            .to_report()
            .push_ctx("scope", scope)
            .with_ctx("chunk_id", chunk_id.raw())
            .with_ctx("registration_mode", self.registration_mode.as_str())
            .with_ctx("fallback", false)
            .attach_note("sparse fixed-buffer registration is unavailable");
        let report = if let Some(user_data) = user_data {
            report.with_ctx("user_data", user_data)
        } else {
            report
        };
        if let Some(errno) = self.fixed_buffers_failure_errno {
            report.with_ctx("errno", errno)
        } else {
            report
        }
    }

    fn raw_fallback(
        &mut self,
        chunk_id: ChunkId,
        user_data: usize,
        errno: Option<i32>,
        reason: &'static str,
    ) -> ChunkRegistrationDecision {
        self.registration_stats.inc_raw_buffer_fallbacks();
        debug!(
            chunk_id = chunk_id.raw(),
            user_data,
            errno = ?errno,
            registration_mode = self.registration_mode.as_str(),
            fallback = true,
            reason,
            "using raw buffer I/O"
        );
        ChunkRegistrationDecision::RawFallback
    }

    fn issue_chunk_update(
        &mut self,
        submitter: &Submitter<'_>,
        index: u32,
        iovecs: &[libc::iovec],
    ) -> UpdateEvidence {
        #[cfg(feature = "test-hooks")]
        if let Some(injection) = self.register_buffers_update_outcomes.pop_front() {
            let outcome = match injection {
                BufferUpdateInjection::Applied => KernelUpdateOutcome::Applied(iovecs.len()),
                BufferUpdateInjection::Rejected(errno) => KernelUpdateOutcome::Rejected(errno),
                BufferUpdateInjection::Unknown(errno) => {
                    KernelUpdateOutcome::Unknown(Some(errno), "test-injected unknown buffer update")
                }
            };
            return UpdateEvidence::new(iovecs.len(), outcome);
        }

        let Some(registration) = self.fixed_buffer_registration else {
            return UpdateEvidence::new(
                iovecs.len(),
                KernelUpdateOutcome::Unknown(None, "fixed-buffer registration token is missing"),
            );
        };

        // SAFETY: `iovecs` points at live chunk memory for the duration of this syscall, and the
        // caller retains ownership of that memory until every in-flight operation completes.
        let outcome =
            match unsafe { submitter.register_buffers_update(registration, index, iovecs) } {
                Ok(updated) => KernelUpdateOutcome::Applied(updated),
                Err(error) => KernelUpdateOutcome::Unknown(
                    error.raw_os_error(),
                    "kernel fixed-buffer update returned an error",
                ),
            };
        UpdateEvidence::new(iovecs.len(), outcome)
    }

    fn chunk_update_report(
        &self,
        scope: &'static str,
        evidence: UpdateEvidence,
        note: &'static str,
    ) -> Report<UringError> {
        let report = UringError::Registration
            .to_report()
            .push_ctx("scope", scope)
            .with_ctx("requested_buffers", evidence.requested())
            .with_ctx("update_evidence", format!("{:?}", evidence.outcome()))
            .attach_note(note);
        match evidence.outcome() {
            KernelUpdateOutcome::Applied(_) => report,
            #[cfg(feature = "test-hooks")]
            KernelUpdateOutcome::Rejected(errno) => report.with_ctx("errno", errno),
            KernelUpdateOutcome::Unknown(errno, _) => {
                if let Some(errno) = errno {
                    report.with_ctx("errno", errno)
                } else {
                    report
                }
            }
        }
    }

    fn quarantine_chunk(
        &mut self,
        id: ChunkId,
        ptr: *const u8,
        len: usize,
        register: UpdateEvidence,
        cleanup: Option<UpdateEvidence>,
    ) {
        if let Some(quarantine) = self.registration_quarantine.as_mut() {
            quarantine.note_anomaly();
        } else {
            *self.registration_quarantine = Some(BufferRegistrationQuarantine::new(
                id,
                register,
                cleanup,
                "driver.register_buffer_internal",
            ));
        }
        self.set_chunk_record(id, ptr, len, ChunkRegistrationState::Quarantined);
    }

    fn clear_registered_cache(&mut self, index: usize) -> UringResult<()> {
        self.registered_chunks_cache.clear(index).map_err(|error| {
            UringError::InvalidState
                .to_report()
                .push_ctx("scope", "driver.register_buffer_internal.bitset_clear")
                .with_ctx("chunk_index", index)
                .with_ctx("bitset_error", format!("{error:?}"))
                .attach_note("failed to clear derived fixed-buffer registration cache")
        })
    }

    fn mark_registered_cache(&mut self, index: usize) -> UringResult<()> {
        #[cfg(feature = "test-hooks")]
        if *self.bitset_set_failure {
            *self.bitset_set_failure = false;
            return UringError::InvalidState
                .push_ctx("scope", "driver.register_buffer_internal.bitset_set")
                .with_ctx("chunk_index", index)
                .attach_note("injected registered chunk bitset failure");
        }

        self.registered_chunks_cache.set(index).map_err(|e| {
            UringError::InvalidState
                .to_report()
                .push_ctx("scope", "driver.register_buffer_internal.bitset_set")
                .with_ctx("chunk_index", index)
                .with_ctx("bitset_error", format!("{e:?}"))
                .attach_note("BitSet set failed after kernel registration")
        })
    }

    fn ensure_registration_healthy(
        &self,
        scope: &'static str,
        chunk_id: ChunkId,
        user_data: Option<usize>,
    ) -> UringResult<()> {
        let Some(quarantine) = self.registration_quarantine.as_ref() else {
            return Ok(());
        };

        let mut report = UringError::InvalidState
            .to_report()
            .push_ctx("scope", scope)
            .with_ctx("chunk_id", chunk_id.raw())
            .with_ctx("quarantine_chunk_id", quarantine.chunk_id().raw())
            .with_ctx("quarantine_scope", quarantine.scope())
            .with_ctx("quarantine_anomalies", quarantine.anomaly_count())
            .with_ctx(
                "registration_evidence",
                format!("{:?}", quarantine.register_outcome()),
            )
            .with_ctx(
                "cleanup_evidence",
                format!("{:?}", quarantine.cleanup_outcome()),
            )
            .attach_note("fixed-buffer registry is quarantined and requires ring rebuild");
        if let Some(errno) = quarantine.register_errno() {
            report = report.with_ctx("registration_errno", errno);
        }
        if let Some(errno) = quarantine.cleanup_errno() {
            report = report.with_ctx("cleanup_errno", errno);
        }
        if let Some(user_data) = user_data {
            Err(report.with_ctx("user_data", user_data))
        } else {
            Err(report)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        BufferRegistrationMode, ChunkRegistrationRecord, ChunkRegistrationState, FixedBufferOwner,
    };
    use veloq_buf::{NoopRegistrar, heap::ChunkId};

    #[test]
    fn chunk_ledger_and_fast_bitset_agree_only_for_registered_records() {
        static REGISTRAR: NoopRegistrar = NoopRegistrar;
        let mut registry = FixedBufferOwner::new(BufferRegistrationMode::Compatible, &REGISTRAR);
        let chunk_id = ChunkId::from_raw(2);
        let byte = 0_u8;

        assert!(registry.chunk_ledger_is_consistent());
        registry.chunk_records[chunk_id.as_usize()] = Some(ChunkRegistrationRecord::new(
            chunk_id,
            &byte,
            1,
            ChunkRegistrationState::Fallback,
        ));
        assert!(registry.chunk_ledger_is_consistent());

        registry
            .registered_chunks_cache
            .set(chunk_id.as_usize())
            .unwrap();
        assert!(!registry.chunk_ledger_is_consistent());
        registry.chunk_records[chunk_id.as_usize()] = Some(ChunkRegistrationRecord::new(
            chunk_id,
            &byte,
            1,
            ChunkRegistrationState::Registered,
        ));
        assert!(registry.chunk_ledger_is_consistent());
    }

    #[test]
    fn chunk_identity_includes_pointer_and_length() {
        let chunk_id = ChunkId::from_raw(3);
        let first = 0_u8;
        let second = 0_u8;
        let record =
            ChunkRegistrationRecord::new(chunk_id, &first, 4, ChunkRegistrationState::Registered);

        assert!(record.matches(chunk_id, &first, 4));
        assert!(!record.matches(chunk_id, &second, 4));
        assert!(!record.matches(chunk_id, &first, 8));
    }
}
