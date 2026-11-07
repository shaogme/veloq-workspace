mod ledger;

use ledger::BufferRegistrationQuarantine;
pub(crate) use ledger::{ChunkRegistrationRecord, ChunkRegistrationState};

use crate::{
    diagnostics::UringCompletionDiagnostics,
    driver::env::{ProvidedBufSqeInfo, SqeEnv},
    driver::registration::{
        FileTable, KernelUpdateOutcome, MAX_CHUNKS, REGISTER_FAILURE_RETRY_COOLDOWN,
        UpdateEvidence, UringRegistrationStats,
    },
    error::{UringError, UringResult},
};
use diagweave::prelude::*;
use libc::iovec;
use veloq_buf::{
    AnyBufPool, BufferBackingOwner, BufferRegistrationRequest, LockedMemoryBudget,
    LockedMemoryPermit, heap::ChunkId,
};
use veloq_driver_core::driver::BufferRegistrationStatus;
use veloq_io_uring::{ResourceKind, ResourceRegistration, ResourceRegistrationState, Submitter};
use veloq_std::{
    boxed::Box, collections::BitSet, format, marker::PhantomData, ptr, sync::Arc, vec, vec::Vec,
};

#[cfg(feature = "test-hooks")]
use veloq_std::time::Instant;

#[cfg(feature = "test-hooks")]
use veloq_std::collections::VecDeque;

pub(crate) struct FixedBufferSubmitPort<'r, 'a> {
    registered_chunks_cache: &'r mut BitSet,
    chunk_records: &'r mut [Option<ChunkRegistrationRecord>],
    chunk_owners: &'r mut [Option<BufferRegistrationOwner>],
    budget: &'r LockedMemoryBudget,
    _lifetime: PhantomData<&'a ()>,
    registration_stats: &'r mut UringRegistrationStats,
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

#[derive(Clone, Copy)]
struct BufferIdentity {
    id: ChunkId,
    ptr: *const u8,
    len: usize,
}

struct BufferRegistrationOwner {
    _permit: LockedMemoryPermit,
    _backing_owner: Arc<dyn BufferBackingOwner>,
}

impl BufferRegistrationOwner {
    fn new(permit: LockedMemoryPermit, backing_owner: Arc<dyn BufferBackingOwner>) -> Self {
        Self {
            _permit: permit,
            _backing_owner: backing_owner,
        }
    }
}

pub(crate) struct FixedBufferOwner<'a> {
    /// A derived fast cache; `chunk_records` is the only registration source of truth. 该位图是派生的快速缓存，注册状态只以 `chunk_records` 为准。
    registered_chunks_cache: BitSet,
    chunk_records: Box<[Option<ChunkRegistrationRecord>]>,
    chunk_owners: Box<[Option<BufferRegistrationOwner>]>,
    _lifetime: PhantomData<&'a ()>,
    _pool_owner: Option<AnyBufPool>,
    stats: UringRegistrationStats,
    budget: LockedMemoryBudget,
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
    pub(crate) fn new(budget: LockedMemoryBudget) -> Self {
        Self {
            registered_chunks_cache: BitSet::new(MAX_CHUNKS),
            chunk_records: vec![None; MAX_CHUNKS].into_boxed_slice(),
            chunk_owners: (0..MAX_CHUNKS)
                .map(|_| None)
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            _lifetime: PhantomData,
            _pool_owner: None,
            stats: UringRegistrationStats::default(),
            budget,
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

    pub(crate) fn retain_pool(&mut self, pool: AnyBufPool) -> UringResult<()> {
        if self._pool_owner.is_some() {
            return UringError::InvalidState
                .attach_note("io_uring driver already owns a buffer pool");
        }
        self._pool_owner = Some(pool);
        Ok(())
    }

    pub(crate) fn split_for_submit(
        &mut self,
        provided: Option<ProvidedBufSqeInfo>,
    ) -> FixedBufferSubmitPort<'_, 'a> {
        debug_assert!(self.chunk_ledger_is_consistent());
        FixedBufferSubmitPort {
            registered_chunks_cache: &mut self.registered_chunks_cache,
            chunk_records: &mut self.chunk_records,
            chunk_owners: &mut self.chunk_owners,
            budget: &self.budget,
            _lifetime: PhantomData,
            registration_stats: &mut self.stats,
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

    pub(crate) fn register_buffer_internal(
        &mut self,
        submitter: &Submitter<'_>,
        diagnostics: &UringCompletionDiagnostics,
        request: BufferRegistrationRequest,
    ) -> UringResult<BufferRegistrationStatus> {
        self.split_for_submit(None)
            .register_buffer_backend(submitter, diagnostics, request)
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
                .is_some_and(|record| record.state() == ChunkRegistrationState::Registered);
            registered == ledger_registered
        })
    }

    #[cfg(feature = "test-hooks")]
    #[inline]
    fn record_is_registered(&self, index: usize) -> bool {
        let Some(record) = self.chunk_records.get(index).copied().flatten() else {
            return false;
        };
        let registered = record.state() == ChunkRegistrationState::Registered;
        debug_assert_eq!(
            self.registered_chunks_cache.get(index).unwrap_or(false),
            registered,
            "fixed-buffer cache diverged from the resource ledger"
        );
        registered
    }
}

impl FixedBufferSubmitPort<'_, '_> {
    /// Narrows the mutable registration projection to the immutable SQE view. 将可变注册视图收窄为供 SQE 使用的只读视图。
    pub(crate) fn sqe_env<'view>(&'view self, file_table: &'view FileTable) -> SqeEnv<'view> {
        SqeEnv::from_parts(file_table, self.chunk_records, self.provided)
    }

    pub(crate) fn validate_registered_chunk(
        &self,
        chunk_id: ChunkId,
        user_data: usize,
        scope: &'static str,
    ) -> UringResult<()> {
        let index = chunk_id.as_usize();
        if index >= MAX_CHUNKS {
            return UringError::InvalidInput
                .push_ctx("scope", scope)
                .with_ctx("chunk_id", index)
                .with_ctx("max_chunks", MAX_CHUNKS)
                .with_ctx("user_data", user_data)
                .attach_note("registered buffer id exceeds maximum chunk count");
        }
        self.ensure_registration_healthy(scope, chunk_id, Some(user_data))?;
        if self.is_record_registered(chunk_id) {
            Ok(())
        } else {
            Err(UringError::InvalidState
                .to_report()
                .push_ctx("scope", scope)
                .with_ctx("chunk_id", chunk_id.raw())
                .with_ctx("user_data", user_data)
                .attach_note("buffer declares registration without a worker registration record"))
        }
    }

    /// Registers `[ptr, ptr + len)` as the kernel's fixed buffer number `id`. 将 `[ptr, ptr + len)` 注册为内核固定 buffer `id`。
    fn register_buffer_backend(
        &mut self,
        submitter: &Submitter<'_>,
        diagnostics: &UringCompletionDiagnostics,
        mut request: BufferRegistrationRequest,
    ) -> UringResult<BufferRegistrationStatus> {
        let identity = BufferIdentity {
            id: request.id(),
            ptr: request.ptr().as_ptr(),
            len: request.len(),
        };
        let BufferIdentity { id, ptr, len } = identity;
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
            return self.identity_mismatch_status(identity, scope, None, record);
        }

        if let Some(last_fail) = self.chunk_records[index].and_then(|record| record.last_failure())
        {
            if last_fail.elapsed() < REGISTER_FAILURE_RETRY_COOLDOWN {
                self.registration_stats
                    .inc_chunk_register_skipped_recent_failure();
                self.set_chunk_record(id, ptr, len, ChunkRegistrationState::Rejected);
                return UringError::Registration
                    .push_ctx("scope", scope)
                    .with_ctx("chunk_id", id.raw())
                    .attach_note("recent chunk registration failure cooldown");
            }
            self.clear_chunk_failure(index);
        }

        let required_charge = request.required_charge(self.budget).map_err(|error| {
            UringError::InvalidInput
                .to_report()
                .push_ctx("scope", scope)
                .with_ctx("chunk_id", id.raw())
                .with_ctx("charge_error", format!("{error:?}"))
                .attach_note("fixed-buffer range cannot be charged safely")
        })?;
        let permit = match request.take_permit() {
            Some(permit) => permit,
            None => match request.try_reserve(self.budget) {
                Ok(Some(permit)) => permit,
                Ok(None) => return Ok(BufferRegistrationStatus::BudgetExceeded),
                Err(error) => {
                    return UringError::InvalidInput
                        .push_ctx("scope", scope)
                        .with_ctx("chunk_id", id.raw())
                        .with_ctx("charge_error", format!("{error:?}"))
                        .attach_note("fixed-buffer range cannot be charged safely");
                }
            },
        };
        if !permit.belongs_to(self.budget) {
            return UringError::InvalidInput
                .push_ctx("scope", scope)
                .with_ctx("chunk_id", id.raw())
                .attach_note("reservation belongs to a different locked-memory budget");
        }
        if permit.charge_bytes() < required_charge {
            return UringError::InvalidInput
                .push_ctx("scope", scope)
                .with_ctx("chunk_id", id.raw())
                .with_ctx("permit_charge", permit.charge_bytes())
                .with_ctx("required_charge", required_charge)
                .attach_note("reservation does not cover the registered page footprint");
        }
        if !self.fixed_buffers_available {
            return Err(self.fixed_buffers_unavailable_report(scope, id, None));
        }

        let iovecs = [iovec {
            iov_base: ptr as *mut _,
            iov_len: len,
        }];
        self.chunk_owners[index] = Some(BufferRegistrationOwner::new(
            permit,
            request.backing_owner(),
        ));
        self.set_chunk_record(id, ptr, len, ChunkRegistrationState::Registering);
        self.registration_stats.inc_chunk_register_attempts();
        let register = self.issue_chunk_update(submitter, index as u32, &iovecs);

        match register.outcome() {
            KernelUpdateOutcome::Applied(updated) if updated == register.requested() => {
                if let Err(set_report) = self.commit_chunk_registration(index, id, ptr, len) {
                    return self.recover_failed_commit(submitter, identity, set_report);
                }
                self.registration_stats.inc_chunk_register_success();
                debug_assert!(self.chunk_ledger_is_consistent());
                Ok(BufferRegistrationStatus::Registered)
            }
            #[cfg(feature = "test-hooks")]
            KernelUpdateOutcome::Rejected(_) => {
                self.note_chunk_registration_failure(id, ptr, len);
                self.chunk_owners[index] = None;
                let report = self.chunk_update_report(
                    "driver.register_buffer_internal.register_buffers_update",
                    register,
                    "fixed-buffer registration was rejected before reaching the kernel",
                );
                Err(report)
            }
            KernelUpdateOutcome::Unknown(_, _) => {
                self.registration_stats.inc_chunk_register_failures();
                self.quarantine_chunk(identity, register, None);
                Err(self.chunk_update_report(
                    "driver.register_buffer_internal.register_buffers_update",
                    register,
                    "fixed-buffer registration result is unknown; ring is quarantined",
                ))
            }
            KernelUpdateOutcome::Applied(_) => {
                self.registration_stats.inc_chunk_register_failures();
                self.quarantine_chunk(identity, register, None);
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
        let last_failure = (state == ChunkRegistrationState::Rejected)
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
        self.set_chunk_record(id, ptr, len, ChunkRegistrationState::Rejected);
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
        // cache used to diagnose an incomplete commit. 记录是权威状态；位图仅在其后更新，作为检测提交未完成的派生缓存。
        self.set_chunk_record(id, ptr, len, ChunkRegistrationState::Registered);
        self.mark_registered_cache(index)
    }

    fn recover_failed_commit(
        &mut self,
        submitter: &Submitter<'_>,
        identity: BufferIdentity,
        set_report: Report<UringError>,
    ) -> UringResult<BufferRegistrationStatus> {
        let BufferIdentity { id, ptr, len } = identity;
        let index = id.as_usize();
        let zero_iovec = [iovec {
            iov_base: ptr::null_mut(),
            iov_len: 0,
        }];
        let cleanup = self.issue_chunk_update(submitter, index as u32, &zero_iovec);
        match cleanup.outcome() {
            KernelUpdateOutcome::Applied(updated) if updated == cleanup.requested() => {
                self.set_chunk_record(id, ptr, len, ChunkRegistrationState::Rejected);
                self.chunk_owners[index] = None;
                self.clear_registered_cache(index)?;
                Err(set_report.attach_note(
                    "fixed-buffer registration was cleared after ledger cache update failed",
                ))
            }
            _ => {
                self.quarantine_chunk(
                    identity,
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
        identity: BufferIdentity,
        scope: &'static str,
        user_data: Option<usize>,
        record: ChunkRegistrationRecord,
    ) -> Report<UringError> {
        let BufferIdentity { id, ptr, len } = identity;
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
        identity: BufferIdentity,
        scope: &'static str,
        user_data: Option<usize>,
        record: ChunkRegistrationRecord,
    ) -> UringResult<BufferRegistrationStatus> {
        let report = self.fixed_identity_mismatch_report(identity, scope, user_data, record);
        Err(report)
    }

    fn chunk_ledger_is_consistent(&self) -> bool {
        (0..MAX_CHUNKS).all(|index| {
            let registered = self.registered_chunks_cache.get(index).unwrap_or(false);
            let ledger_registered = self.chunk_records[index]
                .is_some_and(|record| record.state() == ChunkRegistrationState::Registered);
            registered == ledger_registered
        })
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

    fn issue_chunk_update(
        &mut self,
        submitter: &Submitter<'_>,
        index: u32,
        iovecs: &[iovec],
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
        // caller retains ownership of that memory until every in-flight operation completes. 安全性：系统调用期间 `iovecs` 指向有效 chunk 内存，调用方会持有内存直到所有在途操作完成。
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
        identity: BufferIdentity,
        register: UpdateEvidence,
        cleanup: Option<UpdateEvidence>,
    ) {
        let BufferIdentity { id, ptr, len } = identity;
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
    use super::{ChunkRegistrationRecord, ChunkRegistrationState, FixedBufferOwner};
    use veloq_buf::LockedMemoryBudget;
    use veloq_buf::heap::ChunkId;

    #[test]
    fn chunk_ledger_and_fast_bitset_agree_only_for_registered_records() {
        let mut registry = FixedBufferOwner::new(LockedMemoryBudget::default());
        let chunk_id = ChunkId::from_raw(2);
        let byte = 0_u8;

        assert!(registry.chunk_ledger_is_consistent());
        registry.chunk_records[chunk_id.as_usize()] = Some(ChunkRegistrationRecord::new(
            chunk_id,
            &byte,
            1,
            ChunkRegistrationState::Rejected,
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
