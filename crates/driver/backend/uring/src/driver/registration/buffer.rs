use super::{
    KernelUpdateOutcome, MAX_CHUNKS, UpdateEvidence, UringRegistrationStats,
    provided_buf::{
        ProvidedBufGroup, ProvidedBufStats, ProvidedBufUnregisterFailure,
        ProvidedBufUnregisterResult, RingLifetimeToken,
    },
};
use crate::{
    config::{BufferRegistrationMode, ProvidedBufConfig},
    driver::env::ProvidedBufSqeInfo,
    error::UringResult,
};
use veloq_buf::{AnyBufPool, BufferRegistrar, heap::ChunkId};
use veloq_std::{boxed::Box, collections::BitSet, time::Instant, vec};

#[cfg(feature = "test-hooks")]
use veloq_std::collections::VecDeque;

#[derive(Debug, Clone, Copy)]
pub(crate) struct BufferRegistrationQuarantine {
    pub(crate) chunk_id: ChunkId,
    pub(crate) register_errno: Option<i32>,
    pub(crate) cleanup_errno: Option<i32>,
    pub(crate) register_outcome: KernelUpdateOutcome,
    pub(crate) cleanup_outcome: Option<KernelUpdateOutcome>,
    pub(crate) scope: &'static str,
    pub(crate) anomaly_count: u64,
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
/// `None` in [`UringBufferRegistry::chunk_records`] is the `Absent` state. The remaining states
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
    pub(crate) ptr: usize,
    pub(crate) len: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ChunkRegistrationRecord {
    pub(crate) id: ChunkId,
    pub(crate) ptr: usize,
    pub(crate) len: usize,
    pub(crate) identity: ChunkRegistrationIdentity,
    pub(crate) state: ChunkRegistrationState,
    pub(crate) last_failure: Option<Instant>,
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
}

pub(crate) struct BufferRegistrySubmitView<'r, 'a> {
    pub(crate) registered_chunks_cache: &'r mut BitSet,
    pub(crate) chunk_records: &'r mut [Option<ChunkRegistrationRecord>],
    pub(crate) registrar: &'a (dyn BufferRegistrar + 'a),
    pub(crate) registration_stats: &'r mut UringRegistrationStats,
    pub(crate) registration_mode: BufferRegistrationMode,
    pub(crate) fixed_buffers_available: bool,
    pub(crate) fixed_buffers_failure_errno: Option<i32>,
    pub(crate) registration_quarantine: &'r mut Option<BufferRegistrationQuarantine>,
    #[cfg(feature = "test-hooks")]
    pub(crate) register_buffers_update_outcomes: &'r mut VecDeque<BufferUpdateInjection>,
    #[cfg(feature = "test-hooks")]
    pub(crate) bitset_set_failure: &'r mut bool,
    pub(crate) provided: Option<ProvidedBufSqeInfo>,
}

#[cfg(feature = "test-hooks")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BufferUpdateInjection {
    Applied,
    Rejected(i32),
    Unknown(i32),
}

pub(crate) struct UringBufferRegistry<'a> {
    /// A derived fast cache; `chunk_records` is the only registration source of truth.
    registered_chunks_cache: BitSet,
    chunk_records: Box<[Option<ChunkRegistrationRecord>]>,
    registrar: &'a (dyn BufferRegistrar + 'a),
    stats: UringRegistrationStats,
    mode: BufferRegistrationMode,
    fixed_buffers_available: bool,
    fixed_buffers_failure_errno: Option<i32>,
    registration_quarantine: Option<BufferRegistrationQuarantine>,
    #[cfg(feature = "test-hooks")]
    register_buffers_update_outcomes: VecDeque<BufferUpdateInjection>,
    #[cfg(feature = "test-hooks")]
    bitset_set_failure: bool,
    provided_buf_config: Option<ProvidedBufConfig>,
    provided_buffers: Option<ProvidedBufGroup>,
}

impl<'a> UringBufferRegistry<'a> {
    pub(crate) fn new(
        mode: BufferRegistrationMode,
        provided_buf_config: Option<ProvidedBufConfig>,
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
            registration_quarantine: None,
            #[cfg(feature = "test-hooks")]
            register_buffers_update_outcomes: VecDeque::new(),
            #[cfg(feature = "test-hooks")]
            bitset_set_failure: false,
            provided_buf_config,
            provided_buffers: None,
        }
    }

    pub(crate) fn split_for_submit(&mut self) -> BufferRegistrySubmitView<'_, 'a> {
        debug_assert!(self.chunk_ledger_is_consistent());
        let provided = self
            .provided_buffers
            .as_ref()
            .and_then(ProvidedBufGroup::sqe_info);
        BufferRegistrySubmitView {
            registered_chunks_cache: &mut self.registered_chunks_cache,
            chunk_records: &mut self.chunk_records,
            registrar: self.registrar,
            registration_stats: &mut self.stats,
            registration_mode: self.mode,
            fixed_buffers_available: self.fixed_buffers_available,
            fixed_buffers_failure_errno: self.fixed_buffers_failure_errno,
            registration_quarantine: &mut self.registration_quarantine,
            #[cfg(feature = "test-hooks")]
            register_buffers_update_outcomes: &mut self.register_buffers_update_outcomes,
            #[cfg(feature = "test-hooks")]
            bitset_set_failure: &mut self.bitset_set_failure,
            provided,
        }
    }

    pub(crate) fn set_fixed_buffers_available(&mut self, available: bool, errno: Option<i32>) {
        self.fixed_buffers_available = available;
        self.fixed_buffers_failure_errno = (!available).then_some(errno).flatten();
    }

    #[cfg(feature = "test-hooks")]
    #[inline]
    pub(crate) fn fixed_buffers_available(&self) -> bool {
        self.fixed_buffers_available
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

    #[inline]
    pub(crate) fn provided_buffers_mut(&mut self) -> Option<&mut ProvidedBufGroup> {
        self.provided_buffers.as_mut()
    }

    #[inline]
    pub(crate) fn provided_buf_stats(&self) -> Option<ProvidedBufStats> {
        self.provided_buffers.as_ref().map(ProvidedBufGroup::stats)
    }

    #[inline]
    pub(crate) fn has_provided_buffers(&self) -> bool {
        self.provided_buffers.is_some()
    }

    #[inline]
    pub(crate) fn provided_buffers_enabled(&self) -> bool {
        self.provided_buffers
            .as_ref()
            .is_some_and(ProvidedBufGroup::is_usable)
    }

    #[inline]
    pub(crate) fn provided_buffers_have_selected_bids(&self) -> bool {
        self.provided_buffers
            .as_ref()
            .is_some_and(ProvidedBufGroup::has_selected_bids)
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

    pub(crate) fn attach_buffer_pool(
        &mut self,
        submitter: &io_uring::Submitter<'_>,
        pool: AnyBufPool,
        ring_lifetime_token: RingLifetimeToken,
    ) -> UringResult<bool> {
        let Some(config) = self.provided_buf_config else {
            return Ok(false);
        };
        if self.provided_buffers.is_some() {
            return Ok(true);
        }

        match ProvidedBufGroup::new(submitter, config, pool, ring_lifetime_token) {
            Ok(group) => {
                self.provided_buffers = Some(group);
                Ok(true)
            }
            Err(report) => {
                tracing::debug!(
                    report = ?report,
                    "provided buffer ring unavailable; continuing without it"
                );
                Ok(false)
            }
        }
    }

    pub(crate) fn release_provided_buffers(
        &mut self,
        submitter: &io_uring::Submitter<'_>,
    ) -> UringResult<()> {
        self.release_provided_buffers_with(|group| group.try_unregister(submitter))
    }

    fn release_provided_buffers_with<F>(&mut self, unregister: F) -> UringResult<()>
    where
        F: FnOnce(ProvidedBufGroup) -> ProvidedBufUnregisterResult,
    {
        let Some(group) = self.provided_buffers.take() else {
            return Ok(());
        };

        match unregister(group) {
            Ok(()) => Ok(()),
            Err(failure) => {
                let ProvidedBufUnregisterFailure { report, group } = *failure;
                // 反注册失败时，内核可能仍持有 ring 地址；必须在返回错误前把 group 放回
                // registry，直到 `UringDriver` 的 `ring` 字段先于本 registry 析构。
                self.provided_buffers = Some(group);
                Err(report)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        BufferRegistrationMode, ChunkRegistrationRecord, ChunkRegistrationState,
        UringBufferRegistry,
    };
    use crate::driver::registration::provided_buf::test_group;
    use crate::error::UringResult;
    use veloq_buf::{NoopRegistrar, heap::ChunkId};

    #[test]
    fn failed_unregister_restores_group_to_registry() -> UringResult<()> {
        static REGISTRAR: NoopRegistrar = NoopRegistrar;
        let mut registry =
            UringBufferRegistry::new(BufferRegistrationMode::Compatible, None, &REGISTRAR);
        registry.provided_buffers = Some(test_group(2));
        let original_stats = registry.provided_buf_stats().expect("test group exists");

        let result = registry
            .release_provided_buffers_with(|group| group.try_unregister_with(|_| Err(libc::EIO)));

        assert!(result.is_err(), "injected unregister must fail");
        assert_eq!(registry.provided_buf_stats(), Some(original_stats));

        registry.release_provided_buffers_with(|group| group.try_unregister_with(|_| Ok(())))?;
        assert!(registry.provided_buffers.is_none());
        Ok(())
    }

    #[test]
    fn chunk_ledger_and_fast_bitset_agree_only_for_registered_records() {
        static REGISTRAR: NoopRegistrar = NoopRegistrar;
        let mut registry =
            UringBufferRegistry::new(BufferRegistrationMode::Compatible, None, &REGISTRAR);
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
