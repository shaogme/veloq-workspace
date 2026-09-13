use super::{
    MAX_CHUNKS, UringRegistrationStats,
    provided_buf::{
        ProvidedBufGroup, ProvidedBufStats, ProvidedBufUnregisterFailure,
        ProvidedBufUnregisterResult,
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
    pub(crate) scope: &'static str,
}

pub(crate) struct BufferRegistrySubmitView<'r, 'a> {
    pub(crate) registered_chunks: &'r mut BitSet,
    pub(crate) registrar: &'a (dyn BufferRegistrar + 'a),
    pub(crate) registration_stats: &'r mut UringRegistrationStats,
    pub(crate) registration_mode: BufferRegistrationMode,
    pub(crate) fixed_buffers_available: bool,
    pub(crate) fixed_buffers_failure_errno: Option<i32>,
    pub(crate) chunk_register_failure_at: &'r mut [Option<Instant>],
    pub(crate) registration_quarantine: &'r mut Option<BufferRegistrationQuarantine>,
    #[cfg(feature = "test-hooks")]
    pub(crate) register_buffers_update_outcomes: &'r mut VecDeque<Option<i32>>,
    #[cfg(feature = "test-hooks")]
    pub(crate) bitset_set_failure: &'r mut bool,
    pub(crate) provided: Option<ProvidedBufSqeInfo>,
}

pub(crate) struct UringBufferRegistry<'a> {
    registered_chunks: BitSet,
    registrar: &'a (dyn BufferRegistrar + 'a),
    stats: UringRegistrationStats,
    mode: BufferRegistrationMode,
    fixed_buffers_available: bool,
    fixed_buffers_failure_errno: Option<i32>,
    chunk_register_failure_at: Box<[Option<Instant>]>,
    registration_quarantine: Option<BufferRegistrationQuarantine>,
    #[cfg(feature = "test-hooks")]
    register_buffers_update_outcomes: VecDeque<Option<i32>>,
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
            registered_chunks: BitSet::new(MAX_CHUNKS),
            registrar,
            stats: UringRegistrationStats::default(),
            mode,
            fixed_buffers_available: false,
            fixed_buffers_failure_errno: None,
            chunk_register_failure_at: vec![None; MAX_CHUNKS].into_boxed_slice(),
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
        let provided = self
            .provided_buffers
            .as_ref()
            .map(ProvidedBufGroup::sqe_info);
        BufferRegistrySubmitView {
            registered_chunks: &mut self.registered_chunks,
            registrar: self.registrar,
            registration_stats: &mut self.stats,
            registration_mode: self.mode,
            fixed_buffers_available: self.fixed_buffers_available,
            fixed_buffers_failure_errno: self.fixed_buffers_failure_errno,
            chunk_register_failure_at: &mut self.chunk_register_failure_at,
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
        self.register_buffers_update_outcomes.push_back(Some(errno));
    }

    #[cfg(feature = "test-hooks")]
    pub(crate) fn inject_register_buffers_update_sequence(&mut self, outcomes: &[Option<i32>]) {
        self.register_buffers_update_outcomes.clear();
        self.register_buffers_update_outcomes
            .extend(outcomes.iter().copied());
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
        self.registered_chunks
            .get(chunk_id.as_usize())
            .unwrap_or(false)
    }

    #[inline]
    pub(crate) fn provided_buffers_mut(&mut self) -> Option<&mut ProvidedBufGroup> {
        self.provided_buffers.as_mut()
    }

    #[inline]
    pub(crate) fn provided_buf_stats(&self) -> Option<ProvidedBufStats> {
        self.provided_buffers.as_ref().map(ProvidedBufGroup::stats)
    }

    pub(crate) fn attach_buffer_pool(
        &mut self,
        submitter: &io_uring::Submitter<'_>,
        pool: AnyBufPool,
    ) -> UringResult<bool> {
        let Some(config) = self.provided_buf_config else {
            return Ok(false);
        };
        if self.provided_buffers.is_some() {
            return Ok(true);
        }

        match ProvidedBufGroup::new(submitter, config, pool) {
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
    use super::{BufferRegistrationMode, UringBufferRegistry};
    use crate::driver::registration::provided_buf::test_group;
    use crate::error::UringResult;
    use veloq_buf::NoopRegistrar;

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
}
