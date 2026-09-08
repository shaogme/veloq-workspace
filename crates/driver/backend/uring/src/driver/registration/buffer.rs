use super::{
    MAX_CHUNKS, UringRegistrationStats,
    provided_buf::{ProvidedBufGroup, ProvidedBufStats},
};
use crate::{
    config::{BufferRegistrationMode, ProvidedBufConfig},
    driver::env::ProvidedBufSqeInfo,
    error::UringResult,
};
use veloq_buf::{AnyBufPool, BufferRegistrar};
use veloq_std::{boxed::Box, collections::BitSet, time::Instant, vec};

pub(crate) struct BufferRegistrySubmitView<'r, 'a> {
    pub(crate) registered_chunks: &'r mut BitSet,
    pub(crate) registrar: &'a (dyn BufferRegistrar + 'a),
    pub(crate) registration_stats: &'r mut UringRegistrationStats,
    pub(crate) registration_mode: BufferRegistrationMode,
    pub(crate) chunk_register_failure_at: &'r mut [Option<Instant>],
    pub(crate) provided: Option<ProvidedBufSqeInfo>,
}

pub(crate) struct UringBufferRegistry<'a> {
    registered_chunks: BitSet,
    registrar: &'a (dyn BufferRegistrar + 'a),
    stats: UringRegistrationStats,
    mode: BufferRegistrationMode,
    chunk_register_failure_at: Box<[Option<Instant>]>,
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
            chunk_register_failure_at: vec![None; MAX_CHUNKS].into_boxed_slice(),
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
            chunk_register_failure_at: &mut self.chunk_register_failure_at,
            provided,
        }
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

    pub(crate) fn release_provided_buffers(&mut self, submitter: &io_uring::Submitter<'_>) {
        if let Some(group) = self.provided_buffers.take() {
            group.release(submitter);
        }
    }
}
