pub(crate) use self::file_registration::{
    FileRegistrationBatch, FileRegistrationRequest, FileUpdateTxn,
};
use self::files::FixedFileOwner;
pub(crate) use self::files::RegistrationPorts;

use crate::{
    config::{
        BufferRegistrationMode, FileTableExhaustion, IoFd, ProvidedBufConfig, RawHandle,
        RawHandleKind, UringRawHandle,
    },
    diagnostics::UringCompletionDiagnostics,
    driver::env::{ProvidedBufSqeInfo, SubmitEnvironmentFactory, SubmitResourceView},
    error::{UringError, UringResult},
    op::{CheckedSlotView, SlotView, UringOpRegistry, UringOpRegistryExt},
};
use diagweave::prelude::*;
use veloq_buf::{AnyBufPool, BufferRegistrar, heap::ChunkId};
use veloq_driver_core::driver::{BufferRegistrationStatus, OpToken, RegisterFd};
use veloq_io_uring::{KernelCapabilities, ResourceRegistration, Submitter};
use veloq_std::{time::Duration, vec::Vec};

#[cfg(feature = "test-hooks")]
use veloq_driver_core::driver::test_hooks::RegisterFilesUpdateOutcome;

mod buffer;
mod file_registration;
mod file_table;
mod files;
mod provided_buf;
mod update;

pub(crate) use buffer::{
    ChunkRegistrationRecord, ChunkRegistrationRequest, ChunkRegistrationState, FixedBufferOwner,
    FixedBufferSubmitPort,
};
pub(crate) use file_table::{
    FileTable, FileTablePoisonContext, OwnedFdOwnershipTicket, OwnedLocation, RegisteredFileEntry,
    SqeFd,
};
pub use provided_buf::ProvidedBufferSnapshot;
pub(crate) use provided_buf::{
    PROVIDED_BUF_GROUP_ID, ProvidedBufGroup, ProvidedBufLease, ProvidedBufLeaseAction,
    ProvidedBufLeaseError, ProvidedBufLeasePhase, ProvidedBufPort, ProvidedBufUnregisterResult,
    RingLifetimeOwner, RingLifetimeToken,
};
pub(crate) use update::{KernelUpdateOutcome, UpdateEvidence, UringRegistrationStats};

pub(crate) struct RegistrationEngine<'a> {
    fixed_buffers: FixedBufferOwner<'a>,
    provided_buffers: ProvidedBufferOwner,
    files: FixedFileOwner,
}

/// Owns the optional provided-buffer ring independently from fixed-buffer bookkeeping.
///
/// The owner is the only place that can replace or release the group. Submission receives an
/// immutable SQE descriptor and completion receives a command port, so neither path can move the
/// group out of the owner accidentally.
/// owner 独占可选 provided-buffer ring；提交路径只获得只读 SQE 描述符，完成路径通过命令端口访问，二者都不能移走 group。
pub(crate) struct ProvidedBufferOwner {
    config: Option<ProvidedBufConfig>,
    group: Option<ProvidedBufGroup>,
    failure_errno: Option<i32>,
}

impl ProvidedBufferOwner {
    fn new(config: Option<ProvidedBufConfig>) -> Self {
        Self {
            config,
            group: None,
            failure_errno: None,
        }
    }

    fn sqe_info(&self) -> Option<ProvidedBufSqeInfo> {
        self.group.as_ref().and_then(ProvidedBufGroup::sqe_info)
    }

    fn has_group(&self) -> bool {
        self.group.is_some()
    }

    fn has_selected_bids(&self) -> bool {
        self.group
            .as_ref()
            .is_some_and(ProvidedBufGroup::has_selected_bids)
    }

    fn snapshot(&self) -> Option<ProvidedBufferSnapshot> {
        self.group.as_ref().map(ProvidedBufGroup::stats)
    }

    fn command_port(&mut self) -> Option<ProvidedBufPort<'_>> {
        self.group.as_mut().map(ProvidedBufPort::new)
    }

    fn attach(
        &mut self,
        submitter: &Submitter<'_>,
        pool: AnyBufPool,
        ring_lifetime_token: RingLifetimeToken,
    ) -> UringResult<bool> {
        let Some(config) = self.config else {
            return Ok(false);
        };
        if self.group.is_some() {
            return Ok(true);
        }

        match ProvidedBufGroup::new(submitter, config, pool, ring_lifetime_token) {
            Ok(group) => {
                self.group = Some(group);
                self.failure_errno = None;
                Ok(true)
            }
            Err(failure) => {
                let (report, group) = (*failure).into_parts();
                if let Some(group) = group
                    && group.is_kernel_registered()
                {
                    self.group = Some(group);
                }
                self.failure_errno = report
                    .error_code()
                    .and_then(|code| i32::try_from(code).ok());
                tracing::debug!(
                    report = ?report,
                    "provided buffer ring unavailable; continuing without it"
                );
                Ok(false)
            }
        }
    }

    fn release(&mut self, submitter: &Submitter<'_>) -> UringResult<()> {
        self.release_with(|group| group.try_unregister(submitter))
    }

    fn release_with<F>(&mut self, unregister: F) -> UringResult<()>
    where
        F: FnOnce(ProvidedBufGroup) -> ProvidedBufUnregisterResult,
    {
        let Some(group) = self.group.take() else {
            return Ok(());
        };
        match unregister(group) {
            Ok(()) => Ok(()),
            Err(failure) => {
                let (report, group) = (*failure).into_parts();
                self.group = Some(group);
                Err(report)
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct OperationProbe {
    active_operations: usize,
    armed_provided_multishot: bool,
    pending_completions: bool,
}

impl OperationProbe {
    pub(crate) fn from_registry(ops: &mut UringOpRegistry, pending_completions: bool) -> Self {
        let active_operations = ops.active_count();
        let active_tokens: Vec<OpToken> = ops.active_tokens().collect();
        let armed_provided_multishot = active_tokens.into_iter().any(|token| {
            let Ok(view) = ops.checked_slot_view(token) else {
                return true;
            };
            match view {
                CheckedSlotView::Valid(SlotView::Reserved(mut slot)) => slot
                    .with_access_mut(|access| access.operation().get_ref().is_provided_multishot())
                    .unwrap_or(true),
                CheckedSlotView::Valid(SlotView::InFlightWaiting(mut slot)) => slot
                    .with_access_mut(|access| access.operation().get_ref().is_provided_multishot())
                    .unwrap_or(true),
                CheckedSlotView::Valid(SlotView::InFlightOrphaned(mut slot)) => slot
                    .with_access_mut(|access| access.operation().get_ref().is_provided_multishot())
                    .unwrap_or(true),
                CheckedSlotView::Empty(_)
                | CheckedSlotView::Missing { .. }
                | CheckedSlotView::Stale(_) => true,
            }
        });
        Self {
            active_operations,
            armed_provided_multishot,
            pending_completions,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ProvidedBufQuiesce {
    active_operations: usize,
    armed_provided_multishot: bool,
    pending_completions: bool,
    selected_bids: bool,
}

impl ProvidedBufQuiesce {
    #[inline]
    pub(crate) const fn is_quiescent(self) -> bool {
        self.active_operations == 0
            && !self.armed_provided_multishot
            && !self.pending_completions
            && !self.selected_bids
    }

    pub(crate) const fn active_operations(self) -> usize {
        self.active_operations
    }

    pub(crate) const fn armed_provided_multishot(self) -> bool {
        self.armed_provided_multishot
    }

    pub(crate) const fn pending_completions(self) -> bool {
        self.pending_completions
    }

    pub(crate) const fn selected_bids(self) -> bool {
        self.selected_bids
    }
}

impl<'a> RegistrationEngine<'a> {
    pub(crate) fn new(
        registration_mode: BufferRegistrationMode,
        provided_buffers: Option<ProvidedBufConfig>,
        registrar: &'a (dyn BufferRegistrar + 'a),
        file_table_capacity: u32,
        file_table_exhaustion: FileTableExhaustion,
    ) -> Self {
        Self {
            fixed_buffers: FixedBufferOwner::new(registration_mode, registrar),
            provided_buffers: ProvidedBufferOwner::new(provided_buffers),
            files: FixedFileOwner::new(file_table_capacity, file_table_exhaustion),
        }
    }

    pub(crate) fn build_submit_view<'d>(
        &'d mut self,
        submitter: Submitter<'d>,
        diagnostics: &'d UringCompletionDiagnostics,
    ) -> SubmitResourceView<'d, 'a> {
        SubmitEnvironmentFactory::resource_view(
            submitter,
            self.files.table(),
            diagnostics,
            self.fixed_buffers
                .split_for_submit(self.provided_buffers.sqe_info()),
        )
    }

    pub(crate) fn quiescence(&self, probe: OperationProbe) -> ProvidedBufQuiesce {
        let state = ProvidedBufQuiesce {
            active_operations: probe.active_operations,
            armed_provided_multishot: probe.armed_provided_multishot,
            pending_completions: probe.pending_completions,
            selected_bids: self.provided_buffers.has_selected_bids(),
        };
        if !state.is_quiescent() {
            tracing::debug!(
                active_operations = state.active_operations(),
                armed_provided_multishot = state.armed_provided_multishot(),
                pending_completions = state.pending_completions(),
                selected_bids = state.selected_bids(),
                "provided buffer ring is not quiescent"
            );
        }
        state
    }

    #[inline]
    pub(crate) fn has_provided_buffers(&self) -> bool {
        self.provided_buffers.has_group()
    }

    #[inline]
    pub(crate) fn provided_buffer_snapshot(&self) -> Option<ProvidedBufferSnapshot> {
        self.provided_buffers.snapshot()
    }

    pub(crate) fn release_provided_buffers(
        &mut self,
        submitter: &Submitter<'_>,
    ) -> UringResult<()> {
        self.provided_buffers.release(submitter)
    }

    #[inline]
    pub(crate) fn set_fixed_buffers_available(&mut self, registration: ResourceRegistration) {
        self.fixed_buffers.set_fixed_buffers_available(registration);
    }

    #[cfg(feature = "test-hooks")]
    #[inline]
    pub(crate) fn fixed_buffers_available(&self) -> bool {
        self.fixed_buffers.fixed_buffers_available()
    }

    #[inline]
    pub(crate) fn set_fixed_buffers_unavailable(&mut self, errno: Option<i32>) {
        self.fixed_buffers.set_fixed_buffers_unavailable(errno);
    }

    #[inline]
    pub(crate) fn attach_buffer_pool(
        &mut self,
        submitter: &Submitter<'_>,
        pool: AnyBufPool,
        ring_lifetime_token: RingLifetimeToken,
    ) -> UringResult<bool> {
        self.provided_buffers
            .attach(submitter, pool, ring_lifetime_token)
    }

    #[inline]
    pub(crate) fn provided_buffer_port(&mut self) -> Option<ProvidedBufPort<'_>> {
        self.provided_buffers.command_port()
    }

    #[inline]
    pub(crate) fn resolve_file(
        &self,
        fd: IoFd,
        expected_kind: Option<RawHandleKind>,
        scope: &'static str,
    ) -> UringResult<SqeFd> {
        self.files.table().resolve(fd, expected_kind, scope)
    }

    #[inline]
    pub(crate) fn file_table_is_poisoned(&self) -> bool {
        self.files.is_poisoned()
    }

    #[inline]
    pub(crate) fn file_table_poisoned_report(
        &self,
        scope: &'static str,
        fd: Option<IoFd>,
    ) -> Report<UringError> {
        self.files.poisoned_report(scope, fd)
    }

    pub(crate) fn unregister_fixed_fd(
        &mut self,
        ports: &RegistrationPorts<'_, '_>,
        fd: IoFd,
    ) -> UringResult<()> {
        self.files.unregister_fixed_fd(ports, fd)
    }

    pub(crate) fn unregister_close_owned_fd(
        &mut self,
        ports: &RegistrationPorts<'_, '_>,
        fd: IoFd,
    ) -> UringResult<()> {
        self.files.unregister_close_owned_fd(ports, fd)
    }

    pub(crate) fn replace_registered_fixed_fd(
        &mut self,
        ports: &RegistrationPorts<'_, '_>,
        fixed_fd: IoFd,
        raw: RawHandle,
    ) -> UringResult<()> {
        self.files.replace_registered_fixed_fd(ports, fixed_fd, raw)
    }

    pub(crate) fn ensure_file_table_initialized(
        &mut self,
        submitter: &Submitter<'_>,
        kernel_capabilities: &mut KernelCapabilities,
    ) -> UringResult<()> {
        self.files
            .ensure_file_table_initialized(submitter, kernel_capabilities)
    }

    pub(crate) fn register_buffer_internal(
        &mut self,
        submitter: &Submitter<'_>,
        diagnostics: &UringCompletionDiagnostics,
        id: ChunkId,
        ptr: *const u8,
        len: usize,
    ) -> UringResult<BufferRegistrationStatus> {
        self.fixed_buffers
            .register_buffer_internal(submitter, diagnostics, id, ptr, len)
    }

    pub(crate) fn register_files_internal<'h>(
        &mut self,
        submitter: &Submitter<'_>,
        diagnostics: &UringCompletionDiagnostics,
        kernel_capabilities: &mut KernelCapabilities,
        files: Vec<RegisterFd<'h, UringRawHandle>>,
    ) -> UringResult<Vec<IoFd>> {
        let request = FileRegistrationRequest {
            submitter,
            diagnostics,
            kernel_capabilities,
            files,
            stats: self.fixed_buffers.stats_mut(),
        };
        self.files.register_files_internal(request)
    }

    #[cfg(feature = "test-hooks")]
    pub(crate) fn registration_stats(&self) -> &UringRegistrationStats {
        self.fixed_buffers.stats()
    }

    #[cfg(feature = "test-hooks")]
    pub(crate) fn is_chunk_registered(&self, chunk_id: ChunkId) -> bool {
        self.fixed_buffers.is_chunk_registered(chunk_id)
    }

    #[cfg(feature = "test-hooks")]
    pub(crate) fn inject_register_buffers_update_failure(&mut self, errno: i32) {
        self.fixed_buffers
            .inject_register_buffers_update_failure(errno);
    }

    #[cfg(feature = "test-hooks")]
    pub(crate) fn inject_register_buffers_update_unknown(&mut self, errno: i32) {
        self.fixed_buffers
            .inject_register_buffers_update_unknown(errno);
    }

    #[cfg(feature = "test-hooks")]
    pub(crate) fn inject_register_buffers_update_sequence(&mut self, outcomes: &[Option<i32>]) {
        self.fixed_buffers
            .inject_register_buffers_update_sequence(outcomes);
    }

    #[cfg(feature = "test-hooks")]
    pub(crate) fn inject_bitset_set_failure(&mut self) {
        self.fixed_buffers.inject_bitset_set_failure();
    }

    #[cfg(feature = "test-hooks")]
    pub(crate) fn inject_register_files_update_failure(&mut self, errno: i32) {
        self.files.inject_update_failure(errno);
    }

    #[cfg(feature = "test-hooks")]
    pub(crate) fn inject_register_files_update_sequence(
        &mut self,
        outcomes: &[RegisterFilesUpdateOutcome],
    ) {
        self.files.inject_update_sequence(outcomes);
    }

    #[cfg(feature = "test-hooks")]
    pub(crate) fn file_table_poisoned(&self) -> bool {
        self.files.is_poisoned()
    }

    #[cfg(feature = "test-hooks")]
    pub(crate) fn register_files_update_outcomes_pending(&self) -> usize {
        self.files.pending_update_outcomes()
    }
}

pub(crate) const MAX_CHUNKS: usize = 1024;
pub(crate) const REGISTER_FAILURE_RETRY_COOLDOWN: Duration = Duration::from_millis(250);

#[cfg(test)]
mod tests {
    use super::{FileRegistrationBatch, KernelUpdateOutcome, UpdateEvidence};
    use veloq_std::{vec, vec::Vec};

    /// Walks `slots` the way `install_claimed_files` does, collecting one run per syscall.
    /// 按 `install_claimed_files` 的方式遍历 `slots`，为每次系统调用收集一个连续区间。
    fn runs(slots: &[u32]) -> Vec<&[u32]> {
        let mut runs = Vec::new();
        let mut cursor = 0;
        while cursor < slots.len() {
            let end = FileRegistrationBatch::consecutive_run_end(slots, cursor);
            runs.push(&slots[cursor..end]);
            cursor = end;
        }
        runs
    }

    #[test]
    fn a_fresh_file_table_registers_the_whole_batch_in_one_call() {
        assert_eq!(runs(&[0, 1, 2, 3]), vec![&[0, 1, 2, 3][..]]);
    }

    #[test]
    fn holes_in_the_free_list_split_the_batch_into_runs() {
        assert_eq!(
            runs(&[1, 2, 5, 9, 10]),
            vec![&[1, 2][..], &[5][..], &[9, 10][..]]
        );
    }

    #[test]
    fn a_single_slot_is_one_run() {
        assert_eq!(runs(&[7]), vec![&[7][..]]);
    }

    #[test]
    fn update_evidence_preserves_partial_kernel_application() {
        let evidence = UpdateEvidence::new(3, KernelUpdateOutcome::Applied(1));

        assert_eq!(evidence.updated(), Some(1));
        assert_eq!(evidence.requested(), 3);
    }

    #[cfg(feature = "test-hooks")]
    #[test]
    fn rejected_and_unknown_updates_do_not_claim_applied_entries() {
        for outcome in [
            KernelUpdateOutcome::Rejected(libc::EIO),
            KernelUpdateOutcome::Unknown(Some(libc::EIO), "test evidence"),
        ] {
            let evidence = UpdateEvidence::new(1, outcome);
            assert_eq!(evidence.updated(), None);
        }
    }
}
