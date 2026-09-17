use crate::{
    config::{IoFd, RawHandle, UringRawHandle},
    driver::UringDriver,
    error::{UringError, UringResult},
};
use diagweave::prelude::*;
use tracing::error;
use veloq_buf::heap::ChunkId;
use veloq_driver_core::driver::{BufferRegistrationStatus, RegisterFd};
use veloq_std::{
    collections::HashMap,
    format, io,
    mem::{self, ManuallyDrop},
    string::ToString,
    time::Duration,
    vec,
    vec::Vec,
};

#[cfg(feature = "test-hooks")]
use veloq_driver_core::driver::test_hooks::RegisterFilesUpdateOutcome;

pub(crate) mod buffer;
pub(crate) mod file_table;
pub(crate) mod provided_buf;

pub(crate) use buffer::{
    BufferRegistrationQuarantine, ChunkRegistrationRecord, ChunkRegistrationState,
    UringBufferRegistry,
};
pub(crate) use file_table::{
    FileTable, FileTablePoisonContext, OwnedLocation, RegisteredFileEntry, SqeFd,
};
pub use provided_buf::ProvidedBufStats;
pub(crate) use provided_buf::{PROVIDED_BUF_GROUP_ID, ProvidedBufGroup, RingLifetimeOwner};

#[cfg(test)]
pub(crate) use provided_buf::test_group;

pub(crate) const MAX_CHUNKS: usize = 1024;
pub(crate) const REGISTER_FAILURE_RETRY_COOLDOWN: Duration = Duration::from_millis(250);
/// Upper bound on [`UringConfig::file_table_capacity`](crate::config::UringConfig).
///
/// The kernel's own limit is smaller still (`IORING_MAX_FIXED_FILES`), but it rejects an
/// oversized table only *after* the sparse `-1` vector has been built — and that vector is
/// four bytes per entry, so an unchecked `u32` would ask for gigabytes before the syscall got
/// a chance to say no.
const MAX_FILE_TABLE_CAPACITY: usize = 1 << 20;

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct UringRegistrationStats {
    pub(crate) chunk_register_attempts: u64,
    pub(crate) chunk_register_success: u64,
    pub(crate) chunk_register_failures: u64,
    pub(crate) chunk_register_skipped_recent_failure: u64,
    pub(crate) submission_missing_chunk_info: u64,
    pub(crate) raw_buffer_fallbacks: u64,
    /// Descriptors handed out without a kernel table entry because the table was full.
    pub(crate) file_table_fallback_registrations: u64,
}

/// The kernel-side fact established by one resource update syscall.
///
/// A short successful update is deliberately kept as `Applied(n)`: callers need to know the
/// exact prefix that may be visible in the kernel, instead of treating it as an ordinary error.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum KernelUpdateOutcome {
    Applied(usize),
    #[cfg(feature = "test-hooks")]
    Rejected(i32),
    Unknown(Option<i32>, &'static str),
}

/// Evidence recorded independently from the diagnostic report produced for an update.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct UpdateEvidence {
    pub(crate) requested: usize,
    pub(crate) outcome: KernelUpdateOutcome,
}

impl UpdateEvidence {
    #[inline]
    fn updated(self) -> Option<usize> {
        match self.outcome {
            KernelUpdateOutcome::Applied(updated) => Some(updated),
            #[cfg(feature = "test-hooks")]
            KernelUpdateOutcome::Rejected(_) => None,
            KernelUpdateOutcome::Unknown(_, _) => None,
        }
    }
}

struct FileTableUpdateReport {
    report: Report<UringError>,
    cleanup_errno: Option<i32>,
    scope: &'static str,
    start_index: u32,
    requested_files: usize,
}

struct FileTableUpdateFailure {
    report: FileTableUpdateReport,
    evidence: UpdateEvidence,
}

impl FileTableUpdateFailure {
    fn poison_context(&self, failed_index: Option<u32>) -> FileTablePoisonContext {
        FileTablePoisonContext {
            scope: self.report.scope,
            failed_index,
            start_index: self.report.start_index,
            requested_files: self.report.requested_files,
            updated_files: self.evidence.updated(),
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct OwnedInputInfo {
    first_position: usize,
    count: usize,
    existing: Option<OwnedLocation>,
}

#[derive(Debug, Clone, Copy)]
enum OwnedFdConflictSource {
    Existing(OwnedLocation),
    CurrentBatch(usize),
}

#[derive(Debug, Clone, Copy)]
struct OwnedFdConflict {
    raw_fd: i32,
    input_position: usize,
    source: OwnedFdConflictSource,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FileUpdateDirection {
    Register,
    Clear,
}

/// One kernel file-table update, kept separate from the resource-state commit.
///
/// The transaction owns only the syscall input. The [`FileRegistrationBatch`] owns the entries
/// and decides whether they can be committed after the kernel outcome is known.
struct FileUpdateTxn {
    start: u32,
    fds: Vec<i32>,
    direction: FileUpdateDirection,
    scope: &'static str,
}

impl FileUpdateTxn {
    fn register(start: u32, fds: Vec<i32>, scope: &'static str) -> Self {
        Self {
            start,
            fds,
            direction: FileUpdateDirection::Register,
            scope,
        }
    }

    fn clear(start: u32, count: usize, scope: &'static str) -> Self {
        Self {
            start,
            fds: vec![-1; count],
            direction: FileUpdateDirection::Clear,
            scope,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FileLeaseState {
    Reserved,
    Committed,
    Retained,
    Released,
}

struct FileLease {
    index: u32,
    entry: Option<RegisteredFileEntry>,
    state: FileLeaseState,
}

/// Owns one complete registration batch until it is committed or explicitly aborted.
///
/// Fixed entries remain in this lease until a full register update is applied. This prevents a
/// short or unknown update from being represented as a live userspace entry by accident. Direct
/// fallback inputs stay here until all fixed runs have committed; adopted direct descriptors are
/// tracked so an internal adoption failure can abort the whole batch.
struct FileRegistrationBatch<'h> {
    fixed: Vec<FileLease>,
    direct_files: Vec<RegisterFd<'h, UringRawHandle>>,
    direct_descriptors: Vec<IoFd>,
}

impl<'h> FileRegistrationBatch<'h> {
    fn new(claimed: Vec<u32>, files: Vec<RegisterFd<'h, UringRawHandle>>) -> Self {
        let mut files = files.into_iter();
        let fixed = claimed
            .into_iter()
            .map(|index| FileLease {
                index,
                entry: Some(Self::entry_from_file(
                    files.next().expect("one input per claimed slot"),
                )),
                state: FileLeaseState::Reserved,
            })
            .collect();
        let direct_files = files.collect();
        Self {
            fixed,
            direct_files,
            direct_descriptors: Vec::new(),
        }
    }

    fn entry_from_file(file: RegisterFd<'h, UringRawHandle>) -> RegisteredFileEntry {
        match file {
            RegisterFd::Borrowed(b) => RegisteredFileEntry::BorrowedFd {
                fd: b.raw().as_fd(),
                kind: b.kind(),
            },
            RegisterFd::Owned(o) => RegisteredFileEntry::OwnedHandle(o),
        }
    }

    fn fixed_len(&self) -> usize {
        self.fixed.len()
    }

    fn fixed_indices(&self) -> Vec<u32> {
        self.fixed.iter().map(|lease| lease.index).collect()
    }

    fn registration_txn(&self, start: usize, end: usize) -> FileUpdateTxn {
        let fds = self.fixed[start..end]
            .iter()
            .map(|lease| {
                lease
                    .entry
                    .as_ref()
                    .expect("uncommitted file lease owns its entry")
                    .fd()
            })
            .collect::<Vec<_>>();
        FileUpdateTxn::register(
            self.fixed[start].index,
            fds,
            "driver.register_files_internal.register_files_update",
        )
    }

    fn commit_range(&mut self, table: &mut FileTable, start: usize, end: usize) {
        for lease in &mut self.fixed[start..end] {
            debug_assert_eq!(lease.state, FileLeaseState::Reserved);
            table.install_entry(
                lease.index,
                lease
                    .entry
                    .take()
                    .expect("committing file lease with no entry"),
            );
            lease.state = FileLeaseState::Committed;
        }
    }

    fn retain_range(&mut self, table: &mut FileTable, start: usize, end: usize) {
        for lease in &mut self.fixed[start..end] {
            debug_assert_eq!(lease.state, FileLeaseState::Reserved);
            table.install_entry(
                lease.index,
                lease
                    .entry
                    .take()
                    .expect("retaining file lease with no entry"),
            );
            lease.state = FileLeaseState::Retained;
        }
    }

    fn release_range(&mut self, table: &mut FileTable, start: usize, end: usize) {
        for lease in &mut self.fixed[start..end] {
            debug_assert_eq!(lease.state, FileLeaseState::Reserved);
            table.release(lease.index);
            lease.state = FileLeaseState::Released;
        }
    }

    fn release_reserved(&mut self, table: &mut FileTable) {
        for lease in &mut self.fixed {
            if lease.state == FileLeaseState::Reserved {
                table.release(lease.index);
                lease.state = FileLeaseState::Released;
            }
        }
    }

    fn committed_indices(&self) -> Vec<u32> {
        self.fixed
            .iter()
            .filter(|lease| lease.state == FileLeaseState::Committed)
            .map(|lease| lease.index)
            .collect()
    }

    fn mark_rolled_back(&mut self, retained: &[u32]) {
        for lease in &mut self.fixed {
            if lease.state == FileLeaseState::Committed && !retained.contains(&lease.index) {
                lease.state = FileLeaseState::Released;
            }
        }
    }

    fn retained_slot_count(&self) -> usize {
        self.fixed
            .iter()
            .filter(|lease| {
                matches!(
                    lease.state,
                    FileLeaseState::Committed | FileLeaseState::Retained
                )
            })
            .count()
    }

    fn fixed_descriptors(&self, table: &FileTable) -> Vec<IoFd> {
        self.fixed
            .iter()
            .map(|lease| {
                debug_assert_eq!(lease.state, FileLeaseState::Committed);
                table
                    .descriptor(lease.index)
                    .expect("committed file lease has a live descriptor")
            })
            .collect()
    }

    fn take_direct_files(&mut self) -> Vec<RegisterFd<'h, UringRawHandle>> {
        mem::take(&mut self.direct_files)
    }

    fn push_direct_descriptor(&mut self, descriptor: IoFd) {
        self.direct_descriptors.push(descriptor);
    }

    fn descriptors(&self, table: &FileTable) -> Vec<IoFd> {
        self.fixed_descriptors(table)
            .into_iter()
            .chain(self.direct_descriptors.iter().copied())
            .collect()
    }
}

fn owned_input_raw(file: &RegisterFd<'_, UringRawHandle>) -> Option<UringRawHandle> {
    match file {
        RegisterFd::Borrowed(_) => None,
        RegisterFd::Owned(handle) => Some(handle.raw()),
    }
}

fn preflight_owned_files(
    table: &FileTable,
    files: &[RegisterFd<'_, UringRawHandle>],
) -> (HashMap<i32, OwnedInputInfo>, Option<OwnedFdConflict>) {
    let mut inputs: HashMap<i32, OwnedInputInfo> = HashMap::default();
    let mut conflict = None;

    for (input_position, file) in files.iter().enumerate() {
        let Some(raw) = owned_input_raw(file) else {
            continue;
        };
        let raw_fd = raw.as_fd();
        if let Some(info) = inputs.get_mut(&raw_fd) {
            if conflict.is_none() {
                conflict = Some(OwnedFdConflict {
                    raw_fd,
                    input_position,
                    source: info.existing.map_or(
                        OwnedFdConflictSource::CurrentBatch(info.first_position),
                        OwnedFdConflictSource::Existing,
                    ),
                });
            }
            info.count += 1;
        } else {
            let existing = table.owned_location(raw);
            if conflict.is_none()
                && let Some(location) = existing
            {
                conflict = Some(OwnedFdConflict {
                    raw_fd,
                    input_position,
                    source: OwnedFdConflictSource::Existing(location),
                });
            }
            inputs.insert(
                raw_fd,
                OwnedInputInfo {
                    first_position: input_position,
                    count: 1,
                    existing,
                },
            );
        }
    }

    (inputs, conflict)
}

fn duplicate_owned_fd_report(conflict: OwnedFdConflict) -> Report<UringError> {
    let mut report = UringError::DuplicateOwnedFd
        .report(
            "driver.register_files_internal.preflight",
            "an owned file descriptor is already registered",
        )
        .with_ctx("raw_fd", conflict.raw_fd)
        .with_ctx("input_position", conflict.input_position);
    match conflict.source {
        OwnedFdConflictSource::Existing(OwnedLocation::Fixed(index)) => {
            report = report
                .with_ctx("conflict_source", "existing_registration")
                .with_ctx("existing_location", "fixed")
                .with_ctx("existing_file_index", index);
        }
        OwnedFdConflictSource::Existing(OwnedLocation::Direct(owner)) => {
            report = report
                .with_ctx("conflict_source", "existing_registration")
                .with_ctx("existing_location", "direct")
                .with_ctx("existing_direct_owner", format!("{owner:?}"));
        }
        OwnedFdConflictSource::CurrentBatch(previous_position) => {
            report = report
                .with_ctx("conflict_source", "current_batch")
                .with_ctx("previous_input_position", previous_position);
        }
    }
    report
}

fn cleanup_rejected_owned_inputs<'h>(
    files: Vec<RegisterFd<'h, UringRawHandle>>,
    inputs: &HashMap<i32, OwnedInputInfo>,
) {
    let mut dropped_batch_duplicates = HashMap::default();

    for file in files {
        match file {
            RegisterFd::Borrowed(_) => {}
            RegisterFd::Owned(handle) => {
                let raw = handle.raw();
                let Some(info) = inputs.get(&raw.as_fd()) else {
                    drop(handle);
                    continue;
                };

                if info.existing.is_some() {
                    // The existing registration remains the sole actual owner of this raw fd.
                    mem::forget(handle);
                } else if info.count > 1 {
                    // This malformed batch contains several owners for one raw fd. Close one
                    // wrapper and forget the rest so the number is closed at most once.
                    if dropped_batch_duplicates.insert(raw.as_fd(), ()).is_none() {
                        drop(handle);
                    } else {
                        mem::forget(handle);
                    }
                } else {
                    drop(handle);
                }
            }
        }
    }
}

struct FileTableRollbackFailure {
    failed_index: u32,
    failure: FileTableUpdateFailure,
    remaining_indices: Vec<u32>,
}

impl<'a> UringDriver<'a> {
    #[inline]
    pub(crate) fn register_buffer_internal(
        &mut self,
        id: ChunkId,
        ptr: *const u8,
        len: usize,
    ) -> UringResult<BufferRegistrationStatus> {
        self.submit_env().register_buffer_backend(id, ptr, len)
    }

    /// Runs one `register_files_update` call and retains the evidence needed by rollback.
    fn register_files_update(
        &mut self,
        start: u32,
        files: &[i32],
    ) -> (io::Result<usize>, KernelUpdateOutcome) {
        #[cfg(feature = "test-hooks")]
        if let Some(outcome) = self.register_files_update_outcomes.pop_front() {
            return match outcome {
                RegisterFilesUpdateOutcome::Actual => {
                    let result = self.ring.submitter().register_files_update(start, files);
                    let evidence = match &result {
                        Ok(updated) => KernelUpdateOutcome::Applied(*updated),
                        Err(error) => KernelUpdateOutcome::Unknown(
                            error.raw_os_error(),
                            "kernel file table update returned an error",
                        ),
                    };
                    (result, evidence)
                }
                RegisterFilesUpdateOutcome::Error(errno) => (
                    Err(io::Error::from_raw_os_error(errno)),
                    KernelUpdateOutcome::Rejected(errno),
                ),
                RegisterFilesUpdateOutcome::Updated(updated) => {
                    (Ok(updated), KernelUpdateOutcome::Applied(updated))
                }
            };
        }

        let result = self.ring.submitter().register_files_update(start, files);
        let evidence = match &result {
            Ok(updated) => KernelUpdateOutcome::Applied(*updated),
            Err(error) => KernelUpdateOutcome::Unknown(
                error.raw_os_error(),
                "kernel file table update returned an error",
            ),
        };
        (result, evidence)
    }

    fn record_file_update_outcome(
        &self,
        evidence: UpdateEvidence,
        direction: FileUpdateDirection,
        scope: &'static str,
        start: u32,
    ) {
        match evidence.outcome {
            KernelUpdateOutcome::Applied(_) => self
                .completion_diagnostics
                .backend()
                .inc_file_table_update_applied(),
            #[cfg(feature = "test-hooks")]
            KernelUpdateOutcome::Rejected(_) => self
                .completion_diagnostics
                .backend()
                .inc_file_table_update_rejected(),
            KernelUpdateOutcome::Unknown(_, _) => self
                .completion_diagnostics
                .backend()
                .inc_file_table_update_unknown(),
        }
        tracing::debug!(
            scope,
            direction = ?direction,
            start_index = start,
            requested_files = evidence.requested,
            updated_files = ?evidence.updated(),
            evidence = ?evidence.outcome,
            "recorded kernel file table update evidence"
        );
    }

    fn update_kernel_file_entries(
        &mut self,
        txn: &FileUpdateTxn,
    ) -> Result<(), FileTableUpdateFailure> {
        let requested_files = txn.fds.len();
        let (result, outcome) = self.register_files_update(txn.start, &txn.fds);
        let evidence = UpdateEvidence {
            requested: requested_files,
            outcome,
        };
        self.record_file_update_outcome(evidence, txn.direction, txn.scope, txn.start);
        let updated_files = result.map_err(|error| {
            let cleanup_errno = error.raw_os_error();
            let report = UringError::Registration
                .io_report(txn.scope, error)
                .with_ctx("start_index", txn.start)
                .with_ctx("requested_files", requested_files);
            FileTableUpdateFailure {
                report: FileTableUpdateReport {
                    cleanup_errno,
                    report,
                    scope: txn.scope,
                    start_index: txn.start,
                    requested_files,
                },
                evidence,
            }
        })?;
        if updated_files != txn.fds.len() {
            return Err(FileTableUpdateFailure {
                report: FileTableUpdateReport {
                    report: UringError::Registration
                        .to_report()
                        .push_ctx("scope", txn.scope)
                        .with_ctx("start_index", txn.start)
                        .with_ctx("requested_files", txn.fds.len())
                        .with_ctx("updated_files", updated_files)
                        .attach_note(
                            "io_uring updated fewer registered file entries than requested",
                        ),
                    cleanup_errno: None,
                    scope: txn.scope,
                    start_index: txn.start,
                    requested_files,
                },
                evidence,
            });
        }
        Ok(())
    }

    /// Clears the kernel table entry for `idx`.
    ///
    /// Every slot in the table now mirrors a kernel entry one-to-one — descriptors that did
    /// not fit are handed out as [`IoFd::Direct`] and never reach this path.
    fn clear_kernel_file_entry(
        &mut self,
        idx: u32,
        scope: &'static str,
    ) -> Result<(), FileTableUpdateFailure> {
        let txn = FileUpdateTxn::clear(idx, 1, scope);
        self.update_kernel_file_entries(&txn)
    }

    fn clear_kernel_file_entries(
        &mut self,
        start: u32,
        count: usize,
        scope: &'static str,
    ) -> Result<(), FileTableUpdateFailure> {
        let txn = FileUpdateTxn::clear(start, count, scope);
        self.update_kernel_file_entries(&txn)
    }

    fn poison_file_table(
        &mut self,
        scope: &'static str,
        context: FileTablePoisonContext,
        primary: Report<UringError>,
        secondary: Option<Report<UringError>>,
        rollback_failure: bool,
        remaining_slots: usize,
    ) -> Report<UringError> {
        let transitioned = self.file_table.poison(context);
        if rollback_failure {
            self.completion_diagnostics
                .backend()
                .inc_file_table_rollback_failure();
        }
        if transitioned {
            self.completion_diagnostics
                .backend()
                .inc_file_table_poisoning();
        }

        error!(
            scope,
            file_index = ?context.failed_index,
            start_index = context.start_index,
            requested_files = context.requested_files,
            updated_files = ?context.updated_files,
            remaining_slots,
            original_error = ?primary,
            rollback_error = ?secondary,
            "registered file table entered poisoned state"
        );

        let mut report = self
            .file_table
            .poisoned_report(scope, None)
            .with_ctx("rollback_remaining_slots", remaining_slots);
        report = report.with_diag_src_err(primary);
        if let Some(secondary) = secondary {
            report = report.with_diag_src_err(secondary);
        }
        report
    }

    fn unregister_file_slot(
        &mut self,
        idx: u32,
        advance_generation: bool,
        scope: &'static str,
    ) -> Result<(), FileTableUpdateFailure> {
        let Some(entry) = self.file_table.take_entry(idx) else {
            return Ok(());
        };

        if let Err(failure) = self.clear_kernel_file_entry(idx, scope) {
            self.file_table.install_entry(idx, entry);
            return Err(failure);
        }

        self.file_table.release(idx);
        if advance_generation {
            self.file_table.advance_generation(idx);
        }
        Ok(())
    }

    fn rollback_file_slots(
        &mut self,
        registered: &mut Vec<u32>,
    ) -> Result<(), FileTableRollbackFailure> {
        while let Some(idx) = registered.pop() {
            if let Err(failure) =
                self.unregister_file_slot(idx, false, "driver.register_files_internal.rollback")
            {
                return Err(FileTableRollbackFailure {
                    failed_index: idx,
                    failure,
                    // `unregister_file_slot` restores `idx` when its clear update fails, so the
                    // failed slot is retained alongside the entries still waiting for rollback.
                    remaining_indices: {
                        let mut remaining = mem::take(registered);
                        remaining.push(idx);
                        remaining
                    },
                });
            }
        }
        Ok(())
    }

    fn rollback_committed_batch(
        &mut self,
        batch: &mut FileRegistrationBatch<'_>,
    ) -> Result<(), FileTableRollbackFailure> {
        let mut registered = batch.committed_indices();
        let result = self.rollback_file_slots(&mut registered);
        match &result {
            Ok(()) => batch.mark_rolled_back(&[]),
            Err(rollback) => batch.mark_rolled_back(&rollback.remaining_indices),
        }
        result
    }

    fn abort_file_batch(
        &mut self,
        batch: &mut FileRegistrationBatch<'_>,
        primary: Report<UringError>,
        note: &'static str,
    ) -> Report<UringError> {
        batch.release_reserved(&mut self.file_table);
        for descriptor in mem::take(&mut batch.direct_descriptors) {
            if matches!(descriptor, IoFd::OwnedDirect { .. }) {
                drop(self.file_table.release_direct(descriptor));
            }
        }

        match self.rollback_committed_batch(batch) {
            Ok(()) => primary,
            Err(rollback) => {
                let context = rollback.failure.poison_context(Some(rollback.failed_index));
                let remaining_slots = batch.retained_slot_count();
                self.poison_file_table(
                    "driver.register_files_internal.rollback",
                    context,
                    primary,
                    Some(rollback.failure.report.report),
                    true,
                    remaining_slots,
                )
                .attach_note(note)
            }
        }
    }

    fn abort_file_batch_after_update_failure(
        &mut self,
        batch: &mut FileRegistrationBatch<'_>,
        cursor: usize,
        run_end: usize,
        failure: FileTableUpdateFailure,
    ) -> Report<UringError> {
        let failed_index = batch.fixed[cursor].index;
        let context = failure.poison_context(Some(failed_index));
        let evidence = failure.evidence;
        let primary = failure.report.report;
        let run_len = run_end - cursor;

        match evidence.outcome {
            KernelUpdateOutcome::Applied(updated) => {
                let touched = updated.min(run_len);
                if touched == 0 {
                    batch.release_range(&mut self.file_table, cursor, run_end);
                    return self.abort_file_batch(
                        batch,
                        primary,
                        "registration update applied no entries before abort",
                    );
                }

                match self.clear_kernel_file_entries(
                    failed_index,
                    touched,
                    "driver.register_files_internal.abort.clear_prefix",
                ) {
                    Ok(()) => {
                        batch.release_range(&mut self.file_table, cursor, cursor + touched);
                        batch.release_range(&mut self.file_table, cursor + touched, run_end);
                        self.abort_file_batch(
                            batch,
                            primary,
                            "registration update was rolled back after a partial apply",
                        )
                    }
                    Err(clear_failure) => {
                        let clear_evidence = clear_failure.evidence;
                        let cleared = match clear_evidence.outcome {
                            KernelUpdateOutcome::Applied(updated) => updated.min(touched),
                            #[cfg(feature = "test-hooks")]
                            KernelUpdateOutcome::Rejected(_) => 0,
                            KernelUpdateOutcome::Unknown(_, _) => 0,
                        };
                        batch.release_range(&mut self.file_table, cursor, cursor + cleared);
                        batch.retain_range(
                            &mut self.file_table,
                            cursor + cleared,
                            cursor + touched,
                        );
                        batch.release_range(&mut self.file_table, cursor + touched, run_end);
                        batch.release_reserved(&mut self.file_table);

                        self.poison_file_table(
                            "driver.register_files_internal.rollback",
                            context,
                            primary,
                            Some(clear_failure.report.report),
                            true,
                            batch.retained_slot_count(),
                        )
                        .attach_note("partial registration apply could not be fully cleared")
                    }
                }
            }
            #[cfg(feature = "test-hooks")]
            KernelUpdateOutcome::Rejected(_) => {
                batch.release_range(&mut self.file_table, cursor, run_end);
                self.abort_file_batch(
                    batch,
                    primary,
                    "registration update was rejected before kernel state changed",
                )
            }
            KernelUpdateOutcome::Unknown(_, _) => {
                batch.retain_range(&mut self.file_table, cursor, run_end);
                batch.release_range(&mut self.file_table, run_end, batch.fixed_len());
                batch.release_reserved(&mut self.file_table);
                self.poison_file_table(
                    "driver.register_files_internal",
                    context,
                    primary,
                    None,
                    false,
                    batch.retained_slot_count(),
                )
                .attach_note(
                    "registration update result was unknown; all touched file owners were retained",
                )
            }
        }
    }

    pub(crate) fn unregister_fixed_fd(&mut self, fd: IoFd) -> UringResult<()> {
        match fd {
            // Borrowed direct descriptors have no backend ownership to release.
            IoFd::Direct(_) => Ok(()),
            IoFd::OwnedDirect { .. } => {
                drop(self.file_table.release_direct(fd));
                Ok(())
            }
            IoFd::Registered { index, generation } => {
                if self.file_table.is_poisoned() {
                    return Err(self
                        .file_table
                        .poisoned_report("driver.unregister_fixed_fd", Some(fd)));
                }
                if !self.file_table.is_initialized()
                    || !self.file_table.matches_generation(index, generation)
                {
                    return Ok(());
                }
                match self.unregister_file_slot(index, true, "driver.unregister_fixed_fd") {
                    Ok(()) => Ok(()),
                    Err(failure) => {
                        let context = failure.poison_context(Some(index));
                        Err(self.poison_file_table(
                            "driver.unregister_fixed_fd",
                            context,
                            failure.report.report,
                            None,
                            false,
                            0,
                        ))
                    }
                }
            }
        }
    }

    /// Retires the registration behind a `Close` whose descriptor the kernel already closed.
    ///
    /// Either way the owned handle must be forgotten rather than dropped: dropping it would
    /// close a number the kernel may have already handed to someone else.
    pub(crate) fn unregister_close_owned_fd(&mut self, fd: IoFd) -> UringResult<()> {
        let (index, generation) = match fd {
            IoFd::Direct(_) => {
                return Ok(());
            }
            IoFd::OwnedDirect { .. } => {
                if let Some(handle) = self.file_table.release_direct(fd) {
                    let _ = ManuallyDrop::new(handle);
                }
                return Ok(());
            }
            IoFd::Registered { index, generation } => (index, generation),
        };

        if !self.file_table.is_initialized()
            || !self.file_table.matches_generation(index, generation)
        {
            return Ok(());
        }
        let Some(entry) = self.file_table.take_entry(index) else {
            return Ok(());
        };
        // The kernel has already consumed this descriptor. Keep the Rust ownership object
        // unreachable from every drop path regardless of whether clearing the fixed slot works.
        let _entry = ManuallyDrop::new(entry);
        if self.file_table.is_poisoned() {
            self.file_table.advance_generation(index);
            return Err(self
                .file_table
                .poisoned_report("driver.unregister_close_owned_fd", Some(fd))
                .attach_note("closed owned fd was forgotten; poisoned table skipped clear"));
        }
        if let Err(failure) =
            self.clear_kernel_file_entry(index, "driver.unregister_close_owned_fd")
        {
            self.file_table.quarantine(index);
            self.file_table.advance_generation(index);
            self.completion_diagnostics
                .backend()
                .inc_file_table_cleanup_failure();
            self.completion_diagnostics
                .backend()
                .inc_file_table_quarantine();
            if failure.evidence.updated().is_some() {
                self.completion_diagnostics
                    .backend()
                    .inc_file_table_cleanup_short_update();
            }

            let mut report = UringError::FileTableQuarantined
                .to_report()
                .push_ctx("scope", "driver.unregister_close_owned_fd")
                .with_ctx("file_index", index)
                .with_ctx("generation", generation)
                .with_ctx("expected_files", 1usize)
                .attach_note("closed owned fd was consumed; file slot was quarantined");
            if let Some(errno) = failure.report.cleanup_errno {
                report = report.with_ctx("cleanup_errno", errno);
            }
            if let Some(updated_files) = failure.evidence.updated() {
                report = report.with_ctx("updated_files", updated_files);
            }
            return Err(report.with_diag_src_err(failure.report.report));
        }
        self.file_table.release(index);
        self.file_table.advance_generation(index);
        Ok(())
    }

    /// Points an existing registered slot at a different handle, keeping its descriptor valid.
    ///
    /// Only the waker uses this, to survive an eventfd rebuild without invalidating the
    /// descriptor it already registered. A direct descriptor cannot be replaced in place —
    /// the fd *is* the descriptor — so its caller mints a new one instead.
    pub(crate) fn replace_registered_fixed_fd(
        &mut self,
        fixed_fd: IoFd,
        raw: RawHandle,
    ) -> UringResult<()> {
        let scope = "driver.replace_registered_fixed_fd";
        if self.file_table.is_poisoned() {
            return Err(self.file_table.poisoned_report(scope, Some(fixed_fd)));
        }
        let invalid = |note: &'static str| {
            UringError::InvalidState
                .push_ctx("scope", scope)
                .with_ctx("fd", fixed_fd.to_string())
                .attach_note(note)
        };

        let IoFd::Registered { index, generation } = fixed_fd else {
            return invalid("direct descriptors cannot be replaced in place");
        };

        if !self.file_table.is_initialized() {
            return invalid("registered file table is not initialized");
        }
        if self.file_table.generation(index).is_none() {
            return invalid("registered file index out of bounds");
        }
        if !self.file_table.matches_generation(index, generation) {
            return invalid("registered file generation mismatch while replacing fd");
        }
        if self.file_table.entry(index).is_none() {
            return invalid("registered file slot is empty while replacing fd");
        }

        let fd = raw.raw().as_fd();
        let new_entry = RegisteredFileEntry::BorrowedFd {
            fd,
            kind: raw.kind(),
        };
        let txn = FileUpdateTxn::register(
            index,
            vec![fd],
            "driver.replace_registered_fixed_fd.register_files_update",
        );
        let update = self.update_kernel_file_entries(&txn);
        if let Err(failure) = update {
            let context = failure.poison_context(Some(index));
            return Err(self.poison_file_table(
                scope,
                context,
                failure.report.report,
                None,
                false,
                0,
            ));
        }
        let old_entry = self.file_table.replace_entry(index, new_entry);
        drop(old_entry);
        Ok(())
    }

    pub(crate) fn ensure_file_table_initialized(&mut self) -> UringResult<()> {
        if self.file_table.is_poisoned() {
            return Err(self
                .file_table
                .poisoned_report("driver.ensure_file_table_initialized", None));
        }
        if self.file_table.is_initialized() {
            return Ok(());
        }

        let capacity = self.file_table.fixed_capacity();
        if capacity > MAX_FILE_TABLE_CAPACITY {
            return UringError::InvalidInput
                .push_ctx("scope", "driver.ensure_file_table_initialized")
                .with_ctx("file_table_capacity", capacity)
                .with_ctx("max_file_table_capacity", MAX_FILE_TABLE_CAPACITY)
                .attach_note("configured registered file table capacity is too large");
        }
        if capacity > 0 {
            let sparse = vec![-1; capacity];
            self.ring.submitter().register_files(&sparse).map_err(|e| {
                UringError::Registration.io_report("driver.ensure_file_table_initialized", e)
            })?;
        }

        self.file_table.mark_initialized();
        Ok(())
    }

    /// Registers `files`, taking kernel table slots while they last.
    ///
    /// The first `claimed.len()` inputs get [`IoFd::Registered`] descriptors; anything past
    /// that is handed out as [`IoFd::Direct`], which needs no kernel round trip at all — the
    /// raw fd travels in the SQE. The returned vector is in input order either way.
    pub(crate) fn register_files_internal<'h>(
        &mut self,
        files: Vec<RegisterFd<'h, UringRawHandle>>,
    ) -> UringResult<Vec<IoFd>> {
        if files.is_empty() {
            return Ok(Vec::new());
        }

        let (owned_inputs, conflict) = preflight_owned_files(&self.file_table, &files);
        if let Some(conflict) = conflict {
            let report = duplicate_owned_fd_report(conflict);
            cleanup_rejected_owned_inputs(files, &owned_inputs);
            return Err(report);
        }

        if self.file_table.is_poisoned() {
            return Err(self
                .file_table
                .poisoned_report("driver.register_files_internal", None));
        }

        self.ensure_file_table_initialized()?;

        let claimed = self
            .file_table
            .claim(files.len())
            .push_ctx("scope", "driver.register_files_internal")?;
        let fallback = files.len() - claimed.len();
        let stats = self.buffer_registry.stats_mut();
        stats.file_table_fallback_registrations = stats
            .file_table_fallback_registrations
            .saturating_add(fallback as u64);

        let mut batch = FileRegistrationBatch::new(claimed, files);
        let fixed_indices = batch.fixed_indices();
        let mut cursor = 0usize;
        while cursor < batch.fixed_len() {
            let run_end = consecutive_run_end(&fixed_indices, cursor);
            let txn = batch.registration_txn(cursor, run_end);
            if let Err(failure) = self.update_kernel_file_entries(&txn) {
                return Err(self
                    .abort_file_batch_after_update_failure(&mut batch, cursor, run_end, failure));
            }
            batch.commit_range(&mut self.file_table, cursor, run_end);
            cursor = run_end;
        }

        for file in batch.take_direct_files() {
            let descriptor = match file {
                RegisterFd::Borrowed(b) => IoFd::direct(b.raw()),
                // A direct descriptor is only a handle value, so the driver has to keep the
                // owned handle alive itself until the descriptor is unregistered.
                RegisterFd::Owned(o) => match self.file_table.adopt_direct(o) {
                    Ok(fd) => fd,
                    Err(report) => {
                        return Err(self.abort_file_batch(
                            &mut batch,
                            report,
                            "rollback failed after owned direct adoption failure",
                        ));
                    }
                },
            };
            batch.push_direct_descriptor(descriptor);
        }
        Ok(batch.descriptors(&self.file_table))
    }
}

/// Returns the end (exclusive) of the run of consecutive indices starting at `start`.
fn consecutive_run_end(slots: &[u32], start: usize) -> usize {
    let mut end = start + 1;
    while end < slots.len() && slots[end] == slots[end - 1] + 1 {
        end += 1;
    }
    end
}

#[cfg(test)]
mod tests {
    use super::{KernelUpdateOutcome, UpdateEvidence, consecutive_run_end};
    use veloq_std::{vec, vec::Vec};

    /// Walks `slots` the way `install_claimed_files` does, collecting one run per syscall.
    fn runs(slots: &[u32]) -> Vec<&[u32]> {
        let mut runs = Vec::new();
        let mut cursor = 0;
        while cursor < slots.len() {
            let end = consecutive_run_end(slots, cursor);
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
        let evidence = UpdateEvidence {
            requested: 3,
            outcome: KernelUpdateOutcome::Applied(1),
        };

        assert_eq!(evidence.updated(), Some(1));
        assert_eq!(evidence.requested, 3);
    }

    #[cfg(feature = "test-hooks")]
    #[test]
    fn rejected_and_unknown_updates_do_not_claim_applied_entries() {
        for outcome in [
            KernelUpdateOutcome::Rejected(libc::EIO),
            KernelUpdateOutcome::Unknown(Some(libc::EIO), "test evidence"),
        ] {
            let evidence = UpdateEvidence {
                requested: 1,
                outcome,
            };
            assert_eq!(evidence.updated(), None);
        }
    }
}
