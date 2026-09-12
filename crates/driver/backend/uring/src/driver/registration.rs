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

pub(crate) use buffer::{BufferRegistrationQuarantine, UringBufferRegistry};
pub(crate) use file_table::{
    FileTable, FileTablePoisonContext, OwnedLocation, RegisteredFileEntry, SqeFd,
};
pub use provided_buf::ProvidedBufStats;
pub(crate) use provided_buf::{PROVIDED_BUF_GROUP_ID, ProvidedBufGroup};

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

struct FileTableUpdateFailure {
    report: Report<UringError>,
    cleanup_errno: Option<i32>,
    updated_files: Option<usize>,
    scope: &'static str,
    start_index: u32,
    requested_files: usize,
}

impl FileTableUpdateFailure {
    fn poison_context(&self, failed_index: Option<u32>) -> FileTablePoisonContext {
        FileTablePoisonContext {
            scope: self.scope,
            failed_index,
            start_index: self.start_index,
            requested_files: self.requested_files,
            updated_files: self.updated_files,
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

    /// Runs one `register_files_update` call, with deterministic test-only outcomes when enabled.
    fn register_files_update(&mut self, start: u32, files: &[i32]) -> io::Result<usize> {
        #[cfg(feature = "test-hooks")]
        if let Some(outcome) = self.register_files_update_outcomes.pop_front() {
            return match outcome {
                RegisterFilesUpdateOutcome::Actual => self
                    .ring
                    .submitter()
                    .register_files_update(start, files)
                    .map_err(io::Error::from),
                RegisterFilesUpdateOutcome::Error(errno) => {
                    Err(io::Error::from_raw_os_error(errno))
                }
                RegisterFilesUpdateOutcome::Updated(updated) => Ok(updated),
            };
        }

        self.ring
            .submitter()
            .register_files_update(start, files)
            .map_err(io::Error::from)
    }

    fn update_kernel_file_entries(
        &mut self,
        start: u32,
        files: &[i32],
        scope: &'static str,
    ) -> Result<(), FileTableUpdateFailure> {
        let requested_files = files.len();
        let updated_files = self.register_files_update(start, files).map_err(|error| {
            let cleanup_errno = error.raw_os_error();
            let report = UringError::Registration
                .io_report(scope, error)
                .with_ctx("start_index", start)
                .with_ctx("requested_files", requested_files);
            FileTableUpdateFailure {
                cleanup_errno,
                report,
                updated_files: None,
                scope,
                start_index: start,
                requested_files,
            }
        })?;
        if updated_files != files.len() {
            return Err(FileTableUpdateFailure {
                report: UringError::Registration
                    .to_report()
                    .push_ctx("scope", scope)
                    .with_ctx("start_index", start)
                    .with_ctx("requested_files", files.len())
                    .with_ctx("updated_files", updated_files)
                    .attach_note("io_uring updated fewer registered file entries than requested"),
                cleanup_errno: None,
                updated_files: Some(updated_files),
                scope,
                start_index: start,
                requested_files,
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
        self.update_kernel_file_entries(idx, &[-1], scope)
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
                    remaining_indices: mem::take(registered),
                });
            }
        }
        Ok(())
    }

    fn rollback_descriptors_after_registration_failure(
        &mut self,
        descriptors: &[IoFd],
        primary: Report<UringError>,
    ) -> Report<UringError> {
        let mut registered = descriptors
            .iter()
            .filter_map(|fd| match fd {
                IoFd::Registered { index, .. } => Some(*index),
                IoFd::Direct(_) | IoFd::OwnedDirect { .. } => None,
            })
            .collect::<Vec<_>>();

        for fd in descriptors.iter().copied() {
            if matches!(fd, IoFd::OwnedDirect { .. }) {
                drop(self.file_table.release_direct(fd));
            }
        }

        match self.rollback_file_slots(&mut registered) {
            Ok(()) => primary,
            Err(rollback) => {
                let context = rollback.failure.poison_context(Some(rollback.failed_index));
                self.poison_file_table(
                    "driver.register_files_internal.rollback",
                    context,
                    primary,
                    Some(rollback.failure.report),
                    true,
                    rollback.remaining_indices.len(),
                )
                .attach_note("rollback failed after owned direct adoption failure")
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
                            failure.report,
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
            if failure.updated_files.is_some() {
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
            if let Some(errno) = failure.cleanup_errno {
                report = report.with_ctx("cleanup_errno", errno);
            }
            if let Some(updated_files) = failure.updated_files {
                report = report.with_ctx("updated_files", updated_files);
            }
            return Err(report.with_diag_src_err(failure.report));
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
        let update = self.update_kernel_file_entries(
            index,
            &[fd],
            "driver.replace_registered_fixed_fd.register_files_update",
        );
        if let Err(failure) = update {
            let context = failure.poison_context(Some(index));
            return Err(self.poison_file_table(scope, context, failure.report, None, false, 0));
        }
        let old_entry = self.file_table.replace_entry(
            index,
            RegisteredFileEntry::BorrowedFd {
                fd,
                kind: raw.kind(),
            },
        );
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

    /// Registers `fds` into the `fds.len()` consecutive table slots starting at `start`.
    fn register_file_run(&mut self, start: u32, fds: &[i32]) -> Result<(), FileTableUpdateFailure> {
        self.update_kernel_file_entries(
            start,
            fds,
            "driver.register_files_internal.register_files_update",
        )
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

        let mut files = files.into_iter();
        let entries = files
            .by_ref()
            .take(claimed.len())
            .map(|file| match file {
                RegisterFd::Borrowed(b) => RegisteredFileEntry::BorrowedFd {
                    fd: b.raw().as_fd(),
                    kind: b.kind(),
                },
                RegisterFd::Owned(o) => RegisteredFileEntry::OwnedHandle(o),
            })
            .collect::<Vec<_>>();

        // On failure the untaken `files` drop here, closing any owned handles among them —
        // the same thing that happens to the entries the rollback discards.
        let mut descriptors = self.install_claimed_files(claimed, entries)?;

        for file in files {
            let descriptor = match file {
                RegisterFd::Borrowed(b) => IoFd::direct(b.raw()),
                // A direct descriptor is only a handle value, so the driver has to keep the
                // owned handle alive itself until the descriptor is unregistered.
                RegisterFd::Owned(o) => match self.file_table.adopt_direct(o) {
                    Ok(fd) => fd,
                    Err(report) => {
                        return Err(self.rollback_descriptors_after_registration_failure(
                            &descriptors,
                            report,
                        ));
                    }
                },
            };
            descriptors.push(descriptor);
        }
        Ok(descriptors)
    }

    /// Publishes `entries` into `claimed`, telling the kernel about each consecutive run.
    fn install_claimed_files(
        &mut self,
        claimed: Vec<u32>,
        entries: Vec<RegisteredFileEntry>,
    ) -> UringResult<Vec<IoFd>> {
        debug_assert_eq!(claimed.len(), entries.len());
        let fds = entries
            .iter()
            .map(RegisteredFileEntry::fd)
            .collect::<Vec<_>>();
        let mut entries = entries.into_iter();

        let mut installed = Vec::with_capacity(fds.len());
        let mut cursor = 0usize;
        while cursor < claimed.len() {
            let run_end = consecutive_run_end(&claimed, cursor);
            let outcome = self.register_file_run(claimed[cursor], &fds[cursor..run_end]);

            // The run's entries are recorded even when the update failed: a partial update may
            // have left some fds in the kernel table, and the rollback below needs the entries
            // in place to reset those slots to -1.
            for idx in claimed[cursor..run_end].iter().copied() {
                let entry = entries
                    .next()
                    .expect("one registered file entry per claimed file slot");
                self.file_table.install_entry(idx, entry);
                installed.push(idx);
            }
            cursor = run_end;

            if let Err(failure) = outcome {
                // Hand back the slots this batch never got to.
                self.file_table
                    .release_all(claimed[cursor..].iter().copied());
                if let Err(rollback) = self.rollback_file_slots(&mut installed) {
                    let context = failure.poison_context(Some(rollback.failed_index));
                    let original_report = failure.report;
                    let rollback_report = rollback.failure.report;
                    return Err(self
                        .poison_file_table(
                            "driver.register_files_internal.rollback",
                            context,
                            original_report,
                            Some(rollback_report),
                            true,
                            rollback.remaining_indices.len(),
                        )
                        .attach_note("rollback failed after registered file update failure"));
                }
                return Err(failure.report);
            }
        }

        Ok(claimed
            .into_iter()
            .map(|idx| {
                self.file_table
                    .descriptor(idx)
                    .expect("installed slot exists")
            })
            .collect())
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
    use super::consecutive_run_end;
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
}
