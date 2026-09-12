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
    io,
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
pub(crate) use file_table::{FileTable, FileTablePoisonContext, RegisteredFileEntry, SqeFd};
pub use provided_buf::ProvidedBufStats;
pub(crate) use provided_buf::{PROVIDED_BUF_GROUP_ID, ProvidedBufGroup};

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

    pub(crate) fn unregister_fixed_fd(&mut self, fd: IoFd) -> UringResult<()> {
        match fd {
            // A direct descriptor has no slot; all the table can hold for it is the handle it
            // owns, and dropping that closes the fd.
            IoFd::Direct(raw) => {
                drop(self.file_table.release_direct(raw));
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
            IoFd::Direct(raw) => {
                if let Some(handle) = self.file_table.release_direct(raw) {
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
        self.file_table.install_entry(
            index,
            RegisteredFileEntry::BorrowedFd {
                fd,
                kind: raw.kind(),
            },
        );
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
        if self.file_table.is_poisoned() {
            return Err(self
                .file_table
                .poisoned_report("driver.register_files_internal", None));
        }
        if files.is_empty() {
            return Ok(Vec::new());
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
            descriptors.push(match file {
                RegisterFd::Borrowed(b) => IoFd::direct(b.raw()),
                // A direct descriptor is only a handle value, so the driver has to keep the
                // owned handle alive itself until the descriptor is unregistered.
                RegisterFd::Owned(o) => self.file_table.adopt_direct(o),
            });
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
