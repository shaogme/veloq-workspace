use crate::{
    config::{IoFd, RawHandle, UringRawHandle},
    driver::UringDriver,
    error::{UringError, UringResult},
};
use diagweave::prelude::*;
use veloq_buf::heap::ChunkId;
use veloq_driver_core::driver::RegisterFd;
use veloq_std::{mem::ManuallyDrop, string::ToString, time::Duration, vec, vec::Vec};

pub(crate) mod buffer;
pub(crate) mod file_table;
pub(crate) mod provided_buf;

pub(crate) use buffer::UringBufferRegistry;
pub(crate) use file_table::{FileTable, RegisteredFileEntry, SqeFd};
pub(crate) use provided_buf::ProvidedBufGroup;
pub use provided_buf::ProvidedBufStats;

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
    /// Descriptors handed out without a kernel table entry because the table was full.
    pub(crate) file_table_fallback_registrations: u64,
}

impl<'a> UringDriver<'a> {
    #[inline]
    pub(crate) fn register_chunk_internal(
        &mut self,
        id: ChunkId,
        ptr: *const u8,
        len: usize,
    ) -> UringResult<()> {
        self.submit_env().register_chunk(id, ptr, len)
    }

    /// Clears the kernel table entry for `idx`.
    ///
    /// Every slot in the table now mirrors a kernel entry one-to-one — descriptors that did
    /// not fit are handed out as [`IoFd::Direct`] and never reach this path.
    fn clear_kernel_file_entry(&mut self, idx: u32, scope: &'static str) -> UringResult<()> {
        self.ring
            .submitter()
            .register_files_update(idx, &[-1])
            .map(|_| ())
            .map_err(|e| UringError::Registration.io_report(scope, e))
    }

    fn unregister_file_slot(
        &mut self,
        idx: u32,
        advance_generation: bool,
        scope: &'static str,
    ) -> UringResult<()> {
        let Some(entry) = self.file_table.take_entry(idx) else {
            return Ok(());
        };

        if let Err(report) = self.clear_kernel_file_entry(idx, scope) {
            self.file_table.install_entry(idx, entry);
            return Err(report);
        }

        self.file_table.release(idx);
        if advance_generation {
            self.file_table.advance_generation(idx);
        }
        Ok(())
    }

    fn rollback_file_slots(&mut self, registered: &mut Vec<u32>) -> UringResult<()> {
        let mut first_error = None;
        while let Some(idx) = registered.pop() {
            if let Err(report) =
                self.unregister_file_slot(idx, false, "driver.register_files_internal.rollback")
                && first_error.is_none()
            {
                first_error = Some(report);
            }
        }

        if let Some(report) = first_error {
            Err(report.attach_note("registered file rollback failed"))
        } else {
            Ok(())
        }
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
                if !self.file_table.is_initialized()
                    || !self.file_table.matches_generation(index, generation)
                {
                    return Ok(());
                }
                self.unregister_file_slot(index, true, "driver.unregister_fixed_fd")
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
        if let Err(report) = self.clear_kernel_file_entry(index, "driver.unregister_close_owned_fd")
        {
            self.file_table.install_entry(index, entry);
            return Err(report);
        }
        self.file_table.release(index);
        self.file_table.advance_generation(index);
        let _ = ManuallyDrop::new(entry);
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
        self.ring
            .submitter()
            .register_files_update(index, &[fd])
            .map_err(|e| {
                UringError::Registration.io_report(
                    "driver.replace_registered_fixed_fd.register_files_update",
                    e,
                )
            })?;
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
    fn register_file_run(&mut self, start: u32, fds: &[i32]) -> UringResult<()> {
        let updated = self
            .ring
            .submitter()
            .register_files_update(start, fds)
            .map_err(|e| {
                UringError::Registration
                    .io_report("driver.register_files_internal.register_files_update", e)
            })?;
        if updated != fds.len() {
            return UringError::Registration
                .push_ctx(
                    "scope",
                    "driver.register_files_internal.register_files_update",
                )
                .with_ctx("start_index", start)
                .with_ctx("requested_files", fds.len())
                .with_ctx("updated_files", updated)
                .attach_note("io_uring updated fewer registered file entries than requested");
        }
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

            if let Err(report) = outcome {
                // Hand back the slots this batch never got to.
                self.file_table
                    .release_all(claimed[cursor..].iter().copied());
                if let Err(rollback_report) = self.rollback_file_slots(&mut installed) {
                    return Err(rollback_report
                        .attach_note("rollback failed after registered file update failure"));
                }
                return Err(report);
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
