use crate::driver::UringDriver;
use crate::error::{UringError, UringResult, from_io_error};
use diagweave::report::Report;
use std::time::{Duration, Instant};

use crate::config::{IoFd, OwnedRawHandle, UringRawHandle};
use veloq_driver_core::driver::RegisterFd;

pub(crate) const MAX_CHUNKS: usize = 1024;
pub(crate) const REGISTER_FAILURE_RETRY_COOLDOWN: Duration = Duration::from_millis(250);
const MIN_FILE_TABLE_CAPACITY: usize = 1;

#[derive(Debug)]
pub(crate) enum RegisteredFileEntry {
    BorrowedFd(i32),
    OwnedHandle(OwnedRawHandle),
}

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct UringRegistrationStats {
    pub(crate) chunk_register_attempts: u64,
    pub(crate) chunk_register_success: u64,
    pub(crate) chunk_register_failures: u64,
    pub(crate) chunk_register_skipped_recent_failure: u64,
    pub(crate) submission_missing_chunk_info: u64,
}

impl<'a> UringDriver<'a> {
    pub(crate) fn register_chunk_internal(
        &mut self,
        id: u16,
        ptr: *const u8,
        len: usize,
    ) -> UringResult<()> {
        if let Some(last_fail) = self.chunk_register_failures_recent.get(&id)
            && last_fail.elapsed() < REGISTER_FAILURE_RETRY_COOLDOWN
        {
            self.registration_stats
                .chunk_register_skipped_recent_failure = self
                .registration_stats
                .chunk_register_skipped_recent_failure
                .saturating_add(1);
            return Err(Report::new(UringError::Registration).attach_note(format!(
                "driver.register_chunk_internal: recent failure cooldown, chunk_id={id}"
            )));
        }

        let index = id as usize;
        if index >= MAX_CHUNKS {
            return Err(Report::new(UringError::InvalidInput).attach_note(format!(
                "driver.register_chunk_internal: chunk id exceeds max, id={id}, max={MAX_CHUNKS}"
            )));
        }

        let iovecs = [libc::iovec {
            iov_base: ptr as *mut _,
            iov_len: len,
        }];

        // Use register_buffers_update
        self.registration_stats.chunk_register_attempts = self
            .registration_stats
            .chunk_register_attempts
            .saturating_add(1);
        let register_result = unsafe {
            self.ring
                .submitter()
                .register_buffers_update(index as u32, &iovecs, None)
        };
        if let Err(e) = register_result {
            self.registration_stats.chunk_register_failures = self
                .registration_stats
                .chunk_register_failures
                .saturating_add(1);
            self.chunk_register_failures_recent
                .insert(id, Instant::now());
            return Err(from_io_error(
                UringError::Registration,
                "driver.register_chunk_internal.register_buffers_update",
                e,
            ));
        }

        // Mark as registered in local bitset
        let _ = self.registered_chunks.set(index);
        self.chunk_register_failures_recent.remove(&id);
        self.registration_stats.chunk_register_success = self
            .registration_stats
            .chunk_register_success
            .saturating_add(1);

        Ok(())
    }

    pub(crate) fn unregister_fixed_fd(&mut self, fd: IoFd) -> UringResult<()> {
        if !self.file_table_initialized {
            return Ok(());
        }
        let idx = fd.fixed_index();
        let index = idx as usize;
        if index < self.registered_files.len() {
            if self.file_generations.get(index).copied() != Some(fd.generation()) {
                return Ok(());
            }
            let Some(_entry) = self.registered_files[index].take() else {
                return Ok(());
            };
            self.ring
                .submitter()
                .register_files_update(idx, &[-1])
                .map_err(|e| {
                    from_io_error(UringError::Registration, "driver.unregister_fixed_fd", e)
                })?;
            self.free_file_slots.push(idx);
            self.file_generations[index] = self.file_generations[index].wrapping_add(1);
        }
        Ok(())
    }

    pub(crate) fn ensure_file_table_initialized(&mut self) -> UringResult<()> {
        if self.file_table_initialized {
            return Ok(());
        }

        let capacity = self.ops.local.len().max(MIN_FILE_TABLE_CAPACITY);
        let sparse = vec![-1; capacity];
        self.ring.submitter().register_files(&sparse).map_err(|e| {
            from_io_error(
                UringError::Registration,
                "driver.ensure_file_table_initialized",
                e,
            )
        })?;

        self.registered_files = (0..capacity).map(|_| None).collect();
        self.file_generations = vec![0; capacity];
        self.free_file_slots = (0..capacity as u32).rev().collect();
        self.file_table_initialized = true;
        Ok(())
    }

    pub(crate) fn register_files_internal<'a>(
        &mut self,
        files: Vec<RegisterFd<'a, UringRawHandle>>,
    ) -> UringResult<Vec<IoFd>> {
        self.ensure_file_table_initialized()?;

        let mut fixed_fds = Vec::with_capacity(files.len());
        for file in files {
            let entry = match file {
                RegisterFd::Borrowed(b) => RegisteredFileEntry::BorrowedFd(b.raw().as_fd()),
                RegisterFd::Owned(o) => RegisteredFileEntry::OwnedHandle(o),
            };
            let fd = match &entry {
                RegisteredFileEntry::BorrowedFd(fd) => *fd,
                RegisteredFileEntry::OwnedHandle(o) => o.raw().as_fd(),
            };
            let idx = self.free_file_slots.pop().ok_or_else(|| {
                crate::driver::invalid_state(
                    "driver.register_files_internal",
                    "io_uring registered file table exhausted",
                )
            })?;
            self.ring
                .submitter()
                .register_files_update(idx, &[fd])
                .map_err(|e| {
                    from_io_error(
                        UringError::Registration,
                        "driver.register_files_internal.register_files_update",
                        e,
                    )
                })?;
            self.registered_files[idx as usize] = Some(entry);
            let generation = self.file_generations[idx as usize];
            fixed_fds.push(IoFd::fixed_with_generation(idx, generation));
        }
        Ok(fixed_fds)
    }
}
