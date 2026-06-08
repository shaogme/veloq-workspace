use crate::driver::UringDriver;
use crate::error::{UringError, UringResult};
use diagweave::prelude::*;
use std::time::{Duration, Instant};

use crate::config::{IoFd, OwnedRawHandle, RawHandleKind, UringRawHandle};
use veloq_driver_core::driver::RegisterFd;

pub(crate) const MAX_CHUNKS: usize = 1024;
pub(crate) const REGISTER_FAILURE_RETRY_COOLDOWN: Duration = Duration::from_millis(250);
const MIN_FILE_TABLE_CAPACITY: usize = 1;
const INITIAL_FILE_GENERATION: u64 = 1;

#[derive(Debug)]
pub(crate) enum RegisteredFileEntry {
    BorrowedFd { fd: i32, kind: RawHandleKind },
    OwnedHandle(OwnedRawHandle),
}

impl RegisteredFileEntry {
    #[inline]
    pub(crate) fn fd(&self) -> i32 {
        match self {
            Self::BorrowedFd { fd, .. } => *fd,
            Self::OwnedHandle(handle) => handle.raw().as_fd(),
        }
    }

    #[inline]
    pub(crate) fn kind(&self) -> RawHandleKind {
        match self {
            Self::BorrowedFd { kind, .. } => *kind,
            Self::OwnedHandle(handle) => handle.kind(),
        }
    }
}

pub(crate) fn resolve_registered_fixed_fd(
    registered_files: &[Option<RegisteredFileEntry>],
    file_generations: &[u64],
    fd: IoFd,
    expected_kind: Option<RawHandleKind>,
    scope: &'static str,
) -> UringResult<u32> {
    let idx = fd.fixed_index();
    let index = idx as usize;
    let Some(slot) = registered_files.get(index) else {
        return Err(UringError::ResolveFd
            .to_report()
            .push_ctx("scope", scope)
            .with_ctx("fd_fixed_index", idx)
            .with_ctx("fd_generation", fd.generation())
            .attach_note("registered file descriptor index out of bounds"));
    };

    let current_generation = file_generations.get(index).copied();
    if current_generation != Some(fd.generation()) {
        let mut report = UringError::ResolveFd
            .to_report()
            .push_ctx("scope", scope)
            .with_ctx("fd_fixed_index", idx)
            .with_ctx("fd_generation", fd.generation())
            .attach_note("stale registered file descriptor generation");
        if let Some(current_generation) = current_generation {
            report = report.with_ctx("current_generation", current_generation);
        }
        return Err(report);
    }

    let Some(entry) = slot.as_ref() else {
        return Err(UringError::ResolveFd
            .to_report()
            .push_ctx("scope", scope)
            .with_ctx("fd_fixed_index", idx)
            .with_ctx("fd_generation", fd.generation())
            .attach_note("invalid registered file descriptor"));
    };

    if let Some(expected_kind) = expected_kind {
        let current_kind = entry.kind();
        if current_kind != expected_kind {
            return Err(UringError::ResolveFd
                .to_report()
                .push_ctx("scope", scope)
                .with_ctx("fd_fixed_index", idx)
                .with_ctx("fd_generation", fd.generation())
                .with_ctx("expected_kind", format!("{expected_kind:?}"))
                .with_ctx("current_kind", format!("{current_kind:?}"))
                .attach_note("registered file descriptor kind mismatch"));
        }
    }

    Ok(idx)
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
    #[inline]
    fn advance_file_generation(generation: &mut u64) {
        *generation = generation.wrapping_add(1);
        if *generation == 0 {
            *generation = INITIAL_FILE_GENERATION;
        }
    }

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
            return Err(UringError::Registration
                .to_report()
                .push_ctx("scope", "driver.register_chunk_internal")
                .with_ctx("chunk_id", id as usize)
                .attach_note("recent chunk registration failure cooldown"));
        }

        let index = id as usize;
        if index >= MAX_CHUNKS {
            return Err(UringError::InvalidInput
                .to_report()
                .push_ctx("scope", "driver.register_chunk_internal")
                .with_ctx("chunk_id", id as usize)
                .with_ctx("max_chunks", MAX_CHUNKS)
                .attach_note("chunk id exceeds maximum registered chunk count"));
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
            return Err(UringError::Registration
                .io_report("driver.register_chunk_internal.register_buffers_update", e));
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
                .map_err(|e| UringError::Registration.io_report("driver.unregister_fixed_fd", e))?;
            self.free_file_slots.push(idx);
            Self::advance_file_generation(&mut self.file_generations[index]);
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
            UringError::Registration.io_report("driver.ensure_file_table_initialized", e)
        })?;

        self.registered_files = (0..capacity).map(|_| None).collect();
        self.file_generations = vec![INITIAL_FILE_GENERATION; capacity];
        self.free_file_slots = (0..capacity as u32).rev().collect();
        self.file_table_initialized = true;
        Ok(())
    }

    pub(crate) fn register_files_internal<'h>(
        &mut self,
        files: Vec<RegisterFd<'h, UringRawHandle>>,
    ) -> UringResult<Vec<IoFd>> {
        self.ensure_file_table_initialized()?;

        let mut fixed_fds = Vec::with_capacity(files.len());
        for file in files {
            let entry = match file {
                RegisterFd::Borrowed(b) => RegisteredFileEntry::BorrowedFd {
                    fd: b.raw().as_fd(),
                    kind: b.kind(),
                },
                RegisterFd::Owned(o) => RegisteredFileEntry::OwnedHandle(o),
            };
            let fd = entry.fd();
            let idx = self.free_file_slots.pop().ok_or_else(|| {
                UringError::InvalidState.report(
                    "driver.register_files_internal",
                    "io_uring registered file table exhausted",
                )
            })?;
            self.ring
                .submitter()
                .register_files_update(idx, &[fd])
                .map_err(|e| {
                    UringError::Registration
                        .io_report("driver.register_files_internal.register_files_update", e)
                })?;
            self.registered_files[idx as usize] = Some(entry);
            let generation = self.file_generations[idx as usize];
            fixed_fds.push(IoFd::fixed_with_generation(idx, generation));
        }
        Ok(fixed_fds)
    }
}
