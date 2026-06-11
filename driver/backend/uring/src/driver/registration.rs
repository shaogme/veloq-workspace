use crate::driver::UringDriver;
use crate::error::{UringError, UringResult};
use diagweave::prelude::*;
use std::time::{Duration, Instant};

use crate::config::{IoFd, OwnedRawHandle, RawHandle, RawHandleKind, UringRawHandle};
use veloq_driver_core::driver::RegisterFd;

pub(crate) const MAX_CHUNKS: usize = 1024;
pub(crate) const REGISTER_FAILURE_RETRY_COOLDOWN: Duration = Duration::from_millis(250);
const MIN_FILE_TABLE_CAPACITY: usize = 1;
const INITIAL_FILE_GENERATION: u64 = 1;

#[derive(Debug)]
pub(crate) struct FileSlot {
    pub(crate) entry: Option<RegisteredFileEntry>,
    pub(crate) generation: u64,
}

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
    file_slots: &[FileSlot],
    fd: IoFd,
    expected_kind: Option<RawHandleKind>,
    scope: &'static str,
) -> UringResult<u32> {
    let idx = fd.fixed_index();
    let index = idx as usize;
    let Some(slot) = file_slots.get(index) else {
        return Err(UringError::ResolveFd
            .to_report()
            .push_ctx("scope", scope)
            .with_ctx("fd_fixed_index", idx)
            .with_ctx("fd_generation", fd.generation())
            .attach_note("registered file descriptor index out of bounds"));
    };

    if slot.generation != fd.generation() {
        let mut report = UringError::ResolveFd
            .to_report()
            .push_ctx("scope", scope)
            .with_ctx("fd_fixed_index", idx)
            .with_ctx("fd_generation", fd.generation())
            .attach_note("stale registered file descriptor generation");
        report = report.with_ctx("current_generation", slot.generation);
        return Err(report);
    }

    let Some(entry) = slot.entry.as_ref() else {
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
        id: veloq_buf::heap::ChunkId,
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
                .with_ctx("chunk_id", id.raw())
                .attach_note("recent chunk registration failure cooldown"));
        }

        let index = id.as_usize();
        if index >= MAX_CHUNKS {
            return Err(UringError::InvalidInput
                .to_report()
                .push_ctx("scope", "driver.register_chunk_internal")
                .with_ctx("chunk_id", index)
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

    fn unregister_file_slot(
        &mut self,
        idx: u32,
        advance_generation: bool,
        scope: &'static str,
    ) -> UringResult<()> {
        let index = idx as usize;
        if index >= self.file_slots.len() {
            return Ok(());
        }

        let Some(entry) = self.file_slots[index].entry.take() else {
            return Ok(());
        };

        if let Err(e) = self.ring.submitter().register_files_update(idx, &[-1]) {
            self.file_slots[index].entry = Some(entry);
            return Err(UringError::Registration.io_report(scope, e));
        }

        self.free_file_slots.push(idx);
        if advance_generation {
            Self::advance_file_generation(&mut self.file_slots[index].generation);
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
        if !self.file_table_initialized {
            return Ok(());
        }
        let idx = fd.fixed_index();
        let index = idx as usize;
        if index < self.file_slots.len() {
            if self.file_slots[index].generation != fd.generation() {
                return Ok(());
            }
            self.unregister_file_slot(idx, true, "driver.unregister_fixed_fd")?;
        }
        Ok(())
    }

    pub(crate) fn unregister_close_owned_fd(&mut self, fd: IoFd) -> UringResult<()> {
        if !self.file_table_initialized {
            return Ok(());
        }
        let idx = fd.fixed_index();
        let index = idx as usize;
        if index >= self.file_slots.len() {
            return Ok(());
        }
        if self.file_slots[index].generation != fd.generation() {
            return Ok(());
        }
        let Some(entry) = self.file_slots[index].entry.take() else {
            return Ok(());
        };
        if let Err(e) = self.ring.submitter().register_files_update(idx, &[-1]) {
            self.file_slots[index].entry = Some(entry);
            return Err(UringError::Registration.io_report("driver.unregister_close_owned_fd", e));
        }
        self.free_file_slots.push(idx);
        Self::advance_file_generation(&mut self.file_slots[index].generation);
        let _ = std::mem::ManuallyDrop::new(entry);
        Ok(())
    }

    pub(crate) fn replace_registered_fixed_fd(
        &mut self,
        fixed_fd: IoFd,
        raw: RawHandle,
    ) -> UringResult<()> {
        if !self.file_table_initialized {
            return Err(UringError::InvalidState
                .to_report()
                .push_ctx("scope", "driver.replace_registered_fixed_fd")
                .with_ctx("fd_fixed_index", fixed_fd.fixed_index())
                .attach_note("registered file table is not initialized"));
        }

        let idx = fixed_fd.fixed_index();
        let index = idx as usize;
        let Some(slot) = self.file_slots.get_mut(index) else {
            return Err(UringError::InvalidState
                .to_report()
                .push_ctx("scope", "driver.replace_registered_fixed_fd")
                .with_ctx("fd_fixed_index", idx)
                .with_ctx("fd_generation", fixed_fd.generation())
                .attach_note("registered file index out of bounds"));
        };
        if slot.generation != fixed_fd.generation() {
            return Err(UringError::InvalidState
                .to_report()
                .push_ctx("scope", "driver.replace_registered_fixed_fd")
                .with_ctx("fd_fixed_index", idx)
                .with_ctx("fd_generation", fixed_fd.generation())
                .attach_note("registered file generation mismatch while replacing fd"));
        }
        if slot.entry.is_none() {
            return Err(UringError::InvalidState
                .to_report()
                .push_ctx("scope", "driver.replace_registered_fixed_fd")
                .with_ctx("fd_fixed_index", idx)
                .with_ctx("fd_generation", fixed_fd.generation())
                .attach_note("registered file slot is empty while replacing fd"));
        }

        let fd = raw.raw().as_fd();
        self.ring
            .submitter()
            .register_files_update(idx, &[fd])
            .map_err(|e| {
                UringError::Registration.io_report(
                    "driver.replace_registered_fixed_fd.register_files_update",
                    e,
                )
            })?;
        slot.entry = Some(RegisteredFileEntry::BorrowedFd {
            fd,
            kind: raw.kind(),
        });
        Ok(())
    }

    pub(crate) fn ensure_file_table_initialized(&mut self) -> UringResult<()> {
        if self.file_table_initialized {
            return Ok(());
        }

        let capacity = self.ops.capacity().max(MIN_FILE_TABLE_CAPACITY);
        let sparse = vec![-1; capacity];
        self.ring.submitter().register_files(&sparse).map_err(|e| {
            UringError::Registration.io_report("driver.ensure_file_table_initialized", e)
        })?;

        self.file_slots = (0..capacity)
            .map(|_| FileSlot {
                entry: None,
                generation: INITIAL_FILE_GENERATION,
            })
            .collect();
        self.free_file_slots = (0..capacity as u32).rev().collect();
        self.file_table_initialized = true;
        Ok(())
    }

    pub(crate) fn register_files_internal<'h>(
        &mut self,
        files: Vec<RegisterFd<'h, UringRawHandle>>,
    ) -> UringResult<Vec<IoFd>> {
        self.ensure_file_table_initialized()?;

        let requested = files.len();
        let available = self.free_file_slots.len();
        if requested > available {
            return Err(UringError::InvalidState
                .to_report()
                .push_ctx("scope", "driver.register_files_internal")
                .with_ctx("requested_files", requested)
                .with_ctx("free_file_slots", available)
                .attach_note("io_uring registered file table exhausted"));
        }

        let mut fixed_fds = Vec::with_capacity(files.len());
        let mut registered_slots = Vec::with_capacity(files.len());
        for file in files {
            let entry = match file {
                RegisterFd::Borrowed(b) => RegisteredFileEntry::BorrowedFd {
                    fd: b.raw().as_fd(),
                    kind: b.kind(),
                },
                RegisterFd::Owned(o) => RegisteredFileEntry::OwnedHandle(o),
            };
            let fd = entry.fd();
            let idx = self.free_file_slots.pop().expect(
                "register_files_internal capacity precheck guarantees enough free file slots",
            );
            if let Err(e) = self.ring.submitter().register_files_update(idx, &[fd]) {
                self.free_file_slots.push(idx);
                let report = UringError::Registration
                    .io_report("driver.register_files_internal.register_files_update", e);
                if let Err(rollback_report) = self.rollback_file_slots(&mut registered_slots) {
                    return Err(rollback_report
                        .attach_note("rollback failed after registered file update failure"));
                }
                return Err(report);
            }
            let slot = &mut self.file_slots[idx as usize];
            slot.entry = Some(entry);
            registered_slots.push(idx);
            fixed_fds.push(IoFd::fixed_with_generation(idx, slot.generation));
        }
        Ok(fixed_fds)
    }
}
