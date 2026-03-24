use crate::driver::UringDriver;
use crate::error::{UringError, UringResult, from_io_error};
use error_stack::Report;
use std::time::{Duration, Instant};

pub(crate) const MAX_CHUNKS: usize = 1024;
pub(crate) const REGISTER_FAILURE_RETRY_COOLDOWN: Duration = Duration::from_millis(250);

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct UringRegistrationStats {
    pub(crate) chunk_register_attempts: u64,
    pub(crate) chunk_register_success: u64,
    pub(crate) chunk_register_failures: u64,
    pub(crate) chunk_register_skipped_recent_failure: u64,
    pub(crate) submission_missing_chunk_info: u64,
}

impl UringDriver {
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
            return Err(Report::new(UringError::Registration).attach(format!(
                "driver.register_chunk_internal: recent failure cooldown, chunk_id={id}"
            )));
        }

        let index = id as usize;
        if index >= MAX_CHUNKS {
            return Err(Report::new(UringError::InvalidInput).attach(format!(
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
}
