/// The kernel-side fact established by one resource update syscall.
///
/// A short successful update is deliberately kept as `Applied(n)`: callers need to know the
/// exact prefix that may be visible in the kernel, instead of treating it as an ordinary error.
/// 更新结果记录内核侧实际生效范围；短成功保留为 `Applied(n)`，让调用方知道内核可见的精确前缀。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum KernelUpdateOutcome {
    Applied(usize),
    #[cfg(feature = "test-hooks")]
    Rejected(i32),
    Unknown(Option<i32>, &'static str),
}

/// Evidence recorded independently from the diagnostic report produced for an update.
/// 更新证据独立于该操作生成的诊断报告保存。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct UpdateEvidence {
    requested: usize,
    outcome: KernelUpdateOutcome,
}

impl UpdateEvidence {
    pub(crate) const fn new(requested: usize, outcome: KernelUpdateOutcome) -> Self {
        Self { requested, outcome }
    }

    pub(crate) const fn requested(self) -> usize {
        self.requested
    }

    pub(crate) const fn outcome(self) -> KernelUpdateOutcome {
        self.outcome
    }
}

impl UpdateEvidence {
    #[inline]
    pub(super) fn updated(self) -> Option<usize> {
        match self.outcome {
            KernelUpdateOutcome::Applied(updated) => Some(updated),
            #[cfg(feature = "test-hooks")]
            KernelUpdateOutcome::Rejected(_) => None,
            KernelUpdateOutcome::Unknown(_, _) => None,
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct UringRegistrationStats {
    chunk_register_attempts: u64,
    chunk_register_success: u64,
    chunk_register_failures: u64,
    chunk_register_skipped_recent_failure: u64,
    submission_missing_chunk_info: u64,
    raw_buffer_fallbacks: u64,
    /// Descriptors handed out without a kernel table entry because the table was full.
    /// 固定表已满时发放的 direct descriptor，不对应内核表项。
    file_table_fallback_registrations: u64,
}

impl UringRegistrationStats {
    pub(crate) fn inc_chunk_register_attempts(&mut self) {
        self.chunk_register_attempts = self.chunk_register_attempts.saturating_add(1);
    }

    pub(crate) fn inc_chunk_register_success(&mut self) {
        self.chunk_register_success = self.chunk_register_success.saturating_add(1);
    }

    pub(crate) fn inc_chunk_register_failures(&mut self) {
        self.chunk_register_failures = self.chunk_register_failures.saturating_add(1);
    }

    pub(crate) fn inc_chunk_register_skipped_recent_failure(&mut self) {
        self.chunk_register_skipped_recent_failure =
            self.chunk_register_skipped_recent_failure.saturating_add(1);
    }

    pub(crate) fn inc_submission_missing_chunk_info(&mut self) {
        self.submission_missing_chunk_info = self.submission_missing_chunk_info.saturating_add(1);
    }

    pub(crate) fn inc_raw_buffer_fallbacks(&mut self) {
        self.raw_buffer_fallbacks = self.raw_buffer_fallbacks.saturating_add(1);
    }

    pub(super) fn inc_file_table_fallback_registrations(&mut self, count: usize) {
        self.file_table_fallback_registrations = self
            .file_table_fallback_registrations
            .saturating_add(count as u64);
    }

    #[cfg(feature = "test-hooks")]
    pub(crate) const fn chunk_register_attempts(&self) -> u64 {
        self.chunk_register_attempts
    }

    #[cfg(feature = "test-hooks")]
    pub(crate) const fn chunk_register_failures(&self) -> u64 {
        self.chunk_register_failures
    }

    #[cfg(feature = "test-hooks")]
    pub(crate) const fn chunk_register_skipped_recent_failure(&self) -> u64 {
        self.chunk_register_skipped_recent_failure
    }

    #[cfg(feature = "test-hooks")]
    pub(crate) const fn submission_missing_chunk_info(&self) -> u64 {
        self.submission_missing_chunk_info
    }

    #[cfg(feature = "test-hooks")]
    pub(crate) const fn raw_buffer_fallbacks(&self) -> u64 {
        self.raw_buffer_fallbacks
    }
}
