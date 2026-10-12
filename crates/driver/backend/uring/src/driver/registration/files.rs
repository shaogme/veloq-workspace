use crate::{
    config::{FileTableExhaustion, IoFd, RawHandle},
    diagnostics::UringCompletionDiagnostics,
    driver::registration::{
        FileRegistrationBatch, FileRegistrationRequest, FileTable, FileTablePoisonContext,
        FileUpdateTxn, KernelUpdateOutcome, RegisteredFileEntry, UpdateEvidence,
    },
    error::{UringError, UringResult},
};
use diagweave::prelude::*;
use tracing::error;
use veloq_driver_core::driver::RegisterFd;

#[cfg(feature = "test-hooks")]
use veloq_driver_core::driver::test_hooks::RegisterFilesUpdateOutcome;
use veloq_io_uring::{KernelCapabilities, ResourceLayout, ResourceRegistration, Submitter};
use veloq_std::{
    io,
    mem::{self, ManuallyDrop},
    string::ToString,
    vec,
    vec::Vec,
};

#[cfg(feature = "test-hooks")]
use veloq_std::collections::VecDeque;

pub(crate) struct RegistrationPorts<'ring, 'borrow> {
    submitter: &'borrow Submitter<'ring>,
    diagnostics: &'borrow UringCompletionDiagnostics,
}

impl<'ring, 'borrow> RegistrationPorts<'ring, 'borrow> {
    pub(crate) const fn new(
        submitter: &'borrow Submitter<'ring>,
        diagnostics: &'borrow UringCompletionDiagnostics,
    ) -> Self {
        Self {
            submitter,
            diagnostics,
        }
    }
}

pub(super) struct FixedFileOwner {
    table: FileTable,
    registration: Option<ResourceRegistration>,
    #[cfg(feature = "test-hooks")]
    update_outcomes: VecDeque<RegisterFilesUpdateOutcome>,
}

/// Upper bound on [`UringConfig::file_table_capacity`](crate::config::UringConfig).
///
/// The kernel's own limit is smaller still (`IORING_MAX_FIXED_FILES`), but it rejects an
/// oversized table only *after* the sparse `-1` vector has been built — and that vector is
/// four bytes per entry, so an unchecked `u32` would ask for gigabytes before the syscall got
/// a chance to say no.
/// 容量限制可避免先分配稀疏 `-1` 向量再遭内核拒绝时，为未经检查的 `u32` 请求占用数 GB 内存。
const MAX_FILE_TABLE_CAPACITY: usize = 1 << 20;

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
        FileTablePoisonContext::new(
            self.report.scope,
            failed_index,
            self.report.start_index,
            self.report.requested_files,
        )
        .with_updated_files(self.evidence.updated())
    }
}

struct FileTableRollbackFailure {
    failed_index: u32,
    failure: FileTableUpdateFailure,
    remaining_indices: Vec<u32>,
}

struct FileTablePoisonDetails {
    primary: Report<UringError>,
    secondary: Option<Report<UringError>>,
    rollback_failure: bool,
    remaining_slots: usize,
}

struct FailedFileBatchUpdate {
    cursor: usize,
    run_end: usize,
    failure: FileTableUpdateFailure,
}

impl FixedFileOwner {
    pub(super) fn new(capacity: u32, exhaustion: FileTableExhaustion) -> Self {
        Self {
            table: FileTable::new(capacity, exhaustion),
            registration: None,
            #[cfg(feature = "test-hooks")]
            update_outcomes: VecDeque::new(),
        }
    }

    pub(super) fn table(&self) -> &FileTable {
        &self.table
    }
    pub(super) fn is_poisoned(&self) -> bool {
        self.table.is_poisoned()
    }
    pub(super) fn poisoned_report(
        &self,
        scope: &'static str,
        fd: Option<IoFd>,
    ) -> Report<UringError> {
        self.table.poisoned_report(scope, fd)
    }

    #[cfg(feature = "test-hooks")]
    pub(super) fn inject_update_failure(&mut self, errno: i32) {
        self.update_outcomes
            .push_back(RegisterFilesUpdateOutcome::Error(errno));
    }

    #[cfg(feature = "test-hooks")]
    pub(super) fn inject_update_sequence(&mut self, outcomes: &[RegisterFilesUpdateOutcome]) {
        self.update_outcomes.clear();
        self.update_outcomes.extend(outcomes.iter().copied());
    }

    #[cfg(feature = "test-hooks")]
    pub(super) fn pending_update_outcomes(&self) -> usize {
        self.update_outcomes.len()
    }
}

impl FixedFileOwner {
    /// Runs one `register_files_update` call and retains the evidence needed by rollback. 执行一次 `register_files_update` 并保留回滚所需的结果证据。
    fn register_files_update(
        &mut self,
        ports: &RegistrationPorts<'_, '_>,
        start: u32,
        files: &[i32],
    ) -> (io::Result<usize>, KernelUpdateOutcome) {
        #[cfg(feature = "test-hooks")]
        if let Some(outcome) = self.update_outcomes.pop_front() {
            return match outcome {
                RegisterFilesUpdateOutcome::Actual => {
                    let result = self
                        .registration
                        .as_ref()
                        .ok_or_else(|| io::Error::from_raw_os_error(libc::EINVAL))
                        .and_then(|registration| {
                            ports
                                .submitter
                                .register_files_update(registration, start, files)
                        });
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

        let result = self
            .registration
            .as_ref()
            .ok_or_else(|| io::Error::from_raw_os_error(libc::EINVAL))
            .and_then(|registration| {
                ports
                    .submitter
                    .register_files_update(registration, start, files)
            });
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
        ports: &RegistrationPorts<'_, '_>,
        evidence: UpdateEvidence,
        txn: &FileUpdateTxn,
    ) {
        match evidence.outcome() {
            KernelUpdateOutcome::Applied(_) => ports.diagnostics.inc_file_table_update_applied(),
            #[cfg(feature = "test-hooks")]
            KernelUpdateOutcome::Rejected(_) => ports.diagnostics.inc_file_table_update_rejected(),
            KernelUpdateOutcome::Unknown(_, _) => ports.diagnostics.inc_file_table_update_unknown(),
        }
        tracing::debug!(
            scope = txn.scope(),
            direction = ?txn.direction(),
            start_index = txn.start(),
            requested_files = evidence.requested(),
            updated_files = ?evidence.updated(),
            evidence = ?evidence.outcome(),
            "recorded kernel file table update evidence"
        );
    }

    fn update_kernel_file_entries(
        &mut self,
        ports: &RegistrationPorts<'_, '_>,
        txn: &FileUpdateTxn,
    ) -> Result<(), FileTableUpdateFailure> {
        let requested_files = txn.fds().len();
        let (result, outcome) = self.register_files_update(ports, txn.start(), txn.fds());
        let evidence = UpdateEvidence::new(requested_files, outcome);
        self.record_file_update_outcome(ports, evidence, txn);
        let updated_files = result.map_err(|error| {
            let cleanup_errno = error.raw_os_error();
            let report = UringError::Registration
                .io_report(txn.scope(), error)
                .with_ctx("start_index", txn.start())
                .with_ctx("requested_files", requested_files);
            FileTableUpdateFailure {
                report: FileTableUpdateReport {
                    cleanup_errno,
                    report,
                    scope: txn.scope(),
                    start_index: txn.start(),
                    requested_files,
                },
                evidence,
            }
        })?;
        if updated_files != txn.fds().len() {
            return Err(FileTableUpdateFailure {
                report: FileTableUpdateReport {
                    report: UringError::Registration
                        .to_report()
                        .push_ctx("scope", txn.scope())
                        .with_ctx("start_index", txn.start())
                        .with_ctx("requested_files", txn.fds().len())
                        .with_ctx("updated_files", updated_files)
                        .attach_note(
                            "io_uring updated fewer registered file entries than requested",
                        ),
                    cleanup_errno: None,
                    scope: txn.scope(),
                    start_index: txn.start(),
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
    /// 清除内核表项 `idx`；未能放入固定表的描述符走 direct 路径，不会到达此处。
    fn clear_kernel_file_entry(
        &mut self,
        ports: &RegistrationPorts<'_, '_>,
        idx: u32,
        scope: &'static str,
    ) -> Result<(), FileTableUpdateFailure> {
        let txn = FileUpdateTxn::clear(idx, 1, scope);
        self.update_kernel_file_entries(ports, &txn)
    }

    fn clear_kernel_file_entries(
        &mut self,
        ports: &RegistrationPorts<'_, '_>,
        start: u32,
        count: usize,
        scope: &'static str,
    ) -> Result<(), FileTableUpdateFailure> {
        let txn = FileUpdateTxn::clear(start, count, scope);
        self.update_kernel_file_entries(ports, &txn)
    }

    fn poison_file_table(
        &mut self,
        ports: &RegistrationPorts<'_, '_>,
        context: FileTablePoisonContext,
        details: FileTablePoisonDetails,
    ) -> Report<UringError> {
        let FileTablePoisonDetails {
            primary,
            secondary,
            rollback_failure,
            remaining_slots,
        } = details;
        let scope = context.scope();
        let transitioned = self.table.poison(context);
        if rollback_failure {
            ports.diagnostics.inc_file_table_rollback_failure();
        }
        if transitioned {
            ports.diagnostics.inc_file_table_poisoning();
        }

        error!(
            scope,
            file_index = ?context.failed_index(),
            start_index = context.start_index(),
            requested_files = context.requested_files(),
            updated_files = ?context.updated_files(),
            remaining_slots,
            original_error = ?primary,
            rollback_error = ?secondary,
            "registered file table entered poisoned state"
        );

        let mut report = self
            .table
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
        ports: &RegistrationPorts<'_, '_>,
        idx: u32,
        advance_generation: bool,
        scope: &'static str,
    ) -> Result<(), FileTableUpdateFailure> {
        let Some(entry) = self.table.take_entry(idx) else {
            return Ok(());
        };

        if let Err(failure) = self.clear_kernel_file_entry(ports, idx, scope) {
            self.table.install_entry(idx, entry);
            return Err(failure);
        }

        self.table.release(idx);
        if advance_generation {
            self.table.advance_generation(idx);
        }
        Ok(())
    }

    fn rollback_file_slots(
        &mut self,
        ports: &RegistrationPorts<'_, '_>,
        registered: &mut Vec<u32>,
    ) -> Result<(), FileTableRollbackFailure> {
        while let Some(idx) = registered.pop() {
            if let Err(failure) = self.unregister_file_slot(
                ports,
                idx,
                false,
                "driver.register_files_internal.rollback",
            ) {
                return Err(FileTableRollbackFailure {
                    failed_index: idx,
                    failure,
                    // `unregister_file_slot` restores `idx` when its clear update fails, so the
                    // failed slot is retained alongside the entries still waiting for rollback.
                    // 清空更新失败时 `unregister_file_slot` 会恢复 `idx`，因此失败槽位须与待回滚条目一起保留。
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
        ports: &RegistrationPorts<'_, '_>,
        batch: &mut FileRegistrationBatch<'_>,
    ) -> Result<(), FileTableRollbackFailure> {
        let mut registered = batch.committed_indices();
        let result = self.rollback_file_slots(ports, &mut registered);
        match &result {
            Ok(()) => batch.mark_rolled_back(&[]),
            Err(rollback) => batch.mark_rolled_back(&rollback.remaining_indices),
        }
        result
    }

    fn abort_file_batch(
        &mut self,
        ports: &RegistrationPorts<'_, '_>,
        batch: &mut FileRegistrationBatch<'_>,
        primary: Report<UringError>,
        note: &'static str,
    ) -> Report<UringError> {
        batch.release_reserved(&mut self.table);
        for descriptor in batch.take_direct_descriptors() {
            if matches!(descriptor, IoFd::OwnedDirect { .. }) {
                drop(self.table.release_direct(descriptor));
            }
        }

        match self.rollback_committed_batch(ports, batch) {
            Ok(()) => primary,
            Err(rollback) => {
                let context = rollback.failure.poison_context(Some(rollback.failed_index));
                let remaining_slots = batch.retained_slot_count();
                self.poison_file_table(
                    ports,
                    context,
                    FileTablePoisonDetails {
                        primary,
                        secondary: Some(rollback.failure.report.report),
                        rollback_failure: true,
                        remaining_slots,
                    },
                )
                .attach_note(note)
            }
        }
    }

    fn abort_file_batch_after_update_failure(
        &mut self,
        ports: &RegistrationPorts<'_, '_>,
        batch: &mut FileRegistrationBatch<'_>,
        update: FailedFileBatchUpdate,
    ) -> Report<UringError> {
        let FailedFileBatchUpdate {
            cursor,
            run_end,
            failure,
        } = update;
        let failed_index = batch.fixed_index(cursor);
        let context = failure.poison_context(Some(failed_index));
        let evidence = failure.evidence;
        let primary = failure.report.report;
        let run_len = run_end - cursor;

        match evidence.outcome() {
            KernelUpdateOutcome::Applied(updated) => {
                let touched = updated.min(run_len);
                if touched == 0 {
                    batch.release_range(&mut self.table, cursor, run_end);
                    return self.abort_file_batch(
                        ports,
                        batch,
                        primary,
                        "registration update applied no entries before abort",
                    );
                }

                match self.clear_kernel_file_entries(
                    ports,
                    failed_index,
                    touched,
                    "driver.register_files_internal.abort.clear_prefix",
                ) {
                    Ok(()) => {
                        batch.release_range(&mut self.table, cursor, cursor + touched);
                        batch.release_range(&mut self.table, cursor + touched, run_end);
                        self.abort_file_batch(
                            ports,
                            batch,
                            primary,
                            "registration update was rolled back after a partial apply",
                        )
                    }
                    Err(clear_failure) => {
                        let clear_evidence = clear_failure.evidence;
                        let cleared = match clear_evidence.outcome() {
                            KernelUpdateOutcome::Applied(updated) => updated.min(touched),
                            #[cfg(feature = "test-hooks")]
                            KernelUpdateOutcome::Rejected(_) => 0,
                            KernelUpdateOutcome::Unknown(_, _) => 0,
                        };
                        batch.release_range(&mut self.table, cursor, cursor + cleared);
                        batch.retain_range(&mut self.table, cursor + cleared, cursor + touched);
                        batch.release_range(&mut self.table, cursor + touched, run_end);
                        batch.release_reserved(&mut self.table);

                        self.poison_file_table(
                            ports,
                            context,
                            FileTablePoisonDetails {
                                primary,
                                secondary: Some(clear_failure.report.report),
                                rollback_failure: true,
                                remaining_slots: batch.retained_slot_count(),
                            },
                        )
                        .attach_note("partial registration apply could not be fully cleared")
                    }
                }
            }
            #[cfg(feature = "test-hooks")]
            KernelUpdateOutcome::Rejected(_) => {
                batch.release_range(&mut self.table, cursor, run_end);
                self.abort_file_batch(
                    ports,
                    batch,
                    primary,
                    "registration update was rejected before kernel state changed",
                )
            }
            KernelUpdateOutcome::Unknown(_, _) => {
                batch.retain_range(&mut self.table, cursor, run_end);
                batch.release_range(&mut self.table, run_end, batch.fixed_len());
                batch.release_reserved(&mut self.table);
                self.poison_file_table(
                    ports,
                    context,
                    FileTablePoisonDetails {
                        primary,
                        secondary: None,
                        rollback_failure: false,
                        remaining_slots: batch.retained_slot_count(),
                    },
                )
                .attach_note(
                    "registration update result was unknown; all touched file owners were retained",
                )
            }
        }
    }

    pub(super) fn unregister_fixed_fd(
        &mut self,
        ports: &RegistrationPorts<'_, '_>,
        fd: IoFd,
    ) -> UringResult<()> {
        match fd {
            // Borrowed direct descriptors have no backend ownership to release. 借用型 direct descriptor 不归后端所有，因此无需释放后端资源。
            IoFd::Direct(_) => Ok(()),
            IoFd::OwnedDirect { .. } => {
                drop(self.table.release_direct(fd));
                Ok(())
            }
            IoFd::Registered { index, generation } => {
                if self.table.is_poisoned() {
                    return Err(self
                        .table
                        .poisoned_report("driver.unregister_fixed_fd", Some(fd)));
                }
                if !self.table.is_initialized() || !self.table.matches_generation(index, generation)
                {
                    return Ok(());
                }
                match self.unregister_file_slot(ports, index, true, "driver.unregister_fixed_fd") {
                    Ok(()) => Ok(()),
                    Err(failure) => {
                        let context = failure.poison_context(Some(index));
                        Err(self.poison_file_table(
                            ports,
                            context,
                            FileTablePoisonDetails {
                                primary: failure.report.report,
                                secondary: None,
                                rollback_failure: false,
                                remaining_slots: 0,
                            },
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
    /// 内核已关闭 descriptor 时须遗忘 owned handle，不能 drop 它，否则可能关闭已被内核复用的 fd 数值。
    pub(super) fn unregister_close_owned_fd(
        &mut self,
        ports: &RegistrationPorts<'_, '_>,
        fd: IoFd,
    ) -> UringResult<()> {
        let (index, generation) = match fd {
            IoFd::Direct(_) => {
                return Ok(());
            }
            IoFd::OwnedDirect { .. } => {
                if let Some(handle) = self.table.release_direct(fd) {
                    let _ = ManuallyDrop::new(handle);
                }
                return Ok(());
            }
            IoFd::Registered { index, generation } => (index, generation),
        };

        if !self.table.is_initialized() || !self.table.matches_generation(index, generation) {
            return Ok(());
        }
        let Some(entry) = self.table.take_entry(index) else {
            return Ok(());
        };
        // The kernel has already consumed this descriptor. Keep the Rust ownership object
        // unreachable from every drop path regardless of whether clearing the fixed slot works.
        // 内核已消费该 descriptor；无论固定槽位清理是否成功，都要确保 Rust 所有权对象不会进入任何 drop 路径。
        let _entry = ManuallyDrop::new(entry);
        if self.table.is_poisoned() {
            self.table.advance_generation(index);
            return Err(self
                .table
                .poisoned_report("driver.unregister_close_owned_fd", Some(fd))
                .attach_note("closed owned fd was forgotten; poisoned table skipped clear"));
        }
        if let Err(failure) =
            self.clear_kernel_file_entry(ports, index, "driver.unregister_close_owned_fd")
        {
            self.table.quarantine(index);
            self.table.advance_generation(index);
            ports.diagnostics.inc_file_table_cleanup_failure();
            ports.diagnostics.inc_file_table_quarantine();
            if failure.evidence.updated().is_some() {
                ports.diagnostics.inc_file_table_cleanup_short_update();
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
        self.table.release(index);
        self.table.advance_generation(index);
        Ok(())
    }

    /// Points an existing registered slot at a different handle, keeping its descriptor valid.
    ///
    /// Only the waker uses this, to survive an eventfd rebuild without invalidating the
    /// descriptor it already registered. A direct descriptor cannot be replaced in place —
    /// the fd *is* the descriptor — so its caller mints a new one instead.
    /// 仅 waker 会替换已注册槽位中的句柄以保留原描述符；direct descriptor 的 fd 本身就是 descriptor，必须新建。
    pub(super) fn replace_registered_fixed_fd(
        &mut self,
        ports: &RegistrationPorts<'_, '_>,
        fixed_fd: IoFd,
        raw: RawHandle,
    ) -> UringResult<()> {
        let scope = "driver.replace_registered_fixed_fd";
        if self.table.is_poisoned() {
            return Err(self.table.poisoned_report(scope, Some(fixed_fd)));
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

        if !self.table.is_initialized() {
            return invalid("registered file table is not initialized");
        }
        if self.table.generation(index).is_none() {
            return invalid("registered file index out of bounds");
        }
        if !self.table.matches_generation(index, generation) {
            return invalid("registered file generation mismatch while replacing fd");
        }
        if self.table.entry(index).is_none() {
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
        let update = self.update_kernel_file_entries(ports, &txn);
        if let Err(failure) = update {
            let context = failure.poison_context(Some(index));
            return Err(self.poison_file_table(
                ports,
                context,
                FileTablePoisonDetails {
                    primary: failure.report.report,
                    secondary: None,
                    rollback_failure: false,
                    remaining_slots: 0,
                },
            ));
        }
        let old_entry = self.table.replace_entry(index, new_entry);
        drop(old_entry);
        Ok(())
    }

    pub(super) fn ensure_file_table_initialized(
        &mut self,
        submitter: &Submitter<'_>,
        kernel_capabilities: &mut KernelCapabilities,
    ) -> UringResult<()> {
        if self.table.is_poisoned() {
            return Err(self
                .table
                .poisoned_report("driver.ensure_file_table_initialized", None));
        }
        if self.table.is_initialized() {
            return Ok(());
        }

        let capacity = self.table.fixed_capacity();
        if capacity > MAX_FILE_TABLE_CAPACITY {
            return UringError::InvalidInput
                .push_ctx("scope", "driver.ensure_file_table_initialized")
                .with_ctx("file_table_capacity", capacity)
                .with_ctx("max_file_table_capacity", MAX_FILE_TABLE_CAPACITY)
                .attach_note("configured registered file table capacity is too large");
        }
        if capacity > 0 {
            match submitter.register_files_sparse(capacity) {
                Ok(registration) => {
                    *kernel_capabilities =
                        kernel_capabilities.with_file_registration(&registration);
                    self.registration = Some(registration);
                }
                Err(error) => {
                    *kernel_capabilities = kernel_capabilities.with_file_registration_error(
                        capacity as u32,
                        ResourceLayout::Sparse,
                        false,
                        error.raw_os_error(),
                    );
                    if self.table.falls_back_when_unavailable() {
                        self.table.disable_fixed_table();
                        tracing::warn!(
                            requested_slots = capacity,
                            errno = ?error.raw_os_error(),
                            "sparse fixed-file registration unavailable; using raw file descriptors"
                        );
                    } else {
                        return Err(UringError::Registration
                            .io_report("driver.ensure_file_table_initialized", error)
                            .attach_note("sparse fixed-file registration is unavailable"));
                    }
                }
            }
        }

        self.table.mark_initialized();
        Ok(())
    }

    /// Registers `files`, taking kernel table slots while they last.
    ///
    /// The first `claimed.len()` inputs get [`IoFd::Registered`] descriptors; anything past
    /// that is handed out as [`IoFd::Direct`], which needs no kernel round trip at all — the
    /// raw fd travels in the SQE. The returned vector is in input order either way.
    /// 先占用固定槽位，剩余输入作为 direct descriptor 并将 raw fd 放入 SQE；结果始终保持输入顺序。
    pub(super) fn register_files_internal<
        'h,
        'submitter,
        'ring,
        'diagnostics,
        'capabilities,
        'stats,
    >(
        &mut self,
        request: FileRegistrationRequest<
            'submitter,
            'ring,
            'diagnostics,
            'capabilities,
            'h,
            'stats,
        >,
    ) -> UringResult<Vec<IoFd>> {
        let FileRegistrationRequest {
            submitter,
            diagnostics,
            kernel_capabilities,
            files,
            stats,
        } = request;
        if files.is_empty() {
            return Ok(Vec::new());
        }

        let files = FileRegistrationBatch::preflight_owned_files(&self.table, files)?;

        if self.table.is_poisoned() {
            return Err(self
                .table
                .poisoned_report("driver.register_files_internal", None));
        }

        self.ensure_file_table_initialized(submitter, kernel_capabilities)?;

        let ports = RegistrationPorts::new(submitter, diagnostics);

        let claimed = self
            .table
            .claim(files.len())
            .push_ctx("scope", "driver.register_files_internal")?;
        let fallback = files.len() - claimed.len();
        stats.inc_file_table_fallback_registrations(fallback);

        let mut batch = FileRegistrationBatch::new(claimed, files);
        let fixed_indices = batch.fixed_indices();
        let mut cursor = 0usize;
        while cursor < batch.fixed_len() {
            let run_end = FileRegistrationBatch::consecutive_run_end(&fixed_indices, cursor);
            let txn = batch.registration_txn(cursor, run_end);
            if let Err(failure) = self.update_kernel_file_entries(&ports, &txn) {
                return Err(self.abort_file_batch_after_update_failure(
                    &ports,
                    &mut batch,
                    FailedFileBatchUpdate {
                        cursor,
                        run_end,
                        failure,
                    },
                ));
            }
            batch.commit_range(&mut self.table, cursor, run_end);
            cursor = run_end;
        }

        for file in batch.take_direct_files() {
            let descriptor = match file {
                RegisterFd::Borrowed(b) => IoFd::direct(b.raw()),
                // A direct descriptor is only a handle value, so the driver has to keep the
                // owned handle alive itself until the descriptor is unregistered.
                // direct descriptor 只携带句柄值，因此驱动须自行持有 owned handle，直到 descriptor 注销。
                RegisterFd::Owned(o) => match self.table.adopt_direct(o) {
                    Ok(fd) => fd,
                    Err(report) => {
                        return Err(self.abort_file_batch(
                            &ports,
                            &mut batch,
                            report,
                            "rollback failed after owned direct adoption failure",
                        ));
                    }
                },
            };
            batch.push_direct_descriptor(descriptor);
        }
        Ok(batch.descriptors(&self.table))
    }
}
