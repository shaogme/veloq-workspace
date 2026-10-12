use veloq_std::{collections::HashMap, format, mem, vec, vec::Vec};

use diagweave::prelude::*;
use veloq_driver_core::driver::RegisterFd;
use veloq_io_uring::{KernelCapabilities, Submitter};

use crate::{
    config::{IoFd, UringRawHandle},
    diagnostics::UringCompletionDiagnostics,
    driver::registration::{FileTable, OwnedLocation, RegisteredFileEntry, UringRegistrationStats},
    error::UringError,
};

pub(crate) struct FileRegistrationRequest<
    'submitter,
    'ring,
    'diagnostics,
    'capabilities,
    'h,
    'stats,
> {
    pub(crate) submitter: &'submitter Submitter<'ring>,
    pub(crate) diagnostics: &'diagnostics UringCompletionDiagnostics,
    pub(crate) kernel_capabilities: &'capabilities mut KernelCapabilities,
    pub(crate) files: Vec<RegisterFd<'h, UringRawHandle>>,
    pub(crate) stats: &'stats mut UringRegistrationStats,
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
pub(crate) enum FileUpdateDirection {
    Register,
    Clear,
}

/// One kernel file-table update, kept separate from the resource-state commit.
///
/// The transaction owns only the syscall input. The [`FileRegistrationBatch`] owns the entries
/// and decides whether they can be committed after the kernel outcome is known.
/// 事务只拥有系统调用输入；批次 owner 持有条目，并在获得内核结果后决定是否提交。
pub(crate) struct FileUpdateTxn {
    start: u32,
    fds: Vec<i32>,
    direction: FileUpdateDirection,
    scope: &'static str,
}

impl FileUpdateTxn {
    pub(super) fn register(start: u32, fds: Vec<i32>, scope: &'static str) -> Self {
        Self {
            start,
            fds,
            direction: FileUpdateDirection::Register,
            scope,
        }
    }

    pub(super) fn clear(start: u32, count: usize, scope: &'static str) -> Self {
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
/// 批次 owner 保留所有输入直到整体提交或显式中止；固定条目须等完整更新成功，直接回退输入也在批次结束前保留。
pub(crate) struct FileRegistrationBatch<'h> {
    fixed: Vec<FileLease>,
    direct_files: Vec<RegisterFd<'h, UringRawHandle>>,
    direct_descriptors: Vec<IoFd>,
}

impl<'h> FileRegistrationBatch<'h> {
    pub(super) fn new(claimed: Vec<u32>, files: Vec<RegisterFd<'h, UringRawHandle>>) -> Self {
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

    pub(super) fn fixed_len(&self) -> usize {
        self.fixed.len()
    }

    pub(super) fn fixed_indices(&self) -> Vec<u32> {
        self.fixed.iter().map(|lease| lease.index).collect()
    }

    pub(super) fn registration_txn(&self, start: usize, end: usize) -> FileUpdateTxn {
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

    pub(super) fn commit_range(&mut self, table: &mut FileTable, start: usize, end: usize) {
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

    pub(super) fn retain_range(&mut self, table: &mut FileTable, start: usize, end: usize) {
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

    pub(super) fn release_range(&mut self, table: &mut FileTable, start: usize, end: usize) {
        for lease in &mut self.fixed[start..end] {
            debug_assert_eq!(lease.state, FileLeaseState::Reserved);
            table.release(lease.index);
            lease.state = FileLeaseState::Released;
        }
    }

    pub(super) fn release_reserved(&mut self, table: &mut FileTable) {
        for lease in &mut self.fixed {
            if lease.state == FileLeaseState::Reserved {
                table.release(lease.index);
                lease.state = FileLeaseState::Released;
            }
        }
    }

    pub(super) fn committed_indices(&self) -> Vec<u32> {
        self.fixed
            .iter()
            .filter(|lease| lease.state == FileLeaseState::Committed)
            .map(|lease| lease.index)
            .collect()
    }

    pub(super) fn mark_rolled_back(&mut self, retained: &[u32]) {
        for lease in &mut self.fixed {
            if lease.state == FileLeaseState::Committed && !retained.contains(&lease.index) {
                lease.state = FileLeaseState::Released;
            }
        }
    }

    pub(super) fn retained_slot_count(&self) -> usize {
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

    pub(super) fn fixed_index(&self, position: usize) -> u32 {
        self.fixed[position].index
    }

    pub(super) fn take_direct_descriptors(&mut self) -> Vec<IoFd> {
        mem::take(&mut self.direct_descriptors)
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

    pub(super) fn take_direct_files(&mut self) -> Vec<RegisterFd<'h, UringRawHandle>> {
        mem::take(&mut self.direct_files)
    }

    pub(crate) fn push_direct_descriptor(&mut self, descriptor: IoFd) {
        self.direct_descriptors.push(descriptor);
    }

    pub(super) fn descriptors(&self, table: &FileTable) -> Vec<IoFd> {
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
                    // The existing registration remains the sole actual owner of this raw fd. 现有注册仍是此 raw fd 的唯一实际所有者。
                    mem::forget(handle);
                } else if info.count > 1 {
                    // This malformed batch contains several owners for one raw fd. Close one
                    // wrapper and forget the rest so the number is closed at most once. 异常批次为同一 raw fd 包含多个所有者；只关闭一个封装并遗忘其余封装，避免重复关闭。
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

impl FileUpdateTxn {
    pub(super) const fn start(&self) -> u32 {
        self.start
    }
    pub(super) fn fds(&self) -> &[i32] {
        &self.fds
    }
    pub(super) const fn direction(&self) -> FileUpdateDirection {
        self.direction
    }
    pub(super) const fn scope(&self) -> &'static str {
        self.scope
    }
}

impl<'h> FileRegistrationBatch<'h> {
    pub(super) fn preflight_owned_files(
        table: &FileTable,
        files: Vec<RegisterFd<'h, UringRawHandle>>,
    ) -> Result<Vec<RegisterFd<'h, UringRawHandle>>, Report<UringError>> {
        let (inputs, conflict) = preflight_owned_files(table, &files);
        if let Some(conflict) = conflict {
            let report = duplicate_owned_fd_report(conflict);
            cleanup_rejected_owned_inputs(files, &inputs);
            return Err(report);
        }
        Ok(files)
    }

    pub(super) fn consecutive_run_end(slots: &[u32], start: usize) -> usize {
        let mut end = start + 1;
        while end < slots.len() && slots[end] == slots[end - 1] + 1 {
            end += 1;
        }
        end
    }
}
