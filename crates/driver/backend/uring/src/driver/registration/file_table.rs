//! Bookkeeping for the descriptors a [`UringDriver`](crate::driver::UringDriver) hands out.
//!
//! The kernel's registered file table is a fixed-size allocation sized by
//! [`UringConfig::file_table_capacity`](crate::config::UringConfig::file_table_capacity). This
//! table is the userspace mirror of it, one slot per kernel entry, and a descriptor pointing
//! into it is an [`IoFd::Registered`] — an index plus the generation it was handed out under.
//!
//! Descriptors that do not fit are handed out as [`IoFd::Direct`] instead, which carries the
//! raw fd inside the descriptor itself: submitting one consults no table at all, and its
//! [`RawHandleKind`] comes from the handle rather than from a slot. What the table still owes
//! them is *ownership* — a descriptor registered from [`RegisterFd::Owned`] must stay open
//! until it is unregistered — so owned fallback handles are parked in `direct_owned`, which is
//! touched only on registration and unregistration, never on submission.
//!
//! The trade this makes is explicit: a direct descriptor has no generation, so it cannot
//! detect use-after-close the way a registered one does. See [`FileTable::resolve`].
//!
//! [`RegisterFd::Owned`]: veloq_driver_core::driver::RegisterFd::Owned

use crate::{
    config::{FileTableExhaustion, IoFd, OwnedRawHandle, RawHandleKind, UringRawHandle},
    error::{UringError, UringResult},
};
use diagweave::prelude::*;
use tracing::warn;
use veloq_driver_core::RawHandleMeta;
use veloq_std::{collections::HashMap, format, string::ToString, vec::Vec};

const INITIAL_FILE_GENERATION: u64 = 1;

/// How a resolved descriptor is spelled in an SQE.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SqeFd {
    /// An index into the kernel's registered file table; the SQE sets `IOSQE_FIXED_FILE`.
    Fixed(u32),
    /// A raw descriptor, submitted without the registered-file fast path.
    Direct(i32),
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FileSlotState {
    Vacant,
    Occupied,
    Quarantined,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct FileTablePoisonContext {
    pub(crate) scope: &'static str,
    pub(crate) failed_index: Option<u32>,
    pub(crate) start_index: u32,
    pub(crate) requested_files: usize,
    pub(crate) updated_files: Option<usize>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FileTableHealth {
    Healthy,
    Poisoned(FileTablePoisonContext),
}

#[derive(Debug)]
struct FileSlot {
    entry: Option<RegisteredFileEntry>,
    generation: u64,
    state: FileSlotState,
}

impl FileSlot {
    #[inline]
    const fn vacant() -> Self {
        Self {
            entry: None,
            generation: INITIAL_FILE_GENERATION,
            state: FileSlotState::Vacant,
        }
    }
}

pub(crate) struct FileTable {
    /// One slot per kernel table entry; `slots.len() == fixed_capacity` once initialized.
    slots: Vec<FileSlot>,
    fixed_capacity: usize,
    free_fixed: Vec<u32>,
    /// Handles owned by the driver behind an [`IoFd::Direct`], keyed by raw fd.
    ///
    /// Only [`RegisterFd::Owned`](veloq_driver_core::driver::RegisterFd::Owned) registrations
    /// land here — a borrowed fallback descriptor is nothing but the number in the `IoFd`.
    direct_owned: HashMap<i32, OwnedRawHandle>,
    exhaustion: FileTableExhaustion,
    initialized: bool,
    fallback_reported: bool,
    health: FileTableHealth,
}

impl FileTable {
    pub(crate) fn new(fixed_capacity: u32, exhaustion: FileTableExhaustion) -> Self {
        Self {
            slots: Vec::new(),
            fixed_capacity: fixed_capacity as usize,
            free_fixed: Vec::new(),
            direct_owned: HashMap::default(),
            exhaustion,
            initialized: false,
            fallback_reported: false,
            health: FileTableHealth::Healthy,
        }
    }

    #[inline]
    pub(crate) const fn is_initialized(&self) -> bool {
        self.initialized
    }

    #[inline]
    pub(crate) const fn fixed_capacity(&self) -> usize {
        self.fixed_capacity
    }

    #[inline]
    pub(crate) const fn is_poisoned(&self) -> bool {
        matches!(self.health, FileTableHealth::Poisoned(_))
    }

    /// Permanently marks the registered-file table as unusable.
    ///
    /// Existing entries are deliberately retained. The ring must be destroyed before those
    /// entries are dropped, because the kernel may still refer to an unknown table value.
    pub(crate) fn poison(&mut self, context: FileTablePoisonContext) -> bool {
        if self.is_poisoned() {
            return false;
        }
        self.free_fixed.clear();
        self.health = FileTableHealth::Poisoned(context);
        true
    }

    #[inline]
    pub(crate) fn poison_context(&self) -> Option<FileTablePoisonContext> {
        match self.health {
            FileTableHealth::Healthy => None,
            FileTableHealth::Poisoned(context) => Some(context),
        }
    }

    pub(crate) fn poisoned_report(
        &self,
        scope: &'static str,
        fd: Option<IoFd>,
    ) -> Report<UringError> {
        let Some(context) = self.poison_context() else {
            return UringError::InvalidState.report(
                scope,
                "file table poison report requested while table is healthy",
            );
        };

        let mut report = UringError::FileTablePoisoned
            .to_report()
            .push_ctx("scope", scope)
            .with_ctx("poison_scope", context.scope)
            .with_ctx("start_index", context.start_index)
            .with_ctx("requested_files", context.requested_files)
            .attach_note("registered file table is unrecoverable; recreate the io_uring driver");
        if let Some(index) = context.failed_index {
            report = report.with_ctx("file_index", index);
        }
        if let Some(updated_files) = context.updated_files {
            report = report.with_ctx("updated_files", updated_files);
        }
        if let Some(fd) = fd {
            report = report.with_ctx("fd", fd.to_string());
        }
        report
    }

    /// Seeds the userspace mirror once the kernel table exists.
    ///
    /// Callers must have registered a sparse table of `fixed_capacity` entries first (or have
    /// nothing to register, when the capacity is zero).
    pub(crate) fn mark_initialized(&mut self) {
        debug_assert!(!self.initialized, "file table initialized twice");
        self.slots = (0..self.fixed_capacity)
            .map(|_| FileSlot::vacant())
            .collect();
        self.free_fixed = (0..self.fixed_capacity as u32).rev().collect();
        self.initialized = true;
    }

    #[inline]
    pub(crate) fn entry(&self, index: u32) -> Option<&RegisteredFileEntry> {
        self.slots.get(index as usize)?.entry.as_ref()
    }

    #[inline]
    pub(crate) fn generation(&self, index: u32) -> Option<u64> {
        Some(self.slots.get(index as usize)?.generation)
    }

    /// Whether a registered descriptor still names the registration it was handed out for.
    #[inline]
    pub(crate) fn matches_generation(&self, index: u32, generation: u64) -> bool {
        self.generation(index) == Some(generation)
    }

    /// Turns a user-facing descriptor into the form an SQE needs.
    ///
    /// The two variants are checked differently, and the difference is the whole point of the
    /// split. A registered descriptor is validated against its slot: bounds, generation
    /// (which is what makes a stale descriptor an error rather than a silent hit on whatever
    /// took the slot's place), and the registered handle's kind. A direct descriptor carries
    /// its handle, so the kind check reads straight off it and no lookup happens at all — but
    /// there is **no generation to check**, so a direct descriptor whose fd has since been
    /// closed resolves happily onto whatever the kernel has since assigned that number.
    pub(crate) fn resolve(
        &self,
        fd: IoFd,
        expected_kind: Option<RawHandleKind>,
        scope: &'static str,
    ) -> UringResult<SqeFd> {
        let (index, generation) = match fd {
            IoFd::Direct(raw) => {
                if let Some(expected_kind) = expected_kind {
                    let current_kind = raw.kind();
                    if current_kind != expected_kind {
                        return UringError::ResolveFd
                            .push_ctx("scope", scope)
                            .with_ctx("fd", fd.to_string())
                            .with_ctx("expected_kind", format!("{expected_kind:?}"))
                            .with_ctx("current_kind", format!("{current_kind:?}"))
                            .attach_note("direct file descriptor kind mismatch");
                    }
                }
                return Ok(SqeFd::Direct(raw.as_fd()));
            }
            IoFd::Registered { index, generation } => (index, generation),
        };

        if self.is_poisoned() {
            return Err(self.poisoned_report(scope, Some(fd)));
        }

        let Some(slot) = self.slots.get(index as usize) else {
            return UringError::ResolveFd
                .push_ctx("scope", scope)
                .with_ctx("fd", fd.to_string())
                .attach_note("registered file descriptor index out of bounds");
        };

        if slot.generation != generation {
            return UringError::ResolveFd
                .push_ctx("scope", scope)
                .with_ctx("fd", fd.to_string())
                .attach_note("stale registered file descriptor generation")
                .with_ctx("current_generation", slot.generation);
        }

        if self.is_quarantined(index) {
            return UringError::FileTableQuarantined
                .push_ctx("scope", scope)
                .with_ctx("fd", fd.to_string())
                .with_ctx("file_index", index)
                .with_ctx("generation", generation)
                .attach_note("registered file slot is quarantined and requires driver rebuild");
        }

        let Some(entry) = slot.entry.as_ref() else {
            return UringError::ResolveFd
                .push_ctx("scope", scope)
                .with_ctx("fd", fd.to_string())
                .attach_note("invalid registered file descriptor");
        };

        if let Some(expected_kind) = expected_kind {
            let current_kind = entry.kind();
            if current_kind != expected_kind {
                return UringError::ResolveFd
                    .push_ctx("scope", scope)
                    .with_ctx("fd", fd.to_string())
                    .with_ctx("expected_kind", format!("{expected_kind:?}"))
                    .with_ctx("current_kind", format!("{current_kind:?}"))
                    .attach_note("registered file descriptor kind mismatch");
            }
        }

        Ok(SqeFd::Fixed(index))
    }

    /// Resolves an `IoFd` into `SqeFd::Direct`, bypassing `IOSQE_FIXED_FILE` for multishot operations.
    pub(crate) fn resolve_direct(
        &self,
        fd: IoFd,
        expected_kind: Option<RawHandleKind>,
        scope: &'static str,
    ) -> UringResult<SqeFd> {
        let (index, generation) = match fd {
            IoFd::Direct(raw) => {
                if let Some(expected_kind) = expected_kind {
                    let current_kind = raw.kind();
                    if current_kind != expected_kind {
                        return UringError::ResolveFd
                            .push_ctx("scope", scope)
                            .with_ctx("fd", fd.to_string())
                            .with_ctx("expected_kind", format!("{expected_kind:?}"))
                            .with_ctx("current_kind", format!("{current_kind:?}"))
                            .attach_note("direct file descriptor kind mismatch");
                    }
                }
                return Ok(SqeFd::Direct(raw.as_fd()));
            }
            IoFd::Registered { index, generation } => (index, generation),
        };

        if self.is_poisoned() {
            return Err(self.poisoned_report(scope, Some(fd)));
        }

        let Some(slot) = self.slots.get(index as usize) else {
            return UringError::ResolveFd
                .push_ctx("scope", scope)
                .with_ctx("fd", fd.to_string())
                .attach_note("registered file descriptor index out of bounds");
        };

        if slot.generation != generation {
            return UringError::ResolveFd
                .push_ctx("scope", scope)
                .with_ctx("fd", fd.to_string())
                .attach_note("stale registered file descriptor generation")
                .with_ctx("current_generation", slot.generation);
        }

        if self.is_quarantined(index) {
            return UringError::FileTableQuarantined
                .push_ctx("scope", scope)
                .with_ctx("fd", fd.to_string())
                .with_ctx("file_index", index)
                .with_ctx("generation", generation)
                .attach_note("registered file slot is quarantined and requires driver rebuild");
        }

        let Some(entry) = slot.entry.as_ref() else {
            return UringError::ResolveFd
                .push_ctx("scope", scope)
                .with_ctx("fd", fd.to_string())
                .attach_note("invalid registered file descriptor");
        };

        if let Some(expected_kind) = expected_kind {
            let current_kind = entry.kind();
            if current_kind != expected_kind {
                return UringError::ResolveFd
                    .push_ctx("scope", scope)
                    .with_ctx("fd", fd.to_string())
                    .with_ctx("expected_kind", format!("{expected_kind:?}"))
                    .with_ctx("current_kind", format!("{current_kind:?}"))
                    .attach_note("registered file descriptor kind mismatch");
            }
        }

        Ok(SqeFd::Direct(entry.fd()))
    }

    /// Reserves up to `count` kernel table slots, ascending.
    ///
    /// Returns fewer than `count` when the table runs out; the caller hands the remainder out
    /// as direct descriptors. Nothing is written to the slots themselves — the caller fills
    /// them in with [`Self::install_entry`] once it knows the kernel accepted them.
    ///
    /// With [`FileTableExhaustion::Fail`] a short claim is an error instead, and nothing is
    /// consumed.
    pub(crate) fn claim(&mut self, count: usize) -> UringResult<Vec<u32>> {
        if self.is_poisoned() {
            return Err(self.poisoned_report("driver.file_table.claim", None));
        }

        let from_kernel_table = count.min(self.free_fixed.len());
        let overflow = count - from_kernel_table;

        if overflow > 0 && !self.exhaustion.falls_back() {
            return UringError::InvalidState
                .push_ctx("scope", "driver.file_table.claim")
                .with_ctx("requested_files", count)
                .with_ctx("free_file_slots", from_kernel_table)
                .with_ctx("file_table_capacity", self.fixed_capacity)
                .attach_note("io_uring registered file table exhausted");
        }

        let mut fixed = Vec::with_capacity(from_kernel_table);
        for _ in 0..from_kernel_table {
            fixed.push(
                self.free_fixed
                    .pop()
                    .expect("claim takes at most free_fixed.len() entries"),
            );
            let index = *fixed.last().expect("just-claimed slot exists");
            debug_assert_eq!(
                self.slots[index as usize].state,
                FileSlotState::Vacant,
                "free list contained a non-vacant file slot"
            );
            self.slots[index as usize].state = FileSlotState::Occupied;
        }
        // The free list is seeded in reverse, so a fresh table hands out consecutive indices;
        // sorting keeps that property visible to the batching in `register_files_internal`.
        fixed.sort_unstable();

        if overflow > 0 {
            self.report_fallback(overflow);
        }
        Ok(fixed)
    }

    fn report_fallback(&mut self, count: usize) {
        if self.fallback_reported {
            return;
        }
        self.fallback_reported = true;
        warn!(
            file_table_capacity = self.fixed_capacity,
            fallback_files = count,
            "io_uring registered file table is full; further descriptors submit as raw fds"
        );
    }

    /// Takes ownership of a handle handed out as a direct descriptor.
    ///
    /// The handle stays parked here until [`Self::release_direct`] retires it, which is what
    /// keeps a `RegisterFd::Owned` fallback registration open for as long as its descriptor
    /// is valid.
    pub(crate) fn adopt_direct(&mut self, handle: OwnedRawHandle) -> IoFd {
        let raw = handle.raw();
        self.direct_owned.insert(raw.as_fd(), handle);
        IoFd::direct(raw)
    }

    /// Whether the driver holds the handle behind a direct descriptor.
    #[inline]
    pub(crate) fn owns_direct(&self, raw: UringRawHandle) -> bool {
        self.direct_owned.contains_key(&raw.as_fd())
    }

    /// Retires a direct descriptor, returning the handle when the driver owned one.
    ///
    /// Dropping the returned handle closes the fd; callers whose descriptor was already closed
    /// by the kernel must forget it instead.
    pub(crate) fn release_direct(&mut self, raw: UringRawHandle) -> Option<OwnedRawHandle> {
        self.direct_owned.remove(&raw.as_fd())
    }

    /// Stores the handle backing `index`. The slot must have been claimed first.
    #[inline]
    pub(crate) fn install_entry(&mut self, index: u32, entry: RegisteredFileEntry) {
        debug_assert_eq!(
            self.slots[index as usize].state,
            FileSlotState::Occupied,
            "installed an entry into an unclaimed file slot"
        );
        debug_assert!(
            self.slots[index as usize].entry.is_none(),
            "installed an entry into an occupied file slot"
        );
        self.slots[index as usize].entry = Some(entry);
    }

    #[inline]
    pub(crate) fn take_entry(&mut self, index: u32) -> Option<RegisteredFileEntry> {
        self.slots.get_mut(index as usize)?.entry.take()
    }

    /// Returns `index` to the free list. The entry must already be gone.
    pub(crate) fn release(&mut self, index: u32) {
        if self.is_poisoned() {
            return;
        }
        debug_assert!(
            self.entry(index).is_none(),
            "released a file slot that still owns a handle"
        );
        debug_assert_eq!(
            self.slots[index as usize].state,
            FileSlotState::Occupied,
            "released a file slot that was not claimed"
        );
        self.slots[index as usize].state = FileSlotState::Vacant;
        self.free_fixed.push(index);
    }

    pub(crate) fn release_all(&mut self, indices: impl IntoIterator<Item = u32>) {
        for index in indices {
            self.release(index);
        }
    }

    /// Permanently removes a slot from the reusable fixed-file set.
    ///
    /// The caller must have taken the entry first. A quarantined slot deliberately remains out
    /// of `free_fixed` until the driver is destroyed, because the kernel may still contain an
    /// unknown value after a failed cleanup update.
    pub(crate) fn quarantine(&mut self, index: u32) {
        let slot = &mut self.slots[index as usize];
        debug_assert!(
            slot.entry.is_none(),
            "quarantined a file slot with an entry"
        );
        debug_assert_eq!(
            slot.state,
            FileSlotState::Occupied,
            "quarantined a file slot that was not claimed"
        );
        slot.state = FileSlotState::Quarantined;
    }

    #[inline]
    pub(crate) fn is_quarantined(&self, index: u32) -> bool {
        self.slots
            .get(index as usize)
            .is_some_and(|slot| slot.state == FileSlotState::Quarantined)
    }

    /// Invalidates every [`IoFd`] previously handed out for `index`.
    pub(crate) fn advance_generation(&mut self, index: u32) {
        let Some(slot) = self.slots.get_mut(index as usize) else {
            return;
        };
        slot.generation = slot.generation.wrapping_add(1);
        if slot.generation == 0 {
            slot.generation = INITIAL_FILE_GENERATION;
        }
    }

    /// The descriptor for `index`, valid until the slot is released.
    #[inline]
    pub(crate) fn descriptor(&self, index: u32) -> Option<IoFd> {
        if self.is_poisoned() {
            return None;
        }
        let slot = self.slots.get(index as usize)?;
        (slot.state == FileSlotState::Occupied && slot.entry.is_some())
            .then(|| IoFd::fixed_with_generation(index, slot.generation))
    }
}

#[cfg(test)]
mod tests {
    use super::{FileTable, FileTablePoisonContext, RegisteredFileEntry, SqeFd};
    use crate::config::{FileTableExhaustion, IoFd, RawHandleKind, UringRawHandle};
    use crate::error::UringError;
    use veloq_std::vec::Vec;

    fn borrowed(fd: i32) -> RegisteredFileEntry {
        RegisteredFileEntry::BorrowedFd {
            fd,
            kind: RawHandleKind::File,
        }
    }

    fn table(capacity: u32, exhaustion: FileTableExhaustion) -> FileTable {
        let mut table = FileTable::new(capacity, exhaustion);
        table.mark_initialized();
        table
    }

    /// Registers `fds` the way `register_files_internal` does: kernel slots first, the rest as
    /// direct descriptors.
    fn register(table: &mut FileTable, fds: &[i32]) -> Vec<IoFd> {
        let claimed = table.claim(fds.len()).expect("claim failed");
        let mut descriptors = Vec::with_capacity(fds.len());
        for (index, fd) in claimed.iter().copied().zip(fds.iter().copied()) {
            table.install_entry(index, borrowed(fd));
            descriptors.push(table.descriptor(index).expect("claimed slot exists"));
        }
        for fd in fds[claimed.len()..].iter().copied() {
            descriptors.push(IoFd::direct(UringRawHandle::for_file(fd)));
        }
        descriptors
    }

    #[test]
    fn slots_within_the_capacity_resolve_to_fixed_indices() {
        let mut table = table(4, FileTableExhaustion::Fallback);
        let fds = register(&mut table, &[10, 11]);

        assert_eq!(
            table.resolve(fds[0], None, "test").unwrap(),
            SqeFd::Fixed(0)
        );
        assert_eq!(
            table.resolve(fds[1], None, "test").unwrap(),
            SqeFd::Fixed(1)
        );
    }

    #[test]
    fn descriptors_past_the_capacity_carry_their_raw_fd() {
        let mut table = table(2, FileTableExhaustion::Fallback);
        let fds = register(&mut table, &[10, 11, 12, 13]);

        assert!(fds[1].is_registered());
        assert_eq!(
            table.resolve(fds[1], None, "test").unwrap(),
            SqeFd::Fixed(1)
        );
        assert!(fds[2].is_direct());
        assert_eq!(
            table.resolve(fds[2], None, "test").unwrap(),
            SqeFd::Direct(12)
        );
        assert_eq!(
            table.resolve(fds[3], None, "test").unwrap(),
            SqeFd::Direct(13)
        );
    }

    #[test]
    fn a_zero_capacity_table_submits_everything_as_raw_fds() {
        let mut table = table(0, FileTableExhaustion::Fallback);
        let fds = register(&mut table, &[7]);

        assert_eq!(
            table.resolve(fds[0], None, "test").unwrap(),
            SqeFd::Direct(7)
        );
    }

    #[test]
    fn a_direct_descriptor_resolves_without_consulting_the_table() {
        // Nothing was ever registered, yet the descriptor still submits: the fd is in it.
        let table = table(4, FileTableExhaustion::Fallback);
        let fd = IoFd::direct(UringRawHandle::for_file(9));

        assert_eq!(table.resolve(fd, None, "test").unwrap(), SqeFd::Direct(9));
    }

    #[test]
    fn a_direct_descriptor_is_kind_checked_against_its_own_handle() {
        let table = table(4, FileTableExhaustion::Fallback);
        let fd = IoFd::direct(UringRawHandle::for_file(9));

        assert!(
            table
                .resolve(fd, Some(RawHandleKind::Socket), "test")
                .is_err()
        );
        assert!(table.resolve(fd, Some(RawHandleKind::File), "test").is_ok());
    }

    #[test]
    fn overflow_is_rejected_when_fallback_is_disabled() {
        let mut table = table(2, FileTableExhaustion::Fail);
        let _ = register(&mut table, &[10, 11]);

        assert!(table.claim(1).is_err());
        // The rejected claim must not have consumed anything.
        assert!(table.claim(0).is_ok());
    }

    #[test]
    fn a_partially_claimed_batch_is_fully_returned_on_failure() {
        let mut table = table(1, FileTableExhaustion::Fail);
        assert!(table.claim(2).is_err());

        let fds = register(&mut table, &[10]);
        assert_eq!(
            table.resolve(fds[0], None, "test").unwrap(),
            SqeFd::Fixed(0)
        );
    }

    #[test]
    fn a_released_slot_rejects_its_stale_descriptor() {
        let mut table = table(2, FileTableExhaustion::Fallback);
        let fds = register(&mut table, &[10]);
        let index = fds[0].fixed_index().expect("registered descriptor");

        table.take_entry(index);
        table.release(index);
        table.advance_generation(index);

        assert!(table.resolve(fds[0], None, "test").is_err());
        assert!(!table.matches_generation(index, fds[0].generation().unwrap()));
    }

    #[test]
    fn a_released_slot_is_reused_with_a_fresh_generation() {
        let mut table = table(1, FileTableExhaustion::Fallback);
        let fds = register(&mut table, &[10]);
        let index = fds[0].fixed_index().expect("registered descriptor");

        table.take_entry(index);
        table.release(index);
        table.advance_generation(index);

        let again = register(&mut table, &[11]);
        assert_eq!(again[0].fixed_index(), Some(index));
        assert_ne!(again[0].generation(), fds[0].generation());
    }

    #[test]
    fn resolve_rejects_a_kind_mismatch() {
        let mut table = table(2, FileTableExhaustion::Fallback);
        let fds = register(&mut table, &[10]);

        assert!(
            table
                .resolve(fds[0], Some(RawHandleKind::Socket), "test")
                .is_err()
        );
        assert!(
            table
                .resolve(fds[0], Some(RawHandleKind::File), "test")
                .is_ok()
        );
    }

    #[test]
    fn quarantined_slot_is_not_described_or_reused() {
        let mut table = table(2, FileTableExhaustion::Fail);
        let fds = register(&mut table, &[10]);
        let old = fds[0];
        let index = old.fixed_index().expect("registered descriptor");

        table.take_entry(index);
        table.quarantine(index);
        table.advance_generation(index);

        assert!(table.is_quarantined(index));
        assert!(table.descriptor(index).is_none());
        assert!(matches!(
            table.resolve(old, None, "test"),
            Err(report) if *report.inner() == UringError::ResolveFd
        ));

        let current = IoFd::fixed_with_generation(index, table.generation(index).unwrap());
        assert!(matches!(
            table.resolve(current, None, "test"),
            Err(report) if *report.inner() == UringError::FileTableQuarantined
        ));

        let other = register(&mut table, &[11]);
        assert_eq!(other[0].fixed_index(), Some(1));
        assert!(table.claim(1).is_err());
    }

    #[test]
    fn poisoned_table_rejects_registered_access_without_dropping_entries() {
        let mut table = table(2, FileTableExhaustion::Fallback);
        let fds = register(&mut table, &[10]);
        let index = fds[0].fixed_index().expect("registered descriptor");
        let context = FileTablePoisonContext {
            scope: "test.poison",
            failed_index: Some(index),
            start_index: index,
            requested_files: 2,
            updated_files: Some(1),
        };

        assert_eq!(table.free_fixed.len(), 1);
        assert!(table.poison(context));
        assert!(table.free_fixed.is_empty());
        assert!(table.entry(index).is_some());
        assert!(table.descriptor(index).is_none());
        assert!(matches!(
            table.resolve(fds[0], None, "test.resolve"),
            Err(report) if *report.inner() == UringError::FileTablePoisoned
        ));
        assert!(table.claim(1).is_err());
    }

    #[test]
    fn poisoned_table_keeps_direct_descriptors_on_the_raw_fd_path() {
        let mut table = table(2, FileTableExhaustion::Fallback);
        let fds = register(&mut table, &[10, 11, 12]);
        let index = fds[0].fixed_index().expect("registered descriptor");
        let direct = fds[2];

        assert!(table.poison(FileTablePoisonContext {
            scope: "test.poison",
            failed_index: Some(index),
            start_index: index,
            requested_files: 1,
            updated_files: None,
        }));

        assert_eq!(
            table.resolve(direct, None, "test.direct").unwrap(),
            SqeFd::Direct(12)
        );
        assert_eq!(
            table.resolve_direct(direct, None, "test.direct").unwrap(),
            SqeFd::Direct(12)
        );
        assert!(matches!(
            table.resolve_direct(fds[1], None, "test.registered"),
            Err(report) if *report.inner() == UringError::FileTablePoisoned
        ));
    }

    #[test]
    fn poisoning_is_idempotent_and_cannot_be_recovered() {
        let mut table = table(1, FileTableExhaustion::Fallback);
        let first = FileTablePoisonContext {
            scope: "test.first",
            failed_index: Some(0),
            start_index: 0,
            requested_files: 1,
            updated_files: None,
        };
        let second = FileTablePoisonContext {
            scope: "test.second",
            failed_index: Some(1),
            start_index: 1,
            requested_files: 2,
            updated_files: Some(1),
        };

        assert!(table.poison(first));
        assert!(!table.poison(second));
        assert_eq!(table.poison_context(), Some(first));
        assert!(table.is_poisoned());
        assert!(table.claim(0).is_err());
    }

    #[test]
    fn a_non_quarantined_slot_remains_reusable() {
        let mut table = table(3, FileTableExhaustion::Fail);
        let first = register(&mut table, &[10]);
        let quarantined = first[0].fixed_index().expect("registered descriptor");
        table.take_entry(quarantined);
        table.quarantine(quarantined);
        table.advance_generation(quarantined);

        let second = register(&mut table, &[11]);
        let second_index = second[0].fixed_index().expect("registered descriptor");
        assert_ne!(second_index, quarantined);

        table.take_entry(second_index);
        table.release(second_index);
        table.advance_generation(second_index);

        let third = register(&mut table, &[12]);
        assert_eq!(third[0].fixed_index(), Some(second_index));
        assert_ne!(third[0].generation(), second[0].generation());
    }
}
