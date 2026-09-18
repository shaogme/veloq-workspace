//! Bookkeeping for the descriptors a [`UringDriver`](crate::driver::UringDriver) hands out.
//!
//! The kernel's registered file table is a fixed-size allocation sized by
//! [`UringConfig::file_table_capacity`](crate::config::UringConfig::file_table_capacity). This
//! table is the userspace mirror of it, one slot per kernel entry, and a descriptor pointing
//! into it is an [`IoFd::Registered`] — an index plus the generation it was handed out under.
//!
//! Descriptors that do not fit are handed out as [`IoFd::Direct`] or
//! [`IoFd::OwnedDirect`]. Both carry the raw fd inside the descriptor itself and submit without
//! a fixed-file lookup. Only the latter transfers ownership to the driver; those handles are
//! parked in `direct_owned`, keyed by their opaque owner identity.
//!
//! Borrowed direct descriptors have no generation and retain their caller-owned lifetime
//! contract. Owned direct descriptors instead use owner identity to reject stale resolve,
//! unregister, and close operations. See [`FileTable::resolve`].
//!
//! [`RegisterFd::Owned`]: veloq_driver_core::driver::RegisterFd::Owned

use crate::{
    config::{FileTableExhaustion, IoFd, OwnedRawHandle, RawHandleKind, UringRawHandle},
    error::{UringError, UringResult},
};
use diagweave::prelude::*;
use tracing::warn;
use veloq_driver_core::{DirectOwnerId, RawHandleMeta};
use veloq_std::{collections::HashMap, format, mem, string::ToString, vec, vec::Vec};

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
    /// The slot is owned by a registration/cleanup lease but is not a live descriptor.
    ///
    /// This covers both a fresh claim before the kernel update and the short interval where a
    /// live entry is held by a cleanup transaction. `descriptor` intentionally ignores it.
    Reserved,
    Occupied,
    Quarantined,
    /// The generation counter is exhausted; this slot is never reusable.
    Retired,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OwnedLocation {
    Fixed(u32),
    Direct(DirectOwnerId),
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
    /// Handles owned by the driver behind an [`IoFd::OwnedDirect`], keyed by owner identity.
    ///
    /// Only [`RegisterFd::Owned`](veloq_driver_core::driver::RegisterFd::Owned) registrations
    /// that fall back to direct descriptors land here. A borrowed fallback descriptor is
    /// nothing but the number in the `IoFd`.
    direct_owned: HashMap<DirectOwnerId, OwnedRawHandle>,
    /// Raw descriptor numbers currently held by an owned fixed or direct registration.
    ///
    /// This is a duplicate-detection index only. It must never be used to release an owned
    /// handle because raw descriptor numbers can be reused by the operating system.
    owned_raw_index: HashMap<i32, OwnedLocation>,
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
            owned_raw_index: HashMap::default(),
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

    /// Disable fixed-file slots before initialization when the kernel does not support the
    /// sparse resource ABI and the configured policy allows raw-fd fallback.
    pub(crate) fn disable_fixed_table(&mut self) {
        debug_assert!(!self.initialized);
        debug_assert!(self.slots.is_empty());
        self.fixed_capacity = 0;
    }

    #[inline]
    pub(crate) const fn falls_back_when_unavailable(&self) -> bool {
        self.exhaustion.falls_back()
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
        debug_assert!(self.ledger_is_consistent());
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
    /// The three variants are checked differently, and the difference is the whole point of the
    /// split. A registered descriptor is validated against its slot: bounds, generation
    /// (which is what makes a stale descriptor an error rather than a silent hit on whatever
    /// took the slot's place), and the registered handle's kind. A borrowed direct descriptor
    /// carries its handle, so the kind check reads straight off it and no lookup happens at all.
    /// An owned direct descriptor performs an owner identity lookup before it is accepted,
    /// preventing a stale descriptor from naming a reused raw fd.
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
            IoFd::OwnedDirect { .. } => {
                return self.resolve_owned_direct(fd, expected_kind, scope);
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
            IoFd::OwnedDirect { .. } => {
                return self.resolve_owned_direct(fd, expected_kind, scope);
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
            self.slots[index as usize].state = FileSlotState::Reserved;
        }
        // The free list is seeded in reverse, so a fresh table hands out consecutive indices;
        // sorting keeps that property visible to the batching in `register_files_internal`.
        fixed.sort_unstable();

        if overflow > 0 {
            self.report_fallback(overflow);
        }
        debug_assert!(self.ledger_is_consistent());
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

    fn resolve_owned_direct(
        &self,
        fd: IoFd,
        expected_kind: Option<RawHandleKind>,
        scope: &'static str,
    ) -> UringResult<SqeFd> {
        let IoFd::OwnedDirect { handle: raw, owner } = fd else {
            unreachable!("owned direct resolver called for another descriptor kind")
        };

        let Some(owned) = self.direct_owned.get(&owner) else {
            return UringError::ResolveFd
                .push_ctx("scope", scope)
                .with_ctx("fd", fd.to_string())
                .with_ctx("direct_owner", format!("{owner:?}"))
                .attach_note("owned direct descriptor is stale or already unregistered");
        };
        if owned.raw() != raw
            || !matches!(
                self.owned_raw_index.get(&raw.as_fd()),
                Some(OwnedLocation::Direct(current)) if *current == owner
            )
        {
            return UringError::ResolveFd
                .push_ctx("scope", scope)
                .with_ctx("fd", fd.to_string())
                .with_ctx("direct_owner", format!("{owner:?}"))
                .attach_note("owned direct descriptor identity does not match its live handle");
        }

        if let Some(expected_kind) = expected_kind {
            let current_kind = owned.kind();
            if current_kind != expected_kind {
                return UringError::ResolveFd
                    .push_ctx("scope", scope)
                    .with_ctx("fd", fd.to_string())
                    .with_ctx("expected_kind", format!("{expected_kind:?}"))
                    .with_ctx("current_kind", format!("{current_kind:?}"))
                    .attach_note("owned direct file descriptor kind mismatch");
            }
        }

        Ok(SqeFd::Direct(raw.as_fd()))
    }

    /// Returns the current owned registration for `raw`, if any.
    #[inline]
    pub(crate) fn owned_location(&self, raw: UringRawHandle) -> Option<OwnedLocation> {
        self.owned_raw_index.get(&raw.as_fd()).copied()
    }

    /// Takes ownership of a handle handed out as an owned direct descriptor.
    ///
    /// The handle stays parked here until [`Self::release_direct`] retires it, which is what
    /// keeps a `RegisterFd::Owned` fallback registration open for as long as its descriptor
    /// is valid.
    pub(crate) fn adopt_direct(&mut self, handle: OwnedRawHandle) -> UringResult<IoFd> {
        let raw = handle.raw();
        if let Some(location) = self.owned_location(raw) {
            let report = UringError::DuplicateOwnedFd
                .report(
                    "driver.file_table.adopt_direct",
                    "owned descriptor is already registered",
                )
                .with_ctx("raw_fd", raw.as_fd())
                .with_ctx("existing_location", format!("{location:?}"));
            // This is an internal invariant failure after registration preflight. Do not let
            // the duplicate wrapper close the descriptor held by the existing registration.
            mem::forget(handle);
            return Err(report);
        }

        let descriptor = IoFd::owned_direct(raw);
        let owner = descriptor
            .direct_owner()
            .expect("owned_direct always carries an owner identity");
        assert!(
            self.direct_owned.insert(owner, handle).is_none(),
            "direct owner identity was unexpectedly reused"
        );
        assert!(
            self.owned_raw_index.get(&raw.as_fd()).is_none(),
            "owned raw descriptor index changed after preflight"
        );
        self.owned_raw_index
            .insert(raw.as_fd(), OwnedLocation::Direct(owner));
        Ok(descriptor)
    }

    /// Whether the driver holds the handle behind this owned direct descriptor.
    #[inline]
    pub(crate) fn owns_direct(&self, fd: IoFd) -> bool {
        let IoFd::OwnedDirect { handle: raw, owner } = fd else {
            return false;
        };
        self.direct_owned
            .get(&owner)
            .is_some_and(|handle| handle.raw() == raw)
            && matches!(
                self.owned_raw_index.get(&raw.as_fd()),
                Some(OwnedLocation::Direct(current)) if *current == owner
            )
    }

    /// Retires an owned direct descriptor, returning its handle when the identity matches.
    ///
    /// Dropping the returned handle closes the fd; callers whose descriptor was already closed
    /// by the kernel must forget it instead.
    pub(crate) fn release_direct(&mut self, fd: IoFd) -> Option<OwnedRawHandle> {
        let IoFd::OwnedDirect { handle: raw, owner } = fd else {
            return None;
        };
        let owned = self.direct_owned.get(&owner)?;
        if owned.raw() != raw
            || !matches!(
                self.owned_raw_index.get(&raw.as_fd()),
                Some(OwnedLocation::Direct(current)) if *current == owner
            )
        {
            return None;
        }

        self.owned_raw_index.remove(&raw.as_fd());
        self.direct_owned.remove(&owner)
    }

    /// Stores the handle backing `index`. The slot must have been claimed first.
    #[inline]
    pub(crate) fn install_entry(&mut self, index: u32, entry: RegisteredFileEntry) {
        debug_assert_eq!(
            self.slots[index as usize].state,
            FileSlotState::Reserved,
            "installed an entry into an unclaimed file slot"
        );
        debug_assert!(
            self.slots[index as usize].entry.is_none(),
            "installed an entry into an occupied file slot"
        );
        if let RegisteredFileEntry::OwnedHandle(handle) = &entry {
            assert!(
                self.owned_raw_index.get(&handle.raw().as_fd()).is_none(),
                "installed a duplicate owned file descriptor"
            );
            self.owned_raw_index
                .insert(handle.raw().as_fd(), OwnedLocation::Fixed(index));
        }
        self.slots[index as usize].entry = Some(entry);
        self.slots[index as usize].state = FileSlotState::Occupied;
        debug_assert!(self.ledger_is_consistent());
    }

    #[inline]
    pub(crate) fn take_entry(&mut self, index: u32) -> Option<RegisteredFileEntry> {
        let slot = self.slots.get_mut(index as usize)?;
        let entry = slot.entry.take()?;
        debug_assert_eq!(slot.state, FileSlotState::Occupied);
        slot.state = FileSlotState::Reserved;
        if let RegisteredFileEntry::OwnedHandle(handle) = &entry {
            let raw = handle.raw().as_fd();
            debug_assert_eq!(
                self.owned_raw_index.remove(&raw),
                Some(OwnedLocation::Fixed(index)),
                "owned fixed file descriptor index was inconsistent"
            );
        }
        debug_assert!(self.ledger_is_consistent());
        Some(entry)
    }

    /// Replaces the entry in a live fixed slot while maintaining the owned indexes.
    pub(crate) fn replace_entry(
        &mut self,
        index: u32,
        entry: RegisteredFileEntry,
    ) -> Option<RegisteredFileEntry> {
        let old = {
            let slot = self.slots.get_mut(index as usize)?;
            debug_assert_eq!(slot.state, FileSlotState::Occupied);
            slot.entry.replace(entry)
        };

        if let Some(RegisteredFileEntry::OwnedHandle(handle)) = old.as_ref() {
            let raw = handle.raw().as_fd();
            debug_assert_eq!(
                self.owned_raw_index.remove(&raw),
                Some(OwnedLocation::Fixed(index)),
                "replaced owned fixed file descriptor index was inconsistent"
            );
        }
        if let Some(RegisteredFileEntry::OwnedHandle(handle)) = self.entry(index) {
            assert!(
                self.owned_raw_index.get(&handle.raw().as_fd()).is_none(),
                "replaced with a duplicate owned file descriptor"
            );
            self.owned_raw_index
                .insert(handle.raw().as_fd(), OwnedLocation::Fixed(index));
        }
        debug_assert!(self.ledger_is_consistent());
        old
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
            FileSlotState::Reserved,
            "released a file slot that was not claimed"
        );
        self.slots[index as usize].state = FileSlotState::Vacant;
        self.free_fixed.push(index);
        debug_assert!(self.ledger_is_consistent());
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
            FileSlotState::Reserved,
            "quarantined a file slot that was not claimed"
        );
        slot.state = FileSlotState::Quarantined;
        debug_assert!(self.ledger_is_consistent());
    }

    #[inline]
    pub(crate) fn is_quarantined(&self, index: u32) -> bool {
        self.slots.get(index as usize).is_some_and(|slot| {
            matches!(
                slot.state,
                FileSlotState::Quarantined | FileSlotState::Retired
            )
        })
    }

    /// Invalidates every [`IoFd`] previously handed out for `index`.
    pub(crate) fn advance_generation(&mut self, index: u32) {
        let Some(slot) = self.slots.get_mut(index as usize) else {
            return;
        };
        if slot.generation == u64::MAX {
            slot.state = FileSlotState::Retired;
            self.free_fixed.retain(|candidate| *candidate != index);
        } else {
            slot.generation += 1;
        }
        debug_assert!(self.ledger_is_consistent());
    }

    /// Checks the Rust-side resource ledger without consulting the kernel mirror.
    ///
    /// This is intentionally a debug-only assertion surface for the first migration round. The
    /// existing free list and maps remain the operational data structures; this check makes
    /// their relationship with the explicit slot state observable while the commit/abort model
    /// is still being introduced.
    fn ledger_is_consistent(&self) -> bool {
        let mut free_seen = vec![false; self.slots.len()];
        for &index in &self.free_fixed {
            let Some(seen) = free_seen.get_mut(index as usize) else {
                return false;
            };
            if *seen {
                return false;
            }
            *seen = true;
        }

        for (index, slot) in self.slots.iter().enumerate() {
            let valid_slot = match slot.state {
                FileSlotState::Vacant => slot.entry.is_none(),
                FileSlotState::Reserved => slot.entry.is_none(),
                FileSlotState::Occupied => slot.entry.is_some(),
                FileSlotState::Quarantined => slot.entry.is_none(),
                FileSlotState::Retired => slot.entry.is_none(),
            };
            if !valid_slot {
                return false;
            }
            if !self.is_poisoned() && (slot.state == FileSlotState::Vacant) != free_seen[index] {
                return false;
            }
        }

        !self.is_poisoned() || self.free_fixed.is_empty()
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
    use super::{FileSlotState, FileTable, FileTablePoisonContext, RegisteredFileEntry, SqeFd};
    use crate::config::{
        FileTableExhaustion, IoFd, OwnedRawHandle, RawHandle, RawHandleKind, UringRawHandle,
    };
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

    fn owned_eventfd() -> OwnedRawHandle {
        let fd = unsafe { libc::eventfd(0, libc::EFD_CLOEXEC | libc::EFD_NONBLOCK) };
        assert!(fd >= 0, "eventfd creation failed");
        // SAFETY: eventfd returns a freshly created descriptor owned by this value.
        unsafe { OwnedRawHandle::from_raw_owned(RawHandle::new(UringRawHandle::for_file(fd))) }
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
    fn owned_direct_entries_are_keyed_by_distinct_owner_identities() {
        let mut table = table(0, FileTableExhaustion::Fallback);
        let first = table.adopt_direct(owned_eventfd()).unwrap();
        let second = table.adopt_direct(owned_eventfd()).unwrap();

        assert_ne!(first.direct_owner(), second.direct_owner());
        assert!(table.owns_direct(first));
        assert!(table.owns_direct(second));
        assert_eq!(
            table.resolve(first, None, "test").unwrap(),
            SqeFd::Direct(first.direct_handle().unwrap().as_fd())
        );
        assert_eq!(
            table.resolve(second, None, "test").unwrap(),
            SqeFd::Direct(second.direct_handle().unwrap().as_fd())
        );

        drop(table.release_direct(first));
        assert!(!table.owns_direct(first));
        assert!(table.owns_direct(second));
        drop(table.release_direct(second));
    }

    #[test]
    fn releasing_an_old_owner_does_not_remove_a_reused_raw_fd_owner() {
        let mut table = table(0, FileTableExhaustion::Fallback);
        let first_handle = owned_eventfd();
        let first_fd = first_handle.raw().as_fd();
        let replacement_source =
            unsafe { libc::eventfd(0, libc::EFD_CLOEXEC | libc::EFD_NONBLOCK) };
        assert!(replacement_source >= 0);
        let first = table.adopt_direct(first_handle).unwrap();

        drop(table.release_direct(first));
        assert_eq!(
            unsafe { libc::dup2(replacement_source, first_fd) },
            first_fd
        );
        assert_eq!(unsafe { libc::close(replacement_source) }, 0);

        let replacement_handle = unsafe {
            OwnedRawHandle::from_raw_owned(RawHandle::new(UringRawHandle::for_file(first_fd)))
        };
        let replacement = table.adopt_direct(replacement_handle).unwrap();
        assert!(table.owns_direct(replacement));
        assert!(table.release_direct(first).is_none());
        assert!(table.owns_direct(replacement));
        assert_eq!(
            table.resolve(first, None, "test").unwrap_err().inner(),
            &UringError::ResolveFd
        );
        assert_eq!(
            table.resolve(replacement, None, "test").unwrap(),
            SqeFd::Direct(first_fd)
        );

        drop(table.release_direct(replacement));
    }

    #[test]
    fn fixed_owned_index_survives_take_install_and_replace() {
        let mut table = table(1, FileTableExhaustion::Fail);
        let owned = owned_eventfd();
        let raw_fd = owned.raw().as_fd();
        let index = table.claim(1).unwrap()[0];
        table.install_entry(index, RegisteredFileEntry::OwnedHandle(owned));
        assert_eq!(
            table.owned_location(UringRawHandle::for_file(raw_fd)),
            Some(super::OwnedLocation::Fixed(index))
        );

        let entry = table.take_entry(index).expect("owned entry exists");
        assert_eq!(table.owned_location(UringRawHandle::for_file(raw_fd)), None);
        table.install_entry(index, entry);
        assert_eq!(
            table.owned_location(UringRawHandle::for_file(raw_fd)),
            Some(super::OwnedLocation::Fixed(index))
        );

        let old = table.replace_entry(
            index,
            RegisteredFileEntry::BorrowedFd {
                fd: 42,
                kind: RawHandleKind::File,
            },
        );
        drop(old);
        assert_eq!(table.owned_location(UringRawHandle::for_file(raw_fd)), None);
        let _ = table.take_entry(index);
        table.release(index);
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
    fn reserved_slots_never_produce_descriptors_and_return_to_the_ledger() {
        let mut table = table(1, FileTableExhaustion::Fail);
        let index = table.claim(1).unwrap()[0];

        assert!(table.descriptor(index).is_none());
        assert!(table.ledger_is_consistent());

        table.install_entry(index, borrowed(10));
        assert!(table.descriptor(index).is_some());
        assert!(table.ledger_is_consistent());

        table.take_entry(index);
        assert!(table.descriptor(index).is_none());
        assert!(table.ledger_is_consistent());
        table.release(index);
        assert!(table.ledger_is_consistent());
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
    fn generation_exhaustion_retires_the_slot_without_wrapping() {
        let mut table = table(1, FileTableExhaustion::Fail);
        let fd = register(&mut table, &[10])[0];
        let index = fd.fixed_index().expect("registered descriptor");

        table.take_entry(index);
        table.release(index);
        table.slots[index as usize].generation = u64::MAX;
        table.advance_generation(index);

        assert_eq!(table.slots[index as usize].state, FileSlotState::Retired);
        assert!(table.free_fixed.is_empty());
        assert!(table.claim(1).is_err());

        let retired = IoFd::fixed_with_generation(index, u64::MAX);
        assert!(matches!(
            table.resolve(retired, None, "test.retired"),
            Err(report) if *report.inner() == UringError::FileTableQuarantined
        ));
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
