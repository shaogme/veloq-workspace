//! Core units and configuration for the heap allocator.

use std::{
    fmt,
    num::NonZeroUsize,
    ptr::NonNull,
    sync::atomic::{AtomicBool, AtomicU64, Ordering},
};

/// 2MB minimum memory per thread (Huge Page aligned)
pub(crate) const MIN_THREAD_MEMORY: NonZeroUsize = crate::nz!(2 * 1024 * 1024);

/// Multiplier for thread memory scaling.
/// Each unit represents `MIN_THREAD_MEMORY` (2MB).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ThreadMemoryMultiplier(pub NonZeroUsize);

/// Configuration for GlobalSlotPool
#[derive(Debug, Clone)]
pub struct GlobalAllocatorConfig {
    /// Total memory size in bytes to allocate for the global pool.
    pub total_memory: usize,
}

/// Chunk identifier within a [`GlobalSlotPool`](crate::heap::GlobalSlotPool).
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
#[repr(transparent)]
pub struct ChunkId(pub(crate) u16);

impl ChunkId {
    pub const ZERO: Self = Self(0);

    #[cfg(not(debug_assertions))]
    pub(crate) const fn from_raw(value: u16) -> Self {
        Self(value)
    }

    #[inline]
    pub(crate) fn from_index(index: usize) -> Option<Self> {
        u16::try_from(index).map(Self).ok()
    }

    #[cfg(debug_assertions)]
    pub const fn from_raw(raw: u16) -> Self {
        Self(raw)
    }

    #[inline]
    pub fn raw(&self) -> u16 {
        self.0
    }

    #[inline]
    pub fn as_usize(&self) -> usize {
        self.0 as usize
    }
}

impl fmt::Debug for ChunkId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Chunk(#{})", self.0)
    }
}

impl fmt::Display for ChunkId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// Information about a memory chunk (God View)
#[derive(Debug, Clone, Copy)]
pub struct ChunkInfo {
    pub id: ChunkId,
    pub ptr: NonNull<u8>,
    pub len: NonZeroUsize,
}

// Guarantee thread safety for the info pointing to shared memory
unsafe impl Send for ChunkInfo {}
unsafe impl Sync for ChunkInfo {}

// --- From slot.rs ---

/// Standard Slot Size: 4KB
/// Aligned with the physical page size of most architectures (x86_64/AArch64)
pub(crate) const SLOT_SIZE: usize = 4096;

/// A non-zero byte count that is aligned to the slot/page size.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub struct PageAlignedBytes(NonZeroUsize);

impl PageAlignedBytes {
    #[inline]
    pub(crate) const fn new(size: NonZeroUsize) -> Option<Self> {
        if size.get().is_multiple_of(SLOT_SIZE) {
            Some(Self(size))
        } else {
            None
        }
    }

    #[inline]
    pub(crate) fn from_usize(size: usize) -> Option<Self> {
        NonZeroUsize::new(size).and_then(Self::new)
    }

    #[inline]
    pub(crate) const fn get(self) -> usize {
        self.0.get()
    }

    #[inline]
    pub(crate) const fn as_non_zero(self) -> NonZeroUsize {
        self.0
    }

    #[inline]
    pub(crate) const fn slot_count(self) -> SlotCount {
        SlotCount(self.get() / SLOT_SIZE)
    }
}

impl fmt::Debug for PageAlignedBytes {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}B", self.get())
    }
}

/// Number of slots.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
#[repr(transparent)]
pub struct SlotCount(usize);

impl SlotCount {
    #[inline]
    pub(crate) const fn get(self) -> usize {
        self.0
    }

    #[inline]
    pub(crate) const fn bytes(self) -> usize {
        self.0 * SLOT_SIZE
    }

    #[inline]
    pub(crate) const fn superblock_count(self) -> usize {
        self.0 / SUPERBLOCK_SIZE
    }

    #[inline]
    pub(crate) const fn per_shard(self, shard_count: usize) -> Self {
        Self(self.0 / shard_count)
    }
}

impl fmt::Debug for SlotCount {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} slots", self.0)
    }
}

/// Shard index inside a chunk.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
#[repr(transparent)]
pub(crate) struct ShardIndex(usize);

impl ShardIndex {
    #[inline]
    pub(crate) const fn new(value: usize) -> Self {
        Self(value)
    }

    #[inline]
    pub(crate) const fn get(self) -> usize {
        self.0
    }
}

impl fmt::Debug for ShardIndex {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Shard(#{})", self.0)
    }
}

/// Slot Index
///
/// Represents the index of a Slot in the global continuous memory area (Arena).
/// Range: [0, total_slots)
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub(crate) struct SlotIndex(pub usize);

impl fmt::Debug for SlotIndex {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Slot(#{})", self.0)
    }
}

impl SlotIndex {
    #[inline]
    pub(crate) const fn get(self) -> usize {
        self.0
    }

    /// Calculate the memory offset (byte offset) corresponding to the Slot
    #[inline]
    pub(crate) fn offset(&self) -> usize {
        self.0 * SLOT_SIZE
    }

    /// Convert from byte offset to Slot Index
    #[inline]
    pub(crate) fn from_offset(offset: usize) -> Self {
        Self(offset / SLOT_SIZE)
    }

    #[inline]
    pub(crate) const fn from_shard_local(
        shard: ShardIndex,
        slots_per_shard: SlotCount,
        local: Self,
    ) -> Self {
        Self(shard.get() * slots_per_shard.get() + local.get())
    }

    #[inline]
    pub(crate) const fn shard_local(self, slots_per_shard: SlotCount) -> (ShardIndex, Self) {
        (
            ShardIndex(self.0 / slots_per_shard.get()),
            Self(self.0 % slots_per_shard.get()),
        )
    }

    #[inline]
    pub(crate) const fn superblock_index(self) -> SuperblockIndex {
        SuperblockIndex(self.0 / SUPERBLOCK_SIZE)
    }

    #[inline]
    pub(crate) const fn superblock_offset(self) -> u16 {
        (self.0 % SUPERBLOCK_SIZE) as u16
    }

    #[inline]
    pub(crate) const fn from_superblock_offset(sb_idx: SuperblockIndex, offset: u16) -> Self {
        Self(sb_idx.get() * SUPERBLOCK_SIZE + offset as usize)
    }
}

// --- From superblock.rs ---

/// Order of the Superblock (64 Slots = 2^6)
pub(crate) const SUPERBLOCK_ORDER: usize = 6;
/// Number of slots in a Superblock
pub(crate) const SUPERBLOCK_SIZE: usize = 1 << SUPERBLOCK_ORDER;

/// Superblock index inside a chunk.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
#[repr(transparent)]
pub(crate) struct SuperblockIndex(usize);

impl SuperblockIndex {
    #[inline]
    pub(crate) const fn new(value: usize) -> Self {
        Self(value)
    }

    #[inline]
    pub(crate) const fn get(self) -> usize {
        self.0
    }

    #[inline]
    pub(crate) const fn base_slot(self) -> SlotIndex {
        SlotIndex(self.0 * SUPERBLOCK_SIZE)
    }
}

impl fmt::Debug for SuperblockIndex {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Superblock(#{})", self.0)
    }
}

/// State of a Superblock
///
/// Tracks allocation status and ownership state.
///
/// # Concurrency
/// Uses `SeqCst` ordering to prevent race conditions between "Retiring a Superblock"
/// and "Last Slot Deallocation".
#[derive(Debug)]
pub struct SuperblockState {
    /// Bitmap of free slots. 1 = Free, 0 = Used.
    pub free_mask: AtomicU64,
    /// Indicates if a thread is currently holding this superblock as its active allocation buffer.
    /// If true, the superblock cannot be returned to the global buddy system even if empty.
    pub is_active: AtomicBool,
}

impl Default for SuperblockState {
    fn default() -> Self {
        Self::new()
    }
}

impl SuperblockState {
    pub(crate) const fn new() -> Self {
        Self {
            // Initialize to 0 (All Used).
            // This is "safe" because the superblock is Inactive.
            // It effectively treats the uninitialized state as "Full and Inactive".
            // The actual state is set to "All Free" in `init()` when acquired from Buddy.
            free_mask: AtomicU64::new(0),
            is_active: AtomicBool::new(false),
        }
    }

    /// Reset state for reuse (Called when acquiring from Buddy)
    pub(crate) fn init(&self) {
        self.free_mask.store(u64::MAX, Ordering::Release);
        self.is_active.store(true, Ordering::Release);
    }

    /// Try to allocate one slot `(0..63)`.
    pub(crate) fn alloc_one(&self) -> Option<u16> {
        let mut old = self.free_mask.load(Ordering::Relaxed);
        loop {
            if old == 0 {
                return None;
            }
            let idx = old.trailing_zeros();
            let new = old & !(1u64 << idx);
            match self.free_mask.compare_exchange_weak(
                old,
                new,
                Ordering::Acquire,
                Ordering::Relaxed,
            ) {
                Ok(_) => return Some(idx as u16),
                Err(x) => old = x,
            }
        }
    }

    /// Mark a slot as free.
    /// Returns `true` if the superblock is NOW eligible for return to Buddy System.
    /// (i.e., it is Empty AND Not Active).
    pub(crate) fn free_one(&self, idx: u16) -> bool {
        let mask = 1u64 << idx;

        // SeqCst is required here to synchronize with `set_inactive`.
        // We need to ensure that if we see active=true, the Retiring thread
        // will definitely see our bit update.
        let prev = self.free_mask.fetch_or(mask, Ordering::SeqCst);

        let new_mask = prev | mask;

        if new_mask == u64::MAX {
            // It is empty. Check if it is active.
            !self.is_active.load(Ordering::SeqCst)
        } else {
            false
        }
    }

    /// Mark the superblock as inactive (Thread gave up on it).
    /// Returns `true` if the superblock is Empty and should be returned to Buddy System.
    pub(crate) fn set_inactive(&self) -> bool {
        // SeqCst required.
        self.is_active.store(false, Ordering::SeqCst);

        // internal check
        let mask = self.free_mask.load(Ordering::SeqCst);
        mask == u64::MAX
    }
}
