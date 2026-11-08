//! Core units and configuration for the heap allocator.

use veloq_std::{
    fmt,
    num::NonZeroUsize,
    ptr::NonNull,
    sync::atomic::{AtomicBool, AtomicU64, Ordering},
};

/// Standard Slot Size: 4KB
/// Aligned with the physical page size of most architectures (x86_64/AArch64)
pub(crate) const SLOT_SIZE: usize = 4096;
/// Order of the Superblock (64 Slots = 2^6)
pub(crate) const SUPERBLOCK_ORDER: usize = 6;
/// Number of slots in a Superblock
pub(crate) const SUPERBLOCK_SIZE: usize = 1 << SUPERBLOCK_ORDER;

/// Configuration for GlobalSlotPool
#[derive(Debug, Clone)]
pub struct GlobalAllocatorConfig {
    /// Total memory size in bytes to allocate for the global pool.
    pub total_memory: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PageAllocationStrategy {
    Standard,
    Huge,
}

/// Chunk identifier within a [`GlobalSlotPool`](crate::heap::GlobalSlotPool).
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
#[repr(transparent)]
pub struct ChunkId(u16);

/// Information about a memory chunk.
#[derive(Debug, Clone, Copy)]
pub struct ChunkInfo {
    id: ChunkId,
    ptr: NonNull<u8>,
    len: NonZeroUsize,
    allocation_strategy: PageAllocationStrategy,
}

/// A non-zero byte count that is aligned to the slot/page size.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub struct PageAlignedBytes(NonZeroUsize);

/// Number of slots.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
#[repr(transparent)]
pub struct SlotCount(usize);

/// Shard index inside a chunk.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
#[repr(transparent)]
pub(crate) struct ShardIndex(usize);

/// Slot Index
///
/// Represents the index of a Slot in the global continuous memory area (Arena).
/// Range: [0, total_slots)
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub(crate) struct SlotIndex(usize);

pub(crate) struct SlotAllocation {
    pub(crate) chunk_id: ChunkId,
    pub(crate) slot_idx: SlotIndex,
    pub(crate) ptr: NonNull<u8>,
}

/// Superblock index inside a chunk.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
#[repr(transparent)]
pub(crate) struct SuperblockIndex(usize);

/// State of a Superblock
///
/// Tracks allocation status and ownership state.
///
/// # Concurrency
/// Uses `SeqCst` ordering to prevent race conditions between "Retiring a Superblock"
/// and "Last Slot Deallocation".
#[derive(Debug)]
pub struct SuperblockState {
    free_mask: AtomicU64,
    is_active: AtomicBool,
    is_released: AtomicBool,
}

impl ChunkId {
    pub const ZERO: Self = Self(0);

    pub const fn from_raw(value: u16) -> Self {
        Self(value)
    }

    #[inline]
    pub const fn raw(self) -> u16 {
        self.0
    }

    #[inline]
    pub const fn as_usize(self) -> usize {
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

impl ChunkInfo {
    pub(crate) fn new(
        id: ChunkId,
        ptr: NonNull<u8>,
        len: NonZeroUsize,
        allocation_strategy: PageAllocationStrategy,
    ) -> Self {
        Self {
            id,
            ptr,
            len,
            allocation_strategy,
        }
    }

    pub const fn id(&self) -> ChunkId {
        self.id
    }

    pub const fn ptr(&self) -> NonNull<u8> {
        self.ptr
    }

    pub const fn len(&self) -> NonZeroUsize {
        self.len
    }

    pub const fn allocation_strategy(&self) -> PageAllocationStrategy {
        self.allocation_strategy
    }
}

// Guarantee thread safety for the info pointing to shared memory.
unsafe impl Send for ChunkInfo {}

unsafe impl Sync for ChunkInfo {}

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

impl SlotCount {
    #[inline]
    pub(crate) const fn from_slots(slots: usize) -> Self {
        Self(slots)
    }

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
}

impl fmt::Debug for SlotCount {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} slots", self.0)
    }
}

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

impl SlotIndex {
    #[inline]
    pub(crate) const fn from_index(index: usize) -> Self {
        Self(index)
    }

    #[inline]
    pub(crate) const fn get(self) -> usize {
        self.0
    }

    #[inline]
    pub(crate) fn offset(&self) -> usize {
        self.0 * SLOT_SIZE
    }

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

impl fmt::Debug for SlotIndex {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Slot(#{})", self.0)
    }
}

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

impl Default for SuperblockState {
    fn default() -> Self {
        Self::new()
    }
}

impl SuperblockState {
    #[cfg(not(feature = "loom"))]
    pub(crate) const fn new() -> Self {
        Self {
            free_mask: AtomicU64::new(0),
            is_active: AtomicBool::new(false),
            is_released: AtomicBool::new(true),
        }
    }

    #[cfg(feature = "loom")]
    pub(crate) fn new() -> Self {
        Self {
            free_mask: AtomicU64::new(0),
            is_active: AtomicBool::new(false),
            is_released: AtomicBool::new(true),
        }
    }

    pub(crate) fn init(&self) {
        self.free_mask.store(u64::MAX, Ordering::Release);
        self.is_released.store(false, Ordering::Release);
        self.is_active.store(true, Ordering::Release);
    }

    pub(crate) fn try_mark_released(&self) -> bool {
        self.is_released
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

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

    pub(crate) fn free_one(&self, idx: u16) -> bool {
        let mask = 1u64 << idx;
        let prev = self.free_mask.fetch_or(mask, Ordering::SeqCst);
        let new_mask = prev | mask;
        if new_mask == u64::MAX {
            !self.is_active.load(Ordering::SeqCst)
        } else {
            false
        }
    }

    pub(crate) fn set_inactive(&self) -> bool {
        self.is_active.store(false, Ordering::SeqCst);
        self.free_mask.load(Ordering::SeqCst) == u64::MAX
    }

    pub(crate) fn try_release(&self) -> bool {
        !self.is_active.load(Ordering::SeqCst)
            && self.free_mask.load(Ordering::SeqCst) == u64::MAX
            && self.try_mark_released()
    }
}
