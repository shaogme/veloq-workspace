//! Core memory pool management.

use super::{
    buddy::BuddyAllocator,
    cache::{LocalCacheEntry, TLS_CACHE},
    units::{
        ChunkId, ChunkInfo, GlobalAllocatorConfig, PageAlignedBytes, PageAllocationStrategy,
        SLOT_SIZE, SUPERBLOCK_ORDER, SUPERBLOCK_SIZE, ShardIndex, SlotAllocation, SlotCount,
        SlotIndex, SuperblockIndex, SuperblockState,
    },
};
use crate::{
    buffer::{BufError, BufResult},
    os::Platform,
};
use crossbeam_utils::CachePadded;
use diagweave::prelude::*;
use veloq_std::{
    boxed::Box,
    collections::hash_map::DefaultHasher,
    fmt::{Debug, Formatter, Result as FmtResult},
    hash::{Hash, Hasher},
    num::NonZeroUsize,
    ptr::NonNull,
    string::ToString,
    sync::{Arc, UnpoisonedMutex, UnpoisonedRwLock},
    thread::{available_parallelism, current_id},
    vec,
    vec::Vec,
};

/// Underlying physical memory block (RAII Wrapper).
/// Responsible for actual OS memory allocation and deallocation.
#[derive(Debug)]
pub struct MemoryChunk {
    ptr: NonNull<u8>,
    size: NonZeroUsize,
    allocation_strategy: PageAllocationStrategy,
}

// Guarantee MemoryChunk can be shared across threads (needed for Arc internals)
unsafe impl Send for MemoryChunk {}

unsafe impl Sync for MemoryChunk {}

impl MemoryChunk {
    pub fn new(size: NonZeroUsize, allocation_strategy: PageAllocationStrategy) -> BufResult<Self> {
        let allocated = unsafe {
            match allocation_strategy {
                PageAllocationStrategy::Standard => Platform::alloc_pages(size),
                PageAllocationStrategy::Huge => Platform::alloc_huge_pages(size),
            }
        }
        .trans()?;
        let ptr = NonNull::new(allocated)
            .ok_or_else(|| BufError::AllocFailed("Allocation returned null pointer".to_string()))?;
        Ok(Self {
            ptr,
            size,
            allocation_strategy,
        })
    }

    /// Get the start pointer of the memory region
    #[inline]
    pub fn as_ptr(&self) -> *const u8 {
        self.ptr.as_ptr()
    }

    /// Get the mutable start pointer of the memory region
    #[inline]
    pub fn as_mut_ptr(&self) -> *mut u8 {
        self.ptr.as_ptr()
    }

    /// Get the length of the memory region
    #[inline]
    pub fn len(&self) -> usize {
        self.size.get()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl Drop for MemoryChunk {
    fn drop(&mut self) {
        unsafe {
            match self.allocation_strategy {
                PageAllocationStrategy::Standard => Platform::free_pages(self.ptr, self.size),
                PageAllocationStrategy::Huge => Platform::free_huge_pages(self.ptr, self.size),
            }
        }
    }
}

/// A Managed Chunk of memory with its own Allocator(s).
pub struct Chunk {
    id: ChunkId,

    /// The sharded buddy allocators
    ///
    /// # Drop Order (CRITICAL)
    /// This field MUST be defined BEFORE `memory`.
    /// `BuddyAllocator` accesses the raw pointers in `memory` during its Drop implementation.
    /// Rust drops fields in declaration order, so `shards` will be dropped first, allowing
    /// safe access to `memory`.
    shards: Box<[CachePadded<UnpoisonedMutex<BuddyAllocator>>]>,

    /// 管理最后一个完整 superblock 之后的槽位。 / Manages slots after the last complete superblock.
    tail_allocator: Option<CachePadded<UnpoisonedMutex<BuddyAllocator>>>,

    main_slots: usize,

    /// Superblock States Array
    /// Mapped 1:1 to the 64-slot chunks of the memory.
    ///
    /// # Drop Order
    /// Should also be dropped before `memory`.
    superblocks: Box<[CachePadded<SuperblockState>]>,

    /// 每个 shard 管理的槽位数。 / Number of slots managed by each shard.
    slots_per_shard: SlotCount,

    /// Ownership of the underlying memory.
    ///
    /// # Drop Order (CRITICAL)
    /// This field MUST be defined LAST to ensure it is dropped AFTER all other fields
    /// that might access the memory (like `shards` and `superblocks`).
    memory: Arc<MemoryChunk>,
}

pub(crate) struct SmallAlloc {
    pub(crate) chunk: Arc<Chunk>,
    pub(crate) chunk_id: ChunkId,
    pub(crate) sb_idx: SuperblockIndex,
    pub(crate) slot_idx: SlotIndex,
    pub(crate) ptr: NonNull<u8>,
}

pub(crate) struct LargeAlloc {
    pub(crate) chunk_id: ChunkId,
    pub(crate) slot_idx: SlotIndex,
    pub(crate) ptr: NonNull<u8>,
}

pub(crate) enum ChunkAlloc {
    Small(SmallAlloc),
    Large(LargeAlloc),
}

impl Chunk {
    pub fn id(&self) -> ChunkId {
        self.id
    }

    pub(crate) fn superblock_state(&self, index: SuperblockIndex) -> &SuperblockState {
        &self.superblocks[index.get()]
    }

    pub(crate) fn memory_base_ptr(&self) -> *const u8 {
        self.memory.as_ptr()
    }

    pub fn new(id: ChunkId, size: PageAlignedBytes) -> BufResult<Self> {
        Self::new_with_page_strategy(id, size, PageAllocationStrategy::Standard)
    }

    pub fn new_with_page_strategy(
        id: ChunkId,
        size: PageAlignedBytes,
        allocation_strategy: PageAllocationStrategy,
    ) -> BufResult<Self> {
        let parallelism = available_parallelism().map(|n| n.get()).unwrap_or(1);
        Self::new_with_parallelism(id, size, allocation_strategy, parallelism)
    }

    fn new_with_parallelism(
        id: ChunkId,
        size: PageAlignedBytes,
        allocation_strategy: PageAllocationStrategy,
        parallelism: usize,
    ) -> BufResult<Self> {
        // 1. Allocate the massive slab
        let chunk = Arc::new(MemoryChunk::new(size.as_non_zero(), allocation_strategy)?);

        let total_slots = size.slot_count();
        let superblock_count = total_slots.superblock_count();
        let main_slots = superblock_count * SUPERBLOCK_SIZE;

        let parallelism = parallelism.max(1);
        let mut shard_count = if superblock_count == 0 {
            0
        } else {
            let mut candidate =
                1usize << (usize::BITS - 1 - parallelism.min(superblock_count).leading_zeros());
            while !superblock_count.is_multiple_of(candidate) {
                candidate >>= 1;
            }
            candidate
        };
        if shard_count == 0 && main_slots != 0 {
            shard_count = 1;
        }

        let slots_per_shard = SlotCount::from_slots(main_slots / shard_count.max(1));
        let bytes_per_shard = slots_per_shard.bytes();
        let mut shards = Vec::with_capacity(shard_count);
        let base_ptr = chunk.as_mut_ptr();

        for i in 0..shard_count {
            let offset_bytes = i * bytes_per_shard;
            let shard_ptr = unsafe { NonNull::new_unchecked(base_ptr.add(offset_bytes)) };

            // Initialize BuddyAllocator for this slice
            let allocator = unsafe { BuddyAllocator::new(shard_ptr, bytes_per_shard) };
            shards.push(CachePadded::new(UnpoisonedMutex::new(allocator)));
        }

        let tail_slots = total_slots.get() - main_slots;
        let tail_allocator = if tail_slots == 0 {
            None
        } else {
            let tail_offset = main_slots * SLOT_SIZE;
            let tail_ptr = unsafe { NonNull::new_unchecked(base_ptr.add(tail_offset)) };
            let tail_size = tail_slots * SLOT_SIZE;
            let allocator = unsafe { BuddyAllocator::new(tail_ptr, tail_size) };
            Some(CachePadded::new(UnpoisonedMutex::new(allocator)))
        };

        let states: Vec<CachePadded<SuperblockState>> = (0..superblock_count)
            .map(|_| CachePadded::new(SuperblockState::new()))
            .collect();

        Ok(Self {
            id,
            shards: shards.into_boxed_slice(),
            tail_allocator,
            main_slots,
            superblocks: states.into_boxed_slice(),
            slots_per_shard,
            memory: chunk,
        })
    }

    /// Resolve SlotIndex to Raw Pointer within this Chunk
    pub(crate) fn resolve_ptr(&self, index: SlotIndex) -> NonNull<u8> {
        let offset = index.offset();
        assert!(
            offset < self.memory.len(),
            "SlotIndex out of bounds in Chunk {}",
            self.id
        );
        unsafe { NonNull::new_unchecked(self.memory.as_ptr().add(offset) as *mut u8) }
    }

    // Internal alloc/dealloc methods specific to this Chunk
    pub(crate) fn alloc_slots(
        this: &Arc<Self>,
        order: usize,
        seed: Option<usize>,
    ) -> Option<ChunkAlloc> {
        if order == 0 && this.superblocks.is_empty() {
            return this.alloc_tail(order).map(|(slot_idx, ptr)| {
                ChunkAlloc::Large(LargeAlloc {
                    chunk_id: this.id,
                    slot_idx,
                    ptr,
                })
            });
        }

        if order == 0 {
            // 1. No Active Superblock in global. Just find one via global.
            // Note: with FastPath enabled in GlobalSlotPool, we rarely hit this for order 0.

            // 2. No Active Superblock. Alloc New.
            if let Some((base_idx, _)) = this.alloc_global(SUPERBLOCK_ORDER, seed, |base_idx| {
                this.superblocks[base_idx.superblock_index().get()].init();
            }) {
                let sb_idx = base_idx.superblock_index();

                // Alloc one
                let offset = this.superblocks[sb_idx.get()]
                    .alloc_one()
                    .expect("Fresh superblock must have space");

                let global_idx = SlotIndex::from_superblock_offset(sb_idx, offset);
                let ptr = this.resolve_ptr(global_idx);
                return Some(ChunkAlloc::Small(SmallAlloc {
                    chunk: this.clone(),
                    chunk_id: this.id,
                    sb_idx,
                    slot_idx: global_idx,
                    ptr,
                }));
            }

            return this.alloc_tail(order).map(|(slot_idx, ptr)| {
                ChunkAlloc::Large(LargeAlloc {
                    chunk_id: this.id,
                    slot_idx,
                    ptr,
                })
            });
        }

        this.alloc_global(order, seed, |_| {})
            .or_else(|| this.alloc_tail(order))
            .map(|(slot_idx, ptr)| {
                ChunkAlloc::Large(LargeAlloc {
                    chunk_id: this.id,
                    slot_idx,
                    ptr,
                })
            })
    }

    fn alloc_tail(&self, order: usize) -> Option<(SlotIndex, NonNull<u8>)> {
        let mut allocator = self.tail_allocator.as_ref()?.lock();
        let local_idx = allocator.alloc(order)?;
        let global_idx = SlotIndex::from_index(self.main_slots + local_idx.get());
        Some((global_idx, allocator.ptr_of(local_idx)))
    }

    /// Internal helper for direct global operations
    fn alloc_global(
        &self,
        order: usize,
        seed: Option<usize>,
        mut on_alloc: impl FnMut(SlotIndex),
    ) -> Option<(SlotIndex, NonNull<u8>)> {
        let shard_count = self.shards.len();
        if shard_count == 0 {
            return None;
        }

        // 1. Determine starting shard (Thread Affinity or Seed)
        let hash = if let Some(s) = seed {
            // Use a simple LCG or just the seed directly for maximum determinism across processes
            // (s * 0x27bb2ee687b0b0fd) is a simple way to spread bits if needed, but s is usually small
            s.wrapping_mul(0x27bb2ee687b0b0fd)
        } else {
            let thread_id = current_id();
            let mut hasher = DefaultHasher::new();
            thread_id.hash(&mut hasher);
            hasher.finish() as usize
        };

        // Start shard based on hash
        // Optimization: shard_count is always power of 2 (enforced in new)
        let mask = shard_count - 1;
        let start_shard = ShardIndex::new(hash & mask);

        // Randomized Stealing Stride
        // We use the upper bits of the hash to generate a stride.
        // Stride must be odd to be coprime with shard_count (power of 2),
        // ensuring we visit all shards exactly once in the permuted order.
        let stride = (hash >> 16) | 1;

        // 2. Phase 1: Try Lock (Optimistic)
        // Scan ALL shards using the permuted order.
        for i in 0..shard_count {
            // idx = (start + i * stride) % count
            let idx =
                ShardIndex::new((start_shard.get().wrapping_add(i.wrapping_mul(stride))) & mask);

            if let Some(mut buddy) = self.shards[idx.get()].try_lock()
                && let Some(local_idx) = buddy.alloc(order)
            {
                let global_idx = SlotIndex::from_shard_local(idx, self.slots_per_shard, local_idx);
                on_alloc(global_idx);
                return Some((global_idx, buddy.ptr_of(local_idx)));
            }
        }

        // 3. Phase 2: Blocking
        // If we are here, either all shards are contented, or all are full.
        // Iterate through all shards again, blocking on each.
        for i in 0..shard_count {
            let idx =
                ShardIndex::new((start_shard.get().wrapping_add(i.wrapping_mul(stride))) & mask);

            // Block until lock acquired
            let mut buddy = self.shards[idx.get()].lock();
            if let Some(local_idx) = buddy.alloc(order) {
                let global_idx = SlotIndex::from_shard_local(idx, self.slots_per_shard, local_idx);
                on_alloc(global_idx);
                return Some((global_idx, buddy.ptr_of(local_idx)));
            }
        }

        None
    }

    /// Deallocates a block of slots.
    pub(crate) unsafe fn dealloc_slots(&self, index: SlotIndex, order: usize) {
        if index.get() >= self.main_slots {
            let local_idx = SlotIndex::from_index(index.get() - self.main_slots);
            let Some(tail) = self.tail_allocator.as_ref() else {
                return;
            };
            let mut allocator = tail.lock();
            unsafe {
                allocator
                    .dealloc(local_idx, order)
                    .expect("Chunk tail dealloc failed");
            }
            return;
        }

        // Fast Path: Order 0 uses Superblock State
        if order == 0 {
            let sb_idx = index.superblock_index();
            let offset = index.superblock_offset();

            if sb_idx.get() >= self.superblocks.len() {
                return;
            }

            // Free slot in bitmap
            // If returns true, Block is Empty AND Inactive -> Return to Buddy
            if self.superblocks[sb_idx.get()].free_one(offset) {
                self.dealloc_superblock(sb_idx);
            }
            return;
        }

        unsafe {
            self.dealloc_global(index, order);
        }
    }

    pub(crate) fn dealloc_superblock(&self, sb_idx: SuperblockIndex) {
        let base_slot = sb_idx.base_slot();
        let (shard_idx, local_idx) = self.shard_for_index(base_slot);
        let mut buddy = self.shards[shard_idx.get()].lock();
        let state = &self.superblocks[sb_idx.get()];
        if !state.try_release() {
            return;
        }
        unsafe {
            buddy
                .dealloc(local_idx, SUPERBLOCK_ORDER)
                .expect("Chunk superblock dealloc failed");
        }
    }

    unsafe fn dealloc_global(&self, index: SlotIndex, order: usize) {
        // 1. Map Global Index -> Shard Index
        let (shard_idx, local_idx) = self.shard_for_index(index);

        // Boundary check
        if shard_idx.get() >= self.shards.len() {
            panic!(
                "Chunk {}: Invalid slot index {} (shard {})",
                self.id,
                index.get(),
                shard_idx.get()
            );
        }

        // 2. Deallocate in strict shard
        let mut buddy = self.shards[shard_idx.get()].lock();
        unsafe {
            buddy
                .dealloc(local_idx, order)
                .expect("Chunk dealloc failed");
        }
    }

    /// Helper: Map Global SlotIndex to Shard and Local Index
    fn shard_for_index(&self, index: SlotIndex) -> (ShardIndex, SlotIndex) {
        index.shard_local(self.slots_per_shard)
    }
}

/// Global Slot Pool
///
/// Manages a large contiguous memory arena using a sharded Buddy System.
/// The basic unit is a 4KB `Slot`.
pub struct GlobalSlotPool {
    /// Active chunks
    chunks: CachePadded<UnpoisonedRwLock<Vec<Arc<Chunk>>>>,
}

impl GlobalSlotPool {
    /// Create a new GlobalSlotPool
    pub fn new(config: GlobalAllocatorConfig) -> BufResult<Self> {
        Self::new_with_page_strategy(config, PageAllocationStrategy::Standard)
    }

    pub fn new_with_page_strategy(
        config: GlobalAllocatorConfig,
        allocation_strategy: PageAllocationStrategy,
    ) -> BufResult<Self> {
        let total_size = config.total_memory;

        if total_size < SLOT_SIZE {
            return BufError::ChunkTooSmall.trans();
        }

        let total_size = PageAlignedBytes::from_usize(total_size)
            .ok_or(BufError::PageUnaligned { size: total_size })?;

        // Initial Chunk (ID=0)
        let chunk0 = Chunk::new_with_page_strategy(ChunkId::ZERO, total_size, allocation_strategy)?;

        Ok(Self {
            chunks: CachePadded::new(UnpoisonedRwLock::new(vec![Arc::new(chunk0)])),
        })
    }

    /// Allocates a block of `1 << order` slots.
    ///
    /// Returns:
    /// - `u16`: Chunk ID
    /// - `SlotIndex`: The within-chunk index.
    /// - `NonNull<u8>`: The raw pointer to the memory.
    pub(crate) fn alloc_slots(
        &self,
        order: usize,
        seed: Option<usize>,
        registered_chunks: &[ChunkId],
    ) -> Option<SlotAllocation> {
        // --- Phase 0: Lockless TLS Fast Path (Order 0) ---
        if order == 0 {
            let pool_id = self as *const _ as usize;
            if let Some(res) = TLS_CACHE.with(|cache| cache.try_alloc(pool_id, registered_chunks)) {
                return Some(res);
            }
        }

        // 1. 只有已注册 chunk 才能生成 I/O buffer。
        // 1. Only registered chunks may produce I/O buffers.
        let res = {
            let chunks = self.chunks.read();
            let mut found = None;
            for id in registered_chunks {
                let Some(chunk) = chunks.get(id.as_usize()) else {
                    continue;
                };
                if let Some(alloc) = Chunk::alloc_slots(chunk, order, seed) {
                    found = Some(alloc);
                    break;
                }
            }
            found
        };

        if let Some(alloc) = res {
            return Some(self.finish_alloc(alloc));
        }
        None
    }

    fn finish_alloc(&self, alloc: ChunkAlloc) -> SlotAllocation {
        match alloc {
            ChunkAlloc::Small(alloc) => {
                let chunk_id = alloc.chunk_id;
                let slot_idx = alloc.slot_idx;
                let ptr = alloc.ptr;
                let pool_id = self as *const _ as usize;
                TLS_CACHE.with(|cache| {
                    cache.insert(
                        pool_id,
                        LocalCacheEntry::new(alloc.chunk, alloc.sb_idx, chunk_id),
                    );
                });
                SlotAllocation {
                    chunk_id,
                    slot_idx,
                    ptr,
                }
            }
            ChunkAlloc::Large(alloc) => SlotAllocation {
                chunk_id: alloc.chunk_id,
                slot_idx: alloc.slot_idx,
                ptr: alloc.ptr,
            },
        }
    }

    /// Deallocates a block of slots.
    ///
    /// # Safety
    /// - `chunk_id`, `index`, and `order` must match a previous allocation.
    pub(crate) unsafe fn dealloc_slots(&self, chunk_id: ChunkId, index: SlotIndex, order: usize) {
        let chunks = self.chunks.read();
        if let Some(chunk) = chunks.get(chunk_id.as_usize()) {
            // Verify ID just in case
            debug_assert_eq!(chunk.id, chunk_id);
            unsafe {
                chunk.dealloc_slots(index, order);
            }
        } else {
            // This can happen if we have a logic error or if we support unloading chunks (unlikely for now)
            panic!("GlobalSlotPool: Dealloc on invalid chunk_id {}", chunk_id);
        }
    }

    /// Get Memory Info for a Chunk
    pub fn chunk_info(&self, chunk_id: ChunkId) -> Option<ChunkInfo> {
        let chunks = self.chunks.read();
        chunks.get(chunk_id.as_usize()).map(|chunk| {
            ChunkInfo::new(
                chunk.id,
                chunk.memory.ptr,
                chunk.memory.size,
                chunk.memory.allocation_strategy,
            )
        })
    }

    /// Get memory info for all currently allocated chunks.
    pub fn chunk_infos(&self) -> Vec<ChunkInfo> {
        let chunks = self.chunks.read();
        chunks
            .iter()
            .map(|chunk| {
                ChunkInfo::new(
                    chunk.id,
                    chunk.memory.ptr,
                    chunk.memory.size,
                    chunk.memory.allocation_strategy,
                )
            })
            .collect()
    }

    /// Get Global Memory Info (Legacy/Compat for single chunk)
    /// Returns info for Chunk 0.
    pub fn global_info(&self) -> ChunkInfo {
        self.chunk_info(ChunkId::ZERO).expect("Chunk 0 must exist")
    }
}

impl Debug for GlobalSlotPool {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        let chunks = self.chunks.read();
        f.debug_struct("GlobalSlotPool")
            .field("chunk_count", &chunks.len())
            .finish()
    }
}

#[cfg(test)]
fn test_chunk(total_slots: usize) -> Arc<Chunk> {
    let bytes = total_slots * SLOT_SIZE;
    let size = PageAlignedBytes::from_usize(bytes).expect("test chunk size is slot aligned");
    Arc::new(
        Chunk::new_with_parallelism(ChunkId::ZERO, size, PageAllocationStrategy::Standard, 4)
            .expect("create test chunk"),
    )
}

#[cfg(test)]
#[test]
fn shard_boundaries_and_tail_allocations_do_not_overlap() {
    let total_slots = SUPERBLOCK_SIZE * 4 + 3;
    let chunk = test_chunk(total_slots);
    assert_eq!(chunk.shards.len(), 4);
    assert_eq!(chunk.slots_per_shard.get(), SUPERBLOCK_SIZE);

    let main_slots = chunk.main_slots;
    let mut occupied = Vec::with_capacity(total_slots);
    occupied.resize(total_slots, false);
    let mut main_indices = Vec::with_capacity(4);

    for shard in 0..chunk.shards.len() {
        let seed = shard;
        let (index, ptr) = chunk
            .alloc_global(SUPERBLOCK_ORDER, Some(seed), |_| {})
            .expect("each shard has one superblock available");
        assert_eq!(index.get(), shard * SUPERBLOCK_SIZE);
        assert_eq!(ptr, chunk.resolve_ptr(index));

        let block = &mut occupied[index.get()..index.get() + SUPERBLOCK_SIZE];
        assert!(
            block.iter().all(|slot| !slot),
            "shard allocations must not overlap"
        );
        block.fill(true);
        main_indices.push(index);
    }

    assert!(occupied[..main_slots].iter().all(|slot| *slot));
    assert!(occupied[main_slots..].iter().all(|slot| !slot));

    let mut tail_indices = Vec::with_capacity(total_slots - main_slots);
    for _ in main_slots..total_slots {
        let allocation = Chunk::alloc_slots(&chunk, 0, Some(0))
            .expect("order-zero allocation should use the tail after main exhaustion");
        let ChunkAlloc::Large(tail) = allocation else {
            panic!("tail allocation should use the buddy allocator");
        };
        assert!(tail.slot_idx.get() >= main_slots);
        assert_eq!(tail.ptr, chunk.resolve_ptr(tail.slot_idx));
        assert!(!occupied[tail.slot_idx.get()]);
        occupied[tail.slot_idx.get()] = true;
        tail_indices.push(tail.slot_idx);
    }

    assert!(Chunk::alloc_slots(&chunk, 0, Some(0)).is_none());
    assert!(occupied.iter().all(|slot| *slot));
    tail_indices.sort_unstable();
    assert_eq!(
        tail_indices,
        (main_slots..total_slots)
            .map(SlotIndex::from_index)
            .collect::<Vec<_>>()
    );

    let released_tail = tail_indices[0];
    unsafe {
        chunk.dealloc_slots(released_tail, 0);
    }
    let replacement =
        Chunk::alloc_slots(&chunk, 0, Some(0)).expect("a freed tail slot should be reusable");
    let ChunkAlloc::Large(replacement) = replacement else {
        panic!("reallocated tail slot should use the buddy allocator");
    };
    assert_eq!(replacement.slot_idx, released_tail);
    tail_indices[0] = replacement.slot_idx;

    for index in tail_indices {
        unsafe {
            chunk.dealloc_slots(index, 0);
        }
    }
    for index in main_indices {
        unsafe {
            chunk.dealloc_slots(index, SUPERBLOCK_ORDER);
        }
    }
}

#[cfg(test)]
#[test]
fn order_zero_path_allocates_and_releases_a_superblock() {
    let chunk = test_chunk(SUPERBLOCK_SIZE);
    let allocation = Chunk::alloc_slots(&chunk, 0, Some(0))
        .expect("order-zero allocation should initialize a superblock");
    let ChunkAlloc::Small(first) = allocation else {
        panic!("a full superblock should use the small-allocation path");
    };
    let superblock_index = first.sb_idx;
    let mut indices = Vec::with_capacity(SUPERBLOCK_SIZE);
    indices.push(first.slot_idx);

    for _ in 1..SUPERBLOCK_SIZE {
        let offset = chunk.superblocks[superblock_index.get()]
            .alloc_one()
            .expect("remaining superblock slots should be available");
        indices.push(SlotIndex::from_superblock_offset(superblock_index, offset));
    }

    indices.sort_unstable();
    assert_eq!(
        indices,
        (0..SUPERBLOCK_SIZE)
            .map(SlotIndex::from_index)
            .collect::<Vec<_>>()
    );

    for index in indices {
        unsafe {
            chunk.dealloc_slots(index, 0);
        }
    }
    assert!(chunk.superblocks[superblock_index.get()].set_inactive());
    chunk.dealloc_superblock(superblock_index);

    assert!(Chunk::alloc_slots(&chunk, 0, Some(0)).is_some());
}
