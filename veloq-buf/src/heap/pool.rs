//! Core memory pool management.

use super::buddy::BuddyAllocator;
use super::cache::{LocalCacheEntry, TLS_CACHE};
use super::units::{
    ChunkId, ChunkInfo, GlobalAllocatorConfig, PageAlignedBytes, SUPERBLOCK_ORDER, ShardIndex,
    SlotCount, SlotIndex, SuperblockIndex, SuperblockState,
};
use crate::buffer::{BufError, BufResult};
use crossbeam_utils::CachePadded;
use diagweave::prelude::*;
use parking_lot::{Mutex, RwLock};
use std::hash::{Hash, Hasher};
use std::num::NonZeroUsize;
use std::ptr::NonNull;
use std::sync::Arc;

/// Underlying physical memory block (RAII Wrapper).
/// Responsible for actual OS memory allocation and deallocation.
#[derive(Debug)]
pub struct MemoryChunk {
    pub(crate) ptr: NonNull<u8>,
    pub(crate) size: NonZeroUsize,
}

// Guarantee MemoryChunk can be shared across threads (needed for Arc internals)
unsafe impl Send for MemoryChunk {}
unsafe impl Sync for MemoryChunk {}

impl MemoryChunk {
    pub fn new(size: NonZeroUsize) -> BufResult<Self> {
        let ptr = unsafe {
            // Try Huge Pages first
            match crate::os::alloc_huge_pages(size) {
                Ok(p) => NonNull::new(p),
                Err(_) => {
                    // Fallback to standard pages if Huge Pages failed
                    let p = crate::os::alloc_pages(size).trans()?;
                    NonNull::new(p)
                }
            }
        }
        .ok_or_else(|| BufError::AllocFailed("Allocation returned null pointer".to_string()))?;
        Ok(Self { ptr, size })
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

    /// Obtain the raw parts
    pub fn into_raw_parts(self) -> (NonNull<u8>, NonZeroUsize) {
        let parts = (self.ptr, self.size);
        std::mem::forget(self);
        parts
    }
}

impl Drop for MemoryChunk {
    fn drop(&mut self) {
        unsafe {
            crate::os::free_pages(self.ptr, self.size);
        }
    }
}

/// A Managed Chunk of memory with its own Allocator(s).
pub struct Chunk {
    pub id: ChunkId,

    /// The sharded buddy allocators
    ///
    /// # Drop Order (CRITICAL)
    /// This field MUST be defined BEFORE `memory`.
    /// `BuddyAllocator` accesses the raw pointers in `memory` during its Drop implementation.
    /// Rust drops fields in declaration order, so `shards` will be dropped first, allowing
    /// safe access to `memory`.
    pub(crate) shards: Box<[CachePadded<Mutex<BuddyAllocator>>]>,

    /// Superblock States Array
    /// Mapped 1:1 to the 64-slot chunks of the memory.
    ///
    /// # Drop Order
    /// Should also be dropped before `memory`.
    pub(crate) superblocks: Box<[CachePadded<SuperblockState>]>,

    /// Ownership of the underlying memory.
    ///
    /// # Drop Order (CRITICAL)
    /// This field MUST be defined LAST to ensure it is dropped AFTER all other fields
    /// that might access the memory (like `shards` and `superblocks`).
    pub(crate) memory: Arc<MemoryChunk>,

    /// Number of slots per shard
    pub(crate) slots_per_shard: SlotCount,
}

impl Chunk {
    pub fn new(id: ChunkId, size: PageAlignedBytes) -> BufResult<Self> {
        // 1. Allocate the massive slab
        let chunk = Arc::new(MemoryChunk::new(size.as_non_zero())?);

        let total_slots = size.slot_count();

        // 2. Determine Shard Layout
        // Dynamic sharding based on CPU cores, scaled for contention.
        let parallelism = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1);

        // Start with parallelism next power of 2, minimum 16
        // Note: For dynamically added smaller chunks, we might want fewer shards?
        // But for consistency let's keep it similar for now.
        let mut shard_count = parallelism.next_power_of_two().max(16);

        // Constraint: Each shard must accomodate at least one Superblock (64 slots)
        let max_shards = total_slots.superblock_count();
        if max_shards > 0 && shard_count > max_shards {
            shard_count = 1 << (usize::BITS - 1 - max_shards.leading_zeros());
        } else if max_shards == 0 {
            // Edge case: Total memory is less than SUPERBLOCK_SIZE * SLOT_SIZE
            // Should effectively be 1 or handled by error later
            shard_count = 1;
        }

        let slots_per_shard = total_slots.per_shard(shard_count); // Integer division
        let bytes_per_shard = slots_per_shard.bytes();

        if slots_per_shard.get() == 0 {
            return BufError::ChunkTooSmall.trans();
        }

        // 3. Initialize Buddy Allocators (Sharded)
        let mut shards = Vec::with_capacity(shard_count);
        let base_ptr = chunk.as_mut_ptr();

        for i in 0..shard_count {
            let offset_bytes = i * bytes_per_shard;
            let shard_ptr = unsafe { NonNull::new_unchecked(base_ptr.add(offset_bytes)) };

            // Initialize BuddyAllocator for this slice
            let allocator = unsafe { BuddyAllocator::new(shard_ptr, bytes_per_shard) };
            shards.push(CachePadded::new(Mutex::new(allocator)));
        }

        // 4. Initialize Superblock States
        let num_superblocks = total_slots.superblock_count();
        let states: Vec<CachePadded<SuperblockState>> = (0..num_superblocks)
            .map(|_| CachePadded::new(SuperblockState::new()))
            .collect();

        Ok(Self {
            id,
            shards: shards.into_boxed_slice(),
            superblocks: states.into_boxed_slice(),
            // memory MUST be after shards/superblocks in struct definition
            memory: chunk,
            slots_per_shard,
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
        self: &Arc<Self>,
        order: usize,
        seed: Option<usize>,
    ) -> Option<ChunkAlloc> {
        // Fast Path: Order 0 (4KB) uses Thread Local Superblock Cache
        if order == 0 {
            // 1. No Active Superblock in global. Just find one via global.
            // Note: with FastPath enabled in GlobalSlotPool, we rarely hit this for order 0.

            // 2. No Active Superblock. Alloc New.
            if let Some((base_idx, _)) = self.alloc_global(SUPERBLOCK_ORDER, seed) {
                let sb_idx = base_idx.superblock_index();

                // Initialize State
                // CRITICAL: We must hold the state in "Active" mode before anyone else can touch it.
                // Since we just alloced it, no one else has pointers to slots inside it.
                self.superblocks[sb_idx.get()].init();

                // Alloc one
                let offset = self.superblocks[sb_idx.get()]
                    .alloc_one()
                    .expect("Fresh superblock must have space");

                let global_idx = SlotIndex::from_superblock_offset(sb_idx, offset);
                let ptr = self.resolve_ptr(global_idx);
                return Some(ChunkAlloc::Small(SmallAlloc {
                    chunk: self.clone(),
                    chunk_id: self.id,
                    sb_idx,
                    slot_idx: global_idx,
                    ptr,
                }));
            }

            // Fallback
            return None;
        }

        // Large Allocations: Direct Global
        self.alloc_global(order, seed).map(|(slot_idx, ptr)| {
            ChunkAlloc::Large(LargeAlloc {
                chunk_id: self.id,
                slot_idx,
                ptr,
            })
        })
    }

    /// Internal helper for direct global operations
    fn alloc_global(&self, order: usize, seed: Option<usize>) -> Option<(SlotIndex, NonNull<u8>)> {
        let shard_count = self.shards.len();

        // 1. Determine starting shard (Thread Affinity or Seed)
        let hash = if let Some(s) = seed {
            // Use a simple LCG or just the seed directly for maximum determinism across processes
            // (s * 0x27bb2ee687b0b0fd) is a simple way to spread bits if needed, but s is usually small
            s.wrapping_mul(0x27bb2ee687b0b0fd)
        } else {
            let thread_id = std::thread::current().id();
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
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
                let ptr = buddy.ptr_of(local_idx);

                return Some((
                    SlotIndex::from_shard_local(idx, self.slots_per_shard, local_idx),
                    ptr,
                ));
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
                let ptr = buddy.ptr_of(local_idx);

                return Some((
                    SlotIndex::from_shard_local(idx, self.slots_per_shard, local_idx),
                    ptr,
                ));
            }
        }

        None
    }

    /// Deallocates a block of slots.
    pub(crate) unsafe fn dealloc_slots(&self, index: SlotIndex, order: usize) {
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
        unsafe { self.dealloc_global(sb_idx.base_slot(), SUPERBLOCK_ORDER) };
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

pub type ChunkListener = Box<dyn Fn(ChunkInfo) + Send + Sync>;

/// Global Slot Pool
///
/// Manages a large contiguous memory arena using a sharded Buddy System.
/// The basic unit is a 4KB `Slot`.
pub struct GlobalSlotPool {
    /// Active chunks
    pub(crate) chunks: CachePadded<RwLock<Vec<Arc<Chunk>>>>,
    /// Listener for new chunk allocation (used to notify Runtime/Driver)
    pub(crate) listener: CachePadded<RwLock<Option<ChunkListener>>>,
}

impl GlobalSlotPool {
    /// Create a new GlobalSlotPool
    pub fn new(config: GlobalAllocatorConfig) -> BufResult<Self> {
        let total_size = config.total_memory;

        if total_size < super::units::MIN_THREAD_MEMORY.get() {
            return BufError::ChunkTooSmall.trans();
        }

        let total_size = PageAlignedBytes::from_usize(total_size)
            .ok_or(BufError::PageUnaligned { size: total_size })?;

        // Initial Chunk (ID=0)
        let chunk0 = Chunk::new(ChunkId::ZERO, total_size)?;

        Ok(Self {
            chunks: CachePadded::new(RwLock::new(vec![Arc::new(chunk0)])),
            listener: CachePadded::new(RwLock::new(None)),
        })
    }

    /// Set a listener to be notified when a new chunk is allocated.
    pub fn set_listener<F>(&self, f: F)
    where
        F: Fn(ChunkInfo) + Send + Sync + 'static,
    {
        *self.listener.write() = Some(Box::new(f));
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
    ) -> Option<(ChunkId, SlotIndex, NonNull<u8>)> {
        // --- Phase 0: Lockless TLS Fast Path (Order 0) ---
        if order == 0 {
            let pool_id = self as *const _ as usize;
            if let Some(res) = TLS_CACHE.with(|cache| cache.try_alloc(pool_id)) {
                return Some(res);
            }
        }

        // 1. Optimistic Read Lock: Try existing chunks
        let res = {
            let chunks = self.chunks.read();
            let mut found = None;
            for chunk in chunks.iter() {
                if let Some(alloc) = chunk.alloc_slots(order, seed) {
                    found = Some(alloc);
                    break;
                }
            }
            found
        };

        if let Some(alloc) = res {
            return Some(self.finish_alloc(alloc));
        }

        // 2. Dynamic Expansion (Write Lock)
        {
            let mut chunks = self.chunks.write();

            // Double-check: Someone might have expanded while we waited for the lock
            for chunk in chunks.iter() {
                if let Some(alloc) = chunk.alloc_slots(order, seed) {
                    return Some(self.finish_alloc(alloc));
                }
            }

            // 3. Expand Pool
            let next_id = chunks.len();
            let next_id = ChunkId::from_index(next_id)?;

            // Strategy: fixed expansion size (e.g. 64 MB) to prevent fragmentation from small chunks
            // optimize: maybe exponential growth or configurable?
            const EXPANSION_SIZE: usize = 64 * 1024 * 1024;

            tracing::info!("GlobalSlotPool: Expanding pool with Chunk ID {}", next_id);

            let expansion_size = PageAlignedBytes::from_usize(EXPANSION_SIZE)
                .expect("EXPANSION_SIZE must be page aligned");
            let new_chunk = match Chunk::new(next_id, expansion_size) {
                Ok(c) => Arc::new(c),
                Err(e) => {
                    tracing::error!("Failed to allocate new chunk: {}", e);
                    return None;
                }
            };

            // Try alloc from new chunk
            let res = new_chunk.alloc_slots(order, seed);

            // Commit new chunk
            chunks.push(new_chunk.clone());

            // Notify listener (while holding lock to ensure sequential ordering if needed,
            // though listener usually just updates atomic flag/registry)
            if let Some(listener) = self.listener.read().as_ref() {
                let info = ChunkInfo {
                    id: new_chunk.id,
                    ptr: new_chunk.memory.ptr,
                    len: new_chunk.memory.size,
                };
                listener(info);
            }

            if let Some(alloc) = res {
                return Some(self.finish_alloc(alloc));
            }
        }

        None
    }

    fn finish_alloc(&self, alloc: ChunkAlloc) -> (ChunkId, SlotIndex, NonNull<u8>) {
        match alloc {
            ChunkAlloc::Small(alloc) => {
                let chunk_id = alloc.chunk_id;
                let slot_idx = alloc.slot_idx;
                let ptr = alloc.ptr;
                let pool_id = self as *const _ as usize;
                TLS_CACHE.with(|cache| {
                    cache.insert(
                        pool_id,
                        LocalCacheEntry {
                            chunk: alloc.chunk,
                            sb_idx: alloc.sb_idx,
                            chunk_id,
                        },
                    );
                });
                (chunk_id, slot_idx, ptr)
            }
            ChunkAlloc::Large(alloc) => (alloc.chunk_id, alloc.slot_idx, alloc.ptr),
        }
    }

    /// Deallocates a block of slots.
    ///
    /// # Safety
    /// - `chunk_id`, `index`, and `order` must match a previous allocation.
    pub(crate) unsafe fn dealloc_slots(
        &self,
        chunk_id: impl Into<ChunkId>,
        index: SlotIndex,
        order: usize,
    ) {
        let chunk_id = chunk_id.into();
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
    pub fn chunk_info(&self, chunk_id: impl Into<ChunkId>) -> Option<ChunkInfo> {
        let chunk_id = chunk_id.into();
        let chunks = self.chunks.read();
        chunks.get(chunk_id.as_usize()).map(|c| ChunkInfo {
            id: c.id,
            ptr: c.memory.ptr,
            len: c.memory.size,
        })
    }

    /// Get memory info for all currently allocated chunks.
    pub fn chunk_infos(&self) -> Vec<ChunkInfo> {
        let chunks = self.chunks.read();
        chunks
            .iter()
            .map(|c| ChunkInfo {
                id: c.id,
                ptr: c.memory.ptr,
                len: c.memory.size,
            })
            .collect()
    }

    /// Get Global Memory Info (Legacy/Compat for single chunk)
    /// Returns info for Chunk 0.
    pub fn global_info(&self) -> ChunkInfo {
        self.chunk_info(ChunkId::ZERO).expect("Chunk 0 must exist")
    }
}

impl std::fmt::Debug for GlobalSlotPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let chunks = self.chunks.read();
        f.debug_struct("GlobalSlotPool")
            .field("chunk_count", &chunks.len())
            .finish()
    }
}
