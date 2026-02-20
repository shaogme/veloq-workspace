//! Global Slot Pool Management
//!
//! This module provides the `GlobalSlotPool`, which manages the entire system memory
//! using a Buddy Allocator over a set of 4KB Slots.
//!
//! # Scalability Update
//! To avoid global lock contention, the pool is partitioned into multiple **Shards**.
//! Each shard manages a distinct slice of the global memory.
//!
//! # Dynamic Extension (Phase 1)
//! The pool now supports multiple `Chunk`s. Startups with one chunk, but can grow.

pub mod buddy;
pub mod slot;
pub mod superblock;

use self::buddy::BuddyAllocator;
use self::slot::{SLOT_SIZE, SlotIndex};
use self::superblock::{SUPERBLOCK_ORDER, SUPERBLOCK_SIZE, SuperblockState};
use crossbeam_utils::CachePadded;
use parking_lot::{Mutex, RwLock};
use std::cell::RefCell;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::{io, num::NonZeroUsize, ptr::NonNull, sync::Arc};

/// 2MB minimum memory per thread (Huge Page aligned)
pub const MIN_THREAD_MEMORY: NonZeroUsize = crate::nz!(2 * 1024 * 1024);

/// Underlying physical memory block (RAII Wrapper).
/// Responsible for actual OS memory allocation and deallocation.
#[derive(Debug)]
pub struct MemoryChunk {
    ptr: NonNull<u8>,
    size: NonZeroUsize,
}

// Guarantee MemoryChunk can be shared across threads (needed for Arc internals)
unsafe impl Send for MemoryChunk {}
unsafe impl Sync for MemoryChunk {}

impl MemoryChunk {
    pub fn new(size: NonZeroUsize) -> std::io::Result<Self> {
        let ptr = unsafe {
            // Try Huge Pages first
            match crate::os::alloc_huge_pages(size) {
                Ok(p) => NonNull::new(p),
                Err(_) => {
                    // Fallback to standard pages if Huge Pages failed
                    crate::os::alloc_pages(size).map(NonNull::new)?
                }
            }
        }
        .ok_or_else(|| std::io::Error::other("Allocation failed"))?;
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

/// Information about a memory chunk (God View)
#[derive(Debug, Clone, Copy)]
pub struct ChunkInfo {
    pub id: u16,
    pub ptr: NonNull<u8>,
    pub len: NonZeroUsize,
}

// Guarantee thread safety for the info pointing to shared memory
unsafe impl Send for ChunkInfo {}
unsafe impl Sync for ChunkInfo {}

/// Local Cache Constants
type PoolId = usize;

struct LocalCache {
    // Map PoolID (Memory Base Address) -> Active Superblock Index
    // We only track ONE active superblock per pool for simplicity and cache locality.
    pools: HashMap<PoolId, usize>,
}

thread_local! {
    static TLS_CACHE: RefCell<LocalCache> = RefCell::new(LocalCache {
        pools: HashMap::new(),
    });
}

/// A Managed Chunk of memory with its own Allocator(s).
pub struct Chunk {
    pub id: u16,

    /// The sharded buddy allocators
    ///
    /// # Drop Order (CRITICAL)
    /// This field MUST be defined BEFORE `memory`.
    /// `BuddyAllocator` accesses the raw pointers in `memory` during its Drop implementation.
    /// Rust drops fields in declaration order, so `shards` will be dropped first, allowing
    /// safe access to `memory`.
    shards: Box<[CachePadded<Mutex<BuddyAllocator>>]>,

    /// Superblock States Array
    /// Mapped 1:1 to the 64-slot chunks of the memory.
    ///
    /// # Drop Order
    /// Should also be dropped before `memory`.
    superblocks: Box<[SuperblockState]>,

    /// Ownership of the underlying memory.
    ///
    /// # Drop Order (CRITICAL)
    /// This field MUST be defined LAST to ensure it is dropped AFTER all other fields
    /// that might access the memory (like `shards` and `superblocks`).
    #[allow(dead_code)]
    memory: Arc<MemoryChunk>,

    /// Number of slots per shard
    slots_per_shard: usize,
}

impl Chunk {
    pub fn new(id: u16, size: usize) -> io::Result<Self> {
        // 1. Allocate the massive slab
        let chunk = Arc::new(MemoryChunk::new(unsafe {
            NonZeroUsize::new_unchecked(size)
        })?);

        let total_slots = size / SLOT_SIZE;

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
        let max_shards = total_slots / SUPERBLOCK_SIZE;
        if max_shards > 0 && shard_count > max_shards {
            shard_count = 1 << (usize::BITS - 1 - max_shards.leading_zeros());
        } else if max_shards == 0 {
            // Edge case: Total memory is less than SUPERBLOCK_SIZE * SLOT_SIZE
            // Should effectively be 1 or handled by error later
            shard_count = 1;
        }

        let slots_per_shard = total_slots / shard_count; // Integer division
        let bytes_per_shard = slots_per_shard * SLOT_SIZE;

        if slots_per_shard == 0 {
            return Err(io::Error::new(
                io::ErrorKind::OutOfMemory,
                "Chunk memory size too small for sharding",
            ));
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
        let num_superblocks = total_slots / SUPERBLOCK_SIZE;
        let states: Vec<SuperblockState> = (0..num_superblocks)
            .map(|_| SuperblockState::new())
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
    pub fn resolve_ptr(&self, index: SlotIndex) -> NonNull<u8> {
        let offset = index.offset();
        assert!(
            offset < self.memory.len(),
            "SlotIndex out of bounds in Chunk {}",
            self.id
        );
        unsafe { NonNull::new_unchecked(self.memory.as_ptr().add(offset) as *mut u8) }
    }

    // Internal alloc/dealloc methods specific to this Chunk
    fn alloc_slots(&self, order: usize) -> Option<(SlotIndex, NonNull<u8>)> {
        // Fast Path: Order 0 (4KB) uses Thread Local Superblock Cache
        if order == 0 {
            let pool_id = self.memory.as_ptr() as usize;

            // 1. Try Allocate from Active Superblock
            let sb_idx_opt = TLS_CACHE.with(|cache| cache.borrow().pools.get(&pool_id).cloned());

            if let Some(sb_idx) = sb_idx_opt {
                if let Some(offset) = self.superblocks[sb_idx].alloc_one() {
                    let global_idx = SlotIndex(sb_idx * SUPERBLOCK_SIZE + offset as usize);
                    let ptr = self.resolve_ptr(global_idx);
                    return Some((global_idx, ptr));
                } else {
                    // Superblock Full! Retire it.
                    let should_free = self.superblocks[sb_idx].set_inactive();
                    if should_free {
                        self.dealloc_superblock(sb_idx);
                    }

                    // Remove from cache
                    TLS_CACHE.with(|cache| {
                        cache.borrow_mut().pools.remove(&pool_id);
                    });
                }
            }

            // 2. No Active Superblock. Alloc New.
            if let Some((base_idx, _)) = self.alloc_global(SUPERBLOCK_ORDER) {
                let sb_idx = base_idx.0 / SUPERBLOCK_SIZE;

                // Initialize State
                // CRITICAL: We must hold the state in "Active" mode before anyone else can touch it.
                // Since we just alloced it, no one else has pointers to slots inside it.
                self.superblocks[sb_idx].init();

                // Set as Active
                TLS_CACHE.with(|cache| {
                    cache.borrow_mut().pools.insert(pool_id, sb_idx);
                });

                // Alloc one
                let offset = self.superblocks[sb_idx]
                    .alloc_one()
                    .expect("Fresh superblock must have space");

                let global_idx = SlotIndex(sb_idx * SUPERBLOCK_SIZE + offset as usize);
                let ptr = self.resolve_ptr(global_idx);
                return Some((global_idx, ptr));
            }

            // Fallback
            return None;
        }

        // Large Allocations: Direct Global
        self.alloc_global(order)
    }

    /// Internal helper for direct global operations
    fn alloc_global(&self, order: usize) -> Option<(SlotIndex, NonNull<u8>)> {
        let shard_count = self.shards.len();

        // 1. Determine starting shard (Thread Affinity)
        let thread_id = std::thread::current().id();
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        thread_id.hash(&mut hasher);
        let hash = hasher.finish() as usize;

        // Start shard based on hash
        // Optimization: shard_count is always power of 2 (enforced in new)
        let mask = shard_count - 1;
        let start_shard = hash & mask;

        // Randomized Stealing Stride
        // We use the upper bits of the hash to generate a stride.
        // Stride must be odd to be coprime with shard_count (power of 2),
        // ensuring we visit all shards exactly once in the permuted order.
        let stride = (hash >> 16) | 1;

        // 2. Phase 1: Try Lock (Optimistic)
        // Scan ALL shards using the permuted order.
        for i in 0..shard_count {
            // idx = (start + i * stride) % count
            let idx = (start_shard.wrapping_add(i.wrapping_mul(stride))) & mask;

            if let Some(mut buddy) = self.shards[idx].try_lock()
                && let Some(local_idx) = buddy.alloc(order)
            {
                let ptr = buddy.ptr_of(local_idx);

                // Convert local slot index to global slot index
                // Global = ShardBase + Local
                let global_idx_val = (idx * self.slots_per_shard) + local_idx.0;

                return Some((SlotIndex(global_idx_val), ptr));
            }
        }

        // 3. Phase 2: Blocking
        // If we are here, either all shards are contented, or all are full.
        // Iterate through all shards again, blocking on each.
        for i in 0..shard_count {
            let idx = (start_shard.wrapping_add(i.wrapping_mul(stride))) & mask;

            // Block until lock acquired
            let mut buddy = self.shards[idx].lock();
            if let Some(local_idx) = buddy.alloc(order) {
                let ptr = buddy.ptr_of(local_idx);

                let global_idx_val = (idx * self.slots_per_shard) + local_idx.0;

                return Some((SlotIndex(global_idx_val), ptr));
            }
        }

        None
    }

    /// Deallocates a block of slots.
    unsafe fn dealloc_slots(&self, index: SlotIndex, order: usize) {
        // Fast Path: Order 0 uses Superblock State
        if order == 0 {
            let sb_idx = index.0 / SUPERBLOCK_SIZE;
            let offset = (index.0 % SUPERBLOCK_SIZE) as u16;

            if sb_idx >= self.superblocks.len() {
                return;
            }

            // Free slot in bitmap
            // If returns true, Block is Empty AND Inactive -> Return to Buddy
            if self.superblocks[sb_idx].free_one(offset) {
                self.dealloc_superblock(sb_idx);
            }
            return;
        }

        unsafe {
            self.dealloc_global(index, order);
        }
    }

    fn dealloc_superblock(&self, sb_idx: usize) {
        let global_idx = SlotIndex(sb_idx * SUPERBLOCK_SIZE);
        unsafe { self.dealloc_global(global_idx, SUPERBLOCK_ORDER) };
    }

    unsafe fn dealloc_global(&self, index: SlotIndex, order: usize) {
        // 1. Map Global Index -> Shard Index
        let (shard_idx, local_idx) = self.shard_for_index(index);

        // Boundary check
        if shard_idx >= self.shards.len() {
            panic!(
                "Chunk {}: Invalid slot index {} (shard {})",
                self.id, index.0, shard_idx
            );
        }

        // 2. Deallocate in strict shard
        let mut buddy = self.shards[shard_idx].lock();
        unsafe {
            buddy
                .dealloc(local_idx, order)
                .expect("Chunk dealloc failed");
        }
    }

    /// Helper: Map Global SlotIndex to Shard and Local Index
    fn shard_for_index(&self, index: SlotIndex) -> (usize, SlotIndex) {
        let shard_idx = index.0 / self.slots_per_shard;
        let local_idx = index.0 % self.slots_per_shard;
        (shard_idx, SlotIndex(local_idx))
    }
}

pub type ChunkListener = Box<dyn Fn(ChunkInfo) + Send + Sync>;

/// Global Slot Pool
///
/// Manages a large contiguous memory arena using a sharded Buddy System.
/// The basic unit is a 4KB `Slot`.
pub struct GlobalSlotPool {
    /// Active chunks
    chunks: RwLock<Vec<Arc<Chunk>>>,
    /// Configuration
    #[allow(dead_code)]
    config: GlobalAllocatorConfig,
    /// Listener for new chunk allocation (used to notify Runtime/Driver)
    listener: RwLock<Option<ChunkListener>>,
}

impl GlobalSlotPool {
    /// Create a new GlobalSlotPool
    pub fn new(config: GlobalAllocatorConfig) -> io::Result<Self> {
        let total_size = config.total_memory;

        if total_size < MIN_THREAD_MEMORY.get() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Total memory too small",
            ));
        }

        // Initial Chunk (ID=0)
        let chunk0 = Chunk::new(0, total_size)?;

        Ok(Self {
            chunks: RwLock::new(vec![Arc::new(chunk0)]),
            config,
            listener: RwLock::new(None),
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
    pub fn alloc_slots(&self, order: usize) -> Option<(u16, SlotIndex, NonNull<u8>)> {
        // 1. Optimistic Read Lock: Try existing chunks
        {
            let chunks = self.chunks.read();
            for chunk in chunks.iter() {
                if let Some((idx, ptr)) = chunk.alloc_slots(order) {
                    return Some((chunk.id, idx, ptr));
                }
            }
        }

        // 2. Dynamic Expansion (Write Lock)
        {
            let mut chunks = self.chunks.write();

            // Double-check: Someone might have expanded while we waited for the lock
            for chunk in chunks.iter() {
                if let Some((idx, ptr)) = chunk.alloc_slots(order) {
                    return Some((chunk.id, idx, ptr));
                }
            }

            // 3. Expand Pool
            let next_id = chunks.len();
            if next_id > u16::MAX as usize {
                // ID overflow
                return None;
            }
            let next_id_u16 = next_id as u16;

            // Strategy: fixed expansion size (e.g. 64 MB) to prevent fragmentation from small chunks
            // optimize: maybe exponential growth or configurable?
            const EXPANSION_SIZE: usize = 64 * 1024 * 1024;

            tracing::info!(
                "GlobalSlotPool: Expanding pool with Chunk ID {}",
                next_id_u16
            );

            let new_chunk = match Chunk::new(next_id_u16, EXPANSION_SIZE) {
                Ok(c) => Arc::new(c),
                Err(e) => {
                    tracing::error!("Failed to allocate new chunk: {}", e);
                    return None;
                }
            };

            // Try alloc from new chunk
            let res = new_chunk.alloc_slots(order);

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

            if let Some((idx, ptr)) = res {
                return Some((next_id_u16, idx, ptr));
            }
        }

        None
    }

    /// Deallocates a block of slots.
    ///
    /// # Safety
    /// - `chunk_id`, `index`, and `order` must match a previous allocation.
    pub unsafe fn dealloc_slots(&self, chunk_id: u16, index: SlotIndex, order: usize) {
        let chunks = self.chunks.read();
        if let Some(chunk) = chunks.get(chunk_id as usize) {
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
    pub fn chunk_info(&self, chunk_id: u16) -> Option<ChunkInfo> {
        let chunks = self.chunks.read();
        chunks.get(chunk_id as usize).map(|c| ChunkInfo {
            id: c.id,
            ptr: c.memory.ptr,
            len: c.memory.size,
        })
    }

    /// Get Global Memory Info (Legacy/Compat for single chunk)
    /// Returns info for Chunk 0.
    pub fn global_info(&self) -> ChunkInfo {
        self.chunk_info(0).expect("Chunk 0 must exist")
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
