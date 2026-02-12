//! Global Slot Pool Management
//!
//! This module provides the `GlobalSlotPool`, which manages the entire system memory
//! using a Buddy Allocator over a set of 4KB Slots.
//!
//! # Scalability Update
//! To avoid global lock contention, the pool is partitioned into multiple **Shards**.
//! Each shard manages a distinct slice of the global memory.

use crate::buffer::buddy::BuddyAllocator;
use crate::buffer::superblock::{SUPERBLOCK_ORDER, SUPERBLOCK_SIZE, SuperblockState};
use crate::slot::{SLOT_SIZE, SlotIndex};
use crate::{MIN_THREAD_MEMORY, MemoryChunk};
use crossbeam_utils::CachePadded;
use parking_lot::Mutex;
use std::cell::RefCell;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::{io, num::NonZeroUsize, ptr::NonNull, sync::Arc};

/// Configuration for GlobalSlotPool
#[derive(Debug, Clone)]
pub struct GlobalAllocatorConfig {
    /// Total memory size in bytes to allocate for the global pool.
    pub total_memory: usize,
}

/// Information about the global memory block (God View)
#[derive(Debug, Clone, Copy)]
pub struct GlobalMemoryInfo {
    pub ptr: NonNull<u8>,
    pub len: NonZeroUsize,
}

// Guarantee thread safety for the info pointing to shared memory
unsafe impl Send for GlobalMemoryInfo {}
unsafe impl Sync for GlobalMemoryInfo {}

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

/// Global Slot Pool
///
/// Manages a large contiguous memory arena using a sharded Buddy System.
/// The basic unit is a 4KB `Slot`.
pub struct GlobalSlotPool {
    /// The sharded buddy allocators
    shards: Vec<CachePadded<Mutex<BuddyAllocator>>>,

    /// Superblock States Array
    /// Mapped 1:1 to the 64-slot chunks of the memory.
    superblocks: Box<[SuperblockState]>,

    /// Number of slots per shard (uniform for all except potentially the last,
    /// but we currently enforce uniformity or ignore tail)
    slots_per_shard: usize,

    /// Ownership of the underlying memory.
    /// MUST be declared after `shards` so that shards are dropped first.
    #[allow(dead_code)]
    memory: Arc<MemoryChunk>,

    /// Global memory info for registration
    global_info: GlobalMemoryInfo,
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

        // 1. Allocate the massive slab
        let chunk = Arc::new(MemoryChunk::new(unsafe {
            NonZeroUsize::new_unchecked(total_size)
        })?);

        let global_info = GlobalMemoryInfo {
            ptr: chunk.ptr,
            len: chunk.size,
        };

        // 2. Determine Shard Layout
        // Dynamic sharding based on CPU cores, scaled for contention.
        // We target at least 16 shards for baseline scalability.
        let parallelism = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1);

        // Start with parallelism next power of 2, minimum 16
        let mut shard_count = parallelism.next_power_of_two().max(16);

        let total_slots = total_size / SLOT_SIZE;

        // Constraint: Each shard must accomodate at least one Superblock (64 slots)
        // to support potential superblock allocations.
        // If memory is small, reduce shard count.
        while shard_count > 1 && (total_slots / shard_count) < SUPERBLOCK_SIZE {
            shard_count /= 2;
        }

        let slots_per_shard = total_slots / shard_count; // Integer division
        let bytes_per_shard = slots_per_shard * SLOT_SIZE;

        if slots_per_shard == 0 {
            return Err(io::Error::new(
                io::ErrorKind::OutOfMemory,
                "Total memory too small for sharding",
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
        // Total slots / 64
        let num_superblocks = total_slots / SUPERBLOCK_SIZE;
        let mut states = Vec::with_capacity(num_superblocks);
        for _ in 0..num_superblocks {
            states.push(SuperblockState::new());
        }

        // Warning: If total_slots % shard_count != 0, the tail memory is ignored.
        // Given the huge page sizes and power-of-2 allocations, this is negligible.

        Ok(Self {
            shards,
            superblocks: states.into_boxed_slice(),
            slots_per_shard,
            memory: chunk,
            global_info,
        })
    }

    /// Allocates a block of `1 << order` slots.
    ///
    /// Returns:
    /// - `SlotIndex`: The global index of the first slot.
    /// - `NonNull<u8>`: The raw pointer to the memory.
    pub fn alloc_slots(&self, order: usize) -> Option<(SlotIndex, NonNull<u8>)> {
        // Fast Path: Order 0 (4KB) uses Thread Local Superblock Cache
        if order == 0 {
            let pool_id = self.memory.as_ptr() as usize;

            // 1. Try Allocate from Active Superblock
            let sb_idx_opt = TLS_CACHE.with(|cache| cache.borrow().pools.get(&pool_id).cloned());

            if let Some(sb_idx) = sb_idx_opt {
                if let Some(offset) = self.superblocks[sb_idx].alloc_one() {
                    let global_idx = SlotIndex(sb_idx * SUPERBLOCK_SIZE + offset as usize);
                    // Calculate Ptr
                    let ptr = unsafe {
                        NonNull::new_unchecked(
                            self.memory.as_ptr().add(global_idx.offset()) as *mut u8
                        )
                    };
                    return Some((global_idx, ptr));
                } else {
                    // Superblock Full! Retire it.
                    let should_free = self.superblocks[sb_idx].set_inactive();
                    if should_free {
                        // This case (Full + Empty + Inactive) is impossible (Full != Empty).
                        self.dealloc_superblock(sb_idx);
                    }

                    // Remove from cache
                    TLS_CACHE.with(|cache| {
                        cache.borrow_mut().pools.remove(&pool_id);
                    });
                }
            }

            // 2. No Active Superblock (or just retired). Alloc New.
            // Alloc Order 6 from Buddy
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
                let ptr = unsafe {
                    NonNull::new_unchecked(self.memory.as_ptr().add(global_idx.offset()) as *mut u8)
                };
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

            if let Some(mut buddy) = self.shards[idx].try_lock() {
                if let Some(local_idx) = buddy.alloc(order) {
                    let ptr = buddy.ptr_of(local_idx);

                    // Convert local slot index to global slot index
                    // Global = ShardBase + Local
                    let global_idx_val = (idx * self.slots_per_shard) + local_idx.0;

                    return Some((SlotIndex(global_idx_val), ptr));
                }
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
    ///
    /// # Safety
    /// - `index` and `order` must match a previous allocation.
    pub unsafe fn dealloc_slots(&self, index: SlotIndex, order: usize) {
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
        let shard_idx = index.0 / self.slots_per_shard;
        let local_idx_val = index.0 % self.slots_per_shard;

        // Boundary check
        if shard_idx >= self.shards.len() {
            panic!(
                "GlobalSlotPool: Invalid slot index {} (shard {})",
                index.0, shard_idx
            );
        }

        // 2. Deallocate in strict shard
        let mut buddy = self.shards[shard_idx].lock();
        unsafe {
            buddy
                .dealloc(SlotIndex(local_idx_val), order)
                .expect("GlobalSlotPool dealloc failed");
        }
    }

    /// Get Global Memory Info
    pub fn global_info(&self) -> GlobalMemoryInfo {
        self.global_info
    }
}

impl std::fmt::Debug for GlobalSlotPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GlobalSlotPool")
            .field("global_info", &self.global_info)
            .field("shards_count", &self.shards.len())
            .field("slots_per_shard", &self.slots_per_shard)
            .finish()
    }
}
