//! Global Slot Pool Management
//!
//! This module provides the `GlobalSlotPool`, which manages the entire system memory
//! using a Buddy Allocator over a set of 4KB Slots.
//!
//! # Scalability Update
//! To avoid global lock contention, the pool is partitioned into multiple **Shards**.
//! Each shard manages a distinct slice of the global memory.

use crate::buffer::buddy::BuddyAllocator;
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

/// Constant defining the number of shards
/// Must be power of 2 for optimal performance (optional with modulo, but good practice).
/// Using 16 shards to balance memory overhead and contention reduction.
const SHARD_COUNT: usize = 16;

// Local Cache Constants
const CACHE_LIMIT: usize = 256; // Max 256 slots (1MB) per thread per pool

type PoolId = usize;

struct LocalCache {
    // Map PoolID (Memory Base Address) -> Stack of Free Indices
    pools: HashMap<PoolId, Vec<SlotIndex>>,
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

        // 3. Initialize Buddy Allocators (Sharded)
        let total_slots = total_size / SLOT_SIZE;
        let slots_per_shard = total_slots / SHARD_COUNT; // Integer division
        let bytes_per_shard = slots_per_shard * SLOT_SIZE;

        if slots_per_shard == 0 {
            return Err(io::Error::new(
                io::ErrorKind::OutOfMemory,
                "Total memory too small for sharding",
            ));
        }

        let mut shards = Vec::with_capacity(SHARD_COUNT);
        let base_ptr = chunk.as_mut_ptr();

        for i in 0..SHARD_COUNT {
            let offset_bytes = i * bytes_per_shard;
            let shard_ptr = unsafe { NonNull::new_unchecked(base_ptr.add(offset_bytes)) };

            // Initialize BuddyAllocator for this slice
            let allocator = unsafe { BuddyAllocator::new(shard_ptr, bytes_per_shard) };
            shards.push(CachePadded::new(Mutex::new(allocator)));
        }

        // Warning: If total_slots % SHARD_COUNT != 0, the tail memory is ignored.
        // Given the huge page sizes and power-of-2 allocations, this is negligible.

        Ok(Self {
            shards,
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
        // Fast Path: Order 0 (4KB) uses Thread Local Cache
        if order == 0 {
            let pool_id = self.memory.as_ptr() as usize;
            // 1. Try Cache
            let cached_idx = TLS_CACHE.with(|cache| {
                let mut cache = cache.borrow_mut();
                let list = cache.pools.entry(pool_id).or_default();
                list.pop()
            });

            if let Some(idx) = cached_idx {
                let ptr = unsafe {
                    NonNull::new_unchecked(self.memory.as_ptr().add(idx.offset()) as *mut u8)
                };
                return Some((idx, ptr));
            }

            // 2. Cache Miss: Allocate Batch from Global
            // To reduce locking, we allocate a larger block (e.g., Order 6 = 64 slots)
            // and break it down.
            const BATCH_ORDER: usize = 6; // 64 slots
            if let Some((base_idx, _)) = self.alloc_global(BATCH_ORDER) {
                // Split order 6 block into 64 order 0 blocks
                let start = base_idx.0;
                let count = 1 << BATCH_ORDER;

                TLS_CACHE.with(|cache| {
                    let mut cache = cache.borrow_mut();
                    let list = cache.pools.entry(pool_id).or_default();
                    // Keep one for return, push rest (count-1)
                    // Push in reverse order to keep usage contiguous-ish? Doesn't matter much for free list.
                    for i in 1..count {
                        list.push(SlotIndex(start + i));
                    }
                });

                let ret_idx = SlotIndex(start);
                let ptr = unsafe {
                    NonNull::new_unchecked(self.memory.as_ptr().add(ret_idx.offset()) as *mut u8)
                };
                return Some((ret_idx, ptr));
            }

            // Fallback: If Batch alloc failed (fragmentation?), try alloc Order 0 directly from Global
            return self.alloc_global(0);
        }

        // Large Allocations: Direct Global
        self.alloc_global(order)
    }

    /// Internal helper for direct global operations
    fn alloc_global(&self, order: usize) -> Option<(SlotIndex, NonNull<u8>)> {
        // 1. Determine starting shard (Thread Affinity)
        let thread_id = std::thread::current().id();
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        thread_id.hash(&mut hasher);
        let hash = hasher.finish() as usize;

        // Use high bits for shard selection to avoid bias if low bits are similar?
        // DefaultHasher is usually robust.
        let start_shard = hash % SHARD_COUNT;

        // 2. Try Shards (Linearly Probing / Stealing)
        for i in 0..SHARD_COUNT {
            let shard_idx = (start_shard + i) % SHARD_COUNT;
            let shard_mutex = &self.shards[shard_idx];

            let mut buddy = shard_mutex.lock();
            if let Some(local_idx) = buddy.alloc(order) {
                let ptr = buddy.ptr_of(local_idx);

                // Convert local slot index to global slot index
                // Global = ShardBase + Local
                let global_idx_val = (shard_idx * self.slots_per_shard) + local_idx.0;

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
        // Fast Path: Order 0 uses Thread Local Cache
        if order == 0 {
            let pool_id = self.memory.as_ptr() as usize;
            let overflow = TLS_CACHE.with(|cache| {
                let mut cache = cache.borrow_mut();
                let list = cache.pools.entry(pool_id).or_default();
                list.push(index);

                if list.len() > CACHE_LIMIT {
                    // Drain half
                    let keep = CACHE_LIMIT / 2;
                    let drain_from = keep;
                    // Split off the tail (elements to free)
                    Some(list.split_off(drain_from))
                } else {
                    None
                }
            });

            if let Some(to_free) = overflow {
                self.dealloc_batch_global(to_free, 0);
            }
            return;
        }

        unsafe {
            self.dealloc_global(index, order);
        }
    }

    fn dealloc_batch_global(&self, indices: Vec<SlotIndex>, order: usize) {
        for idx in indices {
            unsafe { self.dealloc_global(idx, order) };
        }
    }

    unsafe fn dealloc_global(&self, index: SlotIndex, order: usize) {
        // 1. Map Global Index -> Shard Index
        let shard_idx = index.0 / self.slots_per_shard;
        let local_idx_val = index.0 % self.slots_per_shard;

        // Boundary check (should not happen for valid indices)
        if shard_idx >= self.shards.len() {
            // In worst case (e.g. index inside the ignored tail), we panic.
            // But alloc_slots never returns such index.
            // Double check logic for tail?
            // We ignored tail in `new`, so no indices >= (slots_per_shard * SHARD_COUNT) exist.
            panic!(
                "GlobalSlotPool: Invalid slot index {} (shard {})",
                index.0, shard_idx
            );
        }

        // 2. Deallocate in strict shard
        let mut buddy = self.shards[shard_idx].lock();
        unsafe {
            // Panic or return error? BuddyAllocator returns Result.
            // For now, we unwrap/expect because dealloc failures are bugs.
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
