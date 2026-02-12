//! Global Slot Pool Management
//!
//! This module provides the `GlobalSlotPool`, which manages the entire system memory
//! using a Buddy Allocator over a set of 4KB Slots.

use crate::buffer::buddy::BuddyAllocator;
use crate::slot::SlotIndex;
use crate::{MIN_THREAD_MEMORY, RawSlab, ThreadMemory, ThreadMemoryMultiplier};
use parking_lot::Mutex;
use std::{io, num::NonZeroUsize, ptr::NonNull, sync::Arc};

/// Configuration for GlobalSlotPool
#[derive(Debug, Clone)]
pub struct GlobalAllocatorConfig {
    /// Used to calculate total memory size: sum(multipliers) * MIN_THREAD_MEMORY * 2
    /// (Legacy: originally per-thread, now just aggregates to total size)
    pub multipliers: Vec<ThreadMemoryMultiplier>,
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

/// Global Slot Pool
///
/// Manages a large contiguous memory arena using a Buddy System.
/// The basic unit is a 4KB `Slot`.
pub struct GlobalSlotPool {
    /// The core buddy allocator (protected by a mutex)
    buddy: Mutex<BuddyAllocator>,

    /// Ownership of the underlying memory.
    /// MUST be declared after `buddy` so that `buddy` is dropped first.
    /// `BuddyAllocator` contains intrusive lists stored in this memory.
    #[allow(dead_code)]
    memory: ThreadMemory,

    /// Global memory info for registration
    global_info: GlobalMemoryInfo,
}

impl GlobalSlotPool {
    /// Create a new GlobalSlotPool
    pub fn new(config: GlobalAllocatorConfig) -> io::Result<Self> {
        if config.multipliers.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Config multipliers cannot be empty",
            ));
        }

        // Calculate total size: same formula as before to maintain capacity expectations
        // Each thread originally got 2 blocks (Primary + Backup).
        let total_size: usize = config
            .multipliers
            .iter()
            .map(|m| MIN_THREAD_MEMORY.get() * m.0.get() * 2)
            .sum();

        // 1. Allocate the massive slab
        let slab = Arc::new(RawSlab::new(unsafe {
            NonZeroUsize::new_unchecked(total_size)
        })?);

        // 2. Create ThreadMemory wrapper to hold it
        let mut memory = ThreadMemory {
            _owner: slab.clone(),
            ptr: slab.ptr,
            len: slab.size,
        };

        let global_info = GlobalMemoryInfo {
            ptr: slab.ptr,
            len: slab.size,
        };

        // 3. Initialize Buddy Allocator
        // SAFETY: memory.ptr is valid and len is correct.
        let buddy_allocator =
            unsafe { BuddyAllocator::new(NonNull::new(memory.as_mut_ptr()).unwrap(), total_size) };
        Ok(Self {
            buddy: Mutex::new(buddy_allocator),
            memory,
            global_info,
        })
    }

    /// Allocates a block of `1 << order` slots.
    ///
    /// Returns:
    /// - `SlotIndex`: The index of the first slot.
    /// - `NonNull<u8>`: The raw pointer to the memory.
    pub fn alloc_slots(&self, order: usize) -> Option<(SlotIndex, NonNull<u8>)> {
        let mut buddy = self.buddy.lock();
        if let Some(idx) = buddy.alloc(order) {
            let ptr = buddy.ptr_of(idx);
            Some((idx, ptr))
        } else {
            None
        }
    }

    /// Deallocates a block of slots.
    ///
    /// # Safety
    /// - `index` and `order` must match a previous allocation.
    pub unsafe fn dealloc_slots(&self, index: SlotIndex, order: usize) {
        let mut buddy = self.buddy.lock();
        unsafe {
            // Panic or return error? BuddyAllocator returns Result.
            // For now, we unwrap/expect because dealloc failures are bugs.
            buddy
                .dealloc(index, order)
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
            .field("buddy", &self.buddy)
            .finish()
    }
}
