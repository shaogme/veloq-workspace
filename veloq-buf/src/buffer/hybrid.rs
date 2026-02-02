use veloq_bitset::BitSet;

use super::{
    AllocError, AllocResult, AnyBufPool, BackingPool, DeallocParams, FixedBuf, GlobalIndex,
    PoolSpec, PoolVTable, RegisteredPool,
};
use crate::ThreadMemory;
use crossbeam_queue::SegQueue;
use std::alloc::{Layout, alloc, dealloc};
use std::cell::UnsafeCell;
use std::num::NonZeroUsize;
use std::ptr::NonNull;
use std::sync::Arc;
use std::thread;

// Alignment requirement for Direct I/O.
// We use 4096 (Page Size) to ensure compatibility with strict Direct I/O requirements.
// This also ensures that the payload length (Capacity) remains a multiple of 4096.
const PAGE_SIZE: usize = 4096;

const SIZE_4K: usize = 4096;
const SIZE_8K: usize = 8192;
const SIZE_16K: usize = 16384;
const SIZE_32K: usize = 32768;
const SIZE_64K: usize = 65536;

// Slab configuration definition
struct SlabConfig {
    block_size: usize,
    count: usize,
}

// Define 5 classes of buffer sizes
// Class 0: 4KB, 1024 count -> 4MB
// Class 1: 8KB, 512 count -> 4MB
// Class 2: 16KB, 128 count -> 2MB
// Class 3: 32KB, 64 count -> 2MB
// Class 4: 64KB, 32 count -> 2MB
// Total memory: 14MB
const SLABS: [SlabConfig; 5] = [
    SlabConfig {
        block_size: SIZE_4K,
        count: 1024,
    },
    SlabConfig {
        block_size: SIZE_8K,
        count: 512,
    },
    SlabConfig {
        block_size: SIZE_16K,
        count: 128,
    },
    SlabConfig {
        block_size: SIZE_32K,
        count: 64,
    },
    SlabConfig {
        block_size: SIZE_64K,
        count: 32,
    },
];

const GLOBAL_ALLOC_CONTEXT: usize = usize::MAX;
const NEXT_NONE: usize = usize::MAX;

struct Slab {
    config: SlabConfig,
    base_offset: usize,
    free_head: usize,
    allocated: BitSet,
    free_count: usize,
}

/// Raw allocation result from HybridAllocator
pub struct RawAlloc {
    pub ptr: NonNull<u8>,
    pub cap: usize,
    pub context: usize,
}

/// Core allocator logic, managing slabs and global fallback
/// Independent of BufPool trait for easier testing
pub struct HybridAllocator {
    memory: ThreadMemory,
    slabs: Vec<Slab>,
}

impl HybridAllocator {
    pub fn new(memory: ThreadMemory) -> Result<Self, AllocError> {
        let mut total_arena_size = 0;
        for config in SLABS.iter() {
            total_arena_size += config.block_size * config.count;
        }

        if memory.len() < total_arena_size {
            return Err(AllocError::Oom);
        }

        let arena_base_ptr = memory.as_ptr() as *mut u8;
        let mut slabs = Vec::with_capacity(SLABS.len());
        let mut current_offset = 0;

        for config in SLABS.iter() {
            // Intrusive list initialization
            let slab_base_ptr = unsafe { arena_base_ptr.add(current_offset) };

            for i in 0..config.count {
                let offset = i * config.block_size;
                let next_idx = if i < config.count - 1 {
                    i + 1
                } else {
                    NEXT_NONE
                };
                unsafe {
                    let ptr = slab_base_ptr.add(offset) as *mut usize;
                    *ptr = next_idx;
                }
            }

            slabs.push(Slab {
                config: SlabConfig {
                    block_size: config.block_size,
                    count: config.count,
                },
                base_offset: current_offset,
                free_head: 0,
                allocated: BitSet::new(config.count),
                free_count: config.count,
            });

            current_offset += config.block_size * config.count;
        }

        Ok(Self { memory, slabs })
    }

    /// Allocate memory. `size` is the total size requirement including header.
    pub fn alloc(&mut self, needed_total: usize) -> Option<RawAlloc> {
        // Find best slab request size
        let slab_idx = if needed_total <= SIZE_4K {
            Some(0)
        } else if needed_total <= SIZE_8K {
            Some(1)
        } else if needed_total <= SIZE_16K {
            Some(2)
        } else if needed_total <= SIZE_32K {
            Some(3)
        } else if needed_total <= SIZE_64K {
            Some(4)
        } else {
            None
        };

        if let Some(idx) = slab_idx {
            let slab = &mut self.slabs[idx];

            if slab.config.block_size >= needed_total {
                if slab.free_head != NEXT_NONE {
                    let index = slab.free_head;
                    let block_offset = slab.base_offset + index * slab.config.block_size;
                    let block_ptr = unsafe { self.memory.as_ptr().add(block_offset) as *mut u8 };

                    // Read next free index embedded in the block
                    let next_free = unsafe { *(block_ptr as *const usize) };
                    slab.free_head = next_free;
                    slab.free_count -= 1;

                    // Encode slab_idx (0-3) and index (0-512) into usize context
                    let context = (idx << 16) | index;

                    if slab.allocated.set(index).is_err() {
                        return None;
                    }

                    return Some(RawAlloc {
                        ptr: unsafe { NonNull::new_unchecked(block_ptr) },
                        cap: slab.config.block_size,
                        context,
                    });
                }
            }
        }

        // Fallback: Global Allocator
        if needed_total > SIZE_64K {
            let layout = Layout::from_size_align(needed_total, PAGE_SIZE).unwrap();
            let block_ptr = unsafe { alloc(layout) };
            if block_ptr.is_null() {
                // Return None instead of panicking on alloc error here to match signature
                return None;
            }
            // Zero init
            unsafe { std::ptr::write_bytes(block_ptr, 0, needed_total) };

            let cap = needed_total;

            return Some(RawAlloc {
                ptr: unsafe { NonNull::new_unchecked(block_ptr) },
                cap,
                context: GLOBAL_ALLOC_CONTEXT,
            });
        }

        None
    }

    /// Deallocate memory block.
    /// `block_ptr`: pointer to the start of the block (header position).
    /// `cap`: total capacity of the block (needed for global dealloc).
    /// `context`: context from allocation.
    /// # Safety
    /// Caller must ensure block_ptr is valid and matches context
    pub unsafe fn dealloc(
        &mut self,
        block_ptr: NonNull<u8>,
        cap: usize,
        context: usize,
    ) -> Result<(), String> {
        if context == GLOBAL_ALLOC_CONTEXT {
            let layout = Layout::from_size_align(cap, PAGE_SIZE)
                .map_err(|e| format!("Layout error: {}", e))?;
            unsafe { dealloc(block_ptr.as_ptr(), layout) };
            return Ok(());
        }

        let slab_idx = context >> 16;
        let index = context & 0xFFFF;

        if let Some(slab) = self.slabs.get_mut(slab_idx) {
            match slab.allocated.get(index) {
                Ok(true) => {
                    // OK, is allocated
                }
                Ok(false) => {
                    return Err(format!(
                        "Double free detected in HybridAllocator: slab={}, index={}",
                        slab_idx, index
                    ));
                }
                Err(e) => {
                    return Err(format!("BitSet access error: {}", e));
                }
            }

            if let Err(e) = slab.allocated.clear(index) {
                return Err(format!("BitSet clear error: {}", e));
            }

            // Return to free list (push to head)
            let offset = slab.base_offset + index * slab.config.block_size;
            let block_ptr = unsafe { self.memory.as_ptr().add(offset) };
            unsafe {
                *(block_ptr as *mut usize) = slab.free_head;
            }
            slab.free_head = index;
            slab.free_count += 1;

            Ok(())
        } else {
            Err(format!("Invalid slab index: {}", slab_idx))
        }
    }

    #[cfg(test)]
    pub fn count_free(&self, slab_idx: usize) -> usize {
        self.slabs[slab_idx].free_count
    }
}

struct SharedHybridState {
    allocator: UnsafeCell<HybridAllocator>,
    return_queue: SegQueue<DeallocParams>,
    owner_id: thread::ThreadId,
}

// SAFETY: Synchronization via owner_id and SegQueue
unsafe impl Send for SharedHybridState {}
unsafe impl Sync for SharedHybridState {}

#[derive(Clone)]
pub struct HybridPool {
    inner: Arc<SharedHybridState>,
}

pub struct HybridSpec;

impl Default for HybridSpec {
    fn default() -> Self {
        Self
    }
}

impl PoolSpec for HybridSpec {
    fn memory_requirement(&self) -> std::num::NonZeroUsize {
        let mut total_arena_size = 0;
        for config in SLABS.iter() {
            total_arena_size += config.block_size * config.count;
        }
        // SAFETY: calculated size is known to be non-zero
        unsafe { std::num::NonZeroUsize::new_unchecked(total_arena_size) }
    }

    fn build(
        self: Box<Self>,
        memory: ThreadMemory,
        registrar: Box<dyn crate::buffer::BufferRegistrar>,
        global_info: crate::global::GlobalMemoryInfo,
    ) -> AnyBufPool {
        let pool = HybridPool::new(memory).expect("Failed to create HybridPool");
        let reg_pool =
            RegisteredPool::new(pool, registrar, global_info).expect("Failed to register pool");
        AnyBufPool::new(reg_pool)
    }

    fn clone_box(&self) -> Box<dyn PoolSpec> {
        Box::new(Self)
    }
}

impl std::fmt::Debug for HybridPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HybridPool").finish_non_exhaustive()
    }
}

// VTable Shim
static HYBRID_POOL_VTABLE: PoolVTable = PoolVTable {
    dealloc: hybrid_dealloc_shim,
    resolve_region_info: hybrid_resolve_region_info_shim,
};

unsafe fn hybrid_dealloc_shim(pool_data: NonNull<()>, params: DeallocParams) {
    let pool_arc = unsafe { Arc::from_raw(pool_data.as_ptr() as *const SharedHybridState) };

    if thread::current().id() == pool_arc.owner_id {
        let inner = unsafe { &mut *pool_arc.allocator.get() };
        unsafe {
            if let Err(_e) = inner.dealloc(params.ptr, params.cap.get(), params.context) {
                #[cfg(debug_assertions)]
                eprintln!("HybridPool dealloc error: {}", _e);
            }
        }
    } else {
        pool_arc.return_queue.push(params);
    }
}

unsafe fn hybrid_resolve_region_info_shim(
    pool_data: NonNull<()>,
    buf: &FixedBuf,
) -> (usize, usize) {
    let raw = pool_data.as_ptr() as *const SharedHybridState;
    let arc = std::mem::ManuallyDrop::new(unsafe { Arc::from_raw(raw) });

    let inner = unsafe { &*arc.allocator.get() };

    // Use global region to calculate offset
    let (global_base, global_len) = inner.memory.global_region();
    let base = global_base.as_ptr() as usize;
    let ptr = buf.as_ptr() as usize;

    // Check bounds against GLOBAL region
    if ptr < base || ptr >= base + global_len {
        // Fallback or out of bounds
        panic!("Buffer not found in HybridPool regions (Global Fallback?)");
    }

    (0, ptr - base)
}

impl HybridPool {
    pub fn new(memory: ThreadMemory) -> Result<Self, AllocError> {
        Ok(Self {
            inner: Arc::new(SharedHybridState {
                allocator: UnsafeCell::new(HybridAllocator::new(memory)?),
                return_queue: SegQueue::new(),
                owner_id: thread::current().id(),
            }),
        })
    }

    // Helper to return proper types for FixedBuf or AllocResult
    fn alloc_mem_inner(
        &self,
        size: usize,
    ) -> Option<(NonNull<u8>, usize, Option<GlobalIndex>, usize)> {
        if thread::current().id() != self.inner.owner_id {
            panic!("HybridPool::alloc_mem called from non-owner thread");
        }
        let allocator = unsafe { &mut *self.inner.allocator.get() };

        // Drain return queue
        while let Some(params) = self.inner.return_queue.pop() {
            unsafe {
                if let Err(_e) = allocator.dealloc(params.ptr, params.cap.get(), params.context) {
                    #[cfg(debug_assertions)]
                    eprintln!("HybridPool deferred dealloc error: {}", _e);
                }
            }
        }

        let needed_total = size; // No offset needed

        if let Some(raw) = allocator.alloc(needed_total) {
            let block_ptr = raw.ptr.as_ptr();
            unsafe {
                Some((
                    NonNull::new_unchecked(block_ptr),
                    raw.cap,
                    None, // BackingPool: global_index is None
                    raw.context,
                ))
            }
        } else {
            None
        }
    }
}

impl BackingPool for HybridPool {
    fn alloc_mem(&self, size: NonZeroUsize) -> AllocResult {
        if let Some((ptr, cap, global_index, context)) = self.alloc_mem_inner(size.get()) {
            AllocResult::Allocated {
                ptr,
                cap: unsafe { NonZeroUsize::new_unchecked(cap) },
                global_index,
                context,
            }
        } else {
            AllocResult::Failed
        }
    }

    fn vtable(&self) -> &'static PoolVTable {
        &HYBRID_POOL_VTABLE
    }

    fn pool_data(&self) -> NonNull<()> {
        unsafe {
            let raw = Arc::into_raw(self.inner.clone());
            NonNull::new_unchecked(raw as *mut ())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nz;
    use std::num::NonZeroUsize;

    const ARENA_SIZE: NonZeroUsize = nz!(20 * 1024 * 1024);

    #[test]
    fn test_allocator_basic() {
        use crate::global::{GlobalAllocator, GlobalAllocatorConfig};
        let multiplier_val = ARENA_SIZE.get() / crate::MIN_THREAD_MEMORY.get();
        let multiplier =
            crate::ThreadMemoryMultiplier(unsafe { NonZeroUsize::new_unchecked(multiplier_val) });
        let config = GlobalAllocatorConfig {
            multipliers: vec![multiplier],
        }; // 20MB
        let mut memories = GlobalAllocator::new(config).unwrap().0;
        let memory = memories.pop().unwrap();

        let mut allocator = HybridAllocator::new(memory).unwrap();
        // Check initial free counts
        assert_eq!(allocator.count_free(0), 1024); // 4K slab
        assert_eq!(allocator.count_free(1), 512); // 8K slab
        assert_eq!(allocator.count_free(2), 128); // 16K slab
        assert_eq!(allocator.count_free(3), 64); // 32K slab
        assert_eq!(allocator.count_free(4), 32); // 64K slab

        // Alloc 4K (fits in 4K slab)
        let raw = allocator.alloc(4096).unwrap();
        assert_eq!(raw.cap, SIZE_4K);
        // RawAlloc no longer has global_index

        unsafe { allocator.dealloc(raw.ptr, raw.cap, raw.context).unwrap() };
        assert_eq!(allocator.count_free(0), 1024); // Restored
    }

    #[test]
    fn test_allocator_small() {
        use crate::global::{GlobalAllocator, GlobalAllocatorConfig};
        let multiplier_val = ARENA_SIZE.get() / crate::MIN_THREAD_MEMORY.get();
        let multiplier =
            crate::ThreadMemoryMultiplier(unsafe { NonZeroUsize::new_unchecked(multiplier_val) });
        let config = GlobalAllocatorConfig {
            multipliers: vec![multiplier],
        };
        let mut memories = GlobalAllocator::new(config).unwrap().0;
        let memory = memories.pop().unwrap();

        let mut allocator = HybridAllocator::new(memory).unwrap();
        // Request very small size. 100 bytes
        let raw = allocator.alloc(100).unwrap();
        // best_fit(100) -> Size4K
        assert_eq!(raw.cap, SIZE_4K);
        assert_eq!(allocator.count_free(0), 1023);
        unsafe { allocator.dealloc(raw.ptr, raw.cap, raw.context).unwrap() };
    }

    #[test]
    fn test_allocator_large() {
        use crate::global::{GlobalAllocator, GlobalAllocatorConfig};
        let multiplier_val = ARENA_SIZE.get() / crate::MIN_THREAD_MEMORY.get();
        let multiplier =
            crate::ThreadMemoryMultiplier(unsafe { NonZeroUsize::new_unchecked(multiplier_val) });
        let config = GlobalAllocatorConfig {
            multipliers: vec![multiplier],
        };
        let mut memories = GlobalAllocator::new(config).unwrap().0;
        let memory = memories.pop().unwrap();

        let mut allocator = HybridAllocator::new(memory).unwrap();
        // Request 1MB
        let raw = allocator.alloc(1024 * 1024).unwrap();
        assert!(raw.cap >= 1024 * 1024);
        assert_eq!(raw.context, GLOBAL_ALLOC_CONTEXT);

        unsafe { allocator.dealloc(raw.ptr, raw.cap, raw.context).unwrap() };
    }

    // Test for HybridPool integration removed/adjusted because direct alloc returns FixedBuf which requires Registration
    // But since HybridPool is BackingPool, we can test alloc_mem.
    #[test]
    fn test_hybrid_pool_alloc_mem() {
        use crate::global::{GlobalAllocator, GlobalAllocatorConfig};
        let multiplier_val = ARENA_SIZE.get() / crate::MIN_THREAD_MEMORY.get();
        let multiplier =
            crate::ThreadMemoryMultiplier(unsafe { NonZeroUsize::new_unchecked(multiplier_val) });
        let config = GlobalAllocatorConfig {
            multipliers: vec![multiplier],
        };
        let mut memories = GlobalAllocator::new(config).unwrap().0;
        let memory = memories.pop().unwrap();

        let pool = HybridPool::new(memory).unwrap();
        let res = pool.alloc_mem(NonZeroUsize::new(4096).unwrap());
        match res {
            AllocResult::Allocated {
                cap, global_index, ..
            } => {
                assert_eq!(cap.get(), 4096);
                assert!(global_index.is_none());
            }
            _ => panic!("Alloc failed"),
        }
    }
}
