use super::AllocError;
use crate::ThreadMemory;
use std::alloc::{alloc, dealloc, Layout};
use std::num::NonZeroUsize;
use std::ptr::NonNull;
use veloq_bitset::BitSet;

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

const GLOBAL_ALLOC_CONTEXT: usize = 0xFFFFFFFF;
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
    pub is_registered: bool,
}

/// Core allocator logic, managing slabs and global fallback
/// Independent of BufPool trait for easier testing
pub struct HybridAllocator {
    memory: ThreadMemory,
    slabs: Vec<Slab>,
}

// SAFETY: HybridAllocator 管理自己的内存，指针指向的是它拥有的内存区域。
// 整个结构体可以安全地跨线程传递（虽然不应该同时从多个线程访问）。
unsafe impl Send for HybridAllocator {}

impl HybridAllocator {
    pub fn new(mut memory: ThreadMemory) -> Result<Self, AllocError> {
        let mut total_arena_size = 0;
        for config in SLABS.iter() {
            total_arena_size += config.block_size * config.count;
        }

        if memory.len() < total_arena_size {
            return Err(AllocError::Oom);
        }

        let arena_base_ptr = memory.as_mut_ptr();
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
                    // Bug Fix: Use as_mut_ptr to ensure correct provenance
                    let block_ptr = unsafe { self.memory.as_mut_ptr().add(block_offset) };

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
                        is_registered: true,
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
                is_registered: false,
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
            // Bug Fix: Use as_mut_ptr to ensure correct provenance for mutable access
            let block_ptr = unsafe { self.memory.as_mut_ptr().add(offset) };
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

// 实现 RawAllocator trait for HybridAllocator
impl crate::block::RawAllocator for HybridAllocator {
    fn alloc(&mut self, size: usize) -> Option<crate::block::RawAllocResult> {
        let raw = self.alloc(size)?;
        Some(crate::block::RawAllocResult {
            ptr: raw.ptr,
            cap: unsafe { NonZeroUsize::new_unchecked(raw.cap) },
            context: raw.context,
            is_registered: raw.is_registered,
        })
    }

    unsafe fn dealloc(&mut self, ptr: NonNull<u8>, cap: usize, context: usize) {
        unsafe {
            let _ = self.dealloc(ptr, cap, context);
        }
    }

    fn global_region(&self) -> (NonNull<u8>, usize) {
        self.memory.global_region()
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
        // Use standalone memory
        let size = ARENA_SIZE;
        let memory = crate::ThreadMemory::new_standalone(size).unwrap();

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
        // Use standalone memory
        let size = ARENA_SIZE;
        let memory = crate::ThreadMemory::new_standalone(size).unwrap();

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
        // Use standalone memory
        let size = ARENA_SIZE;
        let memory = crate::ThreadMemory::new_standalone(size).unwrap();

        let mut allocator = HybridAllocator::new(memory).unwrap();
        // Request 1MB
        let raw = allocator.alloc(1024 * 1024).unwrap();
        assert!(raw.cap >= 1024 * 1024);
        assert_eq!(raw.context, GLOBAL_ALLOC_CONTEXT);

        unsafe { allocator.dealloc(raw.ptr, raw.cap, raw.context).unwrap() };
    }
}
