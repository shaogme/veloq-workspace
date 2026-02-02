pub mod buffer;
mod os;

use std::{io, num::NonZeroUsize, ptr::NonNull, sync::Arc};

/// 2MB minimum memory per thread (Huge Page aligned)
pub const MIN_THREAD_MEMORY: NonZeroUsize = nz!(2 * 1024 * 1024);

/// Multiplier for thread memory scaling.
/// Each unit represents `MIN_THREAD_MEMORY` (2MB).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ThreadMemoryMultiplier(pub NonZeroUsize);

/// Configuration for GlobalAllocator
#[derive(Debug, Clone)]
pub struct GlobalAllocatorConfig {
    /// Defines the memory multiplier for each thread.
    /// The index corresponds to the thread/worker ID.
    pub multipliers: Vec<ThreadMemoryMultiplier>,
}

/// Underlying physical memory block (RAII Wrapper).
/// Responsible for actual OS memory allocation and deallocation.
#[derive(Debug)]
struct RawSlab {
    ptr: NonNull<u8>,
    size: NonZeroUsize,
}

// Guarantee RawSlab can be shared across threads (needed for Arc internals)
unsafe impl Send for RawSlab {}
unsafe impl Sync for RawSlab {}

impl RawSlab {
    fn new(size: NonZeroUsize) -> io::Result<Self> {
        let ptr = unsafe {
            // Reuse existing os::alloc_huge_pages logic
            os::alloc_huge_pages(size).and_then(|p| {
                NonNull::new(p).ok_or(io::Error::new(io::ErrorKind::Other, "Allocation failed"))
            })?
        };
        Ok(Self { ptr, size })
    }
}

impl Drop for RawSlab {
    fn drop(&mut self) {
        unsafe {
            os::free_huge_pages(self.ptr, self.size);
        }
    }
}

/// Thread-exclusive memory handle.
///
/// This structure will be passed to the runtime's Allocator.
/// It holds a reference to the underlying memory, ensuring validity during usage.
#[derive(Debug)]
pub struct ThreadMemory {
    // Holds Arc to keep the underlying huge chunk alive.
    // Even if multiple threads share the same physical Huge Page memory,
    // their ThreadMemory instances are independent handles.
    _owner: Arc<RawSlab>,

    // The memory region available to this thread
    ptr: NonNull<u8>,
    len: NonZeroUsize,
}

// Allow passing ownership across threads
unsafe impl Send for ThreadMemory {}
// Allow sharing references across threads (though typically Allocators are thread-local)
unsafe impl Sync for ThreadMemory {}

impl ThreadMemory {
    /// Get the start pointer of the memory region
    #[inline]
    pub fn as_ptr(&self) -> *const u8 {
        self.ptr.as_ptr()
    }

    /// Get the mutable start pointer of the memory region
    #[inline]
    pub fn as_mut_ptr(&mut self) -> *mut u8 {
        self.ptr.as_ptr()
    }

    /// Get the length of the memory region
    #[inline]
    pub fn len(&self) -> usize {
        self.len.get()
    }

    /// Obtain the global physical memory region this thread memory belongs to.
    /// Returns (base_ptr, total_size).
    /// Used for registering the entire memory block with the kernel (e.g., io_uring).
    pub fn global_region(&self) -> (NonNull<u8>, usize) {
        (self._owner.ptr, self._owner.size.get())
    }
}

/// Global memory allocator factory.
///
/// Currently acts as a pure factory. Can be extended to hold weak references
/// to all RawSlabs for monitoring or management interfaces to support expansion.
pub struct GlobalAllocator;

/// Information about the global memory block for Driver Registration (God View).
#[derive(Debug, Clone, Copy)]
pub struct GlobalMemoryInfo {
    pub ptr: NonNull<u8>,
    pub len: NonZeroUsize,
}

// Guarantee thread safety for the info pointing to shared memory
unsafe impl Send for GlobalMemoryInfo {}
unsafe impl Sync for GlobalMemoryInfo {}

impl GlobalAllocator {
    /// Create a new global memory allocation.
    ///
    /// # Arguments
    /// - `config`: Configuration containing thread memory sizes.
    ///
    /// # Returns
    /// Returns a tuple containing:
    /// 1. A vector of `ThreadMemory` objects, each having independent ownership.
    /// 2. `GlobalMemoryInfo` for registering the entire block with io_uring/RIO.
    pub fn new(config: GlobalAllocatorConfig) -> io::Result<(Vec<ThreadMemory>, GlobalMemoryInfo)> {
        if config.multipliers.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Thread multipliers cannot be empty",
            ));
        }

        let total_size: usize = config
            .multipliers
            .iter()
            .map(|m| MIN_THREAD_MEMORY.get() * m.0.get())
            .sum();

        // 1. Allocate a single large chunk (Maximizes utilization, huge page friendly)
        // SAFETY: total_size is guaranteed to be non-zero because config.thread_sizes is not empty.
        let slab = Arc::new(RawSlab::new(unsafe {
            NonZeroUsize::new_unchecked(total_size)
        })?);

        let global_info = GlobalMemoryInfo {
            ptr: slab.ptr,
            len: slab.size,
        };

        let mut result = Vec::with_capacity(config.multipliers.len());
        let mut current_ptr = slab.ptr.as_ptr();

        // 2. Slice it for each thread
        for &multiplier in &config.multipliers {
            let size = unsafe {
                NonZeroUsize::new_unchecked(MIN_THREAD_MEMORY.get() * multiplier.0.get())
            };
            let thread_ptr = unsafe { NonNull::new_unchecked(current_ptr) };

            result.push(ThreadMemory {
                _owner: slab.clone(), // Increment ref count
                ptr: thread_ptr,
                len: size,
            });

            // Move pointer forward
            unsafe {
                current_ptr = current_ptr.add(size.get());
            }
        }

        Ok((result, global_info))
    }
}

/// 创建 NonZeroUsize 的宏
/// - 输入 0：编译失败
/// - 输入非 0 字面量/常量：编译通过，且无运行时开销
#[macro_export]
macro_rules! nz {
    ($value:expr) => {{
        // 1. 利用匿名常量强制进行编译时检查
        // 如果 $value 为 0，assert! 会 panic，导致编译中断
        const _: () = assert!($value != 0, "nz! macro: Value cannot be zero!");

        // 2. 如果上面通过了，说明 $value 肯定不为 0
        // 使用 unsafe 块调用 new_unchecked
        unsafe { NonZeroUsize::new_unchecked($value) }
    }};
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_global_allocator_lifecycle() {
        let per_thread_size = MIN_THREAD_MEMORY;
        let thread_count = 4;
        let multiplier = ThreadMemoryMultiplier(nz!(1));
        let config = GlobalAllocatorConfig {
            multipliers: vec![multiplier; thread_count],
        };

        match GlobalAllocator::new(config) {
            Ok((memories, _)) => {
                assert_eq!(memories.len(), thread_count);

                for (i, mem) in memories.iter().enumerate() {
                    assert_eq!(mem.len(), per_thread_size.get());

                    // Simple write test to verify access
                    unsafe {
                        let ptr = mem.as_ptr() as *mut u8;
                        *ptr = i as u8;
                        assert_eq!(*ptr, i as u8);
                    }
                }
            }
            Err(e) => {
                eprintln!(
                    "GlobalAllocator::new failed (likely due to missing permissions): {}",
                    e
                );
            }
        }
    }
}
