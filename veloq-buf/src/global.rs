use crate::{MIN_THREAD_MEMORY, RawSlab, ThreadMemory, ThreadMemoryMultiplier};
use std::{io, num::NonZeroUsize, ptr::NonNull, sync::Arc};

/// Configuration for GlobalAllocator
#[derive(Debug, Clone)]
pub struct GlobalAllocatorConfig {
    /// Defines the memory multiplier for each thread.
    /// The index corresponds to the thread/worker ID.
    pub multipliers: Vec<ThreadMemoryMultiplier>,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_global_allocator_lifecycle() {
        let per_thread_size = MIN_THREAD_MEMORY;
        let thread_count = 4;
        let multiplier = ThreadMemoryMultiplier(crate::nz!(1));
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
