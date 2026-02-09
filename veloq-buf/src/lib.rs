mod buffer;
mod os;

pub mod block;
pub mod global;

use std::{num::NonZeroUsize, ptr::NonNull, sync::Arc};

pub use buffer::*;

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
        unsafe { std::num::NonZeroUsize::new_unchecked($value) }
    }};
}

/// 2MB minimum memory per thread (Huge Page aligned)
pub const MIN_THREAD_MEMORY: NonZeroUsize = crate::nz!(2 * 1024 * 1024);

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
    fn new(size: NonZeroUsize) -> std::io::Result<Self> {
        let ptr = unsafe {
            // Reuse existing os::alloc_huge_pages logic
            crate::os::alloc_huge_pages(size).and_then(|p| {
                NonNull::new(p).ok_or(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "Allocation failed",
                ))
            })?
        };
        Ok(Self { ptr, size })
    }
}

impl Drop for RawSlab {
    fn drop(&mut self) {
        unsafe {
            crate::os::free_huge_pages(self.ptr, self.size);
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

    /// Create a standalone ThreadMemory instance (e.g., for testing or single-threaded use).
    /// This allocates a dedicated RawSlab.
    pub fn new_standalone(size: NonZeroUsize) -> std::io::Result<Self> {
        let slab = Arc::new(RawSlab::new(size)?);
        Ok(Self {
            _owner: slab.clone(),
            ptr: slab.ptr,
            len: slab.size,
        })
    }
}

/// Multiplier for thread memory scaling.
/// Each unit represents `MIN_THREAD_MEMORY` (2MB).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ThreadMemoryMultiplier(pub NonZeroUsize);
