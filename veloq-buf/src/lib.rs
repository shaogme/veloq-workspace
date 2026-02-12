mod buffer;
mod os;

pub mod global;
pub mod slot;

use std::{num::NonZeroUsize, ptr::NonNull};

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
                    crate::os::alloc_pages(size).map(|p| NonNull::new(p))?
                }
            }
        }
        .ok_or(std::io::Error::new(
            std::io::ErrorKind::Other,
            "Allocation failed",
        ))?;
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
