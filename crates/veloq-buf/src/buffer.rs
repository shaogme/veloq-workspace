//! # Buffer Management for High-Performance I/O
//!
//! This module provides abstractions for managing memory buffers compatible with modern
//! asynchronous I/O interfaces (like `io_uring` on Linux and IOCP on Windows).
//!
//! ## Core Components
//!
//! - [`FixedBuf`]: An owned handle to a buffer allocated from a pool. It ensures the underlying
//!   memory remains valid as long as the handle exists. It stores compact pool metadata
//!   (`PoolKind + context`) for deallocation and region resolution.
//! - [`BufPool`]: The base trait for memory pool implementations.
//!
//! ## Implementation Requirements
//!
//! Implementing a `BufPool` requires strict adherence to memory layout rules to support
//! Zero-Copy and Direct I/O (O_DIRECT / FILE_FLAG_NO_BUFFERING) operations.
//!
//! ### 1. Memory Stability
//! The pool must guarantee that the memory pointer in `FixedBuf` remains valid until
//! `dealloc` is called. For `io_uring`, this often means the memory must be registered
//! with the kernel and not moved.
//!
//! ### 2. Direct I/O Alignment (Critical)
//! To support `DirectSync` operations, the buffers returned by the pool must satisfy
//! strict alignment requirements imposed by the OS and hardware drivers:
//!
//! - **Payload Alignment**: The `ptr` points to the start of the user data payload.
//!   This address MUST be aligned to at least **512 bytes** (Sector Size). Ideally, align
//!   to **4096 bytes** (Page Size) for best performance.
//!   
//! - **Backing Memory Alignment**: The underlying allocation (Slab, Arena, or Block)
//!   should be **Page Aligned (4096 bytes)**. This prevents splitting pages across
//!   DRAM boundaries in ways that might degrade DMA performance.
//!
//! ### 3. Metadata Management
//! Unlike traditional implementations that might store metadata in an intrusive header
//! *before* the payload, `FixedBuf` stores necessary context (like pool references and VTable)
//! within the handle itself to avoid alignment complexities.
//!
//! **Implementors MUST ensure:**
//! - The pool-specific deallocation path can correctly free memory using:
//!   - The payload pointer (`ptr`)
//!   - The capacity (`cap`)
//!   - An opaque `context` value (u64) provided during allocation
//!
//! See [`SlotBasedPool`] for the implementation relying on [`crate::heap::GlobalSlotPool`].

mod any;
mod common;
mod error;
mod handle;
mod slot_pool;

pub use any::*;
pub use common::*;
pub use error::*;
pub use handle::*;
pub use slot_pool::*;

#[cfg(test)]
mod tests {
    extern crate std;
    use super::*;
    use std::panic::{AssertUnwindSafe, catch_unwind};
    use veloq_std::num::NonZeroUsize;

    #[test]
    fn heap_view_preserves_borrow_semantics() {
        let buf = FixedBuf::alloc_heap(NonZeroUsize::new(16).expect("non-zero length"))
            .expect("heap allocation failed");
        let mut buf = buf;

        for (idx, byte) in buf.as_slice_mut().iter_mut().enumerate() {
            *byte = idx as u8;
        }

        let view = buf.view(4..12);
        assert_eq!(view.as_ptr(), unsafe { buf.as_ptr().add(4) });
        assert_eq!(view.len(), 8);
        assert_eq!(view.capacity(), 8);
        assert_eq!(view.as_slice(), &[4, 5, 6, 7, 8, 9, 10, 11]);
    }

    #[test]
    fn heap_view_supports_zero_length_ranges() {
        let buf = FixedBuf::alloc_heap(NonZeroUsize::new(8).expect("non-zero length"))
            .expect("heap allocation failed");

        let view = buf.view(0..0);
        assert_eq!(view.len(), 0);
        assert_eq!(view.capacity(), 0);
        assert!(view.as_slice().is_empty());
    }

    #[test]
    fn heap_view_rejects_invalid_range_without_retaining() {
        let buf = FixedBuf::alloc_heap(NonZeroUsize::new(8).expect("non-zero length"))
            .expect("heap allocation failed");

        let start = 3;
        let end = 2;
        let result = catch_unwind(AssertUnwindSafe(|| buf.view(start..end)));
        assert!(result.is_err());

        drop(buf);
    }
}
