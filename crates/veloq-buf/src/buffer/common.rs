//! Common traits and types for buffer management.

use veloq_std::{
    fmt::{Debug, Formatter, Result as FmtResult},
    num::{NonZeroU16, NonZeroUsize},
    ptr::NonNull,
    vec::Vec,
};

use bilge::prelude::*;

use super::{error::BufResult, handle::FixedBuf};
use crate::heap::{ChunkId, PageAlignedBytes};

#[bitsize(1)]
#[derive(FromBits, Debug, Clone, Copy, PartialEq, Eq)]
pub enum PoolKind {
    SlotBased,
    Heap,
}

/// A wrapper for `u16` that guarantees it never equals `S`.
/// This enables `Option<NotU16<S>>` to have the same size as `u16`.
#[derive(Copy, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[repr(transparent)]
pub struct NotU16<const S: u16>(NonZeroU16);

impl<const S: u16> NotU16<S> {
    /// Creates a new instance.
    /// Returns `None` if `n` equals `S`.
    #[inline]
    pub const fn new(n: u16) -> Option<Self> {
        match NonZeroU16::new(n ^ S) {
            Some(inner) => Some(Self(inner)),
            None => None,
        }
    }

    /// Creates a new instance without checking.
    ///
    /// # Safety
    /// `n` must not equal `S`.
    #[inline]
    pub const unsafe fn new_unchecked(n: u16) -> Self {
        debug_assert!(n != S, "Value must not be the sentinel value");
        Self(unsafe { NonZeroU16::new_unchecked(n ^ S) })
    }

    /// Returns the primitive value.
    #[inline]
    pub const fn get(self) -> u16 {
        self.0.get() ^ S
    }
}

impl<const S: u16> Debug for NotU16<S> {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(f, "{}", self.get())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RegionInfo {
    pub pool_kind: PoolKind,
    pub id: ChunkId,
    pub offset: usize,
    /// A unique cookie used to distinguish different allocations for the same pointer (e.g. heap reuse).
    pub cookie: u64,
}

#[derive(Debug)]
pub enum AllocResult {
    Allocated {
        ptr: NonNull<u8>,
        cap: NonZeroUsize,
        context: u64,
    },
    Failed,
}

impl AllocResult {
    pub fn into_buf(self, pool: &dyn BackingPool) -> Option<FixedBuf> {
        match self {
            AllocResult::Allocated { ptr, cap, context } => unsafe {
                Some(FixedBuf::new(
                    ptr,
                    cap,
                    pool.pool_data(),
                    pool.pool_kind(),
                    context,
                ))
            },
            AllocResult::Failed => None,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct BufferRegion {
    pub(crate) id: ChunkId,
    pub(crate) ptr: NonNull<u8>,
    pub(crate) len: PageAlignedBytes,
}

impl BufferRegion {
    pub fn new(id: ChunkId, ptr: NonNull<u8>, len: PageAlignedBytes) -> Self {
        Self { id, ptr, len }
    }

    pub fn from_chunk_info(info: crate::heap::ChunkInfo) -> Option<Self> {
        PageAlignedBytes::new(info.len).map(|len| Self {
            id: info.id,
            ptr: info.ptr,
            len,
        })
    }

    pub fn id(&self) -> ChunkId {
        self.id
    }

    pub fn ptr(&self) -> NonNull<u8> {
        self.ptr
    }

    pub fn as_ptr(&self) -> *const u8 {
        self.ptr.as_ptr()
    }

    pub fn as_mut_ptr(&self) -> *mut u8 {
        self.ptr.as_ptr()
    }

    pub fn len(&self) -> usize {
        self.len.get()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

unsafe impl Send for BufferRegion {}
unsafe impl Sync for BufferRegion {}

/// Trait abstraction for driver-specific buffer registration
pub trait BufferRegistrar {
    /// Register memory regions with the kernel.
    /// Returns a list of handles (tokens) corresponding to the regions.
    /// For RIO this is RIO_BUFFERID, for uring it might be ignored or index.
    fn register(&self, regions: &[BufferRegion]) -> BufResult<Vec<ChunkId>>;

    /// Resolve chunk info for a given chunk_id.
    /// Used for lazy registration.
    fn resolve_chunk_info(&self, chunk_id: ChunkId) -> Option<crate::heap::ChunkInfo>;
}

/// A no-op registrar that does nothing.
pub struct NoopRegistrar;

impl BufferRegistrar for NoopRegistrar {
    fn register(&self, _regions: &[BufferRegion]) -> BufResult<Vec<ChunkId>> {
        Ok(Vec::new())
    }

    fn resolve_chunk_info(&self, _chunk_id: ChunkId) -> Option<crate::heap::ChunkInfo> {
        None
    }
}

/// Memory pool implementation providing raw memory allocation.
/// This trait manages memory layout, allocation algorithms, and deallocation.
/// It does NOT handle driver registration.
pub trait BackingPool: Debug {
    /// Allocate memory without registration context.
    /// Returns allocation result containing ptr, capacity, and header context.
    /// The `global_index` in the result should be ignored or None.
    fn alloc_mem(&self, size: NonZeroUsize) -> AllocResult;

    /// Get pool kind for compact deallocation/dispatch.
    fn pool_kind(&self) -> PoolKind;

    /// Get the raw pool data pointer.
    fn pool_data(&self) -> NonNull<()>;
}

/// High-level Buffer Pool trait.
/// Represents a pool that is ready for I/O operations (registered if necessary).
pub trait BufPool: Debug {
    /// Allocate a buffer ready for I/O.
    fn alloc(&self, len: NonZeroUsize) -> Option<FixedBuf>;
}
