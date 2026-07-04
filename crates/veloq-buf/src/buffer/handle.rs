//! Core Buffer handle and views.

use veloq_std::{
    mem::{align_of, size_of},
    num::NonZeroUsize,
    ops::Range,
    ptr::NonNull,
    slice::{from_raw_parts, from_raw_parts_mut},
    sync::atomic::{AtomicU64, Ordering},
};

use bilge::prelude::*;

mod range;
pub use range::{BufIoRangeBound, BufIoRangeError, BufIoRangeErrorKind};

use super::{
    common::{PoolKind, RegionInfo},
    error::{BufError, BufResult},
    slot_pool::{slot_based_dealloc, slot_based_resolve_region_info},
};
use crate::{
    heap::ChunkId,
    os::{alloc_pages, free_pages},
};
use diagweave::prelude::*;

#[bitsize(64)]
#[derive(FromBits, DebugBits, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PackedContext {
    pub slot_idx: u32,
    pub order: u8,
    pub chunk_id: u16,
    pub reserved: u7,
    pub pool_kind: PoolKind,
}

#[repr(C, align(4096))]
pub(crate) struct HeapControlBlock {
    pub(crate) total_size: NonZeroUsize,
}

impl PackedContext {
    pub(crate) fn raw_payload(&self) -> u64 {
        u64::from(*self) & 0x00FFFFFFFFFFFFFF
    }

    pub(crate) fn from_slot_parts(
        slot_idx: u32,
        order: u8,
        chunk_id: ChunkId,
        pool_kind: PoolKind,
    ) -> Self {
        Self::new(slot_idx, order, chunk_id.0, pool_kind)
    }

    pub(crate) fn slot_chunk_id(&self) -> ChunkId {
        ChunkId::from_raw(self.chunk_id())
    }
}

#[derive(Debug)]
#[repr(C, align(32))]
pub struct FixedBuf {
    pub(crate) ptr: NonNull<u8>,
    // Metadata moved from Heap Header to Handle
    pub(crate) pool_data: NonNull<()>,
    pub(crate) context: PackedContext, // [pool_kind:1 | reserved:7 | context_payload:56]
    pub(crate) len: u32,
    /// The actual capacity of this specific handle/view.
    pub(crate) cap: u32,
}

const _: [(); 32] = [(); size_of::<FixedBuf>()];
const _: [(); 32] = [(); align_of::<FixedBuf>()];

// Safety: FixedBuf 拥有其底层内存的所有权。
unsafe impl Send for FixedBuf {}
// Safety: shared access only exposes immutable reads; mutation requires `&mut FixedBuf`.
unsafe impl Sync for FixedBuf {}

impl FixedBuf {
    pub(crate) fn pool_kind(&self) -> PoolKind {
        self.context.pool_kind()
    }

    pub(crate) fn context_raw(&self) -> u64 {
        self.context.raw_payload()
    }

    pub(crate) fn capacity_usize(&self) -> usize {
        self.cap as usize
    }

    pub(crate) unsafe fn from_parts(
        ptr: NonNull<u8>,
        pool_data: NonNull<()>,
        context: PackedContext,
        len: usize,
        cap: usize,
    ) -> Self {
        assert!(len <= cap, "len must be <= capacity");
        assert!(
            cap <= u32::MAX as usize,
            "FixedBuf only supports capacity <= u32::MAX"
        );

        Self {
            ptr,
            pool_data,
            context,
            len: len as u32,
            cap: cap as u32,
        }
    }

    /// # Safety
    /// `ptr` must be valid and allocated by the pool associated with `pool_kind`.
    pub unsafe fn new(
        ptr: NonNull<u8>,
        cap: NonZeroUsize,
        pool_data: NonNull<()>,
        pool_kind: PoolKind,
        context: u64,
    ) -> Self {
        assert!(
            cap.get() <= u32::MAX as usize,
            "FixedBuf only supports capacity <= u32::MAX"
        );

        let mut context = PackedContext::from(context);
        context.set_pool_kind(pool_kind);

        unsafe { Self::from_parts(ptr, pool_data, context, cap.get(), cap.get()) }
    }

    /// Resolve which region this buffer belongs to and its offset.
    /// This is used for driver submission (RIO / io_uring).
    ///
    /// The interpretation of the region index is pool-dependent.
    pub fn resolve_region_info(&self) -> RegionInfo {
        match self.pool_kind() {
            PoolKind::SlotBased => unsafe { slot_based_resolve_region_info(self.pool_data, self) },
            PoolKind::Heap => heap_resolve_region_info(self),
        }
    }

    pub fn as_slice(&self) -> &[u8] {
        unsafe { from_raw_parts(self.ptr.as_ptr(), self.len as usize) }
    }

    pub fn as_slice_mut(&mut self) -> &mut [u8] {
        unsafe { from_raw_parts_mut(self.ptr.as_ptr(), self.len as usize) }
    }

    /// Access the full capacity as a mutable slice for writing data before set_len is called.
    pub fn spare_capacity_mut(&mut self) -> &mut [u8] {
        unsafe { from_raw_parts_mut(self.ptr.as_ptr(), self.capacity_usize()) }
    }

    pub fn as_ptr(&self) -> *const u8 {
        self.ptr.as_ptr()
    }

    // Pointer to start of capacity
    pub fn as_mut_ptr(&mut self) -> *mut u8 {
        self.ptr.as_ptr()
    }

    pub fn capacity(&self) -> usize {
        self.capacity_usize()
    }

    pub fn len(&self) -> usize {
        self.len as usize
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn set_len(&mut self, len: usize) {
        assert!(
            len <= self.capacity_usize(),
            "len must be <= buffer capacity"
        );
        assert!(len <= u32::MAX as usize, "len exceeds u32::MAX");
        self.len = len as u32;
    }

    /// Create a borrowed sub-view of the buffer.
    ///
    /// The returned view borrows `self` and does not change ownership.
    pub fn view(&self, range: Range<usize>) -> FixedBufView<'_> {
        FixedBufView::new(self, range)
    }

    #[inline]
    pub fn checked_read_range(
        &mut self,
        buf_offset: usize,
    ) -> Result<(*mut u8, u32), BufIoRangeError> {
        let len = self.checked_buf_io_len(buf_offset, BufIoRangeBound::Capacity)?;
        // SAFETY: buf_offset is verified to be within 0..=capacity above.
        let ptr = unsafe { self.as_mut_ptr().add(buf_offset) };
        Ok((ptr, len))
    }

    #[inline]
    pub fn checked_write_range(
        &self,
        buf_offset: usize,
    ) -> Result<(*const u8, u32), BufIoRangeError> {
        let len = self.checked_buf_io_len(buf_offset, BufIoRangeBound::Length)?;
        // SAFETY: buf_offset is verified to be within 0..=len above, and len <= capacity.
        let ptr = unsafe { self.as_ptr().add(buf_offset) };
        Ok((ptr, len))
    }

    #[inline]
    fn checked_buf_io_len(
        &self,
        buf_offset: usize,
        bound_kind: BufIoRangeBound,
    ) -> Result<u32, BufIoRangeError> {
        let bound = match bound_kind {
            BufIoRangeBound::Capacity => self.capacity(),
            BufIoRangeBound::Length => self.len(),
        };

        if buf_offset > bound {
            return Err(BufIoRangeError::new(
                BufIoRangeErrorKind::OffsetOutOfBounds,
                buf_offset,
                self.len(),
                self.capacity(),
                bound,
                bound_kind,
                0,
            ));
        }

        let submission_length = bound - buf_offset;
        u32::try_from(submission_length).map_err(|_| {
            BufIoRangeError::new(
                BufIoRangeErrorKind::LengthExceedsU32,
                buf_offset,
                self.len(),
                self.capacity(),
                bound,
                bound_kind,
                submission_length,
            )
        })
    }

    /// Allocate a buffer from the system heap (not from a pool).
    ///
    /// This is used as a fallback when the pool is full.
    /// Note: Heap-allocated buffers may not be registered with the I/O driver
    /// and thus may incur overhead for direct I/O operations.
    pub fn alloc_heap(len: NonZeroUsize) -> BufResult<Self> {
        // Allocate space for the metadata block plus the payload.
        // We use os::alloc_pages to ensure page alignment for both.
        let total_size = len.get().checked_add(4096).ok_or(BufError::Oom)?;
        let total_size_nz = unsafe { NonZeroUsize::new_unchecked(total_size) };

        let base_ptr = unsafe { alloc_pages(total_size_nz) }.trans()?;

        // Initialize the control block in the first page
        let control = unsafe { &mut *(base_ptr as *mut HeapControlBlock) };
        control.total_size = total_size_nz;

        let ptr = unsafe { NonNull::new_unchecked(base_ptr.add(4096)) };

        static HEAP_BUF_COOKIE_GEN: AtomicU64 = AtomicU64::new(1);
        let cookie = HEAP_BUF_COOKIE_GEN.fetch_add(1, Ordering::Relaxed) & 0x00FFFFFFFFFFFFFF;

        Ok(unsafe {
            Self::new(
                ptr,
                len,
                NonNull::new_unchecked(base_ptr as *mut ()),
                PoolKind::Heap,
                cookie,
            )
        })
    }
}

#[derive(Debug, Clone)]
pub struct FixedBufView<'a> {
    pub(crate) buf: &'a FixedBuf,
    pub(crate) range: Range<usize>,
}

impl<'a> FixedBufView<'a> {
    pub fn new(buf: &'a FixedBuf, range: Range<usize>) -> Self {
        let start = range.start;
        let end = range.end;
        assert!(start <= end, "view start must be <= end");
        assert!(end <= buf.len(), "view end must be <= buffer len");

        Self { buf, range }
    }

    pub fn buf(&self) -> &'a FixedBuf {
        self.buf
    }

    pub fn range(&self) -> &Range<usize> {
        &self.range
    }

    pub fn start(&self) -> usize {
        self.range.start
    }

    pub fn end(&self) -> usize {
        self.range.end
    }

    pub fn len(&self) -> usize {
        self.range.end - self.range.start
    }

    pub fn capacity(&self) -> usize {
        self.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn as_ptr(&self) -> *const u8 {
        unsafe { self.buf.as_ptr().add(self.start()) }
    }

    pub fn as_slice(&self) -> &'a [u8] {
        unsafe { from_raw_parts(self.as_ptr(), self.len()) }
    }
}

impl Drop for FixedBuf {
    fn drop(&mut self) {
        unsafe {
            match self.pool_kind() {
                PoolKind::SlotBased => {
                    slot_based_dealloc(self.pool_data, self.context_raw());
                }
                PoolKind::Heap => {
                    heap_dealloc(self.pool_data);
                }
            }
        }
    }
}

#[inline(always)]
pub(crate) unsafe fn heap_dealloc(pool_data: NonNull<()>) {
    let base_ptr = pool_data.as_ptr() as *mut u8;
    let control_ptr = base_ptr as *const HeapControlBlock;
    let total_size = unsafe { (*control_ptr).total_size };
    unsafe {
        free_pages(NonNull::new_unchecked(base_ptr), total_size);
    }
}

#[inline(always)]
pub(crate) fn heap_resolve_region_info(buf: &FixedBuf) -> RegionInfo {
    // Heap-allocated payload starts after the 4KB control block.
    let base = buf.pool_data.as_ptr() as usize + 4096;
    let ptr = buf.as_ptr() as usize;

    RegionInfo {
        pool_kind: PoolKind::Heap,
        id: ChunkId::ZERO,
        offset: ptr.saturating_sub(base),
        cookie: buf.context_raw(),
    }
}
