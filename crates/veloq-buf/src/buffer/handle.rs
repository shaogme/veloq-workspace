//! Core Buffer handle and views.

mod range;
pub use range::{BufIoRangeBound, BufIoRangeError, BufIoRangeErrorKind, BufIoRangeErrorValues};

use veloq_std::{
    mem::{ManuallyDrop, align_of, size_of},
    num::NonZeroUsize,
    ops::Range,
    ptr::NonNull,
    slice::{from_raw_parts, from_raw_parts_mut},
    sync::atomic::{NativeAtomicU64, Ordering},
};

use bilge::prelude::*;

use super::{
    common::{BufError, BufResult, PoolKind, RegionInfo, RegistrationCapability},
    slot_pool::SlotBasedPool,
};
use crate::{heap::ChunkId, os::Platform};
use diagweave::prelude::*;

#[bitsize(64)]
#[derive(FromBits, DebugBits, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PackedContext {
    slot_idx: u32,
    order: u8,
    chunk_id: u16,
    registered: bool,
    reserved: u6,
    pool_kind: PoolKind,
}

#[repr(C, align(4096))]
pub(crate) struct HeapControlBlock {
    total_size: NonZeroUsize,
}

#[derive(Debug)]
#[repr(C, align(32))]
pub struct FixedBuf {
    ptr: NonNull<u8>,
    pool_data: NonNull<()>,
    context: PackedContext,
    len: u32,
    cap: u32,
}

pub struct FixedBufSpec {
    pub ptr: NonNull<u8>,
    pub cap: NonZeroUsize,
    pub len: usize,
    pub pool_data: NonNull<()>,
    pub pool_kind: PoolKind,
    pub context: u64,
    pub registration: RegistrationCapability,
}

#[derive(Debug, Clone)]
pub struct FixedBufView<'a> {
    buf: &'a FixedBuf,
    range: Range<usize>,
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
        registered: bool,
    ) -> Self {
        Self::new(slot_idx, order, chunk_id.raw(), registered, pool_kind)
    }

    pub(crate) fn slot_chunk_id(&self) -> ChunkId {
        ChunkId::from_raw(self.chunk_id())
    }

    pub(crate) fn allocation_order(&self) -> usize {
        self.order() as usize
    }

    pub(crate) fn allocation_slot_index(&self) -> usize {
        self.slot_idx() as usize
    }
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

    pub(crate) fn is_registered(&self) -> bool {
        self.context.registered()
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
    pub unsafe fn new(spec: FixedBufSpec) -> Self {
        assert!(spec.len <= spec.cap.get(), "len must be <= capacity");
        assert!(
            spec.cap.get() <= u32::MAX as usize,
            "FixedBuf only supports capacity <= u32::MAX"
        );

        let mut context = PackedContext::from(spec.context);
        context.set_pool_kind(spec.pool_kind);
        let registered = matches!(
            spec.registration,
            RegistrationCapability::RegisteredChunk(_)
        );
        context.set_registered(registered);
        if let RegistrationCapability::RegisteredChunk(chunk_id) = spec.registration {
            assert_eq!(context.chunk_id(), chunk_id.raw());
        }

        unsafe { Self::from_parts(spec.ptr, spec.pool_data, context, spec.len, spec.cap.get()) }
    }

    /// Resolve which region this buffer belongs to and its offset.
    /// This is used for driver submission (RIO / io_uring).
    ///
    /// The interpretation of the region index is pool-dependent.
    pub fn resolve_region_info(&self) -> RegionInfo {
        let mut info = match self.pool_kind() {
            PoolKind::SlotBased => unsafe {
                SlotBasedPool::resolve_region_info(self.pool_data, self)
            },
            PoolKind::Heap => self.heap_resolve_region_info(),
        };
        info.registration = if self.is_registered() {
            RegistrationCapability::RegisteredChunk(info.id)
        } else {
            RegistrationCapability::Unregistered
        };
        info
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

    /// Consume this handle and return an owned handle for a sub-range.
    ///
    /// The returned handle keeps the original allocation's release metadata, so it
    /// remains the single owner of the allocation. The original handle is consumed
    /// and must not be dropped separately.
    pub fn into_subbuf(self, range: Range<usize>) -> Self {
        let start = range.start;
        let end = range.end;
        assert!(start <= end, "sub-buffer start must be <= end");
        assert!(end <= self.len(), "sub-buffer end must be <= buffer len");

        let len = end - start;
        let this = ManuallyDrop::new(self);
        // SAFETY: `start..end` is within the initialized length, therefore the
        // derived pointer and range are within the allocation owned by `this`.
        // `this` is wrapped in `ManuallyDrop`, so only the returned handle runs
        // the pool deallocation path.
        unsafe {
            Self::from_parts(
                NonNull::new_unchecked(this.ptr.as_ptr().add(start)),
                this.pool_data,
                this.context,
                len,
                len,
            )
        }
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
                BufIoRangeErrorValues {
                    buffer_offset: buf_offset,
                    buffer_length: self.len(),
                    buffer_capacity: self.capacity(),
                    buffer_bound: bound,
                    buffer_bound_kind: bound_kind,
                    submission_length: 0,
                },
            ));
        }

        let submission_length = bound - buf_offset;
        u32::try_from(submission_length).map_err(|_| {
            BufIoRangeError::new(
                BufIoRangeErrorKind::LengthExceedsU32,
                BufIoRangeErrorValues {
                    buffer_offset: buf_offset,
                    buffer_length: self.len(),
                    buffer_capacity: self.capacity(),
                    buffer_bound: bound,
                    buffer_bound_kind: bound_kind,
                    submission_length,
                },
            )
        })
    }

    /// Allocate a heap buffer with `len == capacity`.
    pub fn alloc_heap_full(cap: NonZeroUsize) -> BufResult<Self> {
        Self::alloc_heap(cap, cap.get())
    }

    /// Allocate a buffer from the system heap (not from a pool).
    ///
    /// This is used as a fallback when the pool is full.
    /// Note: Heap-allocated buffers may not be registered with the I/O driver
    /// and thus may incur overhead for direct I/O operations.
    pub fn alloc_heap(cap: NonZeroUsize, len: usize) -> BufResult<Self> {
        assert!(len <= cap.get(), "len must be <= capacity");
        // Allocate space for the metadata block plus the payload.
        // We use os::alloc_pages to ensure page alignment for both.
        let total_size = cap.get().checked_add(4096).ok_or(BufError::Oom)?;
        let total_size_nz = unsafe { NonZeroUsize::new_unchecked(total_size) };

        let base_ptr = unsafe { Platform::alloc_pages(total_size_nz) }.trans()?;

        // Initialize the control block in the first page
        let control = unsafe { &mut *(base_ptr as *mut HeapControlBlock) };
        control.total_size = total_size_nz;

        let ptr = unsafe { NonNull::new_unchecked(base_ptr.add(4096)) };

        static HEAP_BUF_COOKIE_GEN: NativeAtomicU64 = NativeAtomicU64::new(1);
        let cookie = HEAP_BUF_COOKIE_GEN.fetch_add(1, Ordering::Relaxed) & 0x00FFFFFFFFFFFFFF;

        Ok(unsafe {
            Self::new(FixedBufSpec {
                ptr,
                cap,
                len,
                pool_data: NonNull::new_unchecked(base_ptr as *mut ()),
                pool_kind: PoolKind::Heap,
                context: cookie,
                registration: RegistrationCapability::Unregistered,
            })
        })
    }

    #[inline(always)]
    unsafe fn heap_dealloc(pool_data: NonNull<()>) {
        let base_ptr = pool_data.as_ptr() as *mut u8;
        let control_ptr = base_ptr as *const HeapControlBlock;
        let total_size = unsafe { (*control_ptr).total_size };
        unsafe { Platform::free_pages(NonNull::new_unchecked(base_ptr), total_size) };
    }

    #[inline(always)]
    fn heap_resolve_region_info(&self) -> RegionInfo {
        let base = self.pool_data.as_ptr() as usize + 4096;
        let ptr = self.as_ptr() as usize;

        RegionInfo {
            pool_kind: PoolKind::Heap,
            registration: RegistrationCapability::Unregistered,
            id: ChunkId::ZERO,
            base_ptr: unsafe { NonNull::new_unchecked(base as *mut u8) },
            region_len: unsafe {
                NonZeroUsize::new_unchecked(
                    (*((self.pool_data.as_ptr()) as *const HeapControlBlock))
                        .total_size
                        .get()
                        - 4096,
                )
            },
            offset: ptr.saturating_sub(base),
            cookie: self.context_raw(),
        }
    }
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
                    SlotBasedPool::dealloc(self.pool_data, self.context_raw());
                }
                PoolKind::Heap => {
                    Self::heap_dealloc(self.pool_data);
                }
            }
        }
    }
}
