//! Common traits and types for buffer management.

use veloq_std::{
    fmt::{Debug, Formatter, Result as FmtResult},
    num::{NonZeroU16, NonZeroUsize},
    ptr::NonNull,
    string::String,
    sync::Arc,
    vec::Vec,
};

use bilge::prelude::*;
use diagweave::{report::Report, set};

use super::{
    handle::{FixedBuf, FixedBufSpec},
    locked_memory::{LockedMemoryBudget, LockedMemoryError, LockedMemoryPermit},
};
use crate::heap::{ChunkId, ChunkInfo, PageAllocationStrategy};

set! {
    pub SystemError = {
        #[display("OS error {0}")]
        Os(i32),
    }

    pub BufError = {
        #[display("Layout error: {0}")]
        Layout(#[from] veloq_std::alloc::LayoutError),

        #[display("Out of memory")]
        Oom,

        #[display("System error: {0}")]
        System(#[from] SystemError),

        #[display("Other error: {0}")]
        Other(String),

        #[display("Allocation failed: {0}")]
        AllocFailed(String),

        #[display("Chunk memory size too small for sharding")]
        ChunkTooSmall,

        #[display("Chunk memory size must be page aligned: {size}")]
        PageUnaligned { size: usize },

        #[display("Chunk {chunk_id} missing")]
        ChunkMissing { chunk_id: ChunkId },

        #[display("FixedBuf has invalid ChunkID")]
        InvalidChunkId,

        #[display("GlobalSlotPool ID overflow")]
        IdOverflow,

        #[display("Buffer pool size calculation overflow")]
        SizeOverflow,
    }
}

pub type BufResult<T> = Result<T, Report<BufError>>;

#[bitsize(1)]
#[derive(FromBits, Debug, Clone, Copy, PartialEq, Eq)]
pub enum PoolKind {
    SlotBased,
    Heap,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RegistrationCapability {
    Unregistered,
    RegisteredChunk(ChunkId),
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
    pub registration: RegistrationCapability,
    pub id: ChunkId,
    pub base_ptr: NonNull<u8>,
    pub region_len: NonZeroUsize,
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
        registration: RegistrationCapability,
    },
    Failed,
}

pub trait BackingPool: Debug + Send + Sync {
    /// Allocate memory without registration context.
    /// Returns allocation result containing ptr, capacity, and header context.
    /// The `global_index` in the result should be ignored or None.
    fn alloc_mem(&self, size: NonZeroUsize) -> AllocResult;

    /// Get pool kind for compact deallocation/dispatch.
    fn pool_kind(&self) -> PoolKind;

    /// Get the raw pool data pointer.
    fn pool_data(&self) -> NonNull<()>;
}

pub trait BufferBackingOwner: Debug + Send + Sync {}

impl<T> BufferBackingOwner for T where T: Debug + Send + Sync {}

#[derive(Debug, Clone, Copy)]
struct LockedMemoryRange {
    ptr: NonNull<u8>,
    len: NonZeroUsize,
}

pub struct BufferRegistrationRequest {
    id: ChunkId,
    ptr: NonNull<u8>,
    len: NonZeroUsize,
    backing_owner: Arc<dyn BufferBackingOwner>,
    locked_footprint: Option<LockedMemoryRange>,
    requires_locked_footprint: bool,
    permit: Option<LockedMemoryPermit>,
}

impl AllocResult {
    pub fn into_buf(self, pool: &dyn BackingPool, len: usize) -> Option<FixedBuf> {
        match self {
            AllocResult::Allocated {
                ptr,
                cap,
                context,
                registration,
            } => unsafe {
                Some(FixedBuf::new(FixedBufSpec {
                    ptr,
                    cap,
                    len,
                    pool_data: pool.pool_data(),
                    pool_kind: pool.pool_kind(),
                    context,
                    registration,
                }))
            },
            AllocResult::Failed => None,
        }
    }
}

#[derive(Debug)]
pub struct BufferRegion {
    id: ChunkId,
    ptr: NonNull<u8>,
    len: NonZeroUsize,
    backing_owner: Arc<dyn BufferBackingOwner>,
    locked_footprint: Option<LockedMemoryRange>,
    requires_locked_footprint: bool,
    permit: Option<LockedMemoryPermit>,
}

impl BufferRegion {
    /// Creates a registration region backed by an owner that keeps its memory stable.
    /// 创建由 backing owner 保持内存有效且地址稳定的注册区域。
    ///
    /// # Safety
    /// `backing_owner` must keep `[ptr, ptr + len)` allocated and unmoved until every cloned owner
    /// is dropped. `allocation_strategy` must match the allocator, and huge-page ranges must
    /// receive their allocator-reported footprint before registration. / `backing_owner` 必须在所有 owner 副本释放前持续分配并固定 `[ptr, ptr + len)`；`allocation_strategy` 必须与分配器一致，大页范围还必须在注册前提供 allocator 报告的 footprint。
    pub unsafe fn new<O>(
        id: ChunkId,
        ptr: NonNull<u8>,
        len: NonZeroUsize,
        backing_owner: Arc<O>,
        allocation_strategy: PageAllocationStrategy,
    ) -> Self
    where
        O: BufferBackingOwner + 'static,
    {
        // SAFETY: this cast only widens `O` to its implemented trait object and preserves the allocation. / 安全性：此转换仅将 `O` 扩展为其实现的 trait 对象，并保留原分配。
        let backing_owner =
            unsafe { Arc::cast_unsized(backing_owner, |ptr| ptr as *const dyn BufferBackingOwner) };
        Self {
            id,
            ptr,
            len,
            backing_owner,
            locked_footprint: None,
            requires_locked_footprint: allocation_strategy == PageAllocationStrategy::Huge,
            permit: None,
        }
    }

    /// Creates a region from allocator metadata and its owning pool.
    /// 根据 allocator 元数据及其所属 pool 创建注册区域。
    ///
    /// # Safety
    /// `backing_owner` must own the exact chunk described by `info` and keep it stable while any
    /// registration owner exists. Huge-page chunks must also receive their allocator-reported
    /// footprint before registration. / `backing_owner` 必须拥有 `info` 描述的 chunk，并在注册 owner 存活期间保持该 chunk 稳定；大页 chunk 还必须在注册前提供 allocator 报告的 footprint。
    pub unsafe fn from_chunk_info<O>(info: ChunkInfo, backing_owner: Arc<O>) -> Self
    where
        O: BufferBackingOwner + 'static,
    {
        unsafe {
            Self::new(
                info.id(),
                info.ptr(),
                info.len(),
                backing_owner,
                info.allocation_strategy(),
            )
        }
    }

    pub fn with_permit(mut self, permit: LockedMemoryPermit) -> Self {
        self.permit = Some(permit);
        self
    }

    /// Supplies the complete page range pinned by the backend for this registration.
    /// 指定后端为此注册实际锁定的完整页范围。
    ///
    /// # Safety
    /// The supplied range must be the allocator's complete locked footprint and must lie within
    /// the registered range. / 提供的范围必须是 allocator 报告的完整锁定 footprint，且必须位于注册范围内。
    pub unsafe fn with_locked_footprint(mut self, ptr: NonNull<u8>, len: NonZeroUsize) -> Self {
        self.locked_footprint = Some(LockedMemoryRange { ptr, len });
        self
    }

    pub(crate) fn set_permit(&mut self, permit: LockedMemoryPermit) {
        self.permit = Some(permit);
    }

    pub fn registration_request(&mut self) -> BufferRegistrationRequest {
        BufferRegistrationRequest {
            id: self.id,
            ptr: self.ptr,
            len: self.len,
            backing_owner: self.backing_owner.clone(),
            locked_footprint: self.locked_footprint,
            requires_locked_footprint: self.requires_locked_footprint,
            permit: self.permit.take(),
        }
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

impl BufferRegistrationRequest {
    pub fn id(&self) -> ChunkId {
        self.id
    }

    pub fn ptr(&self) -> NonNull<u8> {
        self.ptr
    }

    pub fn len(&self) -> usize {
        self.len.get()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn backing_owner(&self) -> Arc<dyn BufferBackingOwner> {
        self.backing_owner.clone()
    }

    pub fn required_charge(&self, budget: &LockedMemoryBudget) -> Result<usize, LockedMemoryError> {
        let Some(footprint) = self.locked_footprint else {
            if self.requires_locked_footprint {
                return Err(LockedMemoryError::MissingLockedFootprint);
            }
            return budget.required_charge(self.ptr.as_ptr(), self.len.get());
        };
        let range_start = self.ptr.as_ptr() as usize;
        let range_end = range_start
            .checked_add(self.len.get())
            .ok_or(LockedMemoryError::ArithmeticOverflow)?;
        let footprint_start = footprint.ptr.as_ptr() as usize;
        let footprint_end = footprint_start
            .checked_add(footprint.len.get())
            .ok_or(LockedMemoryError::ArithmeticOverflow)?;
        if footprint_start < range_start || footprint_end > range_end {
            return Err(LockedMemoryError::InvalidRange);
        }
        budget.required_charge(footprint.ptr.as_ptr(), footprint.len.get())
    }

    pub fn try_reserve(
        &self,
        budget: &LockedMemoryBudget,
    ) -> Result<Option<LockedMemoryPermit>, LockedMemoryError> {
        self.required_charge(budget)?;
        let (ptr, len) = self
            .locked_footprint
            .map_or((self.ptr, self.len), |footprint| {
                (footprint.ptr, footprint.len)
            });
        budget.try_reserve_range(ptr.as_ptr(), len.get())
    }

    pub fn take_permit(&mut self) -> Option<LockedMemoryPermit> {
        self.permit.take()
    }
}

impl Debug for BufferRegistrationRequest {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> FmtResult {
        formatter
            .debug_struct("BufferRegistrationRequest")
            .field("id", &self.id)
            .field("ptr", &self.ptr)
            .field("len", &self.len)
            .field("has_permit", &self.permit.is_some())
            .finish()
    }
}

/// Trait abstraction for driver-specific buffer registration
pub trait BufferRegistrar {
    /// Register memory regions with the kernel.
    /// Returns handles only for regions that were actually registered. / 仅返回已成功注册区域的句柄。
    /// For RIO this is RIO_BUFFERID, for uring it might be ignored or index.
    fn register(&self, regions: &mut [BufferRegion]) -> BufResult<Vec<ChunkId>>;
}

/// A no-op registrar that does nothing.
pub struct NoopRegistrar;

impl BufferRegistrar for NoopRegistrar {
    fn register(&self, _regions: &mut [BufferRegion]) -> BufResult<Vec<ChunkId>> {
        Ok(Vec::new())
    }
}

/// High-level Buffer Pool trait.
/// Represents a pool that is ready for I/O operations (registered if necessary).
pub trait BufPool: Debug {
    /// Allocate a buffer ready for I/O with explicit capacity and initial length.
    fn alloc(&self, cap: NonZeroUsize, len: usize) -> Option<FixedBuf>;

    /// Allocate a buffer with `len == capacity`.
    fn alloc_full(&self, cap: NonZeroUsize) -> Option<FixedBuf> {
        self.alloc(cap, cap.get())
    }
}
