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
//! See [`SlotBasedPool`] for the implementation relying on [`crate::global::GlobalSlotPool`].

use std::{
    alloc::LayoutError,
    cell::Cell,
    num::{NonZeroU16, NonZeroUsize},
    ptr::NonNull,
    sync::Arc,
};

use bilge::prelude::*;

#[bitsize(1)]
#[derive(FromBits, Debug, Clone, Copy, PartialEq, Eq)]
pub enum PoolKind {
    SlotBased,
    Heap,
}

#[bitsize(64)]
#[derive(FromBits, DebugBits, Clone, Copy, PartialEq, Eq)]
pub struct PackedContext {
    pub slot_idx: u32,
    pub order: u8,
    pub chunk_id: u16,
    pub reserved: u7,
    pub pool_kind: PoolKind,
}

#[repr(C, align(4096))]
struct HeapControlBlock {
    ref_count: std::sync::atomic::AtomicU32,
    total_size: NonZeroUsize,
}

impl PackedContext {
    #[inline(always)]
    pub fn raw_payload(&self) -> u64 {
        u64::from(*self) & 0x00FFFFFFFFFFFFFF
    }
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

impl<const S: u16> std::fmt::Debug for NotU16<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.get())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RegionInfo {
    pub pool_kind: PoolKind,
    pub id: u16,
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
    ptr: NonNull<u8>,
    len: NonZeroUsize,
}

impl BufferRegion {
    pub fn new(ptr: NonNull<u8>, len: NonZeroUsize) -> Self {
        Self { ptr, len }
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
    fn register(&self, regions: &[BufferRegion]) -> std::io::Result<Vec<usize>>;

    /// Resolve chunk info for a given chunk_id.
    /// Used for lazy registration.
    fn resolve_chunk_info(&self, chunk_id: u16) -> Option<crate::heap::ChunkInfo>;
}

/// A no-op registrar that does nothing.
pub struct NoopRegistrar;

impl BufferRegistrar for NoopRegistrar {
    fn register(&self, _regions: &[BufferRegion]) -> std::io::Result<Vec<usize>> {
        Ok(Vec::new())
    }

    fn resolve_chunk_info(&self, _chunk_id: u16) -> Option<crate::heap::ChunkInfo> {
        None
    }
}

/// Memory pool implementation providing raw memory allocation.
/// This trait manages memory layout, allocation algorithms, and deallocation.
/// It does NOT handle driver registration.
pub trait BackingPool: std::fmt::Debug {
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
pub trait BufPool: std::fmt::Debug {
    /// Allocate a buffer ready for I/O.
    fn alloc(&self, len: NonZeroUsize) -> Option<FixedBuf>;
}

/// 定义 Runtime 所有工作线程的缓冲池拓扑结构
/// Defines the buffer pool topology for the runtime.
///
/// This trait manages the creation and distribution of buffer pools across worker threads.
/// It allows for diverse configurations:
/// - Shared Global Pools (e.g., `UniformSlot`)
/// - Independent Per-Thread Pools
/// - Hybrid approaches
pub trait PoolTopology: Clone + Send + Sync {
    /// Shared state initialized once at startup.
    /// This is passed to every worker during pool construction.
    /// Must be `Clone` (shared via `Arc` or `&'static`) and thread-safe.
    type State: Clone + Send + Sync;

    /// Initialize the global/shared state.
    /// Called once by the Runtime Builder.
    fn init(&self, worker_count: usize) -> std::io::Result<Self::State>;

    /// Build the `AnyBufPool` for a specific worker.
    /// Called within each worker thread.
    ///
    /// Responsibilities:
    /// 1. Register necessary memory regions via `registrar`.
    /// 2. Construct and return the `AnyBufPool`.
    fn build(
        &self,
        state: &Self::State,
        worker_idx: usize,
        registrar: Box<dyn BufferRegistrar>,
    ) -> AnyBufPool;

    /// Connect a listener to the shared state to receive notifications about new memory chunks.
    /// Used for dynamic expansion.
    #[allow(unused_variables)]
    fn connect_listener(
        &self,
        state: &Self::State,
        listener: Box<dyn Fn(crate::heap::ChunkInfo) + Send + Sync>,
    ) {
        // Default implementation does nothing
    }
}

/// 标准 Slot 拓扑：使用 GlobalSlotPool
#[derive(Clone)]
pub struct UniformSlot {
    /// 内存倍数 (用于计算总内存大小)
    pub multiplier: crate::heap::ThreadMemoryMultiplier,
}

impl UniformSlot {
    /// 创建新的 UniformSlot topology
    pub fn new(multiplier: crate::heap::ThreadMemoryMultiplier) -> Self {
        Self { multiplier }
    }

    // Backward compatibility shim for tests if needed
    pub fn create_pool(
        &self,
        worker_count: usize,
    ) -> std::io::Result<Arc<crate::heap::GlobalSlotPool>> {
        self.init(worker_count)
    }
}

impl PoolTopology for UniformSlot {
    type State = Arc<crate::heap::GlobalSlotPool>;

    fn init(&self, worker_count: usize) -> std::io::Result<Self::State> {
        let total_size =
            self.multiplier.0.get() * crate::heap::MIN_THREAD_MEMORY.get() * 2 * worker_count;
        let config = crate::heap::GlobalAllocatorConfig {
            total_memory: total_size,
        };

        let pool = crate::heap::GlobalSlotPool::new(config)?;

        // Return Arc instead of leaking
        Ok(Arc::new(pool))
    }

    fn build(
        &self,
        pool: &Self::State,
        worker_idx: usize,
        registrar: Box<dyn BufferRegistrar>,
    ) -> AnyBufPool {
        // 在 Slot 架构中，所有线程共享一个大的连续区域
        // Phase 1: For now, we only register the initial chunk (Chunk 0).
        let global_info = pool.chunk_info(0).expect("Chunk 0 missing");
        let region = crate::buffer::BufferRegion::new(global_info.ptr, global_info.len);

        // 注册内存区域
        let _ids = registrar
            .register(&[region])
            .expect("Failed to register global buffer region (check 'ulimit -l' / RLIMIT_MEMLOCK)");

        let slot_pool = SlotBasedPool::with_seed(pool.clone(), worker_idx);
        AnyBufPool::new(slot_pool)
    }

    fn connect_listener(
        &self,
        state: &Self::State,
        listener: Box<dyn Fn(crate::heap::ChunkInfo) + Send + Sync>,
    ) {
        state.set_listener(listener);
    }
}

#[derive(Debug)]
#[repr(C, align(32))]
pub struct FixedBuf {
    ptr: NonNull<u8>,
    // Metadata moved from Heap Header to Handle
    pool_data: NonNull<()>,
    context: PackedContext, // [pool_kind:1 | reserved:7 | context_payload:56]
    len: u32,
    /// The actual capacity of this specific handle/view.
    cap: u32,
}

impl Clone for FixedBuf {
    fn clone(&self) -> Self {
        match self.pool_kind() {
            PoolKind::SlotBased => unsafe {
                slot_based_increment(self.pool_data, self.context.raw_payload());
            },
            PoolKind::Heap => unsafe {
                heap_increment(self.pool_data);
            },
        }
        Self {
            ptr: self.ptr,
            pool_data: self.pool_data,
            context: self.context,
            len: self.len,
            cap: self.cap,
        }
    }
}

const _: [(); 32] = [(); std::mem::size_of::<FixedBuf>()];
const _: [(); 32] = [(); std::mem::align_of::<FixedBuf>()];

// Safety: FixedBuf 拥有其底层内存的所有权。
unsafe impl Send for FixedBuf {}

impl FixedBuf {
    #[inline(always)]
    fn pool_kind(&self) -> PoolKind {
        self.context.pool_kind()
    }

    #[inline(always)]
    fn context_raw(&self) -> u64 {
        self.context.raw_payload()
    }

    #[inline(always)]
    fn capacity_usize(&self) -> usize {
        self.cap as usize
    }

    /// # Safety
    /// `ptr` must be valid and allocated by the pool associated with `pool_kind`.
    #[inline(always)]
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

        Self {
            ptr,
            pool_data,
            context,
            len: cap.get() as u32,
            cap: cap.get() as u32,
        }
    }

    /// Resolve which region this buffer belongs to and its offset.
    /// This is used for driver submission (RIO / io_uring).
    ///
    /// The interpretation of the region index is pool-dependent.
    #[inline(always)]
    pub fn resolve_region_info(&self) -> RegionInfo {
        match self.pool_kind() {
            PoolKind::SlotBased => unsafe { slot_based_resolve_region_info(self.pool_data, self) },
            PoolKind::Heap => heap_resolve_region_info(self),
        }
    }

    #[inline(always)]
    pub fn as_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), self.len as usize) }
    }

    #[inline(always)]
    pub fn as_slice_mut(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr(), self.len as usize) }
    }

    /// Access the full capacity as a mutable slice for writing data before set_len is called.
    #[inline(always)]
    pub fn spare_capacity_mut(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr(), self.capacity_usize()) }
    }

    #[inline(always)]
    pub fn as_ptr(&self) -> *const u8 {
        self.ptr.as_ptr()
    }

    // Pointer to start of capacity
    #[inline(always)]
    pub fn as_mut_ptr(&mut self) -> *mut u8 {
        self.ptr.as_ptr()
    }

    #[inline(always)]
    pub fn capacity(&self) -> usize {
        self.capacity_usize()
    }

    #[inline(always)]
    pub fn len(&self) -> usize {
        self.len as usize
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    #[inline(always)]
    pub fn set_len(&mut self, len: usize) {
        assert!(
            len <= self.capacity_usize(),
            "len must be <= buffer capacity"
        );
        assert!(len <= u32::MAX as usize, "len exceeds u32::MAX");
        self.len = len as u32;
    }

    /// Allocate a buffer from the system heap (not from a pool).
    ///
    /// This is used as a fallback when the pool is full.
    /// Note: Heap-allocated buffers may not be registered with the I/O driver
    /// and thus may incur overhead for direct I/O operations.
    pub fn alloc_heap(len: NonZeroUsize) -> Result<Self, AllocError> {
        // Allocate space for the metadata block plus the payload.
        // We use os::alloc_pages to ensure page alignment for both.
        let total_size = len.get().checked_add(4096).ok_or(AllocError::Oom)?;
        let total_size_nz = unsafe { NonZeroUsize::new_unchecked(total_size) };

        let base_ptr =
            unsafe { crate::os::alloc_pages(total_size_nz) }.map_err(|_| AllocError::Oom)?;

        // Initialize the control block in the first page
        let control = unsafe { &mut *(base_ptr as *mut HeapControlBlock) };
        control.ref_count = std::sync::atomic::AtomicU32::new(1);
        control.total_size = total_size_nz;

        let ptr = unsafe { NonNull::new_unchecked(base_ptr.add(4096)) };

        static HEAP_BUF_COOKIE_GEN: std::sync::atomic::AtomicU64 =
            std::sync::atomic::AtomicU64::new(1);
        let cookie = HEAP_BUF_COOKIE_GEN.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            & 0x00FFFFFFFFFFFFFF;

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

    /// Create a new sub-view of the buffer that shares the same underlying memory.
    ///
    /// The new buffer will have its own length and offset, but it will keep the
    /// original allocation alive until all views are dropped.
    #[inline(always)]
    pub fn slice(&self, range: std::ops::Range<usize>) -> Self {
        let mut new_buf = self.clone();
        let start = range.start;
        let end = range.end;
        assert!(start <= end, "slice start must be <= end");
        assert!(end <= self.len as usize, "slice end must be <= buffer len");

        new_buf.ptr = unsafe { NonNull::new_unchecked(self.ptr.as_ptr().add(start)) };
        new_buf.len = (end - start) as u32;
        new_buf.cap = (end - start) as u32; // Slice has its own independent capacity
        new_buf
    }
}

impl Drop for FixedBuf {
    #[inline(always)]
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

#[derive(Debug)]
pub enum AllocError {
    Layout(LayoutError),
    Oom,
}

impl std::fmt::Display for AllocError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AllocError::Layout(e) => write!(f, "Layout error: {}", e),
            AllocError::Oom => write!(f, "Out of memory"),
        }
    }
}

impl std::error::Error for AllocError {}

impl From<AllocError> for std::io::Error {
    fn from(err: AllocError) -> Self {
        match err {
            AllocError::Layout(_) => {
                std::io::Error::new(std::io::ErrorKind::InvalidInput, "Layout error")
            }
            AllocError::Oom => {
                std::io::Error::new(std::io::ErrorKind::OutOfMemory, "Out of memory")
            }
        }
    }
}

// ============================================================================
// Type-Erased Box<dyn BufPool> Replacement (Thread-Local Friendly)
// ============================================================================

/// 手写 VTable，用于动态分发 BufPool 的方法而不使用 dyn
pub struct BufPoolVTable {
    pub alloc: unsafe fn(*const u8, NonZeroUsize) -> Option<FixedBuf>,
    pub clone: unsafe fn(*const u8) -> AnyBufPool,
    pub drop: unsafe fn(*mut u8),
    pub fmt: unsafe fn(*const u8, &mut std::fmt::Formatter<'_>) -> std::fmt::Result,
}

/// A type-erased handle to any `BufPool`.
///
/// Designed with Small Object Optimization (SOO) to eliminate heap allocations
/// for common pool implementations (like `SlotBasedPool` which is just an `Arc`).
pub struct AnyBufPool {
    storage: [usize; 3], // 24 bytes
    vtable: &'static BufPoolVTable,
}

impl AnyBufPool {
    /// 从任意实现了 `BufPool + Clone` 的类型构造 `AnyBufPool`。
    pub fn new<P: BufPool + Clone>(pool: P) -> Self {
        // Size of the storage in bytes
        const STORAGE_SIZE: usize = std::mem::size_of::<[usize; 3]>();

        // Check if P fits in storage (SOO)
        let is_inline = std::mem::size_of::<P>() <= STORAGE_SIZE
            && std::mem::align_of::<P>() <= std::mem::align_of::<usize>();

        unsafe fn alloc_shim<P: BufPool + Clone>(
            ptr: *const u8,
            size: NonZeroUsize,
        ) -> Option<FixedBuf> {
            const STORAGE_SIZE: usize = std::mem::size_of::<[usize; 3]>();
            if std::mem::size_of::<P>() <= STORAGE_SIZE
                && std::mem::align_of::<P>() <= std::mem::align_of::<usize>()
            {
                let pool = unsafe { &*(ptr as *const P) };
                pool.alloc(size)
            } else {
                let pool = unsafe { &**(ptr as *const *const P) };
                pool.alloc(size)
            }
        }

        unsafe fn clone_shim<P: BufPool + Clone>(ptr: *const u8) -> AnyBufPool {
            const STORAGE_SIZE: usize = std::mem::size_of::<[usize; 3]>();
            if std::mem::size_of::<P>() <= STORAGE_SIZE
                && std::mem::align_of::<P>() <= std::mem::align_of::<usize>()
            {
                let pool = unsafe { &*(ptr as *const P) };
                AnyBufPool::new(pool.clone())
            } else {
                let pool = unsafe { &**(ptr as *const *const P) };
                AnyBufPool::new(pool.clone())
            }
        }

        unsafe fn drop_shim<P: BufPool + Clone>(ptr: *mut u8) {
            const STORAGE_SIZE: usize = std::mem::size_of::<[usize; 3]>();
            if std::mem::size_of::<P>() <= STORAGE_SIZE
                && std::mem::align_of::<P>() <= std::mem::align_of::<usize>()
            {
                unsafe { std::ptr::drop_in_place(ptr as *mut P) };
            } else {
                unsafe {
                    let _ = Box::from_raw(*(ptr as *mut *mut P));
                }
            }
        }

        unsafe fn fmt_shim<P: BufPool + Clone>(
            ptr: *const u8,
            f: &mut std::fmt::Formatter<'_>,
        ) -> std::fmt::Result {
            const STORAGE_SIZE: usize = std::mem::size_of::<[usize; 3]>();
            if std::mem::size_of::<P>() <= STORAGE_SIZE
                && std::mem::align_of::<P>() <= std::mem::align_of::<usize>()
            {
                let pool = unsafe { &*(ptr as *const P) };
                std::fmt::Debug::fmt(pool, f)
            } else {
                let pool = unsafe { &**(ptr as *const *const P) };
                std::fmt::Debug::fmt(pool, f)
            }
        }

        struct VTableGen<P>(std::marker::PhantomData<P>);

        impl<P: BufPool + Clone> VTableGen<P> {
            const VTABLE: BufPoolVTable = BufPoolVTable {
                alloc: alloc_shim::<P>,
                clone: clone_shim::<P>,
                drop: drop_shim::<P>,
                fmt: fmt_shim::<P>,
            };
        }

        let mut storage = [0usize; 3];
        if is_inline {
            unsafe {
                std::ptr::write(storage.as_mut_ptr() as *mut P, pool);
            }
        } else {
            storage[0] = Box::into_raw(Box::new(pool)) as usize;
        }

        AnyBufPool {
            storage,
            vtable: &VTableGen::<P>::VTABLE,
        }
    }
}

impl BufPool for AnyBufPool {
    fn alloc(&self, len: NonZeroUsize) -> Option<FixedBuf> {
        unsafe { (self.vtable.alloc)(self.storage.as_ptr() as *const u8, len) }
    }
}

impl Clone for AnyBufPool {
    fn clone(&self) -> Self {
        unsafe { (self.vtable.clone)(self.storage.as_ptr() as *const u8) }
    }
}

impl Drop for AnyBufPool {
    fn drop(&mut self) {
        unsafe { (self.vtable.drop)(self.storage.as_mut_ptr() as *mut u8) }
    }
}

impl std::fmt::Debug for AnyBufPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        unsafe { (self.vtable.fmt)(self.storage.as_ptr() as *const u8, f) }
    }
}

/// 基于 GlobalSlotPool 的 Pool 实现
///
/// 这个 Pool 使用 GlobalSlotPool 来分配内存。
#[derive(Clone)]
pub struct SlotBasedPool {
    /// 全局 Slot Pool 的引用 (Arc)
    pool: Arc<crate::heap::GlobalSlotPool>,
    /// Optional seed for deterministic shard selection
    seed: Option<usize>,
}

impl SlotBasedPool {
    /// 创建新的 SlotBasedPool
    pub fn new(pool: Arc<crate::heap::GlobalSlotPool>) -> Self {
        Self { pool, seed: None }
    }

    /// 使用特定的 seed 创建 SlotBasedPool，确保 shard 选择是确定性的
    pub fn with_seed(pool: Arc<crate::heap::GlobalSlotPool>, seed: usize) -> Self {
        Self {
            pool,
            seed: Some(seed),
        }
    }

    /// Calculate order for a given size
    fn calculate_order(size: NonZeroUsize) -> usize {
        let size = size.get();
        if size <= 4096 {
            0
        } else {
            // (size + 4095) / 4096 next power of two
            // But buddy allocator works better if we just use ilog2 of (rounded up to power of two)
            // if size = 4097 -> 8192 -> order 1
            let needed = size.next_power_of_two();
            needed.ilog2() as usize - 12
        }
    }
}

impl std::fmt::Debug for SlotBasedPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SlotBasedPool").finish()
    }
}

const ARC_CACHE_LIMIT: usize = 64;

#[derive(Copy, Clone)]
struct PoolCacheInner {
    ptr: *const crate::heap::GlobalSlotPool,
    balance: u32,
}

struct ThreadLocalPoolCache(Cell<PoolCacheInner>);

impl ThreadLocalPoolCache {
    /// 构造一个新的空缓存。由于字段是简单的原始类型和指针，此构造函数是 const 的。
    const fn new() -> Self {
        Self(Cell::new(PoolCacheInner {
            ptr: std::ptr::null(),
            balance: 0,
        }))
    }
}

impl Drop for ThreadLocalPoolCache {
    fn drop(&mut self) {
        let inner = self.0.get();
        if inner.balance > 0 && !inner.ptr.is_null() {
            // 线程退出时刷新剩余的引用计数
            for _ in 0..inner.balance {
                unsafe {
                    Arc::decrement_strong_count(inner.ptr);
                }
            }
        }
    }
}

thread_local! {
    /// 缓存当前线程使用的全局池引用计数。
    ///
    /// 注：在 Windows GNU 平台上，Clippy 会对常量初始化产生误报
    #[cfg_attr(all(target_arch = "x86_64", target_os = "windows", target_env = "gnu"), allow(clippy::missing_const_for_thread_local))]
    static POOL_CACHE: ThreadLocalPoolCache = const { ThreadLocalPoolCache::new() };
}

#[inline]
fn with_pool_cache<R>(f: impl FnOnce(&ThreadLocalPoolCache) -> R) -> R {
    POOL_CACHE.with(f)
}

impl BackingPool for SlotBasedPool {
    fn alloc_mem(&self, size: NonZeroUsize) -> AllocResult {
        let order = Self::calculate_order(size);

        if let Some((chunk_id, slot_idx, ptr)) = self.pool.alloc_slots(order, self.seed) {
            let capacity = crate::heap::buddy::BuddyAllocator::capacity_of(order);
            let context_data = PackedContext::new(
                slot_idx.0 as u32,
                order as u8,
                chunk_id,
                PoolKind::SlotBased,
            );
            let context = u64::from(context_data);

            let cap = unsafe { NonZeroUsize::new_unchecked(capacity) };

            AllocResult::Allocated { ptr, cap, context }
        } else {
            AllocResult::Failed
        }
    }

    fn pool_kind(&self) -> PoolKind {
        PoolKind::SlotBased
    }

    fn pool_data(&self) -> NonNull<()> {
        let current_ptr = Arc::as_ptr(&self.pool);

        with_pool_cache(|cache| {
            let mut inner = cache.0.get();

            if inner.ptr == current_ptr {
                if inner.balance > 0 {
                    // Fast path: Reuse credit
                    inner.balance -= 1;
                    cache.0.set(inner);
                    return unsafe { NonNull::new_unchecked(current_ptr as *mut ()) };
                }
            } else {
                // Flush old pool logic
                if inner.balance > 0 && !inner.ptr.is_null() {
                    let old_ptr = inner.ptr;
                    let count = inner.balance;
                    for _ in 0..count {
                        unsafe {
                            Arc::decrement_strong_count(old_ptr);
                        }
                    }
                }
                inner.ptr = current_ptr;
                inner.balance = 0;
            }

            // Fallback: Batch Prefetching
            // Instead of 1, we increment by BATCH.
            // This reduces atomics on future allocs significantly.
            const PREFETCH_COUNT: u32 = 32;
            for _ in 0..PREFETCH_COUNT {
                // Equivalent to cloning Arc and forgetting it, but avoids
                // constructing and forgetting temporary Arc values.
                unsafe {
                    Arc::increment_strong_count(current_ptr);
                }
            }

            // Since we used clone() PREFETCH_COUNT times, we have PREFETCH_COUNT references.
            // 1 will be used now, others go to balance.
            inner.balance = PREFETCH_COUNT - 1;
            cache.0.set(inner);
            unsafe { NonNull::new_unchecked(current_ptr as *mut ()) }
        })
    }
}

impl BufPool for SlotBasedPool {
    fn alloc(&self, len: NonZeroUsize) -> Option<FixedBuf> {
        if let Some(buf) = self.alloc_mem(len).into_buf(self) {
            return Some(buf);
        }

        // Fallback path:
        // `GlobalSlotPool` already attempts automatic chunk expansion internally.
        // We only fallback to heap after slot allocation (including expansion) is exhausted.
        FixedBuf::alloc_heap(len).ok()
    }
}

#[inline(always)]
unsafe fn heap_increment(pool_data: NonNull<()>) {
    let control_ptr = pool_data.as_ptr() as *const HeapControlBlock;
    unsafe {
        (*control_ptr)
            .ref_count
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
}

#[inline(always)]
unsafe fn heap_dealloc(pool_data: NonNull<()>) {
    let base_ptr = pool_data.as_ptr() as *mut u8;
    let control_ptr = base_ptr as *const HeapControlBlock;

    if unsafe {
        (*control_ptr)
            .ref_count
            .fetch_sub(1, std::sync::atomic::Ordering::Release)
    } == 1
    {
        std::sync::atomic::fence(std::sync::atomic::Ordering::Acquire);
        let total_size = unsafe { (*control_ptr).total_size };
        unsafe {
            crate::os::free_pages(NonNull::new_unchecked(base_ptr), total_size);
        }
    }
}

#[inline(always)]
fn heap_resolve_region_info(buf: &FixedBuf) -> RegionInfo {
    // Heap-allocated payload starts after the 4KB control block.
    let base = buf.pool_data.as_ptr() as usize + 4096;
    let ptr = buf.as_ptr() as usize;

    RegionInfo {
        pool_kind: PoolKind::Heap,
        id: 0,
        offset: ptr.saturating_sub(base),
        cookie: buf.context_raw(),
    }
}

unsafe fn slot_based_increment(pool_data: NonNull<()>, context: u64) {
    let raw_ptr = pool_data.as_ptr() as *const crate::heap::GlobalSlotPool;

    let ctx = PackedContext::from(context);
    let chunk_id = ctx.chunk_id();
    let slot_idx = crate::heap::slot::SlotIndex(ctx.slot_idx() as usize);

    // 1. Increment slot ref count
    let pool = unsafe { &*raw_ptr };
    pool.increment_ref_count(chunk_id, slot_idx, std::sync::atomic::Ordering::Relaxed);

    // 2. Increment Arc ref count (using cache if possible)
    let used_cache = with_pool_cache(|cache| {
        let mut inner = cache.0.get();
        if inner.ptr == raw_ptr && inner.balance > 0 {
            inner.balance -= 1;
            cache.0.set(inner);
            true
        } else {
            false
        }
    });

    if !used_cache {
        unsafe {
            Arc::increment_strong_count(raw_ptr);
        }
    }
}

unsafe fn slot_based_dealloc(pool_data: NonNull<()>, context: u64) {
    let raw_ptr = pool_data.as_ptr() as *const crate::heap::GlobalSlotPool;

    let ctx = PackedContext::from(context);
    let chunk_id = ctx.chunk_id();
    let order = ctx.order() as usize;
    let slot_idx = crate::heap::slot::SlotIndex(ctx.slot_idx() as usize);

    // Try recycle
    let recycled = with_pool_cache(|cache| {
        let mut inner = cache.0.get();
        if inner.ptr == raw_ptr && inner.balance < ARC_CACHE_LIMIT as u32 {
            inner.balance += 1;
            cache.0.set(inner);
            true
        } else {
            false
        }
    });

    if recycled {
        // We recycled the ref (put into TLS balance).
        // So we do NOT drop the Arc.
        let pool = unsafe { &*raw_ptr };
        unsafe {
            pool.dealloc_slots(chunk_id, slot_idx, order);
        }
    } else {
        // Standard drop (Atomic Decrement)
        let pool = unsafe { Arc::from_raw(raw_ptr) };
        unsafe {
            pool.dealloc_slots(chunk_id, slot_idx, order);
        }
    }
}

unsafe fn slot_based_resolve_region_info(pool_data: NonNull<()>, buf: &FixedBuf) -> RegionInfo {
    // 1. Cast back to GlobalSlotPool
    let pool = unsafe { &*(pool_data.as_ptr() as *const crate::heap::GlobalSlotPool) };

    // 2. Unpack ChunkID
    let ctx = PackedContext::from(buf.context_raw());
    let chunk_id = ctx.chunk_id();

    // 3. Get base address from ChunkInfo
    let chunk_info = pool
        .chunk_info(chunk_id)
        .expect("FixedBuf has invalid ChunkID");
    let base = chunk_info.ptr.as_ptr() as usize;
    let ptr = buf.as_ptr() as usize;

    // Return RegionInfo { id, offset, cookie }
    RegionInfo {
        pool_kind: PoolKind::SlotBased,
        id: chunk_id,
        offset: ptr.saturating_sub(base),
        cookie: 0, // Cookies are currently only used for heap buffers
    }
}
