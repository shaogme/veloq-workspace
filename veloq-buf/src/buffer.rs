//! # Buffer Management for High-Performance I/O
//!
//! This module provides abstractions for managing memory buffers compatible with modern
//! asynchronous I/O interfaces (like `io_uring` on Linux and IOCP on Windows).
//!
//! ## Core Components
//!
//! - [`FixedBuf`]: An owned handle to a buffer allocated from a pool. It ensures the underlying
//!   memory remains valid as long as the handle exists. It uses type erasure (VTables) to
//!   delegate deallocation back to the source pool.
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
//! - The `dealloc` function in the VTable can correctly free the memory using only:
//!   - The payload pointer (`ptr`)
//!   - The capacity (`cap`)
//!   - An opaque `context` value (u64) provided during allocation
//!
//! See [`SlotBasedPool`] for the implementation relying on [`crate::global::GlobalSlotPool`].

use std::{
    alloc::LayoutError,
    num::{NonZeroU16, NonZeroUsize},
    ptr::NonNull,
    sync::Arc,
};

const NO_REGISTRATION_INDEX: u16 = u16::MAX;

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

pub type GlobalIndex = NotU16<NO_REGISTRATION_INDEX>;

#[derive(Debug)]
pub struct PoolVTable {
    pub dealloc: unsafe fn(pool_data: NonNull<()>, params: DeallocParams),
    pub resolve_region_info: unsafe fn(pool_data: NonNull<()>, buf: &FixedBuf) -> (usize, usize),
}

#[derive(Debug)]
pub struct DeallocParams {
    pub ptr: NonNull<u8>,  // Points to the Payload (data), not the header
    pub cap: NonZeroUsize, // Capacity of the Payload
    pub context: u64,      // Context restored from header
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
                    pool.vtable(),
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
}

unsafe impl Send for BufferRegion {}
unsafe impl Sync for BufferRegion {}

/// Trait abstraction for driver-specific buffer registration
pub trait BufferRegistrar {
    /// Register memory regions with the kernel.
    /// Returns a list of handles (tokens) corresponding to the regions.
    /// For RIO this is RIO_BUFFERID, for uring it might be ignored or index.
    fn register(&self, regions: &[BufferRegion]) -> std::io::Result<Vec<usize>>;
}

/// Memory pool implementation providing raw memory allocation.
/// This trait manages memory layout, allocation algorithms, and deallocation.
/// It does NOT handle driver registration.
pub trait BackingPool: std::fmt::Debug + 'static {
    /// Allocate memory without registration context.
    /// Returns allocation result containing ptr, capacity, and header context.
    /// The `global_index` in the result should be ignored or None.
    fn alloc_mem(&self, size: NonZeroUsize) -> AllocResult;

    /// Get the VTable for this pool (used by FixedBuf for deallocation).
    fn vtable(&self) -> &'static PoolVTable;

    /// Get the raw pool data pointer.
    fn pool_data(&self) -> NonNull<()>;
}

/// High-level Buffer Pool trait.
/// Represents a pool that is ready for I/O operations (registered if necessary).
pub trait BufPool: std::fmt::Debug + 'static {
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
pub trait PoolTopology: Clone + Send + Sync + 'static {
    /// Shared state initialized once at startup.
    /// This is passed to every worker during pool construction.
    /// Must be `Clone` (shared via `Arc` or `&'static`) and thread-safe.
    type State: Clone + Send + Sync + 'static;

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
        _worker_idx: usize,
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

        // Build mapping: ChunkID (Index) -> RegionIndex (Value)
        // Since we only registered Chunk 0, the first ID corresponds to Chunk 0.
        // Even if registration happens here, SlotBasedPool no longer tracks it.
        // It's up to the Driver to verify registration.

        // We still call register for now to keep existing behavior until Driver is updated,
        // but we ignore the returned IDs in SlotBasedPool.

        let slot_pool = SlotBasedPool::new(pool.clone());
        AnyBufPool::new(slot_pool)
    }
}

#[derive(Debug)]
pub struct FixedBuf {
    ptr: NonNull<u8>,
    len: usize,
    cap: NonZeroUsize,
    // Metadata moved from Heap Header to Handle
    pool_data: NonNull<()>,
    vtable: &'static PoolVTable,
    context: u64,
}

// Safety: FixedBuf 拥有其底层内存的所有权。
unsafe impl Send for FixedBuf {}

impl FixedBuf {
    /// # Safety
    /// `ptr` must be valid and allocated by the pool associated with `vtable`.
    #[inline(always)]
    pub unsafe fn new(
        ptr: NonNull<u8>,
        cap: NonZeroUsize,
        pool_data: NonNull<()>,
        vtable: &'static PoolVTable,
        context: u64,
    ) -> Self {
        Self {
            ptr,
            len: cap.get(),
            cap,
            pool_data,
            vtable,
            context,
        }
    }

    /// Resolve which region this buffer belongs to and its offset.
    /// This is used for driver submission (RIO / io_uring).
    ///
    /// The interpretation of the region index is pool-dependent.
    #[inline(always)]
    pub fn resolve_region_info(&self) -> (usize, usize) {
        unsafe { (self.vtable.resolve_region_info)(self.pool_data, self) }
    }

    #[inline(always)]
    pub fn as_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), self.len) }
    }

    #[inline(always)]
    pub fn as_slice_mut(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr(), self.len) }
    }

    /// Access the full capacity as a mutable slice for writing data before set_len is called.
    #[inline(always)]
    pub fn spare_capacity_mut(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr(), self.cap.get()) }
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
        self.cap.get()
    }

    #[inline(always)]
    pub fn len(&self) -> usize {
        self.len
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        false
    }

    #[inline(always)]
    pub fn set_len(&mut self, len: usize) {
        assert!(len <= self.cap.get());
        self.len = len;
    }
}

impl Drop for FixedBuf {
    #[inline(always)]
    fn drop(&mut self) {
        unsafe {
            let params = DeallocParams {
                ptr: self.ptr,
                cap: self.cap,
                context: self.context,
            };

            // Call dealloc via vtable stored in handle
            (self.vtable.dealloc)(self.pool_data, params);
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

// ============================================================================
// Type-Erased Box<dyn BufPool> Replacement (Thread-Local Friendly)
// ============================================================================

/// 手写 VTable，用于动态分发 BufPool 的方法而不使用 dyn
pub struct BufPoolVTable {
    pub alloc: unsafe fn(*const (), NonZeroUsize) -> Option<FixedBuf>,
    pub clone: unsafe fn(*const ()) -> *mut (),
    pub drop: unsafe fn(*mut ()),
    pub fmt: unsafe fn(*const (), &mut std::fmt::Formatter<'_>) -> std::fmt::Result,
}

/// A type-erased handle to any `BufPool`.
pub struct AnyBufPool {
    data: *mut (),
    vtable: &'static BufPoolVTable,
}

impl AnyBufPool {
    /// 从任意实现了 `BufPool + Clone` 的类型构造 `AnyBufPool`。
    pub fn new<P: BufPool + Clone + 'static>(pool: P) -> Self {
        unsafe fn alloc_shim<P: BufPool>(ptr: *const (), size: NonZeroUsize) -> Option<FixedBuf> {
            unsafe {
                let pool = &*(ptr as *const P);
                pool.alloc(size)
            }
        }

        unsafe fn clone_shim<P: BufPool + Clone>(ptr: *const ()) -> *mut () {
            unsafe {
                let pool = &*(ptr as *const P);
                let new_pool = Box::new(pool.clone());
                Box::into_raw(new_pool) as *mut ()
            }
        }

        unsafe fn drop_shim<P: BufPool>(ptr: *mut ()) {
            unsafe {
                let _ = Box::from_raw(ptr as *mut P);
            }
        }

        unsafe fn fmt_shim<P: BufPool>(
            ptr: *const (),
            f: &mut std::fmt::Formatter<'_>,
        ) -> std::fmt::Result {
            unsafe {
                let pool = &*(ptr as *const P);
                std::fmt::Debug::fmt(pool, f)
            }
        }

        struct VTableGen<P>(std::marker::PhantomData<P>);

        impl<P: BufPool + Clone + 'static> VTableGen<P> {
            const VTABLE: BufPoolVTable = BufPoolVTable {
                alloc: alloc_shim::<P>,
                clone: clone_shim::<P>,
                drop: drop_shim::<P>,
                fmt: fmt_shim::<P>,
            };
        }

        AnyBufPool {
            data: Box::into_raw(Box::new(pool)) as *mut (),
            vtable: &VTableGen::<P>::VTABLE,
        }
    }
}

impl BufPool for AnyBufPool {
    fn alloc(&self, len: NonZeroUsize) -> Option<FixedBuf> {
        unsafe { (self.vtable.alloc)(self.data, len) }
    }
}

impl Clone for AnyBufPool {
    fn clone(&self) -> Self {
        unsafe {
            let new_data = (self.vtable.clone)(self.data);
            Self {
                data: new_data,
                vtable: self.vtable,
            }
        }
    }
}

impl Drop for AnyBufPool {
    fn drop(&mut self) {
        unsafe { (self.vtable.drop)(self.data) }
    }
}

impl std::fmt::Debug for AnyBufPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        unsafe { (self.vtable.fmt)(self.data, f) }
    }
}

/// 基于 GlobalSlotPool 的 Pool 实现
///
/// 这个 Pool 使用 GlobalSlotPool 来分配内存。
//
#[derive(Clone)]
pub struct SlotBasedPool {
    /// 全局 Slot Pool 的引用 (Arc)
    pool: Arc<crate::heap::GlobalSlotPool>,
}

impl SlotBasedPool {
    /// 创建新的 SlotBasedPool
    pub fn new(pool: Arc<crate::heap::GlobalSlotPool>) -> Self {
        Self { pool }
    }

    /// Calculate order for a given size
    fn calculate_order(size: usize) -> usize {
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

impl BackingPool for SlotBasedPool {
    fn alloc_mem(&self, size: NonZeroUsize) -> AllocResult {
        let size_val = size.get();
        let order = Self::calculate_order(size_val);

        if let Some((chunk_id, slot_idx, ptr)) = self.pool.alloc_slots(order) {
            let capacity = crate::heap::buddy::BuddyAllocator::capacity_of(order);

            // Pack Metadata into Context (64-bit)
            // Layout: [ChunkID 16b] [Reserved 16b] [Order 8b] [SlotIndex 24b]
            // Constraint: SlotIndex must fit in 24 bits (16 Million slots = 64GB @ 4KB).

            let chunk_id_val = chunk_id as u64;
            // Reserved: 0
            let s_idx = slot_idx.0 as u64;

            debug_assert!(
                s_idx < (1 << 24),
                "SlotIndex exceeded 24 bits (Chunk too large)"
            );

            let context = (chunk_id_val << 48)
                // | (0 << 32) // Reserved
                | ((order as u64 & 0xFF) << 24)
                | (s_idx & 0xFFFFFF);

            let cap = unsafe { NonZeroUsize::new_unchecked(capacity) };

            AllocResult::Allocated { ptr, cap, context }
        } else {
            AllocResult::Failed
        }
    }

    fn vtable(&self) -> &'static PoolVTable {
        &SLOT_BASED_POOL_VTABLE
    }

    fn pool_data(&self) -> NonNull<()> {
        let ptr = {
            // Always acquire strong reference to ensure safety for FixedBuf lifetime
            Arc::into_raw(self.pool.clone())
        };

        unsafe { NonNull::new_unchecked(ptr as *mut ()) }
    }
}

impl BufPool for SlotBasedPool {
    fn alloc(&self, len: NonZeroUsize) -> Option<FixedBuf> {
        self.alloc_mem(len).into_buf(self)
    }
}

// VTable for SlotBasedPool
static SLOT_BASED_POOL_VTABLE: PoolVTable = PoolVTable {
    dealloc: slot_based_dealloc_shim,
    resolve_region_info: slot_based_resolve_region_info_shim,
};

unsafe fn slot_based_dealloc_shim(pool_data: NonNull<()>, params: DeallocParams) {
    let raw_ptr = pool_data.as_ptr() as *const crate::heap::GlobalSlotPool;

    // Unpack context (u64)
    // Layout: [ChunkID 16b] [RegionIndex 16b] [Order 8b] [SlotIndex 24b]
    let chunk_id = ((params.context >> 48) & 0xFFFF) as u16;
    let order = ((params.context >> 24) & 0xFF) as usize;
    let slot_idx_val = (params.context & 0xFFFFFF) as usize;
    let slot_idx = crate::heap::slot::SlotIndex(slot_idx_val);

    // Restore ownership of Arc to drop it (decrement ref count)
    let pool = unsafe { Arc::from_raw(raw_ptr) };
    unsafe {
        pool.dealloc_slots(chunk_id, slot_idx, order);
    }
}

unsafe fn slot_based_resolve_region_info_shim(
    pool_data: NonNull<()>,
    buf: &FixedBuf,
) -> (usize, usize) {
    // 1. Cast back to GlobalSlotPool
    let pool = unsafe { &*(pool_data.as_ptr() as *const crate::heap::GlobalSlotPool) };

    // 2. Unpack ChunkID
    // Layout: [ChunkID 16b] [Reserved 16b] [Order 8b] [SlotIndex 24b]
    let chunk_id = ((buf.context >> 48) & 0xFFFF) as u16;

    // 3. Get base address from ChunkInfo
    let chunk_info = pool
        .chunk_info(chunk_id)
        .expect("FixedBuf has invalid ChunkID");
    let base = chunk_info.ptr.as_ptr() as usize;
    let ptr = buf.as_ptr() as usize;

    // Return (ChunkID, Offset)
    (chunk_id as usize, ptr.saturating_sub(base))
}
