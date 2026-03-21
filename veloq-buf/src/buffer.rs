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
    cell::Cell,
    num::{NonZeroU16, NonZeroUsize},
    ptr::NonNull,
    sync::{Arc, Mutex, OnceLock},
};

const NO_REGISTRATION_INDEX: u16 = u16::MAX;
const CONTEXT_PAYLOAD_MASK: u64 = (1u64 << 56) - 1;
const VTABLE_IDX_SLOT_BASED: u8 = 0;
const VTABLE_IDX_HEAP: u8 = 1;

#[inline(always)]
fn pack_context(vtable_idx: u8, payload: u64) -> u64 {
    assert!(
        payload <= CONTEXT_PAYLOAD_MASK,
        "Context payload exceeds 56 bits"
    );
    ((vtable_idx as u64) << 56) | payload
}

#[inline(always)]
fn context_vtable_idx(context_packed: u64) -> u8 {
    (context_packed >> 56) as u8
}

#[inline(always)]
fn context_payload(context_packed: u64) -> u64 {
    context_packed & CONTEXT_PAYLOAD_MASK
}

#[inline(always)]
fn pack_slot_context_payload(chunk_id: u16, order: usize, slot_idx: usize) -> u64 {
    assert!(order <= u8::MAX as usize, "Order exceeds 8 bits");
    assert!(
        slot_idx <= u32::MAX as usize,
        "SlotIndex exceeded 32 bits in FixedBuf handle"
    );
    ((chunk_id as u64) << 40) | ((order as u64) << 32) | (slot_idx as u64)
}

#[inline(always)]
fn unpack_slot_context_payload(payload: u64) -> (u16, usize, crate::heap::slot::SlotIndex) {
    let chunk_id = ((payload >> 40) & 0xFFFF) as u16;
    let order = ((payload >> 32) & 0xFF) as usize;
    let slot_idx = crate::heap::slot::SlotIndex((payload & 0xFFFF_FFFF) as usize);
    (chunk_id, order, slot_idx)
}

#[inline(always)]
fn slot_capacity_from_payload(payload: u64) -> usize {
    let order = ((payload >> 32) & 0xFF) as usize;
    crate::heap::buddy::BuddyAllocator::capacity_of(order)
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

pub type GlobalIndex = NotU16<NO_REGISTRATION_INDEX>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RegionInfo {
    pub id: u16,
    pub offset: usize,
    /// A unique cookie used to distinguish different allocations for the same pointer (e.g. heap reuse).
    pub cookie: u64,
}

#[derive(Debug)]
pub struct PoolVTable {
    pub dealloc: unsafe fn(pool_data: NonNull<()>, params: DeallocParams),
    pub resolve_region_info: unsafe fn(pool_data: NonNull<()>, buf: &FixedBuf) -> RegionInfo,
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
    context_packed: u64, // [vtable_idx:8 | context_payload:56]
    len: u32,
    // For non-slot buffers, stores capacity in bytes (u32).
    // For slot buffers, currently unused (reserved for future flags).
    flags: u32,
}

const _: [(); 32] = [(); std::mem::size_of::<FixedBuf>()];
const _: [(); 32] = [(); std::mem::align_of::<FixedBuf>()];

// Safety: FixedBuf 拥有其底层内存的所有权。
unsafe impl Send for FixedBuf {}

impl FixedBuf {
    #[inline(always)]
    fn vtable_idx(&self) -> u8 {
        context_vtable_idx(self.context_packed)
    }

    #[inline(always)]
    fn context_raw(&self) -> u64 {
        context_payload(self.context_packed)
    }

    #[inline(always)]
    fn capacity_usize(&self) -> usize {
        match self.vtable_idx() {
            VTABLE_IDX_SLOT_BASED => slot_capacity_from_payload(self.context_raw()),
            _ => self.flags as usize,
        }
    }

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
        assert!(
            cap.get() <= u32::MAX as usize,
            "FixedBuf only supports capacity <= u32::MAX"
        );

        let vtable_idx = vtable_to_index(vtable);
        let payload = context & CONTEXT_PAYLOAD_MASK;
        let context_packed = pack_context(vtable_idx, payload);

        let flags = if vtable_idx == VTABLE_IDX_SLOT_BASED {
            debug_assert_eq!(cap.get(), slot_capacity_from_payload(payload));
            0
        } else {
            cap.get() as u32
        };

        Self {
            ptr,
            pool_data,
            context_packed,
            len: cap.get() as u32,
            flags,
        }
    }

    /// Resolve which region this buffer belongs to and its offset.
    /// This is used for driver submission (RIO / io_uring).
    ///
    /// The interpretation of the region index is pool-dependent.
    #[inline(always)]
    pub fn resolve_region_info(&self) -> RegionInfo {
        let vtable = vtable_from_index(self.vtable_idx());
        unsafe { (vtable.resolve_region_info)(self.pool_data, self) }
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
        // Use 4KB alignment for general compatibility
        let layout =
            std::alloc::Layout::from_size_align(len.get(), 4096).map_err(AllocError::Layout)?;

        let ptr = unsafe { std::alloc::alloc(layout) };
        if ptr.is_null() {
            return Err(AllocError::Oom);
        }

        let ptr = unsafe { NonNull::new_unchecked(ptr) };

        static HEAP_BUF_COOKIE_GEN: std::sync::atomic::AtomicU64 =
            std::sync::atomic::AtomicU64::new(1);
        let cookie = HEAP_BUF_COOKIE_GEN.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            & CONTEXT_PAYLOAD_MASK;

        Ok(unsafe {
            Self::new(
                ptr,
                len,
                NonNull::dangling(), // No pool context for heap buffers
                &HEAP_POOL_VTABLE,
                cookie,
            )
        })
    }
}

impl Drop for FixedBuf {
    #[inline(always)]
    fn drop(&mut self) {
        unsafe {
            let cap = NonZeroUsize::new_unchecked(self.capacity_usize());
            let params = DeallocParams {
                ptr: self.ptr,
                cap,
                context: self.context_raw(),
            };

            // Call dealloc via vtable resolved from compact index
            let vtable = vtable_from_index(self.vtable_idx());
            (vtable.dealloc)(self.pool_data, params);
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
    pub fn new<P: BufPool + Clone + 'static>(pool: P) -> Self {
        // Size of the storage in bytes
        const STORAGE_SIZE: usize = std::mem::size_of::<[usize; 3]>();

        // Check if P fits in storage (SOO)
        let is_inline = std::mem::size_of::<P>() <= STORAGE_SIZE
            && std::mem::align_of::<P>() <= std::mem::align_of::<usize>();

        unsafe fn alloc_shim<P: BufPool + Clone + 'static>(
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

        unsafe fn clone_shim<P: BufPool + Clone + 'static>(ptr: *const u8) -> AnyBufPool {
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

        unsafe fn drop_shim<P: BufPool + Clone + 'static>(ptr: *mut u8) {
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

        unsafe fn fmt_shim<P: BufPool + Clone + 'static>(
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

        impl<P: BufPool + Clone + 'static> VTableGen<P> {
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
    /// 在 Windows GNU 平台上，Clippy 会对常量初始化产生误报，
    /// 这里通过重构为直接存储并使用 const 块来优化。
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
            let context = pack_slot_context_payload(chunk_id, order, slot_idx.0);

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
        self.alloc_mem(len).into_buf(self)
    }
}

// VTable for SlotBasedPool
static SLOT_BASED_POOL_VTABLE: PoolVTable = PoolVTable {
    dealloc: slot_based_dealloc_shim,
    resolve_region_info: slot_based_resolve_region_info_shim,
};

// VTable for Heap-allocated buffers
static HEAP_POOL_VTABLE: PoolVTable = PoolVTable {
    dealloc: heap_dealloc_shim,
    resolve_region_info: heap_resolve_region_info_shim,
};

fn custom_vtable_registry() -> &'static Mutex<Vec<&'static PoolVTable>> {
    static REGISTRY: OnceLock<Mutex<Vec<&'static PoolVTable>>> = OnceLock::new();
    REGISTRY.get_or_init(|| Mutex::new(vec![&SLOT_BASED_POOL_VTABLE, &HEAP_POOL_VTABLE]))
}

#[inline(always)]
fn vtable_to_index(vtable: &'static PoolVTable) -> u8 {
    if std::ptr::eq(vtable, &SLOT_BASED_POOL_VTABLE) {
        return VTABLE_IDX_SLOT_BASED;
    }
    if std::ptr::eq(vtable, &HEAP_POOL_VTABLE) {
        return VTABLE_IDX_HEAP;
    }

    let mut registry = custom_vtable_registry()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);

    if let Some(index) = registry
        .iter()
        .position(|entry| std::ptr::eq(*entry, vtable))
    {
        return index as u8;
    }

    assert!(
        registry.len() < (u8::MAX as usize + 1),
        "PoolVTable registry exceeded u8 index space"
    );
    registry.push(vtable);
    (registry.len() - 1) as u8
}

#[inline(always)]
fn vtable_from_index(index: u8) -> &'static PoolVTable {
    match index {
        VTABLE_IDX_SLOT_BASED => &SLOT_BASED_POOL_VTABLE,
        VTABLE_IDX_HEAP => &HEAP_POOL_VTABLE,
        _ => {
            let registry = custom_vtable_registry()
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            registry
                .get(index as usize)
                .copied()
                .expect("Invalid PoolVTable index in FixedBuf handle")
        }
    }
}

unsafe fn heap_dealloc_shim(_pool_data: NonNull<()>, params: DeallocParams) {
    let layout = std::alloc::Layout::from_size_align(params.cap.get(), 4096).unwrap();
    unsafe {
        std::alloc::dealloc(params.ptr.as_ptr(), layout);
    }
}

unsafe fn heap_resolve_region_info_shim(_pool_data: NonNull<()>, buf: &FixedBuf) -> RegionInfo {
    // Return NO_REGISTRATION_INDEX to indicate this buffer is not registered.
    // Use context payload as the heap cookie.
    RegionInfo {
        id: NO_REGISTRATION_INDEX,
        offset: 0,
        cookie: buf.context_raw(),
    }
}

unsafe fn slot_based_dealloc_shim(pool_data: NonNull<()>, params: DeallocParams) {
    let raw_ptr = pool_data.as_ptr() as *const crate::heap::GlobalSlotPool;

    let (chunk_id, order, slot_idx) = unpack_slot_context_payload(params.context);

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

unsafe fn slot_based_resolve_region_info_shim(
    pool_data: NonNull<()>,
    buf: &FixedBuf,
) -> RegionInfo {
    // 1. Cast back to GlobalSlotPool
    let pool = unsafe { &*(pool_data.as_ptr() as *const crate::heap::GlobalSlotPool) };

    // 2. Unpack ChunkID
    let (chunk_id, _order, _slot_idx) = unpack_slot_context_payload(buf.context_raw());

    // 3. Get base address from ChunkInfo
    let chunk_info = pool
        .chunk_info(chunk_id)
        .expect("FixedBuf has invalid ChunkID");
    let base = chunk_info.ptr.as_ptr() as usize;
    let ptr = buf.as_ptr() as usize;

    // Return RegionInfo { id, offset, cookie }
    RegionInfo {
        id: chunk_id,
        offset: ptr.saturating_sub(base),
        cookie: 0, // Cookies are currently only used for heap buffers
    }
}
