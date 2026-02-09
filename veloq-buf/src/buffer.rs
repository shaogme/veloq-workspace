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
//!   - An opaque `context` value (usize) provided during allocation
//!
//! See [`BlockBasedPool`] for the primary implementation relying on [`crate::global::GlobalBlockPool`].

use std::{
    alloc::LayoutError,
    num::{NonZeroU16, NonZeroUsize},
    ptr::NonNull,
    sync::Arc,
};

use crossbeam_utils::CachePadded;

pub mod buddy;
pub mod hybrid;

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
    pub context: usize,    // Context restored from header
}

#[derive(Debug)]
pub enum AllocResult {
    Allocated {
        ptr: NonNull<u8>,
        cap: NonZeroUsize,
        global_index: Option<GlobalIndex>,
        context: usize,
    },
    Failed,
}

impl AllocResult {
    #[inline]
    pub fn into_buf(self, pool: &dyn BackingPool) -> Option<FixedBuf> {
        match self {
            AllocResult::Allocated {
                ptr,
                cap,
                global_index,
                context,
            } => unsafe {
                Some(FixedBuf::new(
                    ptr,
                    cap,
                    global_index,
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

/// 核心 Trait：定义 Buffer Pool 的规格
/// 职责：
/// 1. 声明所需的内存大小 (Binding Size)
/// 2. 通过分配好的内存构建 Pool (Building)
pub trait PoolSpec: Clone + Send + Sync + 'static {
    /// 此配置所需的内存大小。
    const MEMORY_REQUIREMENT: NonZeroUsize;

    /// 消耗自身配置，将分配好的 ThreadMemory 和 Registrar 组装成 AnyBufPool。
    fn build(
        self,
        memory: crate::ThreadMemory,
        registrar: Box<dyn BufferRegistrar>,
        global_info: crate::global::GlobalMemoryInfo,
    ) -> AnyBufPool;
}

/// 定义 Runtime 所有工作线程的缓冲池拓扑结构
pub trait PoolTopology: Clone + Send + Sync + 'static {
    /// 步骤 1: 计算布局
    /// 返回一个向量，描述每个 Worker (0..N) 所需的内存大小。
    /// Runtime 将根据此列表向操作系统申请对齐的内存。
    fn memory_requirements(&self, worker_count: usize) -> Vec<NonZeroUsize>;

    /// 步骤 2: 构建实例
    /// 为指定的 worker_index 构建具体的 Pool。
    /// 传入的 `memory` 大小保证与 `memory_requirements` 中返回的一致。
    fn build(
        &self,
        worker_index: usize,
        memory: crate::ThreadMemory,
        registrar: Box<dyn BufferRegistrar>,
        global_info: crate::global::GlobalMemoryInfo,
    ) -> AnyBufPool;
}

// ============================================================================
// Block-Based Topology (New Architecture)
// ============================================================================

/// Block 拓扑结构 trait
///
/// 与 PoolTopology 不同，BlockTopology 使用 GlobalBlockPool，
/// 所有线程共享同一个 Block Pool。
pub trait BlockTopology: Clone + Send + Sync + 'static {
    /// 为所有工作线程创建 GlobalBlockPool
    ///
    /// # 参数
    /// - `worker_count`: 工作线程数量
    ///
    /// # 返回
    /// `GlobalBlockPool`，包含所有线程的 N*2 个 Block
    fn create_pool(
        &self,
        worker_count: usize,
    ) -> std::io::Result<Arc<crate::global::GlobalBlockPool>>;

    /// 为指定的 worker 构建 BufPool
    ///
    /// # 参数
    /// - `pool`: 全局 Block Pool 的引用
    /// - `worker_index`: 工作线程索引
    /// - `registrar`: 驱动注册器
    ///
    /// # 返回
    /// 该 worker 的 BufPool 实例
    fn build_for_worker(
        &self,
        pool: Arc<crate::global::GlobalBlockPool>,
        worker_index: usize,
        registrar: Box<dyn BufferRegistrar>,
    ) -> AnyBufPool;
}

/// 标准 Block 拓扑：所有线程使用相同的分配器类型和大小
#[derive(Clone)]
pub struct UniformBlock {
    /// 每个 Block 的内存倍数
    pub multiplier: crate::ThreadMemoryMultiplier,
    /// 分配器工厂函数
    factory: Arc<
        dyn Fn(usize, crate::ThreadMemory) -> Box<dyn crate::block::RawAllocator> + Send + Sync,
    >,
}

impl UniformBlock {
    /// 创建新的 UniformBlock topology
    ///
    /// # 参数
    /// - `multiplier`: 每个 Block 的内存大小倍数
    /// - `factory`: 用于创建分配器的工厂函数
    pub fn new<F>(multiplier: crate::ThreadMemoryMultiplier, factory: F) -> Self
    where
        F: Fn(usize, crate::ThreadMemory) -> Box<dyn crate::block::RawAllocator>
            + Send
            + Sync
            + 'static,
    {
        Self {
            multiplier,
            factory: Arc::new(factory),
        }
    }

    /// 使用 HybridAllocator 的便捷构造函数
    pub fn hybrid(multiplier: crate::ThreadMemoryMultiplier) -> Self {
        Self::new(multiplier, |_thread_idx, memory| {
            Box::new(
                crate::buffer::hybrid::HybridAllocator::new(memory)
                    .expect("Failed to create HybridAllocator"),
            )
        })
    }

    /// 使用 BuddyAllocator 的便捷构造函数
    pub fn buddy(multiplier: crate::ThreadMemoryMultiplier) -> Self {
        Self::new(multiplier, |_thread_idx, memory| {
            Box::new(
                crate::buffer::buddy::BuddyAllocator::new(memory)
                    .expect("Failed to create BuddyAllocator"),
            )
        })
    }
}

impl BlockTopology for UniformBlock {
    fn create_pool(
        &self,
        worker_count: usize,
    ) -> std::io::Result<Arc<crate::global::GlobalBlockPool>> {
        let config = crate::global::GlobalAllocatorConfig {
            multipliers: vec![self.multiplier; worker_count],
        };

        let factory = self.factory.clone();
        let pool = crate::global::GlobalAllocator::new(
            config,
            Box::new(move |thread_idx, memory| (factory)(thread_idx, memory)),
        )?;

        Ok(Arc::new(pool))
    }

    fn build_for_worker(
        &self,
        pool: Arc<crate::global::GlobalBlockPool>,
        worker_index: usize,
        registrar: Box<dyn BufferRegistrar>,
    ) -> AnyBufPool {
        // 在 Block 架构中，我们需要为每个 Worker 注册全局内存块
        let global_info = pool.global_info();
        let region = crate::buffer::BufferRegion::new(global_info.ptr, global_info.len);

        // 注册内存区域
        let global_index = match registrar.register(&[region]) {
            Ok(ids) => ids.first().and_then(|&id| GlobalIndex::new(id as u16)),
            Err(e) => {
                // 如果注册失败，这里是否应该 panic?
                // 通常这意味着驱动出现了严重问题或不支持，降级为无注册 Buffer
                #[cfg(debug_assertions)]
                eprintln!("Failed to register global buffer region: {}", e);
                None
            }
        };

        let block_pool = BlockBasedPool::new(pool, worker_index, global_index);
        AnyBufPool::new(block_pool)
    }
}

impl std::fmt::Debug for UniformBlock {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UniformBlock")
            .field("multiplier", &self.multiplier)
            .field("factory", &"<function>")
            .finish()
    }
}

// 组合注册池

/// A wrapper that binds a backing pool with a registrar.
/// This is the bridge between raw memory and driver-aware buffers.
#[derive(Clone)]
pub struct RegisteredPool<P> {
    pool: P,
    // Rc is required to satisfy Clone for AnyBufPool
    #[allow(dead_code)]
    registrar: std::rc::Rc<dyn BufferRegistrar>,
    registration_ids: std::rc::Rc<Vec<usize>>,
}

impl<P: BackingPool> RegisteredPool<P> {
    pub fn new(
        pool: P,
        registrar: Box<dyn BufferRegistrar>,
        global_info: crate::global::GlobalMemoryInfo,
    ) -> std::io::Result<Self> {
        // God View Registration: Register the SINGLE global block as Index 0.
        let regions = [BufferRegion {
            ptr: global_info.ptr,
            len: global_info.len,
        }];
        let ids = registrar.register(&regions)?;
        Ok(Self {
            pool,
            registrar: std::rc::Rc::from(registrar),
            registration_ids: std::rc::Rc::new(ids),
        })
    }
}

impl<P: BackingPool> std::fmt::Debug for RegisteredPool<P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RegisteredPool")
            .field("pool", &self.pool)
            .field("registration_ids", &self.registration_ids)
            .finish()
    }
}

impl<P: BackingPool> BufPool for RegisteredPool<P> {
    fn alloc(&self, len: NonZeroUsize) -> Option<FixedBuf> {
        match self.pool.alloc_mem(len) {
            AllocResult::Allocated {
                ptr, cap, context, ..
            } => {
                // Use the first registration ID as the global index.
                // For complex multi-region pools, we might need mapping logic,
                // but currently Buddy/Hybrid are single-region arenas.
                let global_index = self
                    .registration_ids
                    .first()
                    .copied()
                    .and_then(|idx| GlobalIndex::new(idx as u16));

                unsafe {
                    let mut buf = FixedBuf::new(
                        ptr,
                        cap,
                        global_index,
                        self.pool.pool_data(),
                        self.pool.vtable(),
                        context,
                    );
                    buf.set_len(len);
                    Some(buf)
                }
            }
            AllocResult::Failed => None,
        }
    }
}

#[derive(Debug)]
pub struct FixedBuf {
    ptr: NonNull<u8>,
    len: NonZeroUsize,
    cap: NonZeroUsize,
    global_index: Option<GlobalIndex>,
    // Metadata moved from Heap Header to Handle
    pool_data: NonNull<()>,
    vtable: &'static PoolVTable,
    context: usize,
}

// Safety: FixedBuf 拥有其底层内存的所有权。
// 使用者需要确保底层的 BufPool 是线程安全的（支持跨线程 Dealloc），
// 或者只在单线程 Runtime 环境下使用。
unsafe impl Send for FixedBuf {}

// Safety: This buffer is generally not Send because it refers to thread-local pool logic
// but in Thread-per-Core it stays on thread.

impl FixedBuf {
    /// # Safety
    /// `ptr` must be valid and allocated by the pool associated with `vtable`.
    #[inline(always)]
    pub unsafe fn new(
        ptr: NonNull<u8>,
        cap: NonZeroUsize,
        global_index: Option<GlobalIndex>,
        pool_data: NonNull<()>,
        vtable: &'static PoolVTable,
        context: usize,
    ) -> Self {
        Self {
            ptr,
            len: cap,
            cap,
            global_index,
            pool_data,
            vtable,
            context,
        }
    }

    #[inline(always)]
    pub fn buf_index(&self) -> Option<GlobalIndex> {
        self.global_index
    }

    /// Resolve which region this buffer belongs to and its offset.
    /// This is used for driver submission (RIO / io_uring).
    #[inline(always)]
    pub fn resolve_region_info(&self) -> (usize, usize) {
        unsafe { (self.vtable.resolve_region_info)(self.pool_data, self) }
    }

    #[inline(always)]
    pub fn as_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), self.len.get()) }
    }

    #[inline(always)]
    pub fn as_slice_mut(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr(), self.len.get()) }
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
        self.len.get()
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        false
    }

    #[inline(always)]
    pub fn set_len(&mut self, len: NonZeroUsize) {
        assert!(len <= self.cap);
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

// ============================================================================
// Block-Based Pool (New Architecture)
// ============================================================================

// ============================================================================
// Block-Based Pool (New Architecture with Local & Global State)
// ============================================================================

/// Thread-Local Pool State wrapper.
///
/// This structure acts as a thread-local anchor for the GlobalBlockPool.
/// Each worker thread (and its associated BlockBasedPool) holds its own instance of `LocalPoolState`
/// wrapped in an `Arc`.
///
/// When `FixedBuf` takes a reference to the pool, it increments the ref-count of this LOCAL
/// `Arc<LocalPoolState>`, effectively sharding the reference counting traffic.
/// The underlying `GlobalBlockPool` is kept alive by `LocalPoolState` holding a strong reference to it.
struct LocalPoolState {
    global: CachePadded<Arc<crate::global::GlobalBlockPool>>,
}

/// 基于 GlobalBlockPool 的 Pool 实现
///
/// 这个 Pool 对应单个线程，通过 GlobalBlockPool 进行分配和释放。
/// context 字段编码了 block_idx，以便在释放时找到正确的 Block。
#[derive(Clone)]
pub struct BlockBasedPool {
    /// 本地状态 (Sharded Ref-Counting Anchor)
    state: Arc<LocalPoolState>,
    /// 当前线程的索引
    thread_idx: usize,
    /// 全局索引（用于驱动注册）
    global_index: Option<GlobalIndex>,
}

impl BlockBasedPool {
    /// 创建新的 BlockBasedPool
    pub fn new(
        pool: Arc<crate::global::GlobalBlockPool>,
        thread_idx: usize,
        global_index: Option<GlobalIndex>,
    ) -> Self {
        Self {
            state: Arc::new(LocalPoolState {
                global: CachePadded::new(pool),
            }),
            thread_idx,
            global_index,
        }
    }
}

impl std::fmt::Debug for BlockBasedPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BlockBasedPool")
            .field("thread_idx", &self.thread_idx)
            .field("global_index", &self.global_index)
            .finish()
    }
}

impl BackingPool for BlockBasedPool {
    fn alloc_mem(&self, size: NonZeroUsize) -> AllocResult {
        // 通过 GlobalBlockPool 的 4 级优先级分配
        if let Some((block_idx, result)) = self.state.global.alloc(self.thread_idx, size.get()) {
            // 将 block_idx 编码到 context 的高 32 位
            let combined_context = ((block_idx as usize) << 32) | (result.context & 0xFFFFFFFF);

            AllocResult::Allocated {
                ptr: result.ptr,
                cap: result.cap,
                global_index: self.global_index,
                context: combined_context,
            }
        } else {
            AllocResult::Failed
        }
    }

    fn vtable(&self) -> &'static PoolVTable {
        &BLOCK_BASED_POOL_VTABLE
    }

    fn pool_data(&self) -> NonNull<()> {
        // Critical Fix: FixedBuf holds a strong reference to LocalPoolState (wrapper).
        // This increments the LOCAL Arc counter, avoiding global contention.
        let arc = self.state.clone();
        let ptr = Arc::into_raw(arc) as *mut ();
        unsafe { NonNull::new_unchecked(ptr) }
    }
}

impl BufPool for BlockBasedPool {
    fn alloc(&self, len: NonZeroUsize) -> Option<FixedBuf> {
        self.alloc_mem(len).into_buf(self)
    }
}

// VTable for BlockBasedPool
static BLOCK_BASED_POOL_VTABLE: PoolVTable = PoolVTable {
    dealloc: block_based_dealloc_shim,
    resolve_region_info: block_based_resolve_region_info_shim,
};

unsafe fn block_based_dealloc_shim(pool_data: NonNull<()>, params: DeallocParams) {
    // 1. Recover ownership of the Arc<LocalPoolState>
    // This pointer was created via Arc::into_raw in pool_data()
    let raw_ptr = pool_data.as_ptr() as *const LocalPoolState;
    let state = unsafe { Arc::from_raw(raw_ptr) };

    // 2. Extract block_idx from context (high 32 bits)
    let block_idx = (params.context >> 32) as usize;
    let allocator_context = params.context & 0xFFFFFFFF;

    // 3. Delegate to GlobalBlockPool via LocalPoolState
    unsafe {
        state
            .global
            .dealloc(block_idx, params.ptr, params.cap.get(), allocator_context);
    }

    // 4. state (Arc) goes out of scope here and is dropped, decrementing the LOCAL ref count.
}

unsafe fn block_based_resolve_region_info_shim(
    pool_data: NonNull<()>,
    buf: &FixedBuf,
) -> (usize, usize) {
    // 1. Access LocalPoolState temporarily
    // We do NOT take ownership (do not drop the Arc), just use the pointer.
    let raw_ptr = pool_data.as_ptr() as *const LocalPoolState;
    let state = unsafe { &*raw_ptr };

    // 2. Get base address from global info
    let global_info = state.global.global_info();
    let base = global_info.ptr.as_ptr() as usize;
    let ptr = buf.as_ptr() as usize;

    // 3. return (region_index, offset)
    // We use the global_index stored in the FixedBuf itself
    let region_index = buf.buf_index().map(|idx| idx.get() as usize).unwrap_or(0);
    (region_index, ptr.saturating_sub(base))
}
