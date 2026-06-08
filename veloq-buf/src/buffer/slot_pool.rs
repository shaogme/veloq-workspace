//! Slot-based buffer pool implementation.

use std::cell::Cell;
use std::num::NonZeroUsize;
use std::ptr::NonNull;
use std::sync::Arc;

use super::any::AnyBufPool;
use super::common::{
    AllocResult, BackingPool, BufPool, BufferRegion, BufferRegistrar, PoolKind, RegionInfo,
};
use super::error::{BufError, BufResult};
use super::handle::{FixedBuf, PackedContext};

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
    fn init(&self, worker_count: usize) -> BufResult<Self::State>;

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
        registrar: &dyn BufferRegistrar,
    ) -> BufResult<AnyBufPool>;

    /// Connect a listener to the shared state to receive notifications about new memory chunks.
    /// Used for dynamic expansion.
    fn connect_listener(
        &self,
        state: &Self::State,
        listener: Box<dyn Fn(crate::heap::ChunkInfo) + Send + Sync>,
    );
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
    pub fn create_pool(&self, worker_count: usize) -> BufResult<Arc<crate::heap::GlobalSlotPool>> {
        self.init(worker_count)
    }
}

impl PoolTopology for UniformSlot {
    type State = Arc<crate::heap::GlobalSlotPool>;

    fn init(&self, worker_count: usize) -> BufResult<Self::State> {
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
        registrar: &dyn BufferRegistrar,
    ) -> BufResult<AnyBufPool> {
        // 在 Slot 架构中，所有线程共享一个大的连续区域
        let regions = pool
            .chunk_infos()
            .into_iter()
            .map(|info| {
                BufferRegion::from_chunk_info(info).ok_or(BufError::PageUnaligned {
                    size: info.len.get(),
                })
            })
            .collect::<Result<Vec<_>, _>>()?;

        // 注册内存区域
        let _ids = registrar.register(&regions)?;

        let slot_pool = SlotBasedPool::with_seed(pool.clone(), worker_idx);
        Ok(AnyBufPool::new(slot_pool))
    }

    fn connect_listener(
        &self,
        state: &Self::State,
        listener: Box<dyn Fn(crate::heap::ChunkInfo) + Send + Sync>,
    ) {
        state.set_listener(listener);
    }
}

/// 基于 GlobalSlotPool 的 Pool 实现
///
/// 这个 Pool 使用 GlobalSlotPool 来分配内存。
#[derive(Clone)]
pub struct SlotBasedPool {
    /// 全局 Slot Pool 的引用 (Arc)
    pub(crate) pool: Arc<crate::heap::GlobalSlotPool>,
    /// Optional seed for deterministic shard selection
    pub(crate) seed: Option<usize>,
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
                slot_idx.get() as u32,
                order as u8,
                chunk_id.get(),
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

pub(crate) unsafe fn slot_based_dealloc(pool_data: NonNull<()>, context: u64) {
    let raw_ptr = pool_data.as_ptr() as *const crate::heap::GlobalSlotPool;

    let ctx = PackedContext::from(context);
    let chunk_id = ctx.chunk_id();
    let order = ctx.order() as usize;
    let slot_idx = crate::heap::SlotIndex(ctx.slot_idx() as usize);

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

pub(crate) unsafe fn slot_based_resolve_region_info(
    pool_data: NonNull<()>,
    buf: &FixedBuf,
) -> RegionInfo {
    // 1. Cast back to GlobalSlotPool
    let pool = unsafe { &*(pool_data.as_ptr() as *const crate::heap::GlobalSlotPool) };

    // 2. Unpack ChunkID
    let ctx = PackedContext::from(buf.context_raw());
    let chunk_id = crate::heap::ChunkId::from(ctx.chunk_id());

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
