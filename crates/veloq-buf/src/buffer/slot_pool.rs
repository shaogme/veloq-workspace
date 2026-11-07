//! Slot-based buffer pool implementation.

use diagweave::{DiagnosticError, Transform};
use veloq_std::{
    boxed::Box,
    cell::Cell,
    fmt::{self, Debug, Formatter},
    format,
    num::NonZeroUsize,
    ptr::{NonNull, null},
    sync::{Arc, UnpoisonedMutex},
    vec::Vec,
};

use super::{
    any::AnyBufPool,
    common::{
        AllocResult, BackingPool, BufPool, BufferRegion, BufferRegistrar, PoolKind, RegionInfo,
        RegistrationCapability,
    },
    error::{BufError, BufResult},
    handle::{FixedBuf, PackedContext},
    locked_memory::{LockedMemoryBudget, LockedMemoryPermit},
};
use crate::{
    heap::{
        ChunkId, GlobalAllocatorConfig, GlobalSlotPool, SLOT_SIZE, SlotIndex, buddy::BuddyAllocator,
    },
    os,
};

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
    fn init(&self, context: PoolInitContext) -> BufResult<Self::State>;

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
}

#[derive(Clone)]
pub struct PoolInitContext {
    worker_count: NonZeroUsize,
    registration_workers: Box<[usize]>,
    budget: LockedMemoryBudget,
}

pub struct UniformPoolState {
    registration_workers: Box<[usize]>,
    pool: Option<Arc<GlobalSlotPool>>,
    reservations: UnpoisonedMutex<Vec<Option<Vec<LockedMemoryPermit>>>>,
}

impl UniformPoolState {
    fn heap_only(worker_count: NonZeroUsize, registration_workers: Box<[usize]>) -> Self {
        Self {
            registration_workers,
            pool: None,
            reservations: UnpoisonedMutex::new((0..worker_count.get()).map(|_| None).collect()),
        }
    }

    fn registered(
        worker_count: NonZeroUsize,
        registration_workers: Box<[usize]>,
        pool: Arc<GlobalSlotPool>,
        permits: Vec<LockedMemoryPermit>,
    ) -> Self {
        let mut reservations: Vec<Option<Vec<LockedMemoryPermit>>> =
            (0..worker_count.get()).map(|_| None).collect();
        for (worker, permit) in registration_workers.iter().copied().zip(permits) {
            reservations[worker] = Some(Vec::from([permit]));
        }
        Self {
            registration_workers,
            pool: Some(pool),
            reservations: UnpoisonedMutex::new(reservations),
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct HeapOnlyPool;

impl BufPool for HeapOnlyPool {
    fn alloc(&self, cap: NonZeroUsize, len: usize) -> Option<FixedBuf> {
        FixedBuf::alloc_heap(cap, len).ok()
    }
}

impl PoolInitContext {
    pub fn new(
        worker_count: usize,
        registration_workers: Vec<usize>,
        budget: LockedMemoryBudget,
    ) -> BufResult<Self> {
        let worker_count = NonZeroUsize::new(worker_count)
            .ok_or_else(|| BufError::Other("worker count must be non-zero".into()).to_report())?;
        let mut workers = registration_workers;
        workers.sort_unstable();
        if workers.iter().any(|worker| *worker >= worker_count.get())
            || workers.windows(2).any(|pair| pair[0] == pair[1])
        {
            return BufError::Other("invalid or repeated registration worker index".into()).trans();
        }
        Ok(Self {
            worker_count,
            registration_workers: workers.into_boxed_slice(),
            budget,
        })
    }

    pub fn worker_count(&self) -> NonZeroUsize {
        self.worker_count
    }

    pub fn registration_workers(&self) -> &[usize] {
        &self.registration_workers
    }

    pub fn budget(&self) -> &LockedMemoryBudget {
        &self.budget
    }
}

/// 标准 Slot 拓扑：使用 GlobalSlotPool
#[derive(Clone)]
pub struct UniformSlot {
    capacity_bytes: usize,
}

impl UniformSlot {
    /// 创建新的 UniformSlot topology
    pub fn new(capacity_bytes: usize) -> Self {
        Self { capacity_bytes }
    }

    pub fn create_pool(&self, worker_count: usize) -> BufResult<Arc<GlobalSlotPool>> {
        let _ = worker_count;
        let capacity = self.capacity_bytes / SLOT_SIZE * SLOT_SIZE;
        if capacity == 0 {
            return BufError::ChunkTooSmall.trans();
        }
        Ok(Arc::new(GlobalSlotPool::new(GlobalAllocatorConfig {
            total_memory: capacity,
        })?))
    }
}

impl PoolTopology for UniformSlot {
    type State = Arc<UniformPoolState>;

    fn init(&self, context: PoolInitContext) -> BufResult<Self::State> {
        let worker_count = context.registration_workers.len();
        if worker_count == 0 || self.capacity_bytes < SLOT_SIZE {
            return Ok(Arc::new(UniformPoolState::heap_only(
                context.worker_count(),
                context.registration_workers,
            )));
        }
        let page_size = os::page_size().ok_or_else(|| {
            BufError::Other("failed to query target OS page size".into()).to_report()
        })?;
        if !page_size.is_power_of_two() {
            return BufError::Other("target OS page size is not a power of two".into()).trans();
        }
        let requested_capacity = self.capacity_bytes / SLOT_SIZE * SLOT_SIZE;
        loop {
            let available = context.budget.available_bytes();
            let per_worker_limit = available / worker_count;
            let mut capacity = requested_capacity.min(per_worker_limit) / SLOT_SIZE * SLOT_SIZE;
            let charge = loop {
                if capacity < SLOT_SIZE {
                    return Ok(Arc::new(UniformPoolState::heap_only(
                        context.worker_count(),
                        context.registration_workers,
                    )));
                }
                let rounded = capacity
                    .checked_add(page_size - 1)
                    .ok_or(BufError::SizeOverflow)?
                    & !(page_size - 1);
                if rounded <= per_worker_limit {
                    break rounded;
                }
                capacity -= SLOT_SIZE;
            };
            let Some(permits) = context
                .budget
                .try_reserve_repeated(charge, worker_count)
                .map_err(|error| BufError::Other(format!("{error:?}")).to_report())?
            else {
                continue;
            };
            let pool = Arc::new(GlobalSlotPool::new(GlobalAllocatorConfig {
                total_memory: capacity,
            })?);
            let state = UniformPoolState::registered(
                context.worker_count(),
                context.registration_workers,
                pool,
                permits,
            );
            return Ok(Arc::new(state));
        }
    }

    fn build(
        &self,
        state: &Self::State,
        worker_idx: usize,
        registrar: &dyn BufferRegistrar,
    ) -> BufResult<AnyBufPool> {
        let Some(pool) = state.pool.as_ref() else {
            return Ok(AnyBufPool::new(HeapOnlyPool));
        };
        if !state.registration_workers.contains(&worker_idx) {
            return Ok(AnyBufPool::new(HeapOnlyPool));
        }
        let permits = state.reservations.lock()[worker_idx]
            .take()
            .ok_or_else(|| {
                BufError::Other("worker reservation was already consumed".into()).to_report()
            })?;
        let mut regions: Vec<BufferRegion> = pool
            .chunk_infos()
            .into_iter()
            .map(|info| {
                // SAFETY: `info` comes from `pool`, and each region owner retains that pool. / 安全性：`info` 来自 `pool`，每个 region owner 都会保留该 pool。
                unsafe { BufferRegion::from_chunk_info(info, pool.clone()) }
            })
            .collect();
        if permits.len() != regions.len() {
            return BufError::Other("reservation bundle does not match pool regions".into())
                .trans();
        }
        for (region, permit) in regions.iter_mut().zip(permits) {
            region.set_permit(permit);
        }

        let mut ids = registrar.register(&mut regions)?;
        if ids
            .iter()
            .any(|id| !regions.iter().any(|region| region.id() == *id))
        {
            return BufError::InvalidChunkId.trans();
        }
        ids.sort_unstable();
        if ids.windows(2).any(|pair| pair[0] == pair[1]) {
            return BufError::InvalidChunkId.trans();
        }

        let slot_pool = SlotBasedPool::with_registered_chunks(pool.clone(), worker_idx, ids)?;
        Ok(AnyBufPool::new(slot_pool))
    }
}

/// 基于 GlobalSlotPool 的 Pool 实现
///
/// 这个 Pool 使用 GlobalSlotPool 来分配内存。
#[derive(Clone)]
pub struct SlotBasedPool {
    /// 全局 Slot Pool 的引用 (Arc)
    pool: Arc<GlobalSlotPool>,
    /// Optional seed for deterministic shard selection
    seed: Option<usize>,
    registered_chunks: Box<[ChunkId]>,
}

impl SlotBasedPool {
    /// 创建新的 SlotBasedPool
    pub fn new(pool: Arc<GlobalSlotPool>) -> Self {
        Self {
            pool,
            seed: None,
            registered_chunks: Box::new([]),
        }
    }

    /// 使用特定的 seed 创建 SlotBasedPool，确保 shard 选择是确定性的
    pub fn with_seed(pool: Arc<GlobalSlotPool>, seed: usize) -> Self {
        Self {
            pool,
            seed: Some(seed),
            registered_chunks: Box::new([]),
        }
    }

    /// 创建仅从调用方成功注册区域分配的 pool 视图。
    /// Create a pool view limited to chunks that the caller successfully registered.
    ///
    /// 调用方只能传入 `BufferRegistrar` 为这些区域成功返回的 ID。
    /// The caller must pass only IDs returned by its `BufferRegistrar` for these exact regions.
    pub fn with_registered_chunks(
        pool: Arc<GlobalSlotPool>,
        seed: usize,
        mut registered_chunks: Vec<ChunkId>,
    ) -> BufResult<Self> {
        if registered_chunks
            .iter()
            .any(|id| pool.chunk_info(*id).is_none())
        {
            return BufError::InvalidChunkId.trans();
        }
        registered_chunks.sort_unstable();
        if registered_chunks.windows(2).any(|pair| pair[0] == pair[1]) {
            return BufError::InvalidChunkId.trans();
        }
        Ok(Self {
            pool,
            seed: Some(seed),
            registered_chunks: registered_chunks.into_boxed_slice(),
        })
    }

    /// Calculate order for a given size
    fn calculate_order(size: NonZeroUsize) -> Option<usize> {
        let size = size.get();
        if size <= 4096 {
            Some(0)
        } else {
            // (size + 4095) / 4096 next power of two
            // But buddy allocator works better if we just use ilog2 of (rounded up to power of two)
            // if size = 4097 -> 8192 -> order 1
            let needed = size.checked_next_power_of_two()?;
            Some(needed.ilog2() as usize - 12)
        }
    }
}

impl Debug for SlotBasedPool {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("SlotBasedPool").finish()
    }
}

const ARC_CACHE_LIMIT: usize = 64;

#[derive(Copy, Clone)]
struct PoolCacheInner {
    ptr: *const GlobalSlotPool,
    balance: u32,
}

struct ThreadLocalPoolCache(Cell<PoolCacheInner>);

impl ThreadLocalPoolCache {
    /// 构造一个新的空缓存。由于字段是简单的原始类型和指针，此构造函数是 const 的。
    const fn new() -> Self {
        Self(Cell::new(PoolCacheInner {
            ptr: null(),
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

veloq_tls::veloq_tls! {
    /// 缓存当前线程使用的全局池引用计数。
    static POOL_CACHE: ThreadLocalPoolCache = ThreadLocalPoolCache::new();
}

#[inline]
fn with_pool_cache<R>(f: impl FnOnce(&ThreadLocalPoolCache) -> R) -> R {
    POOL_CACHE.with_or_init(f, POOL_CACHE::init)
}

impl BackingPool for SlotBasedPool {
    fn alloc_mem(&self, size: NonZeroUsize) -> AllocResult {
        let Some(order) = Self::calculate_order(size) else {
            return AllocResult::Failed;
        };

        if let Some((chunk_id, slot_idx, ptr)) =
            self.pool
                .alloc_slots(order, self.seed, &self.registered_chunks)
        {
            let capacity = BuddyAllocator::capacity_of(order);
            let context_data = PackedContext::from_slot_parts(
                slot_idx.get() as u32,
                order as u8,
                chunk_id,
                PoolKind::SlotBased,
                true,
            );
            let context = u64::from(context_data);

            let cap = unsafe { NonZeroUsize::new_unchecked(capacity) };

            AllocResult::Allocated {
                ptr,
                cap,
                context,
                registration: RegistrationCapability::RegisteredChunk(chunk_id),
            }
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
    fn alloc(&self, cap: NonZeroUsize, len: usize) -> Option<FixedBuf> {
        if let Some(buf) = self.alloc_mem(cap).into_buf(self, len) {
            return Some(buf);
        }

        // Pool 容量固定，耗尽后回退为普通未注册内存。
        // The pool has fixed capacity, so exhaustion falls back to ordinary unregistered memory.
        FixedBuf::alloc_heap(cap, len).ok()
    }
}

pub(crate) unsafe fn slot_based_dealloc(pool_data: NonNull<()>, context: u64) {
    let raw_ptr = pool_data.as_ptr() as *const GlobalSlotPool;

    let ctx = PackedContext::from(context);
    let chunk_id = ctx.slot_chunk_id();
    let order = ctx.order() as usize;
    let slot_idx = SlotIndex(ctx.slot_idx() as usize);

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
    let pool = unsafe { &*(pool_data.as_ptr() as *const GlobalSlotPool) };

    // 2. Unpack ChunkID
    let ctx = PackedContext::from(buf.context_raw());
    let chunk_id = ctx.slot_chunk_id();

    // 3. Get base address from ChunkInfo
    let chunk_info = pool
        .chunk_info(chunk_id)
        .expect("FixedBuf has invalid ChunkID");
    let base = chunk_info.ptr.as_ptr() as usize;
    let ptr = buf.as_ptr() as usize;

    RegionInfo {
        pool_kind: PoolKind::SlotBased,
        registration: if ctx.registered() {
            RegistrationCapability::RegisteredChunk(chunk_id)
        } else {
            RegistrationCapability::Unregistered
        },
        id: chunk_id,
        base_ptr: chunk_info.ptr,
        region_len: chunk_info.len,
        offset: ptr.saturating_sub(base),
        cookie: 0, // Cookies are currently only used for heap buffers
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::heap::SUPERBLOCK_SIZE;
    use veloq_std::cell::Cell;
    use veloq_std::vec;

    struct RecordingRegistrar {
        calls: Cell<usize>,
    }

    impl RecordingRegistrar {
        fn new() -> Self {
            Self {
                calls: Cell::new(0),
            }
        }
    }

    impl BufferRegistrar for RecordingRegistrar {
        fn register(&self, regions: &mut [BufferRegion]) -> BufResult<Vec<ChunkId>> {
            self.calls.set(self.calls.get() + 1);
            Ok(regions.iter().map(BufferRegion::id).collect())
        }
    }

    fn initialized_registered_pool(capacity_bytes: usize) -> (Arc<UniformPoolState>, AnyBufPool) {
        let topology = UniformSlot::new(capacity_bytes);
        let budget = LockedMemoryBudget::default();
        let context = PoolInitContext::new(1, vec![0], budget).expect("valid pool context");
        let state = topology.init(context).expect("initialize pool");
        let pool = topology
            .build(&state, 0, &RecordingRegistrar::new())
            .expect("build registered pool");
        (state, pool)
    }

    #[test]
    fn registered_pool_covers_slot_capacity_boundaries() {
        let slot_size = NonZeroUsize::new(SLOT_SIZE).expect("slot size is non-zero");

        for slot_count in [1, 63, 64, 65] {
            let capacity_bytes = slot_count * SLOT_SIZE;
            let (state, pool) = initialized_registered_pool(capacity_bytes);
            let backing_pool = state.pool.as_ref().expect("capacity should be registered");
            let region = backing_pool
                .chunk_info(ChunkId::ZERO)
                .expect("registered chunk exists");
            assert_eq!(region.len.get(), capacity_bytes);

            let mut buffers = Vec::with_capacity(slot_count);
            let mut offsets = Vec::with_capacity(slot_count);
            for _ in 0..slot_count {
                let buffer = pool
                    .alloc_full(slot_size)
                    .expect("registered slot allocation");
                let info = buffer.resolve_region_info();
                assert_eq!(info.pool_kind, PoolKind::SlotBased);
                assert_eq!(
                    info.registration,
                    RegistrationCapability::RegisteredChunk(ChunkId::ZERO)
                );
                assert_eq!(info.region_len.get(), capacity_bytes);
                offsets.push(info.offset);
                buffers.push(buffer);
            }

            offsets.sort_unstable();
            assert_eq!(
                offsets,
                (0..slot_count)
                    .map(|slot| slot * SLOT_SIZE)
                    .collect::<Vec<_>>()
            );

            let fallback = pool
                .alloc_full(slot_size)
                .expect("heap fallback allocation");
            let info = fallback.resolve_region_info();
            assert_eq!(info.pool_kind, PoolKind::Heap);
            assert_eq!(info.registration, RegistrationCapability::Unregistered);
        }
    }

    #[test]
    fn registered_pool_keeps_shards_and_tail_disjoint() {
        let total_slots = SUPERBLOCK_SIZE * 4 + 3;
        let capacity_bytes = total_slots * SLOT_SIZE;
        let (state, pool) = initialized_registered_pool(capacity_bytes);
        let backing_pool = state.pool.as_ref().expect("capacity should be registered");
        let chunk = backing_pool.chunks.read()[0].clone();
        let main_slots = chunk.superblocks.len() * SUPERBLOCK_SIZE;
        let shard_count = chunk.shards.len();
        let slots_per_shard = chunk.slots_per_shard.get();

        assert_eq!(chunk.superblocks.len(), 4);
        assert!(shard_count.is_power_of_two());
        assert_eq!(slots_per_shard * shard_count, main_slots);
        assert!(slots_per_shard.is_multiple_of(SUPERBLOCK_SIZE));
        assert_eq!(total_slots - main_slots, 3);

        let slot_size = NonZeroUsize::new(SLOT_SIZE).expect("slot size is non-zero");
        let mut buffers = Vec::with_capacity(total_slots);
        let mut offsets = Vec::with_capacity(total_slots);
        for _ in 0..total_slots {
            let buffer = pool
                .alloc_full(slot_size)
                .expect("registered slot allocation");
            let info = buffer.resolve_region_info();
            assert_eq!(info.pool_kind, PoolKind::SlotBased);
            assert_eq!(
                info.registration,
                RegistrationCapability::RegisteredChunk(ChunkId::ZERO)
            );
            assert_eq!(info.region_len.get(), capacity_bytes);
            assert_eq!(info.offset % SLOT_SIZE, 0);
            offsets.push(info.offset);
            buffers.push((info.offset, buffer));
        }

        offsets.sort_unstable();
        assert_eq!(
            offsets,
            (0..total_slots)
                .map(|slot| slot * SLOT_SIZE)
                .collect::<Vec<_>>()
        );

        let mut shard_slot_counts = vec![0; shard_count];
        for offset in &offsets {
            let slot = offset / SLOT_SIZE;
            if slot < main_slots {
                let shard = slot / slots_per_shard;
                assert!(shard < shard_count);
                shard_slot_counts[shard] += 1;
            }
        }
        assert!(
            shard_slot_counts
                .iter()
                .all(|slot_count| *slot_count == slots_per_shard)
        );

        let tail_start = main_slots * SLOT_SIZE;
        let tail_buffer_index = buffers
            .iter()
            .position(|(offset, _)| *offset >= tail_start)
            .expect("tail allocation exists");
        let (released_offset, released_buffer) = buffers.swap_remove(tail_buffer_index);
        drop(released_buffer);

        let replacement = pool
            .alloc_full(slot_size)
            .expect("tail slot should be reusable");
        let replacement_info = replacement.resolve_region_info();
        assert_eq!(replacement_info.offset, released_offset);
        assert_eq!(replacement_info.pool_kind, PoolKind::SlotBased);
        assert_eq!(
            replacement_info.registration,
            RegistrationCapability::RegisteredChunk(ChunkId::ZERO)
        );
        buffers.push((replacement_info.offset, replacement));

        let fallback = pool
            .alloc_full(slot_size)
            .expect("heap fallback allocation");
        let fallback_info = fallback.resolve_region_info();
        assert_eq!(fallback_info.pool_kind, PoolKind::Heap);
        assert_eq!(
            fallback_info.registration,
            RegistrationCapability::Unregistered
        );
    }

    #[test]
    fn sub_slot_capacities_build_heap_only_pools() {
        let slot_size = NonZeroUsize::new(SLOT_SIZE).expect("slot size is non-zero");

        for capacity_bytes in [0, SLOT_SIZE - 1] {
            let topology = UniformSlot::new(capacity_bytes);
            let budget = LockedMemoryBudget::default();
            let context =
                PoolInitContext::new(1, vec![0], budget.clone()).expect("valid pool context");
            let state = topology.init(context).expect("initialize heap-only pool");
            assert!(state.pool.is_none());
            assert_eq!(budget.used_bytes(), 0);

            let registrar = RecordingRegistrar::new();
            let pool = topology
                .build(&state, 0, &registrar)
                .expect("build heap-only pool");
            assert_eq!(registrar.calls.get(), 0);

            let buffer = pool.alloc_full(slot_size).expect("heap allocation");
            let info = buffer.resolve_region_info();
            assert_eq!(info.pool_kind, PoolKind::Heap);
            assert_eq!(info.registration, RegistrationCapability::Unregistered);
        }
    }
}
