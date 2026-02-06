use super::{
    AllocError, AllocResult, AnyBufPool, BackingPool, DeallocParams, FixedBuf, PoolSpec,
    PoolVTable, RegisteredPool,
};
use crate::{ThreadMemory, nz};
use crossbeam_queue::SegQueue;
use std::cell::UnsafeCell;
use std::num::NonZeroUsize;
use std::ptr::NonNull;
use std::thread;

// Buddy System Constants
const ARENA_SIZE: NonZeroUsize = nz!(32 * 1024 * 1024); // 32MB Total to support higher concurrency with overhead
const MIN_BLOCK_SIZE: usize = 4096; // 4KB to support 4KB payload with 4KB alignment

// Number of orders: 4KB, 8KB, 16KB, 32KB, 64KB, 128KB, 256KB, 512KB, 1MB, 2MB, 4MB, 8MB, 16MB, 32MB
const NUM_ORDERS: usize = 14;

// Max order to cache in slabs (Order 5 = 128KB)
const MAX_SLAB_ORDER: usize = 5;

// Capacities for each slab order (aiming for ~4MB cache per order to avoid excessive overhead)
const SLAB_CAPACITIES: [usize; MAX_SLAB_ORDER + 1] = [
    1024, // Order 0 (4KB)   -> 4MB
    512,  // Order 1 (8KB)   -> 4MB
    256,  // Order 2 (16KB)  -> 4MB
    128,  // Order 3 (32KB)  -> 4MB
    64,   // Order 4 (64KB)  -> 4MB
    32,   // Order 5 (128KB) -> 4MB
];

const TAG_ALLOCATED: u8 = 0x80;
const TAG_ORDER_MASK: u8 = 0x7F;

/// 侵入式双向链表节点，存储在空闲块的头部
#[repr(C)]
struct FreeNode {
    prev: Option<NonNull<FreeNode>>,
    next: Option<NonNull<FreeNode>>,
}

/// 侵入式链表封装，管理 FreeNode
#[derive(Clone, Copy)]
struct FreeList {
    head: Option<NonNull<FreeNode>>,
}

impl FreeList {
    fn new() -> Self {
        Self { head: None }
    }

    fn is_empty(&self) -> bool {
        self.head.is_none()
    }

    ///将节点插入头部
    unsafe fn push(&mut self, mut node_ptr: NonNull<FreeNode>) {
        // SAFETY: Caller guarantees node_ptr is valid and exclusive for modification
        let node = unsafe { node_ptr.as_mut() };
        node.next = self.head;
        node.prev = None;

        if let Some(mut head_ptr) = self.head {
            // SAFETY: Existing head is valid as per FreeList invariants
            unsafe { head_ptr.as_mut() }.prev = Some(node_ptr);
        }
        self.head = Some(node_ptr);
    }

    /// 弹出头部节点
    unsafe fn pop(&mut self) -> Option<NonNull<FreeNode>> {
        let mut head_ptr = self.head?;
        // SAFETY: Head pointer is valid as per FreeList invariants
        let head = unsafe { head_ptr.as_mut() };
        let next = head.next;

        self.head = next;
        if let Some(mut next_ptr) = next {
            // SAFETY: Next pointer is valid as per FreeList invariants
            unsafe { next_ptr.as_mut() }.prev = None;
        }

        // 清理指针
        head.next = None;
        head.prev = None;

        Some(head_ptr)
    }

    /// 移除指定节点
    unsafe fn remove(&mut self, mut node_ptr: NonNull<FreeNode>) {
        // SAFETY: Caller guarantees node_ptr is valid and in this list
        let node = unsafe { node_ptr.as_mut() };
        let prev = node.prev;
        let next = node.next;

        if let Some(mut prev_ptr) = prev {
            // SAFETY: Prev pointer is valid as per FreeList invariants
            unsafe { prev_ptr.as_mut() }.next = next;
        } else {
            // 是头节点
            self.head = next;
        }

        if let Some(mut next_ptr) = next {
            // SAFETY: Next pointer is valid as per FreeList invariants
            unsafe { next_ptr.as_mut() }.prev = prev;
        }

        node.prev = None;
        node.next = None;
    }
}

/// 地址计算辅助器
struct BlockCalculator {
    base_ptr: *mut u8,
}

impl BlockCalculator {
    fn new(base_ptr: *mut u8) -> Self {
        Self { base_ptr }
    }

    /// 获取相对于基地址的偏移量
    /// SAFETY: ptr 必须在 Arena 范围内
    unsafe fn offset_of(&self, ptr: NonNull<u8>) -> usize {
        // SAFETY: Caller guarantees ptr is valid relative to base_ptr
        unsafe { ptr.as_ptr().offset_from(self.base_ptr) as usize }
    }

    /// 根据偏移量获取指针
    /// SAFETY: offset 必须在 valid range
    unsafe fn ptr_at(&self, offset: usize) -> NonNull<u8> {
        // SAFETY: Caller guarantees offset is valid; NonNull::new_unchecked is safe for valid arena ptrs
        unsafe { NonNull::new_unchecked(self.base_ptr.add(offset)) }
    }

    /// 根据偏移量获取对应的 Tag 索引 (block_idx)
    fn tag_index(&self, offset: usize) -> usize {
        offset / MIN_BLOCK_SIZE
    }

    /// 计算给定 Order 的块大小
    fn block_size(&self, order: usize) -> usize {
        MIN_BLOCK_SIZE << order
    }

    /// 计算 Buddy 的偏移量
    fn buddy_offset(&self, offset: usize, order: usize) -> usize {
        offset ^ self.block_size(order)
    }
}

/// 核心分配器逻辑，管理内存块和空闲列表 (无缓存层)
struct RawBuddyAllocator {
    // 保持对内存的所有权
    _memory_owner: ThreadMemory,

    // 地址计算辅助器 (替代原本的 base_ptr)
    calculator: BlockCalculator,

    // 每个阶数（Order）对应的空闲链表
    free_lists: [FreeList; NUM_ORDERS],

    // 位图索引，加速空闲块查找
    free_bitmap: u16,

    // 块标签数组，索引为 block_offset / 4096
    // 记录块的 Order 和是否已分配状态
    tags: Vec<u8>,
}

impl RawBuddyAllocator {
    fn new(memory: ThreadMemory) -> Result<Self, AllocError> {
        // Check memory size
        if memory.len() < ARENA_SIZE.get() {
            // For now fail if provided memory is less than ARENA_SIZE
            // In future we could adjust ARENA_SIZE dynamically
            return Err(AllocError::Oom);
        }

        let base_ptr = memory.as_ptr() as *mut u8;
        let calculator = BlockCalculator::new(base_ptr);

        let mut free_lists = [FreeList::new(); NUM_ORDERS];

        // 初始化最大的块（Order 12, 16MB） -> Wait, order 13 is 32MB. 2^12 * 4096 = 16M?
        // MIN_BLOCK = 4096 (2^12.
        // MAX Order = 13 (NUM_ORDERS - 1). Size = 4096 << 13 = 32MB.
        let max_order = NUM_ORDERS - 1;

        // SAFETY: 刚刚分配的内存，指针有效且大小足够
        let root_node_ptr = unsafe { NonNull::new_unchecked(base_ptr as *mut FreeNode) };
        unsafe {
            *(base_ptr as *mut FreeNode) = FreeNode {
                prev: None,
                next: None,
            };
        }

        free_lists[max_order].head = Some(root_node_ptr);
        let free_bitmap = 1 << max_order;

        let leaf_count = ARENA_SIZE.get() / MIN_BLOCK_SIZE;
        let mut tags = vec![0u8; leaf_count];
        // 标记第一个最大块为空闲
        tags[0] = max_order as u8;

        Ok(Self {
            _memory_owner: memory,
            calculator,
            free_lists,
            free_bitmap,
            tags,
        })
    }

    fn global_region(&self) -> (NonNull<u8>, usize) {
        self._memory_owner.global_region()
    }

    /// 分配指定 Order 的内存块
    fn alloc(&mut self, order: usize) -> Option<NonNull<u8>> {
        // 寻找合适的空闲块 - Bitmap 加速查找 (O(1))
        let search_mask = 0xFFFFu16 << order;
        let candidates = self.free_bitmap & search_mask;

        if candidates == 0 {
            return None;
        }

        // 找到最小的满足条件的 Order
        let found_order = candidates.trailing_zeros() as usize;

        // SAFETY: bitmap 对应的位为 1，意味着 free_lists[found_order] 必定非空
        let node_ptr = unsafe { self.free_lists[found_order].pop().unwrap_unchecked() };
        if self.free_lists[found_order].is_empty() {
            self.free_bitmap &= !(1 << found_order);
        }

        let mut curr_order = found_order;
        // 此时我们其实需要 offset，但 node_ptr 刚拿出来，先计算一次 offset
        // SAFETY: node_ptr 必定在 Arena 内
        let curr_offset = unsafe { self.calculator.offset_of(node_ptr.cast::<u8>()) };

        // 迭代分裂直到达到所需大小
        while curr_order > order {
            curr_order -= 1;
            let block_size = self.calculator.block_size(curr_order);

            // Buddy 是高地址的那一半
            // SAFETY: 向下分裂时，block_size 必定在当前块范围内
            let buddy_offset = curr_offset + block_size;
            let buddy_ptr = unsafe { self.calculator.ptr_at(buddy_offset) };

            // 将 Buddy 初始化为 FreeNode 并加入对应的空闲链表
            // SAFETY: buddy_ptr 指向有效的未使用内存
            unsafe {
                self.free_lists[curr_order].push(buddy_ptr.cast::<FreeNode>());
                self.free_bitmap |= 1 << curr_order;
            };

            // 更新 Buddy 的 Tag
            let buddy_idx = self.calculator.tag_index(buddy_offset);
            // SAFETY: buddy_idx is calculated from a valid offset within the arena,
            // so it's guaranteed to be in bounds.
            unsafe {
                *self.tags.get_unchecked_mut(buddy_idx) = curr_order as u8;
            }
        }

        // 标记分配出的块
        let idx = self.calculator.tag_index(curr_offset);
        // SAFETY: idx is calculated from the current offset, which is always valid.
        unsafe {
            *self.tags.get_unchecked_mut(idx) = (order as u8) | TAG_ALLOCATED;
        }

        // SAFETY: curr_offset 始终有效
        Some(unsafe { self.calculator.ptr_at(curr_offset) })
    }

    /// 释放内存块
    unsafe fn dealloc(&mut self, ptr: NonNull<u8>, order: usize) {
        // SAFETY: 假定 ptr 是由 alloc 返回的，在 Arena 范围内
        let offset = unsafe { self.calculator.offset_of(ptr) };

        let mut curr_offset = offset;
        let mut curr_order = order;

        // 立即标记为空闲
        let idx = self.calculator.tag_index(curr_offset);
        // SAFETY: The offset is from a pointer previously allocated by this allocator,
        // so the index is guaranteed to be in bounds.
        unsafe {
            *self.tags.get_unchecked_mut(idx) = curr_order as u8;
        }

        // 尝试向上合并
        while curr_order < NUM_ORDERS - 1 {
            let buddy_offset = self.calculator.buddy_offset(curr_offset, curr_order);

            if buddy_offset >= ARENA_SIZE.get() {
                break;
            }

            let buddy_idx = self.calculator.tag_index(buddy_offset);
            // SAFETY: buddy_offset is checked to be within ARENA_SIZE,
            // so buddy_idx is in bounds.
            let buddy_tag = unsafe { *self.tags.get_unchecked(buddy_idx) };

            // 检查 Buddy 是否空闲且 Order 一致
            if (buddy_tag & TAG_ALLOCATED) == 0 && (buddy_tag & TAG_ORDER_MASK) == curr_order as u8
            {
                // 合并 Buddy
                // SAFETY: buddy_offset 经过检查在 Arena 范围内
                let buddy_ptr = unsafe { self.calculator.ptr_at(buddy_offset) };
                let buddy_node_ptr = buddy_ptr.cast::<FreeNode>();

                // 从空闲链表中移除 Buddy
                // SAFETY: buddy 是空闲块，必定在链表中
                unsafe {
                    self.free_lists[curr_order].remove(buddy_node_ptr);
                    if self.free_lists[curr_order].is_empty() {
                        self.free_bitmap &= !(1 << curr_order);
                    }
                };

                // 更新为合并后的大块
                curr_offset = std::cmp::min(curr_offset, buddy_offset);
                curr_order += 1;

                // 更新新块的 Tag
                let new_idx = self.calculator.tag_index(curr_offset);
                // SAFETY: curr_offset is the minimum of two valid offsets,
                // so it's also a valid offset.
                unsafe {
                    *self.tags.get_unchecked_mut(new_idx) = curr_order as u8;
                }
            } else {
                break;
            }
        }

        // 将最终的空闲块加入链表
        // SAFETY: curr_offset 始终在 Arena 范围内
        let final_ptr = unsafe { self.calculator.ptr_at(curr_offset) };
        let final_node_ptr = final_ptr.cast::<FreeNode>();

        // SAFETY: final_ptr 有效
        unsafe {
            self.free_lists[curr_order].push(final_node_ptr);
            self.free_bitmap |= 1 << curr_order;
        };
    }
}

/// 包含缓存层 (Slab) 的分配器封装
struct BuddyAllocator {
    raw: RawBuddyAllocator,

    // Slab 缓存：存储常用 Order 的空闲块 (Order 0..=MAX_SLAB_ORDER)
    slabs: [Vec<NonNull<u8>>; MAX_SLAB_ORDER + 1],
}

impl BuddyAllocator {
    fn new(memory: ThreadMemory) -> Result<Self, AllocError> {
        Ok(Self {
            raw: RawBuddyAllocator::new(memory)?,
            slabs: Default::default(),
        })
    }

    fn calculate_order(size: usize) -> Option<usize> {
        if size > ARENA_SIZE.get() {
            return None;
        }
        if size <= MIN_BLOCK_SIZE {
            return Some(0);
        }
        // MIN_BLOCK_SIZE is 4096 (2^12)
        let order = size.next_power_of_two().ilog2() as usize - 12;
        if order >= NUM_ORDERS {
            None
        } else {
            Some(order)
        }
    }

    /// 分配指定大小的内存块
    fn alloc(&mut self, size: usize) -> Option<(NonNull<u8>, usize)> {
        let needed_order = Self::calculate_order(size)?;

        // 1. 尝试从 Slab 分配
        if needed_order <= MAX_SLAB_ORDER {
            if let Some(ptr) = self.slabs[needed_order].pop() {
                return Some((ptr, needed_order));
            }
        }

        // 2. Buddy 分配逻辑
        let ptr = self.raw.alloc(needed_order)?;
        Some((ptr, needed_order))
    }

    /// 释放内存块
    unsafe fn dealloc(&mut self, ptr: NonNull<u8>, order: usize) {
        // 1. 尝试放入 Slab 缓存
        if order <= MAX_SLAB_ORDER {
            if self.slabs[order].len() < SLAB_CAPACITIES[order] {
                self.slabs[order].push(ptr);
                return;
            }
        }

        // 2. 委托给 RawBuddyAllocator
        // SAFETY: Caller ensures ptr is valid for deallocation
        unsafe { self.raw.dealloc(ptr, order) };
    }

    #[cfg(test)]
    fn count_free(&self, order: usize) -> usize {
        let mut count = 0;
        let mut curr = self.raw.free_lists[order].head;
        unsafe {
            while let Some(node) = curr {
                count += 1;
                curr = node.as_ref().next;
            }
        }
        count
    }

    #[cfg(test)]
    fn count_slab(&self, order: usize) -> usize {
        if order <= MAX_SLAB_ORDER {
            self.slabs[order].len()
        } else {
            0
        }
    }

    fn global_region(&self) -> (NonNull<u8>, usize) {
        self.raw.global_region()
    }
}

/// Shared state for thread-safe access and deferred deallocation
struct SharedBuddyState {
    allocator: UnsafeCell<BuddyAllocator>,
    event_queue: SegQueue<DeallocParams>,
    owner_id: thread::ThreadId,
    // Using UnsafeCell for ref_count since it's only modified by the owner thread.
    // Cross-thread ops are queued.
    ref_count: UnsafeCell<usize>,
}

/// 各种 BufferPool 实现的包装器
pub struct BuddyPool {
    inner: NonNull<SharedBuddyState>,
}

// Manual Clone for ref-counting
impl Clone for BuddyPool {
    fn clone(&self) -> Self {
        // Since BuddyPool is !Send, we are guaranteed to be on the owner thread.
        unsafe {
            let inner = self.inner.as_ref();
            // We are on owner thread, safe to mutate ref_count
            *inner.ref_count.get() += 1;
        }
        Self { inner: self.inner }
    }
}

// Manual Drop for ref-counting
impl Drop for BuddyPool {
    fn drop(&mut self) {
        unsafe {
            let inner = self.inner.as_ref();
            // Since BuddyPool is !Send, we are guaranteed to be on the owner thread.
            let count = inner.ref_count.get();
            *count -= 1;
            if *count == 0 {
                // Last reference, cleanup
                let _ = Box::from_raw(self.inner.as_ptr());
            }
        }
    }
}

// BuddyPool is !Send and !Sync implies it cannot be moved to other threads.
// This allows us to remove runtime thread-ID checks for handle usage.

#[derive(Clone, Copy, Debug)]
pub struct BuddySpec<const SIZE: usize = 33554432>; // 32MB default

impl<const SIZE: usize> Default for BuddySpec<SIZE> {
    fn default() -> Self {
        Self
    }
}

impl<const SIZE: usize> PoolSpec for BuddySpec<SIZE> {
    fn memory_requirement(&self) -> NonZeroUsize {
        NonZeroUsize::new(SIZE).expect("Memory size must be > 0")
    }

    fn build(
        self,
        memory: crate::ThreadMemory,
        registrar: Box<dyn crate::buffer::BufferRegistrar>,
        global_info: crate::global::GlobalMemoryInfo,
    ) -> AnyBufPool {
        let pool = BuddyPool::new(memory).expect("Failed to create BuddyPool");
        let reg_pool =
            RegisteredPool::new(pool, registrar, global_info).expect("Failed to register pool");
        AnyBufPool::new(reg_pool)
    }
}

impl std::fmt::Debug for BuddyPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BuddyPool").finish_non_exhaustive()
    }
}

// Static VTable for Type Erasure
static BUDDY_POOL_VTABLE: PoolVTable = PoolVTable {
    dealloc: buddy_dealloc_shim,
    resolve_region_info: buddy_resolve_region_info_shim,
};

unsafe fn buddy_dealloc_shim(pool_data: NonNull<()>, params: DeallocParams) {
    // 1. Recover the Pool Pointer
    let ptr = pool_data.as_ptr() as *mut SharedBuddyState;
    // SAFETY: We hold a refcount (logically), so ptr is valid.
    let state = unsafe { &*ptr };

    let is_owner = thread::current().id() == state.owner_id;

    // 2. Dealloc logic
    if is_owner {
        // Local dealloc
        // SAFETY: We checked ownership.
        let allocator = unsafe { &mut *state.allocator.get() };
        // SAFETY: ptr is valid and allocated from this pool.
        unsafe { allocator.dealloc(params.ptr, params.context) };

        // Decrement RefCount for the returned buffer
        unsafe {
            let ref_count = &mut *state.ref_count.get();
            *ref_count -= 1;
            if *ref_count == 0 {
                let _ = Box::from_raw(ptr);
            }
        }
    } else {
        // Remote dealloc: push to queue
        state.event_queue.push(params);
    }
}

unsafe fn buddy_resolve_region_info_shim(pool_data: NonNull<()>, buf: &FixedBuf) -> (usize, usize) {
    let raw = pool_data.as_ptr() as *const SharedBuddyState;
    // We don't touch refcount here, just access data
    // SAFETY: pool_data implies a valid ref.
    let inner = unsafe { &*raw };
    // SAFETY: We are only reading global region, which is constant/safe.
    // RawBuddyAllocator::global_region returns values from ThreadMemory, which is Sync.
    // It's safe to read allocator specific fields that are constant (global region).
    let allocator = unsafe { &*inner.allocator.get() };
    let (global_base, _) = allocator.global_region();
    (
        0,
        (buf.as_ptr() as usize).saturating_sub(global_base.as_ptr() as usize),
    )
}

impl BuddyPool {
    pub fn new(memory: ThreadMemory) -> Result<Self, AllocError> {
        let state = SharedBuddyState {
            allocator: UnsafeCell::new(BuddyAllocator::new(memory)?),
            event_queue: SegQueue::new(),
            owner_id: thread::current().id(),
            ref_count: UnsafeCell::new(1),
        };

        let ptr = Box::into_raw(Box::new(state));
        Ok(Self {
            inner: unsafe { NonNull::new_unchecked(ptr) },
        })
    }
}

impl BackingPool for BuddyPool {
    fn alloc_mem(&self, size: NonZeroUsize) -> AllocResult {
        // Enforce thread locality for allocation
        let inner = unsafe { self.inner.as_ref() };
        // No need to check thread ID, !Send guarantees we are on owner thread.

        let allocator = unsafe { &mut *inner.allocator.get() };
        let ref_count = unsafe { &mut *inner.ref_count.get() };

        // Drain return queue
        while let Some(params) = inner.event_queue.pop() {
            unsafe { allocator.dealloc(params.ptr, params.context) };
            *ref_count -= 1;
        }

        // Check if potentially refs are 0 after processing queue?
        // Impossible because `self` is a live reference (ref_count >= 1).

        match allocator.alloc(size.get()) {
            Some((block_ptr, order)) => {
                let capacity = MIN_BLOCK_SIZE << order;
                // Increment ref_count for the new FixedBuf
                *ref_count += 1;

                AllocResult::Allocated {
                    ptr: block_ptr,
                    cap: unsafe { NonZeroUsize::new_unchecked(capacity) },
                    // BackingPool doesn't know about registration
                    global_index: None,
                    context: order,
                }
            }
            None => AllocResult::Failed,
        }
    }

    fn vtable(&self) -> &'static PoolVTable {
        &BUDDY_POOL_VTABLE
    }

    fn pool_data(&self) -> NonNull<()> {
        let ptr = self.inner.as_ptr();
        unsafe { NonNull::new_unchecked(ptr as *mut ()) }
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_alloc_basic() {
        use crate::global::{GlobalAllocator, GlobalAllocatorConfig};

        // Create a real ThreadMemory for testing
        let multiplier_val = ARENA_SIZE.get() / crate::MIN_THREAD_MEMORY.get();
        let multiplier =
            crate::ThreadMemoryMultiplier(unsafe { NonZeroUsize::new_unchecked(multiplier_val) });
        let config = GlobalAllocatorConfig {
            multipliers: vec![multiplier],
        };
        let mut memories = GlobalAllocator::new(config).unwrap().0;
        let memory = memories.pop().unwrap();

        let mut allocator = BuddyAllocator::new(memory).unwrap();
        // 初始状态：1个 MaxOrder 块 (Order 13)
        assert_eq!(allocator.count_free(NUM_ORDERS - 1), 1);

        // 分配 4KB (Order 0)
        let (ptr1, order1) = allocator.alloc(4096).unwrap();
        assert_eq!(order1, 0);

        // 分裂路径验证
        // MaxOrder -> ... -> 8K -> 4K(Allocated) + 4K(Free)
        // 所有的中间级 (4K ... MaxOrder) 都应该各有一个 Free 块
        assert_eq!(allocator.count_free(0), 1); // 剩下一个 4K
        assert_eq!(allocator.count_free(1), 1); // 剩下一个 8K
        assert_eq!(allocator.count_free(NUM_ORDERS - 1), 0); // MaxOrder 没了

        // 释放后应完全合并 (Slab 为空，因为 Order 0 被合并了? 不，dealloc 会先放 Slab)
        // 第一次释放 4KB -> 放入 Slab
        unsafe { allocator.dealloc(ptr1, order1) };
        assert_eq!(allocator.count_slab(0), 1);
        assert_eq!(allocator.count_free(0), 1); // 之前 alloc 留下的另一个 4K 还在 free list

        // 只有 Slab 满了或者手动清空，才会合并。
        // 这里手动从 Slab 取出并走正常的 dealloc 流程比较难模拟，除非我们在测试里不走 dealloc
        // 但我们可以测试 Slab 复用
        let (ptr2, order2) = allocator.alloc(4096).unwrap();
        assert_eq!(order2, 0);
        assert_eq!(ptr2, ptr1); // 应该复用 Slab 里的
        assert_eq!(allocator.count_slab(0), 0);
    }
}
