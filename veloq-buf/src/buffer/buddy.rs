use super::AllocError;
use crate::{ThreadMemory, nz};
use std::mem::ManuallyDrop;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::ptr::NonNull;
use veloq_intrusive_linklist::{Link, LinkedList, intrusive_adapter};

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
const TAG_SLAB: u8 = 0x40;
const TAG_ORDER_MASK: u8 = 0x3F;

/// 侵入式双向链表节点，存储在空闲块的头部
#[repr(C)]
struct FreeNode {
    link: Link,
}

intrusive_adapter!(FreeNodeAdapter = FreeNode { link: Link });

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
    free_lists: ManuallyDrop<[LinkedList<FreeNodeAdapter>; NUM_ORDERS]>,

    // 位图索引，加速空闲块查找
    free_bitmap: u16,

    // 块标签数组，索引为 block_offset / 4096
    // 记录块的 Order 和是否已分配状态
    tags: Vec<u8>,
}

impl RawBuddyAllocator {
    fn new(mut memory: ThreadMemory) -> Result<Self, AllocError> {
        // Check memory size
        if memory.len() < ARENA_SIZE.get() {
            // For now fail if provided memory is less than ARENA_SIZE
            // In future we could adjust ARENA_SIZE dynamically
            return Err(AllocError::Oom);
        }

        // Bug Fix: Use as_mut_ptr to ensure correct provenance for mutable access
        let base_ptr = memory.as_mut_ptr();
        let calculator = BlockCalculator::new(base_ptr);

        let mut free_lists = std::array::from_fn(|_| LinkedList::new(FreeNodeAdapter));

        // 初始化最大的块 (Order 12, 16MB) -> Wait, order 13 is 32MB. 2^12 * 4096 = 16M?
        // MIN_BLOCK = 4096 (2^12.
        // MAX Order = 13 (NUM_ORDERS - 1). Size = 4096 << 13 = 32MB.
        let max_order = NUM_ORDERS - 1;

        // SAFETY: 刚刚分配的内存，指针有效且大小足够
        unsafe {
            let root_node = &mut *(base_ptr as *mut FreeNode);
            root_node.link = Link::new();

            free_lists[max_order].push_front(Pin::new_unchecked(root_node));
        }
        let free_bitmap = 1 << max_order;

        let leaf_count = ARENA_SIZE.get() / MIN_BLOCK_SIZE;
        let mut tags = vec![0u8; leaf_count];
        // 标记第一个最大块为空闲
        tags[0] = max_order as u8;

        Ok(Self {
            _memory_owner: memory,
            calculator,
            free_lists: ManuallyDrop::new(free_lists),
            free_bitmap,
            tags,
        })
    }

    fn global_region(&self) -> (NonNull<u8>, usize) {
        self._memory_owner.global_region()
    }

    // --- Helper Methods to Encapsulate Tag Logic ---

    #[inline]
    unsafe fn mark_allocated(&mut self, offset: usize, order: usize) {
        let idx = self.calculator.tag_index(offset);
        // SAFETY: idx checked
        unsafe {
            *self.tags.get_unchecked_mut(idx) = (order as u8) | TAG_ALLOCATED;
        }
    }

    #[inline]
    unsafe fn mark_unallocated(&mut self, offset: usize, order: usize) {
        let idx = self.calculator.tag_index(offset);
        // SAFETY: idx checked
        unsafe {
            *self.tags.get_unchecked_mut(idx) = order as u8;
        }
    }

    /// Mark block as currently in Slab cache
    /// SAFETY: Caller ensures ptr is valid
    unsafe fn mark_slab(&mut self, ptr: NonNull<u8>) {
        let offset = unsafe { self.calculator.offset_of(ptr) };
        let idx = self.calculator.tag_index(offset);
        unsafe {
            let tag = self.tags.get_unchecked_mut(idx);
            // Must be allocated to be in slab
            if (*tag & TAG_ALLOCATED) == 0 {
                panic!(
                    "Logic Error: Attempt to put free block into Slab: ptr={:?}",
                    ptr
                );
            }
            if (*tag & TAG_SLAB) != 0 {
                panic!("Double free detected (Slab Re-entry): ptr={:?}", ptr);
            }
            *tag |= TAG_SLAB;
        }
    }

    /// Unmark block from Slab cache (retrieve for use)
    /// SAFETY: Caller ensures ptr is valid
    unsafe fn unmark_slab(&mut self, ptr: NonNull<u8>) {
        let offset = unsafe { self.calculator.offset_of(ptr) };
        let idx = self.calculator.tag_index(offset);
        unsafe {
            let tag = self.tags.get_unchecked_mut(idx);
            // Must be allocated
            debug_assert!(
                (*tag & TAG_ALLOCATED) != 0,
                "Slab block must be marked allocated"
            );
            debug_assert!((*tag & TAG_SLAB) != 0, "Slab block must be marked as Slab");

            *tag &= !TAG_SLAB;
        }
    }

    /// Check for double free before Raw Dealloc
    unsafe fn check_double_free(&self, offset: usize, order: usize) {
        let idx = self.calculator.tag_index(offset);
        let tag = unsafe { *self.tags.get_unchecked(idx) };

        if (tag & TAG_ALLOCATED) == 0 {
            panic!(
                "Double free detected (Raw): offset={}, order={}",
                offset, order
            );
        }
        // If it's in slab, it shouldn't be deallocated via raw unless removed from slab first
        // But dealloc usually implies it's NOT in slab anymore or never was (if we skipped slab).
        // Actually, if it has TAG_SLAB, it means it IS in the slab list. If we try to raw-dealloc it, that's wrong.
        if (tag & TAG_SLAB) != 0 {
            panic!(
                "Double free detected (In Slab): offset={}, order={}",
                offset, order
            );
        }
        if (tag & TAG_ORDER_MASK) != order as u8 {
            panic!(
                "Dealloc order mismatch: offset={}, expected={}, actual={}",
                offset,
                tag & TAG_ORDER_MASK,
                order
            );
        }
    }

    // --- End Helper Methods ---

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
        let node_ref = unsafe {
            (*self.free_lists)[found_order]
                .pop_front()
                .unwrap_unchecked()
        };
        // Get raw pointer before we potentially invalidate the reference by splitting etc (though we own it now)
        let node_ptr = unsafe { NonNull::from(node_ref.get_unchecked_mut()) };

        if (*self.free_lists)[found_order].is_empty() {
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
                let buddy_node = &mut *(buddy_ptr.as_ptr() as *mut FreeNode);
                buddy_node.link = Link::new();
                (*self.free_lists)[curr_order].push_front(Pin::new_unchecked(buddy_node));
                self.free_bitmap |= 1 << curr_order;
            };

            // 更新 Buddy 的 Tag (Clean State, implies unallocated)
            unsafe {
                self.mark_unallocated(buddy_offset, curr_order);
            }
        }

        // 标记分配出的块
        unsafe {
            self.mark_allocated(curr_offset, order);
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

        // Double-Free Check
        unsafe { self.check_double_free(curr_offset, curr_order) };

        // 立即标记为空闲
        unsafe { self.mark_unallocated(curr_offset, curr_order) };

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
            // 注意: 被合并的 Buddy 必须不是 Allocated 也不是 internal Slab 状态(虽然 unallocated 肯定不是 slab)
            // 只要 (buddy_tag & TAG_ALLOCATED) == 0，它就是真正的 Raw Free
            if (buddy_tag & TAG_ALLOCATED) == 0 && (buddy_tag & TAG_ORDER_MASK) == curr_order as u8
            {
                // 合并 Buddy
                // SAFETY: buddy_offset 经过检查在 Arena 范围内
                let buddy_ptr = unsafe { self.calculator.ptr_at(buddy_offset) };
                let buddy_node_ptr = buddy_ptr.cast::<FreeNode>();

                // 从空闲链表中移除 Buddy
                // SAFETY: buddy 是空闲块，必定在链表中
                unsafe {
                    let mut cursor =
                        (*self.free_lists)[curr_order].cursor_mut_from_ptr(buddy_node_ptr);
                    cursor.remove();

                    if (*self.free_lists)[curr_order].is_empty() {
                        self.free_bitmap &= !(1 << curr_order);
                    }
                };

                // 更新为合并后的大块
                curr_offset = std::cmp::min(curr_offset, buddy_offset);
                curr_order += 1;

                // 更新新块的 Tag (Upper level calls might reset this later, but keep consistent)
                unsafe { self.mark_unallocated(curr_offset, curr_order) };
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
            let final_node = &mut *final_node_ptr.as_ptr();
            final_node.link = Link::new();

            (*self.free_lists)[curr_order].push_front(Pin::new_unchecked(final_node));
            self.free_bitmap |= 1 << curr_order;
        };
    }
}

impl Drop for RawBuddyAllocator {
    fn drop(&mut self) {
        // SAFETY: 必须显式 drop free_lists，确保在 _memory_owner 析构前清理链表。
        // 因为链表节点存储在 _memory_owner 管理的内存中。
        unsafe {
            ManuallyDrop::drop(&mut self.free_lists);
        }
    }
}

/// 包含缓存层 (Slab) 的分配器封装
pub struct BuddyAllocator {
    raw: RawBuddyAllocator,

    // Slab 缓存：存储常用 Order 的空闲块 (Order 0..=MAX_SLAB_ORDER)
    slabs: [Vec<NonNull<u8>>; MAX_SLAB_ORDER + 1],
}

// SAFETY: BuddyAllocator 管理自己的内存，指针指向的是它拥有的内存区域。
// 整个结构体可以安全地跨线程传递（虽然不应该同时从多个线程访问）。
unsafe impl Send for BuddyAllocator {}

impl BuddyAllocator {
    pub fn new(memory: ThreadMemory) -> Result<Self, AllocError> {
        // Pre-allocate slab vectors to avoid reallocation jitter
        let slabs = std::array::from_fn(|i| Vec::with_capacity(SLAB_CAPACITIES[i]));

        Ok(Self {
            raw: RawBuddyAllocator::new(memory)?,
            slabs,
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
                // Update Tag: Clear SLAB bit
                unsafe { self.raw.unmark_slab(ptr) };
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
                // Mark as in SLAB & Check Double Free
                unsafe { self.raw.mark_slab(ptr) };

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
        (*self.raw.free_lists)[order].len()
    }

    #[cfg(test)]
    fn count_slab(&self, order: usize) -> usize {
        if order <= MAX_SLAB_ORDER {
            self.slabs[order].len()
        } else {
            0
        }
    }
}

// 实现 RawAllocator trait for BuddyAllocator
impl crate::block::RawAllocator for BuddyAllocator {
    fn alloc(&mut self, size: usize) -> Option<crate::block::RawAllocResult> {
        let (ptr, order) = self.alloc(size)?;
        let capacity = MIN_BLOCK_SIZE << order;
        Some(crate::block::RawAllocResult {
            ptr,
            cap: unsafe { NonZeroUsize::new_unchecked(capacity) },
            context: order,
            is_registered: true,
        })
    }

    unsafe fn dealloc(&mut self, ptr: NonNull<u8>, _cap: usize, context: usize) {
        // context is the order
        unsafe {
            self.dealloc(ptr, context);
        }
    }

    fn global_region(&self) -> (NonNull<u8>, usize) {
        self.raw.global_region()
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_alloc_basic() {
        // Create a real ThreadMemory for testing
        let size = ARENA_SIZE;
        let memory = crate::ThreadMemory::new_standalone(size).unwrap();

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
