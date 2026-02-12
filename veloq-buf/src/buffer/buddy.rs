use crate::slot::{SLOT_SIZE, SlotIndex};
use std::fmt;
use std::mem::ManuallyDrop;
use std::pin::Pin;
use std::ptr::NonNull;
use veloq_intrusive_linklist::{Link, LinkedList};

/// Buddy System Constants
pub const MIN_ORDER: usize = 0; // 4KB
pub const MAX_ORDER: usize = 18; // 2^18 * 4KB = 1GB (Maximum single allocation supported)

// Allocator Tag Constants
const TAG_ALLOCATED: u8 = 0x80;
const TAG_ORDER_MASK: u8 = 0x3F;

/// Intrusive Doubly Linked List Node
/// Stored in the head of a free Slot.
#[repr(C)]
struct FreeNode {
    link: ManuallyDrop<Link>,
}

struct FreeNodeAdapter;

unsafe impl veloq_intrusive_linklist::Adapter for FreeNodeAdapter {
    type Value = FreeNode;

    unsafe fn get_link(&self, value: NonNull<Self::Value>) -> NonNull<Link> {
        // FreeNode is #[repr(C)] (implied for safe transmutability in this context) and link is the first field.
        // ManuallyDrop<T> has the same layout as T.
        value.cast()
    }

    unsafe fn get_value(&self, link: NonNull<Link>) -> NonNull<Self::Value> {
        link.cast()
    }
}

/// Error type for Buddy Allocator
#[derive(Debug)]
pub enum BuddyError {
    Oom,
    InvalidFree,
    DoubleFree,
}

/// Core Buddy Allocator
///
/// Designed to manage a large continuous memory implementation `GlobalSlotPool`.
/// It does NOT own the memory, but manages the state of the memory provided to it.
pub struct BuddyAllocator {
    base_ptr: NonNull<u8>,

    // Free lists for each order
    // Order 0: 4KB, Order 1: 8KB, ..., Order 18: 1GB
    free_lists: ManuallyDrop<[LinkedList<FreeNodeAdapter>; MAX_ORDER + 1]>,

    // Bitmap to quickly find non-empty orders
    // bit k is 1 if free_lists[k] is not empty
    free_bitmap: u32,

    // Tags array to track the state of each block (allocated/order)
    // One u8 per Slot.
    // Optimization: This could be 2 bits per slot (4 values: Free, Used, Split, etc), but u8 is simpler for now.
    tags: Vec<u8>,

    // Total slots managed
    total_slots: usize,
}

// SAFETY: BuddyAllocator is Send/Sync as long as we synchronize access (which Mutex in GlobalSlotPool will do)
unsafe impl Send for BuddyAllocator {}
unsafe impl Sync for BuddyAllocator {}

impl BuddyAllocator {
    /// Initialize the Buddy Allocator from a raw memory region
    ///
    /// # Safety
    /// `ptr` must point to a valid memory region of at least `len` bytes.
    /// The memory region must be alive as long as this allocator is used.
    pub unsafe fn new(ptr: NonNull<u8>, len: usize) -> Self {
        assert!(len >= SLOT_SIZE, "Memory too small");
        // Ensure alignment? Assuming ptr is 4K aligned from HugePage/Page alloc.

        let total_slots = len / SLOT_SIZE;
        let free_lists = std::array::from_fn(|_| LinkedList::new(FreeNodeAdapter));

        let mut allocator = Self {
            base_ptr: ptr,
            free_lists: ManuallyDrop::new(free_lists),
            free_bitmap: 0,
            tags: vec![0; total_slots], // Initially all 0 (Implicitly means order 0?? No, need init)
            total_slots,
        };

        // Initialize the memory as a set of maximal free blocks
        allocator.init_free_blocks(total_slots);

        allocator
    }

    #[inline(always)]
    unsafe fn ptr_from_index(&self, index: SlotIndex) -> NonNull<u8> {
        // SAFETY: Caller must ensure index is valid
        unsafe { NonNull::new_unchecked(self.base_ptr.as_ptr().add(index.offset())) }
    }

    #[inline(always)]
    unsafe fn index_from_ptr(&self, ptr: NonNull<u8>) -> SlotIndex {
        let offset = unsafe { ptr.as_ptr().offset_from(self.base_ptr.as_ptr()) as usize };
        SlotIndex::from_offset(offset)
    }

    #[inline(always)]
    fn buddy_index(&self, index: SlotIndex, order: usize) -> SlotIndex {
        // Buddy index is XORed by the size of the block (in slots)
        // Order 0: 1 slot (1 << 0). XOR 1.
        // Order k: 2^k slots. XOR (1 << k).
        SlotIndex(index.0 ^ (1 << order))
    }

    /// Initialize the free lists by breaking down the total slots into maximal power-of-two blocks
    fn init_free_blocks(&mut self, total_slots: usize) {
        let mut start_idx: usize = 0;
        let mut remaining = total_slots;

        while remaining > 0 {
            // Find the largest order that fits in `remaining` AND satisfies alignment of `start_idx`
            // A block of order K must start at index multiple of 2^K
            // size = 1 << k

            // 1. Largest power of two <= remaining
            let order_by_size = (usize::BITS - remaining.leading_zeros() - 1) as usize;

            // 2. Largest alignment of start_idx
            let order_by_align = if start_idx == 0 {
                MAX_ORDER // Aligned to anything
            } else {
                start_idx.trailing_zeros() as usize
            };

            // Constraint: Must fit in MAX_ORDER
            let order = order_by_size.min(order_by_align).min(MAX_ORDER);

            // Add this block to the free list
            unsafe {
                self.add_to_free_list(SlotIndex(start_idx), order);
            }

            let block_size = 1 << order;
            start_idx += block_size;
            remaining -= block_size;
        }
    }

    /// Helper: Add a block to the free list
    unsafe fn add_to_free_list(&mut self, index: SlotIndex, order: usize) {
        let ptr = unsafe { self.ptr_from_index(index) };
        let node = unsafe { &mut *(ptr.as_ptr() as *mut FreeNode) };
        // We use ManuallyDrop to wrap the Link. Assigning to it does NOT drop the old value
        // (which is implicitly ManuallyDrop::drop, doing nothing).
        // This safely overwrites garbage data without panicking.
        node.link = ManuallyDrop::new(Link::new());

        unsafe {
            (*self.free_lists)[order].push_front(Pin::new_unchecked(node));
        }
        self.free_bitmap |= 1 << order;

        // Mark tag as clean (Order K, Not Allocated)
        self.set_tag(index, order, false);
    }

    /// Helper: Remove a block from the free list
    unsafe fn remove_from_free_list(&mut self, index: SlotIndex, order: usize) {
        let ptr = unsafe { self.ptr_from_index(index) };
        let node_ptr = unsafe { NonNull::new_unchecked(ptr.as_ptr() as *mut FreeNode) };

        // We need a cursor to remove specific node, or just pop if we don't care which one (for alloc).
        // But for merging (dealloc), we have a specific node address (buddy).
        // `veloq_intrusive_linklist` supports cursor removal.

        unsafe {
            let mut cursor = (*self.free_lists)[order].cursor_mut_from_ptr(node_ptr);
            cursor.remove();
        }

        if (*self.free_lists)[order].is_empty() {
            self.free_bitmap &= !(1 << order);
        }
    }

    /// Set tag for a block
    /// We only set the tag for the *head* slot of the block.
    fn set_tag(&mut self, index: SlotIndex, order: usize, allocated: bool) {
        if index.0 >= self.tags.len() {
            return;
        }
        let val = (order as u8) & TAG_ORDER_MASK;
        self.tags[index.0] = if allocated { val | TAG_ALLOCATED } else { val };
    }

    fn get_tag(&self, index: SlotIndex) -> u8 {
        self.tags[index.0]
    }

    /// Allocation: Request 2^order slots
    pub fn alloc(&mut self, order: usize) -> Option<SlotIndex> {
        if order > MAX_ORDER {
            return None;
        }

        // 1. Search for best fit
        let search_mask = !((1u32 << order) - 1); // Bitmask >= order
        let candidates = self.free_bitmap & search_mask;

        if candidates == 0 {
            return None;
        }

        let found_order = candidates.trailing_zeros() as usize;

        // 2. Pop from the found list
        // FIX: Extract raw pointer immediately to drop borrow of free_lists
        let ptr = unsafe {
            let node = (*self.free_lists)[found_order]
                .pop_front()
                .unwrap_unchecked();
            // Assuming node is Pin<&mut FreeNode> or similar that allows get_unchecked_mut
            // intrusive_linklist usually returns a cursor or reference.
            // If it returns a reference, we need to convert to raw pointer.
            NonNull::new_unchecked(node.get_unchecked_mut() as *mut FreeNode as *mut u8)
        };

        if (*self.free_lists)[found_order].is_empty() {
            self.free_bitmap &= !(1 << found_order);
        }

        let found_idx = unsafe { self.index_from_ptr(ptr) };

        // 3. Split until needed order
        let mut curr_order = found_order;
        // Fix unused mut warning
        let curr_idx = found_idx;

        while curr_order > order {
            curr_order -= 1;
            let buddy_idx = SlotIndex(curr_idx.0 + (1 << curr_order));

            // Add buddy to free list
            unsafe {
                self.add_to_free_list(buddy_idx, curr_order);
            }
        }

        // 4. Mark allocated
        self.set_tag(curr_idx, order, true);

        Some(curr_idx)
    }

    /// Deallocation
    ///
    /// # Safety
    /// index must be a valid allocated block start.
    /// order must match allocation order (though we can verify with tags).
    pub unsafe fn dealloc(&mut self, index: SlotIndex, order: usize) -> Result<(), BuddyError> {
        let mut curr_idx = index;
        let mut curr_order = order;

        // Verify Tag
        let tag = self.get_tag(curr_idx);
        if (tag & TAG_ALLOCATED) == 0 {
            return Err(BuddyError::DoubleFree);
        }
        if (tag & TAG_ORDER_MASK) as usize != order {
            // This might happen if user passed wrong order, or corruption
            return Err(BuddyError::InvalidFree);
        }

        // Mark As Free immediately
        self.set_tag(curr_idx, curr_order, false);

        // Merge loop
        while curr_order < MAX_ORDER {
            let buddy_idx = self.buddy_index(curr_idx, curr_order);

            // Boundary Check
            if buddy_idx.0 >= self.total_slots {
                break;
            }

            // Check Buddy Status
            let buddy_tag = self.get_tag(buddy_idx);

            // Can merge if:
            // 1. Buddy is NOT allocated
            // 2. Buddy is same order
            if (buddy_tag & TAG_ALLOCATED) == 0
                && ((buddy_tag & TAG_ORDER_MASK) as usize == curr_order)
            {
                // Merge!

                // Remove buddy from free list
                unsafe {
                    self.remove_from_free_list(buddy_idx, curr_order);
                }

                // Move current to the lower index (alignment)
                if buddy_idx < curr_idx {
                    curr_idx = buddy_idx;
                }

                curr_order += 1;

                // Update tag for the new merged block head
                self.set_tag(curr_idx, curr_order, false);
            } else {
                break;
            }
        }

        // Add merged block to free list
        unsafe {
            self.add_to_free_list(curr_idx, curr_order);
        }

        Ok(())
    }

    /// Convert SlotIndex to Pointer
    pub fn ptr_of(&self, index: SlotIndex) -> NonNull<u8> {
        unsafe { self.ptr_from_index(index) }
    }

    /// Helper: Get allocated capacity in bytes
    pub fn capacity_of(order: usize) -> usize {
        (1 << order) * SLOT_SIZE
    }
}

impl fmt::Debug for BuddyAllocator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BuddyAllocator")
            .field("total_slots", &self.total_slots)
            .field("free_bitmap", &format_args!("{:b}", self.free_bitmap))
            .finish()
    }
}

impl Drop for BuddyAllocator {
    fn drop(&mut self) {
        unsafe {
            ManuallyDrop::drop(&mut self.free_lists);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::alloc::{Layout, alloc, dealloc};

    struct TestMemory {
        ptr: NonNull<u8>,
        layout: Layout,
    }

    impl TestMemory {
        fn new(size: usize) -> Self {
            let layout = Layout::from_size_align(size, 4096).unwrap();
            let ptr = unsafe { NonNull::new(alloc(layout)).unwrap() };
            unsafe { ptr.as_ptr().write_bytes(0, size) };
            Self { ptr, layout }
        }
    }

    impl Drop for TestMemory {
        fn drop(&mut self) {
            unsafe { dealloc(self.ptr.as_ptr(), self.layout) };
        }
    }

    #[test]
    fn test_buddy_alloc_basic() {
        // 1MB = 256 Slots
        let mem_size = 1024 * 1024;
        let memory = TestMemory::new(mem_size);

        let mut buddy = unsafe { BuddyAllocator::new(memory.ptr, mem_size) };

        // Alloc 4KB (Order 0)
        let idx1 = buddy.alloc(0).unwrap();

        // Alloc 8KB (Order 1)
        let idx2 = buddy.alloc(1).unwrap();

        // Valid Indices
        assert!(idx1.offset() < mem_size);
        assert!(idx2.offset() < mem_size);
        assert_ne!(idx1, idx2);

        // Dealloc
        unsafe {
            buddy.dealloc(idx1, 0).unwrap();
            buddy.dealloc(idx2, 1).unwrap();
        }

        // Re-alloc should succeed
        let _idx3 = buddy.alloc(MAX_ORDER).unwrap_or(SlotIndex(usize::MAX));
        // Max order for 1MB??
        // 1MB = 256 * 4096 = 2^8 * 4096.
        // So max order is 8.
        // MAX_ORDER is 18 (1GB). So alloc(18) should fail.
        let idx_fail = buddy.alloc(18);
        assert!(idx_fail.is_none());

        let idx_full = buddy.alloc(8); // 1MB
        assert!(idx_full.is_some());
    }
}
