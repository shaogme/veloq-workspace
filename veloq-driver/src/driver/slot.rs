use crossbeam_utils::CachePadded;
use std::cell::UnsafeCell;
use std::mem::ManuallyDrop;
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};

#[cfg(windows)]
use windows_sys::Win32::System::IO::OVERLAPPED;

/// Manual payload container: raw pointer + static kind + drop fn.
pub struct ErasedPayload {
    pub(crate) ptr: *mut (),
    pub(crate) kind: u16,
    pub(crate) drop_fn: unsafe fn(*mut ()),
}

unsafe impl Send for ErasedPayload {}

impl ErasedPayload {
    #[inline]
    pub(crate) fn leak_ptr(self) -> *mut () {
        let this = ManuallyDrop::new(self);
        this.ptr
    }
}

impl Drop for ErasedPayload {
    fn drop(&mut self) {
        if !self.ptr.is_null() {
            unsafe { (self.drop_fn)(self.ptr) };
            self.ptr = std::ptr::null_mut();
        }
    }
}

#[repr(C)]
#[cfg(windows)]
pub(crate) struct OverlappedEntry {
    pub(crate) inner: OVERLAPPED,
    pub(crate) user_data: usize,
    pub(crate) generation: u32,
    pub(crate) blocking_result: Option<std::io::Result<usize>>,
}

#[cfg(windows)]
impl Default for OverlappedEntry {
    fn default() -> Self {
        Self {
            inner: unsafe { std::mem::zeroed() },
            user_data: 0,
            generation: 0,
            blocking_result: None,
        }
    }
}

#[derive(Debug)]
#[cfg_attr(windows, repr(C))]
pub(crate) struct Slot<Op> {
    // Basic metadata
    #[cfg(windows)]
    index: usize, // Self-reference index
    pub(crate) generation: AtomicU32, // Generation to prevent ABA

    // Intrusive free list pointer (replaces remote_free_queue)
    pub(crate) next_free: AtomicUsize,

    // Resource storage
    // - SUBMITTED: Driver reads Op pointer to pass to kernel
    // - COMPLETED: Future takes Op
    pub(crate) op: UnsafeCell<Option<Op>>,
    /// Detailed completion result for cases not representable by raw errno/event res.
    pub(crate) result: UnsafeCell<Option<std::io::Result<usize>>>,
    /// Type-erased user payload owned by the slot while op is in-flight.
    pub(crate) payload: UnsafeCell<Option<ErasedPayload>>,

    // Windows IOCP specific field (Embedded Overlapped)
    // Enabled only on Windows for pointer reconstruction (Container_of pattern)
    // Only enabled on Windows, used for pointer back-tracing (Container_of pattern)
    #[cfg(windows)]
    pub(crate) overlapped: UnsafeCell<OverlappedEntry>,
}

// Slot must be Sync as it is referenced by multiple threads
// Safety relies on atomic state transitions
unsafe impl<Op: Send> Sync for Slot<Op> {}

impl<Op> Slot<Op> {
    // Should be consistent with SlotTable::NULL_INDEX, but we can't easily reference it here without generic.
    // We use usize::MAX as sentinel.
    const NULL_INDEX: usize = usize::MAX;

    pub(crate) fn new(#[cfg(windows)] index: usize) -> Self {
        Self {
            #[cfg(windows)]
            index,
            generation: AtomicU32::new(0),
            next_free: AtomicUsize::new(Self::NULL_INDEX),
            op: UnsafeCell::new(None),
            result: UnsafeCell::new(None),
            payload: UnsafeCell::new(None),
            #[cfg(windows)]
            overlapped: UnsafeCell::new(OverlappedEntry {
                inner: unsafe { std::mem::zeroed() },
                user_data: index,
                generation: 0,
                blocking_result: None,
            }),
        }
    }

    pub(crate) fn reset(&self, generation: u32) {
        unsafe {
            // Ensure stale resources from previous generation are dropped before reuse.
            *self.op.get() = None;
            *self.result.get() = None;
            *self.payload.get() = None;
        }
        self.generation.store(generation, Ordering::Release);
        #[cfg(windows)]
        unsafe {
            let entry = OverlappedEntry {
                user_data: self.index,
                generation,
                ..Default::default()
            };
            *self.overlapped.get() = entry;
        }
    }

    #[cfg(windows)]
    pub(crate) fn overlapped_ptr(&self) -> *mut OVERLAPPED {
        unsafe { &mut (*self.overlapped.get()).inner }
    }
}

// Note: Use CachePadded<Slot> in Slab/Table to avoid false sharing
pub(crate) type SlotEntry<Op> = CachePadded<Slot<Op>>;

pub struct SlotTable<Op> {
    pub(crate) slots: Box<[SlotEntry<Op>]>,
    // Intrusive Treiber stack head
    pub(crate) remote_free_head: AtomicUsize,
}

unsafe impl<Op: Send> Sync for SlotTable<Op> {}
unsafe impl<Op: Send> Send for SlotTable<Op> {}

impl<Op> SlotTable<Op> {
    pub(crate) const NULL_INDEX: usize = usize::MAX;

    pub(crate) fn new(capacity: usize) -> Self {
        let mut slots = Vec::with_capacity(capacity);
        for _i in 0..capacity {
            slots.push(CachePadded::new(Slot::new(
                #[cfg(windows)]
                _i,
            )));
        }
        Self {
            slots: slots.into_boxed_slice(),
            remote_free_head: AtomicUsize::new(Self::NULL_INDEX),
        }
    }

    /// Pushes an index onto the remote free stack.
    /// This is lock-free and can be called from multiple threads.
    pub(crate) fn push_free(&self, idx: usize) {
        let slot = &self.slots[idx];
        let mut head = self.remote_free_head.load(Ordering::Relaxed);
        loop {
            // Point the new node to the current head
            slot.next_free.store(head, Ordering::Relaxed);
            // Try to swap the head
            match self.remote_free_head.compare_exchange_weak(
                head,
                idx,
                Ordering::Release,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(current) => head = current,
            }
        }
    }

    /// Pops all items from the remote free stack.
    /// Returns the head of the linked list.
    /// This is used by the driver to bulk-reclaim slots.
    pub(crate) fn pop_all(&self) -> usize {
        self.remote_free_head
            .swap(Self::NULL_INDEX, Ordering::Acquire)
    }
}
