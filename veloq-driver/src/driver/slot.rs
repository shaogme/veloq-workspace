use crossbeam_utils::CachePadded;
use std::cell::UnsafeCell;
use std::mem::ManuallyDrop;
use std::sync::atomic::{AtomicU8, AtomicU32, AtomicUsize, Ordering};

#[cfg(windows)]
use windows_sys::Win32::System::IO::OVERLAPPED;

// State definitions
pub const STATE_EMPTY: u8 = 0;
pub const STATE_SUBMITTED: u8 = 1; // Submitted to kernel (Driver owns Op)

/// Manual payload container: raw pointer + static kind + drop fn.
pub struct ErasedPayload {
    pub ptr: *mut (),
    pub kind: u16,
    pub drop_fn: unsafe fn(*mut ()),
}

unsafe impl Send for ErasedPayload {}

impl ErasedPayload {
    #[inline]
    pub fn leak_ptr(self) -> *mut () {
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
pub struct OverlappedEntry {
    pub inner: OVERLAPPED,
    pub user_data: usize,
    pub generation: u32,
    pub blocking_result: Option<std::io::Result<usize>>,
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
pub struct Slot<Op> {
    // Basic metadata
    pub index: usize,          // Self-reference index
    pub generation: AtomicU32, // Generation to prevent ABA
    pub state: AtomicU8,       // Atomic state

    // Intrusive free list pointer (replaces remote_free_queue)
    pub next_free: AtomicUsize,

    // Resource storage
    // - SUBMITTED: Driver reads Op pointer to pass to kernel
    // - COMPLETED: Future takes Op
    pub op: UnsafeCell<Option<Op>>,
    /// Detailed completion result for cases not representable by raw errno/event res.
    pub result: UnsafeCell<Option<std::io::Result<usize>>>,
    /// Type-erased user payload owned by the slot while op is in-flight.
    pub payload: UnsafeCell<Option<ErasedPayload>>,
    /// Last completed payload keyed by generation, used by detached/local futures
    /// to recover data even if the slot has already been reused.
    pub completed_payload: UnsafeCell<Option<(u32, ErasedPayload)>>,
    /// Last completed detailed result keyed by generation.
    pub completed_result: UnsafeCell<Option<(u32, std::io::Result<usize>)>>,

    // Windows IOCP specific field (Embedded Overlapped)
    // Enabled only on Windows for pointer reconstruction (Container_of pattern)
    // Only enabled on Windows, used for pointer back-tracing (Container_of pattern)
    #[cfg(windows)]
    pub overlapped: UnsafeCell<OverlappedEntry>,
}

// Slot must be Sync as it is referenced by multiple threads
// Safety relies on atomic state transitions
unsafe impl<Op: Send> Sync for Slot<Op> {}

impl<Op> Slot<Op> {
    // Should be consistent with SlotTable::NULL_INDEX, but we can't easily reference it here without generic.
    // We use usize::MAX as sentinel.
    const NULL_INDEX: usize = usize::MAX;

    pub fn new(index: usize) -> Self {
        Self {
            index,
            generation: AtomicU32::new(0),
            state: AtomicU8::new(STATE_EMPTY),
            next_free: AtomicUsize::new(Self::NULL_INDEX),
            op: UnsafeCell::new(None),
            result: UnsafeCell::new(None),
            payload: UnsafeCell::new(None),
            completed_payload: UnsafeCell::new(None),
            completed_result: UnsafeCell::new(None),
            #[cfg(windows)]
            overlapped: UnsafeCell::new(OverlappedEntry {
                inner: unsafe { std::mem::zeroed() },
                user_data: index,
                generation: 0,
                blocking_result: None,
            }),
        }
    }

    pub fn reset(&self, generation: u32) {
        unsafe {
            // Ensure stale resources from previous generation are dropped before reuse.
            *self.op.get() = None;
            *self.result.get() = None;
            *self.payload.get() = None;
            *self.completed_payload.get() = None;
            *self.completed_result.get() = None;
        }
        self.state.store(STATE_EMPTY, Ordering::Release);
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
    pub fn overlapped_ptr(&self) -> *mut OVERLAPPED {
        unsafe { &mut (*self.overlapped.get()).inner }
    }
}

// Note: Use CachePadded<Slot> in Slab/Table to avoid false sharing
pub type SlotEntry<Op> = CachePadded<Slot<Op>>;

pub struct SlotTable<Op> {
    pub slots: Box<[SlotEntry<Op>]>,
    // Intrusive Treiber stack head
    pub remote_free_head: AtomicUsize,
}

unsafe impl<Op: Send> Sync for SlotTable<Op> {}
unsafe impl<Op: Send> Send for SlotTable<Op> {}

impl<Op> SlotTable<Op> {
    pub const NULL_INDEX: usize = usize::MAX;

    pub fn new(capacity: usize) -> Self {
        let mut slots = Vec::with_capacity(capacity);
        for i in 0..capacity {
            slots.push(CachePadded::new(Slot::new(i)));
        }
        Self {
            slots: slots.into_boxed_slice(),
            remote_free_head: AtomicUsize::new(Self::NULL_INDEX),
        }
    }

    /// Pushes an index onto the remote free stack.
    /// This is lock-free and can be called from multiple threads.
    pub fn push_free(&self, idx: usize) {
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
    pub fn pop_all(&self) -> usize {
        self.remote_free_head
            .swap(Self::NULL_INDEX, Ordering::Acquire)
    }
}
