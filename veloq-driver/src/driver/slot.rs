use crossbeam_queue::SegQueue;
use crossbeam_utils::CachePadded;
use std::cell::UnsafeCell;
use std::sync::atomic::{AtomicU8, AtomicU32, Ordering};
use veloq_atomic_waker::AtomicWaker;

#[cfg(windows)]
use windows_sys::Win32::System::IO::OVERLAPPED;

// State definitions
pub const STATE_EMPTY: u8 = 0;
pub const STATE_SUBMITTED: u8 = 1; // Submitted to kernel (Driver owns Op)
pub const STATE_COMPLETED: u8 = 2; // Kernel completed, result written (Future owns Op)
pub const STATE_CONSUMED: u8 = 3; // Future consumed result, waiting for recycle
// pub const STATE_CANCELLED: u8 = 4; // Future dropped, requesting cancellation

#[repr(C)]
#[cfg(windows)]
pub struct OverlappedEntry {
    pub inner: OVERLAPPED,
    pub user_data: usize,
    pub blocking_result: Option<std::io::Result<usize>>,
}

#[cfg(windows)]
impl Default for OverlappedEntry {
    fn default() -> Self {
        Self {
            inner: unsafe { std::mem::zeroed() },
            user_data: 0,
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

    // Synchronization primitive
    pub waker: AtomicWaker, // Consumer waker

    // Result storage (UnsafeCell safe to access only in COMPLETED/CONSUMED state)
    pub result: UnsafeCell<Option<std::io::Result<usize>>>,

    // Resource storage
    // - SUBMITTED: Driver reads Op pointer to pass to kernel
    // - COMPLETED: Future takes Op
    pub op: UnsafeCell<Option<Op>>,

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
    pub fn new(index: usize) -> Self {
        Self {
            index,
            generation: AtomicU32::new(0),
            state: AtomicU8::new(STATE_EMPTY),
            waker: AtomicWaker::new(),
            result: UnsafeCell::new(None),
            op: UnsafeCell::new(None),
            #[cfg(windows)]
            overlapped: UnsafeCell::new(OverlappedEntry {
                inner: unsafe { std::mem::zeroed() },
                user_data: index,
                blocking_result: None,
            }),
        }
    }

    pub fn reset(&self, generation: u32) {
        self.state.store(STATE_EMPTY, Ordering::Release);
        self.generation.store(generation, Ordering::Release);
        // Waker resets itself on register
        // Result and Op are logically empty/invalid
        #[cfg(windows)]
        unsafe {
            *self.overlapped.get() = OverlappedEntry::default();
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
    pub remote_free_queue: SegQueue<usize>,
}

unsafe impl<Op: Send> Sync for SlotTable<Op> {}
unsafe impl<Op: Send> Send for SlotTable<Op> {}

impl<Op> SlotTable<Op> {
    pub fn new(capacity: usize) -> Self {
        let mut slots = Vec::with_capacity(capacity);
        for i in 0..capacity {
            slots.push(CachePadded::new(Slot::new(i)));
        }
        Self {
            slots: slots.into_boxed_slice(),
            remote_free_queue: SegQueue::new(),
        }
    }
}
