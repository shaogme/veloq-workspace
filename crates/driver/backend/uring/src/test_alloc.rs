//! Test-only allocation counter for operation fast-path baselines.

use core::{
    alloc::{GlobalAlloc, Layout},
    cell::Cell,
    cmp::min,
    mem::align_of,
    ptr::{copy_nonoverlapping, null_mut, write_bytes},
    sync::atomic::{AtomicUsize, Ordering},
};

use veloq_std::thread::Tls;

static ALLOCATIONS: AtomicUsize = AtomicUsize::new(0);
static ZEROED_ALLOCATIONS: AtomicUsize = AtomicUsize::new(0);
static REALLOCATIONS: AtomicUsize = AtomicUsize::new(0);
static DEALLOCATIONS: AtomicUsize = AtomicUsize::new(0);
static TRACKING: Tls<Cell<bool>> = Tls::new();

struct CountingAllocator;

#[global_allocator]
static GLOBAL_ALLOCATOR: CountingAllocator = CountingAllocator;

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = allocate(&layout);
        record(&ALLOCATIONS, ptr);
        ptr
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let ptr = allocate(&layout);
        if !ptr.is_null() {
            unsafe { write_bytes(ptr, 0, layout.size().max(1)) };
        }
        record(&ZEROED_ALLOCATIONS, ptr);
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, _layout: Layout) {
        if tracking_on_current_thread() {
            DEALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        }
        unsafe { libc::free(ptr.cast()) };
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let new_ptr = allocate(
            &Layout::from_size_align(new_size, layout.align())
                .expect("reallocation layout should be valid"),
        );
        if !new_ptr.is_null() {
            unsafe {
                copy_nonoverlapping(ptr, new_ptr, min(layout.size(), new_size));
                libc::free(ptr.cast());
            }
        }
        record(&REALLOCATIONS, new_ptr);
        new_ptr
    }
}

fn allocate(layout: &Layout) -> *mut u8 {
    let size = layout.size().max(1);
    let alignment = layout.align().max(align_of::<*mut u8>());
    let mut ptr = null_mut();
    let result = unsafe { libc::posix_memalign(&mut ptr, alignment, size) };
    if result == 0 { ptr.cast() } else { null_mut() }
}

fn tracking_on_current_thread() -> bool {
    TRACKING.try_with(Cell::get).unwrap_or(false)
}

fn record(counter: &AtomicUsize, ptr: *mut u8) {
    if !ptr.is_null() && tracking_on_current_thread() {
        counter.fetch_add(1, Ordering::Relaxed);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct AllocationCounts {
    pub(crate) allocations: usize,
    pub(crate) zeroed_allocations: usize,
    pub(crate) reallocations: usize,
    pub(crate) deallocations: usize,
}

impl AllocationCounts {
    #[inline]
    pub(crate) const fn dynamic_allocations(self) -> usize {
        self.allocations + self.zeroed_allocations + self.reallocations
    }
}

struct TrackingGuard;

impl Drop for TrackingGuard {
    fn drop(&mut self) {
        TRACKING.with_or_default(|tracking| tracking.set(false));
    }
}

#[inline]
pub(crate) fn measure<F, R>(operation: F) -> (R, AllocationCounts)
where
    F: FnOnce() -> R,
{
    let already_tracking = TRACKING.with_or_default(Cell::get);
    assert!(
        !already_tracking,
        "allocation measurements cannot be nested"
    );
    TRACKING.with_or_default(|tracking| tracking.set(true));
    ALLOCATIONS.store(0, Ordering::Relaxed);
    ZEROED_ALLOCATIONS.store(0, Ordering::Relaxed);
    REALLOCATIONS.store(0, Ordering::Relaxed);
    DEALLOCATIONS.store(0, Ordering::Relaxed);
    let guard = TrackingGuard;

    let result = operation();
    let counts = AllocationCounts {
        allocations: ALLOCATIONS.load(Ordering::Relaxed),
        zeroed_allocations: ZEROED_ALLOCATIONS.load(Ordering::Relaxed),
        reallocations: REALLOCATIONS.load(Ordering::Relaxed),
        deallocations: DEALLOCATIONS.load(Ordering::Relaxed),
    };
    drop(guard);
    (result, counts)
}
