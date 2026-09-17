//! Submission queue access for the Linux shared ring.

use core::{
    fmt,
    sync::atomic::{self, AtomicU32, Ordering},
};

use crate::{mmap::Mmap, sys};

/// The 64-byte submission queue entry defined by the Linux ABI.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct Entry {
    pub(crate) inner: sys::IoUringSqe,
}

/// Flags carried by a submission queue entry.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct Flags(u8);

impl Flags {
    /// Use the registered file table instead of treating `fd` as a raw fd.
    pub const FIXED_FILE: Self = Self(sys::IOSQE_FIXED_FILE);
    /// Ask the kernel to select a buffer from the configured buffer group.
    pub const BUFFER_SELECT: Self = Self(sys::IOSQE_BUFFER_SELECT);

    #[inline]
    pub const fn bits(self) -> u8 {
        self.0
    }

    #[inline]
    pub const fn from_bits_retain(bits: u8) -> Self {
        Self(bits)
    }

    #[inline]
    pub const fn contains(self, other: Self) -> bool {
        self.0 & other.0 == other.0
    }

    #[inline]
    pub const fn empty() -> Self {
        Self(0)
    }

    #[inline]
    pub const fn union(self, other: Self) -> Self {
        Self(self.0 | other.0)
    }
}

impl Entry {
    #[inline]
    pub fn flags(mut self, flags: Flags) -> Self {
        self.inner.flags |= flags.bits();
        self
    }

    #[inline]
    pub fn user_data(mut self, data: u64) -> Self {
        self.set_user_data(data);
        self
    }

    #[inline]
    pub fn set_user_data(&mut self, data: u64) {
        self.inner.user_data = data;
    }

    #[inline]
    pub fn get_user_data(&self) -> u64 {
        self.inner.user_data
    }
}

pub(crate) struct Inner {
    head: *const AtomicU32,
    tail: *const AtomicU32,
    ring_mask: u32,
    ring_entries: u32,
    flags: *const AtomicU32,
    dropped: *const AtomicU32,
    array: *mut u32,
    sqes: *mut Entry,
}

impl Inner {
    /// Bind queue pointers to kernel-provided offsets in the mapped ring.
    pub(crate) unsafe fn new(sq_mmap: &Mmap, sqe_mmap: &Mmap, params: &sys::IoUringParams) -> Self {
        let head = unsafe { sq_mmap.offset(params.sq_off.head).cast() };
        let tail = unsafe { sq_mmap.offset(params.sq_off.tail).cast() };
        let ring_mask = unsafe { sq_mmap.offset(params.sq_off.ring_mask).cast::<u32>().read() };
        let ring_entries = unsafe {
            sq_mmap
                .offset(params.sq_off.ring_entries)
                .cast::<u32>()
                .read()
        };
        let flags = unsafe { sq_mmap.offset(params.sq_off.flags).cast() };
        let dropped = unsafe { sq_mmap.offset(params.sq_off.dropped).cast() };
        let array = if params.flags & sys::IORING_SETUP_NO_SQARRAY == 0 {
            unsafe { sq_mmap.offset(params.sq_off.array).cast::<u32>() }
        } else {
            core::ptr::null_mut()
        };
        let sqes = sqe_mmap.as_mut_ptr().cast();

        if !array.is_null() {
            for index in 0..ring_entries {
                unsafe { array.add(index as usize).write_volatile(index) };
            }
        }

        Self {
            head,
            tail,
            ring_mask,
            ring_entries,
            flags,
            dropped,
            array,
            sqes,
        }
    }

    #[inline]
    pub(crate) unsafe fn borrow(&self) -> SubmissionQueue<'_> {
        SubmissionQueue {
            head: unsafe { (*self.head).load(Ordering::Acquire) },
            tail: unsafe { self.tail.cast::<u32>().read_volatile() },
            queue: self,
        }
    }

    #[inline]
    pub(crate) fn submitter_parts(&self) -> (*const AtomicU32, *const AtomicU32, *const AtomicU32) {
        (self.head, self.tail, self.flags)
    }
}

/// A queue-local view of the submission ring.
pub struct SubmissionQueue<'ring> {
    head: u32,
    tail: u32,
    queue: &'ring Inner,
}

/// Returned when the submission ring has no free slot.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PushError;

impl fmt::Display for PushError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("submission queue is full")
    }
}

impl SubmissionQueue<'_> {
    /// Publish the local tail and refresh the kernel-owned head.
    #[inline]
    pub fn sync(&mut self) {
        unsafe {
            (*self.queue.tail).store(self.tail, Ordering::Release);
            self.head = (*self.queue.head).load(Ordering::Acquire);
        }
    }

    /// Check whether an SQPOLL kernel thread needs a wakeup syscall.
    #[inline]
    pub fn need_wakeup(&self) -> bool {
        atomic::fence(Ordering::SeqCst);
        unsafe { (*self.queue.flags).load(Ordering::Relaxed) & sys::IORING_SQ_NEED_WAKEUP != 0 }
    }

    #[inline]
    pub fn dropped(&self) -> u32 {
        unsafe { (*self.queue.dropped).load(Ordering::Acquire) }
    }

    #[inline]
    pub fn capacity(&self) -> usize {
        self.queue.ring_entries as usize
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.tail.wrapping_sub(self.head) as usize
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[inline]
    pub fn is_full(&self) -> bool {
        self.len() >= self.capacity()
    }

    /// Add an SQE to the local ring.
    ///
    /// # Safety
    ///
    /// All pointer values contained in `entry` must remain valid until the
    /// kernel has consumed and completed the corresponding request. The queue
    /// must not outlive the `IoUring` that owns its mapped memory.
    #[inline]
    pub unsafe fn push(&mut self, entry: &Entry) -> core::result::Result<(), PushError> {
        if self.is_full() {
            return Err(PushError);
        }

        let index = (self.tail & self.queue.ring_mask) as usize;
        unsafe { self.queue.sqes.add(index).write(*entry) };
        if !self.queue.array.is_null() {
            unsafe { self.queue.array.add(index).write_volatile(index as u32) };
        }
        self.tail = self.tail.wrapping_add(1);
        Ok(())
    }
}

impl Drop for SubmissionQueue<'_> {
    #[inline]
    fn drop(&mut self) {
        unsafe {
            (*self.queue.tail).store(self.tail, Ordering::Release);
        }
    }
}

#[cfg(test)]
mod tests {
    use core::sync::atomic::AtomicU32;

    use super::*;

    #[test]
    fn push_publishes_tail_and_rejects_full_queue() {
        let head = AtomicU32::new(0);
        let tail = AtomicU32::new(0);
        let flags = AtomicU32::new(0);
        let dropped = AtomicU32::new(0);
        let mut array = [0_u32; 2];
        let mut sqes = [Entry::default(); 2];
        let inner = Inner {
            head: &head,
            tail: &tail,
            ring_mask: 1,
            ring_entries: 2,
            flags: &flags,
            dropped: &dropped,
            array: array.as_mut_ptr(),
            sqes: sqes.as_mut_ptr(),
        };

        let mut queue = unsafe { inner.borrow() };
        let first = Entry::default().user_data(11);
        let second = Entry::default().user_data(22);
        assert!(unsafe { queue.push(&first) }.is_ok());
        assert!(unsafe { queue.push(&second) }.is_ok());
        assert!(unsafe { queue.push(&first) }.is_err());
        assert_eq!(queue.len(), 2);
        drop(queue);

        assert_eq!(tail.load(Ordering::Acquire), 2);
        assert_eq!(array, [0, 1]);
        assert_eq!(sqes[0].get_user_data(), 11);
        assert_eq!(sqes[1].get_user_data(), 22);
    }

    #[test]
    fn sync_refreshes_head_after_kernel_consumes_entries() {
        let head = AtomicU32::new(0);
        let tail = AtomicU32::new(1);
        let flags = AtomicU32::new(sys::IORING_SQ_NEED_WAKEUP);
        let dropped = AtomicU32::new(3);
        let mut sqes = [Entry::default(); 2];
        let inner = Inner {
            head: &head,
            tail: &tail,
            ring_mask: 1,
            ring_entries: 2,
            flags: &flags,
            dropped: &dropped,
            array: core::ptr::null_mut(),
            sqes: sqes.as_mut_ptr(),
        };

        let mut queue = unsafe { inner.borrow() };
        assert_eq!(queue.len(), 1);
        assert!(queue.need_wakeup());
        assert_eq!(queue.dropped(), 3);
        head.store(1, Ordering::Release);
        queue.sync();
        assert!(queue.is_empty());
    }
}
