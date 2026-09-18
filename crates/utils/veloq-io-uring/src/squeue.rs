//! Submission queue access for the Linux shared ring.

use core::{
    fmt,
    marker::PhantomData,
    mem::size_of,
    sync::atomic::{self, AtomicU32, Ordering},
};

use veloq_std::io::{Error, Result};

use crate::{mmap::Mmap, sys};

/// The 64-byte submission queue entry defined by the Linux ABI.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct Entry {
    pub(crate) inner: sys::IoUringSqe,
}

/// An atomic field in the kernel-owned shared ring memory.
///
/// The pointer is constructed only after [`Mmap::range`] has checked the
/// complete field range and alignment. Keeping the pointer behind this type
/// makes every shared atomic access use the same ordering boundary.
#[derive(Clone, Copy)]
pub(crate) struct SharedAtomicU32(*const AtomicU32);

impl SharedAtomicU32 {
    /// # Safety
    ///
    /// `pointer` must point to an aligned, live `u32` in an io_uring shared
    /// mapping for the lifetime of this wrapper.
    #[inline]
    pub(crate) const unsafe fn from_ptr(pointer: *const AtomicU32) -> Self {
        Self(pointer)
    }

    #[inline]
    pub(crate) fn load(self, ordering: Ordering) -> u32 {
        // SAFETY: the constructor requires a live aligned shared field.
        unsafe { (*self.0).load(ordering) }
    }

    #[inline]
    pub(crate) fn load_volatile(self) -> u32 {
        // SAFETY: the constructor requires a live aligned shared field.
        unsafe { self.0.cast::<u32>().read_volatile() }
    }

    #[inline]
    pub(crate) fn store(self, value: u32, ordering: Ordering) {
        // SAFETY: the constructor requires a live aligned shared field.
        unsafe { (*self.0).store(value, ordering) }
    }
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
    /// Execute this SQE after all earlier SQEs have completed.
    pub const IO_DRAIN: Self = Self(sys::IOSQE_IO_DRAIN);
    /// Link this SQE to the following SQE.
    pub const IO_LINK: Self = Self(sys::IOSQE_IO_LINK);
    /// Link this SQE without cancelling the chain on failure.
    pub const IO_HARDLINK: Self = Self(sys::IOSQE_IO_HARDLINK);
    /// Force asynchronous execution when possible.
    pub const ASYNC: Self = Self(sys::IOSQE_ASYNC);
    /// Suppress a successful CQE for this SQE.
    pub const CQE_SKIP_SUCCESS: Self = Self(sys::IOSQE_CQE_SKIP_SUCCESS);

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

    /// Return whether all bits are defined by the Linux SQE ABI.
    #[inline]
    pub const fn is_known(self) -> bool {
        self.0 & !Self::KNOWN_BITS == 0
    }

    const KNOWN_BITS: u8 = sys::IOSQE_FIXED_FILE
        | sys::IOSQE_IO_DRAIN
        | sys::IOSQE_IO_LINK
        | sys::IOSQE_IO_HARDLINK
        | sys::IOSQE_ASYNC
        | sys::IOSQE_BUFFER_SELECT
        | sys::IOSQE_CQE_SKIP_SUCCESS;
}

impl Entry {
    #[inline]
    pub fn flags(mut self, flags: Flags) -> Self {
        debug_assert!(flags.is_known());
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
    head: SharedAtomicU32,
    tail: SharedAtomicU32,
    ring_mask: u32,
    ring_entries: u32,
    flags: SharedAtomicU32,
    dropped: SharedAtomicU32,
    array: *mut u32,
    sqes: *mut Entry,
}

impl Inner {
    /// Bind queue pointers to kernel-provided offsets in the mapped ring.
    pub(crate) unsafe fn new(
        sq_mmap: &Mmap,
        sqe_mmap: &Mmap,
        params: &sys::IoUringParams,
    ) -> Result<Self> {
        let head = unsafe {
            SharedAtomicU32::from_ptr(sq_mmap.range(params.sq_off.head, size_of::<u32>())?.cast())
        };
        let tail = unsafe {
            SharedAtomicU32::from_ptr(sq_mmap.range(params.sq_off.tail, size_of::<u32>())?.cast())
        };
        let ring_mask = unsafe {
            sq_mmap
                .range(params.sq_off.ring_mask, size_of::<u32>())?
                .cast::<u32>()
                .read_volatile()
        };
        let ring_entries = unsafe {
            sq_mmap
                .range(params.sq_off.ring_entries, size_of::<u32>())?
                .cast::<u32>()
                .read_volatile()
        };
        if ring_entries == 0
            || !ring_entries.is_power_of_two()
            || ring_entries != params.sq_entries
            || ring_mask != ring_entries - 1
        {
            return Err(Error::from_raw_os_error(libc::EINVAL));
        }
        let flags = unsafe {
            SharedAtomicU32::from_ptr(sq_mmap.range(params.sq_off.flags, size_of::<u32>())?.cast())
        };
        let dropped = unsafe {
            SharedAtomicU32::from_ptr(
                sq_mmap
                    .range(params.sq_off.dropped, size_of::<u32>())?
                    .cast(),
            )
        };
        let array = if params.flags & sys::IORING_SETUP_NO_SQARRAY == 0 {
            let array_len = (ring_entries as usize)
                .checked_mul(size_of::<u32>())
                .ok_or_else(|| Error::from_raw_os_error(libc::EOVERFLOW))?;
            sq_mmap.range(params.sq_off.array, array_len)?.cast::<u32>()
        } else {
            core::ptr::null_mut()
        };
        let sqe_len = (ring_entries as usize)
            .checked_mul(size_of::<Entry>())
            .ok_or_else(|| Error::from_raw_os_error(libc::EOVERFLOW))?;
        let sqes = sqe_mmap.range(0, sqe_len)?.cast();

        if !array.is_null() {
            for index in 0..ring_entries {
                unsafe { array.add(index as usize).write_volatile(index) };
            }
        }

        Ok(Self {
            head,
            tail,
            ring_mask,
            ring_entries,
            flags,
            dropped,
            array,
            sqes,
        })
    }

    #[inline]
    pub(crate) fn borrow(&mut self) -> SubmissionQueue<'_> {
        SubmissionQueue {
            head: self.head.load(Ordering::Acquire),
            tail: self.tail.load_volatile(),
            queue: self,
            _owner: PhantomData,
        }
    }

    #[inline]
    pub(crate) fn submitter_parts(&self) -> SubmissionState {
        SubmissionState {
            head: self.head,
            tail: self.tail,
            flags: self.flags,
        }
    }
}

/// The shared SQ state consumed by the submitter.
#[derive(Clone, Copy)]
pub(crate) struct SubmissionState {
    head: SharedAtomicU32,
    tail: SharedAtomicU32,
    flags: SharedAtomicU32,
}

impl SubmissionState {
    #[inline]
    pub(crate) fn len(self) -> usize {
        self.tail
            .load_volatile()
            .wrapping_sub(self.head.load(Ordering::Acquire)) as usize
    }

    #[inline]
    pub(crate) fn flags(self, ordering: Ordering) -> u32 {
        self.flags.load(ordering)
    }
}

/// A queue-local view of the submission ring.
pub struct SubmissionQueue<'ring> {
    head: u32,
    tail: u32,
    queue: &'ring Inner,
    _owner: PhantomData<&'ring mut Inner>,
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
        self.queue.tail.store(self.tail, Ordering::Release);
        self.head = self.queue.head.load(Ordering::Acquire);
    }

    /// Check whether an SQPOLL kernel thread needs a wakeup syscall.
    #[inline]
    pub fn need_wakeup(&self) -> bool {
        atomic::fence(Ordering::SeqCst);
        self.queue.flags.load(Ordering::Relaxed) & sys::IORING_SQ_NEED_WAKEUP != 0
    }

    #[inline]
    pub fn dropped(&self) -> u32 {
        self.queue.dropped.load(Ordering::Acquire)
    }

    /// Whether the CQ overflow flag is currently set.
    #[inline]
    pub fn cq_overflow(&self) -> bool {
        self.queue.flags.load(Ordering::Acquire) & sys::IORING_SQ_CQ_OVERFLOW != 0
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
        self.queue.tail.store(self.tail, Ordering::Release);
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
            head: unsafe { SharedAtomicU32::from_ptr(&head) },
            tail: unsafe { SharedAtomicU32::from_ptr(&tail) },
            ring_mask: 1,
            ring_entries: 2,
            flags: unsafe { SharedAtomicU32::from_ptr(&flags) },
            dropped: unsafe { SharedAtomicU32::from_ptr(&dropped) },
            array: array.as_mut_ptr(),
            sqes: sqes.as_mut_ptr(),
        };

        let mut inner = inner;
        let mut queue = inner.borrow();
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
        let flags = AtomicU32::new(sys::IORING_SQ_NEED_WAKEUP | sys::IORING_SQ_CQ_OVERFLOW);
        let dropped = AtomicU32::new(3);
        let mut sqes = [Entry::default(); 2];
        let inner = Inner {
            head: unsafe { SharedAtomicU32::from_ptr(&head) },
            tail: unsafe { SharedAtomicU32::from_ptr(&tail) },
            ring_mask: 1,
            ring_entries: 2,
            flags: unsafe { SharedAtomicU32::from_ptr(&flags) },
            dropped: unsafe { SharedAtomicU32::from_ptr(&dropped) },
            array: core::ptr::null_mut(),
            sqes: sqes.as_mut_ptr(),
        };

        let mut inner = inner;
        let mut queue = inner.borrow();
        assert_eq!(queue.len(), 1);
        assert!(queue.need_wakeup());
        assert!(queue.cq_overflow());
        assert_eq!(queue.dropped(), 3);
        head.store(1, Ordering::Release);
        queue.sync();
        assert!(queue.is_empty());
    }

    #[test]
    fn wraparound_preserves_full_and_empty_protocol() {
        let head = AtomicU32::new(u32::MAX - 1);
        let tail = AtomicU32::new(u32::MAX - 1);
        let flags = AtomicU32::new(0);
        let dropped = AtomicU32::new(0);
        let mut array = [0_u32; 2];
        let mut sqes = [Entry::default(); 2];
        let mut inner = Inner {
            head: unsafe { SharedAtomicU32::from_ptr(&head) },
            tail: unsafe { SharedAtomicU32::from_ptr(&tail) },
            ring_mask: 1,
            ring_entries: 2,
            flags: unsafe { SharedAtomicU32::from_ptr(&flags) },
            dropped: unsafe { SharedAtomicU32::from_ptr(&dropped) },
            array: array.as_mut_ptr(),
            sqes: sqes.as_mut_ptr(),
        };

        let mut queue = inner.borrow();
        assert!(queue.is_empty());
        assert!(unsafe { queue.push(&Entry::default()) }.is_ok());
        assert!(unsafe { queue.push(&Entry::default()) }.is_ok());
        assert!(queue.is_full());
        assert!(unsafe { queue.push(&Entry::default()) }.is_err());
        drop(queue);

        head.store(0, Ordering::Release);
        let mut queue = inner.borrow();
        queue.sync();
        assert_eq!(queue.len(), 0);
        assert!(queue.is_empty());
    }
}
