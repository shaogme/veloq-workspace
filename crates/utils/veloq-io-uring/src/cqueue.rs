//! Completion queue access for the Linux shared ring.

use core::sync::atomic::{AtomicU32, Ordering};

use crate::{mmap::Mmap, sys};

/// The 16-byte completion queue entry defined by the Linux ABI.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct Entry {
    pub(crate) inner: sys::IoUringCqe,
}

impl Entry {
    #[inline]
    pub fn user_data(&self) -> u64 {
        self.inner.user_data
    }

    #[inline]
    pub fn result(&self) -> i32 {
        self.inner.result
    }

    #[inline]
    pub fn flags(&self) -> u32 {
        self.inner.flags
    }
}

/// Return the buffer id encoded in CQE flags, if a buffer was selected.
#[inline]
pub fn buffer_select(flags: u32) -> Option<u16> {
    if flags & sys::IORING_CQE_F_BUFFER == 0 {
        None
    } else {
        Some((flags >> sys::IORING_CQE_BUFFER_SHIFT) as u16)
    }
}

/// Whether a CQE belongs to a multishot request that remains active.
#[inline]
pub fn more(flags: u32) -> bool {
    flags & sys::IORING_CQE_F_MORE != 0
}

pub(crate) struct Inner {
    head: *const AtomicU32,
    tail: *const AtomicU32,
    ring_mask: u32,
    ring_entries: u32,
    overflow: *const AtomicU32,
    cqes: *const Entry,
    flags: *const AtomicU32,
}

impl Inner {
    /// Bind queue pointers to kernel-provided offsets in the mapped ring.
    pub(crate) unsafe fn new(cq_mmap: &Mmap, params: &sys::IoUringParams) -> Self {
        let head = unsafe { cq_mmap.offset(params.cq_off.head).cast() };
        let tail = unsafe { cq_mmap.offset(params.cq_off.tail).cast() };
        let ring_mask = unsafe { cq_mmap.offset(params.cq_off.ring_mask).cast::<u32>().read() };
        let ring_entries = unsafe {
            cq_mmap
                .offset(params.cq_off.ring_entries)
                .cast::<u32>()
                .read()
        };
        let overflow = unsafe { cq_mmap.offset(params.cq_off.overflow).cast() };
        let cqes = unsafe { cq_mmap.offset(params.cq_off.cqes).cast() };
        let flags = unsafe { cq_mmap.offset(params.cq_off.flags).cast() };

        Self {
            head,
            tail,
            ring_mask,
            ring_entries,
            overflow,
            cqes,
            flags,
        }
    }

    #[inline]
    pub(crate) unsafe fn borrow(&self) -> CompletionQueue<'_> {
        CompletionQueue {
            head: unsafe { self.head.cast::<u32>().read_volatile() },
            tail: unsafe { (*self.tail).load(Ordering::Acquire) },
            queue: self,
        }
    }
}

/// A queue-local view of the completion ring.
pub struct CompletionQueue<'ring> {
    head: u32,
    tail: u32,
    queue: &'ring Inner,
}

impl CompletionQueue<'_> {
    /// Publish consumed CQEs and refresh the kernel-owned tail.
    #[inline]
    pub fn sync(&mut self) {
        unsafe {
            (*self.queue.head).store(self.head, Ordering::Release);
            self.tail = (*self.queue.tail).load(Ordering::Acquire);
        }
    }

    #[inline]
    pub fn overflow(&self) -> u32 {
        unsafe { (*self.queue.overflow).load(Ordering::Acquire) }
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

    /// Copy and consume the next CQE.
    #[inline]
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Option<Entry> {
        if self.head == self.tail {
            return None;
        }

        let index = (self.head & self.queue.ring_mask) as usize;
        let entry = unsafe { self.queue.cqes.add(index).read() };
        self.head = self.head.wrapping_add(1);
        Some(entry)
    }

    /// Whether CQ eventfd notifications are disabled for this ring.
    #[inline]
    pub fn eventfd_disabled(&self) -> bool {
        unsafe {
            (*self.queue.flags).load(Ordering::Acquire) & sys::IORING_CQ_EVENTFD_DISABLED != 0
        }
    }
}

impl Iterator for CompletionQueue<'_> {
    type Item = Entry;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        CompletionQueue::next(self)
    }
}

impl ExactSizeIterator for CompletionQueue<'_> {
    #[inline]
    fn len(&self) -> usize {
        CompletionQueue::len(self)
    }
}

impl Drop for CompletionQueue<'_> {
    #[inline]
    fn drop(&mut self) {
        unsafe {
            (*self.queue.head).store(self.head, Ordering::Release);
        }
    }
}

#[cfg(test)]
mod tests {
    use core::sync::atomic::AtomicU32;

    use super::*;

    #[test]
    fn next_copies_cqe_and_publishes_consumed_head() {
        let head = AtomicU32::new(0);
        let tail = AtomicU32::new(1);
        let overflow = AtomicU32::new(7);
        let flags = AtomicU32::new(0);
        let mut entries = [Entry {
            inner: sys::IoUringCqe {
                user_data: 42,
                result: -libc::EAGAIN,
                flags: sys::IORING_CQE_F_BUFFER
                    | sys::IORING_CQE_F_MORE
                    | (9 << sys::IORING_CQE_BUFFER_SHIFT),
                big_cqe: [],
            },
        }];
        let inner = Inner {
            head: &head,
            tail: &tail,
            ring_mask: 0,
            ring_entries: 1,
            overflow: &overflow,
            cqes: entries.as_mut_ptr(),
            flags: &flags,
        };

        let mut queue = unsafe { inner.borrow() };
        let entry = queue.next().expect("one CQE");
        assert_eq!(entry.user_data(), 42);
        assert_eq!(entry.result(), -libc::EAGAIN);
        assert_eq!(buffer_select(entry.flags()), Some(9));
        assert!(more(entry.flags()));
        assert_eq!(queue.overflow(), 7);
        assert!(queue.is_empty());
        drop(queue);
        assert_eq!(head.load(Ordering::Acquire), 1);
    }

    #[test]
    fn buffer_helpers_ignore_unrelated_flags() {
        assert_eq!(buffer_select(0), None);
        assert!(!more(0));
    }
}
