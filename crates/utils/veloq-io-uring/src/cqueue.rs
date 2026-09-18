//! Completion queue access for the Linux shared ring.

use core::{marker::PhantomData, mem::size_of, sync::atomic::Ordering};

use veloq_std::io::{Error, Result};

use crate::{mmap::Mmap, squeue::SharedAtomicU32, sys};

/// The 16-byte completion queue entry defined by the Linux ABI.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct Entry {
    pub(crate) inner: sys::IoUringCqe,
}

/// Flags returned in a completion queue entry.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct Flags(u32);

impl Flags {
    /// The completion selected a buffer from a provided-buffer ring.
    pub const BUFFER: Self = Self(sys::IORING_CQE_F_BUFFER);
    /// The originating request remains active after this completion.
    pub const MORE: Self = Self(sys::IORING_CQE_F_MORE);
    /// The socket still has data available.
    pub const SOCK_NONEMPTY: Self = Self(sys::IORING_CQE_F_SOCK_NONEMPTY);
    /// The completion is a zerocopy notification.
    pub const NOTIF: Self = Self(sys::IORING_CQE_F_NOTIF);
    /// More buffers remain in the selected buffer bundle.
    pub const BUF_MORE: Self = Self(sys::IORING_CQE_F_BUF_MORE);

    #[inline]
    pub const fn from_bits_retain(bits: u32) -> Self {
        Self(bits)
    }

    #[inline]
    pub const fn bits(self) -> u32 {
        self.0
    }

    #[inline]
    pub const fn contains(self, other: Self) -> bool {
        self.0 & other.0 == other.0
    }
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
    if !Flags::from_bits_retain(flags).contains(Flags::BUFFER) {
        None
    } else {
        Some((flags >> sys::IORING_CQE_BUFFER_SHIFT) as u16)
    }
}

/// Whether a CQE belongs to a multishot request that remains active.
#[inline]
pub fn more(flags: u32) -> bool {
    Flags::from_bits_retain(flags).contains(Flags::MORE)
}

pub(crate) struct Inner {
    head: SharedAtomicU32,
    tail: SharedAtomicU32,
    ring_mask: u32,
    ring_entries: u32,
    overflow: SharedAtomicU32,
    cqes: *const Entry,
    flags: SharedAtomicU32,
}

impl Inner {
    /// Bind queue pointers to kernel-provided offsets in the mapped ring.
    pub(crate) unsafe fn new(cq_mmap: &Mmap, params: &sys::IoUringParams) -> Result<Self> {
        let head = unsafe {
            SharedAtomicU32::from_ptr(cq_mmap.range(params.cq_off.head, size_of::<u32>())?.cast())
        };
        let tail = unsafe {
            SharedAtomicU32::from_ptr(cq_mmap.range(params.cq_off.tail, size_of::<u32>())?.cast())
        };
        let ring_mask = unsafe {
            cq_mmap
                .range(params.cq_off.ring_mask, size_of::<u32>())?
                .cast::<u32>()
                .read_volatile()
        };
        let ring_entries = unsafe {
            cq_mmap
                .range(params.cq_off.ring_entries, size_of::<u32>())?
                .cast::<u32>()
                .read_volatile()
        };
        if ring_entries == 0
            || !ring_entries.is_power_of_two()
            || ring_entries != params.cq_entries
            || ring_mask != ring_entries - 1
        {
            return Err(Error::from_raw_os_error(libc::EINVAL));
        }
        let overflow = unsafe {
            SharedAtomicU32::from_ptr(
                cq_mmap
                    .range(params.cq_off.overflow, size_of::<u32>())?
                    .cast(),
            )
        };
        let cqes_len = (ring_entries as usize)
            .checked_mul(size_of::<Entry>())
            .ok_or_else(|| Error::from_raw_os_error(libc::EOVERFLOW))?;
        let cqes = cq_mmap.range(params.cq_off.cqes, cqes_len)?.cast();
        let flags = unsafe {
            SharedAtomicU32::from_ptr(cq_mmap.range(params.cq_off.flags, size_of::<u32>())?.cast())
        };

        Ok(Self {
            head,
            tail,
            ring_mask,
            ring_entries,
            overflow,
            cqes,
            flags,
        })
    }

    #[inline]
    pub(crate) fn borrow(&mut self) -> CompletionQueue<'_> {
        CompletionQueue {
            head: self.head.load_volatile(),
            tail: self.tail.load(Ordering::Acquire),
            queue: self,
            _owner: PhantomData,
        }
    }
}

/// A queue-local view of the completion ring.
pub struct CompletionQueue<'ring> {
    head: u32,
    tail: u32,
    queue: &'ring Inner,
    _owner: PhantomData<&'ring mut Inner>,
}

impl CompletionQueue<'_> {
    /// Publish consumed CQEs and refresh the kernel-owned tail.
    #[inline]
    pub fn sync(&mut self) {
        self.queue.head.store(self.head, Ordering::Release);
        self.tail = self.queue.tail.load(Ordering::Acquire);
    }

    #[inline]
    pub fn overflow(&self) -> u32 {
        self.queue.overflow.load(Ordering::Acquire)
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
        self.queue.flags.load(Ordering::Acquire) & sys::IORING_CQ_EVENTFD_DISABLED != 0
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
        self.queue.head.store(self.head, Ordering::Release);
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
            head: unsafe { SharedAtomicU32::from_ptr(&head) },
            tail: unsafe { SharedAtomicU32::from_ptr(&tail) },
            ring_mask: 0,
            ring_entries: 1,
            overflow: unsafe { SharedAtomicU32::from_ptr(&overflow) },
            cqes: entries.as_mut_ptr(),
            flags: unsafe { SharedAtomicU32::from_ptr(&flags) },
        };

        let mut inner = inner;
        let mut queue = inner.borrow();
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

    #[test]
    fn wraparound_and_overflow_are_visible_without_losing_cqes() {
        let head = AtomicU32::new(u32::MAX);
        let tail = AtomicU32::new(1);
        let overflow = AtomicU32::new(12);
        let flags = AtomicU32::new(0);
        let mut entries = [Entry::default(); 2];
        entries[1].inner.user_data = 99;
        let mut inner = Inner {
            head: unsafe { SharedAtomicU32::from_ptr(&head) },
            tail: unsafe { SharedAtomicU32::from_ptr(&tail) },
            ring_mask: 1,
            ring_entries: 2,
            overflow: unsafe { SharedAtomicU32::from_ptr(&overflow) },
            cqes: entries.as_mut_ptr(),
            flags: unsafe { SharedAtomicU32::from_ptr(&flags) },
        };

        let mut queue = inner.borrow();
        assert_eq!(queue.len(), 2);
        assert_eq!(queue.next().expect("wrapped CQE").user_data(), 99);
        assert_eq!(queue.next().expect("second CQE").user_data(), 0);
        assert_eq!(queue.overflow(), 12);
        assert!(queue.is_empty());
        drop(queue);
        assert_eq!(head.load(Ordering::Acquire), 1);
    }
}
