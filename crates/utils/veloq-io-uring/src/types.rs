//! Public ABI types used by submission and registration.

use core::{
    convert::TryFrom,
    marker::PhantomData,
    mem::{align_of, size_of},
    ptr,
    ptr::NonNull,
    sync::atomic::{AtomicU16, Ordering},
};

use veloq_std::{
    io::{Error, ErrorKind, Result},
    os::unix::fd::RawFd,
    time::Duration,
};

use crate::sys;

/// A raw file descriptor that has not been registered with the ring.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(transparent)]
pub struct Fd(pub RawFd);

/// An index into the ring's registered file table.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(transparent)]
pub struct Fixed(pub u32);

/// A descriptor selector accepted by opcodes that support fixed files.
pub trait UseFixed: Copy {
    /// Return the raw descriptor when this is a normal file descriptor.
    fn raw_fd(self) -> Option<RawFd>;

    /// Return the registered-file index when this is a fixed descriptor.
    fn fixed_index(self) -> Option<u32>;
}

impl UseFixed for Fd {
    #[inline]
    fn raw_fd(self) -> Option<RawFd> {
        Some(self.0)
    }

    #[inline]
    fn fixed_index(self) -> Option<u32> {
        None
    }
}

impl UseFixed for Fixed {
    #[inline]
    fn raw_fd(self) -> Option<RawFd> {
        None
    }

    #[inline]
    fn fixed_index(self) -> Option<u32> {
        Some(self.0)
    }
}

/// Flags accepted by the `Fsync` operation.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[repr(transparent)]
pub struct FsyncFlags(u32);

impl FsyncFlags {
    /// Flush data without requiring metadata to be synchronized.
    pub const DATASYNC: Self = Self(sys::IORING_FSYNC_DATASYNC);

    #[inline]
    pub const fn empty() -> Self {
        Self(0)
    }

    #[inline]
    pub const fn bits(self) -> u32 {
        self.0
    }

    #[inline]
    pub const fn from_bits_retain(bits: u32) -> Self {
        Self(bits)
    }

    #[inline]
    pub const fn contains(self, other: Self) -> bool {
        self.0 & other.0 == other.0
    }

    #[inline]
    pub const fn union(self, other: Self) -> Self {
        Self(self.0 | other.0)
    }
}

/// Flags accepted by the `Timeout` operation.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[repr(transparent)]
pub struct TimeoutFlags(u32);

impl TimeoutFlags {
    /// Treat the timespec as an absolute deadline.
    pub const ABS: Self = Self(sys::IORING_TIMEOUT_ABS);
    /// Update a linked timeout request.
    pub const UPDATE: Self = Self(sys::IORING_TIMEOUT_UPDATE);
    /// Use the boottime clock instead of the monotonic clock.
    pub const BOOTTIME: Self = Self(sys::IORING_TIMEOUT_BOOTTIME);
    /// Use the realtime clock instead of the monotonic clock.
    pub const REALTIME: Self = Self(sys::IORING_TIMEOUT_REALTIME);
    /// Update a linked timeout rather than creating a new timeout.
    pub const LINK_TIMEOUT_UPDATE: Self = Self(sys::IORING_LINK_TIMEOUT_UPDATE);
    /// Preserve linked requests when the timeout expires.
    pub const ETIME_SUCCESS: Self = Self(sys::IORING_TIMEOUT_ETIME_SUCCESS);
    /// Keep producing timeout completions.
    pub const MULTISHOT: Self = Self(sys::IORING_TIMEOUT_MULTISHOT);

    #[inline]
    pub const fn empty() -> Self {
        Self(0)
    }

    #[inline]
    pub const fn bits(self) -> u32 {
        self.0
    }

    #[inline]
    pub const fn from_bits_retain(bits: u32) -> Self {
        Self(bits)
    }

    #[inline]
    pub const fn contains(self, other: Self) -> bool {
        self.0 & other.0 == other.0
    }

    #[inline]
    pub const fn union(self, other: Self) -> Self {
        Self(self.0 | other.0)
    }
}

/// Matching criteria accepted by the asynchronous cancel operation.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[repr(transparent)]
pub struct AsyncCancelFlags(u32);

impl AsyncCancelFlags {
    /// Cancel every request matching the criteria.
    pub const ALL: Self = Self(sys::IORING_ASYNC_CANCEL_ALL);
    /// Match the original request's raw file descriptor.
    pub const FD: Self = Self(sys::IORING_ASYNC_CANCEL_FD);
    /// Match any request in the ring.
    pub const ANY: Self = Self(sys::IORING_ASYNC_CANCEL_ANY);
    /// Match the original request's fixed file descriptor.
    pub const FD_FIXED: Self = Self(sys::IORING_ASYNC_CANCEL_FD_FIXED);
    /// Match the request's user data.
    pub const USERDATA: Self = Self(sys::IORING_ASYNC_CANCEL_USERDATA);
    /// Match the request's opcode.
    pub const OP: Self = Self(sys::IORING_ASYNC_CANCEL_OP);

    #[inline]
    pub const fn empty() -> Self {
        Self(0)
    }

    #[inline]
    pub const fn bits(self) -> u32 {
        self.0
    }

    #[inline]
    pub const fn from_bits_retain(bits: u32) -> Self {
        Self(bits)
    }

    #[inline]
    pub const fn contains(self, other: Self) -> bool {
        self.0 & other.0 == other.0
    }

    #[inline]
    pub const fn union(self, other: Self) -> Self {
        Self(self.0 | other.0)
    }
}

/// The Linux `__kernel_timespec` used by extended enter waits.
#[derive(Clone, Copy, Debug, Default)]
#[repr(transparent)]
pub struct Timespec(pub(crate) sys::KernelTimespec);

impl Timespec {
    #[inline]
    #[must_use]
    pub const fn new() -> Self {
        Self(sys::KernelTimespec {
            tv_sec: 0,
            tv_nsec: 0,
        })
    }

    #[inline]
    pub fn sec(mut self, sec: u64) -> Result<Self> {
        self.0.tv_sec = i64::try_from(sec).map_err(|_| Error::from(ErrorKind::InvalidInput))?;
        Ok(self)
    }

    #[inline]
    pub fn nsec(mut self, nsec: u32) -> Result<Self> {
        if nsec >= 1_000_000_000 {
            return Err(Error::from(ErrorKind::InvalidInput));
        }
        self.0.tv_nsec = i64::from(nsec);
        Ok(self)
    }
}

impl TryFrom<Duration> for Timespec {
    type Error = Error;

    #[inline]
    fn try_from(duration: Duration) -> Result<Self> {
        Self::new()
            .sec(duration.as_secs())?
            .nsec(duration.subsec_nanos())
    }
}

/// Arguments passed to `io_uring_enter` with `EXT_ARG`.
///
/// The lifetime transition in the builder methods prevents a timespec or
/// signal mask from being dropped while the resulting argument is retained.
#[derive(Clone, Copy, Debug, Default)]
#[repr(transparent)]
pub struct SubmitArgs<'prev: 'now, 'now> {
    pub(crate) args: sys::IoUringGeteventsArg,
    prev: PhantomData<&'prev ()>,
    now: PhantomData<&'now ()>,
}

impl SubmitArgs<'static, 'static> {
    #[inline]
    #[must_use]
    pub const fn new() -> Self {
        Self {
            args: sys::IoUringGeteventsArg {
                sigmask: 0,
                sigmask_size: 0,
                min_wait_usec: 0,
                timespec: 0,
            },
            prev: PhantomData,
            now: PhantomData,
        }
    }
}

impl<'prev, 'now> SubmitArgs<'prev, 'now> {
    /// Set the signal mask used while waiting.
    #[inline]
    #[must_use]
    pub fn sigmask<'next>(self, sigmask: &'next libc::sigset_t) -> SubmitArgs<'now, 'next> {
        let mut args = self.args;
        args.sigmask = ptr::from_ref(sigmask) as u64;
        args.sigmask_size = core::mem::size_of::<libc::sigset_t>() as u32;
        SubmitArgs {
            args,
            prev: PhantomData,
            now: PhantomData,
        }
    }

    /// Set the minimum wait timeout timespec.
    #[inline]
    #[must_use]
    pub fn timespec<'next>(self, timespec: &'next Timespec) -> SubmitArgs<'now, 'next> {
        let mut args = self.args;
        args.timespec = ptr::from_ref(&timespec.0) as u64;
        SubmitArgs {
            args,
            prev: PhantomData,
            now: PhantomData,
        }
    }

    /// Reborrow the argument for one `io_uring_enter` call.
    #[inline]
    #[must_use]
    pub fn reborrow(self) -> SubmitArgs<'now, 'now> {
        SubmitArgs {
            args: self.args,
            prev: PhantomData,
            now: PhantomData,
        }
    }

    pub(crate) fn validate(&self) -> Result<()> {
        if (self.args.sigmask == 0) != (self.args.sigmask_size == 0) {
            return Err(Error::from(ErrorKind::InvalidInput));
        }
        if self.args.sigmask != 0 && self.args.sigmask_size != size_of::<libc::sigset_t>() as u32 {
            return Err(Error::from(ErrorKind::InvalidInput));
        }
        if self.args.sigmask != 0
            && !(self.args.sigmask as usize).is_multiple_of(align_of::<libc::sigset_t>())
        {
            return Err(Error::from(ErrorKind::InvalidInput));
        }
        if self.args.timespec != 0
            && !(self.args.timespec as usize).is_multiple_of(align_of::<sys::KernelTimespec>())
        {
            return Err(Error::from(ErrorKind::InvalidInput));
        }
        Ok(())
    }
}

/// An entry in a provided-buffer ring.
#[derive(Clone, Copy, Debug, Default)]
#[repr(transparent)]
pub struct BufRingEntry(sys::IoUringBuf);

impl BufRingEntry {
    #[inline]
    fn write(&mut self, item: BufRingItem) {
        self.0.addr = item.addr;
        self.0.len = item.len;
        self.0.bid = item.bid;
    }

    #[cfg(test)]
    #[inline]
    fn read(&self) -> BufRingItem {
        BufRingItem {
            addr: self.0.addr,
            len: self.0.len,
            bid: self.0.bid,
        }
    }
}

/// A validated buffer descriptor ready to be published to a provided-buffer ring.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BufRingItem {
    addr: u64,
    len: u32,
    bid: u16,
}

#[allow(clippy::len_without_is_empty)]
impl BufRingItem {
    /// Create a descriptor for a non-empty buffer with a non-null address.
    ///
    /// # Safety
    ///
    /// The memory starting at `addr` must be valid for `len` bytes, writable by the kernel, and
    /// remain valid until the kernel consumes the descriptor or the provided-buffer ring is
    /// unregistered.
    pub unsafe fn new(addr: u64, len: u32, bid: u16) -> Result<Self> {
        if addr == 0 || len == 0 {
            return Err(Error::from(ErrorKind::InvalidInput));
        }
        Ok(Self { addr, len, bid })
    }

    #[inline]
    pub fn addr(self) -> u64 {
        self.addr
    }

    #[inline]
    pub fn len(self) -> u32 {
        self.len
    }

    #[inline]
    pub fn bid(self) -> u16 {
        self.bid
    }
}

/// Safe writer for a registered provided-buffer ring.
///
/// The constructor is unsafe because the caller must keep the pointed-to mapping alive and
/// exclusively owned for the lifetime of this value. Once constructed, entries are written and
/// the shared tail is published as one release operation per batch; callers never manipulate the
/// kernel-visible entry or tail pointers directly.
pub struct ProvidedBufRing {
    base: NonNull<BufRingEntry>,
    tail_ptr: NonNull<AtomicU16>,
    entries: u16,
    mask: u16,
    tail: u16,
}

impl ProvidedBufRing {
    /// Bind a userspace view to the mapping backing a provided-buffer ring.
    ///
    /// # Safety
    ///
    /// `base` must be a non-null, [`BufRingEntry`]-aligned pointer to at least `entries`
    /// initialized entries in a writable mapping. The mapping must remain valid, and no other
    /// code may mutate the entries or shared tail, until the returned value is dropped.
    pub unsafe fn from_raw_parts(base: *mut BufRingEntry, entries: u16) -> Result<Self> {
        if base.is_null()
            || !(entries as usize).is_power_of_two()
            || entries > (1 << 15)
            || !(base as usize).is_multiple_of(align_of::<BufRingEntry>())
        {
            return Err(Error::from(ErrorKind::InvalidInput));
        }

        let tail = unsafe { ptr::addr_of_mut!((*base).0.resv).cast::<AtomicU16>() };

        Ok(Self {
            // SAFETY: null and alignment were checked above.
            base: unsafe { NonNull::new_unchecked(base) },
            // SAFETY: `base` is non-null and points to initialized, ABI-aligned entries. The
            // reserved field is the shared provided-ring tail and is naturally aligned for
            // `AtomicU16`.
            tail_ptr: unsafe { NonNull::new_unchecked(tail) },
            entries,
            mask: entries - 1,
            tail: 0,
        })
    }

    /// Publish all descriptors in `items` with one release store to the shared tail.
    pub fn publish(&mut self, items: &[BufRingItem]) -> Result<()> {
        if items.len() > self.entries as usize {
            return Err(Error::from(ErrorKind::InvalidInput));
        }

        let mut next_tail = self.tail;
        for item in items {
            let index = (next_tail & self.mask) as usize;
            // SAFETY: the constructor proved that the mapping contains `entries` entries and
            // `index` is masked into that range. `&mut self` provides exclusive userspace access.
            unsafe {
                (*self.base.as_ptr().add(index)).write(*item);
            }
            next_tail = next_tail.wrapping_add(1);
        }

        if !items.is_empty() {
            // SAFETY: the constructor validated the entry mapping and cached the shared tail
            // pointer. The kernel and this owner use the field through the same atomic ABI.
            unsafe {
                self.tail_ptr.as_ref().store(next_tail, Ordering::Release);
            }
            self.tail = next_tail;
        }
        Ok(())
    }

    #[inline]
    pub fn entries(&self) -> u16 {
        self.entries
    }

    #[cfg(test)]
    #[inline]
    fn entry(&self, index: usize) -> Option<BufRingItem> {
        (index < self.entries as usize).then(|| {
            // SAFETY: the index is within the validated mapping and tests have exclusive access.
            unsafe { (*self.base.as_ptr().add(index)).read() }
        })
    }

    #[cfg(test)]
    #[inline]
    fn tail(&self) -> u16 {
        self.tail
    }
}

#[cfg(test)]
mod tests {
    use core::mem::{align_of, offset_of, size_of};

    use super::*;

    #[test]
    fn public_types_match_kernel_layouts() {
        assert_eq!(size_of::<Timespec>(), 16);
        assert_eq!(align_of::<Timespec>(), 8);
        assert_eq!(size_of::<SubmitArgs<'static, 'static>>(), 24);
        assert_eq!(size_of::<BufRingEntry>(), 16);
        assert_eq!(align_of::<BufRingEntry>(), 8);
        assert_eq!(offset_of!(sys::IoUringBuf, len), 8);
        assert_eq!(offset_of!(sys::IoUringBuf, bid), 12);
        assert_eq!(offset_of!(sys::IoUringBuf, resv), 14);
    }

    #[test]
    fn timespec_accepts_veloq_duration() {
        let timespec = Timespec::try_from(Duration::new(3, 17)).unwrap();
        assert_eq!(timespec.0.tv_sec, 3);
        assert_eq!(timespec.0.tv_nsec, 17);
    }

    #[test]
    fn timespec_rejects_values_that_do_not_fit_kernel_abi() {
        assert!(Timespec::new().sec(u64::MAX).is_err());
        assert!(Timespec::new().nsec(1_000_000_000).is_err());
    }

    #[test]
    fn submit_args_reject_invalid_pointer_fields() {
        let mut missing_size = SubmitArgs::new();
        missing_size.args.sigmask = 1;
        assert!(missing_size.validate().is_err());

        let mut misaligned_sigmask = SubmitArgs::new();
        misaligned_sigmask.args.sigmask = 1;
        misaligned_sigmask.args.sigmask_size = size_of::<libc::sigset_t>() as u32;
        assert!(misaligned_sigmask.validate().is_err());

        let mut misaligned_timespec = SubmitArgs::new();
        misaligned_timespec.args.timespec = 1;
        assert!(misaligned_timespec.validate().is_err());
    }

    #[test]
    fn provided_ring_publishes_a_batch_with_one_tail_advance() {
        let mut storage = [BufRingEntry::default(); 4];
        let mut backing = [0_u8; 256];
        let mut ring = unsafe {
            ProvidedBufRing::from_raw_parts(storage.as_mut_ptr(), 4)
                .expect("aligned test storage must be accepted")
        };
        let items = [
            unsafe { BufRingItem::new(backing.as_mut_ptr() as u64, 64, 2) }.expect("valid item"),
            unsafe { BufRingItem::new(backing.as_mut_ptr().wrapping_add(128) as u64, 128, 3) }
                .expect("valid item"),
        ];

        ring.publish(&items)
            .expect("batch publication must succeed");

        assert_eq!(ring.tail(), 2);
        assert_eq!(ring.entry(0), Some(items[0]));
        assert_eq!(ring.entry(1), Some(items[1]));
    }

    #[test]
    fn provided_ring_rejects_invalid_descriptors_and_oversized_batches() {
        let mut backing = [0_u8; 3];
        let address = backing.as_mut_ptr() as u64;
        assert!(unsafe { BufRingItem::new(0, 64, 0) }.is_err());
        assert!(unsafe { BufRingItem::new(address, 0, 0) }.is_err());

        let mut storage = [BufRingEntry::default(); 2];
        let mut ring = unsafe {
            ProvidedBufRing::from_raw_parts(storage.as_mut_ptr(), 2).expect("valid ring")
        };
        let items = [
            unsafe { BufRingItem::new(address, 1, 0) }.unwrap(),
            unsafe { BufRingItem::new(address + 1, 1, 1) }.unwrap(),
            unsafe { BufRingItem::new(address + 2, 1, 2) }.unwrap(),
        ];
        assert!(ring.publish(&items).is_err());
        assert_eq!(ring.tail(), 0);
    }
}
